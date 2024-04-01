// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/kthread.h>
#include <linux/random.h>
#include <linux/sched/clock.h>
#include <linux/sort.h>
#include <linux/xarray.h>

#include "city.h"
#include "fifo.h"
#include "nvmev.h"
#include "demand_ftl.h"

#include "demand/cache.h"
#include "demand/coarse_old.h"
#include "demand/d_param.h"
#include "demand/demand.h"
#include "demand/utility.h"

/*
 * fastmode parameters -> for filling the device quickly.
 */
struct demand_shard* __fast_fill_shard;

void schedule_internal_operation(int sqid, unsigned long long nsecs_target,
				 struct buffer *write_buffer, unsigned int buffs_to_release);

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

bool kv_identify_nvme_io_cmd(struct nvmev_ns *ns, struct nvme_command cmd)
{
	return is_kv_cmd(cmd.common.opcode);
}

static unsigned int cmd_key_length(struct nvme_kv_command *cmd)
{
	if (cmd->common.opcode == nvme_cmd_kv_store) {
		return cmd->kv_store.key_len + 1;
	} else if (cmd->common.opcode == nvme_cmd_kv_retrieve) {
		return cmd->kv_retrieve.key_len + 1;
	} else if (cmd->common.opcode == nvme_cmd_kv_delete) {
		return cmd->kv_delete.key_len + 1;
	} else {
		return cmd->kv_store.key_len + 1;
	}
}

static unsigned int cmd_value_length(struct nvme_kv_command *cmd)
{
	if (cmd->common.opcode == nvme_cmd_kv_store) {
		return (cmd->kv_store.value_len << 2) - cmd->kv_store.invalid_byte;
	} else if (cmd->common.opcode == nvme_cmd_kv_retrieve) {
		return cmd->kv_retrieve.value_len << 2;
	} else if (cmd->common.opcode == nvme_cmd_kv_append) {
		return (cmd->kv_append.value_len << 2) - cmd->kv_append.invalid_byte;
    } else if (cmd->common.opcode == nvme_cmd_kv_delete) {
        return 0;
    } else {
        NVMEV_ASSERT(false);
    }
}

inline bool last_pg_in_wordline(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	return (ppa->g.pg % spp->pgs_per_oneshotpg) == (spp->pgs_per_oneshotpg - 1);
}

static bool should_gc(struct demand_shard *demand_shard)
{
	return (demand_shard->lm.free_line_cnt <= demand_shard->cp.gc_thres_lines);
}

static inline bool should_gc_high(struct demand_shard *demand_shard)
{
    NVMEV_DEBUG("Free LC %d:\n", demand_shard->lm.free_line_cnt);

    //if(demand_shard->lm.free_line_cnt <= 3) {
    //    struct list_head *p;
    //    struct line *my;
    //    list_for_each(p, &demand_shard->lm.free_line_list) {
    //        /* my points to the structure in which the list is embedded */
    //        my = list_entry(p, struct line, entry);
    //        NVMEV_ERROR("%d\n", my->id);
    //    }
    //}

	return demand_shard->lm.free_line_cnt <= demand_shard->cp.gc_thres_lines_high;
}

static inline struct ppa get_maptbl_ent(struct demand_shard *demand_shard, uint64_t lpn)
{
	return demand_shard->maptbl[lpn];
}

static inline void set_maptbl_ent(struct demand_shard *demand_shard, uint64_t lpn, struct ppa *ppa)
{
	NVMEV_ASSERT(lpn < demand_shard->ssd->sp.tt_pgs);
	demand_shard->maptbl[lpn] = *ppa;
}

uint64_t ppa2line(struct demand_shard *demand_shard, struct ppa *ppa)
{
    struct line* l = get_line(demand_shard, ppa); 
    return l->id;
}

ppa_t ppa2pgidx(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	ppa_t pgidx;

	NVMEV_DEBUG_VERBOSE("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", __func__,
			ppa->g.ch, ppa->g.lun, ppa->g.pl, ppa->g.blk, ppa->g.pg);

	pgidx = ppa->g.ch * spp->pgs_per_ch + ppa->g.lun * spp->pgs_per_lun +
		ppa->g.pl * spp->pgs_per_pl + ppa->g.blk * spp->pgs_per_blk + ppa->g.pg;

    if(pgidx > spp->tt_pgs) {
        NVMEV_ERROR("Tried to return page %u.\n", pgidx);
    }

	NVMEV_ASSERT(pgidx < spp->tt_pgs);

	return pgidx;
}

static inline uint64_t get_rmap_ent(struct demand_shard *demand_shard, struct ppa *ppa)
{
	uint64_t pgidx = ppa2pgidx(demand_shard, ppa);

	return demand_shard->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct demand_shard *demand_shard, uint64_t lpn, struct ppa *ppa)
{
	uint64_t pgidx = ppa2pgidx(demand_shard, ppa);

	demand_shard->rmap[pgidx] = lpn;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
	return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
	return ((struct line *)a)->vgc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
	((struct line *)a)->vgc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
	return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
	((struct line *)a)->pos = pos;
}

void consume_write_credit(struct demand_shard *demand_shard, uint32_t len)
{
	demand_shard->wfc.write_credits -= len;
    NVMEV_DEBUG("Consuming %u credits. %d remaining.\n", len,
                 demand_shard->wfc.write_credits);
}

static uint64_t forground_gc(struct demand_shard *demand_shard);
inline uint64_t check_and_refill_write_credit(struct demand_shard *demand_shard)
{
	struct write_flow_control *wfc = &(demand_shard->wfc);
    uint64_t nsecs_completed = 0;

	if ((int32_t) wfc->write_credits <= (int32_t) 0) {
		forground_gc(demand_shard);
		wfc->write_credits += wfc->credits_to_refill;
	} 

    return nsecs_completed;
}

static void init_lines(struct demand_shard *demand_shard)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	struct line_mgmt *lm = &demand_shard->lm;
	struct line *line;
	int i;

	lm->tt_lines = spp->blks_per_pl;
	NVMEV_ASSERT(lm->tt_lines == spp->tt_lines);
	lm->lines = vmalloc(sizeof(struct line) * lm->tt_lines);

	INIT_LIST_HEAD(&lm->free_line_list);
	INIT_LIST_HEAD(&lm->full_line_list);

	lm->victim_line_pq = pqueue_init(spp->tt_lines, victim_line_cmp_pri, victim_line_get_pri,
					 victim_line_set_pri, victim_line_get_pos,
					 victim_line_set_pos);

	lm->free_line_cnt = 0;
	for (i = 0; i < lm->tt_lines; i++) {
		lm->lines[i] = (struct line){
			.id = i,
			.ipc = 0,
			.vpc = 0,
            .vgc = 0,
            .igc = 0,
			.pos = 0,
            .map = false,
			.entry = LIST_HEAD_INIT(lm->lines[i].entry),
		};

		/* initialize all the lines as free lines */
		list_add_tail(&lm->lines[i].entry, &lm->free_line_list);
		lm->free_line_cnt++;
	}

	NVMEV_ASSERT(lm->free_line_cnt == lm->tt_lines);
	lm->victim_line_cnt = 0;
	lm->full_line_cnt = 0;
}

static void remove_lines(struct demand_shard *demand_shard)
{
	pqueue_free(demand_shard->lm.victim_line_pq);
	vfree(demand_shard->lm.lines);
}

static void init_write_flow_control(struct demand_shard *demand_shard)
{
	struct write_flow_control *wfc = &(demand_shard->wfc);
	struct ssdparams *spp = &demand_shard->ssd->sp;

	wfc->write_credits = spp->pgs_per_line * GRAIN_PER_PAGE;
	wfc->credits_to_refill = spp->pgs_per_line * GRAIN_PER_PAGE;
}

static void alloc_gc_mem(struct demand_shard *demand_shard) {
    struct gc_data *gcd = &demand_shard->gcd;
    struct ssdparams *spp = &demand_shard->ssd->sp;

    gcd->offset = GRAIN_PER_PAGE;
    gcd->last = false;
    gcd->lpa_lens = (struct lpa_len_ppa*) kzalloc(sizeof(struct lpa_len_ppa) * 
                                                  GRAIN_PER_PAGE * 
                                                  spp->pgs_per_blk, GFP_KERNEL);
    xa_init(&gcd->inv_mapping_xa);
    atomic_set(&gcd->gc_rem, 0);
}

static void free_gc_mem(struct demand_shard *demand_shard) {
    struct gc_data *gcd = &demand_shard->gcd;
    struct ssdparams *spp = &demand_shard->ssd->sp;

    gcd->offset = GRAIN_PER_PAGE;
    gcd->last = false;
    xa_destroy(&gcd->inv_mapping_xa);
}

static inline void check_addr(int a, int max)
{
	NVMEV_ASSERT(a >= 0 && a < max);
}

static struct line *get_next_free_line(struct demand_shard *demand_shard)
{
	struct line_mgmt *lm = &demand_shard->lm;
	struct line *curline = list_first_entry_or_null(&lm->free_line_list, struct line, entry);

	if (!curline) {
		NVMEV_ERROR("No free line left in VIRT !!!!\n");
        BUG_ON(true);
		return NULL;
	}

	list_del_init(&curline->entry);
	lm->free_line_cnt--;
	NVMEV_DEBUG("%s: free_line_cnt %d\n", __func__, lm->free_line_cnt);
	return curline;
}

static struct write_pointer *__get_wp(struct demand_shard *ftl, uint32_t io_type)
{
    if (io_type == USER_IO) {
        return &ftl->wp;
    } else if (io_type == MAP_IO) {
        return &ftl->map_wp;
    } else if (io_type == GC_IO) {
        return &ftl->gc_wp;
    } else if (io_type == GC_MAP_IO) {
        return &ftl->map_gc_wp;
    }

	NVMEV_ASSERT(0);
	return NULL;
}

static void prepare_write_pointer(struct demand_shard *demand_shard, uint32_t io_type)
{
	struct write_pointer *wp = __get_wp(demand_shard, io_type);
	struct line *curline = get_next_free_line(demand_shard);

    if(io_type == MAP_IO || io_type == GC_MAP_IO) {
        curline->map = true;
    } else {
        curline->map = false;
    }

	NVMEV_ASSERT(wp);
	NVMEV_ASSERT(curline);

    //NVMEV_INFO("Giving line %d to %u\n", curline->id, io_type);

	/* wp->curline is always our next-to-write super-block */
	*wp = (struct write_pointer){
		.curline = curline,
		.ch = 0,
		.lun = 0,
		.pg = 0,
		.blk = curline->id,
		.pl = 0,
	};
}

uint64_t prev_vgc = UINT_MAX;
uint64_t prev_blk = UINT_MAX;
bool advance_write_pointer(struct demand_shard *demand_shard, uint32_t io_type)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	struct line_mgmt *lm = &demand_shard->lm;
	struct write_pointer *wpp = __get_wp(demand_shard, io_type);

	NVMEV_DEBUG("current wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n",
			wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg);

    if(io_type == GC_IO ) {
        gc_pgs_this_gc++;
    } else if(io_type == GC_MAP_IO) {
        map_gc_pgs_this_gc++;
    } else {
        map_pgs_this_gc++;
    }

    struct ppa p;
    p.g.ch = wpp->ch;
    p.g.lun = wpp->lun;
    p.g.pl = wpp->pl;
    p.g.blk = wpp->blk;
    p.g.pg = wpp->pg;

    struct nand_block *blk = get_blk(demand_shard->ssd, &p);
    
    //if(wpp->blk == prev_blk && blk->vgc == prev_vgc) {
    //    NVMEV_INFO("caller is %ps\n", __builtin_return_address(0));
    //    NVMEV_INFO("caller is %ps\n", __builtin_return_address(1));
    //    NVMEV_INFO("caller is %ps\n", __builtin_return_address(2));
    //    NVMEV_ERROR("vgc %u prev vgc %u\n", blk->vgc, prev_vgc);
    //}

    //if(wpp->blk == prev_blk) {
    //    NVMEV_ASSERT(blk->vgc != prev_vgc);
    //}

    prev_vgc = blk->vgc;
    prev_blk = wpp->blk;

	check_addr(wpp->pg, spp->pgs_per_blk);
	wpp->pg++;
	if ((wpp->pg % spp->pgs_per_oneshotpg) != 0)
		goto out;

	wpp->pg -= spp->pgs_per_oneshotpg;
	check_addr(wpp->ch, spp->nchs);
	wpp->ch++;
	if (wpp->ch != spp->nchs)
		goto out;

	wpp->ch = 0;
	check_addr(wpp->lun, spp->luns_per_ch);
	wpp->lun++;
	/* in this case, we should go to next lun */
	if (wpp->lun != spp->luns_per_ch)
		goto out;

	wpp->lun = 0;
	/* go to next wordline in the block */
	wpp->pg += spp->pgs_per_oneshotpg;
	if (wpp->pg != spp->pgs_per_blk)
		goto out;

	wpp->pg = 0;

    NVMEV_DEBUG("vgc of curline %d (%ld)\n", 
                  wpp->curline->vgc, 
                  spp->pgs_per_line * GRAIN_PER_PAGE);

	/* move current line to {victim,full} line list */
	if (wpp->curline->igc == 0) {
		/* all pgs are still valid, move to full line list */
		NVMEV_ASSERT(wpp->curline->ipc == 0);
		list_add_tail(&wpp->curline->entry, &lm->full_line_list);
		lm->full_line_cnt++;
		NVMEV_DEBUG("wpp: move line %d to full_line_list\n", wpp->curline->id);
	} else {
		NVMEV_DEBUG("wpp: line %d is moved to victim list PQ\n", wpp->curline->id);
		//NVMEV_ASSERT(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
        NVMEV_ASSERT(wpp->curline->vgc >= 0 && wpp->curline->vgc < spp->pgs_per_line * GRAIN_PER_PAGE);
		/* there must be some invalid pages in this line */
		NVMEV_ASSERT(wpp->curline->igc > 0);
		pqueue_insert(lm->victim_line_pq, wpp->curline);
		lm->victim_line_cnt++;
	}
	/* current line is used up, pick another empty line */
	check_addr(wpp->blk, spp->blks_per_pl);
	wpp->curline = get_next_free_line(demand_shard);

    if(io_type == MAP_IO || io_type == GC_MAP_IO) {
        wpp->curline->map = true;
    } else {
        wpp->curline->map = false;
    }

    if(!wpp->curline) {
        return false;
    }

	NVMEV_DEBUG("wpp: got new clean line %d\n", wpp->curline->id);

	wpp->blk = wpp->curline->id;
	check_addr(wpp->blk, spp->blks_per_pl);

	/* make sure we are starting from page 0 in the super block */
	NVMEV_ASSERT(wpp->pg == 0);
	NVMEV_ASSERT(wpp->lun == 0);
	NVMEV_ASSERT(wpp->ch == 0);
	/* TODO: assume # of pl_per_lun is 1, fix later */
	NVMEV_ASSERT(wpp->pl == 0);
out:
	NVMEV_DEBUG("advanced wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d (curline %d)\n",
			wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg, wpp->curline->id);

    return true;
}

struct ppa get_new_page(struct demand_shard *shard, uint32_t io_type)
{
	struct ppa ppa;
	struct write_pointer *wp = __get_wp(shard, io_type);

	ppa.ppa = 0;
	ppa.g.ch = wp->ch;
	ppa.g.lun = wp->lun;
	ppa.g.pg = wp->pg;
	ppa.g.blk = wp->blk;
	ppa.g.pl = wp->pl;

	NVMEV_ASSERT(ppa.g.pl == 0);
	return ppa;
}

static void init_maptbl(struct demand_shard *demand_shard)
{
    return;
}

static void remove_maptbl(struct demand_shard *demand_shard)
{
    return;
}

static void init_rmap(struct demand_shard *demand_shard)
{
    return;
}

static void remove_rmap(struct demand_shard *demand_shard)
{
    return;
}

extern struct algorithm __demand;
extern struct lower_info virt_info;

#ifndef GC_STANDARD
char **inv_mapping_bufs;
uint64_t *inv_mapping_offs;
uint8_t *pg_inv_cnt;
#endif

void demand_init(struct demand_shard *shard, uint64_t size, 
                 struct ssd* ssd) 
{
    struct ssdparams *spp = &ssd->sp;

    uint64_t tt_grains = spp->tt_pgs * GRAIN_PER_PAGE; 

    spp->tt_map_pgs = tt_grains / EPP;
    spp->tt_data_pgs = spp->tt_pgs - spp->tt_map_pgs;

#ifndef GC_STANDARD
    pg_inv_cnt = (uint8_t*) vmalloc_node(spp->tt_pgs * sizeof(uint8_t),
            numa_node_id());
    NVMEV_ASSERT(pg_inv_cnt);
    memset(pg_inv_cnt, 0x0, spp->tt_pgs * sizeof(uint8_t));

    uint64_t inv_per_line = (spp->pgs_per_line * spp->pgsz) / GRAINED_UNIT;
    uint64_t inv_per_pg = INV_PAGE_SZ / (sizeof(lpa_t) + sizeof(ppa_t));
    uint64_t inv_ppl = spp->inv_ppl = inv_per_line / inv_per_pg;

    NVMEV_DEBUG("inv_per_line %u inv_per_pg %u inv_ppl %u\n", 
            inv_per_line, inv_per_pg, inv_ppl);

    inv_mapping_bufs = 
        (char**) kzalloc(spp->tt_lines * sizeof(char*), GFP_KERNEL);
    inv_mapping_offs = 
        (uint64_t*) kzalloc(spp->tt_lines * sizeof(uint64_t), GFP_KERNEL);

    for(int i = 0; i < spp->tt_lines; i++) {
        inv_mapping_bufs[i] =
            (char*) vmalloc(INV_PAGE_SZ);
        inv_mapping_offs[i] = 0;
        NVMEV_ASSERT(inv_mapping_bufs[i]);
    }

    NVMEV_INFO("tt_grains %llu tt_map %lu tt_data %lu tt_lines %lu "
            "invalid_per_line %llu inv_ppl %llu\n", 
            tt_grains, spp->tt_map_pgs, spp->tt_data_pgs, spp->tt_lines,
            inv_per_line, inv_ppl);
#endif

    NVMEV_INFO("tt_lines %lu\n", spp->tt_lines);

    /*
     * OOB stores LPA to grain information.
     */

	shard->oob = (uint64_t**)vmalloc_node(spp->tt_pgs * sizeof(uint64_t*),
            numa_node_id());
	shard->oob_mem = 
	(uint64_t*) vmalloc_node(spp->tt_pgs * GRAIN_PER_PAGE * sizeof(uint64_t),
            numa_node_id());

	NVMEV_ASSERT(shard->oob);
    NVMEV_ASSERT(shard->oob_mem);

    for(int i = 0; i < spp->tt_pgs; i++) {
        shard->oob[i] = shard->oob_mem + i * GRAIN_PER_PAGE;
        for(int j = 0; j < GRAIN_PER_PAGE; j++) {
            shard->oob[i][j] = 2;
        }
	}

#ifdef GC_STANDARD
    shard->grain_bitmap = (bool*) vmalloc(tt_grains * sizeof(bool));
    memset(shard->grain_bitmap, 0x0, tt_grains * sizeof(bool));
    NVMEV_INFO("Grain bitmap (%p) allocated for shard %llu %llu grains shard %p.\n", 
            shard->grain_bitmap, shard->id, tt_grains, shard);
#endif

    shard->env = (struct demand_env*) kzalloc(sizeof(*shard->env), GFP_KERNEL);
    shard->ftl = (struct demand_member*) 
                  kzalloc(sizeof(*shard->ftl), GFP_KERNEL);
    shard->cache = (struct demand_cache*)
                    kzalloc(sizeof(*shard->cache), GFP_KERNEL);
    demand_create(shard, &virt_info, NULL, &__demand, ssd, size);

    shard->dram  = ((uint64_t) nvmev_vdev->config.cache_dram_mb) << 20;

    cgo_create(shard, OLD_COARSE_GRAINED);
    cgo_cache[shard->id] = shard->cache;

    /*
     * Since fast mode is called from main.c with no knowledge of the
     * underlying FTL structures, not sure if there's a better way to do
     * this than setting a global here.
     */

    __fast_fill_shard = shard;
    shard->fastmode = false;

    shard->offset = 0;

	print_demand_stat(&d_stat);
}

void demand_free(struct demand_shard *shard) {
    struct ssdparams *spp = &shard->ssd->sp;

    cgo_destroy(shard->cache);

#ifndef GC_STANDARD
    vfree(pg_inv_cnt);

    for(int i = 0; i < spp->tt_lines; i++) {
        vfree(inv_mapping_bufs[i]);
    }

    kfree(inv_mapping_bufs);
    kfree(inv_mapping_offs);
#endif

    vfree(shard->oob_mem);
    vfree(shard->oob);
}

static void conv_init_ftl(uint64_t id, struct demand_shard *demand_shard, 
                          struct convparams *cpp, struct ssd *ssd)
{
	/*copy convparams*/
	demand_shard->cp = *cpp;
	demand_shard->ssd = ssd;
    demand_shard->id = id;

	/* initialize maptbl */
	init_maptbl(demand_shard); // mapping table

	/* initialize rmap */
	init_rmap(demand_shard); // reverse mapping table (?)

	/* initialize all the lines */
	init_lines(demand_shard);

	/* initialize write pointer, this is how we allocate new pages for writes */
	prepare_write_pointer(demand_shard, USER_IO);
    prepare_write_pointer(demand_shard, MAP_IO);
    prepare_write_pointer(demand_shard, GC_MAP_IO);
	prepare_write_pointer(demand_shard, GC_IO);

	init_write_flow_control(demand_shard);

    demand_init(demand_shard, ssd->sp.tt_pgs * ssd->sp.pgsz, ssd);

    /* for storing invalid mappings during GC */
    alloc_gc_mem(demand_shard);

	NVMEV_INFO("Init FTL instance with %d channels (%ld pages)\n", demand_shard->ssd->sp.nchs,
		        demand_shard->ssd->sp.tt_pgs);

	return;
}

static void conv_remove_ftl(struct demand_shard *demand_shard)
{
	//remove_lines(demand_shard);
	remove_rmap(demand_shard);
	remove_maptbl(demand_shard);
}

static void conv_init_params(struct convparams *cpp)
{
	cpp->op_area_pcent = OP_AREA_PERCENT;
#ifdef GC_STANDARD
	cpp->gc_thres_lines = 8; /* (host write, gc, map, map gc)*/
    cpp->gc_thres_lines_high = 8; /* (host write, gc, map, map gc)*/
#else
	cpp->gc_thres_lines = 8; /* (host write, gc, map, map gc)*/
    cpp->gc_thres_lines_high = 8; /* (host write, gc, map, map gc)*/
#endif
	cpp->enable_gc_delay = 1;
	cpp->pba_pcent = (int)((1 + cpp->op_area_pcent) * 100);
}

#ifndef GC_STANDARD
static bool __store(struct nvmev_ns *ns, struct nvmev_request *req, 
                    struct nvmev_result *ret, bool internal,
                    bool append);
static void __reclaim_completed_reqs(void);

uint32_t lpas[EPP];
uint32_t ppas[EPP];

#define REAP 4096
void **bufs = NULL;

struct fast_fill_args {
    struct nvmev_ns *ns;
    uint64_t size;
    uint32_t vlen;
    uint32_t pairs;
};

int fast_fill_t(void *data) {
    struct fast_fill_args *args;
    struct demand_shard *shard;
    struct demand_cache* cache;
    struct cache_member* cmbr;
    struct nvmev_ns *ns;
    uint64_t size;
    uint32_t vlen;
    uint32_t pairs;

    args = (struct fast_fill_args*) data;
    shard = __fast_fill_shard;
    cache = shard->cache;
    cmbr = &cache->member;
    ns = args->ns;
    size = args->size;
    vlen = args->vlen;
    pairs = args->pairs;

    shard->fastmode = true;

    vlen += sizeof(uint32_t);

    memset(nvmev_vdev->storage_mapped, 0x0, 
           nvmev_vdev->config.storage_size);

    uint64_t **oob = shard->oob;
    uint8_t klen = 8;
    uint32_t g_len = vlen / GRAINED_UNIT;

    if(vlen % GRAINED_UNIT) {
        g_len++;
    }

    if(GRAIN_PER_PAGE % g_len) {
        NVMEV_INFO("Fastmode failed!\n");
        return 0; 
    }

    NVMEV_INFO("Starting fastmode vlen %u pairs %u.\n", vlen, pairs);

    bufs = kzalloc(sizeof(char*) * REAP, GFP_KERNEL);
    for(int i = 0; i < REAP; i++) {
        bufs[i] = kzalloc(vlen, GFP_KERNEL);
    } 

    ktime_t tstart, tend; 
    tstart = ktime_get();

    for(uint64_t i = 1; i < pairs; i++) {
        struct nvme_kv_command cmd;
        memset(&cmd, 0, sizeof (struct nvme_kv_command));
        cmd.common.opcode = nvme_cmd_kv_store;

        NVMEV_ASSERT(klen == sizeof(i));
        memcpy(bufs[i % REAP], &klen, sizeof(klen));
        memcpy(bufs[i % REAP] + sizeof(klen), &i, sizeof(i));
        memcpy(cmd.kv_store.key, &i, sizeof(uint64_t));
        cmd.kv_store.key_len = klen - 1;

        cmd.kv_store.dptr.prp1 = (__u64) bufs[i % REAP];
        cmd.kv_store.value_len = vlen >> 2;
		cmd.kv_store.invalid_byte = 0;

        struct nvmev_request req = {
            .cmd = (struct nvme_command*) &cmd,
            .sq_id = 0,
            .nsecs_start = 0,
        };
        struct nvmev_result ret = {
            .nsecs_target = 0,
            .status = NVME_SC_SUCCESS,
            .cb = NULL,
            .args = NULL,
        };

        //NVMEV_INFO("Sending fastmode copy key %llu.\n", *(uint64_t*) cmd.kv_store.key);
        __store(ns, &req, &ret, false, false);

        if(i % REAP == 0) {
            __reclaim_completed_reqs();
        }

        if(i > 0 && ((i & 1048575) == 0)) {
            tend = ktime_get();
            uint64_t elapsed = ktime_to_ns(ktime_sub(tend, tstart)) / 1000000000;

            if(elapsed > 0) {
                uint64_t ops_s = i / elapsed;
                NVMEV_INFO("%llu fastmode writes done. %llu elapsed. %llu ops/s\n", 
                        i, elapsed, ops_s);
            }

            cond_resched();
        }
    }

    NVMEV_INFO("Before map.\n");

    uint64_t collision = 0;
    //size = size - (6144LU << 20);

    bool first_cmt = false;
    uint32_t first_cmt_ppa = 0;

    for(uint64_t i = 0; i < (size / GRAINED_UNIT); i++) {
        if((i * GRAINED_UNIT) + 1 > size) {
            break;
        }

        uint64_t at = *(uint64_t*) (nvmev_vdev->ns[0].mapped + ((i * GRAINED_UNIT) + 1));

        if(at == 0 || at == UINT_MAX || at > pairs) {
            continue;
        }

        //NVMEV_INFO("Got key %llu at %llu\n", at, ((i * GRAINED_UNIT)));
        uint64_t hash = CityHash64((void*) &at, sizeof(at));

        KEYT key;
        key.key = (char*) &at;
        key.len = sizeof(at);

        struct hash_params h; 
        h.hash = hash;
        h.cnt = 0;
        h.find = HASH_KEY_INITIAL;
        h.lpa = 0;

again:
        lpa_t lpa = get_lpa(cache, key, &h);
        struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

        if(cmt->t_ppa == UINT_MAX) {
skip:
            struct ppa p = get_new_page(shard, MAP_IO);
            ppa_t ppa = ppa2pgidx(shard, &p);

            advance_write_pointer(shard, MAP_IO);
            mark_page_valid(shard, &p);
            mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

            if(ppa == 0) {
                mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
                goto skip;
            }

            cmt->t_ppa = ppa;

            if(!first_cmt) {
                first_cmt_ppa = ppa;
                first_cmt = true;
            }

            oob[ppa][0] = cmt->idx * EPP;
            cmt->pt = (struct pt_struct*) (nvmev_vdev->ns[0].mapped + 
                                          ((uint64_t) ppa) * PAGESIZE);
            for(int i = 0; i < EPP; i++) {
                cmt->pt[i].lpa = UINT_MAX;
                cmt->pt[i].ppa = UINT_MAX;
            }
        }

        cmt->state = DIRTY;

        //NVMEV_ASSERT(cmt->pt);
        if(!cmt->pt) {
            continue;
        }

        if(cmt->pt[OFFSET(lpa)].lpa == lpa) {
            h.cnt++;
            shard->ftl->max_try = (h.cnt > shard->ftl->max_try) ? h.cnt : 
                                   shard->ftl->max_try;
            collision++;
            goto again;
        }

        cmt->pt[OFFSET(lpa)].lpa = lpa;
        cmt->pt[OFFSET(lpa)].ppa = i;
        cmt->cached_cnt++;

        if (((cmt->cached_cnt * ENTRY_SIZE) & (GRAINED_UNIT - 1)) == 0) {
            cmt->len_on_disk++;
        }

        if ((i & 1048575) == 0) {
            NVMEV_INFO("Finished %llu out of %llu grains\n", i, size / GRAINED_UNIT);
            cond_resched();
        }
    }


    NVMEV_INFO("Starting CMT compaction.\n");

    uint32_t cnt = 0;
    for(uint64_t i = 0; i < cache->env.nr_valid_tpages; i++) {
        struct cmt_struct *cmt = cmbr->cmt[i];
        if(cmt->pt) {
            for(int j = 0; j < EPP; j++) {
                if(cmt->pt[j].lpa != UINT_MAX && cmt->pt[j].lpa != 0) {
                    lpas[cnt] = cmt->pt[j].lpa;
                    ppas[cnt] = cmt->pt[j].ppa;
                    cnt++;
                }
            }

            if(cnt > 0) {
                if(cnt != cmt->cached_cnt) {
                    NVMEV_INFO("cnt was %u cached cnt %u\n", cnt, cmt->cached_cnt);
                }

                NVMEV_ASSERT(cnt == cmt->cached_cnt);
                for(int j = 0; j < cnt; j++) {
                    cmt->pt[j].lpa = lpas[j];
                    cmt->pt[j].ppa = ppas[j];
                    //NVMEV_INFO("Moving LPA %u PPA %u to %d CMT IDX %u\n", 
                    //            cmt->pt[j].lpa, cmt->pt[j].ppa, j, cmt->idx);
                }
            }

            cnt = 0;

            if(cmt->len_on_disk < GRAIN_PER_PAGE) {
                mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, cmt->len_on_disk), 
                                   GRAIN_PER_PAGE - cmt->len_on_disk);
            }

            for(int i = 1; i < cmt->len_on_disk; i++) {
                oob[cmt->t_ppa][i] = 0;
            }
            for(int i = cmt->len_on_disk; i < GRAIN_PER_PAGE; i++) {
                oob[cmt->t_ppa][i] = UINT_MAX;
            }
            cmt->pt = NULL;
        }

        if (i > 0 && (i & 1048575) == 0) {
            NVMEV_INFO("Finished %llu out of %d CMTS\n", 
                        i, cache->env.nr_valid_tpages);
            cond_resched();
        }
    }

    shard->fastmode = false;
    NVMEV_INFO("Fast fill done. %llu collisions\n", collision);

    kfree(args);

    return 0;
}

void fast_fill(struct nvmev_ns *ns, uint64_t size, uint32_t vlen, uint32_t pairs) {
    struct fast_fill_args *args;

    args = kzalloc(sizeof(*args), GFP_KERNEL);
    args->ns = ns;
    args->size = size;
    args->vlen = vlen;
    args->pairs = pairs;

    kthread_run(fast_fill_t, args, "fastfiller");
    return;
}
#else
void fast_fill(struct nvmev_ns *ns, uint64_t size, uint32_t vlen, uint32_t pairs) {
    NVMEV_ERROR("Fast fill for the original scheme hasn't been written yet!\n");
}
#endif

uint64_t dsize = 0;
uint8_t* wb;
uint64_t wb_offs;
void conv_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
                         uint32_t cpu_nr_dispatcher)
{
	struct ssdparams spp;
	struct convparams cpp;
	struct demand_shard *demand_shards;
	struct ssd *ssd;
	uint32_t i;
	const uint32_t nr_parts = SSD_PARTITIONS;

	ssd_init_params(&spp, size, nr_parts);
	conv_init_params(&cpp);

	demand_shards = kmalloc(sizeof(struct demand_shard) * nr_parts, GFP_KERNEL);
	for (i = 0; i < nr_parts; i++) {
		ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
		ssd_init(ssd, &spp, cpu_nr_dispatcher);
		conv_init_ftl(i, &demand_shards[i], &cpp, ssd);
	}
    
    nvmev_vdev->space_used = 0;

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		kfree(demand_shards[i].ssd->pcie->perf_model);
		kfree(demand_shards[i].ssd->pcie);
		kfree(demand_shards[i].ssd->write_buffer);

		demand_shards[i].ssd->pcie = demand_shards[0].ssd->pcie;
		demand_shards[i].ssd->write_buffer = demand_shards[0].ssd->write_buffer;
	}

	ns->id = id;
	ns->csi = NVME_CSI_NVM;
	ns->nr_parts = nr_parts;
	ns->ftls = (void *)demand_shards;
	ns->size = (uint64_t)((size * 100) / cpp.pba_pcent);
	ns->mapped = mapped_addr;
	/*register io command handler*/
    ns->proc_io_cmd = kv_proc_nvme_io_cmd;
	ns->identify_io_cmd = kv_identify_nvme_io_cmd;

    wb = ns->mapped + size;
    wb_offs = 0;

	NVMEV_INFO("FTL physical space: %lld, logical space: %lld (physical/logical * 100 = %d)\n",
		        size, ns->size, cpp.pba_pcent);

	return;
}

void conv_remove_namespace(struct nvmev_ns *ns)
{
	struct demand_shard *demand_shards = (struct demand_shard *)ns->ftls;
	const uint32_t nr_parts = SSD_PARTITIONS;
	uint32_t i;

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		/*
		 * These were freed from conv_init_namespace() already.
		 * Mark these NULL so that ssd_remove() skips it.
		 */
		demand_shards[i].ssd->pcie = NULL;
		demand_shards[i].ssd->write_buffer = NULL;
    }

    free_gc_mem(&demand_shards[0]);
    demand_free(&demand_shards[0]);

	for (i = 0; i < nr_parts; i++) {
		conv_remove_ftl(&demand_shards[i]);
		ssd_remove(demand_shards[i].ssd);
		kfree(demand_shards[i].ssd);
    }

	kfree(demand_shards);
	ns->ftls = NULL;
}

static inline bool valid_ppa(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	int ch = ppa->g.ch;
	int lun = ppa->g.lun;
	int pl = ppa->g.pl;
	int blk = ppa->g.blk;
	int pg = ppa->g.pg;
	//int sec = ppa->g.sec;

	if (ch < 0 || ch >= spp->nchs)
		return false;
	if (lun < 0 || lun >= spp->luns_per_ch)
		return false;
	if (pl < 0 || pl >= spp->pls_per_lun)
		return false;
	if (blk < 0 || blk >= spp->blks_per_pl)
		return false;
	if (pg < 0 || pg >= spp->pgs_per_blk)
		return false;

	return true;
}

static inline bool valid_lpn(struct demand_shard *demand_shard, uint64_t lpn)
{
	return (lpn < demand_shard->ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
	return !(ppa->ppa == UNMAPPED_PPA);
}

inline struct line *get_line(struct demand_shard *demand_shard, struct ppa *ppa)
{
	return &(demand_shard->lm.lines[ppa->g.blk]);
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
void mark_page_invalid(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	struct line_mgmt *lm = &demand_shard->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct line *line;

    //NVMEV_INFO("Marking PPA %u invalid\n", ppa2pgidx(demand_shard, ppa));

	/* update corresponding page status */
	pg = get_pg(demand_shard->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);
	pg->status = PG_INVALID;

#ifndef GC_STANDARD
    NVMEV_ASSERT(pg_inv_cnt[ppa2pgidx(demand_shard, ppa)] == GRAIN_PER_PAGE);
#endif
}

static struct ppa ppa_to_struct(const struct ssdparams *spp, uint64_t ppa_)
{
    struct ppa ppa;

    ppa.ppa = 0;
    ppa.g.ch = (ppa_ / spp->pgs_per_ch) % spp->pgs_per_ch;
    ppa.g.lun = (ppa_ % spp->pgs_per_ch) / spp->pgs_per_lun;
    ppa.g.pl = 0 ; //ppa_ % spp->tt_pls; // (ppa_ / spp->pgs_per_pl) % spp->pls_per_lun;
    ppa.g.blk = (ppa_ % spp->pgs_per_lun) / spp->pgs_per_blk;
    ppa.g.pg = ppa_ % spp->pgs_per_blk;

    //NVMEV_INFO("%s: For PPA %u we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

    if(ppa_ > spp->tt_pgs) {
        //NVMEV_INFO("Tried to convert PPA %llu\n", ppa_);
    }
	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

/*
 * Only to be called after mark_page_valid.
 */

void mark_grain_valid(struct demand_shard *shard, uint64_t grain, uint32_t len) {
	struct ssdparams *spp = &shard->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

    uint64_t page = G_IDX(grain);
    //NVMEV_INFO("Marking grain %llu valid length %u in PPA %llu\n", 
    //            grain, len, page);
    struct ppa ppa = ppa_to_struct(spp, page);

	/* update page status */
	pg = get_pg(shard->ssd, &ppa);

    if(pg->status != PG_VALID) {
        NVMEV_ERROR("Page %llu was %d\n", page, pg->status);
    }

	NVMEV_ASSERT(pg->status == PG_VALID);

	/* update corresponding block status */
	blk = get_blk(shard->ssd, &ppa);
	//NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);

    if(blk->vgc < 0 || blk->vgc > spp->pgs_per_blk * GRAIN_PER_PAGE) {
        NVMEV_INFO("Blk VGC %d (%d)\n", blk->vgc, spp->pgs_per_blk);
    } 
    
    NVMEV_ASSERT(blk->vgc >= 0 && blk->vgc <= spp->pgs_per_blk * GRAIN_PER_PAGE);
    blk->vgc += len;

	/* update corresponding line status */
	line = get_line(shard, &ppa);
	//NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    NVMEV_ASSERT(line->vgc >= 0 && line->vgc <= spp->pgs_per_line * GRAIN_PER_PAGE);
    line->vgc += len;

    //NVMEV_INFO("Marking grain %llu length %u in PPA %llu line %d valid shard %llu vgc %u\n", 
    //            grain, len, page, line->id, shard->id, line->vgc);

#ifdef GC_STANDARD
    /*
     * We leave the grains after the first grain as zero here,
     * so that during GC we can figure out the length of the KV pairs
     * by iterating over them.
     *
     * A: 1 0 0 0 B: 1 ... -> A is length 4.
     */

    //NVMEV_INFO("Marking grain %llu valid shard %llu (%p)\n", 
    //        grain, shard->id, shard->grain_bitmap);
    
    if(shard->grain_bitmap[grain] == 1) {
        NVMEV_INFO("!!!! grain %llu page %llu len %u\n", grain, G_IDX(grain), len);
    }

    NVMEV_ASSERT(shard->grain_bitmap[grain] != 1);
    shard->grain_bitmap[grain] = 1;
#endif
}

#ifdef GC_STANDARD
bool page_grains_invalid(struct demand_shard *shard, uint64_t ppa) {
    uint64_t page = ppa;
    uint64_t offset = page * GRAIN_PER_PAGE;

    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        if(shard->grain_bitmap[offset + i] == 1) {
            NVMEV_DEBUG("Grain %u PPA %u was valid\n",
                    offset + i, page);
            return false;
        }
    }

    NVMEV_DEBUG("All grains invalid PPA %u (%u)\n", page, offset);
    return true;
}
#endif

void mark_grain_invalid(struct demand_shard *shard, uint64_t grain, uint32_t len) {
	struct ssdparams *spp = &shard->ssd->sp;
	struct line_mgmt *lm = &shard->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct line *line;

    uint64_t page = G_IDX(grain);

    //NVMEV_INFO("Marking grain %llu length %u in PPA %llu invalid shard %llu\n", 
    //            grain, len, page, shard->id);

    struct ppa ppa = ppa_to_struct(spp, page);

	/* update corresponding page status */
	pg = get_pg(shard->ssd, &ppa);

    if(pg->status != PG_VALID) {
        NVMEV_INFO("PPA %u %lld! Grain %llu len %u\n", pg->status, page, grain, len);
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(0));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(1));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(2));
    }

	NVMEV_ASSERT(pg->status == PG_VALID);

	/* update corresponding block status */
	blk = get_blk(shard->ssd, &ppa);
	//NVMEV_ASSERT(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
	//NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    
    if(blk->igc < 0 || blk->igc >= spp->pgs_per_blk * GRAIN_PER_PAGE) {
        NVMEV_INFO("IGC PPA %u was %d\n", ppa2pgidx(shard, &ppa), blk->igc);
    }
    
    NVMEV_ASSERT(blk->igc < spp->pgs_per_blk * GRAIN_PER_PAGE);
    NVMEV_ASSERT(blk->vgc > 0 && blk->vgc <= spp->pgs_per_blk * GRAIN_PER_PAGE);
    blk->igc += len;

	/* update corresponding line status */
	line = get_line(shard, &ppa);

    //NVMEV_INFO("Marking grain %llu length %u in PPA %u line %d invalid shard %llu\n", 
    //             grain, len, ppa2pgidx(shard, &ppa), line->id, shard->id);

	//NVMEV_ASSERT(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    NVMEV_ASSERT(line->igc >= 0 && line->igc < spp->pgs_per_line * GRAIN_PER_PAGE);
	if (line->vgc == spp->pgs_per_line * GRAIN_PER_PAGE) {
		NVMEV_ASSERT(line->igc == 0);
        was_full_line = true;
	}
    NVMEV_ASSERT(line->igc < spp->pgs_per_line * GRAIN_PER_PAGE);
    line->igc += len;

	/* Adjust the position of the victime line in the pq under over-writes */
	if (line->pos) {
		/* Note that line->vgc will be updated by this call */
		pqueue_change_priority(lm->victim_line_pq, line->vgc - len, line);
	} else {
		line->vgc -= len;
	}

    if (was_full_line) {
        /* move line: "full" -> "victim" */
        list_del_init(&line->entry);
        lm->full_line_cnt--;
        NVMEV_DEBUG("Inserting line %d to PQ vgc %d\n", line->id, line->vgc);
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }

	//NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    if(line->vgc < 0 || line->vgc > spp->pgs_per_line * GRAIN_PER_PAGE) {
        NVMEV_INFO("Line %d's VGC was %u\n", line->id, line->vgc);
    }
    NVMEV_ASSERT(line->vgc >= 0 && line->vgc <= spp->pgs_per_line * GRAIN_PER_PAGE);

    //if(grain_bitmap[grain] == 0) {
    //    NVMEV_INFO("Caller is %pS\n", __builtin_return_address(0));
    //    NVMEV_INFO("Caller is %pS\n", __builtin_return_address(1));
    //    NVMEV_INFO("Caller is %pS\n", __builtin_return_address(2));
    //}

#ifdef GC_STANDARD
    NVMEV_ASSERT(shard->grain_bitmap[grain] != 0);
    shard->grain_bitmap[grain] = 0;

    //if(page_grains_invalid(shard, page)) {
    //    mark_page_invalid(shard, &ppa);
    //}
#else
    if(pg_inv_cnt[page] + len > GRAIN_PER_PAGE) {
        NVMEV_INFO("inv_cnt was %u PPA %llu (tried to add %u)\n", 
                    pg_inv_cnt[page], page, len * GRAINED_UNIT);
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(0));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(1));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(2));
        BUG_ON(true);
    }

    NVMEV_ASSERT(pg_inv_cnt[page] + len <= GRAIN_PER_PAGE);
    pg_inv_cnt[page] += len;

    NVMEV_DEBUG("inv_cnt for %u is %u\n", page, pg_inv_cnt[page]);

    if(pg_inv_cnt[page] == GRAIN_PER_PAGE) {
        mark_page_invalid(shard, &ppa);
    }
#endif
}

void mark_page_valid(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

    //NVMEV_INFO("Marking PPA %u valid\n", ppa2pgidx(demand_shard, ppa));

	/* update page status */
	pg = get_pg(demand_shard->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_FREE);
	pg->status = PG_VALID;

	///* update corresponding block status */
	//blk = get_blk(demand_shard->ssd, ppa);
	//NVMEV_ASSERT(blk->vpc >= 0 && blk->vpc < spp->pgs_per_blk);
	//blk->vpc++;

	///* update corresponding line status */
	//line = get_line(demand_shard, ppa);
	//NVMEV_ASSERT(line->vpc >= 0 && line->vpc < spp->pgs_per_line);
	//line->vpc++;
}

uint64_t __maybe_advance(struct demand_shard *shard, struct ppa *ppa, int type,
                         uint64_t stime) {
    struct ssdparams *spp = &shard->ssd->sp;
    uint64_t nsecs = 0;

    if (last_pg_in_wordline(shard, ppa)) {
        struct nand_cmd swr = {
            .type = type,
            .cmd = NAND_WRITE,
            .interleave_pci_dma = false,
            .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
        };

        swr.stime = stime;
        swr.ppa = ppa;

        nsecs = ssd_advance_nand(shard->ssd, &swr);
        d_stat.inv_m_w += spp->pgsz * spp->pgs_per_oneshotpg;

        //schedule_internal_operation(req->sq_id, nsecs_completed, wbuf,
        //        spp->pgs_per_oneshotpg * spp->pgsz);
    }

    return nsecs;
}

struct inv_entry {
    uint64_t key; // This will be used as the key in the hash table
    struct hlist_node node; // Hash table uses hlist_node to chain items
};
DEFINE_HASHTABLE(inv_m_hash, 22); // 2^8 buckets = 256 buckets

#ifndef GC_STANDARD
static uint64_t __record_inv_mapping(struct demand_shard *shard, lpa_t lpa, 
                                     ppa_t ppa, uint64_t *credits) {
    struct ssdparams *spp = &shard->ssd->sp;
    struct gc_data *gcd = &shard->gcd;
    struct ppa p = ppa_to_struct(spp, G_IDX(ppa));
    struct line* l = get_line(shard, &p); 
    uint64_t line = (uint64_t) l->id;
    uint64_t nsecs_completed = 0;
    uint64_t **oob = shard->oob;

    NVMEV_DEBUG("Got an invalid LPA %u PPA %u mapping line %llu (%llu)\n", 
                lpa, ppa, line, inv_mapping_offs[line]);

    if((inv_mapping_offs[line] + sizeof(lpa) + sizeof(ppa)) > INV_PAGE_SZ) {
        /*
         * Anything bigger complicates implementation. Keep to pgsz for now.
         */

        NVMEV_ASSERT(INV_PAGE_SZ == spp->pgsz);

skip:
        struct ppa n_p = get_new_page(shard, MAP_IO);
        uint64_t pgidx = ppa2pgidx(shard, &n_p);

        NVMEV_ASSERT(pg_inv_cnt[pgidx] == 0);
        advance_write_pointer(shard, MAP_IO);
        mark_page_valid(shard, &n_p);
        mark_grain_valid(shard, PPA_TO_PGA(ppa2pgidx(shard, &n_p), 0), GRAIN_PER_PAGE);

        if(pgidx == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
            goto skip;
        }

        NVMEV_DEBUG("Flushing an invalid mapping page for line %llu off %llu to PPA %llu\n", 
                     line, inv_mapping_offs[line], pgidx);

        oob[pgidx][0] = UINT_MAX;
        oob[pgidx][1] = line;
        oob[pgidx][2] = pgidx;

        uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
        uint64_t off = shard_off + (pgidx * spp->pgsz);
        uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;

        memcpy(ptr, inv_mapping_bufs[line], spp->pgsz);
        nsecs_completed = __maybe_advance(shard, &n_p, MAP_IO, 0);

        nvmev_vdev->space_used += INV_PAGE_SZ;

        NVMEV_DEBUG("Added %u (%u %u) to XA.\n", (line << 32) | pgidx, line, pgidx);
        xa_store(&gcd->inv_mapping_xa, (line << 32) | pgidx, 
                  xa_mk_value(pgidx), GFP_KERNEL);
        
        memset(inv_mapping_bufs[line], 0x0, INV_PAGE_SZ);
        inv_mapping_offs[line] = 0;

        if(credits) {
            (*credits) += GRAIN_PER_PAGE;
        }
    }

    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &lpa, sizeof(lpa));
    inv_mapping_offs[line] += sizeof(lpa);
    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &ppa, sizeof(ppa));
    inv_mapping_offs[line] += sizeof(ppa);

    return nsecs_completed;
}
#endif

bool __invalid_mapping_ppa(struct demand_shard *demand_shard, uint64_t ppa, 
                           unsigned long key) {
    struct ssdparams *spp = &demand_shard->ssd->sp;
    struct gc_data *gcd = &demand_shard->gcd;

    NVMEV_DEBUG("Checking key %lu (%u)\n", key, ppa);
    void *xa_entry = xa_load(&gcd->inv_mapping_xa, key);
    if(xa_entry) {
        uint64_t xa_ppa = xa_to_value(xa_entry);
        NVMEV_DEBUG("Got xa_ppa %u which targets line %ld\n", 
                    xa_ppa, key >> 32);
        if(xa_ppa == ppa) {
            /*
             * This page was used to store in valid mappings.
             */
            return true;
        } else {
            BUG_ON(true);
        }
    } else {
        /*
         * This page wasn't used to store invalid mappings.
         */
        return false;
    }

    //bool sample = false;
    //uint64_t start, end;
    //uint32_t rand;
    //get_random_bytes(&rand, sizeof(rand));
    //if(rand % 100 > 90) {
    //    start = local_clock();
    //    sample = true;
    //}

    //for(int i = 0; i < spp->tt_lines; i++) {
    //    for(int j = 0; j < inv_mapping_cnts[i]; j++) {
    //        if(inv_mapping_ppas[i][j] == ppa) {
    //            NVMEV_DEBUG("Caught a mapping PPA %u line %d\n", ppa, i);

    //            if(sample) {
    //                end = local_clock();
    //                NVMEV_DEBUG("%s 1 took %u ns for ONE pair\n", __func__, end - start);
    //            }

    //            return true;
    //        } 
    //    }
    //}

    //if(sample) {
    //    end = local_clock();
    //    NVMEV_DEBUG("%s 2 took %u ns for ONE pair\n", __func__, end - start);
    //}

    return false;
}

void clear_oob_block(struct demand_shard *shard, uint64_t start_pgidx) {
    struct ssdparams *spp = &shard->ssd->sp;
    uint64_t *start_address = shard->oob[start_pgidx];
    size_t total_size = sizeof(uint64_t) * GRAIN_PER_PAGE * spp->pgs_per_blk;
    memset(start_address, 2, total_size);
}

void clear_oob(struct demand_shard *shard, uint64_t pgidx) {
	NVMEV_DEBUG("Clearing OOB for %u\n", pgidx);
	for(int i = 0; i < GRAIN_PER_PAGE; i++) {
		shard->oob[pgidx][i] = 2;
	}
}

static void mark_block_free(struct demand_shard *shard, struct ppa *ppa)
{
	struct ssdparams *spp = &shard->ssd->sp;
	struct nand_block *blk = get_blk(shard->ssd, ppa);
	struct nand_page *pg = NULL;
    struct ppa ppa_copy;
    uint64_t page;
	int i;

    ppa_copy = *ppa;
    ppa_copy.g.pg = 0;
    page = ppa2pgidx(shard, &ppa_copy);
    //clear_oob_block(shard, ppa2pgidx(shard, &ppa_copy));

	for (i = 0; i < spp->pgs_per_blk; i++) {
		/* reset page status */
		pg = &blk->pg[i];
		NVMEV_ASSERT(pg->nsecs == spp->secs_per_pg);
        //NVMEV_INFO("Marking PPA %u free\n", ppa2pgidx(shard, &ppa_copy) + i);
		pg->status = PG_FREE;

#ifndef GC_STANDARD
        if(pg_inv_cnt[ppa2pgidx(shard, &ppa_copy) + i] == 0) {
            NVMEV_INFO("FAIL PPA %u\n", ppa2pgidx(shard, &ppa_copy) + i);
        }
        NVMEV_ASSERT(pg_inv_cnt[ppa2pgidx(shard, &ppa_copy) + i] > 0);
        pg_inv_cnt[ppa2pgidx(shard, &ppa_copy) + i] = 0;
#endif
		clear_oob(shard, ppa2pgidx(shard, &ppa_copy) + i);
	}

	/* reset block status */
	NVMEV_ASSERT(blk->npgs == spp->pgs_per_blk);
	blk->ipc = 0;
	blk->vpc = 0;
    blk->igc = 0;
    blk->vgc = 0;
	blk->erase_cnt++;
}

static struct line *select_victim_line(struct demand_shard *demand_shard, bool force)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	struct line_mgmt *lm = &demand_shard->lm;
	struct line *victim_line = NULL;

	victim_line = pqueue_peek(lm->victim_line_pq);
	if (!victim_line) {
        NVMEV_ERROR("Had no victim line for GC!\n");
		return NULL;
	}

	if (!force && (victim_line->vpc > (spp->pgs_per_line / 8))) {
        BUG_ON(true);
		return NULL;
	}

	pqueue_pop(lm->victim_line_pq);
	victim_line->pos = 0;
	lm->victim_line_cnt--;

    NVMEV_DEBUG("Took victim line %d off the pq\n", victim_line->id);
	NVMEV_DEBUG("ipc=%d(%d),igc=%d(%d),victim=%d,full=%d,free=%d\n", 
		    victim_line->ipc, victim_line->vpc, victim_line->igc, victim_line->vgc,
            demand_shard->lm.victim_line_cnt, demand_shard->lm.full_line_cnt, 
            demand_shard->lm.free_line_cnt);

	/* victim_line is a danggling node now */
	return victim_line;
}

bool oob_empty(struct demand_shard *shard, uint64_t pgidx) {
    return true;
    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        if(shard->oob[pgidx][i] == 1) {
            NVMEV_ERROR("Page %llu offset %d was %llu\n", 
                         pgidx, i, shard->oob[pgidx][i]);
            return false;
        }
    }
    return true;
}

uint64_t clean_first_half = 0;
uint64_t clean_second_half = 0;
uint64_t clean_third_half = 0;
uint64_t mapping_searches = 0;

uint64_t __get_inv_mappings(struct demand_shard *shard, uint64_t line) {
#ifdef GC_STANDARD
    return 0;
#else
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &shard->ssd->sp;
    struct gc_data *gcd = &shard->gcd;
    struct ppa p;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off;
    uint8_t *ptr;

    xa_init(&gcd->gc_xa);

    unsigned long index;
    unsigned long start = index = (line << 32);
    unsigned long end = (line + 1) << 32;
    void* xa_entry = NULL;

    struct nand_cmd swr = {
        .type = USER_IO,
        .cmd = NAND_READ,
        .interleave_pci_dma = false,
        .xfer_size = spp->pgsz,
        .stime = 0, /* TODO fix */
    };

    NVMEV_DEBUG("Starting an XA scan %lu %lu\n", start, end);
    xa_for_each_range(&gcd->inv_mapping_xa, index, xa_entry, start, end) {
        uint64_t m_ppa = xa_to_value(xa_entry);

        off = shard_off + (m_ppa * spp->pgsz);
        ptr = nvmev_vdev->ns[0].mapped + off;

        NVMEV_DEBUG("Reading mapping page from PPA %llu (idx %lu)\n", m_ppa, index);

        p = ppa_to_struct(spp, m_ppa);
        swr.ppa = &p;
        nsecs_completed = ssd_advance_nand(ssd, &swr);
        nsecs_latest = max(nsecs_latest, nsecs_completed);

        d_stat.inv_m_r += spp->pgsz;

        BUG_ON(m_ppa % spp->pgs_per_blk + (INV_PAGE_SZ / spp->pgsz) > spp->pgs_per_blk);

        for(int j = 0; j < INV_PAGE_SZ / (uint32_t) INV_ENTRY_SZ; j++) {
            lpa_t lpa = *(lpa_t*) (ptr + (j * INV_ENTRY_SZ));
            ppa_t ppa = *(lpa_t*) (ptr + (j * INV_ENTRY_SZ) + 
                                         sizeof(lpa_t));

            if(lpa == UINT_MAX) {
                continue;
                //NVMEV_INFO("IDX was %d\n", j);
            }
            //BUG_ON(lpa == UINT_MAX);

            NVMEV_DEBUG("%s XA 1 Inserting inv LPA %u PPA %u "
                        "(%llu)\n", 
                        __func__, lpa, ppa, ((uint64_t) ppa << 32) | lpa);

            struct inv_entry *entry;
            entry = kmalloc(sizeof(*entry), GFP_KERNEL);
            entry->key = ((uint64_t) ppa << 32) | lpa;
            hash_add(inv_m_hash, &entry->node, entry->key);

            //xa_store(&gcd->gc_xa, ((uint64_t) ppa << 32) | lpa, 
            //          xa_mk_value(((uint64_t) ppa << 32) | lpa),  GFP_KERNEL);
        }

        NVMEV_DEBUG("Erasing %lu from XA.\n", index);
        xa_erase(&gcd->inv_mapping_xa, index);
        mark_grain_invalid(shard, PPA_TO_PGA(m_ppa, 0), GRAIN_PER_PAGE);
    }

    NVMEV_DEBUG("Copying %lld (%lld %lu) inv mapping pairs from mem.\n",
                inv_mapping_offs[line] / INV_ENTRY_SZ, 
                inv_mapping_offs[line], INV_ENTRY_SZ);

    for(int j = 0; j < inv_mapping_offs[line] / (uint32_t) INV_ENTRY_SZ; j++) {
        lpa_t lpa = *(lpa_t*) (inv_mapping_bufs[line] + (j * INV_ENTRY_SZ));
        ppa_t ppa = *(ppa_t*) (inv_mapping_bufs[line] + (j * INV_ENTRY_SZ) + 
                                     sizeof(lpa_t));

        BUG_ON(lpa == UINT_MAX);

        NVMEV_DEBUG("%s XA 2 Inserting inv LPA %u PPA %u "
                    "(%llu)\n", 
                    __func__, lpa, ppa, ((uint64_t) ppa << 32) | lpa);

        struct inv_entry *entry;
        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        entry->key = ((uint64_t) ppa << 32) | lpa;
        hash_add(inv_m_hash, &entry->node, entry->key);

        //xa_store(&gcd->gc_xa, ((uint64_t) ppa << 32) | lpa, 
        //         xa_mk_value(((uint64_t) ppa << 32) | lpa), GFP_KERNEL);
    }

    inv_mapping_offs[line] = 0;
    return nsecs_completed;
#endif
}

void __clear_inv_mapping(struct demand_shard *demand_shard, unsigned long key) {
#ifdef GC_STANDARD
    return;
#else
    struct gc_data *gcd = &demand_shard->gcd;

    NVMEV_DEBUG("Trying to erase %lu\n", key);
    void* xa_entry = xa_erase(&gcd->inv_mapping_xa, key);
    NVMEV_ASSERT(xa_entry != NULL);

    return;
#endif
}

bool __valid_mapping(struct demand_shard *demand_shard, uint64_t lpa, uint64_t ppa) {
#ifdef GC_STANDARD
    return true;
#else
    struct ssdparams *spp = &demand_shard->ssd->sp;
    struct gc_data *gcd = &demand_shard->gcd;

    uint64_t start = 0, end = 0;
    start = ktime_get();

    uint64_t key = (ppa << 32) | lpa;
    bool valid = true;
    struct inv_entry *item;
    // Iterate over the hash table to find the item
    hash_for_each_possible(inv_m_hash, item, node, key) {
        if (item->key == key) {
            valid = false;
            break;
        }
    } 

    end = ktime_get();
    mapping_searches += ktime_to_us(end) - ktime_to_us(start);
    return valid;
#endif
}

void __update_mapping_ppa(struct demand_shard *demand_shard, uint64_t new_ppa, 
                          uint64_t line) {
#ifdef GC_STANDARD
    return;
#else
    struct gc_data *gcd = &demand_shard->gcd;
    unsigned long new_key = (line << 32) | new_ppa;
    NVMEV_DEBUG("%s adding %lu to XA.\n", __func__, new_key);
    xa_store(&gcd->inv_mapping_xa, new_key, xa_mk_value(new_ppa), GFP_KERNEL);
    return;
#endif
}

void __clear_gc_data(struct demand_shard* demand_shard) {
    struct ssdparams *spp = &demand_shard->ssd->sp;
    struct gc_data *gcd = &demand_shard->gcd;

	struct inv_entry *item;
	struct hlist_node *tmp;
	int bkt;

	// Iterate over each bucket
	hash_for_each_safe(inv_m_hash, bkt, tmp, item, node) {
		// Remove the item from the hash table
		hash_del(&item->node);
		// Free the memory allocated for the item
		kfree(item);
	}

    xa_destroy(&gcd->gc_xa);
}

uint64_t user_pgs_this_gc = 0;
uint64_t gc_pgs_this_gc = 0;
uint64_t map_pgs_this_gc = 0;
uint64_t map_gc_pgs_this_gc = 0;

static void __reclaim_completed_reqs(void)
{
	unsigned int turn;

	for (turn = 0; turn < nvmev_vdev->config.nr_io_workers; turn++) {
		struct nvmev_io_worker *worker;
		struct nvmev_io_work *w;

		unsigned int first_entry = -1;
		unsigned int last_entry = -1;
		unsigned int curr;
		int nr_reclaimed = 0;

		worker = &nvmev_vdev->io_workers[turn];

		first_entry = worker->io_seq;
		curr = first_entry;

		while (curr != -1) {
			w = &worker->work_queue[curr];
			if (w->is_completed == true && w->is_copied == true &&
			    w->nsecs_target <= worker->latest_nsecs) {
				last_entry = curr;
				curr = w->next;
				nr_reclaimed++;
			} else {
				break;
			}
		}

		if (last_entry != -1) {
			w = &worker->work_queue[last_entry];
			worker->io_seq = w->next;
			if (w->next != -1) {
				worker->work_queue[w->next].prev = -1;
			}
			w->next = -1;

			w = &worker->work_queue[first_entry];
			w->prev = worker->free_seq_end;

			w = &worker->work_queue[worker->free_seq_end];
			w->next = first_entry;

			worker->free_seq_end = last_entry;
			NVMEV_DEBUG_VERBOSE("%s: %u -- %u, %d\n", __func__,
					first_entry, last_entry, nr_reclaimed);
		}
	}
}

static inline uint32_t __lpa_from_oob(uint64_t oob) {
    return oob & 0xFFFFFFFF;
}

static inline uint32_t __glen_from_oob(uint64_t oob) {
    return oob >> 32;
}

uint64_t __maybe_write(struct demand_shard *shard, struct ppa *ppa, bool map) {
    struct ssdparams *spp;
    struct convparams *cpp;
    uint64_t nsecs_completed = 0;

    spp = &shard->ssd->sp;
    cpp = &shard->cp;

    if (cpp->enable_gc_delay) {
        struct nand_cmd gcw = {
            .type = GC_IO,
            .cmd = NAND_NOP,
            .stime = 0,
            .interleave_pci_dma = false,
            .ppa = ppa,
        };

        if (last_pg_in_wordline(shard, ppa)) {
            gcw.cmd = NAND_WRITE;
            gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;

            if(map) {
                d_stat.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
            } else {
                d_stat.data_w_dgc += spp->pgsz * spp->pgs_per_oneshotpg;
            }
        }

        nsecs_completed = ssd_advance_nand(shard->ssd, &gcw);
        //if (last_pg_in_wordline(shard, &new_ppa)) {
        //    schedule_internal_operation(UINT_MAX, nsecs_completed, NULL,
        //                                spp->pgs_per_oneshotpg * spp->pgsz);
        //}
    }

    return nsecs_completed;
}

void __new_gc_ppa(struct demand_shard *shard, bool map) {
    struct gc_data *gcd;
    uint64_t pgidx;

    gcd = &shard->gcd;
again:
    gcd->gc_ppa = get_new_page(shard, map ? GC_MAP_IO : GC_IO);
    gcd->pgidx = pgidx = ppa2pgidx(shard, &gcd->gc_ppa);
    gcd->offset = 0;

    advance_write_pointer(shard, map ? GC_MAP_IO : GC_IO);
    mark_page_valid(shard, &gcd->gc_ppa);

    if(pgidx == 0) {
        mark_grain_valid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
        mark_grain_invalid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
        goto again;
    }
}

void __copy_inv_map(struct demand_shard *shard, uint64_t old_grain, 
                    uint32_t target_line) {
    struct ssdparams *spp;
    struct demand_cache *cache;
    struct cmt_struct *cmt;
    struct gc_data *gcd;
    struct ppa ppa;
    uint32_t offset;
    uint64_t pgidx;
    uint64_t **oob;
    uint32_t len;
    uint32_t grain;

    spp = &shard->ssd->sp;
    cache = shard->cache;
    gcd = &shard->gcd;
    oob = shard->oob;
    len = GRAIN_PER_PAGE;

    if(gcd->offset >= GRAIN_PER_PAGE) {
        __new_gc_ppa(shard, true);
    }

again:
    ppa = gcd->gc_ppa;
    offset = gcd->offset;
    pgidx = gcd->pgidx;
    grain = PPA_TO_PGA(pgidx, 0);

    if(len > GRAIN_PER_PAGE - offset) {
        /*
         * There's not enough space left in this page.
         * Mark the rest of the page invalid.
         *
         * We would do well here to have an implementation
         * that packs values into pages like in the original work.
         */
        NVMEV_ASSERT(offset > 0);
        mark_grain_valid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);
        mark_grain_invalid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);

        uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                GRAINED_UNIT);

        __new_gc_ppa(shard, true);

        goto again;
    }

    mark_grain_valid(shard, grain, len);

    uint64_t copy_to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
    uint64_t copy_from = (G_IDX(old_grain) * spp->pgsz) + 
                         (G_OFFSET(old_grain) * GRAINED_UNIT);
    /*
     * This is an invalid mapping page, which are always the
     * size of a full page. We don't need to set any of the OOB
     * except the first.
     */
#ifdef GC_STANDARD
    NVMEV_ASSERT(false);
#endif

    NVMEV_ASSERT(offset == 0);

    oob[pgidx][0] = UINT_MAX;
    oob[pgidx][1] = target_line;
    oob[pgidx][2] = pgidx;

    memcpy(nvmev_vdev->ns[0].mapped + copy_to,
           nvmev_vdev->ns[0].mapped + copy_from, len * GRAINED_UNIT);

    __update_mapping_ppa(shard, pgidx, target_line);
    gcd->offset += GRAIN_PER_PAGE;
}

void __copy_map(struct demand_shard *shard, lpa_t idx, uint32_t len) {
    struct ssdparams *spp;
    struct demand_cache *cache;
    struct cmt_struct *cmt;
    struct gc_data *gcd;
    struct ppa ppa;
    uint32_t offset;
    uint64_t pgidx;
    uint64_t **oob;
    uint64_t grain;

    spp = &shard->ssd->sp;
    cache = shard->cache;
    gcd = &shard->gcd;
    oob = shard->oob;

    if(gcd->offset >= GRAIN_PER_PAGE) {
        __new_gc_ppa(shard, true);
    }

again:
    ppa = gcd->gc_ppa;
    offset = gcd->offset;
    pgidx = gcd->pgidx;
    grain = PPA_TO_PGA(pgidx, offset);

    if(len > GRAIN_PER_PAGE - offset) {
        /*
         * There's not enough space left in this page.
         * Mark the rest of the page invalid.
         *
         * We would do well here to have an implementation
         * that packs values into pages like in the original work.
         */
        NVMEV_ASSERT(offset > 0);
        mark_grain_valid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);
        mark_grain_invalid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);

        uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                GRAINED_UNIT);

        __new_gc_ppa(shard, true);

        goto again;
    }

    mark_grain_valid(shard, grain, len);

    cmt = cache->member.cmt[idx];
    cmt->t_ppa = pgidx;
    cmt->g_off = gcd->offset;

    //NVMEV_INFO("Moved IDX %u to PPA %llu\n", idx, pgidx);

    oob[pgidx][offset] = ((uint64_t) len << 32) | IDX2LPA(idx);
    gcd->offset += len;
}

void __copy_valid_pair(struct demand_shard *shard, lpa_t lpa, uint32_t len,
                       struct lpa_len_ppa *out, uint32_t out_idx) {
    struct ssdparams *spp;
    struct gc_data *gcd;
    struct ppa ppa;
    uint32_t offset;
    uint64_t pgidx;
    uint64_t **oob;
    uint64_t grain;

    spp = &shard->ssd->sp;
    gcd = &shard->gcd;
    oob = shard->oob;

    if(gcd->offset >= GRAIN_PER_PAGE) {
        __new_gc_ppa(shard, false);
    }

again:
    ppa = gcd->gc_ppa;
    offset = gcd->offset;
    pgidx = ppa2pgidx(shard, &ppa);
    grain = PPA_TO_PGA(pgidx, offset);

    if(len > GRAIN_PER_PAGE - offset) {
        /*
         * There's not enough space left in this page.
         * Mark the rest of the page invalid.
         *
         * We would do well here to have an implementation
         * that packs values into pages like in the original work.
         */
        NVMEV_ASSERT(offset > 0);
        mark_grain_valid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);
        mark_grain_invalid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);

        uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                GRAINED_UNIT);

        __new_gc_ppa(shard, false);

        goto again;
    }

    mark_grain_valid(shard, grain, len);

    oob[pgidx][offset] = ((uint64_t) len << 32) | lpa;
    gcd->offset += len;

    out[out_idx] = (struct lpa_len_ppa) {lpa, grain};
}

uint64_t clearing = 0, clear_count = 0, copying = 0;
/* here ppa identifies the block we want to clean */
void clean_one_flashpg(struct demand_shard *shard, struct ppa *ppa)
{
    struct ssdparams *spp = &shard->ssd->sp;
    struct convparams *cpp = &shard->cp;
    struct demand_cache *cache = shard->cache;
    struct gc_data *gcd = &shard->gcd;
	struct nand_page *pg_iter = NULL;
	int page_cnt = 0, cnt = 0, i = 0, len = 0;
	uint64_t reads_done = 0, pgidx = 0;
	struct ppa ppa_copy = *ppa;
    struct line* l = get_line(shard, ppa); 
    uint64_t **oob = shard->oob;

    struct lpa_len_ppa *lpa_lens;
    uint64_t tt_rewrite = 0;
    bool mapping_line = gcd->map;
    //uint64_t idx;
    uint64_t lpa_len_idx = 0;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint32_t gcs = 0;
    uint64_t nsecs_completed = 0;

    uint64_t start = 0, end = 0, clear_start = 0, copy_start = 0;

    lpa_lens = gcd->lpa_lens;
    NVMEV_ASSERT(lpa_lens);

    start = ktime_get();
	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(shard->ssd, &ppa_copy);
        pgidx = ppa2pgidx(shard, &ppa_copy);

#ifdef GC_STANDARD
        if(page_grains_invalid(shard, pgidx)) {
            mark_page_invalid(shard, &ppa_copy);
        }
#endif

        /* there shouldn't be any free page in victim blocks */
        NVMEV_ASSERT(pg_iter->status != PG_FREE);
        nvmev_vdev->space_used -= spp->pgsz;
		if (pg_iter->status == PG_VALID) {
            page_cnt++;
        } else if(pg_iter->status == PG_INVALID) {
#ifndef GC_STANDARD
            NVMEV_ASSERT(pg_inv_cnt[pgidx] == GRAIN_PER_PAGE);
#endif
            ppa_copy.g.pg++;
            continue;
#ifdef GC_STANDARD
        }
#else
        } else if(pg_inv_cnt[pgidx] == GRAIN_PER_PAGE) {
            NVMEV_ASSERT(pg_iter->status == PG_INVALID);
            ppa_copy.g.pg++;
            continue;
        }
#endif

        NVMEV_DEBUG("Cleaning PPA %llu\n", pgidx);
        for(int i = 0; i < GRAIN_PER_PAGE; i++) {
            uint64_t grain = PPA_TO_PGA(pgidx, i);
#ifdef GC_STANDARD
            bool valid_g = shard->grain_bitmap[grain];
#else
            bool valid_g = true;
#endif
            if(i != 0 && oob[pgidx][i] == UINT_MAX) {
                /*
                 * This section of the OOB was marked as invalid,
                 * because we didn't have enough space in the page
                 * to write the next value.
                 */
                i += GRAIN_PER_PAGE;
                continue;
            }

            if(shard->offset / GRAINED_UNIT == grain) {
                /*
                 * Edge case where in __store we got the last
                 * page of a line for the offset, and GC thinks the
                 * line is available to garbage collect because all
                 * of the pages have been used.
                 */
                shard->offset = 0;
                NVMEV_INFO("Set offset to 0 grain %llu!!!\n",
                            grain);
            }

            NVMEV_DEBUG("Grain %llu (%d)\n", grain, i);
            if(i == 0 && mapping_line && oob[pgidx][i] == UINT_MAX) {
#ifdef GC_STANDARD
                /*
                 * Original scheme doesn't have invalid mapping pages!
                 */

                NVMEV_ASSERT(false);
#else
                /*
                 * This is a page that contains invalid LPA -> PPA mappings.
                 * We need to copy the whole page to somewhere else.
                 */

                uint32_t target_line = oob[pgidx][1];
                uint32_t page = oob[pgidx][2];
                unsigned long key = 
                ((unsigned long) target_line << 32) | page;

                NVMEV_DEBUG("Got invalid mapping PPA %llu key %lu target line %lu in GC\n", 
                             pgidx, key, key >> 32);
                NVMEV_ASSERT(i == 0);
                NVMEV_ASSERT(mapping_line);

                __clear_inv_mapping(shard, key);

                copy_start = ktime_get();
                __copy_inv_map(shard, grain, target_line);
                copying += ktime_to_us(ktime_get()) - ktime_to_us(copy_start);

                d_stat.inv_m_w += spp->pgsz;
                d_stat.inv_m_r += spp->pgsz;

                mark_grain_invalid(shard, grain, GRAIN_PER_PAGE);
                i += GRAIN_PER_PAGE;
#endif
            } else if(mapping_line && valid_g) {
                clear_start = ktime_get();
                clear_count++;

                /*
                 * A grain containing live LPA to PPA mapping information.
                 */

                if(oob[pgidx][i] == UINT_MAX) {
                    NVMEV_ASSERT(false);
                    continue;
                }

                struct cmt_struct *cmt = cache->get_cmt(cache, oob[pgidx][i]);
                uint32_t idx = IDX(__lpa_from_oob(oob[pgidx][i]));

#ifdef GC_STANDARD
                len = GRAIN_PER_PAGE;
#else
                len = __glen_from_oob(oob[pgidx][i]);
#endif

#ifdef GC_STANDARD
                NVMEV_ASSERT(cmt->g_off == 0 && i == 0);
#endif
                if(cmt->t_ppa != pgidx || cmt->g_off != i) {
                    NVMEV_DEBUG("CMT IDX %u moved from PPA %llu to PPA %u\n", 
                                 idx, pgidx, cmt->t_ppa);
                    i += len - 1;
                    clearing += ktime_to_us(ktime_get()) - ktime_to_us(clear_start);
                    continue;
                }

                //NVMEV_INFO("CMT IDX %u was %u grains in size from grain %llu PPA %llu (%u)\n", 
                //             idx, g_len, grain, G_IDX(grain + i), cmt->t_ppa);
                mark_grain_invalid(shard, grain, len);
                __copy_map(shard, idx, len);

                i += len - 1;
                clearing += ktime_to_us(ktime_get()) - ktime_to_us(clear_start);
            } else if(!mapping_line && valid_g) {
#ifndef GC_STANDARD
                NVMEV_ASSERT(pg_inv_cnt[pgidx] <= GRAIN_PER_PAGE);
#endif
                NVMEV_ASSERT(!mapping_line);
                
                uint64_t lpa = __lpa_from_oob(oob[pgidx][i]);
                len = __glen_from_oob(oob[pgidx][i]);

                if(__valid_mapping(shard, lpa, grain)) {
                    mark_grain_invalid(shard, grain, len);
                    __copy_valid_pair(shard, lpa, len, lpa_lens, lpa_len_idx);
                    lpa_len_idx++;
                }

                i += len - 1;
            } 
        }

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

    end = ktime_get();
    clean_first_half += ktime_to_us(end) - ktime_to_us(start);

	if (page_cnt <= 0) {
		return;
    }

	if (cpp->enable_gc_delay) {
		struct nand_cmd gcr = {
			.type = GC_IO,
			.cmd = NAND_READ,
			.stime = 0,
			.xfer_size = spp->pgsz * page_cnt,
			.interleave_pci_dma = false,
			.ppa = &ppa_copy,
		};
		ssd_advance_nand(shard->ssd, &gcr);

        if(mapping_line) {
            d_stat.trans_r_tgc += spp->pgsz * page_cnt;
        } else {
            d_stat.data_r_dgc += spp->pgsz * page_cnt;
        }
	}

    start = ktime_get();
    if(!mapping_line) {
        do_bulk_mapping_update_v(shard, lpa_lens, lpa_len_idx);
    }
    end = ktime_get();
    clean_third_half += ktime_to_us(end) - ktime_to_us(start);

    return;
}

static void mark_line_free(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct line_mgmt *lm = &demand_shard->lm;
	struct line *line = get_line(demand_shard, ppa);

    NVMEV_DEBUG("Marking line %d free\n", line->id);

	line->ipc = 0;
	line->vpc = 0;
    line->igc = 0;
    line->vgc = 0;
	/* move this line to free line list */
	list_add_tail(&line->entry, &lm->free_line_list);
	lm->free_line_cnt++;
}

static uint64_t do_gc(struct demand_shard *shard, bool force)
{
	struct line *victim_line = NULL;
	struct ssdparams *spp = &shard->ssd->sp;
    struct convparams *cpp = &shard->cp;
	struct ppa ppa;
    struct gc_data *gcd = &shard->gcd;
	int flashpg;
    uint64_t pgidx;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

    uint64_t map = 0, total = 0, cleaning = 0, freeing = 0;

    ktime_t gc_start, start, gc_end, end;
    gc_start = ktime_get();

	victim_line = select_victim_line(shard, force);
	if (!victim_line) {
        BUG_ON(true);
		return nsecs_completed;
	}

    gcd->map = victim_line->map;

    user_pgs_this_gc = gc_pgs_this_gc = map_gc_pgs_this_gc = map_pgs_this_gc = 0;

	ppa.g.blk = victim_line->id;
	NVMEV_INFO("GC-ing %s line:%d,ipc=%d(%d),igc=%d(%d),victim=%d,full=%d,free=%d\n", 
            gcd->map? "MAP" : "USER", ppa.g.blk,
		    victim_line->ipc, victim_line->vpc, victim_line->igc, victim_line->vgc,
            shard->lm.victim_line_cnt, shard->lm.full_line_cnt, 
            shard->lm.free_line_cnt);

    if(gcd->map) {
        d_stat.tgc_cnt++;
    } else {
        d_stat.dgc_cnt++;
    }

	shard->wfc.credits_to_refill = victim_line->igc;
#ifndef GC_STANDARD
    start = ktime_get();
    if(!gcd->map) {
        nsecs_completed = __get_inv_mappings(shard, victim_line->id);
    }
    end = ktime_get();
    map = ktime_to_us(end) - ktime_to_us(start);
#endif
    nsecs_latest = max(nsecs_latest, nsecs_completed);

    atomic_set(&gcd->gc_rem, 0);

	/* copy back valid data */
	for (flashpg = 0; flashpg < spp->flashpgs_per_blk; flashpg++) {
		int ch, lun;

		ppa.g.pg = flashpg * spp->pgs_per_flashpg;
		for (ch = 0; ch < spp->nchs; ch++) {
			for (lun = 0; lun < spp->luns_per_ch; lun++) {
				struct nand_lun *lunp;

				ppa.g.ch = ch;
				ppa.g.lun = lun;
				ppa.g.pl = 0;
				lunp = get_lun(shard->ssd, &ppa);

                start = ktime_get();
				clean_one_flashpg(shard, &ppa);
                end = ktime_get();
                cleaning += ktime_to_us(end) - ktime_to_us(start);

				if (flashpg == (spp->flashpgs_per_blk - 1)) {
					struct convparams *cpp = &shard->cp;

                    start = ktime_get();
					mark_block_free(shard, &ppa);

					if (cpp->enable_gc_delay) {
						struct nand_cmd gce = {
							.type = GC_IO,
							.cmd = NAND_ERASE,
							.stime = 0,
							.interleave_pci_dma = false,
							.ppa = &ppa,
						};
						nsecs_completed = ssd_advance_nand(shard->ssd, &gce);
                        nsecs_latest = max(nsecs_latest, nsecs_completed);
					}
                    end = ktime_get();
                    freeing += ktime_to_us(end) - ktime_to_us(start);

					lunp->gc_endtime = lunp->next_lun_avail_time;
				}
			}
		}
	}

    NVMEV_ASSERT(gcd->offset > 0);

    if(gcd->offset < GRAIN_PER_PAGE) {
        uint64_t pgidx = gcd->pgidx;
        uint32_t offset = gcd->offset;
        uint64_t grain = PPA_TO_PGA(pgidx, offset);
        struct ppa ppa = ppa_to_struct(spp, pgidx);

        NVMEV_DEBUG("Marking %d grains invalid after GC copies pgidx %u.\n", 
                    GRAIN_PER_PAGE - offset, pgidx);

        uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
        uint64_t to = shard_off + (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
               GRAINED_UNIT);

        for(int i = offset; i < GRAIN_PER_PAGE; i++) {
            shard->oob[pgidx][i] = UINT_MAX;
#ifdef GC_STANDARD
            shard->grain_bitmap[PPA_TO_PGA(pgidx, i)] = 0;
#endif
        }

        mark_grain_valid(shard, PPA_TO_PGA(pgidx, offset), 
                         GRAIN_PER_PAGE - offset);
        mark_grain_invalid(shard, PPA_TO_PGA(pgidx, offset), 
                           GRAIN_PER_PAGE - offset);

        if (cpp->enable_gc_delay) {
            struct nand_cmd gcw = {
                .type = GC_IO,
                .cmd = NAND_NOP,
                .stime = 0,
                .interleave_pci_dma = false,
                .ppa = &ppa,
            };

            if (last_pg_in_wordline(shard, &ppa)) {
                gcw.cmd = NAND_WRITE;
                gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;

                if(gcd->map) {
                    d_stat.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
                } else {
                    d_stat.data_w_dgc += spp->pgsz * spp->pgs_per_oneshotpg;
                }
            }

            nsecs_completed = ssd_advance_nand(shard->ssd, &gcw);
            //if (last_pg_in_wordline(shard, &ppa)) {
            //    schedule_internal_operation(UINT_MAX, nsecs_completed, NULL,
            //            spp->pgs_per_oneshotpg * spp->pgsz);
            //}
        }

        nvmev_vdev->space_used += (GRAIN_PER_PAGE - offset) * GRAINED_UNIT;

        gcd->offset = GRAIN_PER_PAGE;
        gcd->pgidx = UINT_MAX;
    }

    gc_end = ktime_get();
    total = ktime_to_us(gc_end) - ktime_to_us(gc_start);

    NVMEV_ASSERT(user_pgs_this_gc == 0);
    NVMEV_INFO("%llu user %llu GC %llu map GC this round. %lu pgs_per_line."
               " Took %llu microseconds (%llu map %llu clean %llu free).\n"
               " Clean breakdown %llu first half %llu second half %llu third half"
               " %llu time spent searching %llu clearing %llu copying %llu clear_count.", 
                user_pgs_this_gc, gc_pgs_this_gc, map_gc_pgs_this_gc, 
                spp->pgs_per_line, total, map, cleaning, freeing,
                clean_first_half, clean_second_half, clean_third_half, 
                mapping_searches, clearing, copying, clear_count);

    clean_first_half = clean_second_half = clean_third_half = mapping_searches = 0;
    clearing = copying = clear_count = 0;

    /* update line status */
	mark_line_free(shard, &ppa);
#ifndef GC_STANDARD
    if(!gcd->map) {
        __clear_gc_data(shard);
    }
#endif

	return nsecs_latest;
}

uint32_t loops = 0;
static uint64_t forground_gc(struct demand_shard *demand_shard)
{
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

	while(should_gc_high(demand_shard)) {
		NVMEV_INFO("should_gc_high passed %u %u", 
					demand_shard->lm.free_line_cnt,  
					demand_shard->cp.gc_thres_lines_high);
		/* perform GC here until !should_gc(demand_shard) */
		nsecs_completed = do_gc(demand_shard, true);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
	}
    
    return nsecs_latest;
}

static bool is_same_flash_page(struct demand_shard *demand_shard, struct ppa ppa1, struct ppa ppa2)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	uint64_t ppa1_page = ppa1.g.pg / spp->pgs_per_flashpg;
	uint64_t ppa2_page = ppa2.g.pg / spp->pgs_per_flashpg;

	return (ppa1.h.blk_in_ssd == ppa2.h.blk_in_ssd) && (ppa1_page == ppa2_page);
}

static inline uint8_t __klen_from_value(uint8_t *ptr) {
    return *(uint8_t*) ptr;
}

static inline char* __key_from_value(uint8_t *ptr) {
    return (char*) (ptr + 1);
}

struct pte_e_args {
    uint64_t out_ppa;
    uint64_t prev_ppa;
    struct cmt_struct *cmt;
    uint64_t idx;
    struct ssdparams *spp;
    uint32_t grain;
    uint32_t len;
};

void __pte_evict_work(void *voidargs, uint64_t*, uint64_t*) {
    struct pte_e_args *args = (struct pte_e_args*) voidargs;

    uint64_t out_ppa = args->out_ppa;
    struct cmt_struct *cmt = args->cmt;
    struct pt_struct *pt = cmt->pt;
    uint64_t idx = args->idx;
    struct ssdparams *spp = args->spp;
    uint64_t start_lpa = idx * EPP;
    uint32_t grain = args->grain;
    uint32_t len = args->len * GRAINED_UNIT;
    uint32_t extra = 0;

#ifndef GC_STANDARD
    extra = sizeof(lpa_t);
#endif

    //NVMEV_INFO("In %s %p\n", __func__, &cmt->outgoing);

    /*
     * TODO shard off.
     */

    uint64_t off = (out_ppa * spp->pgsz) + (grain * GRAINED_UNIT);
    uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;

    NVMEV_DEBUG("IDX %llu copying %u grains to PPA %llu grain %u to off %llu\n", 
                 idx, args->len, out_ppa, grain, off);

    for(int i = 0; i < len / ENTRY_SIZE; i++) {
#ifndef GC_STANDARD
        lpa_t lpa = pt[i].lpa;
        memcpy(ptr + (i * ENTRY_SIZE), &lpa, sizeof(lpa));
#endif
        ppa_t ppa = pt[i].ppa;
        memcpy(ptr + (i * ENTRY_SIZE) + extra, &ppa, sizeof(ppa));

#ifdef STORE_KEY_FP
        fp_t fp = pt[i].key_fp;
        memcpy(ptr + (i * ENTRY_SIZE) + sizeof(ppa), &fp, sizeof(fp));
#endif

        if(G_IDX(ppa) > spp->tt_pgs && ppa != UINT_MAX) {
            NVMEV_ERROR("Sending out LPA %llu PPA %u IDX %llu key %llu new PPA %llu prev PPA %llu\n", 
                         start_lpa + i, ppa, idx, *(uint64_t*) (ptr + 1), 
                         out_ppa, args->prev_ppa);
            NVMEV_ASSERT(false);
        }
        //if(ppa != UINT_MAX) {
        //    NVMEV_INFO("Sending out LPA %llu PPA %u in %s.\n", start_lpa + i, ppa, __func__);
        //}
    }

    cmt->lru_ptr = NULL;
    cmt->pt = NULL;

    atomic_dec(&cmt->outgoing);
    kfree(args);

    NVMEV_DEBUG("%s done for IDX %llu new ppa %llu old PPA %llu\n", 
                 __func__, idx, out_ppa, args->prev_ppa);
}

static inline uint32_t __entries_to_grains(struct demand_shard *shard,
                                           struct cmt_struct *cmt) {
    struct ssdparams *spp;
    uint32_t cnt;
    uint32_t ret;

    spp = &shard->ssd->sp;
#ifdef GC_STANDARD
    return GRAIN_PER_PAGE;
#else
    cnt = cmt->cached_cnt;
    ret = (cnt * ENTRY_SIZE) / GRAINED_UNIT;

    if((cnt * ENTRY_SIZE) % GRAINED_UNIT) {
        ret++;
    }

    return ret > cmt->len_on_disk ? ret : cmt->len_on_disk;
#endif
}

#ifndef GC_STANDARD
struct expand_w_args {
    struct demand_shard *shard;
    struct ssdparams *spp;
    struct cmt_struct *cmt;
    struct pt_struct *old;
    struct pt_struct *new;
    struct pt_struct pte;
    struct cache_member *cmbr;
    uint32_t len;
};

void __expand_work(void *voidargs, uint64_t*, uint64_t*) {
    struct expand_w_args *args;
    struct demand_shard *shard;
    struct ssdparams *spp;
    struct cmt_struct *cmt;
    struct pt_struct *old;
    struct pt_struct *new;
    struct pt_struct pte;
    struct cache_member *cmbr;
    uint32_t len;

    args = (struct expand_w_args*) voidargs;
    shard = args->shard;
    spp = args->spp;
    cmt = args->cmt;
    old = args->old;
    new = args->new;
    pte = args->pte;
    cmbr = args->cmbr;
    len = args->len;

    memcpy(new, old, len);

    for(int i = len / ENTRY_SIZE; i < spp->pgsz / ENTRY_SIZE; i++) {
        cmt->pt[i].lpa = UINT_MAX;
        cmt->pt[i].ppa = UINT_MAX;
    }

    cmt->pt[cmt->cached_cnt].lpa = pte.lpa;
    cmt->pt[cmt->cached_cnt].ppa = pte.ppa;
    cmt->cached_cnt++;

    cmt->state = DIRTY;
    //lru_update(cmbr->lru, cmt->lru_ptr);

    atomic_dec(&cmt->outgoing);
    kfree(args);
}

static inline uint32_t __need_expand(struct cmt_struct *cmt) {
    uint32_t expand_to = (cmt->cached_cnt * ENTRY_SIZE) / GRAINED_UNIT;

    if((cmt->cached_cnt * ENTRY_SIZE) % GRAINED_UNIT) {
        expand_to++;
    }

    if(expand_to < cmt->len_on_disk) {
        return false;
    }

    return (cmt->cached_cnt * ENTRY_SIZE) % GRAINED_UNIT == 0;
}

/*
 * This function just assigns a new physical page
 * to the mapping entry. Some space is wasted if the mapping
 * entry doesn't contain enough entries to fill a page, but
 * later it will be packed into a page with other mapping
 * entries during eviction.
 */

static void __expand_map_entry(struct demand_shard *shard, 
                               struct cmt_struct *cmt, struct pt_struct pte,
                               void *mem) {
    struct demand_cache *cache;
    struct ssdparams *spp;
    struct cache_member *cmbr;
    struct ppa p;
    uint64_t ppa;
    uint64_t **oob;

    cache = shard->cache;
    spp = &shard->ssd->sp;
    cmbr = &cache->member;
    oob = shard->oob;

    mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, cmt->g_off), 
                       cmt->len_on_disk);

    /*
     * Mapping table entries can't be over a page in size.
     */

    NVMEV_ASSERT(cmt->len_on_disk < GRAIN_PER_PAGE);

skip:
    p = get_new_page(shard, MAP_IO);
    ppa = ppa2pgidx(shard, &p);

    BUG_ON(!cmt->lru_ptr);

    advance_write_pointer(shard, MAP_IO);
    mark_page_valid(shard, &p);
    mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

    if(ppa == 0) {
        mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
        goto skip;
    }

    cmt->pt[cmt->cached_cnt].lpa = pte.lpa;
    cmt->pt[cmt->cached_cnt].ppa = pte.ppa;
    cmt->mems[cmt->cached_cnt] = mem;
    cmt->cached_cnt++;
    cmt->state = DIRTY;

    cmt->t_ppa = ppa;
    cmt->g_off = 0;
    cmt->len_on_disk++;
    cmbr->nr_cached_tentries++;

    oob[ppa][0] = ((uint64_t) cmt->len_on_disk << 32) | cmt->idx * EPP;
    //for(int i = 1; i < cmt->len_on_disk; i++) {
    //    oob[ppa][i] = 0;
    //}

    //for(int i = cmt->len_on_disk; i < GRAIN_PER_PAGE; i++) {
    //    oob[ppa][i] = UINT_MAX;
    //}

    if(GRAIN_PER_PAGE - cmt->len_on_disk > 0) {
        mark_grain_invalid(shard, PPA_TO_PGA(ppa, cmt->len_on_disk), 
                           GRAIN_PER_PAGE - cmt->len_on_disk);
    }
}
#endif

static void __update_map(struct demand_shard *shard, struct cmt_struct *cmt,
                         lpa_t lpa, void* mem, 
                         struct pt_struct pte, uint32_t pos) {
    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;

    NVMEV_ASSERT(cmt->pt);
#ifdef GC_STANDARD
    cmt->pt[OFFSET(lpa)] = pte;
    cmt->mems[OFFSET(lpa)] = mem;
    cmt->state = DIRTY;
    //lru_update(cmbr->lru, cmt->lru_ptr);
#else
    //NVMEV_INFO("LPA %u gets PPA %u\n", lpa, pte.ppa);
    cmt->state = DIRTY;
    if(pos != UINT_MAX) {
        if(cmt->pt[pos].lpa == lpa && cmt->pt[pos].ppa != UINT_MAX) {
            /*
             * The PPA can be UINT_MAX here if it was previously
             * deleted.
             */
            __record_inv_mapping(shard, lpa, cmt->pt[pos].ppa, NULL);
        }

        //NVMEV_INFO("Had Pos %u for LPA %u it was LPA %u\n", pos, lpa,
        //            cmt->pt[pos].lpa);
        NVMEV_ASSERT(cmt->pt[pos].lpa == lpa);
        NVMEV_ASSERT(cmt->mems[pos] = mem);
        cmt->pt[pos] = pte;
        return;
	} else if(cmt->cached_cnt == 0) {
        cmt->mems[0] = mem;
        cmt->pt[0] = pte;
        cmt->cached_cnt++;
		return;
    } else if(!__need_expand(cmt)) {
        cmt->mems[cmt->cached_cnt] = mem;
        cmt->pt[cmt->cached_cnt++] = pte;
        return;
    }

	if(cmt->cached_cnt > 0 && __need_expand(cmt)) {
		__expand_map_entry(shard, cmt, pte, mem);
	} else {
		NVMEV_ASSERT(false);
		cmt->pt[cmt->cached_cnt].lpa = lpa;
		cmt->pt[cmt->cached_cnt].ppa = pte.ppa;
		cmt->cached_cnt++;
	}
#endif
}

static bool __cache_hit(struct demand_cache *cache, lpa_t lpa) {
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cache->get_cmt(cache, lpa);
    return cmt->pt != NULL;
}

struct pt_struct __lpa_to_pte(struct demand_shard *shard,
                              struct cmt_struct *cmt, lpa_t lpa,
                              uint32_t *pos) {
#ifdef GC_STANDARD
    //NVMEV_INFO("Reading offset %lu LPA %u PPA %u addr %p\n", 
    //            OFFSET(lpa), lpa, cmt->pt[OFFSET(lpa)].ppa, cmt->pt);

    if(cmt->pt[OFFSET(lpa)].ppa == UINT_MAX) {
        NVMEV_ASSERT(cmt->mems[OFFSET(lpa)] == NULL);
    }

    return cmt->pt[OFFSET(lpa)];
#else
    uint32_t g_len = __entries_to_grains(shard, cmt);
    struct pt_struct pte;
    struct pt_struct *found_pte;

    pte.lpa = lpa;
	pte.ppa = UINT_MAX;

    for(int i = 0; i < cmt->cached_cnt; i++) {
        if(cmt->pt[i].lpa == lpa) {
            pte.lpa = lpa;
            pte.ppa = cmt->pt[i].ppa;

            if(pos) {
                *pos = i;
            }

            //NVMEV_INFO("Returning PPA %u\n", pte.ppa);
            break;
        }
    }

    return pte;
#endif
}

#define MAX_SEARCH 256
struct cmt_struct *search[MAX_SEARCH];

/*
 * Simple, greedy approach that assumes we can find a page worth
 * of grains to evict.
 */

static uint32_t __collect_victims(struct demand_shard *shard, uint32_t target_g) {
    struct demand_cache *cache;
    struct ssdparams *spp;
    struct cache_member *cmbr;
    uint32_t idx, count, g_len;
    struct cmt_struct *victim, *prev_victim;
    bool all_clean = true;

    cache = shard->cache;
    spp = &shard->ssd->sp;
    cmbr = &cache->member;
    idx = 0;
    count = 0;

    uint64_t total = 0;

	//victim = lru_peek(cmbr->lru);
	victim = fifo_dequeue(cmbr->fifo);

    for(int i = 0; i < MAX_SEARCH; i++) {
        if(!victim) {
            //NVMEV_INFO("Exiting eviction because no victim i %d.\n", i);
            break;
        }

        if(victim->state == DIRTY) {
            all_clean = false;
        }

        g_len = __entries_to_grains(shard, victim);
        total += g_len;

#ifdef GC_STANDARD
        NVMEV_ASSERT(g_len == GRAIN_PER_PAGE);
#endif

        NVMEV_DEBUG("Got %u grains for IDX %u in eviction.\n",
                    g_len, victim->idx);
        NVMEV_ASSERT(victim->lru_ptr);

        if(g_len + count <= target_g) {
            search[idx++] = victim;
            count += g_len;
            NVMEV_DEBUG("We added it to count. Count is now %u\n", count);

            cmbr->nr_cached_tentries -= g_len;
        } else {
            fifo_enqueue(cmbr->fifo, victim);
            break;
        }

        if(count >= target_g) {
            break;
        }
    
        victim = fifo_dequeue(cmbr->fifo);
        //victim = (struct cmt_struct *)lru_prev(cmbr->lru, victim->lru_ptr);
    }

    //NVMEV_INFO("Removing %u entries from the LRU. AV glen %llu\n", idx,
    //            total / idx);

    for(int i = 0 ; i < idx; i++) {
        NVMEV_ASSERT(search[i]->lru_ptr);

        while(atomic_read(&search[i]->outgoing) > 0) {
            cpu_relax();
        }

        if(all_clean) {
            search[i]->pt = NULL;
        }

        //lru_delete(cmbr->lru, search[i]->lru_ptr);
        search[i]->lru_ptr = NULL;
    }

    return all_clean ? UINT_MAX : idx;
}

static uint64_t __stime_or_clock(uint64_t stime) {
    uint64_t clock = __get_wallclock();
    return clock > stime ? clock : stime;
}

static uint64_t __evict_one(struct demand_shard *shard, struct nvmev_request *req,
                            uint64_t stime, uint64_t *credits) {
    struct demand_cache *cache;
    struct cache_member *cmbr;
    struct ssdparams *spp;
    struct cache_stat *cstat;
    struct cmt_struct* victim;
    uint64_t **oob;
    bool got_ppa = false;
    uint32_t grain = 0;
    uint32_t g_len = 0;
    uint32_t byte_len = 0;
    uint32_t evicted = 0;
    uint32_t cnt = 0;
    struct ppa p;
    ppa_t ppa;
    bool all_clean = true;
    uint64_t nsecs_completed = 0;

	uint64_t start = 0, end = 0;

    cache = shard->cache;
    cmbr = &cache->member;
    spp = &shard->ssd->sp;
    cstat = &cache->stat;
    oob = shard->oob;

	start = ktime_get();
    cnt = __collect_victims(shard, GRAIN_PER_PAGE);

    if(cnt == UINT_MAX) {
        cstat->clean_evict++;
        return nsecs_completed;
    }

#ifdef GC_STANDARD
    NVMEV_ASSERT(cnt == 1);
#endif
    for(int i = 0; i < cnt; i++) {
        victim = search[i];

        NVMEV_ASSERT(victim);
        NVMEV_ASSERT(atomic_read(&victim->outgoing) == 0);
        NVMEV_DEBUG("Cache is full. Got victim with IDX %u\n", victim->idx);

        g_len = __entries_to_grains(shard, victim);

        if (victim->state == DIRTY) {
            bool first = g_len == victim->len_on_disk;
#ifdef GC_STANDARD
            NVMEV_ASSERT(first);
#endif
            if(first) { 
                /*
                 * The first holds true if this mapping table entry hasn't expanded.
                 * If it previously expanded, the grains that it mapped too
                 * were already marked invalid, and the above condition will
                 * be false as len_on_disk will be greater.
                 */
                NVMEV_DEBUG("Trying to mark IDX %u TPPA %u len %u off %llu invalid.\n",
                             victim->idx, victim->t_ppa, victim->len_on_disk, 
                             victim->g_off);
                NVMEV_ASSERT(victim->len_on_disk > 0);
                mark_grain_invalid(shard, PPA_TO_PGA(victim->t_ppa, victim->g_off), 
                                   victim->len_on_disk);
            }

#ifdef GC_STANDARD
            NVMEV_ASSERT(!got_ppa);
            NVMEV_ASSERT(victim->len_on_disk == GRAIN_PER_PAGE);
#endif
skip:
            uint64_t prev_ppa = victim->t_ppa;
            if(!got_ppa) {
                p = get_new_page(shard, MAP_IO);
                ppa = ppa2pgidx(shard, &p);

                advance_write_pointer(shard, MAP_IO);
                mark_page_valid(shard, &p);

                if(ppa == 0) {
                    mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
                    mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
                    goto skip;
                }

                (*credits) += GRAIN_PER_PAGE;
            }

            NVMEV_DEBUG("Assigned PPA %u (old %u) grain %u to victim at IDX %u in %s. Prev PPA %llu\n",
                         ppa, victim->t_ppa, grain, victim->idx, __func__, prev_ppa);

            got_ppa = true;

#ifdef GC_STANDARD
            NVMEV_ASSERT(victim->g_off == 0);
            NVMEV_ASSERT(victim->len_on_disk == GRAIN_PER_PAGE);

            grain = 0;
            g_len = spp->pgsz / GRAINED_UNIT;
#else
            NVMEV_DEBUG("Reduced cached tentries to %u\n", 
                        cmbr->nr_cached_tentries);

            g_len = __entries_to_grains(shard, victim);

            victim->g_off = grain;
            victim->len_on_disk = g_len;

            NVMEV_DEBUG("Set the length on disk for IDX %u to %u.\n", victim->idx,
                         g_len);
#endif
            /*
             * The IDX is used during GC so that we know which CMT entry
             * to update.
             */

            oob[ppa][grain] = ((uint64_t) g_len << 32) | victim->idx * EPP;
            NVMEV_DEBUG("Set OOB PPA %u grain %u to IDX %u\n", ppa, grain, victim->idx);
            //for(int j = 1; j < g_len; j++) {
            //    NVMEV_ASSERT(grain + j < GRAIN_PER_PAGE);
            //    oob[ppa][grain + j] = 0;
            //}

            mark_grain_valid(shard, PPA_TO_PGA(ppa, grain), g_len);

            victim->t_ppa = ppa;
            victim->state = CLEAN;

            grain += g_len;

            struct ppa p = ppa_to_struct(spp, victim->t_ppa);
            struct nand_cmd swr = {
                .type = USER_IO,
                .cmd = NAND_WRITE,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            };

            if (!shard->fastmode && last_pg_in_wordline(shard, &p)) {
                swr.stime = __stime_or_clock(stime);
                swr.ppa = &p;
                d_stat.trans_w += spp->pgsz * spp->pgs_per_oneshotpg;
                nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
            }

            victim->lru_ptr = NULL;
            victim->pt = NULL;

            //NVMEV_INFO("Evicted DIRTY mapping entry IDX %u in %s.\n",
            //        victim->idx, __func__);
            all_clean = false;
            cstat->dirty_evict++;
        } else {
            //NVMEV_INFO("Evicted CLEAN mapping entry IDX %u in %s.\n",
            //        victim->idx, __func__);
            victim->lru_ptr = NULL;
            victim->pt = NULL;
            cstat->clean_evict++;
        }
    }
	end = ktime_get();
    //NVMEV_INFO("Spent %llu us evicting.\n", 
    //            ktime_to_us(end) - ktime_to_us(start));

    if(!all_clean && grain < GRAIN_PER_PAGE) {
        /*
         * We couldn't fill a page with mapping entries. Clear the rest of
         * the page.
         */

        NVMEV_ASSERT(GRAIN_PER_PAGE - grain > 0);
        mark_grain_valid(shard, PPA_TO_PGA(ppa, grain), GRAIN_PER_PAGE - grain);
        mark_grain_invalid(shard, PPA_TO_PGA(ppa, grain), GRAIN_PER_PAGE - grain);
    }

    return nsecs_completed;
}

uint64_t __get_one(struct demand_shard *shard, struct cmt_struct *cmt,
                   bool first, uint64_t stime, bool *missed) {
    struct demand_cache *cache;
    struct cache_member *cmbr;
    struct ssdparams *spp;
    struct cache_stat *cstat;
    struct cmt_struct* victim;
    uint64_t nsecs_completed = 0;
    uint32_t grain;
    uint64_t **oob;

    cache = shard->cache;
    cmbr = &cache->member;
    spp = &shard->ssd->sp;
    cstat = &cache->stat;
    grain = cmt->g_off;
	oob = shard->oob;

	if(!shard->fastmode) {
		while(atomic_read(&cmt->outgoing) == 1) {
			cpu_relax();
		}
	}

    if(!first) {
        cmt->pt = (struct pt_struct*) cmt->pt_mem;
        NVMEV_DEBUG("__get_one for IDX %u CMT PPA %u\n", cmt->idx, cmt->t_ppa);

        /*
         * If this wasn't the first access of this mapping
         * table page, we need to read it from disk.
         */

        if(cmt->t_ppa > spp->tt_pgs) {
            NVMEV_INFO("%s tried to convert PPA %u\n", __func__, cmt->t_ppa);
        }

        if(!shard->fastmode) {
            struct ppa p = ppa_to_struct(spp, cmt->t_ppa);
            struct nand_cmd srd = {
                .type = USER_IO,
                .cmd = NAND_READ,
                .stime = __stime_or_clock(stime),
                .interleave_pci_dma = false,
                .ppa = &p,
                .xfer_size = spp->pgsz
            };

            nsecs_completed = ssd_advance_nand(shard->ssd, &srd);
            d_stat.trans_r += spp->pgsz;
        }
        //NVMEV_INFO("Sent a read for CMT PPA %u\n", cmt->t_ppa);
    } else {
        cmt->pt = kmalloc(spp->pgsz, GFP_KERNEL);
        cmt->pt_mem = cmt->pt;

        NVMEV_ASSERT(cmt->pt);
#ifdef GC_STANDARD
        cmt->len_on_disk = GRAIN_PER_PAGE;
        cmt->g_off = 0;
        
        for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
            cmt->pt[i].ppa = UINT_MAX;
            cmt->mems[i] = NULL;
        }
#else
        cmt->len_on_disk = ORIG_GLEN;
        cmt->g_off = 0;

        for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
            cmt->pt[i].lpa = UINT_MAX;
            cmt->pt[i].ppa = UINT_MAX;
            cmt->mems[i] = NULL;
        }
#endif
    }

    cmt->lru_ptr = fifo_enqueue(cmbr->fifo, (void*) cmt);
    cmbr->nr_cached_tentries += cmt->len_on_disk;

    *missed = true;
    cstat->cache_miss++;

    return nsecs_completed;
}

static uint64_t __read_and_compare(struct demand_shard *shard, ppa_t grain,
                                   void* mem, struct hash_params *h_params, 
                                   KEYT *k, uint64_t stime,
                                   uint64_t *nsecs) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct inflight_params *i_params;
    uint64_t nsecs_completed;
    uint64_t local;
    uint32_t failures = 0;

    uint64_t ppa = G_IDX(grain);
    uint64_t offset = G_OFFSET(grain);

    NVMEV_ASSERT(mem);

    if(ppa > spp->tt_pgs) {
        NVMEV_INFO("Tried to convert PPA %llu\n", ppa);
    }
	NVMEV_ASSERT(ppa < spp->tt_pgs);

    struct ppa p = ppa_to_struct(spp, ppa);
    struct nand_cmd swr = {
        .type = USER_IO,
        .cmd = NAND_READ,
        .interleave_pci_dma = false,
        .xfer_size = spp->pgsz,
        .stime = stime,
    };

    if(!shard->fastmode) {
        swr.ppa = &p;
        *nsecs = ssd_advance_nand(ssd, &swr);
    }
    d_stat.data_r += spp->pgsz;
    //NVMEV_INFO("Read completed %llu\n", *nsecs);

    uint8_t* ptr = mem;
    uint8_t klen = __klen_from_value(ptr);
    char* key = __key_from_value(ptr);

    KEYT check_key;
    KEYT actual_key;

    check_key.len = k->len;
    check_key.key = k->key;

    actual_key.len = klen;
    actual_key.key = key;

    BUG_ON(!h_params);

    if (KEYCMP(actual_key, check_key) == 0) {
        //NVMEV_INFO("Match %llu %llu when checking offset %llu.\n", 
        //*(uint64_t*) actual_key.key, *(uint64_t*) check_key.key,
        //      *(uint64_t*) ptr);

        /* hash key found -> update */
        d_stat.fp_match_w++;
        return 0;
    } else {
        //NVMEV_INFO("Fail got %llu wanted %llu when checking offset %llu.\n", 
        //           *(uint64_t*) actual_key.key, *(uint64_t*) check_key.key,
        //           (uint64_t) ptr);

        /* retry */
        d_stat.fp_collision_w++;
        h_params->cnt++;
        return 1;
    }
}

static inline uint32_t __vlen_from_value(uint8_t* mem) {
    uint8_t klen = *(uint8_t*) (mem);
    uint32_t vlen = *(uint32_t*) (mem + KLEN_MARKER_SZ + klen);
    return vlen;
}

static bool __read(struct nvmev_ns *ns, struct nvmev_request *req, 
                   struct nvmev_result *ret, bool for_del) {
    struct demand_shard *demand_shards = (struct demand_shard *)ns->ftls;
    struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;

    uint64_t nsecs_start = req->nsecs_start;
    uint64_t nsecs_latest, nsecs_completed = 0;
    uint64_t credits = 0;
    uint32_t status = 0;

    uint8_t klen = cmd_key_length(cmd);
    uint32_t vlen = cmd_value_length(cmd);

    uint64_t hash = CityHash64(cmd->kv_store.key, klen);
    struct demand_shard *shard = &demand_shards[hash % SSD_PARTITIONS];
    struct ssdparams *spp = &shard->ssd->sp;

    /*
     * This assumes we're reading 4K pages for the mappings and data.
     * Needs to change if that changes.
     */

    uint64_t nsecs_fw;
    if (vlen <= KB(4)) {
        nsecs_fw = spp->fw_4kb_rd_lat;
    } else {
        nsecs_fw = spp->fw_rd_lat;
    }

    nsecs_start += nsecs_fw;
    nsecs_latest = nsecs_start;

    KEYT key;
    key.key = cmd->kv_store.key;
    key.len = klen;

    NVMEV_ASSERT(key.key);
    NVMEV_ASSERT(cmd->kv_store.key);
    NVMEV_ASSERT(cmd);
    NVMEV_ASSERT(vlen <= spp->pgsz);
    NVMEV_ASSERT(klen <= 16);

    uint32_t pos = UINT_MAX;
    uint32_t len;
    bool missed = false;
    struct hash_params h; 
    h.hash = hash;
    h.cnt = 0;
    h.find = HASH_KEY_INITIAL;
    h.lpa = 0;

    uint64_t **oob = shard->oob;
lpa:
    lpa_t lpa = get_lpa(shard->cache, key, &h);
    h.lpa = lpa;

    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cache->get_cmt(cache, lpa);
    struct cache_stat *cstat = &cache->stat;

    if (h.cnt > shard->ftl->max_try) {
        /*
         * max_try is the most we've hashed the same key to find an empty
         * LPA.
         */
        cmd->kv_retrieve.value_len = 0;
        cmd->kv_retrieve.rsvd = U64_MAX;

        if(nsecs_latest == nsecs_start) {
            nsecs_latest = local_clock();
        }

        //NVMEV_INFO("Key %s (%llu) not found.\n", 
        //        (char*) cmd->kv_store.key, *(uint64_t*) cmd->kv_store.key);

        status = KV_ERR_KEY_NOT_EXIST;
        goto out;
    }

    if(cmt->t_ppa == UINT_MAX) {
        NVMEV_INFO("Key %s (%llu) tried to read missing CMT entry IDX %u.\n", 
                (char*) cmd->kv_store.key, *(uint64_t*) cmd->kv_store.key, cmt->idx);
        h.cnt++;

        missed = false;
        goto lpa;
    }

    while(atomic_read(&cmt->outgoing) == 1) {
        cpu_relax();
    }

cache:
    if(__cache_hit(cache, lpa)) { 
        struct pt_struct pte = __lpa_to_pte(shard, cmt, lpa, &pos);
        void* old_mem;

        //NVMEV_INFO("Read for key %llu (%llu) checks LPA %u PPA %u\n", 
        //            *(uint64_t*) (key.key), *(uint64_t*) &(cmd->kv_store.key), 
        //            lpa, pte.ppa);

        if (!IS_INITIAL_PPA(pte.ppa)) {
            d_stat.d_read_on_read += spp->pgsz;

#ifdef GC_STANDARD
            old_mem = cmt->mems[OFFSET(lpa)];
#else
            old_mem = cmt->mems[pos];
#endif

            if(__read_and_compare(shard, pte.ppa, old_mem, &h, &key, 
                                  nsecs_latest, &nsecs_completed)) {
                nsecs_latest = max(nsecs_latest, nsecs_completed);

                if (vlen <= KB(4)) {
                    nsecs_latest += spp->fw_4kb_rd_lat;
                } else {
                    nsecs_latest += spp->fw_rd_lat;
                }

                pos = UINT_MAX;
                //missed = false;
                goto lpa;
            }

            nsecs_latest = max(nsecs_latest, nsecs_completed);

            len = __vlen_from_value(old_mem);
            if(!for_del) {
                cmd->kv_retrieve.value_len = len;
                cmd->kv_retrieve.rsvd = (uint64_t) old_mem;
                //cmd->kv_retrieve.rsvd = ((uint64_t) pte.ppa) * GRAINED_UNIT;
            } else {
                cmd->kv_retrieve.rsvd = U64_MAX;
                mark_grain_invalid(shard, pte.ppa, 
                                   len % GRAINED_UNIT ? 
                                   (len / GRAINED_UNIT) + 1 :
                                   len / GRAINED_UNIT);
                pte.ppa = UINT_MAX;
                __update_map(shard, cmt, lpa, NULL, pte, pos);
            }

            if(!for_del && (vlen < len)) {
                NVMEV_ERROR("Buffer with size %u too small for value %u\n",
                             vlen, len);
                cmd->kv_retrieve.rsvd = U64_MAX;
                status = KV_ERR_BUFFER_SMALL;
            } else {
                status = NVME_SC_SUCCESS;
            }
        } else {
            NVMEV_INFO("Miss LPA %u key %llu\n", lpa, *(uint64_t*) key.key);
            cmd->kv_retrieve.value_len = 0;
            cmd->kv_retrieve.rsvd = U64_MAX;
            status = KV_ERR_KEY_NOT_EXIST;
        }

        shard->cache->touch(shard->cache, lpa);

        if(!missed) {
            cstat->cache_hit++;
        }

        goto out;
    } else if(cmt->t_ppa != UINT_MAX) {
        if (cgo_is_full(cache)) {
            nsecs_completed = __evict_one(shard, req, nsecs_latest, &credits);
            nsecs_latest = max(nsecs_latest, nsecs_completed);
            d_stat.t_write_on_read += spp->pgsz;
        }

        nsecs_completed = __get_one(shard, cmt, false, nsecs_latest, &missed);
        nsecs_latest = max(nsecs_latest, nsecs_completed);

        d_stat.t_read_on_read += spp->pgsz;
        goto cache;
    }

out:
    consume_write_credit(shard, credits);
    check_and_refill_write_credit(shard);
    nsecs_latest = max(nsecs_latest, nsecs_completed);

    if(status == 0) {
        //NVMEV_INFO("Read for key %llu (%llu %u) finishes with LPA %u PPA %llu vlen %u"
        //            " count %u\n", 
        //            *(uint64_t*) (key.key), 
        //            *(uint64_t*) (key.key + 4), 
        //            *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), 
        //            lpa, cmd->kv_retrieve.rsvd == U64_MAX ? U64_MAX :
        //            cmd->kv_retrieve.rsvd / GRAINED_UNIT, 
        //            cmd->kv_retrieve.value_len, h.cnt);
    } else {
        //NVMEV_INFO("Read for %s key %llu (%llu %u) FAILS with LPA %u PPA %llu vlen %u"
        //            " count %u\n", 
        //            key.key[0] == 'L' ? "log" : "regular",
        //            *(uint64_t*) (key.key),
        //            *(uint64_t*) (key.key + 4), 
        //            *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), 
        //            lpa, cmd->kv_retrieve.rsvd == U64_MAX ? U64_MAX :
        //            cmd->kv_retrieve.rsvd / GRAINED_UNIT, 
        //            cmd->kv_retrieve.value_len, h.cnt);
    }

    d_stat.read_req_cnt++;

    ret->nsecs_target = nsecs_latest;
    ret->status = status;
    return true;
}

char kbuf[255];
static bool conv_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
    return __read(ns, req, ret, false);
}

static bool conv_delete(struct nvmev_ns *ns, struct nvmev_request *req, 
                        struct nvmev_result *ret)
{
    return __read(ns, req, ret, true);
}

struct ppa cur_page;
//inline bool __crossing_page(struct ssdparams *spp, uint64_t offset, uint32_t vlen) {
//    if(offset % spp->pgsz == 0) {
//        return true;
//    }
//    return !((offset % spp->pgsz) + vlen <= spp->pgsz);
//}

inline bool __crossing_page(struct ssdparams *spp, uint64_t offset, uint32_t vlen) {
    uint64_t page_mask = spp->pgsz - 1;
    // Check if offset is at the start of a new page
    if ((offset & page_mask) == 0) return true;
    // Check if the operation crosses a page boundary
    return (offset & ~page_mask) != ((offset + vlen - 1) & ~page_mask);
}

static struct hash_params *make_hash_params(request *const req, uint64_t hash) {
	struct hash_params *h_params = 
    (struct hash_params *)kzalloc(sizeof(struct hash_params), GFP_KERNEL);
    h_params->hash = hash;
	h_params->cnt = 0;
	h_params->find = HASH_KEY_INITIAL;
	h_params->lpa = 0;

	return h_params;
}

struct pt_struct* __idx_to_memaddr(struct ssdparams *spp, uint64_t ppa) {
    uint64_t off = ppa * spp->pgsz;
    //NVMEV_INFO("Returning offset %llu in %s\n", off, __func__);
    return nvmev_vdev->ns[0].mapped + off;
}

void __mark_early(uint64_t grain, uint8_t klen, char* key) {
    uint64_t off = grain * GRAINED_UNIT;

    /*
     * TODO: shard off.
     */

    memcpy(nvmev_vdev->ns[0].mapped + off, &klen, sizeof(klen));
    memcpy(nvmev_vdev->ns[0].mapped + off + sizeof(klen), key, klen);
}

uint64_t __release_map(void *voidargs, uint64_t*, uint64_t*) {
    atomic_t *outgoing = (atomic_t*) voidargs;
    atomic_dec(outgoing);
    return 0;
}

static bool __store(struct nvmev_ns *ns, struct nvmev_request *req, 
                    struct nvmev_result *ret, bool internal,
                    bool append) 
{
    struct demand_shard *demand_shards = (struct demand_shard *)ns->ftls;
    struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;

    uint64_t nsecs_latest = 0, nsecs_completed = 0, nsecs_xfer_completed = 0;
    uint64_t credits = 0;
    uint32_t allocated_buf_size;

    uint8_t klen = cmd_key_length(cmd);
    uint32_t vlen = cmd_value_length(cmd);

    vlen += VLEN_MARKER_SZ;

    uint64_t glen;
    uint64_t grain;
    uint64_t page;
    uint64_t g_off;
    uint32_t status = 0;

    uint64_t hash = CityHash64(cmd->kv_store.key, klen);
    struct demand_shard *shard = &demand_shards[hash % SSD_PARTITIONS];
    struct ssdparams *spp = &shard->ssd->sp;
    struct buffer *wbuf = shard->ssd->write_buffer;

    /*
     * Space in memory for the pair. Not necessarily linked to the PPA.
     */
    void* pair_mem;

    if(!shard->fastmode) {
        nsecs_xfer_completed = ssd_advance_write_buffer(shard->ssd, req->nsecs_start, 
                vlen);
        nsecs_latest = nsecs_xfer_completed;
    }

    KEYT key;
    key.key = NULL;
    key.key = cmd->kv_store.key;
    key.len = klen;

    //if(key.key[0] == 'L') {
    //    NVMEV_INFO("Log key write. Bid %llu log num %u vlen %u\n", 
    //                *(uint64_t*) (key.key + 4), 
    //                *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), vlen);
    //}

    uint64_t **oob = shard->oob;
    bool need_new = false;
newpage:
    if(shard->offset == 0 || __crossing_page(spp, shard->offset, vlen)) {
        if(!shard->fastmode) {
            if (last_pg_in_wordline(shard, &cur_page)) {
                struct nand_cmd swr = {
                    .type = USER_IO,
                    .cmd = NAND_WRITE,
                    .interleave_pci_dma = false,
                    .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
                };

                swr.stime = nsecs_xfer_completed;
                swr.ppa = &cur_page;

                nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
                nsecs_latest = max(nsecs_latest, nsecs_completed);

                d_stat.d_write_on_write += spp->pgsz * spp->pgs_per_oneshotpg;
                d_stat.data_w += spp->pgsz * spp->pgs_per_oneshotpg;
                schedule_internal_operation(req->sq_id, nsecs_completed, wbuf,
                        spp->pgs_per_oneshotpg * spp->pgsz);
            }
        }

        if(shard->offset % spp->pgsz) {
            NVMEV_ASSERT(!shard->fastmode);
            uint64_t ppa = ppa2pgidx(shard, &cur_page);
            uint64_t g = shard->offset / GRAINED_UNIT;
            uint64_t g_off = g % GRAIN_PER_PAGE;

            NVMEV_ASSERT(shard->offset % GRAINED_UNIT == 0);

            for(int i = g_off; i < GRAIN_PER_PAGE; i++) {
                oob[ppa][i] = UINT_MAX;
            }

            if(g_off < GRAIN_PER_PAGE) {
                mark_grain_valid(shard, PPA_TO_PGA(ppa, g_off), 
                        GRAIN_PER_PAGE - g_off);
                mark_grain_invalid(shard, PPA_TO_PGA(ppa, g_off), 
                        GRAIN_PER_PAGE - g_off);
            }
        }

again:
        cur_page = get_new_page(shard, USER_IO);
        advance_write_pointer(shard, USER_IO);
        mark_page_valid(shard, &cur_page);

        uint64_t pgidx = ppa2pgidx(shard, &cur_page);
        if(pgidx == 0) {
            mark_grain_valid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
            mark_grain_invalid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
            goto again;
        }

        shard->offset = ((uint64_t) ppa2pgidx(shard, &cur_page)) * spp->pgsz;
        //NVMEV_INFO("1 Set offset to %llu PPA %llu\n", offset, G_IDX(offset / GRAINED_UNIT));
    }

    //NVMEV_ASSERT(key.key);
    //NVMEV_ASSERT(cmd->kv_store.key);
    //NVMEV_ASSERT(cmd);
    //NVMEV_ASSERT(vlen > klen);
    //NVMEV_ASSERT(vlen <= spp->pgsz);
    //NVMEV_ASSERT(klen <= 16);

    //if(key.key[0] == 'L') {
    //    NVMEV_INFO("Log key write. Bid %llu log num %u len %u\n", 
    //                *(uint64_t*) (key.key + 4), 
    //                *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), vlen);
    //}

    glen = vlen / GRAINED_UNIT;
    grain = shard->offset / GRAINED_UNIT;
    page = G_IDX(grain);
    g_off = G_OFFSET(grain);

	if(vlen & (GRAINED_UNIT - 1)) {
		glen++;
	}

    mark_grain_valid(shard, grain, glen);
    credits += glen;

    if(!shard->fastmode) {
        __mark_early(grain, klen, cmd->kv_store.key);
    }

	shard->offset += vlen & ~(GRAINED_UNIT - 1);
    //NVMEV_INFO("2 Set offset to %llu PPA %llu\n", offset, page);

	if(vlen & (GRAINED_UNIT - 1)) {
		shard->offset += GRAINED_UNIT;
	}

    if(need_new == true) {
        //NVMEV_INFO("Going back.\n");
        goto append;
    }

    struct hash_params h; 
    h.hash = hash;
    h.cnt = 0;
    h.find = HASH_KEY_INITIAL;
    h.lpa = 0;

    uint32_t pos = UINT_MAX;
    bool missed = false;
    bool first = false;
    struct pt_struct new_pte;
    new_pte.ppa = grain;

lpa:
    lpa_t lpa = get_lpa(shard->cache, key, &h);
    h.lpa = lpa;

#ifndef GC_STANDARD
    new_pte.lpa = lpa;
#endif

    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cache->get_cmt(cache, lpa);
    struct cache_stat *cstat = &cache->stat;

    //NVMEV_INFO("Got LPA %u IDX %lu\n", lpa, IDX(lpa));

    if(shard->fastmode) {
        goto fm_out;
    }

    if(cmt->t_ppa == UINT_MAX) {
        /*
         * Previously unused cached mapping table entry.
         * Different from the original implementation, we
         * actually give it a page here when we first see it,
         * so we have an area of virt's reserved memory to
         * work with.
         */

skip:
        struct ppa p = get_new_page(shard, MAP_IO);
        ppa_t ppa = ppa2pgidx(shard, &p);

        advance_write_pointer(shard, MAP_IO);
        mark_page_valid(shard, &p);
        mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        if(ppa == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
            goto skip;
        }

        /*
         * The IDX is used during GC so that we know which CMT entry
         * to update.
         */

        cmt->t_ppa = ppa;
        if(!shard->fastmode) {
            cmt->pt = NULL;
        } else {
            cmt->pt = (struct pt_struct*) (nvmev_vdev->ns[0].mapped + 
                                          ((uint64_t) ppa) * spp->pgsz);
        }

        /*
         * Despite the new scheme only using as many mapping entries as needed,
         * instead of a full page each time, we still set the length to the
         * size of a page here for the first ever access. We do this because
         * if we were to set the length on disk to the actual amount of mappings
         * the entry holds initially (1), we would need to add some logic to
         * pack this mapping table entry into an existing page with other
         * mapping table entries that are less than the size of a page right now.
         *
         * That's already being done in the eviction logic later, so we just
         * set its length to the size of a page for now and waste some space
         * until its evicted.
         */

#ifdef GC_STANDARD
        oob[ppa][0] = ((uint64_t) GRAIN_PER_PAGE << 32) | cmt->idx * EPP;
        cmt->len_on_disk = GRAIN_PER_PAGE;
        //for(int i = 1; i < GRAIN_PER_PAGE; i++) {
        //    oob[ppa][i] = 0;
        //}
#else
        oob[ppa][0] = (1LU << 32) | cmt->idx * EPP;
        if(!shard->fastmode) {
            if(ORIG_GLEN < GRAIN_PER_PAGE) {
                mark_grain_invalid(shard, PPA_TO_PGA(ppa, ORIG_GLEN), 
                        GRAIN_PER_PAGE - ORIG_GLEN);
            }

            cmt->len_on_disk = ORIG_GLEN;

            //for(int i = 1; i < ORIG_GLEN; i++) {
            //    oob[ppa][i] = 0;
            //}

            //for(int i = ORIG_GLEN; i < GRAIN_PER_PAGE; i++) {
            //    /*
            //     * We set to UINT_MAX here instead of 0 so that this mapping
            //     * page will be recognized as length 1 if the OOB needs
            //     * checked.
            //     */
            //    oob[ppa][i] = UINT_MAX;
            //}
        } else {
            cmt->len_on_disk = 1;
            cmt->g_off = 0;

            for(int i = 0; i < EPP; i++) {
                cmt->pt[i].lpa = UINT_MAX;
                cmt->pt[i].ppa = UINT_MAX;
            }
        }
#endif

#ifndef GC_STANDARD
        cmt->cached_cnt = 0;
#endif
        /*
         * Will be marked dirty below in update().
         */

        cmt->state = CLEAN;
        first = true;
        //NVMEV_INFO("Assigned a new T PPA %u to IDX %u\n", ppa, cmt->idx);
	}

	if(!shard->fastmode) {
		while(atomic_read(&cmt->outgoing) == 1) {
			cpu_relax();
		}
	}

    if(shard->fastmode) {
        goto fm_out;
    }

cache:
    if(__cache_hit(cache, lpa)) {
        struct pt_struct pte = __lpa_to_pte(shard, cmt, lpa, &pos);
        void* old_mem;

        if(!IS_INITIAL_PPA(pte.ppa)) {
            //NVMEV_INFO("Hit for LPA %u IDX %lu, got grain %u\n", lpa, IDX(lpa), pte.ppa);
            d_stat.d_read_on_write += spp->pgsz;
#ifdef GC_STANDARD
            old_mem = cmt->mems[OFFSET(lpa)];
#else
            old_mem = cmt->mems[pos];
#endif

            if(__read_and_compare(shard, pte.ppa, old_mem, &h, &key, 
                                  nsecs_latest, &nsecs_completed)) {
                nsecs_latest = max(nsecs_latest, nsecs_completed);
                //missed = false;
                pos = UINT_MAX;
                goto lpa;
            }

            //NVMEV_INFO("Overwrite LPA %u PPA %u\n", lpa, pte.ppa);
            nsecs_latest = max(nsecs_latest, nsecs_completed);

            uint64_t old_g_off = G_OFFSET(pte.ppa);
            uint32_t len = __glen_from_oob(oob[G_IDX(pte.ppa)][old_g_off]);
            //while(g_off + len < GRAIN_PER_PAGE && oob[G_IDX(pte.ppa)][g_off + len] == 0) {
            //    len++;
            //}

            //NVMEV_INFO("Got len %u PPA %u\n", len, G_IDX(pte.ppa));

            if(append) {
                /*
                 * We are interested in the real value length this time,
                 * not just the length in grains, so that we can append
                 * within the same grain to save space.
                 */
                uint32_t prev_vlen;
                prev_vlen = __vlen_from_value(old_mem);
                if(prev_vlen + vlen > spp->pgsz) {
                    NVMEV_INFO("Too big.\n");
                    mark_grain_invalid(shard, grain, glen);
                    cmd->kv_store.rsvd = U64_MAX;
                    status = KV_ERR_BUFFER_SMALL;
                    goto out;
                }

                vlen = prev_vlen + vlen;
                if(__crossing_page(spp, shard->offset, vlen)) {
                    NVMEV_INFO("Crossing vlen %u offset %llu.\n", 
                                vlen, shard->offset);
                    mark_grain_invalid(shard, grain, glen);
                    need_new = true;
                    goto newpage;
                }
append:
                if(need_new) {
                    /*
                     * We had to go back up and get a new page. The offset
                     * and grain have been reset. vlen was also set to the
                     * new vlen a few lines above, and its grains were marked
                     * valid after we got the new page.
                     */
                    NVMEV_INFO("Back here new PPA %llu.\n", page);
                    new_pte.ppa = grain;
                } else {
                    /*
                     * We didn't need to get a new page, just add
                     * to the offset. Mark the remaining grains
                     * (length of the original pair) valid. Grains
                     * representing the additional value length were marked
                     * valid at the beginning of the function.
                     */

                    uint64_t new_glen = vlen / GRAINED_UNIT;
                    if(vlen % GRAINED_UNIT) {
                        new_glen++;
                    }

                    NVMEV_INFO("vlen %u glen %llu new_glen %llu\n",
                                vlen, glen, new_glen);
                    if(new_glen > glen) {
                        mark_grain_valid(shard, grain + glen, new_glen - glen);
                        shard->offset += (new_glen - glen) * GRAINED_UNIT;
                        glen = new_glen;

                        NVMEV_INFO("Set new glen to %llu\n", glen);
                    }

                    NVMEV_ASSERT((shard->offset / spp->pgsz) == page);
                    //NVMEV_INFO("Append glen now %llu\n", glen);
                }

                //NVMEV_INFO("Offset %llu\n", offset);

                /*
                 * The original location of the KV pair will be used
                 * in do_perform_io_kv later for the read phase
                 * of the append.
                 *
                 * This is awkward, but we need some way to put over 32
                 * bits of information into the command outside of the
                 * original rsvd field we use.
                 */
                uint64_t off = (uint64_t) pte.ppa * GRAINED_UNIT;
                uint32_t b_len = prev_vlen;

                cmd->kv_append.nsid = (uint32_t) (off & 0xFFFFFFFF); 
                cmd->kv_append.rsvd2 = (uint32_t) (off >> 32);
                cmd->kv_append.offset = b_len;
                NVMEV_INFO("Set original append vlen to %u\n", b_len);
            } else if(len == glen) {
                uint8_t *ptr = ((uint8_t*) old_mem) + 1;
                cmd->kv_store.rsvd = (uint64_t) old_mem;
                pair_mem = old_mem;
            } else if(len != glen) {
                NVMEV_ASSERT(false);
            }

            NVMEV_ASSERT(len > 0);
            //NVMEV_INFO("OW\n");
            mark_grain_invalid(shard, pte.ppa, len);
        } else {
            pair_mem = kzalloc(vlen, GFP_KERNEL);
            NVMEV_ASSERT(pair_mem);
            if(append) {
                /*
                 * Set offset to 0 here to indicate that we don't need to copy
                 * a previously existing KV pair, because this is an insert.
                 */
                cmd->kv_append.offset = 0;
                cmd->kv_append.rsvd = (uint64_t) pair_mem;
            } else {
                cmd->kv_store.rsvd = (uint64_t) pair_mem;
            }
        }

        if(!missed) {
            cstat->cache_hit++;
        }

        shard->cache->touch(shard->cache, lpa);
    } else if(cmt->t_ppa != UINT_MAX) {
        //NVMEV_INFO("Miss for LPA %u IDX %lu\n", lpa, IDX(lpa));

        if (cgo_is_full(cache)) {
            nsecs_completed = __evict_one(shard, req, nsecs_latest, &credits);
            nsecs_latest = max(nsecs_latest, nsecs_completed);
            d_stat.t_write_on_write += spp->pgsz;
        }

        nsecs_completed = __get_one(shard, cmt, first, nsecs_latest, &missed);
        nsecs_latest = max(nsecs_latest, nsecs_completed);

        d_stat.t_read_on_write += spp->pgsz;
        goto cache;
    }

    oob[page][g_off] = ((uint64_t) glen << 32) | lpa;
    //for(int i = 1; i < glen; i++) {
    //    oob[page][g_off + i] = 0;
    //}

    shard->ftl->max_try = (h.cnt > shard->ftl->max_try) ? h.cnt : 
                           shard->ftl->max_try;

    __update_map(shard, cmt, lpa, pair_mem, new_pte, pos);
    if (cgo_is_full(cache)) {
        nsecs_completed = __evict_one(shard, req, nsecs_latest, &credits);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        d_stat.t_write_on_write += spp->pgsz;
    }

out:
    d_stat.write_req_cnt++;

    consume_write_credit(shard, credits);
    check_and_refill_write_credit(shard);
    nsecs_latest = max(nsecs_latest, nsecs_completed);

    //NVMEV_INFO("%s for key %llu (%llu) klen %u vlen %u grain %llu PPA %llu LPA %u\n",
    //            append ? "Append" : "Write",
    //            *(uint64_t*) (key.key), *(uint64_t*) &(cmd->kv_store.key),
    //            klen, vlen, grain, page, lpa);

    if(!shard->fastmode) {
		atomic_inc(&cmt->outgoing);
        ret->cb = __release_map;
        ret->args = &cmt->outgoing;
        ret->nsecs_target = nsecs_latest;
    } else {
fm_out:
        void* from = (void*) cmd->kv_store.dptr.prp1;
        uint64_t to = cmd->kv_store.rsvd;

        /*
         * Haven't updated fastmode for new OOB len scheme.
         */
        NVMEV_ASSERT(false);

        oob[page][g_off] = lpa;
        for(int i = 1; i < glen; i++) {
            oob[page][g_off + i] = 0;
        }

        memcpy(nvmev_vdev->ns[0].mapped + to, from, glen * GRAINED_UNIT);
        memmove(nvmev_vdev->ns[0].mapped + to + 
                sizeof(uint8_t) + klen + sizeof(uint32_t),
                nvmev_vdev->ns[0].mapped + to + 
                sizeof(uint8_t) + klen, vlen - sizeof(uint8_t) - klen);
        memcpy(nvmev_vdev->ns[0].mapped + to + 
                sizeof(uint8_t) + klen,
                &vlen, sizeof(uint32_t));
    }

    ret->status = status;
    return true;
}

static bool conv_write(struct nvmev_ns *ns, struct nvmev_request *req, 
                       struct nvmev_result *ret, bool internal)
{
    return __store(ns, req, ret, false, false);
}

static bool conv_append(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
    return __store(ns, req, ret, false, true);
}

/*
 * A batch buffer be structured as follows :
 * (uint8_t) key length (N) key bytes (uint32_t) value length (N) value <- KV pair 1
 * (uint8_t) key length (N) key bytes (uint32_t) value length (N) value <- KV pair 2
 *
 * For example :
 *
 * char* key = "key1"
 * char* value = "..."
 *
 * uint8_t key_length = strlen(key); 
 * uint32_t vlen = 1024;
 * uint32_t offset = 0;
 *
 * memcpy(buffer + offset, &key_length, sizeof(key_length));
 * memcpy(buffer + offset + sizeof(key_length), key, key_length);
 * memcpy(buffer + offset + sizeof(key_length) + key_length, &vlen, sizeof(vlen));
 * memcpy(buffer + offset + sizeof(key_length) + key_length + vlen, value, vlen);
 * 
 * offset += sizeof(key_length) + key_length + sizeof(vlen) + vlen;
 *
 * key = "key2"
 * value = "..."
 *
 * key_length = strlen(key);
 * vlen = 1024;
 *
 * memcpy(buffer + offset, &key_length, sizeof(key_length));
 * memcpy(buffer + offset + sizeof(key_length), key, key_length);
 * memcpy(buffer + offset + sizeof(key_length) + key_length, &vlen, sizeof(vlen));
 * memcpy(buffer + offset + sizeof(key_length) + key_length + vlen, value, vlen);
 *
 * And so on.
 *
 * The value length sent to the KVSSD is the value length of the entire buffer.
 */

static bool conv_batch(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
    struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;

    uint8_t klen = 0;
    uint32_t offset = 0, vlen = 0;
    uint32_t remaining = cmd_value_length(cmd);
    KEYT key;

    /*
     * Not updated for new design yet.
     */

    BUG_ON(true);

    char* value = (char*) kzalloc(remaining, GFP_KERNEL);
    //__quick_copy(cmd, value, remaining);
    
    while(remaining > 0) {
        klen = *(uint8_t*) value + offset;
        offset += sizeof(klen);
        
        key.key = (char*)kzalloc(klen + 1, GFP_KERNEL);     
        memcpy(key.key, value + offset, klen);
    }

    return true;
}

static void conv_flush(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	uint64_t start, latest;
	uint32_t i;
	struct demand_shard *demand_shards = (struct demand_shard *)ns->ftls;

	start = local_clock();
	latest = start;
	for (i = 0; i < ns->nr_parts; i++) {
		latest = max(latest, ssd_next_idle_time(demand_shards[i].ssd));
	}

	NVMEV_DEBUG_VERBOSE("%s: latency=%u\n", __func__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
	return;
}

bool kv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;

    switch (cmd->common.opcode) {
        case nvme_cmd_kv_batch:
            NVMEV_ASSERT(false);
            break;
        case nvme_cmd_kv_store:
            ret->nsecs_target = conv_write(ns, req, ret, false);
            break;
        case nvme_cmd_kv_retrieve:
            ret->nsecs_target = conv_read(ns, req, ret);
            break;
        case nvme_cmd_kv_delete:
            ret->nsecs_target = conv_delete(ns, req, ret);
            break;
        case nvme_cmd_kv_append:
            ret->nsecs_target = conv_append(ns, req, ret);
            break;
        case nvme_cmd_write:
        case nvme_cmd_read:
        case nvme_cmd_flush:
            ret->nsecs_target = __get_wallclock() + 10;
            break;
        default:
            ret->nsecs_target = __get_wallclock() + 10;
            break;
    }

    return true;
}

bool conv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;

	NVMEV_ASSERT(ns->csi == NVME_CSI_NVM);

	switch (cmd->common.opcode) {
	case nvme_cmd_write:
		if (!conv_write(ns, req, ret, false))
			return false;
		break;
	case nvme_cmd_read:
		if (!conv_read(ns, req, ret))
			return false;
		break;
	case nvme_cmd_flush:
		conv_flush(ns, req, ret);
		break;
	default:
		NVMEV_ERROR("%s: command not implemented: %s (0x%x)\n", __func__,
				nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
		break;
	}

	return true;
}
