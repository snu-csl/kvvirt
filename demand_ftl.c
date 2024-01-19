// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/random.h>
#include <linux/sched/clock.h>
#include <linux/sort.h>
#include <linux/xarray.h>

#include "nvmev.h"
#include "demand_ftl.h"

#include "demand/cache.h"
#include "demand/coarse.h"
#include "demand/d_param.h"
#include "demand/demand.h"
#include "demand/utility.h"

DEFINE_HASHTABLE(mapping_ht, 20);

void schedule_internal_operation(int sqid, unsigned long long nsecs_target,
				 struct buffer *write_buffer, unsigned int buffs_to_release);

bool kv_identify_nvme_io_cmd(struct nvmev_ns *ns, struct nvme_command cmd)
{
	return is_kv_cmd(cmd.common.opcode);
}

static unsigned int cmd_key_length(struct nvme_kv_command cmd)
{
	if (cmd.common.opcode == nvme_cmd_kv_store) {
		return cmd.kv_store.key_len + 1;
	} else if (cmd.common.opcode == nvme_cmd_kv_retrieve) {
		return cmd.kv_retrieve.key_len + 1;
	} else if (cmd.common.opcode == nvme_cmd_kv_delete) {
		return cmd.kv_delete.key_len + 1;
	} else {
		return cmd.kv_store.key_len + 1;
	}
}

static unsigned int cmd_value_length(struct nvme_kv_command cmd)
{
	if (cmd.common.opcode == nvme_cmd_kv_store) {
		return cmd.kv_store.value_len << 2;
	} else if (cmd.common.opcode == nvme_cmd_kv_retrieve) {
		return cmd.kv_retrieve.value_len << 2;
	} else {
		return cmd.kv_store.value_len << 2;
	}
}

inline bool last_pg_in_wordline(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	return (ppa->g.pg % spp->pgs_per_oneshotpg) == (spp->pgs_per_oneshotpg - 1);
}

static bool should_gc(struct conv_ftl *conv_ftl)
{
	return (conv_ftl->lm.free_line_cnt <= conv_ftl->cp.gc_thres_lines);
}

static inline bool should_gc_high(struct conv_ftl *conv_ftl)
{
    NVMEV_DEBUG("Free LC %d:\n", conv_ftl->lm.free_line_cnt);

    //if(conv_ftl->lm.free_line_cnt <= 3) {
    //    struct list_head *p;
    //    struct line *my;
    //    list_for_each(p, &conv_ftl->lm.free_line_list) {
    //        /* my points to the structure in which the list is embedded */
    //        my = list_entry(p, struct line, entry);
    //        NVMEV_ERROR("%d\n", my->id);
    //    }
    //}

	return conv_ftl->lm.free_line_cnt <= conv_ftl->cp.gc_thres_lines_high;
}

static inline struct ppa get_maptbl_ent(struct conv_ftl *conv_ftl, uint64_t lpn)
{
	return conv_ftl->maptbl[lpn];
}

static inline void set_maptbl_ent(struct conv_ftl *conv_ftl, uint64_t lpn, struct ppa *ppa)
{
	NVMEV_ASSERT(lpn < conv_ftl->ssd->sp.tt_pgs);
	conv_ftl->maptbl[lpn] = *ppa;
}

uint64_t ppa2line(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
    struct line* l = get_line(ftl, ppa); 
    return l->id;
}

ppa_t ppa2pgidx(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	ppa_t pgidx;

	NVMEV_DEBUG_VERBOSE("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", __func__,
			ppa->g.ch, ppa->g.lun, ppa->g.pl, ppa->g.blk, ppa->g.pg);

	pgidx = ppa->g.ch * spp->pgs_per_ch + ppa->g.lun * spp->pgs_per_lun +
		ppa->g.pl * spp->pgs_per_pl + ppa->g.blk * spp->pgs_per_blk + ppa->g.pg;

	NVMEV_ASSERT(pgidx < spp->tt_pgs);

	return pgidx;
}

static inline uint64_t get_rmap_ent(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	uint64_t pgidx = ppa2pgidx(conv_ftl, ppa);

	return conv_ftl->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct conv_ftl *conv_ftl, uint64_t lpn, struct ppa *ppa)
{
	uint64_t pgidx = ppa2pgidx(conv_ftl, ppa);

	conv_ftl->rmap[pgidx] = lpn;
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

void consume_write_credit(struct conv_ftl *conv_ftl, uint32_t len)
{
	conv_ftl->wfc.write_credits -= len;
    NVMEV_DEBUG("Consuming %u credits. %d remaining.\n", len,
                 conv_ftl->wfc.write_credits);
}

static uint64_t forground_gc(struct conv_ftl *conv_ftl);
inline uint64_t check_and_refill_write_credit(struct conv_ftl *conv_ftl)
{
	struct write_flow_control *wfc = &(conv_ftl->wfc);
    uint64_t nsecs_completed = 0;

	if ((int32_t) wfc->write_credits <= (int32_t) 0) {
		forground_gc(conv_ftl);
		wfc->write_credits += wfc->credits_to_refill;
	} 

    return nsecs_completed;
}

static void init_lines(struct conv_ftl *conv_ftl)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
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

static void remove_lines(struct conv_ftl *conv_ftl)
{
	pqueue_free(conv_ftl->lm.victim_line_pq);
	vfree(conv_ftl->lm.lines);
}

static void init_write_flow_control(struct conv_ftl *conv_ftl)
{
	struct write_flow_control *wfc = &(conv_ftl->wfc);
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	wfc->write_credits = spp->pgs_per_line * GRAIN_PER_PAGE;
	wfc->credits_to_refill = spp->pgs_per_line * GRAIN_PER_PAGE;
}

static void alloc_gc_mem(struct conv_ftl *conv_ftl) {
    struct gc_data *gcd = &conv_ftl->gcd;
    struct ssdparams *spp = &conv_ftl->ssd->sp;

    gcd->offset = GRAIN_PER_PAGE;
    gcd->last = false;
    xa_init(&gcd->inv_mapping_xa);
}

static void free_gc_mem(struct conv_ftl *conv_ftl) {
    struct gc_data *gcd = &conv_ftl->gcd;
    struct ssdparams *spp = &conv_ftl->ssd->sp;

    gcd->offset = GRAIN_PER_PAGE;
    gcd->last = false;
    xa_destroy(&gcd->inv_mapping_xa);
}

static inline void check_addr(int a, int max)
{
	NVMEV_ASSERT(a >= 0 && a < max);
}

static struct line *get_next_free_line(struct conv_ftl *conv_ftl)
{
	struct line_mgmt *lm = &conv_ftl->lm;
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

static struct write_pointer *__get_wp(struct conv_ftl *ftl, uint32_t io_type)
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

static void prepare_write_pointer(struct conv_ftl *conv_ftl, uint32_t io_type)
{
	struct write_pointer *wp = __get_wp(conv_ftl, io_type);
	struct line *curline = get_next_free_line(conv_ftl);

    if(io_type == MAP_IO || io_type == GC_MAP_IO) {
        curline->map = true;
    } else {
        curline->map = false;
    }

	NVMEV_ASSERT(wp);
	NVMEV_ASSERT(curline);

    //printk("Giving line %d to %u\n", curline->id, io_type);

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
bool advance_write_pointer(struct conv_ftl *conv_ftl, uint32_t io_type)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct write_pointer *wpp = __get_wp(conv_ftl, io_type);

	NVMEV_DEBUG("current wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n",
			wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg);

    if(io_type == USER_IO) {
        user_pgs_this_gc++;
        pgs_this_flush++;
    } else if(io_type == GC_IO ) {
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

    struct nand_block *blk = get_blk(conv_ftl->ssd, &p);
    
    //if(wpp->blk == prev_blk && blk->vgc == prev_vgc) {
    //    printk("Caller is %pS\n", __builtin_return_address(0));
    //    printk("Caller is %pS\n", __builtin_return_address(1));
    //    printk("Caller is %pS\n", __builtin_return_address(2));
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
	wpp->curline = get_next_free_line(conv_ftl);

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

struct ppa get_new_page(struct conv_ftl *conv_ftl, uint32_t io_type)
{
	struct ppa ppa;
	struct write_pointer *wp = __get_wp(conv_ftl, io_type);

	ppa.ppa = 0;
	ppa.g.ch = wp->ch;
	ppa.g.lun = wp->lun;
	ppa.g.pg = wp->pg;
	ppa.g.blk = wp->blk;
	ppa.g.pl = wp->pl;

	NVMEV_ASSERT(ppa.g.pl == 0);
    NVMEV_ASSERT(oob_empty(ppa2pgidx(conv_ftl, &ppa)));

	return ppa;
}

static void init_maptbl(struct conv_ftl *conv_ftl)
{
	int i;
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	conv_ftl->maptbl = vmalloc(sizeof(struct ppa) * spp->tt_pgs);
	for (i = 0; i < spp->tt_pgs; i++) {
		conv_ftl->maptbl[i].ppa = UNMAPPED_PPA;
	}
}

static void remove_maptbl(struct conv_ftl *conv_ftl)
{
	vfree(conv_ftl->maptbl);
}

static void init_rmap(struct conv_ftl *conv_ftl)
{
	int i;
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	conv_ftl->rmap = vmalloc(sizeof(uint64_t) * spp->tt_pgs);
	for (i = 0; i < spp->tt_pgs; i++) {
		conv_ftl->rmap[i] = INVALID_LPN;
	}
}

static void remove_rmap(struct conv_ftl *conv_ftl)
{
	vfree(conv_ftl->rmap);
}

static void conv_init_ftl(struct conv_ftl *conv_ftl, struct convparams *cpp, struct ssd *ssd)
{
	/*copy convparams*/
	conv_ftl->cp = *cpp;

	conv_ftl->ssd = ssd;

	/* initialize maptbl */
	init_maptbl(conv_ftl); // mapping table

	/* initialize rmap */
	init_rmap(conv_ftl); // reverse mapping table (?)

	/* initialize all the lines */
	init_lines(conv_ftl);

	/* initialize write pointer, this is how we allocate new pages for writes */
	prepare_write_pointer(conv_ftl, USER_IO);
    prepare_write_pointer(conv_ftl, MAP_IO);
    prepare_write_pointer(conv_ftl, GC_MAP_IO);
	prepare_write_pointer(conv_ftl, GC_IO);

	init_write_flow_control(conv_ftl);

	NVMEV_INFO("Init FTL instance with %d channels (%ld pages)\n", conv_ftl->ssd->sp.nchs,
		        conv_ftl->ssd->sp.tt_pgs);

	return;
}

static void conv_remove_ftl(struct conv_ftl *conv_ftl)
{
	//remove_lines(conv_ftl);
	remove_rmap(conv_ftl);
	remove_maptbl(conv_ftl);
}

static void conv_init_params(struct convparams *cpp)
{
	cpp->op_area_pcent = OP_AREA_PERCENT;
	cpp->gc_thres_lines = 8; /* (host write, gc, map, map gc)*/
	cpp->gc_thres_lines_high = 8; /* (host write, gc, map, map gc)*/
	cpp->enable_gc_delay = 1;
	cpp->pba_pcent = (int)((1 + cpp->op_area_pcent) * 100);
}

extern struct algorithm __demand;
extern struct lower_info virt_info;
extern struct blockmanager pt_bm;

char** wb_bufs;
uint64_t wb_off;
char **inv_mapping_bufs;
uint64_t *inv_mapping_offs;

void demand_init(uint64_t size, struct ssd* ssd) 
{
    struct ssdparams *spp = &ssd->sp;
    spp->nr_segs = size / (_PPS * PAGESIZE);

    virt_info.NOB = spp->tt_blks;
    virt_info.NOP = spp->tt_pgs;
    virt_info.SOB = spp->pgs_per_blk * spp->secsz * spp->secs_per_pg;
    virt_info.SOP = spp->pgsz;
    virt_info.PPB = spp->pgs_per_blk;
    virt_info.PPS = spp->pgs_per_blk * BPS;
    virt_info.TS = size;
    virt_info.DEV_SIZE = size;
    virt_info.all_pages_in_dev = size / PAGESIZE;

    virt_info.create(&virt_info, &pt_bm);

    uint64_t tt_grains = spp->tt_pgs * GRAIN_PER_PAGE; 

    spp->tt_map_pgs = tt_grains / EPP;
    spp->tt_data_pgs = spp->tt_pgs - spp->tt_map_pgs;

#ifndef GC_STANDARD
    pg_inv_cnt = (uint64_t*) vmalloc(spp->tt_pgs * sizeof(uint64_t));
    pg_v_cnt = (uint64_t*) vmalloc(spp->tt_pgs * sizeof(uint64_t));
    NVMEV_ASSERT(pg_inv_cnt);
    NVMEV_ASSERT(pg_v_cnt);
    memset(pg_inv_cnt, 0x0, spp->tt_pgs * sizeof(uint64_t));
    memset(pg_v_cnt, 0x0, spp->tt_pgs * sizeof(uint64_t));

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

    printk("tt_grains %llu tt_map %lu tt_data %lu tt_lines %lu "
            "invalid_per_line %llu inv_ppl %llu\n", 
            tt_grains, spp->tt_map_pgs, spp->tt_data_pgs, spp->tt_lines,
            inv_per_line, inv_ppl);
#endif

    printk("tt_lines %lu\n", spp->tt_lines);

    /*
     * OOB stores LPA to grain information.
     */

    oob = (uint64_t**)vmalloc((spp->tt_pgs * sizeof(uint64_t*)));

    for(int i = 0; i < spp->tt_pgs; i++) {
        oob[i] = 
        (uint64_t*)kzalloc(GRAIN_PER_PAGE * sizeof(uint64_t), GFP_KERNEL);
        for(int j = 0; j < GRAIN_PER_PAGE; j++) {
            oob[i][j] = 2;
        }
    }

    wb_off = 0;
    wb_bufs = (char**) kzalloc(sizeof(char*) * MAX_WRITE_BUF, GFP_KERNEL);
    for(int i = 0; i < MAX_WRITE_BUF; i++) {
        wb_bufs[i] = (char*) kzalloc(spp->pgsz, GFP_KERNEL);
    }

    int temp[PARTNUM];
    temp[MAP_S] = spp->tt_map_pgs;
    temp[DATA_S] = spp->tt_data_pgs;
    pt_bm.pt_create(&pt_bm, PARTNUM, temp, &virt_info);

    demand_create(&virt_info, &pt_bm, &__demand, ssd, size);
    print_demand_stat(&d_stat);
}

void demand_free(struct conv_ftl *conv_ftl) {
    struct ssdparams *spp = &conv_ftl->ssd->sp;

#ifndef GC_STANDARD
    vfree(pg_inv_cnt);
    vfree(pg_v_cnt);

    for(int i = 0; i < spp->tt_lines; i++) {
        vfree(inv_mapping_bufs[i]);
    }

    kfree(inv_mapping_bufs);
    kfree(inv_mapping_offs);
#endif

    for(int i = 0; i < spp->tt_pgs; i++) {
        kfree(oob[i]); 
    }

    vfree(oob);
}

static bool conv_write(struct nvmev_ns*, struct nvmev_request*, 
                       struct nvmev_result*, bool);
uint64_t dsize = 0;
void conv_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
                         uint32_t cpu_nr_dispatcher)
{
	struct ssdparams spp;
	struct convparams cpp;
	struct conv_ftl *conv_ftls;
	struct ssd *ssd;
	uint32_t i;
	const uint32_t nr_parts = SSD_PARTITIONS;

    dsize = size;

	ssd_init_params(&spp, size, nr_parts);
	conv_init_params(&cpp);

	conv_ftls = kmalloc(sizeof(struct conv_ftl) * nr_parts, GFP_KERNEL);

	for (i = 0; i < nr_parts; i++) {
		ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
		ssd_init(ssd, &spp, cpu_nr_dispatcher);
		conv_init_ftl(&conv_ftls[i], &cpp, ssd);
	}
    
    nvmev_vdev->space_used = 0;
    ftl = &conv_ftls[0];

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		kfree(conv_ftls[i].ssd->pcie->perf_model);
		kfree(conv_ftls[i].ssd->pcie);
		kfree(conv_ftls[i].ssd->write_buffer);

		conv_ftls[i].ssd->pcie = conv_ftls[0].ssd->pcie;
		conv_ftls[i].ssd->write_buffer = conv_ftls[0].ssd->write_buffer;
	}

    demand_init(dsize, conv_ftls[0].ssd);

    /* for storing invalid mappings during GC */
    alloc_gc_mem(&conv_ftls[0]);

	ns->id = id;
	ns->csi = NVME_CSI_NVM;
	ns->nr_parts = nr_parts;
	ns->ftls = (void *)conv_ftls;
	ns->size = (uint64_t)((size * 100) / cpp.pba_pcent);
	ns->mapped = mapped_addr;
	/*register io command handler*/
    ns->proc_io_cmd = kv_proc_nvme_io_cmd;
	ns->identify_io_cmd = kv_identify_nvme_io_cmd;

	NVMEV_INFO("FTL physical space: %lld, logical space: %lld (physical/logical * 100 = %d)\n",
		        size, ns->size, cpp.pba_pcent);

	return;
}

void demand_warmup(struct nvmev_ns* ns) {
    uint8_t klen = 16;
    uint16_t vlen = 1024;
    struct nvmev_request req;
    struct nvme_kv_command cmd;
    struct nvmev_result res;

    cmd.common.opcode = nvme_cmd_kv_store;
    cmd.kv_store.key_len = klen - 1;
    cmd.kv_store.value_len = vlen >> 2;
    req.cmd = (struct nvme_command*) &cmd;

    for(int i = 0; i < 1000000; i++) {
        char* value = kzalloc(vlen, GFP_KERNEL);
        cmd.kv_store.dptr.prp1 = (uint64_t) value;
        sprintf(cmd.kv_store.key, "%0*d", klen, i);
        memcpy(value, &klen, sizeof(klen));
        memcpy(value + sizeof(klen), cmd.kv_store.key, klen);
        //NVMEV_INFO("Internal population for key %s\n", cmd.kv_store.key);
        conv_write(ns, &req, &res, true);
    }
}

void conv_remove_namespace(struct nvmev_ns *ns)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	const uint32_t nr_parts = SSD_PARTITIONS;
	uint32_t i;

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		/*
		 * These were freed from conv_init_namespace() already.
		 * Mark these NULL so that ssd_remove() skips it.
		 */
		conv_ftls[i].ssd->pcie = NULL;
		conv_ftls[i].ssd->write_buffer = NULL;
	}

    free_gc_mem(&conv_ftls[0]);
    demand_free(&conv_ftls[0]);
    __demand.destroy(&virt_info, &__demand);

	for (i = 0; i < nr_parts; i++) {
		conv_remove_ftl(&conv_ftls[i]);
		ssd_remove(conv_ftls[i].ssd);
		kfree(conv_ftls[i].ssd);
	}

	kfree(conv_ftls);
	ns->ftls = NULL;
}

static inline bool valid_ppa(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
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

static inline bool valid_lpn(struct conv_ftl *conv_ftl, uint64_t lpn)
{
	return (lpn < conv_ftl->ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
	return !(ppa->ppa == UNMAPPED_PPA);
}

inline struct line *get_line(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	return &(conv_ftl->lm.lines[ppa->g.blk]);
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
void mark_page_invalid(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct line *line;

    NVMEV_DEBUG("Marking PPA %u invalid\n", ppa2pgidx(conv_ftl, ppa));

	/* update corresponding page status */
	pg = get_pg(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);
	pg->status = PG_INVALID;

#ifndef GC_STANDARD
    NVMEV_ASSERT(pg_inv_cnt[ppa2pgidx(conv_ftl, ppa)] == spp->pgsz);
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

    //printk("%s: For PPA %u we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

/*
 * Only to be called after mark_page_valid.
 */

void mark_grain_valid(struct conv_ftl *conv_ftl, uint64_t grain, uint32_t len) {
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

    uint64_t page = G_IDX(grain);
    struct ppa ppa = ppa_to_struct(spp, page);

	/* update page status */
	pg = get_pg(conv_ftl->ssd, &ppa);

    if(pg->status != PG_VALID) {
        NVMEV_ERROR("Page %u was %d\n", page, pg->status);
    }

	NVMEV_ASSERT(pg->status == PG_VALID);

	/* update corresponding block status */
	blk = get_blk(conv_ftl->ssd, &ppa);
	//NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    NVMEV_ASSERT(blk->vgc >= 0 && blk->vgc <= spp->pgs_per_blk * GRAIN_PER_PAGE);
    blk->vgc += len;

	/* update corresponding line status */
	line = get_line(conv_ftl, &ppa);
	//NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    NVMEV_ASSERT(line->vgc >= 0 && line->vgc <= spp->pgs_per_line * GRAIN_PER_PAGE);
    line->vgc += len;

    NVMEV_INFO("Marking grain %llu length %u in PPA %llu line %d valid\n", 
            grain, len, page, line->id);

#ifdef GC_STANDARD
    /*
     * We leave the grains after the first grain as zero here,
     * so that during GC we can figure out the length of the KV pairs
     * by iterating over them.
     *
     * A: 1 0 0 0 B: 1 ... -> A is length 4.
     */

    NVMEV_ASSERT(grain_bitmap[grain] != 1);
    grain_bitmap[grain] = 1;
#endif

    //NVMEV_ASSERT(pg_v_cnt[page] + (len * GRAINED_UNIT) <= spp->pgsz);
    //pg_v_cnt[page] += len * GRAINED_UNIT;
}

#ifdef GC_STANDARD
bool page_grains_invalid(uint64_t ppa) {
    uint64_t page = ppa;
    uint64_t offset = page * GRAIN_PER_PAGE;

    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        if(grain_bitmap[offset + i] == 1) {
            NVMEV_DEBUG("Grain %u PPA %u was valid\n",
                    offset + i, page);
            return false;
        }
    }

    NVMEV_DEBUG("All grains invalid PPA %u (%u)\n", page, offset);
    return true;
}
#endif

void mark_grain_invalid(struct conv_ftl *conv_ftl, uint64_t grain, uint32_t len) {
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct line *line;

    uint64_t page = G_IDX(grain);
    struct ppa ppa = ppa_to_struct(spp, page);

	/* update corresponding page status */
	pg = get_pg(conv_ftl->ssd, &ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);

	/* update corresponding block status */
	blk = get_blk(conv_ftl->ssd, &ppa);
	//NVMEV_ASSERT(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
	//NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    
    if(blk->igc >= spp->pgs_per_blk * GRAIN_PER_PAGE) {
        NVMEV_INFO("IGC PPA %u was %d\n", ppa2pgidx(conv_ftl, &ppa), blk->igc);
    }
    
    NVMEV_ASSERT(blk->igc < spp->pgs_per_blk * GRAIN_PER_PAGE);
    NVMEV_ASSERT(blk->vgc > 0 && blk->vgc <= spp->pgs_per_blk * GRAIN_PER_PAGE);
    blk->igc += len;

	/* update corresponding line status */
	line = get_line(conv_ftl, &ppa);

    NVMEV_INFO("Marking grain %llu length %u in PPA %u line %d invalid\n", 
            grain, len, ppa2pgidx(conv_ftl, &ppa), line->id);

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
      NVMEV_DEBUG("Line %d's VGC was %u\n", line->id, line->vgc);
    }
    NVMEV_ASSERT(line->vgc >= 0 && line->vgc <= spp->pgs_per_line * GRAIN_PER_PAGE);

    //if(grain_bitmap[grain] == 0) {
    //    printk("Caller is %pS\n", __builtin_return_address(0));
    //    printk("Caller is %pS\n", __builtin_return_address(1));
    //    printk("Caller is %pS\n", __builtin_return_address(2));
    //}

#ifdef GC_STANDARD
    NVMEV_ASSERT(grain_bitmap[grain] != 0);
    grain_bitmap[grain] = 0;

    if(page_grains_invalid(page)) {
        mark_page_invalid(conv_ftl, &ppa);
    }
#else
    if(pg_inv_cnt[page] + (len * GRAINED_UNIT) > spp->pgsz) {
        NVMEV_DEBUG("inv_cnt was %u PPA %u (tried to add %u)\n", 
                    pg_inv_cnt[page], page, len);
        printk("Caller is %pS\n", __builtin_return_address(0));
        printk("Caller is %pS\n", __builtin_return_address(1));
        printk("Caller is %pS\n", __builtin_return_address(2));
        BUG_ON(true);
    }

    NVMEV_ASSERT(pg_inv_cnt[page] + (len * GRAINED_UNIT) <= spp->pgsz);
    pg_inv_cnt[page] += len * GRAINED_UNIT;

    NVMEV_DEBUG("inv_cnt for %u is %u\n", page, pg_inv_cnt[page]);

    if(pg_inv_cnt[page] == spp->pgsz) {
        if(pg_inv_cnt[page] != spp->pgsz) {
            NVMEV_ERROR("inv was %u v was %u ppa %u\n", 
                         pg_inv_cnt[page], pg_v_cnt[page], page);
        }

        //NVMEV_ASSERT(pg_v_cnt[page] == 0);
        NVMEV_ASSERT(pg_inv_cnt[page] == spp->pgsz);
        mark_page_invalid(conv_ftl, &ppa);
    }
#endif
}

void mark_page_valid(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

    NVMEV_DEBUG("Marking PPA %u valid\n", ppa2pgidx(conv_ftl, ppa));

	/* update page status */
	pg = get_pg(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_FREE);
	pg->status = PG_VALID;

	///* update corresponding block status */
	//blk = get_blk(conv_ftl->ssd, ppa);
	//NVMEV_ASSERT(blk->vpc >= 0 && blk->vpc < spp->pgs_per_blk);
	//blk->vpc++;

	///* update corresponding line status */
	//line = get_line(conv_ftl, ppa);
	//NVMEV_ASSERT(line->vpc >= 0 && line->vpc < spp->pgs_per_line);
	//line->vpc++;
}

bool __invalid_mapping_ppa(struct conv_ftl *conv_ftl, uint64_t ppa, 
                           unsigned long key) {
    struct ssdparams *spp = &conv_ftl->ssd->sp;
    struct gc_data *gcd = &conv_ftl->gcd;

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

static void mark_block_free(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = get_blk(conv_ftl->ssd, ppa);
	struct nand_page *pg = NULL;
    struct ppa ppa_copy;
    uint64_t page;
	int i;

    ppa_copy = *ppa;
    ppa_copy.g.pg = 0;
    page = ppa2pgidx(conv_ftl, &ppa_copy);

	for (i = 0; i < spp->pgs_per_blk; i++) {
		/* reset page status */
		pg = &blk->pg[i];
		NVMEV_ASSERT(pg->nsecs == spp->secs_per_pg);
        NVMEV_DEBUG("Marking PPA %u free\n", ppa2pgidx(conv_ftl, &ppa_copy) + i);
		pg->status = PG_FREE;

#ifndef GC_STANDARD
        //if(pg_inv_cnt[ppa2pgidx(conv_ftl, &ppa_copy) + i] == 0) {
        //    NVMEV_DEBUG("pg was %u %s\n", ppa2pgidx(conv_ftl, &ppa_copy) + i,
        //            __invalid_mapping_ppa(conv_ftl, page) ? "MAPPING PPA" : "NOT MAPPING PPA");
        //}

        NVMEV_ASSERT(pg_inv_cnt[ppa2pgidx(conv_ftl, &ppa_copy) + i] > 0);
        pg_inv_cnt[ppa2pgidx(conv_ftl, &ppa_copy) + i] = 0;
#endif
        clear_oob(ppa2pgidx(conv_ftl, &ppa_copy) + i);
	}

	/* reset block status */
	NVMEV_ASSERT(blk->npgs == spp->pgs_per_blk);
	blk->ipc = 0;
	blk->vpc = 0;
    blk->igc = 0;
    blk->vgc = 0;
	blk->erase_cnt++;
}

static struct line *select_victim_line(struct conv_ftl *conv_ftl, bool force)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
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
            conv_ftl->lm.victim_line_cnt, conv_ftl->lm.full_line_cnt, 
            conv_ftl->lm.free_line_cnt);

	/* victim_line is a danggling node now */
	return victim_line;
}

static int len_cmp(const void *a, const void *b)
{
    const struct lpa_len_ppa *da = a, *db = b;

    if (db->len < da->len) return -1;
    if (db->len > da->len) return 1;
    return 0;
}

void clear_oob(uint64_t pgidx) {
    NVMEV_DEBUG("Clearing OOB for %u\n", pgidx);
    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        oob[pgidx][i] = 2;
    }
}

bool oob_empty(uint64_t pgidx) {
    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        if(oob[pgidx][i] == 1) {
            NVMEV_ERROR("Page %u offset %d was %u\n", pgidx, i, oob[pgidx][i]);
            return false;
        }
    }
    return true;
}

#ifndef GC_STANDARD
char inv_m_buf[INV_PAGE_SZ];
uint64_t __get_inv_mappings(struct conv_ftl *conv_ftl, uint64_t line) {
    struct ssdparams *spp = &conv_ftl->ssd->sp;
    struct gc_data *gcd = &conv_ftl->gcd;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

    xa_init(&gcd->gc_xa);

    unsigned long index;
    unsigned long start = index = (line << 32);
    unsigned long end = (line + 1) << 32;
    void* xa_entry = NULL;

    NVMEV_DEBUG("Starting an XA scan %lu %lu\n", start, end);
    xa_for_each_range(&gcd->inv_mapping_xa, index, xa_entry, start, end) {
        uint64_t m_ppa = xa_to_value(xa_entry);

        memset(inv_m_buf, 0x0, INV_PAGE_SZ);

        struct value_set value;
        value.value = inv_m_buf;
        value.ssd = conv_ftl->ssd;
        value.length = INV_PAGE_SZ;

        NVMEV_INFO("Reading mapping page from PPA %llu (idx %lu)\n", m_ppa, index);
        nsecs_completed = __demand.li->read(m_ppa, INV_PAGE_SZ, &value, false, NULL);
        nsecs_latest = max(nsecs_latest, nsecs_completed);

        BUG_ON(m_ppa % spp->pgs_per_blk + (INV_PAGE_SZ / spp->pgsz) > spp->pgs_per_blk);

        for(int j = 0; j < INV_PAGE_SZ / (uint32_t) INV_ENTRY_SZ; j++) {
            lpa_t lpa = *(lpa_t*) (value.value + (j * INV_ENTRY_SZ));
            ppa_t ppa = *(lpa_t*) (value.value + (j * INV_ENTRY_SZ) + 
                                         sizeof(lpa_t));

            if(lpa == UINT_MAX) {
                continue;
                //printk("IDX was %d\n", j);
            }
            //BUG_ON(lpa == UINT_MAX);

            NVMEV_DEBUG("%s XA 1 Inserting inv LPA %u PPA %u "
                        "(%llu)\n", 
                        __func__, lpa, ppa, ((uint64_t) ppa << 32) | lpa);
            xa_store(&gcd->gc_xa, ((uint64_t) ppa << 32) | lpa, 
                     xa_mk_value(((uint64_t) ppa << 32) | lpa),  GFP_KERNEL);
        }

        NVMEV_DEBUG("Erasing %lu from XA.\n", index);
        xa_erase(&gcd->inv_mapping_xa, index);
        mark_grain_invalid(conv_ftl, PPA_TO_PGA(m_ppa, 0), GRAIN_PER_PAGE);
        d_stat.inv_r++;
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
        xa_store(&gcd->gc_xa, ((uint64_t) ppa << 32) | lpa, 
                 xa_mk_value(((uint64_t) ppa << 32) | lpa), GFP_KERNEL);
    }

    inv_mapping_offs[line] = 0;
    return nsecs_completed;
}

void __clear_inv_mapping(struct conv_ftl *conv_ftl, unsigned long key) {
    struct gc_data *gcd = &conv_ftl->gcd;

    NVMEV_DEBUG("Trying to erase %lu\n", key);
    void* xa_entry = xa_erase(&gcd->inv_mapping_xa, key);
    NVMEV_ASSERT(xa_entry != NULL);

    return;
}

bool __valid_mapping(struct conv_ftl *conv_ftl, uint64_t lpa, uint64_t ppa) {
    struct ssdparams *spp = &conv_ftl->ssd->sp;
    struct gc_data *gcd = &conv_ftl->gcd;

    uint64_t key = (ppa << 32) | lpa;
    void* xa_entry = xa_load(&gcd->gc_xa, key);
    if(xa_entry) {
        uint64_t xa_ppa = (xa_to_value(xa_entry) >> 32);
        if(xa_ppa == ppa) {
            /*
             * There was an invalid mapping that matched this LPA PPA
             * combination.
             */
            return false;
        } else {
            BUG_ON(true);
            /*
             * There was no such mapping, the mapping is valid.
             */
            return true;
        }
    } else {
        /*
         * There was no such mapping, the mapping is valid.
         */
        return true;
    }
}

void __update_mapping_ppa(struct conv_ftl *conv_ftl, uint64_t new_ppa, 
                          uint64_t line) {
    struct gc_data *gcd = &conv_ftl->gcd;
    unsigned long new_key = (line << 32) | new_ppa;
    NVMEV_DEBUG("%s adding %lu to XA.\n", __func__, new_key);
    xa_store(&gcd->inv_mapping_xa, new_key, xa_mk_value(new_ppa), GFP_KERNEL);
    return;
}
#endif

void __clear_gc_data(struct conv_ftl* conv_ftl) {
    struct ssdparams *spp = &conv_ftl->ssd->sp;
    struct gc_data *gcd = &conv_ftl->gcd;

    xa_destroy(&gcd->gc_xa);
}

//void __load_cmt_entry(struct conv_ftl *conv_ftl, uint64_t idx) {
//    uint64_t lpa = idx * EPP;
//    struct ssdparams *spp = &conv_ftl->ssd->sp;
//    struct request r;
//
//    NVMEV_ERROR("Loading CMT entry at LPA %u IDX %u\n", lpa, idx);
//
//    //r.mapping_v = (struct value_set*) kzalloc(sizeof(struct value_set*), GFP_KERNEL);
//    //r.mapping_v->value = kzalloc(spp->pgsz, GFP_KERNEL);
//
//    NVMEV_ERROR("Passed alloc.\n");
//    d_cache->load(lpa, &r, NULL, NULL);
//    NVMEV_ERROR("Passed load.\n");
//    d_cache->list_up(lpa, &r, NULL, NULL);
//    NVMEV_ERROR("Passed list_up.\n");
//
//    kfree(r.mapping_v->value);
//    kfree(r.mapping_v);
//}

uint64_t read_cmts[100];
uint64_t read_cmts_idx = 0;

uint64_t user_pgs_this_gc = 0;
uint64_t gc_pgs_this_gc = 0;
uint64_t map_pgs_this_gc = 0;
uint64_t map_gc_pgs_this_gc = 0;

#ifdef GC_STANDARD
void __update_cmt_ppa(struct conv_ftl *conv_ftl, uint64_t new_ppa,
                      uint64_t lpa) {
    struct cmt_struct *c = d_cache->get_cmt(lpa);
    c->t_ppa = new_ppa;
    oob[new_ppa][1] = lpa;
    NVMEV_ERROR("%s updated CMT PPA of IDX %u to %u.\n", __func__, IDX(lpa), new_ppa);
}

/* here ppa identifies the block we want to clean */
void clean_one_flashpg(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
    struct gc_data *gcd = &conv_ftl->gcd;
	struct nand_page *pg_iter = NULL;
	int page_cnt = 0, cnt = 0, i = 0, len = 0;
	uint64_t completed_time = 0, pgidx = 0;
	struct ppa ppa_copy = *ppa;
    struct line* l = get_line(conv_ftl, ppa); 

    struct lpa_len_ppa *lpa_lens;
    uint64_t tt_rewrite = 0;
    bool mapping_line = gcd->map;

    uint64_t lpa_len_idx = 0;
    lpa_lens = (struct lpa_len_ppa*) kzalloc(sizeof(struct lpa_len_ppa) * 
                                             GRAIN_PER_PAGE * 
                                             spp->pgs_per_blk, GFP_KERNEL);
    NVMEV_ASSERT(lpa_lens);

	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(conv_ftl->ssd, &ppa_copy);
        pgidx = ppa2pgidx(conv_ftl, &ppa_copy);
		/* there shouldn't be any free page in victim blocks */
		NVMEV_ASSERT(pg_iter->status != PG_FREE);
        nvmev_vdev->space_used -= spp->pgsz;
		if (pg_iter->status == PG_VALID) {
            page_cnt++;
        } else if(pg_iter->status == PG_INVALID) {
            ppa_copy.g.pg++;
            continue;
        }

        if(!mapping_line) {
            d_stat.data_r_dgc++;
        } else {
            d_stat.trans_r_tgc++;
        }

        NVMEV_DEBUG("Cleaning PPA %u\n", pgidx);
        for(int i = 0; i < GRAIN_PER_PAGE; i++) {
            uint64_t grain = PPA_TO_PGA(pgidx, i);

            if(oob[pgidx][i] == UINT_MAX) {
                /*
                 * This section of the OOB was marked as invalid,
                 * because we didn't have enough space in the page
                 * to write the next value.
                 */
                continue;
            }

            if(!mapping_line && oob[pgidx][i] != 0 && 
               oob[pgidx][i] != 2 && oob[pgidx][i] != UINT_MAX) {
                uint64_t off = pgidx * spp->pgsz + (i * GRAINED_UNIT);
                uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;
                uint64_t key = *(uint64_t*) (ptr + sizeof(uint8_t));
                NVMEV_INFO("Checking LPA %llu in PPA %llu key %llu\n", 
                            oob[pgidx][i], pgidx, key);
            }

            if(grain_bitmap[grain] == 1) {
                if(mapping_line) {
                    NVMEV_DEBUG("Got CMT PPA %u in GC\n", pgidx);
                    NVMEV_ASSERT(i == 0);
                    NVMEV_ASSERT(mapping_line);

                    lpa_t lpa = oob[pgidx][0];
                    uint64_t idx = IDX(lpa);

                    if(lpa == 2 || lpa == UINT_MAX) {
                        NVMEV_DEBUG("WUUUUT %u %d %u\n", pgidx, i, oob[pgidx][0]);
                    }
                    BUG_ON(lpa == 2);

                    NVMEV_DEBUG("LPA %u for PPA %u off %u\n", lpa, pgidx, i);

                    struct cmt_struct *c = d_cache->get_cmt(lpa);
                    if(c->t_ppa != pgidx) {
                        NVMEV_DEBUG("!!! IDX %u's CMT changed from %u to %u !!!\n",
                                idx, pgidx, c->t_ppa);

                        /*
                         * This can happen if a valid KV pair had to be moved during
                         * a previous GC iteration, and this mapping page was the
                         * one that maps that KV pair. This page will have been invalidated
                         * in do_bulk_mapping_update already, and it is ready to be 
                         * erased.
                         */

                        i += GRAIN_PER_PAGE;
                        continue;
                    }

                    if(c->state == DIRTY) {
                        NVMEV_DEBUG("CMT IDX %u was dirty in memory. Already invalidated.\n", 
                                    idx);
                        i += GRAIN_PER_PAGE;
                        continue;
                    }

                    lpa_lens[lpa_len_idx++] =
                        (struct lpa_len_ppa) {idx, GRAIN_PER_PAGE, grain, UINT_MAX - 1};

                    mark_grain_invalid(conv_ftl, grain, GRAIN_PER_PAGE);
                    cnt++;
                    tt_rewrite += GRAIN_PER_PAGE * GRAINED_UNIT;

                    i += GRAIN_PER_PAGE;
                } else if(oob[pgidx][i] != 2 && oob[pgidx][i] != 0) {
                    NVMEV_INFO("Got regular PPA %llu LPA %llu in GC\n", pgidx, oob[pgidx][i]);

                    len = 1;
                    while(i + len < GRAIN_PER_PAGE && oob[pgidx][i + len] == 0) {
                        len++;
                    }

                    lpa_lens[lpa_len_idx++] =
                        (struct lpa_len_ppa) {oob[pgidx][i], len, grain, UINT_MAX};

                    mark_grain_invalid(conv_ftl, grain, len);
                    cnt++;
                    tt_rewrite += len * GRAINED_UNIT;

                }

            } 
        }

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

	if (cnt <= 0) {
        kfree(lpa_lens);
		return;
    }

    sort(lpa_lens, lpa_len_idx, sizeof(struct lpa_len_ppa), &len_cmp, NULL);

	if (cpp->enable_gc_delay) {
		struct nand_cmd gcr = {
			.type = GC_MAP_IO,
			.cmd = NAND_READ,
			.stime = 0,
			.xfer_size = spp->pgsz * page_cnt,
			.interleave_pci_dma = false,
			.ppa = &ppa_copy,
		};
		completed_time = ssd_advance_nand(conv_ftl->ssd, &gcr);
	}

    NVMEV_DEBUG("Copying %d pairs from %d pages.\n",
                 cnt, page_cnt);

    uint64_t grains_rewritten = 0;
    uint64_t remain = tt_rewrite;

    struct ppa new_ppa;
    uint32_t offset;
    uint64_t to = 0, from = 0;

    if(gcd->offset < GRAIN_PER_PAGE) {
        new_ppa = gcd->gc_ppa;
        offset = gcd->offset;
        pgidx = ppa2pgidx(conv_ftl, &new_ppa);
    } else {
        new_ppa = get_new_page(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);
        offset = 0;
        pgidx = ppa2pgidx(conv_ftl, &new_ppa);
        NVMEV_ASSERT(oob_empty(ppa2pgidx(conv_ftl, &new_ppa)));
        mark_page_valid(conv_ftl, &new_ppa);
        advance_write_pointer(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);

        if(!mapping_line) {
            d_stat.data_w_dgc++;
        } else {
            d_stat.trans_w_tgc++;
        }
    }

    NVMEV_ASSERT(remain > 0 && grains_rewritten < cnt);

    while(grains_rewritten < cnt) {
        uint32_t length = lpa_lens[grains_rewritten].len;
        uint64_t lpa = lpa_lens[grains_rewritten].lpa;
        uint64_t old_grain = lpa_lens[grains_rewritten].prev_ppa;
        uint64_t grain = PPA_TO_PGA(pgidx, offset);

        if(length > GRAIN_PER_PAGE - offset) {
            /*
             * There's not enough space left in this page.
             * Mark the rest of the page invalid.
             *
             * We would do well here to have an implementation
             * that packs values into pages like in the original work.
             */

            NVMEV_ASSERT(offset > 0);
            mark_grain_valid(conv_ftl, PPA_TO_PGA(pgidx, offset), 
                    GRAIN_PER_PAGE - offset);
            mark_grain_invalid(conv_ftl, PPA_TO_PGA(pgidx, offset), 
                    GRAIN_PER_PAGE - offset);

            uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
            memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                   GRAINED_UNIT);

            NVMEV_DEBUG("Marking %d grains invalid during loop pgidx %u offset %u.\n", 
                    GRAIN_PER_PAGE - offset, pgidx, offset);

            for(int i = offset; i < GRAIN_PER_PAGE; i++) {
                oob[pgidx][i] = UINT_MAX;
                grain_bitmap[PPA_TO_PGA(pgidx, i)] = 0;
            }

            if (cpp->enable_gc_delay) {
                struct nand_cmd gcw = {
                    .type = GC_IO,
                    .cmd = NAND_NOP,
                    .stime = 0,
                    .interleave_pci_dma = false,
                    .ppa = &new_ppa,
                };

                if (last_pg_in_wordline(conv_ftl, &new_ppa)) {
                    gcw.cmd = NAND_WRITE;
                    gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
                }

                ssd_advance_nand(conv_ftl->ssd, &gcw);
            }

            nvmev_vdev->space_used += (GRAIN_PER_PAGE - offset) * GRAINED_UNIT;
            goto new_ppa;
        }

        if(lpa == UINT_MAX) {
            /*
             * offset == 0 assumes an invalid mapping page is
             * the size of a page.
             */

            NVMEV_ASSERT(offset == 0);
            NVMEV_ASSERT(length == GRAIN_PER_PAGE);
        }

        to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        from = (G_IDX(old_grain) * spp->pgsz) + 
               (G_OFFSET(old_grain) * GRAINED_UNIT);

        NVMEV_INFO("LPA/IDX %llu length %u klen %u going from %llu (G%llu) to %llu (G%llu)\n",
                    lpa, length, *(uint8_t*) (nvmev_vdev->ns[0].mapped + from), 
                    G_IDX(old_grain), old_grain, pgidx, grain);

        memcpy(nvmev_vdev->ns[0].mapped + to, 
               nvmev_vdev->ns[0].mapped + from, length * GRAINED_UNIT);
        nvmev_vdev->space_used += length * GRAINED_UNIT;

        if(!mapping_line) {
            BUG_ON(lpa == 2);
        }

        lpa_lens[grains_rewritten].new_ppa = PPA_TO_PGA(pgidx, offset);

        if(mapping_line) {
            oob[pgidx][offset] = IDX2LPA(lpa);
        } else {
            NVMEV_INFO("Setting OOB of page %llu offset %u to LPA %llu\n",
                        pgidx, offset, lpa);
            oob[pgidx][offset] = lpa;
        }

        for(int i = 1; i < length; i++) {
            oob[pgidx][offset + i] = 0;
        }

        mark_grain_valid(conv_ftl, grain, length);

        if(mapping_line) {
            uint64_t idx = lpa;
            struct cmt_struct *c = d_cache->get_cmt(IDX2LPA(idx));
    
            if(c->t_ppa == G_IDX(old_grain)) {
                NVMEV_DEBUG("CMT IDX %u moving from PPA %u to PPA %u\n", 
                            idx, G_IDX(old_grain), pgidx);
                c->t_ppa = pgidx;
                c->grain = offset;
            }
        }

        offset += length;
        remain -= length * GRAINED_UNIT;
        grains_rewritten++;

        NVMEV_ASSERT(offset <= GRAIN_PER_PAGE);

        if(offset == GRAIN_PER_PAGE && grains_rewritten < cnt) {
            NVMEV_ASSERT(remain > 0);
new_ppa:
            new_ppa = get_new_page(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);
            offset = 0;
            pgidx = ppa2pgidx(conv_ftl, &new_ppa);
            NVMEV_ASSERT(oob_empty(ppa2pgidx(conv_ftl, &new_ppa)));
            mark_page_valid(conv_ftl, &new_ppa);
            advance_write_pointer(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);

            if(!mapping_line) {
                d_stat.data_w_dgc++;
            } else {
                d_stat.trans_w_tgc++;
            }
            //NVMEV_ERROR("2 Picked up PPA %u %u remaining grains\n", 
            //        pgidx, GRAIN_PER_PAGE - offset);
        }
    }

    if(remain != 0) {
        NVMEV_ERROR("Remain was %u\n", remain);
    }

    NVMEV_ASSERT(offset > 0);
    NVMEV_ASSERT(remain == 0);

    if(GRAIN_PER_PAGE - offset > 0) {    
        gcd->offset = offset;
    } else {
        gcd->offset = GRAIN_PER_PAGE;
    }

    gcd->gc_ppa = new_ppa;
    gcd->pgidx = pgidx;

    //if (cpp->enable_gc_delay) {
    //    struct nand_cmd gcw = {
    //        .type = GC_IO,
    //        .cmd = NAND_NOP,
    //        .stime = 0,
    //        .interleave_pci_dma = false,
    //        .ppa = &new_ppa,
    //    };

    //    if (last_pg_in_wordline(conv_ftl, &new_ppa)) {
    //        gcw.cmd = NAND_WRITE;
    //        gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
    //    }

    //    ssd_advance_nand(conv_ftl->ssd, &gcw);
    //}

    if(!mapping_line) {
        do_bulk_mapping_update_v(lpa_lens, cnt, read_cmts, read_cmts_idx);
    }

    read_cmts_idx = 0;
    kfree(lpa_lens);

    return;
}
#else
/* here ppa identifies the block we want to clean */
void clean_one_flashpg(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
    struct gc_data *gcd = &conv_ftl->gcd;
	struct nand_page *pg_iter = NULL;
	int page_cnt = 0, cnt = 0, i = 0, len = 0;
	uint64_t completed_time = 0, pgidx = 0;
	struct ppa ppa_copy = *ppa;
    struct line* l = get_line(conv_ftl, ppa); 
    bool mapping_line = gcd->map;

    struct lpa_len_ppa *lpa_lens;
    uint64_t tt_rewrite = 0;

    uint64_t lpa_len_idx = 0;
    lpa_lens = (struct lpa_len_ppa*) kzalloc(sizeof(struct lpa_len_ppa) * 
                                             GRAIN_PER_PAGE * 
                                             spp->pgs_per_blk, GFP_KERNEL);
    NVMEV_ASSERT(lpa_lens);

    //read_cmts = (uint64_t*) kzalloc(spp->pgs_per_flashpg, GFP_KERNEL);
    //NVMEV_ASSERT(read_cmts);

	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(conv_ftl->ssd, &ppa_copy);
        pgidx = ppa2pgidx(conv_ftl, &ppa_copy);
		/* there shouldn't be any free page in victim blocks */
		NVMEV_ASSERT(pg_iter->status != PG_FREE);
        nvmev_vdev->space_used -= spp->pgsz;
		if (pg_iter->status == PG_VALID) {
            page_cnt++;
        } else if(pg_iter->status == PG_INVALID) {
            //NVMEV_ERROR("inv cnt pg %u %lld\n", pgidx, pg_inv_cnt[pgidx]); 
            NVMEV_ASSERT(pg_inv_cnt[pgidx] == spp->pgsz);
            ppa_copy.g.pg++;
            continue;
        } else if(pg_inv_cnt[pgidx] == spp->pgsz) {
            NVMEV_ASSERT(pg_iter->status == PG_INVALID);
            ppa_copy.g.pg++;
            continue;
        }

        if(!mapping_line) {
            d_stat.data_r_dgc++;
        } else {
            d_stat.trans_r_tgc++;
        }

        NVMEV_INFO("Cleaning PPA %llu\n", pgidx);
        for(int i = 0; i < GRAIN_PER_PAGE; i++) {
            uint64_t grain = PPA_TO_PGA(pgidx, i);

            if(i == 0 && oob[pgidx][i] == UINT_MAX) {
                unsigned long key = oob[pgidx][1];
                //printk("Got invalid mapping PPA %llu key %lu target line %lu in GC\n", 
                //            pgidx, key, key >> 32);
                NVMEV_ASSERT(i == 0);
                NVMEV_ASSERT(mapping_line);

                if(!__invalid_mapping_ppa(conv_ftl, G_IDX(grain), key)) {
                    //printk("Mapping PPA %llu key %lu has since been rewritten. Skipping.\n",

                    //        pgidx, key);

                    i += GRAIN_PER_PAGE;
                    continue;
                } else {
                    __clear_inv_mapping(conv_ftl, key);
                }

                lpa_lens[lpa_len_idx++] =
                (struct lpa_len_ppa) {UINT_MAX, GRAIN_PER_PAGE, grain, 
                                      key >> 32 /* The line these invalid mappings target. */};

                mark_grain_invalid(conv_ftl, grain, GRAIN_PER_PAGE);
                cnt++;
                tt_rewrite += GRAIN_PER_PAGE * GRAINED_UNIT;
                i += GRAIN_PER_PAGE;
                d_stat.trans_r_dgc_2++;
            } else if(mapping_line) {
                NVMEV_ASSERT(i == 0);
                NVMEV_ASSERT(mapping_line);

                uint8_t oob_idx = 0;
                uint64_t lpa, idx;
                bool need_update = false;
                while(oob_idx < GRAIN_PER_PAGE && oob[pgidx][oob_idx] != UINT_MAX) {
                    lpa = oob[pgidx][oob_idx];
                    idx = IDX(lpa);

                    if(lpa == 2) {
                        NVMEV_DEBUG("WUUUUT %u %d %u\n", pgidx, oob_idx, oob[pgidx][0]);
                    }
                    BUG_ON(lpa == 2);

                    if(lpa == UINT_MAX - 1) {
                        oob_idx++;
                        continue;
                    }

                    if(lpa == UINT_MAX) {
                        NVMEV_DEBUG("PPA %u closed at grain %u\n", pgidx, oob_idx);
                        break;
                    }

                    NVMEV_DEBUG("LPA %u for PPA %u off %u\n", lpa, pgidx, oob_idx);

                    struct cmt_struct *c = d_cache->get_cmt(lpa);
                    if(c->t_ppa != pgidx) {
                        NVMEV_DEBUG("CMT IDX %u moved from %u to %u\n", 
                                    idx, pgidx, c->t_ppa);
                        oob_idx++;
                        continue;
                    } 

                    if(c->state == DIRTY) {
                        NVMEV_DEBUG("CMT IDX %u was dirty in memory. Already invalidated.\n", 
                                    idx);
                        oob_idx++;
                        continue;
                    }

                    NVMEV_DEBUG("CMT IDX %u checking LPA %u\n", idx, lpa);
                    need_update = true;

                    len = 1;
                    while(oob_idx + len < GRAIN_PER_PAGE && oob[pgidx][oob_idx + len] == UINT_MAX - 1) {
                        len++;
                    }

                    NVMEV_DEBUG("Its CMT IDX %u was %u grains in size from grain %u (%u %u)\n", 
                                idx, len, grain, G_IDX(grain + oob_idx), G_OFFSET(grain + oob_idx));
                    mark_grain_invalid(conv_ftl, grain + oob_idx, len);

                    lpa_lens[lpa_len_idx++] =
                    (struct lpa_len_ppa) {idx, len, grain + oob_idx, 
                                          UINT_MAX - 1, UINT_MAX};

                    oob_idx += len;
                    tt_rewrite += len * GRAINED_UNIT;
                    cnt++;
                }

                if(!need_update) {
                    i += GRAIN_PER_PAGE;
                    continue;
                }

                //mark_grain_invalid(conv_ftl, grain, 
                //                  (spp->pgsz - pg_inv_cnt[pgidx]) / GRAINED_UNIT);
                i += GRAIN_PER_PAGE;
            } else if(oob[pgidx][i] != UINT_MAX && oob[pgidx][i] != 2 && oob[pgidx][i] != 0 && 
               __valid_mapping(conv_ftl, oob[pgidx][i], pgidx)) {
                NVMEV_INFO("Got regular PPA %llu LPA %llu in GC grain %u\n", 
                        pgidx, oob[pgidx][i], i);
                NVMEV_ASSERT(pg_inv_cnt[pgidx] <= spp->pgsz);
                NVMEV_ASSERT(!mapping_line);
                
                uint64_t lpa = oob[pgidx][i];

                len = 1;
                while(i + len < GRAIN_PER_PAGE && oob[pgidx][i + len] == 0) {
                    len++;
                }
                
                //lengths[valid_lpa_cnt - 1] = len;

                lpa_lens[lpa_len_idx++] =
                (struct lpa_len_ppa) {oob[pgidx][i], len, grain, UINT_MAX};

                //NVMEV_ASSERT(__valid_mapping_ht(oob[pgidx][i], grain));

                mark_grain_invalid(conv_ftl, grain, len);
                cnt++;
                tt_rewrite += len * GRAINED_UNIT;
            } else {
            } 
        }

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

	if (cnt <= 0) {
        NVMEV_DEBUG("Returning with no copies.\n");
        kfree(lpa_lens);
		return;
    }

    sort(lpa_lens, lpa_len_idx, sizeof(struct lpa_len_ppa), &len_cmp, NULL);

	if (cpp->enable_gc_delay) {
		struct nand_cmd gcr = {
			.type = GC_IO,
			.cmd = NAND_READ,
			.stime = 0,
			.xfer_size = spp->pgsz * page_cnt,
			.interleave_pci_dma = false,
			.ppa = &ppa_copy,
		};
		completed_time = ssd_advance_nand(conv_ftl->ssd, &gcr);
	}

    NVMEV_DEBUG("Copying %d pairs from %d pages.\n",
                cnt, page_cnt);

    uint64_t grains_rewritten = 0;
    uint64_t remain = tt_rewrite;

    struct ppa new_ppa;
    uint32_t offset;
    uint64_t to = 0, from = 0;
    uint64_t new_line;

    if(gcd->offset < GRAIN_PER_PAGE) {
        new_ppa = gcd->gc_ppa;
        offset = gcd->offset;
        pgidx = ppa2pgidx(conv_ftl, &new_ppa);
        new_line = ppa2line(conv_ftl, &new_ppa);
        NVMEV_DEBUG("Picked up PPA %u %u remaining grains\n", 
                pgidx, GRAIN_PER_PAGE - offset);
    } else {
        new_ppa = get_new_page(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);
        offset = 0;
        pgidx = ppa2pgidx(conv_ftl, &new_ppa);
        new_line = ppa2line(conv_ftl, &new_ppa);
        NVMEV_ASSERT(oob_empty(ppa2pgidx(conv_ftl, &new_ppa)));
        mark_page_valid(conv_ftl, &new_ppa);
        advance_write_pointer(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);

        if(!mapping_line) {
            d_stat.data_w_dgc++;
        } else {
            d_stat.trans_w_tgc++;
        }

        NVMEV_DEBUG("Got PPA %u here remain %u gr %u (%u)\n", 
        pgidx, remain, grains_rewritten, cnt);
    }

    NVMEV_ASSERT(remain > 0 && grains_rewritten < cnt);

    while(grains_rewritten < cnt) {
        uint32_t length = lpa_lens[grains_rewritten].len;
        uint64_t lpa = lpa_lens[grains_rewritten].lpa;
        uint64_t old_grain = lpa_lens[grains_rewritten].prev_ppa;
        uint64_t grain = PPA_TO_PGA(pgidx, offset);

        if(length > GRAIN_PER_PAGE - offset) {
            /*
             * There's not enough space left in this page.
             * Mark the rest of the page invalid.
             *
             * We would do well here to have an implementation
             * that packs values into pages like in the original work.
             */

            NVMEV_ASSERT(offset > 0);
            mark_grain_valid(conv_ftl, PPA_TO_PGA(pgidx, offset), 
                    GRAIN_PER_PAGE - offset);
            mark_grain_invalid(conv_ftl, PPA_TO_PGA(pgidx, offset), 
                    GRAIN_PER_PAGE - offset);

            uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
            memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                   GRAINED_UNIT);

            NVMEV_DEBUG("Marking %d grains invalid during loop pgidx %u offset %u.\n", 
                    GRAIN_PER_PAGE - offset, pgidx, offset);

            for(int i = offset; i < GRAIN_PER_PAGE; i++) {
                oob[pgidx][i] = UINT_MAX;
            }

            if (cpp->enable_gc_delay) {
                struct nand_cmd gcw = {
                    .type = GC_IO,
                    .cmd = NAND_NOP,
                    .stime = 0,
                    .interleave_pci_dma = false,
                    .ppa = &new_ppa,
                };

                if (last_pg_in_wordline(conv_ftl, &new_ppa)) {
                    gcw.cmd = NAND_WRITE;
                    gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
                }

                ssd_advance_nand(conv_ftl->ssd, &gcw);
            }

            nvmev_vdev->space_used += (GRAIN_PER_PAGE - offset) * GRAINED_UNIT;
            goto new_ppa;
        }

        if(lpa == UINT_MAX) {
            /*
             * offset == 0 assumes an invalid mapping page is
             * the size of a page.
             */

            NVMEV_ASSERT(offset == 0);
            NVMEV_ASSERT(length == GRAIN_PER_PAGE);
            NVMEV_ASSERT(remain >= INV_PAGE_SZ);
        }

        NVMEV_INFO("LPA/IDX %llu length %u going from %llu (G%llu) to %llu (G%llu)\n",
                lpa, length, G_IDX(old_grain), old_grain, pgidx, grain);

        to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        from = (G_IDX(old_grain) * spp->pgsz) + 
            (G_OFFSET(old_grain) * GRAINED_UNIT);

        memcpy(nvmev_vdev->ns[0].mapped + to, 
               nvmev_vdev->ns[0].mapped + from, length * GRAINED_UNIT);
        nvmev_vdev->space_used += length * GRAINED_UNIT;

        if(lpa == UINT_MAX) {
            oob[pgidx][offset] = lpa;
        } else {
            lpa_lens[grains_rewritten].new_ppa = PPA_TO_PGA(pgidx, offset);
            oob[pgidx][offset] = mapping_line ? IDX2LPA(lpa) : lpa;
        }

        for(int i = 1; i < length; i++) {
            oob[pgidx][offset + i] = mapping_line ? UINT_MAX - 1 : 0;
        }

        mark_grain_valid(conv_ftl, grain, length);

        if(lpa == UINT_MAX) { // __invalid_mapping_ppa(conv_ftl, G_IDX(old_grain), l->id)) {
            unsigned long target_line = lpa_lens[grains_rewritten].new_ppa;
            __update_mapping_ppa(conv_ftl, pgidx, target_line);
            NVMEV_DEBUG("Putting %u in the OOB for mapping PPA %u which targets line %lu\n",
                        (target_line << 32) | pgidx, pgidx, target_line);
            oob[pgidx][1] = (target_line << 32) | pgidx;
        } else if(mapping_line) {
            uint64_t idx = lpa;
            struct cmt_struct *c = d_cache->get_cmt(IDX2LPA(idx));
    
            if(c->t_ppa == G_IDX(old_grain)) {
                NVMEV_DEBUG("CMT IDX %u moving from PPA %u to PPA %u\n", 
                    idx, G_IDX(old_grain), pgidx);
                c->t_ppa = pgidx;
                c->grain = offset;
            }
        }

        offset += length;
        remain -= length * GRAINED_UNIT;
        grains_rewritten++;

        NVMEV_ASSERT(offset <= GRAIN_PER_PAGE);

        if(offset == GRAIN_PER_PAGE && grains_rewritten < cnt) {
            NVMEV_ASSERT(remain > 0);
new_ppa:
            new_ppa = get_new_page(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);
            new_line = ppa2line(conv_ftl, &new_ppa);
            offset = 0;
            pgidx = ppa2pgidx(conv_ftl, &new_ppa);
            NVMEV_ASSERT(oob_empty(ppa2pgidx(conv_ftl, &new_ppa)));
            mark_page_valid(conv_ftl, &new_ppa);
            advance_write_pointer(conv_ftl, mapping_line ? GC_MAP_IO : GC_IO);

            if(!mapping_line) {
                d_stat.data_w_dgc++;
            } else {
                d_stat.trans_w_tgc++;
            }
        }
    }

    if(remain != 0) {
        NVMEV_DEBUG("Remain was %u\n", remain);
    }

    NVMEV_ASSERT(offset > 0);
    NVMEV_ASSERT(remain == 0);

    if(GRAIN_PER_PAGE - offset > 0) {    
        gcd->offset = offset;
    } else {
        gcd->offset = GRAIN_PER_PAGE;
    }

    gcd->gc_ppa = new_ppa;
    gcd->pgidx = pgidx;

    //if (cpp->enable_gc_delay) {
    //    struct nand_cmd gcw = {
    //        .type = GC_IO,
    //        .cmd = NAND_NOP,
    //        .stime = 0,
    //        .interleave_pci_dma = false,
    //        .ppa = &new_ppa,
    //    };

    //    if (last_pg_in_wordline(conv_ftl, &new_ppa)) {
    //        gcw.cmd = NAND_WRITE;
    //        gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
    //    }

    //    /*
    //     * TODO are we skipping some writes because this isn't triggering?
    //     */

    //    ssd_advance_nand(conv_ftl->ssd, &gcw);
    //}

    if(!mapping_line) {
        do_bulk_mapping_update_v(lpa_lens, cnt, read_cmts, read_cmts_idx);
    }

    read_cmts_idx = 0;
    //kfree(read_cmts);
    kfree(lpa_lens);

    return;
}
#endif

static void mark_line_free(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *line = get_line(conv_ftl, ppa);

    NVMEV_DEBUG("Marking line %d free\n", line->id);

	line->ipc = 0;
	line->vpc = 0;
    line->igc = 0;
    line->vgc = 0;
	/* move this line to free line list */
	list_add_tail(&line->entry, &lm->free_line_list);
	lm->free_line_cnt++;
}

static uint64_t do_gc(struct conv_ftl *conv_ftl, bool force)
{
	struct line *victim_line = NULL;
	struct ssdparams *spp = &conv_ftl->ssd->sp;
    struct convparams *cpp = &conv_ftl->cp;
	struct ppa ppa;
    struct gc_data *gcd = &conv_ftl->gcd;
	int flashpg;
    uint64_t pgidx;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

	victim_line = select_victim_line(conv_ftl, force);
	if (!victim_line) {
        BUG_ON(true);
		return nsecs_completed;
	}

    gcd->map = victim_line->map;

    user_pgs_this_gc = gc_pgs_this_gc = map_gc_pgs_this_gc = map_pgs_this_gc = 0;

	ppa.g.blk = victim_line->id;
	printk("GC-ing %s line:%d,ipc=%d(%d),igc=%d(%d),victim=%d,full=%d,free=%d\n", 
            gcd->map? "MAP" : "USER", ppa.g.blk,
		    victim_line->ipc, victim_line->vpc, victim_line->igc, victim_line->vgc,
            conv_ftl->lm.victim_line_cnt, conv_ftl->lm.full_line_cnt, 
            conv_ftl->lm.free_line_cnt);

    if(gcd->map) {
        d_stat.tgc_cnt++;
    } else {
        d_stat.dgc_cnt++;
    }

	conv_ftl->wfc.credits_to_refill = victim_line->igc;
#ifndef GC_STANDARD
    if(!gcd->map) {
        nsecs_completed = __get_inv_mappings(conv_ftl, victim_line->id);
    }
#endif
    nsecs_latest = max(nsecs_latest, nsecs_completed);

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
				lunp = get_lun(conv_ftl->ssd, &ppa);
				clean_one_flashpg(conv_ftl, &ppa);

				if (flashpg == (spp->flashpgs_per_blk - 1)) {
					struct convparams *cpp = &conv_ftl->cp;

					mark_block_free(conv_ftl, &ppa);

					if (cpp->enable_gc_delay) {
						struct nand_cmd gce = {
							.type = GC_IO,
							.cmd = NAND_ERASE,
							.stime = 0,
							.interleave_pci_dma = false,
							.ppa = &ppa,
						};
						nsecs_completed = ssd_advance_nand(conv_ftl->ssd, &gce);
                        nsecs_latest = max(nsecs_latest, nsecs_completed);
					}

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

        uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                GRAINED_UNIT);

        for(int i = offset; i < GRAIN_PER_PAGE; i++) {
            oob[pgidx][i] = UINT_MAX;
#ifdef GC_STANDARD
            grain_bitmap[PPA_TO_PGA(pgidx, i)] = 0;
#endif
        }

        mark_grain_valid(conv_ftl, PPA_TO_PGA(pgidx, offset), 
                         GRAIN_PER_PAGE - offset);
        mark_grain_invalid(conv_ftl, PPA_TO_PGA(pgidx, offset), 
                           GRAIN_PER_PAGE - offset);

        if (cpp->enable_gc_delay) {
            struct nand_cmd gcw = {
                .type = GC_IO,
                .cmd = NAND_NOP,
                .stime = 0,
                .interleave_pci_dma = false,
                .ppa = &ppa,
            };

            if (last_pg_in_wordline(conv_ftl, &ppa)) {
                gcw.cmd = NAND_WRITE;
                gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
            }

            ssd_advance_nand(conv_ftl->ssd, &gcw);
        }

        nvmev_vdev->space_used += (GRAIN_PER_PAGE - offset) * GRAINED_UNIT;

        gcd->offset = GRAIN_PER_PAGE;
        gcd->pgidx = UINT_MAX;
    }

    /* update line status */
	mark_line_free(conv_ftl, &ppa);
#ifndef GC_STANDARD
    if(!gcd->map) {
        __clear_gc_data(conv_ftl);
    }
#endif

    NVMEV_ASSERT(user_pgs_this_gc == 0);
    printk("%llu user %llu GC %llu map GC this round. %lu pgs_per_line.", 
            user_pgs_this_gc, gc_pgs_this_gc, map_gc_pgs_this_gc, spp->pgs_per_line);

	return nsecs_latest;
}

uint32_t loops = 0;
static uint64_t forground_gc(struct conv_ftl *conv_ftl)
{
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

	while(should_gc_high(conv_ftl)) {
		NVMEV_DEBUG("should_gc_high passed");
		/* perform GC here until !should_gc(conv_ftl) */
		nsecs_completed = do_gc(conv_ftl, true);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
	}
    
    return nsecs_latest;
}

static bool is_same_flash_page(struct conv_ftl *conv_ftl, struct ppa ppa1, struct ppa ppa2)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	uint64_t ppa1_page = ppa1.g.pg / spp->pgs_per_flashpg;
	uint64_t ppa2_page = ppa2.g.pg / spp->pgs_per_flashpg;

	return (ppa1.h.blk_in_ssd == ppa2.h.blk_in_ssd) && (ppa1_page == ppa2_page);
}

bool end_w(struct request *req) 
{
    return true;
}

uint32_t __get_vlen(uint64_t grain) {
    uint32_t ret = 1, i = G_OFFSET(grain);
    while(i + ret < GRAIN_PER_PAGE && oob[G_IDX(grain)][i + ret] == 0) {
        ret++;
    }

    return ret * GRAINED_UNIT;
}

bool end_d(struct request *req) 
{
    return true;
    //if(req->ppa == UINT_MAX) {
    //    NVMEV_DEBUG("Delete succeeded for key %s\n", req->cmd->kv_retrieve.key);
    //    req->ppa = UINT_MAX;
    //    return true;
    //}

    //NVMEV_DEBUG("Delete failed for key %s\n", req->cmd->kv_retrieve.key);
    //return false;
}

char read_buf[4096];
static bool conv_delete(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
    struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
    struct conv_ftl *conv_ftl = &conv_ftls[0];

    /* wbuf and spp are shared by all instances */
    struct ssdparams *spp = &conv_ftl->ssd->sp;

    struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;

    uint64_t nsecs_latest;
    uint64_t nsecs_xfer_completed;

    struct request d_req;
    KEYT key;

    memset(&d_req, 0x0, sizeof(d_req));

    d_req.ssd = conv_ftl->ssd;
    d_req.req = req;
    d_req.hash_params = NULL;

    key.key = (char*)kzalloc(cmd_key_length(*cmd), GFP_KERNEL);

    NVMEV_ASSERT(key.key);
    NVMEV_ASSERT(cmd);
    NVMEV_ASSERT(cmd->kv_delete.key);

    uint8_t klen = cmd_key_length(*cmd);

    key.key = (char*)kzalloc(klen + 1, GFP_KERNEL);

    memcpy(key.key, cmd->kv_retrieve.key, klen);
    key.key[klen] = '\0';
    key.len = klen;
    d_req.key = key;

    NVMEV_INFO("Delete for key %llu len %u\n", *(uint64_t*) key.key, key.len);

    struct value_set *value;
    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->ssd = conv_ftl->ssd;

    /*
     * We still provide a read buffer here, because a delete will
     * read pages from disk to confirm its deleting the right
     * KV pair.
     */

    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->value = read_buf;
    value->ssd = conv_ftl->ssd;
    value->length = spp->pgsz;
    d_req.value = value;
    d_req.end_req = &end_d;
    d_req.cmd = cmd;
    nsecs_latest = nsecs_xfer_completed = __demand.remove(&d_req);

    ret->nsecs_target = nsecs_latest;

    if(d_req.ppa == UINT_MAX) {
        NVMEV_DEBUG("NOT_EXIST delete for key %s len %u\n", key.key, key.len);
        ret->status = KV_ERR_KEY_NOT_EXIST;
        cmd->kv_delete.rsvd = d_req.ppa;
    } else {
        NVMEV_DEBUG("Delete successful for key %s len %u\n", key.key, key.len);
        ret->nsecs_target = nsecs_latest;
        ret->status = NVME_SC_SUCCESS;

        /*
         * Switch back to UINT_MAX to avoid a copy to user in
         * __do_perform_io_kv in io.c.
         */

        cmd->kv_delete.rsvd = UINT_MAX;
    }

    kfree(value);
    kfree(key.key);
    return true;
}

bool end_r(struct request *req) 
{
    if(req->ppa == UINT_MAX) {
        req->ppa = UINT_MAX;
        return false;
    }

    //uint32_t real_vlen = 
    //    *(uint32_t*) ((uint8_t*) req->value->value + req->key.len + sizeof(uint8_t));
    req->cmd->kv_retrieve.value_len = req->value->length;
    NVMEV_DEBUG("Set value length to %u for key %s\n", 
                req->value->length, req->cmd->kv_retrieve.key);

    //NVMEV_ERROR("2 Got a real VLEN of %u (%u %lu %lu)\n", 
    //        real_vlen, req->key.len, sizeof(uint8_t), sizeof(uint32_t));

    if(req->ppa == UINT_MAX - 1) {
        return true;
    }

    uint64_t pgsz = req->ssd->sp.pgsz;
    uint32_t prev = req->ppa;
    //req->ppa = (G_IDX(req->ppa) * pgsz) + (G_OFFSET(req->ppa) * GRAINED_UNIT);
    //NVMEV_INFO("%s switching ppa %u to offset %u\n", __func__, prev, req->ppa);

    return true;
}

static bool conv_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
    struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
    struct conv_ftl *conv_ftl = &conv_ftls[0];

    /* wbuf and spp are shared by all instances */
    struct ssdparams *spp = &conv_ftl->ssd->sp;

    struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;
    struct nvme_kv_command tmp = *cmd;

    uint64_t nsecs_latest;
    uint64_t nsecs_xfer_completed;

    struct request d_req;
    KEYT key;

    /*
     * Haven't done this the new way yet.
     */

    BUG_ON(true);

    memset(&d_req, 0x0, sizeof(d_req));

    d_req.ssd = conv_ftl->ssd;
    d_req.req = req;
    d_req.hash_params = NULL;

    BUG_ON(!cmd->kv_retrieve.key);
    BUG_ON(!cmd);

    uint8_t klen = cmd_key_length(tmp);
    uint32_t vlen = cmd_value_length(*cmd);

    key.key = NULL;
    key.key = (char*)kzalloc(klen + 1, GFP_KERNEL);

    memcpy(key.key, cmd->kv_retrieve.key, klen);
    key.key[klen] = '\0';
    key.len = klen;
    d_req.key = key;

    NVMEV_DEBUG("Read for key %s (%llu) klen %u vlen %u\n", 
                key.key, *(uint64_t*) key.key, klen, vlen);

    if(!strncmp(key.key, "LOG", 3)) {
        uint64_t bid = *(uint64_t*) (key.key + 4);
        uint16_t log_num = *(uint16_t*) (key.key + 4 + sizeof(bid));
        NVMEV_INFO("Log key. Bid %llu log num %u\n", bid, log_num);
    }

    memset(read_buf, 0x0, 4096);

    struct value_set *value;
    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->value = kzalloc(4096, GFP_KERNEL); // read_buf;
    value->ssd = conv_ftl->ssd;
    value->length = vlen;
    d_req.value = value;
    d_req.end_req = &end_r;
    d_req.cmd = cmd;
    nsecs_latest = nsecs_xfer_completed = __demand.read(&d_req);

    kfree(value->value);
    kfree(value);
    kfree(key.key);
    return true;
}

/*
 * In block-device virt, we would get the LBA -> PPA mapping in here and schedule IO
 * on that PPA. The actual data copy is done later in __do_perform_io, where the data
 * itself is either copied to or copied from the slba offset in the allocated kernel memory.
 *
 * The slba offset in kernel memory doesn't change with the PPA changes in here, and thus
 * this fuction doesn't feed back to the IO copy functions to tell them where to copy to
 * and from.
 *
 * With the KVSSD FTL, we don't do IO using an slba, and thus we don't know where to copy
 * to and from kernel memory later.
 *
 * We perform KV FTL functions here which schedule IO on PPAs and return an offset on the disk.
 * That offset then overwrites the slba in the original NVMe command, which is used in
 * __do_perform_io later.
 */

char* __get_wb_buf(void) {
    char* ret = wb_bufs[wb_off++];
    if(wb_off == MAX_WRITE_BUF) {
        wb_off = 0;
    }
    return ret;
}

static bool conv_write(struct nvmev_ns *ns, struct nvmev_request *req, 
                       struct nvmev_result *ret, bool internal)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];

	/* wbuf and spp are shared by all instances */
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;

	uint64_t nsecs_latest;
	uint64_t nsecs_xfer_completed;

    struct request *d_req;
    KEYT key;

    d_req = (struct request*) kzalloc(sizeof(*d_req), GFP_KERNEL);
    d_req->ssd = conv_ftl->ssd;
    d_req->req = req;
    d_req->hash_params = NULL;
    d_req->cmd = cmd;

    uint8_t klen = cmd_key_length(*cmd);
    uint32_t vlen = cmd_value_length(*cmd);

    key.key = NULL;
    key.key = (char*)kzalloc(klen + 1, GFP_KERNEL);

    BUG_ON(!key.key);
    BUG_ON(!cmd->kv_store.key);
    BUG_ON(!cmd);

    NVMEV_ASSERT(vlen > klen);

    memcpy(key.key, cmd->kv_store.key, klen);
    key.key[klen] = '\0';
    key.len = klen;
    d_req->key = key;

    NVMEV_DEBUG("Write for key %s (%llu) klen %u vlen %u\n", 
                key.key, *(uint64_t*) (key.key), klen, vlen);

    struct value_set *value;
    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->value = (char*)kzalloc(spp->pgsz, GFP_KERNEL);
    value->ssd = conv_ftl->ssd;
    value->length = vlen;
    d_req->value = value;
    d_req->end_req = &end_w;
    d_req->sqid = req->sq_id;

    NVMEV_ASSERT(value->value);

    schedule_internal_operation_cb(req->sq_id, 0,
                                   (void*) cmd->kv_store.dptr.prp1, 0,
                                   value->length, (void*) __demand.write, 
                                   (void*) d_req, false);

    /*
     * This write shouldn't complete until __demand.write has completed,
     * so we give it a high timestamp.
     *
     * Won't work if you're using this code sometime in the year 2554.
     */

	ret->nsecs_target = U64_MAX;
	ret->status = NVME_SC_SUCCESS;

	return true;
}

static bool conv_append(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;
	struct conv_ftl *conv_ftl = &conv_ftls[0];

	/* wbuf and spp are shared by all instances */
	struct ssdparams *spp = &conv_ftl->ssd->sp;

	struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;
    struct nvme_kv_command tmp = *cmd;

	uint64_t nsecs_latest;
	uint64_t nsecs_xfer_completed;

    struct request d_req;
    KEYT key;

    /*
     * Haven't done this the new way yet.
     */

    BUG_ON(true);

    memset(&d_req, 0x0, sizeof(d_req));

    d_req.ssd = conv_ftl->ssd;
    d_req.req = req;
    d_req.hash_params = NULL;

    uint8_t klen = cmd_key_length(tmp);
    uint32_t vlen = cmd_value_length(*cmd);

    key.key = NULL;
    key.key = (char*)kzalloc(klen + 1, GFP_KERNEL);

    BUG_ON(!key.key);
    BUG_ON(!cmd->kv_store.key);
    BUG_ON(!cmd);

    NVMEV_ASSERT(vlen > klen);

    memcpy(key.key, cmd->kv_append.key, klen);
    key.key[klen] = '\0';
    key.len = klen;
    d_req.key = key;

    NVMEV_INFO("Append for key %s (%llu)  klen %u vlen %u\n", 
                key.key, *(uint64_t*) key.key, klen, vlen);

    struct value_set *value;
    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->value = (char*)kzalloc(spp->pgsz, GFP_KERNEL);
    value->ssd = conv_ftl->ssd;

    /*
     * This length will be overwritten in the read, so we don't use it to
     * store the length of the append.
     */

    value->length = vlen;
    
    /*
     * We use this field for the length of the data to be appended.
     */

    d_req.target_len = vlen;
    d_req.target_buf = kzalloc(spp->pgsz, GFP_KERNEL);

    d_req.value = value;
    d_req.end_req = &end_w;
    d_req.sqid = req->sq_id;

    //nsecs_latest = nsecs_xfer_completed = __demand.append(&d_req);

    if(d_req.ppa == UINT_MAX - 3) {
        /*
         * We couldn't append because the resulting value would have been
         * over a page in size. The user must read this value themselves
         * and shrink it somehow if they wish to append further.
         */
        cmd->kv_store.rsvd = UINT_MAX;
        NVMEV_INFO("Append failure TS %llu.\n", ret->nsecs_target);
        ret->nsecs_target = req->nsecs_start;
        ret->status = KV_ERR_BUFFER_SMALL;
    } else {
        /*
         * write() puts the KV pair in the memory buffer, which is flushed to
         * disk at a later time.
         *
         * We set rsvd to UINT_MAX here so that in __do_perform_io_kv we skip
         * a memory copy to virt's reserved disk memory (since this KV pair isn't
         * actually on the disk yet).
         *
         * Even if this pair causes a flush of the write buffer, that's done 
         * asynchronously and the copy to virt's reserved disk memory happens
         * in nvmev_io_worker().
         */
        cmd->kv_store.rsvd = UINT_MAX;
        NVMEV_INFO("Append success TS %llu.\n", ret->nsecs_target);
        ret->nsecs_target = nsecs_latest;
        ret->status = NVME_SC_SUCCESS;
    }

	return true;
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
    uint32_t remaining = cmd_value_length(*cmd);
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
	struct conv_ftl *conv_ftls = (struct conv_ftl *)ns->ftls;

	start = local_clock();
	latest = start;
	for (i = 0; i < ns->nr_parts; i++) {
		latest = max(latest, ssd_next_idle_time(conv_ftls[i].ssd));
	}

	NVMEV_DEBUG_VERBOSE("%s: latency=%u\n", __func__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
	return;
}

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

bool kv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;

    switch (cmd->common.opcode) {
        case nvme_cmd_kv_batch:
            break;
            //ret->nsecs_target = conv_batch(ns, req, ret);
        case nvme_cmd_kv_store:
            ret->nsecs_target = conv_write(ns, req, ret, false);
            //NVMEV_DEBUG("%d, %u, %u\n", cmd_value_length(*((struct nvme_kv_command *)cmd)),
            //        __get_wallclock(), ret->nsecs_target);
            break;
        case nvme_cmd_kv_retrieve:
            ret->nsecs_target = conv_read(ns, req, ret);
            //NVMEV_DEBUG("%d, %u, %u\n", cmd_value_length(*((struct nvme_kv_command *)cmd)),
            //        __get_wallclock(), ret->nsecs_target);
            break;
        case nvme_cmd_kv_delete:
            ret->nsecs_target = conv_delete(ns, req, ret);
            //NVMEV_DEBUG("%d, %u, %u\n", cmd_value_length(*((struct nvme_kv_command *)cmd)),
            //        __get_wallclock(), ret->nsecs_target);
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
            //NVMEV_ERROR("%s: command not implemented: %s (0x%x)\n", __func__,
            //        nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
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
