// SPDX-License-Identifier: GPL-2.0-only

#include <linux/delay.h>
#include <linux/ktime.h>
#include <linux/kthread.h>
#include <linux/mutex.h>
#include <linux/random.h>
#include <linux/sched/clock.h>
#include <linux/sort.h>
#include <linux/xarray.h>

#include "city.h"
#include "fifo.h"
#include "nvmev.h"
#include "demand_ftl.h"
#include "hashset.h"

#ifndef ORIGINAL
#include "btree.h"
#endif

spinlock_t entry_spin;
spinlock_t te_spin;
spinlock_t ev_spin;
spinlock_t wfc_spin;
spinlock_t v_spin;
spinlock_t inv_spin;
spinlock_t lm_spin;
spinlock_t inv_m_spin;
atomic_t gcing;

/*
 * fastmode parameters -> for filling the device quickly.
 */
struct demand_shard* __fast_fill_shard;

/*
 * This isn't stricly necessary, but is here to try to avoid
 * a situation where the exact same page is read repeatedly
 * and the channel runs out of space for requests.
 *
 */
struct hashset *cached_pages;

void schedule_internal_operation(int sqid, unsigned long long nsecs_target,
				 struct buffer *write_buffer, unsigned int buffs_to_release);

void __warn_not_found(char* key, uint8_t klen) {
    char k[255];

    memcpy(k, key, klen);
    k[klen] = '\0';

    NVMEV_DEBUG("Key %s (%llu) not found.\n", k, *(uint64_t*) k);
}

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
}

static uint64_t forground_gc(struct demand_shard *demand_shard);
inline uint64_t check_and_refill_write_credit(struct demand_shard *demand_shard)
{
	struct write_flow_control *wfc = &(demand_shard->wfc);
    uint64_t nsecs_completed = 0;

	if ((int32_t) wfc->write_credits <= (int32_t) 0) {
		if(forground_gc(demand_shard) == 0) {
            return nsecs_completed;
        }
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

    NVMEV_INFO("Giving line %d to %u\n", curline->id, io_type);

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
spinlock_t map_spin;

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

    if(io_type == MAP_IO) {
        spin_lock(&map_spin);
    }

    struct ppa p;
    p.g.ch = wpp->ch;
    p.g.lun = wpp->lun;
    p.g.pl = wpp->pl;
    p.g.blk = wpp->blk;
    p.g.pg = wpp->pg;

    struct nand_block *blk = get_blk(demand_shard->ssd, &p);
    
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

    spin_lock(&lm_spin);
	/* move current line to {victim,full} line list */
	if (wpp->curline->igc == 0) {
		/* all pgs are still valid, move to full line list */
		NVMEV_ASSERT(wpp->curline->ipc == 0);
		list_add_tail(&wpp->curline->entry, &lm->full_line_list);
		lm->full_line_cnt++;
		NVMEV_ERROR("wpp: move line %d to full_line_list\n", wpp->curline->id);
	} else {
		NVMEV_ERROR("wpp: line %d is moved to victim list PQ\n", wpp->curline->id);
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
    spin_unlock(&lm_spin);

    if(io_type == MAP_IO || io_type == GC_MAP_IO) {
        wpp->curline->map = true;
    } else {
        wpp->curline->map = false;
    }

    if(!wpp->curline) {
        return false;
    }

	NVMEV_ERROR("wpp: got new clean line %d for type %u\n", 
                 wpp->curline->id, io_type);

	wpp->blk = wpp->curline->id;
	check_addr(wpp->blk, spp->blks_per_pl);

	/* make sure we are starting from page 0 in the super block */
	NVMEV_ASSERT(wpp->pg == 0);
	NVMEV_ASSERT(wpp->lun == 0);
	NVMEV_ASSERT(wpp->ch == 0);
	/* TODO: assume # of pl_per_lun is 1, fix later */
	NVMEV_ASSERT(wpp->pl == 0);
out:
    if(io_type == MAP_IO) {
        spin_unlock(&map_spin);
    }

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

#ifndef ORIGINAL
/*
 * Plus's in-memory per-superblock invalid mapping buffers.
 */
char **inv_mapping_bufs;

/*
 * The current offset for each invalid mapping buffer.
 */
uint64_t *inv_mapping_offs;

/*
 * Plus's per-page invalid count.
 */
uint8_t *pg_inv_cnt;
#endif

void demand_init(struct demand_shard *shard, uint64_t size, 
                 struct ssd* ssd) 
{
    struct ssdparams *spp = &ssd->sp;

#ifndef ORIGINAL
    pg_inv_cnt = (uint8_t*) vmalloc_node(spp->tt_pgs * sizeof(uint8_t),
                                         numa_node_id());
    NVMEV_ASSERT(pg_inv_cnt);
    memset(pg_inv_cnt, 0x0, spp->tt_pgs * sizeof(uint8_t));

    inv_mapping_bufs = 
    (char**) kzalloc_node(spp->tt_lines * sizeof(char*), GFP_KERNEL, 
                          numa_node_id());
    inv_mapping_offs = 
    (uint64_t*) kzalloc_node(spp->tt_lines * sizeof(uint64_t), GFP_KERNEL,
                             numa_node_id());

    for(int i = 0; i < spp->tt_lines; i++) {
        inv_mapping_bufs[i] =
            (char*) vmalloc(INV_PAGE_SZ);
        inv_mapping_offs[i] = 0;
        NVMEV_ASSERT(inv_mapping_bufs[i]);
    }
#endif

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

#ifdef ORIGINAL
    uint64_t tt_grains = spp->tt_pgs * GRAIN_PER_PAGE; 
    shard->grain_bitmap = (bool*) vmalloc(tt_grains * sizeof(bool));
    memset(shard->grain_bitmap, 0x0, tt_grains * sizeof(bool));
#endif
    shard->dram  = ((uint64_t) nvmev_vdev->config.cache_dram_mb) << 20;

    init_cache(&shard->cache, spp->tt_pgs, shard->dram);

    /*
     * Since fast mode is called from main.c with no knowledge of the
     * underlying FTL structures, not sure if there's a better way to do
     * this than setting a global here.
     */

    __fast_fill_shard = shard;
    shard->fastmode = false;

    shard->offset = 0;

    atomic_set(&shard->candidates, 0);
    atomic_set(&shard->have_victims, 0);

	cached_pages = hashset_create();
}

void demand_free(struct demand_shard *shard) {
    struct ssdparams *spp = &shard->ssd->sp;

    destroy_cache(&shard->cache);

#ifndef ORIGINAL
    vfree(pg_inv_cnt);

    for(int i = 0; i < spp->tt_lines; i++) {
        vfree(inv_mapping_bufs[i]);
    }

    kfree(inv_mapping_bufs);
    kfree(inv_mapping_offs);
#else
    vfree(shard->grain_bitmap);
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
	remove_lines(demand_shard);
}

static void conv_init_params(struct convparams *cpp)
{
	cpp->op_area_pcent = OP_AREA_PERCENT;
#ifdef ORIGINAL
	cpp->gc_thres_lines = 8; /* (host write, gc, map, map gc)*/
    cpp->gc_thres_lines_high = 2; /* (host write, gc, map, map gc)*/
#else
	cpp->gc_thres_lines = 8; /* (host write, gc, map, map gc)*/
    cpp->gc_thres_lines_high = 2; /* (host write, gc, map, map gc)*/
#endif
	cpp->enable_gc_delay = 1;
	cpp->pba_pcent = (int)((1 + cpp->op_area_pcent) * 100);
}

int bg_gc_t(void*);
int bg_ev_t(void*);

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

	demand_shards = kmalloc_node(sizeof(struct demand_shard) * nr_parts, GFP_KERNEL, 
                                 numa_node_id());
	for (i = 0; i < nr_parts; i++) {
		ssd = kmalloc_node(sizeof(struct ssd), GFP_KERNEL, numa_node_id());
		ssd_init(ssd, &spp, cpu_nr_dispatcher);
		conv_init_ftl(i, &demand_shards[i], &cpp, ssd);
	}

    //spin_lock_init(&inv_m_spin);
    spin_lock_init(&lm_spin);

    demand_shards[0].bg_gc_t = kthread_create(bg_gc_t, &demand_shards[0], "bg_gc");
	if (nvmev_vdev->config.cpu_nr_bg_gc != -1)
		kthread_bind(demand_shards[0].bg_gc_t, nvmev_vdev->config.cpu_nr_bg_gc);
    wake_up_process(demand_shards[0].bg_gc_t);

    demand_shards[0].bg_ev_t = kthread_create(bg_ev_t, &demand_shards[0], "bg_ev");
	if (nvmev_vdev->config.cpu_nr_ev_t != -1)
		kthread_bind(demand_shards[0].bg_ev_t, nvmev_vdev->config.cpu_nr_ev_t);
    wake_up_process(demand_shards[0].bg_ev_t);
    
    nvmev_vdev->space_used = 0;

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		kfree(demand_shards[i].ssd->pcie->perf_model);
		kfree(demand_shards[i].ssd->pcie);
		kfree(demand_shards[i].ssd->write_buffer);

		demand_shards[i].ssd->pcie = demand_shards[0].ssd->pcie;
		demand_shards[i].ssd->write_buffer = demand_shards[0].ssd->write_buffer;
	}

    vb.head = 0;
    vb.tail = 0;

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

    NVMEV_INFO("Removing namespace.\n");

    if (!IS_ERR_OR_NULL(demand_shards[0].bg_gc_t)) {
        kthread_stop(demand_shards[0].bg_gc_t);
        demand_shards[0].bg_gc_t = NULL;
    }

    if (!IS_ERR_OR_NULL(demand_shards[0].bg_ev_t)) {
        kthread_stop(demand_shards[0].bg_ev_t);
        demand_shards[0].bg_ev_t = NULL;
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
	struct nand_page *pg = NULL;

    //NVMEV_INFO("Marking PPA %u invalid\n", ppa2pgidx(demand_shard, ppa));

	/* update corresponding page status */
	pg = get_pg(demand_shard->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);
	pg->status = PG_INVALID;

#ifndef ORIGINAL
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
        NVMEV_ERROR("Tried to convert PPA %llu\n", ppa_);
    }
	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

static int __grain2lineid(struct demand_shard *shard, uint64_t grain) {
    struct ssdparams *spp = &shard->ssd->sp;
    struct ppa p = ppa_to_struct(spp, G_IDX(grain));
    return get_line(shard, &p)->id;
}

/*
 * Only to be called after mark_page_valid.
 */
void mark_grain_valid(struct demand_shard *shard, uint64_t grain, uint32_t len) {
	struct ssdparams *spp = &shard->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

    spin_lock(&v_spin);

    uint64_t page = G_IDX(grain);
    //NVMEV_ERROR("Marking grain %llu valid length %u in PPA %llu\n", 
    //            grain, len, page);
    struct ppa ppa = ppa_to_struct(spp, page);

	/* update page status */
	pg = get_pg(shard->ssd, &ppa);

    if(pg->status != PG_VALID) {
        NVMEV_ERROR("Page %llu was %d grain %llu len %u\n", 
                     page, pg->status, grain, len);
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

    spin_lock(&lm_spin);
	/* update corresponding line status */
	line = get_line(shard, &ppa);
	//NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    
    if(line->vgc < 0 || line->vgc > spp->pgs_per_line * GRAIN_PER_PAGE) {
        NVMEV_ERROR("Line %d VGC %d!!!!\n", line->id, line->vgc);
    }
    
    NVMEV_ASSERT(line->vgc >= 0 && line->vgc <= spp->pgs_per_line * GRAIN_PER_PAGE);
    line->vgc += len;
    spin_unlock(&lm_spin);

    //NVMEV_ERROR("Marking grain %llu length %u in PPA %llu line %d valid shard %llu vgc %u\n", 
    //            grain, len, page, line->id, shard->id, line->vgc);

#ifdef ORIGINAL
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

    spin_unlock(&v_spin);
}

#ifdef ORIGINAL
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

    spin_lock(&inv_spin);

    //NVMEV_ERROR("Marking grain %llu length %u in PPA %llu invalid shard %llu\n", 
    //            grain, len, page, shard->id);

    struct ppa ppa = ppa_to_struct(spp, page);

	/* update corresponding page status */
	pg = get_pg(shard->ssd, &ppa);

    if(pg->status != PG_VALID) {
        NVMEV_ERROR("PPA %u %lld! Grain %llu len %u\n", pg->status, page, grain, len);
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

    spin_lock(&lm_spin);
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
        NVMEV_ERROR("Inserting line %d to PQ vgc %d\n", line->id, line->vgc);
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }

	//NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    if(line->vgc < 0 || line->vgc > spp->pgs_per_line * GRAIN_PER_PAGE) {
        NVMEV_INFO("Line %d's VGC was %u\n", line->id, line->vgc);
    }
    NVMEV_ASSERT(line->vgc >= 0 && line->vgc <= spp->pgs_per_line * GRAIN_PER_PAGE);
    spin_unlock(&lm_spin);

    //if(grain_bitmap[grain] == 0) {
    //    NVMEV_INFO("Caller is %pS\n", __builtin_return_address(0));
    //    NVMEV_INFO("Caller is %pS\n", __builtin_return_address(1));
    //    NVMEV_INFO("Caller is %pS\n", __builtin_return_address(2));
    //}

#ifdef ORIGINAL
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

    spin_unlock(&inv_spin);
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
        shard->stats.inv_m_w += spp->pgsz * spp->pgs_per_oneshotpg;

        //schedule_internal_operation(req->sq_id, nsecs_completed, wbuf,
        //        spp->pgs_per_oneshotpg * spp->pgsz);
    }

    return nsecs;
}

#ifndef ORIGINAL

/*
 * This is where Plus records invalid hash index to grain mappings
 * in invalid mappings buffers. It is called on each overwrite of a pair.
 */
static uint64_t __record_inv_mapping(struct demand_shard *shard, lpa_t lpa, 
                                     ppa_t ppa, uint64_t *credits) {
    struct ssdparams *spp = &shard->ssd->sp;
    struct gc_data *gcd = &shard->gcd;

    struct ppa p = ppa_to_struct(spp, G_IDX(ppa));
    struct line* l = get_line(shard, &p); 
    uint64_t line = (uint64_t) l->id;
    uint64_t nsecs_completed = 0;
    uint64_t **oob = shard->oob;

    spin_lock(&inv_m_spin);
    //NVMEV_ERROR("Got an invalid LPA %u PPA %u mapping line %llu (%llu)\n", 
    //             lpa, ppa, line, inv_mapping_offs[line]);

    if((inv_mapping_offs[line] + sizeof(lpa) + sizeof(ppa)) > INV_PAGE_SZ) {
        /*
         * This buffer is full, flush it to an invalid mapping page.
         * Anything bigger complicates implementation. Keep to pgsz for now.
         */
        NVMEV_ASSERT(INV_PAGE_SZ == spp->pgsz);

skip:
        spin_lock(&ev_spin);
        struct ppa n_p = get_new_page(shard, MAP_IO);
        uint64_t pgidx = ppa2pgidx(shard, &n_p);

        NVMEV_ASSERT(pg_inv_cnt[pgidx] == 0);
        advance_write_pointer(shard, MAP_IO);
        mark_page_valid(shard, &n_p);
        spin_unlock(&ev_spin);
        mark_grain_valid(shard, PPA_TO_PGA(ppa2pgidx(shard, &n_p), 0), GRAIN_PER_PAGE);

        if(pgidx == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
            goto skip;
        }

        NVMEV_INFO("Flushing an invalid mapping page for line %llu off %llu to PPA %llu\n", 
                     line, inv_mapping_offs[line], pgidx);

        /*
         * GC will see UINT_MAX at grain 0 and know it's an invalid mapping page.
         */
        oob[pgidx][0] = UINT_MAX;
        /*
         * The superblock this invalid mapping page is targeting.
         */
        oob[pgidx][1] = line;
        oob[pgidx][2] = pgidx;

        for(int i = 3; i < GRAIN_PER_PAGE; i++) {
            oob[pgidx][i] = UINT_MAX;
        }

        uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
        uint64_t off = shard_off + (pgidx * spp->pgsz);
        uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;

        memcpy(ptr, inv_mapping_bufs[line], spp->pgsz);
        nsecs_completed = __maybe_advance(shard, &n_p, MAP_IO, 0);

        NVMEV_DEBUG("Added %u (%u %u) to XA.\n", (line << 32) | pgidx, line, pgidx);

        /*
         * This xarray stores the all of the current invalid mapping
         * pages on disk. Shifting line by 32 allows us to iterate
         * over the invalid mapping page addresses for a certain superblock
         * during GC.
         */
        xa_store(&gcd->inv_mapping_xa, (line << 32) | pgidx, 
                  xa_mk_value(pgidx), GFP_KERNEL);
        
        memset(inv_mapping_bufs[line], 0x0, INV_PAGE_SZ);
        inv_mapping_offs[line] = 0;

        if(credits) {
            /*
             * We wrote a page here, so consume some credits.
             * Credits are used for flow control between foreground
             * writes and GC.
             */
            (*credits) += GRAIN_PER_PAGE;
        }
    }

    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &lpa, sizeof(lpa));
    inv_mapping_offs[line] += sizeof(lpa);
    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &ppa, sizeof(ppa));
    inv_mapping_offs[line] += sizeof(ppa);

    spin_unlock(&inv_m_spin);

    return nsecs_completed;
}
#endif

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
        //NVMEV_ERROR("Marking PPA %u free\n", ppa2pgidx(shard, &ppa_copy) + i);
		pg->status = PG_FREE;

#ifndef ORIGINAL
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

static struct line *select_victim_line(struct demand_shard *demand_shard, bool bg)
{
	struct ssdparams *spp = &demand_shard->ssd->sp;
	struct line_mgmt *lm = &demand_shard->lm;
	struct line *victim_line = NULL, *dummy = NULL;

    spin_lock(&lm_spin);

again:
	victim_line = pqueue_peek(lm->victim_line_pq);

	if (!victim_line) {
        spin_unlock(&lm_spin);
		return NULL;
	}

    if(victim_line->id == 
       __grain2lineid(demand_shard, demand_shard->offset / GRAINED_UNIT)) {
        /*
         * In a rare case, the victim line chosen here can actually
         * be the same line as the current line that stores will
         * be directed to.
         *
         * This happens when we call get_new_page, then
         * advance_write_pointer in store, and then GC starts.
         * advance_write_pointer placed the line on the victim line
         * PQ if it was called on the last page in the line. Depending
         * on its priority, GC can select it as the victim.
         *
         * We don't want GC and stores to happen on the same line,
         * because we generally assume a lot of the structures like the
         * grain bitmap and OOB don't receive modifications to the same
         * entries at the same time.
         *
         * Just pop the line, get another line, then place it back on the
         * queue.
         */
        dummy = victim_line;
        pqueue_pop(lm->victim_line_pq); 
        goto again;
    }    

	pqueue_pop(lm->victim_line_pq);
	victim_line->pos = 0;
	lm->victim_line_cnt--;

    NVMEV_DEBUG("Took victim line %d off the pq\n", victim_line->id);
	NVMEV_DEBUG("ipc=%d(%d),igc=%d(%d),victim=%d,full=%d,free=%d\n", 
		    victim_line->ipc, victim_line->vpc, victim_line->igc, victim_line->vgc,
            demand_shard->lm.victim_line_cnt, demand_shard->lm.full_line_cnt, 
            demand_shard->lm.free_line_cnt);

    if(dummy) {
        pqueue_insert(lm->victim_line_pq, dummy);
    }

    spin_unlock(&lm_spin);
	/* victim_line is a danggling node now */
	return victim_line;
}

uint64_t clean_first_half = 0;
uint64_t clean_second_half = 0;
uint64_t clean_third_half = 0;
uint64_t mapping_searches = 0;

/*
 * Plus's transient GC hash table, containing invalid
 * hash index to grain mappings.
 */
struct inv_entry {
    uint64_t key; // This will be used as the key in the hash table
    struct hlist_node node; // Hash table uses hlist_node to chain items
};
DEFINE_HASHTABLE(inv_m_hash, 22); // 2^8 buckets = 256 buckets

uint64_t __get_inv_mappings(struct demand_shard *shard, uint64_t line) {
#ifdef ORIGINAL
    return 0;
#else
    /*
     * Where Plus gets it invalid hash index to grain
     * mappings before GC.
     */
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &shard->ssd->sp;
    struct gc_data *gcd = &shard->gcd;
    struct ppa p;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off;
    uint8_t *ptr;

    int hsize = 0;
    unsigned long index;

    /*
     * We want to get the physical addresses of invalid mapping pages
     * that target the line (superblock) we are collecting.
     *
     * Range scan the xarray between start and end.
     */
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
    //spin_lock(&inv_m_spin);
    xa_for_each_range(&gcd->inv_mapping_xa, index, xa_entry, start, end) {
        uint64_t m_ppa = xa_to_value(xa_entry);

        off = shard_off + (m_ppa * spp->pgsz);
        ptr = nvmev_vdev->ns[0].mapped + off;

        NVMEV_DEBUG("Reading mapping page from PPA %llu (idx %lu)\n", m_ppa, index);

        p = ppa_to_struct(spp, m_ppa);
        swr.ppa = &p;
        nsecs_completed = ssd_advance_nand(ssd, &swr);
        nsecs_latest = max(nsecs_latest, nsecs_completed);

        shard->stats.inv_m_r += spp->pgsz;

        for(int j = 0; j < INV_PAGE_SZ / (uint32_t) INV_ENTRY_SZ; j++) {
            /*
             * An invalid hash index to grain mapping.
             */
            lpa_t lpa = *(lpa_t*) (ptr + (j * INV_ENTRY_SZ));
            ppa_t ppa = *(lpa_t*) (ptr + (j * INV_ENTRY_SZ) + 
                                         sizeof(lpa_t));

            if(lpa == UINT_MAX) {
                continue;
            }

            NVMEV_INFO("%s XA 1 Inserting inv LPA %u PPA %u "
                        "(%llu) in GC for line %llu\n", 
                        __func__, lpa, ppa, ((uint64_t) ppa << 32) | lpa,
                        line);

            struct inv_entry *entry;
            entry = kmalloc_node(sizeof(*entry), GFP_KERNEL, numa_node_id());
            entry->key = ((uint64_t) ppa << 32) | lpa;

            struct inv_entry *item;
            struct hlist_node *next;
            bool deleted = false;

            hash_for_each_possible_safe(inv_m_hash, item, next, node, entry->key) {
                if (item->key == entry->key) {
                    NVMEV_ASSERT(false);
                    NVMEV_INFO("Found dupe LPA %u PPA %u\n", lpa, ppa);
                    hash_del(&item->node);
                    deleted = true;
                    break;
                }
            } 

            if(deleted) {
                continue;
            }

            /*
             * Add it to the hash table.
             */
            hash_add(inv_m_hash, &entry->node, entry->key);
            hsize++;
        }

        NVMEV_DEBUG("Erasing %lu from XA.\n", index);
        xa_erase(&gcd->inv_mapping_xa, index);

        /*
         * We don't need this page anymore.
         */
        mark_grain_invalid(shard, PPA_TO_PGA(m_ppa, 0), GRAIN_PER_PAGE);
    }

    NVMEV_DEBUG("Copying %lld (%lld %lu) inv mapping pairs from mem.\n",
                inv_mapping_offs[line] / INV_ENTRY_SZ, 
                inv_mapping_offs[line], INV_ENTRY_SZ);

    /*
     * The current in-memory invalid mapping buffer.
     */
    spin_lock(&inv_m_spin);
    for(int j = 0; j < inv_mapping_offs[line] / (uint32_t) INV_ENTRY_SZ; j++) {
        lpa_t lpa = *(lpa_t*) (inv_mapping_bufs[line] + (j * INV_ENTRY_SZ));
        ppa_t ppa = *(ppa_t*) (inv_mapping_bufs[line] + (j * INV_ENTRY_SZ) + 
                                     sizeof(lpa_t));

        if(lpa == UINT_MAX) {
            continue;
        }

        NVMEV_INFO("%s XA 2 off %lu Inserting inv LPA %u PPA %u "
                    "(%llu)\n", 
                    __func__, j * INV_ENTRY_SZ, lpa, ppa, ((uint64_t) ppa << 32) | lpa);

        struct inv_entry *entry;
        entry = kmalloc_node(sizeof(*entry), GFP_KERNEL, numa_node_id());
        entry->key = ((uint64_t) ppa << 32) | lpa;
        hash_add(inv_m_hash, &entry->node, entry->key);
        hsize++;
    }

    NVMEV_ERROR("Hsize was %d\n", hsize);

    inv_mapping_offs[line] = 0;
    spin_unlock(&inv_m_spin);

    return nsecs_completed;
#endif
}

void __clear_inv_mapping(struct demand_shard *demand_shard, unsigned long key) {
#ifdef ORIGINAL
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
#ifdef ORIGINAL
    return true;
#else
    /*
     * Where Plus determines if the KV pair at a grain is valid or not.
     * If we find it in inv_m_hash, that means it was previously invalidated.
     */
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
    return valid;
#endif
}

void __update_mapping_ppa(struct demand_shard *demand_shard, uint64_t new_ppa, 
                          uint64_t line) {
#ifdef ORIGINAL
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
}

uint64_t user_pgs_this_gc = 0;
uint64_t gc_pgs_this_gc = 0;
uint64_t map_pgs_this_gc = 0;
uint64_t map_gc_pgs_this_gc = 0;

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
            nsecs_completed = ssd_advance_nand(shard->ssd, &gcw);
        }

        //if (last_pg_in_wordline(shard, &new_ppa)) {
        //    schedule_internal_operation(UINT_MAX, nsecs_completed, NULL,
        //                                spp->pgs_per_oneshotpg * spp->pgsz);
        //}
    }

    return nsecs_completed;
}

bool __new_gc_ppa(struct demand_shard *shard, bool map) {
    struct gc_data *gcd;
    uint64_t pgidx;
    bool wrote = false;

    gcd = &shard->gcd;

    if(__maybe_write(shard, &gcd->gc_ppa, map) > 0) {
        wrote = true;
    }

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

    return wrote;
}

/*
 * We found an invalid mapping page during GC.
 * This page targets a superblock that we aren't currently GCing.
 * It's still live, so we need to copy it somewhere else.
 */
void __copy_inv_map(struct demand_shard *shard, uint64_t old_grain, 
                    uint32_t target_line) {
    struct ssdparams *spp;
    struct cache *cache;
    struct ht_section *ht;
    struct gc_data *gcd;
    struct ppa ppa;
    uint32_t offset;
    uint64_t pgidx;
    uint64_t **oob;
    uint32_t len;
    uint32_t grain;

    spp = &shard->ssd->sp;
    cache = &shard->cache;
    gcd = &shard->gcd;
    oob = shard->oob;
    len = GRAIN_PER_PAGE;

    if(gcd->offset >= GRAIN_PER_PAGE) {
        if(__new_gc_ppa(shard, true)) {
            shard->stats.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
        }
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

        for(int i = offset; i < GRAIN_PER_PAGE; i++) {
            oob[pgidx][i] = UINT_MAX;
        }

        uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                GRAINED_UNIT);

        if(__new_gc_ppa(shard, true)) {
            shard->stats.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
        }

        goto again;
    }

    NVMEV_INFO("Inv map grain %u in copy PPA %u\n",
                grain, G_IDX(grain));
    mark_grain_valid(shard, grain, len);

    uint64_t copy_to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
    uint64_t copy_from = (G_IDX(old_grain) * spp->pgsz) + 
                         (G_OFFSET(old_grain) * GRAINED_UNIT);
    /*
     * This is an invalid mapping page, which are always the
     * size of a full page. We don't need to set any of the OOB
     * except the first.
     */
#ifdef ORIGINAL
    NVMEV_ASSERT(false);
#endif

    NVMEV_ASSERT(offset == 0);

    oob[pgidx][0] = UINT_MAX;
    oob[pgidx][1] = target_line;
    oob[pgidx][2] = pgidx;

    for(int i = 3; i < GRAIN_PER_PAGE; i++) {
        oob[pgidx][i] = UINT_MAX;
    }

    memcpy(nvmev_vdev->ns[0].mapped + copy_to,
           nvmev_vdev->ns[0].mapped + copy_from, len * GRAINED_UNIT);

    __update_mapping_ppa(shard, pgidx, target_line);
    gcd->offset += GRAIN_PER_PAGE;
}

void __copy_map(struct demand_shard *shard, lpa_t idx, uint32_t len) {
    struct ssdparams *spp;
    struct cache *cache;
    struct ht_section *ht;
    struct gc_data *gcd;
    struct ppa ppa;
    uint32_t offset;
    uint64_t pgidx;
    uint64_t **oob;
    uint64_t grain;

    spp = &shard->ssd->sp;
    cache = &shard->cache;
    gcd = &shard->gcd;
    oob = shard->oob;

    if(gcd->offset >= GRAIN_PER_PAGE) {
        if(__new_gc_ppa(shard, true)) {
            shard->stats.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
        }
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
         */
        NVMEV_ASSERT(offset > 0);
        mark_grain_valid(shard, PPA_TO_PGA(pgidx, offset), 
                GRAIN_PER_PAGE - offset);
        //mark_grain_invalid(shard, PPA_TO_PGA(pgidx, offset), 
        //        GRAIN_PER_PAGE - offset);

        for(int i = offset; i < GRAIN_PER_PAGE; i++) {
            oob[pgidx][i] = UINT_MAX;
        }

        //uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        //memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
        //       GRAINED_UNIT);

        if(__new_gc_ppa(shard, true)) {
            shard->stats.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
        }

        goto again;
    }

    mark_grain_valid(shard, grain, len);

    ht = cache->ht[idx];
    atomic_set(&ht->t_ppa, pgidx);
    ht->g_off = gcd->offset;

    //NVMEV_ERROR("Moved IDX %u len %u to PPA %llu grain %llu\n", idx, len, pgidx,
    //             PPA_TO_PGA(pgidx, ht->g_off));

    oob[pgidx][offset] = ((uint64_t) len << 32) | IDX2LPA(idx);

    gcd->offset += len;
}

void __copy_valid_pair(struct demand_shard *shard, lpa_t lpa, uint32_t len,
                       struct lpa_len_ppa *out, uint32_t out_idx,
                       struct ht_section *ht, uint32_t pos,
                       int gc_line) {
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
        if(__new_gc_ppa(shard, false)) {
            shard->stats.data_w_dgc += spp->pgsz * spp->pgs_per_oneshotpg;
        }
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

        //uint64_t to = (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        //memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
        //        GRAINED_UNIT);

        if(__new_gc_ppa(shard, false)) {
            shard->stats.data_w_dgc += spp->pgsz * spp->pgs_per_oneshotpg;
        }

        goto again;
    }

    NVMEV_INFO("Valid grain %llu pos %u in copy len %u PPA %llu LPA %u line %d\n",
                grain, pos, len, G_IDX(grain), lpa, __grain2lineid(shard, grain));
    mark_grain_valid(shard, grain, len);

    oob[pgidx][offset] = ((uint64_t) len << 32) | lpa;
    gcd->offset += len;

    uint32_t before;

#ifdef ORIGINAL
    before = atomic_read(&ht->mappings[pos].ppa);
#else
    struct root *root;
    root = (struct root*) ht->mappings;
    btree_direct_read(root, pos, &before, sizeof(before));
#endif

    if(__grain2lineid(shard, before) == gc_line) {
        NVMEV_INFO("SAME LINE Valid grain %llu in copy len %u PPA %llu LPA %u line %d\n",
                grain, len, G_IDX(grain), lpa, __grain2lineid(shard, grain));
        /*
         * If this pair is still on the line that's being garbage collected
         * at this point, we can try to switch with the new grain. If it's
         * not on the line anymore, it means it has already been updated
         * in store and we can skip this update.
         *
         * Update the ppa if and only if the value is the same as before,
         * otherwise it has been updated outside of here (in store)
         * and the update will fail, which is fine as the updates
         * in store take precedence over GC copies.
         */

#ifdef ORIGINAL
        atomic_cmpxchg(&ht->mappings[pos].ppa, before, grain);
#else
        btree_insert(ht, root, lpa, grain, pos);
#endif
    } else {
        NVMEV_ASSERT(false);
        NVMEV_INFO("MOVED LINE to grain %u Valid grain %llu in copy len %u PPA %llu LPA %u line %d\n",
                    before, grain, len, G_IDX(grain), lpa, __grain2lineid(shard, grain));
    }
}

uint32_t shadow_idx[GRAIN_PER_PAGE * FLASH_PAGE_SIZE];
uint32_t shadow_idx_idx = 0;
bool __shadow_read(struct cache *cache, uint32_t ppa) {
    for(int i = 0; i < shadow_idx_idx; i++) {
        if(atomic_read(&cache->ht[shadow_idx[i]]->t_ppa) == ppa) {
            return true;
        }
    }
    return false;
}

uint64_t clearing = 0, clear_count = 0, copying = 0;
/* here ppa identifies the block we want to clean */
void clean_one_flashpg(struct demand_shard *shard, struct ppa *ppa)
{
    struct ssdparams *spp = &shard->ssd->sp;
    struct convparams *cpp = &shard->cp;
    struct cache *cache = &shard->cache;
    struct gc_data *gcd = &shard->gcd;
	struct nand_page *pg_iter = NULL;
	int page_cnt = 0, cnt = 0, i = 0, len = 0;
	uint64_t reads_done = 0, pgidx = 0;
	struct ppa ppa_copy = *ppa;
    struct line* l = get_line(shard, ppa); 
    uint64_t **oob = shard->oob;

    uint64_t tt_rewrite = 0;
    bool mapping_line = gcd->map;
    //uint64_t idx;
    uint64_t lpa_len_idx = 0;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint32_t gcs = 0;
    uint64_t nsecs_completed = 0;

    uint64_t start = 0, end = 0, clear_start = 0, copy_start = 0;

    start = ktime_get();
	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(shard->ssd, &ppa_copy);
        pgidx = ppa2pgidx(shard, &ppa_copy);

#ifdef ORIGINAL
        if(i == 0 && page_grains_invalid(shard, pgidx)) {
            mark_page_invalid(shard, &ppa_copy);
        }
#endif

        /* there shouldn't be any free page in victim blocks */
        NVMEV_ASSERT(pg_iter->status != PG_FREE);
        nvmev_vdev->space_used -= spp->pgsz;
		if (pg_iter->status == PG_VALID) {
            page_cnt++;
        } else if(pg_iter->status == PG_INVALID) {
#ifndef ORIGINAL
            NVMEV_ASSERT(pg_inv_cnt[pgidx] == GRAIN_PER_PAGE);
#endif
            ppa_copy.g.pg++;
            NVMEV_INFO("1 skipping PPA %llu in GC.\n", pgidx);
            continue;
#ifdef ORIGINAL
        }
#else
        } else if(pg_inv_cnt[pgidx] == GRAIN_PER_PAGE) {
            NVMEV_ASSERT(pg_iter->status == PG_INVALID);
            ppa_copy.g.pg++;
            NVMEV_INFO("2 skipping PPA %llu in GC.\n", pgidx);
            continue;
        }
#endif

        NVMEV_INFO("Cleaning PPA %llu\n", pgidx);
        for(int i = 0; i < GRAIN_PER_PAGE; i++) {
            uint64_t grain = PPA_TO_PGA(pgidx, i);
#ifdef ORIGINAL
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
                 * Edge case where in store we got the last
                 * page of a line for the offset, and GC thinks the
                 * line is available to garbage collect because all
                 * of the pages have been used.
                 */
                shard->offset = 0;
                //NVMEV_INFO("Set offset to 0 grain %llu!!!\n",
                //            grain);
            }

            NVMEV_DEBUG("Grain %llu (%d)\n", grain, i);
            if(i == 0 && mapping_line && oob[pgidx][i] == UINT_MAX) {
#ifdef ORIGINAL
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

                shard->stats.inv_m_w += spp->pgsz;
                shard->stats.inv_m_r += spp->pgsz;

                mark_grain_invalid(shard, grain, GRAIN_PER_PAGE);
                i += GRAIN_PER_PAGE;
#endif
            } else if(mapping_line && valid_g) {
                clear_start = ktime_get();
                clear_count++;

                /*
                 * A grain containing live hash index to grain mapping 
                 * information.
                 */

                uint32_t lpa = __lpa_from_oob(oob[pgidx][i]);
                uint32_t idx = IDX(lpa);

                NVMEV_ASSERT(oob[pgidx][i] != UINT_MAX);
                NVMEV_ASSERT(idx > 0);

                struct ht_section *ht;
                ht = cache_get_ht(cache, lpa);
 
#ifdef ORIGINAL
                len = GRAIN_PER_PAGE;
#else
                len = __glen_from_oob(oob[pgidx][i]);
#endif

#ifdef ORIGINAL
                NVMEV_ASSERT(ht->g_off == 0 && i == 0);
#endif
                uint32_t t_ppa = atomic_read(&ht->t_ppa);
                if(t_ppa != pgidx || ht->g_off != i) {
                    NVMEV_DEBUG("CMT IDX %u moved from PPA %llu to PPA %u\n", 
                                 idx, pgidx, t_ppa);
                    i += len - 1;
                    atomic_set(&ht->outgoing, 0);
                    continue;
                }

                NVMEV_ASSERT(ht->len_on_disk == len);

                NVMEV_INFO("CMT IDX %u was %u grains in size from grain %llu PPA %llu (%u)\n", 
                             idx, len, grain, G_IDX(grain + i), t_ppa);
                mark_grain_invalid(shard, grain, len);
                __copy_map(shard, idx, len);
                i += len - 1;

				atomic_set(&ht->outgoing, 0);
                clearing += ktime_to_us(ktime_get()) - ktime_to_us(clear_start);
            } else if(!mapping_line && valid_g) {
#ifndef ORIGINAL
                NVMEV_ASSERT(pg_inv_cnt[pgidx] <= GRAIN_PER_PAGE);
#endif
                NVMEV_ASSERT(!mapping_line);
                
                uint64_t lpa = __lpa_from_oob(oob[pgidx][i]);
                len = __glen_from_oob(oob[pgidx][i]);

                struct ht_section *ht;
                ht = cache_get_ht(cache, lpa);

                if(__valid_mapping(shard, lpa, grain)) {
                    if(!ht->mappings) {
                        /*
                         * CMT isn't cached, but bringing it in through the usual
                         * evict/collect path greatly increases complexity, because
                         * now we need two threads potentially calling evict at the
                         * same time (instead of the background thread collecting 
                         * victims and foreground thread performing the evictions). 
                         *
                         * Skip putting it on the regular fifo and don't update
                         * nr_cached_tentries.
                         *
                         * Instead, assume it's stored in some sort of reserved
                         * area. The limits of the size of this reserved area are
                         * GRAIN_PER_PAGE * FLASH_PAGE_SIZE, because each grain could
                         * have a KV pair from a different CMT. With a 32K
                         * flash and 64B grains, that's 512 pages, which is 4MB.
                         */
                        if(!__shadow_read(cache, atomic_read(&ht->t_ppa))) {
                            //NVMEV_ERROR("Added IDX %u LPA %llu grain %d PPA %u pgidx %llu to shadow.\n", 
                            //             ht->idx, lpa, i, atomic_read(&ht->t_ppa), pgidx);
                            struct ppa p = ppa_to_struct(spp, atomic_read(&ht->t_ppa));
                            struct nand_cmd gcr = {
                                .type = GC_IO,
                                .cmd = NAND_READ,
                                .stime = 0,
                                .xfer_size = spp->pgsz,
                                .interleave_pci_dma = false,
                                .ppa = &p,
                            };
                            shard->stats.trans_r_dgc += spp->pgsz;
                            ssd_advance_nand(shard->ssd, &gcr);
                        }
                        ht->mappings = (struct h_to_g_mapping*) ht->mem;
                        shadow_idx[shadow_idx_idx++] = ht->idx;
                    }

                    uint32_t pos = UINT_MAX;
                    struct h_to_g_mapping pte = cache_hidx_to_grain(ht, lpa, &pos);

                    //NVMEV_ERROR("Got valid mapping from IDX %u\n", ht->idx);

                    if(atomic_read(&pte.ppa) != grain) {
                        NVMEV_INFO("IDX %u LPA %llu grain %llu PPA %llu modified during GC.",
                                ht->idx, lpa, grain, G_IDX(grain));
#ifndef ORIGINAL
                        /*
                         * We enter this path because while we were
                         * garbage collecting this line, this pair was
                         * updated in store. This means it created an 
                         * invalid mapping entry for this line, even though
                         * it's currently being garbage collected. We only
                         * collect invalid mapping entries right at the
                         * beginning of GC, and thus we miss this newly created
                         * entry.
                         *
                         * The next time we garbage collect this line, this
                         * incorrect invalid mapping entry will be there.
                         * In a rare case, it's possible that the exact same
                         * LPA goes to the exact same grain, and if its valid
                         * the copy would be skipped.
                         *
                         * Re-record this invalid mapping entry so it's seen 
                         * as a duplicate when we call get_inv_mappings the 
                         * next time this line is garbage collected.
                         * Duplicates are seen as invalid entries and are
                         * removed from the invalid mapping search table.
                         */
                        __record_inv_mapping(shard, lpa, grain, NULL);
#endif
                    } else {
                        //NVMEV_INFO("Invalidating LPA %llu grain %llu line %d\b", 
                        //            lpa, grain, __grain2lineid(shard, grain));
                        mark_grain_invalid(shard, grain, len);
                        __copy_valid_pair(shard, lpa, len, NULL,
                                lpa_len_idx, ht, pos, l->id);
                        lpa_len_idx++;
                    }
                }

                //atomic_dec(&ht->outgoing);
                atomic_set(&ht->outgoing, 0);
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

    if(mapping_line) {
        shard->stats.trans_r_tgc += spp->pgsz * page_cnt;
    }

    /*
     * Place extra mapping tables we read during GC onto the FIFO for
     * later eviction.
     */
    struct ht_section *ht;
    for(int i = 0; i < shadow_idx_idx; i++) {
        ht = cache_get_ht(cache, shadow_idx[i] * EPP);
        fifo_enqueue(cache->fifo, ht);

        spin_lock(&entry_spin);
        cache->nr_cached_tentries += ht->len_on_disk;
        spin_unlock(&entry_spin);

		//NVMEV_ERROR("Removed IDX %u from shadow.\n", ht->idx);
        atomic_set(&ht->outgoing, 0);
    }

    shadow_idx_idx = 0;
    clean_third_half += ktime_to_us(end) - ktime_to_us(start);

    return;
}

static void mark_line_free(struct demand_shard *demand_shard, struct ppa *ppa)
{
	struct line_mgmt *lm = &demand_shard->lm;
	struct line *line = get_line(demand_shard, ppa);

    NVMEV_DEBUG("Marking line %d free\n", line->id);

    spin_lock(&lm_spin);

	line->ipc = 0;
	line->vpc = 0;
    line->igc = 0;
    line->vgc = 0;
	/* move this line to free line list */
	list_add_tail(&line->entry, &lm->free_line_list);
	lm->free_line_cnt++;

    spin_unlock(&lm_spin);
}

static uint64_t do_gc(struct demand_shard *shard, bool bg)
{
	struct line *victim_line = NULL;
	struct ssdparams *spp = &shard->ssd->sp;
    struct convparams *cpp = &shard->cp;
	struct ppa ppa;
    struct gc_data *gcd = &shard->gcd;
    struct cache *cache;
	int flashpg;
    uint64_t pgidx;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

    uint64_t map = 0, total = 0, cleaning = 0, freeing = 0;

    ktime_t gc_start, start, gc_end, end;

	victim_line = select_victim_line(shard, bg);
	if (!victim_line) {
		return UINT_MAX;
	}

    gc_start = ktime_get();

    gcd->map = victim_line->map;
    cache = &shard->cache;

    user_pgs_this_gc = gc_pgs_this_gc = map_gc_pgs_this_gc = map_pgs_this_gc = 0;

	ppa.g.blk = victim_line->id;
	NVMEV_ERROR("%s GC-ing %s line:%d,ipc=%d(%d),igc=%d(%d),victim=%d,full=%d,free=%d\n", 
            bg ? "BACKGROUND" : "FOREGROUND", gcd->map? "MAP" : "USER", ppa.g.blk,
		    victim_line->ipc, victim_line->vpc, victim_line->igc, victim_line->vgc,
            shard->lm.victim_line_cnt, shard->lm.full_line_cnt, 
            shard->lm.free_line_cnt);

    if(gcd->map) {
        shard->stats.tgc_cnt++;
    } else {
        shard->stats.dgc_cnt++;
    }

	shard->wfc.credits_to_refill = victim_line->igc;
#ifndef ORIGINAL
    //start = ktime_get();
    if(victim_line->vgc > 0 && !gcd->map) {
        nsecs_completed = __get_inv_mappings(shard, victim_line->id);
    }
    //end = ktime_get();
    //map = ktime_to_us(end) - ktime_to_us(start);
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
				lunp = get_lun(shard->ssd, &ppa);

                //start = ktime_get();
                if(victim_line->vgc > 0) {
                    clean_one_flashpg(shard, &ppa);
                }
                //end = ktime_get();
                //cleaning += ktime_to_us(end) - ktime_to_us(start);

				if (flashpg == (spp->flashpgs_per_blk - 1)) {
					struct convparams *cpp = &shard->cp;

                    //start = ktime_get();
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
                    //end = ktime_get();
                    //freeing += ktime_to_us(end) - ktime_to_us(start);

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
#ifdef ORIGINAL
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
                    shard->stats.trans_w_tgc += spp->pgsz * spp->pgs_per_oneshotpg;
                } else {
                    shard->stats.data_w_dgc += spp->pgsz * spp->pgs_per_oneshotpg;
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

    /* update line status */
	mark_line_free(shard, &ppa);
#ifndef ORIGINAL
    if(!gcd->map) {
        __clear_gc_data(shard);
    }
#endif

    gc_end = ktime_get();
    total = ktime_to_us(gc_end) - ktime_to_us(gc_start);

    NVMEV_ASSERT(user_pgs_this_gc == 0);
    NVMEV_ERROR("%llu user %llu GC %llu map GC this round. %lu pgs_per_line."
               " Took %llu microseconds (%llu map %llu clean %llu free).\n"
               " Clean breakdown %llu first half %llu second half %llu third half"
               " %llu time spent searching %llu clearing %llu copying %llu clear_count.", 
                user_pgs_this_gc, gc_pgs_this_gc, map_gc_pgs_this_gc, 
                spp->pgs_per_line, total, map, cleaning, freeing,
                clean_first_half, clean_second_half, clean_third_half, 
                mapping_searches, clearing, copying, clear_count);

    clean_first_half = clean_second_half = clean_third_half = mapping_searches = 0;
    clearing = copying = clear_count = 0;

    NVMEV_ERROR("Leaving GC for line %d \n", victim_line->id);

	return nsecs_latest;
}

#define VICTIM_RB_SZ 131072
#define MAX_SEARCH 131072

static inline uint32_t __entries_to_grains(struct demand_shard *shard,
                                           struct ht_section *ht) {
    struct ssdparams *spp;
    uint32_t cnt;
    uint32_t ret;

    spp = &shard->ssd->sp;
#ifdef ORIGINAL
    return GRAIN_PER_PAGE;
#else
    cnt = ht->cached_cnt;
    ret = (cnt * ENTRY_SIZE) / GRAINED_UNIT;

    if((cnt * ENTRY_SIZE) % GRAINED_UNIT) {
        ret++;
    }

    return ret > ht->len_on_disk ? ret : ht->len_on_disk;
#endif
}

void __mark_dirty(struct ht_section *ht) {
    if(ht->state == C_CANDIDATE) {
        ht->state = D_CANDIDATE;
    } else if(ht->state != D_CANDIDATE) {
        ht->state = DIRTY;
    }
}

static void __collect_victims(struct demand_shard *shard, uint32_t target_g) {
    struct cache *cache;
    struct ht_section *victim;
    uint32_t g_len;
    bool all_clean;
    int dist;
    atomic_t *candidates, *have_victims;

    cache = &shard->cache;
    all_clean = true;
    candidates = &shard->candidates;
    have_victims = &shard->have_victims;

    dist = vb.head - vb.tail;
    if(dist < 0) {
        dist += VICTIM_RB_SZ;
    }

    if(dist > (GRAIN_PER_PAGE * 10)) {
        atomic_set(have_victims, 1);
        NVMEV_ERROR("Set victims because dist was %d\n", dist);
        return;
    }

again:
	victim = fifo_dequeue(cache->fifo);

    for(int i = 0; i < MAX_SEARCH; i++) {
        if(!victim) {
            break;
        }

        while(atomic_cmpxchg(&victim->outgoing, 0, 1) != 0) {
            cpu_relax();
        }

        NVMEV_ASSERT(victim->state != C_CANDIDATE &&
                     victim->state != D_CANDIDATE);

        if(victim->state == DIRTY) {
            all_clean = false;

            /*
             * After being selected for eviction, a CMT can be expanded.
             * This means that the current count we have here of how many
             * grains we are evicting will be incorrect.
             *
             * Mark the victim as CANDIDATE, so that during expansion
             * we can update the count accordingly.
             */
            victim->state = D_CANDIDATE;
        } else {
            victim->state = C_CANDIDATE;
        }

        g_len = __entries_to_grains(shard, victim);

#ifdef ORIGINAL
        NVMEV_ASSERT(g_len == GRAIN_PER_PAGE);
#endif

        NVMEV_ASSERT(victim);

        vb.hts[vb.head] = victim;
        vb.head = (vb.head + 1) % VICTIM_RB_SZ;

        atomic_add(g_len, candidates);

        //NVMEV_ERROR("Added IDX %u len %u to head %u. Count is now %u\n", 
        //             victim->idx, g_len, vb.head - 1, 
        //             atomic_read(candidates));

        if(atomic_read(candidates) >= target_g) {
            atomic_set(&victim->outgoing, 0);
            break;
        }
    
        atomic_set(&victim->outgoing, 0);
        victim = fifo_dequeue(cache->fifo);
    }

    if(atomic_read(candidates) < target_g) {
        if(kthread_should_stop()) {
            return;
        }

        cond_resched();
        goto again;
    }

    //NVMEV_ERROR("Really got %u victims u head %d.\n", 
    //             atomic_read(candidates), vb.head);
    atomic_set(have_victims, 1);

    return;
}

int bg_ev_t(void *data) {
    struct demand_shard *shard;
    struct cache *cache;
    struct write_flow_control *wfc;
    uint32_t threshold;
    atomic_t *have_victims;

    NVMEV_INFO("Started background eviction thread.\n");

    shard = (struct demand_shard*) data;
    cache = &shard->cache;
    wfc = &(shard->wfc);
    threshold = cache->max_cached_tentries >> 2;
    have_victims = &shard->have_victims;

    while(!kthread_should_stop()) {
        if(atomic_read(have_victims) == 0) {
            atomic_set(&shard->candidates, 0);
            __collect_victims(shard, GRAIN_PER_PAGE);
        }
        cond_resched();
    }

    NVMEV_INFO("Evict thread returning!\n");

    return 0;
}

int bg_gc_t(void *data) {
    struct demand_shard *shard;
    struct cache *cache;
    struct write_flow_control *wfc;

    NVMEV_INFO("Started background GC thread.\n");

    shard = (struct demand_shard*) data;
    cache = &shard->cache;
    wfc = &(shard->wfc);

    while(!kthread_should_stop()) {
        if(!should_gc(shard)) {
            cond_resched();
            continue;
        }

        if(atomic_cmpxchg(&gcing, 0, 1) != 0) {
            cond_resched();
            continue;
        }

        if(do_gc(shard, true) != UINT_MAX) {
            spin_lock(&wfc_spin);
            wfc->write_credits += wfc->credits_to_refill;
            spin_unlock(&wfc_spin);
        }

        atomic_set(&gcing, 0);
        cond_resched();
    }

    NVMEV_INFO("Background thread returning!\n");

    return 0;
}

uint32_t loops = 0;
static uint64_t forground_gc(struct demand_shard *demand_shard)
{
    uint64_t nsecs_completed = 0, nsecs_latest = 0;
    ktime_t start, end;
    uint64_t total;

	while(should_gc_high(demand_shard)) {
        cpu_relax();
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
    struct ht_section *ht;
    uint64_t idx;
    struct ssdparams *spp;
    uint32_t grain;
    uint32_t len;
};

#ifndef ORIGINAL
struct expand_w_args {
    struct demand_shard *shard;
    struct ssdparams *spp;
    struct ht_section *ht;
    struct h_to_g_mapping *old;
    struct h_to_g_mapping *new;
    struct h_to_g_mapping pte;
    uint32_t len;
};

static inline uint32_t __need_expand(struct ht_section *ht) {
    uint32_t expand_to = (ht->cached_cnt * ENTRY_SIZE) / GRAINED_UNIT;

    if((ht->cached_cnt * ENTRY_SIZE) % GRAINED_UNIT) {
        expand_to++;
    }

    if(expand_to < ht->len_on_disk) {
        return false;
    }

    return (ht->cached_cnt * ENTRY_SIZE) % GRAINED_UNIT == 0;
}

/*
 * This function just assigns a new physical page
 * to the mapping entry. Some space is wasted if the mapping
 * entry doesn't contain enough entries to fill a page, but
 * later it will be packed into a page with other mapping
 * entries during eviction.
 */
static void __expand_map_entry(struct demand_shard *shard, 
                               struct ht_section *ht, struct h_to_g_mapping pte,
                               void *mem) {
    struct cache *cache;
    struct ssdparams *spp;
    struct ppa p;
    uint64_t ppa;
    uint64_t **oob;
    uint32_t t_ppa;

    cache = &shard->cache;
    spp = &shard->ssd->sp;
    oob = shard->oob;
    t_ppa = atomic_read(&ht->t_ppa);

    NVMEV_ERROR("Invalidating IDX %u PPA %u grain %llu len %u before expand.\n", 
                ht->idx, t_ppa, PPA_TO_PGA(t_ppa, ht->g_off),
                ht->len_on_disk);
    mark_grain_invalid(shard, PPA_TO_PGA(t_ppa, ht->g_off), ht->len_on_disk);

    /*
     * Mapping table entries can't be over a page in size.
     */
    NVMEV_ASSERT(ht->len_on_disk < GRAIN_PER_PAGE);

skip:
    spin_lock(&ev_spin);
    p = get_new_page(shard, MAP_IO);
    ppa = ppa2pgidx(shard, &p);

    //BUG_ON(!ht->lru_ptr);

    advance_write_pointer(shard, MAP_IO);
    mark_page_valid(shard, &p);
    spin_unlock(&ev_spin);
    mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

    if(ppa == 0) {
        mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
        goto skip;
    }

    atomic_set(&ht->t_ppa, ppa);
    ht->g_off = 0;
    ht->len_on_disk++;
    spin_lock(&entry_spin);
    cache->nr_cached_tentries++;
    spin_unlock(&entry_spin);

    if(ht->state == C_CANDIDATE) {
        atomic_inc(&shard->candidates);
        ht->state = D_CANDIDATE;
    } else if(ht->state == D_CANDIDATE) {
        atomic_inc(&shard->candidates);
    } else {
        ht->state = DIRTY;
    }

    //NVMEV_INFO("Moved IDX %u to PPA %u in expansion.\n", 
    //            ht->idx, ppa);

    oob[ppa][0] = ((uint64_t) ht->len_on_disk << 32) | (ht->idx * EPP);
    for(int i = ht->len_on_disk; i < GRAIN_PER_PAGE; i++) {
        oob[ppa][i] = UINT_MAX;
    }

    if(GRAIN_PER_PAGE - ht->len_on_disk > 0) {
        mark_grain_invalid(shard, PPA_TO_PGA(ppa, ht->len_on_disk), 
                           GRAIN_PER_PAGE - ht->len_on_disk);
    }
}
#endif

static void __update_map(struct demand_shard *shard, struct ht_section *ht,
                         lpa_t lpa, void* mem, 
                         struct h_to_g_mapping pte, uint32_t pos) {
    struct cache *cache = &shard->cache;

    NVMEV_ASSERT(ht->mappings);
    __mark_dirty(ht);
#ifdef ORIGINAL
    ht->mappings[OFFSET(lpa)] = pte;
    ht->pair_mem[OFFSET(lpa)] = mem;
#else
	struct root *root;
    uint32_t max;

    root = (struct root*) ht->mappings;
    max = root->cnt * IN_LEAF;

    if(ht->cached_cnt == max && pos == UINT_MAX) {
        __expand_map_entry(shard, ht, pte, mem);
        btree_expand(root);
        btree_reload(ht, root, UINT_MAX, UINT_MAX);
    }

	if(pos != UINT_MAX) {
        ppa_t old_ppa;
        btree_direct_read(root, pos, &old_ppa, sizeof(old_ppa));
        __record_inv_mapping(shard, lpa, old_ppa, NULL);
    }

    btree_insert(ht, root, lpa, atomic_read(&pte.ppa), pos);
    ht->pair_mem[OFFSET(lpa)] = mem;
    return;
#endif
}

static uint64_t __stime_or_clock(uint64_t stime) {
    uint64_t clock = __get_wallclock();
    return clock > stime ? clock : stime;
}

static uint64_t __evict_one(struct demand_shard *shard, struct nvmev_request *req,
                            uint64_t stime, uint64_t *credits) {
    struct cache *cache;
    struct ssdparams *spp;
    struct ht_section* victim;
    uint64_t **oob;
    bool got_ppa, got_lock;
    uint32_t grain, g_len, cnt;
    uint32_t t_ppa;
    struct ppa p;
    ppa_t ppa;
    bool all_clean;
    uint64_t nsecs_completed;
    uint32_t evicted;
    atomic_t *have_victims;

	uint64_t start = 0, end = 0;

    cache = &shard->cache;
    spp = &shard->ssd->sp;
    oob = shard->oob;
    got_ppa = got_lock = false;
    grain = g_len = cnt = 0;
    ppa = UINT_MAX;
    all_clean = true;
    nsecs_completed = 0;
    evicted = 0;
    have_victims = &shard->have_victims;

	start = ktime_get();
    while(atomic_cmpxchg(have_victims, 1, 2) != 1) {
        cpu_relax();
    }
    end = ktime_get();

    if(ktime_to_us(end) - ktime_to_us(start) > 5) {
        NVMEV_ERROR("Collecting %u victims took %llu %d cached\n", cnt, 
                ktime_to_us(end) - ktime_to_us(start), 
                cache->nr_cached_tentries);
    } 

    while(cache->nr_cached_tentries > 
          cache->max_cached_tentries - GRAIN_PER_PAGE) {
        if(grain == GRAIN_PER_PAGE) {
            NVMEV_ERROR("EVICTION: we wrote a full page.\n");
new_page:
            p = ppa_to_struct(spp, ppa);
            struct nand_cmd swr = {
                .type = USER_IO,
                .cmd = NAND_WRITE,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            };

            if (!shard->fastmode && last_pg_in_wordline(shard, &p)) {
                swr.stime = __stime_or_clock(stime);
                swr.ppa = &p;
                shard->stats.trans_w += spp->pgsz * spp->pgs_per_oneshotpg;
                nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
            }

            grain = 0;
            got_ppa = false;
        }

        victim = vb.hts[vb.tail]; 

        NVMEV_ASSERT(victim);
        NVMEV_ASSERT(victim->mem);
        NVMEV_ASSERT(victim->state == C_CANDIDATE || 
                     victim->state == D_CANDIDATE);

        if(!got_lock) {
            cache_get_ht(cache, victim->idx * EPP);
            got_lock = true;
        }

        g_len = __entries_to_grains(shard, victim);
        if(victim->state == D_CANDIDATE && grain + g_len > GRAIN_PER_PAGE) {
            //NVMEV_ERROR("IDX %u was too big! New page!\n", 
            //        victim->idx);

            mark_grain_valid(shard, PPA_TO_PGA(ppa, grain), GRAIN_PER_PAGE - grain);
            //mark_grain_invalid(shard, PPA_TO_PGA(ppa, grain), GRAIN_PER_PAGE - grain);

            for(int i = grain; i < GRAIN_PER_PAGE; i++) {
                oob[ppa][i] = UINT_MAX;
            }

            goto new_page;
        }

        vb.tail = (vb.tail + 1) % VICTIM_RB_SZ;
        t_ppa = atomic_read(&victim->t_ppa);

        //NVMEV_ERROR("Got victim with IDX %u len %u from tail %d in eviction.\n", 
        //             victim->idx, g_len, vb.tail - 1);

        if (victim->state == D_CANDIDATE) {
            bool same_size = g_len == victim->len_on_disk;
#ifdef ORIGINAL
            NVMEV_ASSERT(same_size);
#endif
            if(same_size) { 
                /*
                 * same_size holds true if this mapping table entry hasn't expanded.
                 * If it previously expanded, the grains that it mapped too
                 * were already marked invalid, and the above condition will
                 * be false as len_on_disk will be greater.
                 */
                //NVMEV_ERROR("Trying to mark IDX %u TPPA %u len %u off %llu invalid.\n",
                //             victim->idx, t_ppa, victim->len_on_disk, 
                //             victim->g_off);
                NVMEV_ASSERT(victim->len_on_disk > 0);
                mark_grain_invalid(shard, PPA_TO_PGA(t_ppa, victim->g_off), 
                        victim->len_on_disk);
            }

#ifdef ORIGINAL
            NVMEV_ASSERT(!got_ppa);
            NVMEV_ASSERT(victim->len_on_disk == GRAIN_PER_PAGE);
#endif
skip:
            uint64_t prev_ppa = t_ppa;
            if(!got_ppa) {
                spin_lock(&ev_spin);
                p = get_new_page(shard, MAP_IO);
                ppa = ppa2pgidx(shard, &p);

                advance_write_pointer(shard, MAP_IO);
                mark_page_valid(shard, &p);
                spin_unlock(&ev_spin);

                if(ppa == 0) {
                    mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
                    mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
                    goto skip;
                }

                if(credits) {
                    (*credits) += GRAIN_PER_PAGE;
                }
            }

            NVMEV_DEBUG("Assigned PPA %u (old %u) grain %u to victim at IDX %u in %s. Prev PPA %llu\n",
                    ppa, t_ppa, grain, victim->idx, __func__, prev_ppa);

            got_ppa = true;

#ifdef ORIGINAL
            NVMEV_ASSERT(victim->g_off == 0);
            NVMEV_ASSERT(victim->len_on_disk == GRAIN_PER_PAGE);

            grain = 0;
            g_len = spp->pgsz / GRAINED_UNIT;
#else
            g_len = __entries_to_grains(shard, victim);

            //NVMEV_ERROR("Reduced cached tentries to %d\n", 
            //             cmbr->nr_cached_tentries);

            victim->g_off = grain;
            victim->len_on_disk = g_len;

            NVMEV_DEBUG("Set the length on disk for IDX %u to %u.\n", victim->idx,
                    g_len);
#endif
            /*
             * The IDX is used during GC so that we know which CMT entry
             * to update.
             */

            oob[ppa][grain] = ((uint64_t) g_len << 32) | (victim->idx * EPP);
            NVMEV_DEBUG("Set OOB PPA %u grain %u to IDX %u\n", ppa, grain, victim->idx);

            NVMEV_ASSERT(grain < GRAIN_PER_PAGE);
            mark_grain_valid(shard, PPA_TO_PGA(ppa, grain), g_len);

            atomic_set(&victim->t_ppa, ppa);

            grain += g_len;

            //NVMEV_ERROR("Evicted DIRTY mapping entry IDX %u in %s.\n",
            //        victim->idx, __func__);
            all_clean = false;
            shard->stats.dirty_evict++;
        } else {
            //NVMEV_ERROR("Evicted CLEAN mapping entry IDX %u in %s.\n",
            //        victim->idx, __func__);
            shard->stats.clean_evict++;
        }

        evicted += g_len;

        spin_lock(&entry_spin);
        cache->nr_cached_tentries -= g_len;
        spin_unlock(&entry_spin);

        victim->mem = NULL;
        victim->state = CLEAN;

        got_lock = false;
        atomic_set(&victim->outgoing, 0);

        if(evicted >= GRAIN_PER_PAGE) {
            break;
        }
    }
    end = ktime_get();
    //NVMEV_ERROR("Total took %lluus %d cached.\n", 
    //            ktime_to_us(end) - ktime_to_us(start),
    //            cmbr->nr_cached_tentries);

//out:
    if(!all_clean && grain < GRAIN_PER_PAGE) {
        /*
         * We couldn't fill a page with mapping entries. Clear the rest of
         * the page.
         */

        NVMEV_ASSERT(GRAIN_PER_PAGE - grain > 0);
        mark_grain_valid(shard, PPA_TO_PGA(ppa, grain), GRAIN_PER_PAGE - grain);
        //mark_grain_invalid(shard, PPA_TO_PGA(ppa, grain), GRAIN_PER_PAGE - grain);

        for(int i = grain; i < GRAIN_PER_PAGE; i++) {
            oob[ppa][i] = UINT_MAX;
        }
    }

    if(got_ppa) {
        p = ppa_to_struct(spp, ppa);
        struct nand_cmd swr = {
            .type = USER_IO,
            .cmd = NAND_WRITE,
            .interleave_pci_dma = false,
            .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
        };

        if (!shard->fastmode && last_pg_in_wordline(shard, &p)) {
            swr.stime = __stime_or_clock(stime);
            swr.ppa = &p;
            shard->stats.trans_w += spp->pgsz * spp->pgs_per_oneshotpg;
            nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
        }
    }

    //NVMEV_ERROR("Evicted %d items %d cached tail %d.\n", 
    //             evicted, cmbr->nr_cached_tentries, vb.tail);
    atomic_set(have_victims, 0);

    return nsecs_completed;
}

uint64_t __get_one(struct demand_shard *shard, struct ht_section *ht,
                   bool first, uint64_t stime, bool *missed) {
    struct cache *cache;
    struct ssdparams *spp;
    struct ht_section* victim;
    uint64_t nsecs_completed = 0;
    uint32_t grain, t_ppa;
    uint64_t **oob;

    cache = &shard->cache;
    spp = &shard->ssd->sp;
    grain = ht->g_off;
	oob = shard->oob;
    t_ppa = atomic_read(&ht->t_ppa);

	//if(!shard->fastmode) {
	//	while(atomic_read(&ht->outgoing) == 1) {
	//		cpu_relax();
	//	}
	//}

    if(!first) {
        ht->mappings = (struct h_to_g_mapping*) ht->mem;
        NVMEV_DEBUG("__get_one for IDX %u CMT PPA %u\n", ht->idx, t_ppa);

        /*
         * If this wasn't the first access of this mapping
         * table page, we need to read it from disk.
         */
        if(!shard->fastmode) {
            struct ppa p = ppa_to_struct(spp, t_ppa);
            struct nand_cmd srd = {
                .type = USER_IO,
                .cmd = NAND_READ,
                .stime = __stime_or_clock(stime),
                .interleave_pci_dma = false,
                .ppa = &p,
                .xfer_size = spp->pgsz,
            };

            nsecs_completed = ssd_advance_nand(shard->ssd, &srd);
            shard->stats.trans_r += spp->pgsz;
        }
        //NVMEV_INFO("Sent a read for CMT PPA %u\n", ht->t_ppa);
    } else {
        ht->mappings = kzalloc_node(spp->pgsz, GFP_KERNEL, numa_node_id());
        ht->mem = ht->mappings;

        NVMEV_ASSERT(ht->mappings);
#ifdef ORIGINAL
        ht->len_on_disk = GRAIN_PER_PAGE;
        ht->g_off = 0;
        
        for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
            atomic_set(&ht->mappings[i].ppa, UINT_MAX);
            ht->pair_mem[i] = NULL;
        }
#else
        btree_init((struct root*) ht->mappings);

        ht->len_on_disk = ORIG_GLEN;
        ht->g_off = 0;

        for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
        //    ht->mappings[i].lpa = UINT_MAX;
        //    atomic_set(&ht->mappings[i].ppa, UINT_MAX);
            ht->pair_mem[i] = NULL;
        }
#endif
    }

    //ht->lru_ptr = 
    fifo_enqueue(cache->fifo, (void*) ht);
    //NVMEV_ERROR("Added IDX %u len %u in get_one.\n", 
    //             ht->idx, ht->len_on_disk);
    spin_lock(&entry_spin);
    cache->nr_cached_tentries += ht->len_on_disk;
    spin_unlock(&entry_spin);

    *missed = true;
    shard->stats.cache_miss++;

    return nsecs_completed;
}

uint32_t prev_ch = 0;
static uint64_t __read_and_compare(struct demand_shard *shard, ppa_t grain,
                                   void* mem, struct hash_params *h_params, 
                                   char *key_from_user, uint32_t u_klen,
                                   uint64_t stime, uint64_t *nsecs) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    uint64_t nsecs_completed;
    uint64_t local;
    uint64_t ppa = G_IDX(grain);
    uint64_t offset = G_OFFSET(grain);
    char *key_on_disk;

    NVMEV_ASSERT(mem);

    if(ppa > spp->tt_pgs) {
        NVMEV_INFO("Tried to convert PPA %llu\n", ppa);
    }
	NVMEV_ASSERT(ppa < spp->tt_pgs);

    struct ppa p = ppa_to_struct(spp, ppa);

	uint64_t tmp = ppa2pgidx(shard, &p);
	NVMEV_ASSERT(tmp == ppa);

    if(p.g.ch == prev_ch) {
        p.g.ch++;
        p.g.ch = p.g.ch % spp->nchs;
    }

    prev_ch = p.g.ch;

    struct nand_cmd swr = {
        .type = USER_IO,
        .cmd = NAND_READ,
        .interleave_pci_dma = false,
        .xfer_size = spp->pgsz,
        .ppa = &p,
    };

    if(!shard->fastmode && !hashset_contains(cached_pages, ppa)) {
		swr.stime = __stime_or_clock(stime);
		*nsecs = ssd_advance_nand(ssd, &swr);
		hashset_insert(cached_pages, ppa);
		//NVMEV_ERROR("Miss on hashset PPA %llu CH %d!\n", ppa, p.g.ch);
	} else {
		//NVMEV_ERROR("Hit on hashset PPA %llu!\n", ppa);
		*nsecs = 0;
	}

    if(!shard->fastmode) {
        //uint64_t res = ssd_advance_nand(ssd, &swr);
        //if(res > stime) {
        //    NVMEV_ERROR("This read added %llu nanoseconds to our time. cnt %d",
        //                res - stime, h_params->cnt);
        //} else {
        //    NVMEV_ERROR("WTF!!!\n");
        //}

        //*nsecs = ssd_advance_nand(ssd, &swr);
    }
    shard->stats.data_r += spp->pgsz;
    //NVMEV_INFO("Read completed %llu\n", *nsecs);


    uint8_t* ptr = mem;
    uint8_t klen = __klen_from_value(ptr);
    key_on_disk = __key_from_value(ptr);

    if(klen != u_klen) {
        shard->stats.fp_collision_w++;
        h_params->cnt++;
        return 1;
    }

    if (!strncmp(key_from_user, key_on_disk, klen)) {
        //NVMEV_INFO("Match %llu %llu when checking offset %llu.\n", 
        //*(uint64_t*) actual_key.key, *(uint64_t*) check_key.key,
        //      *(uint64_t*) ptr);

        /* hash key found -> update */
        shard->stats.fp_match_w++;
        return 0;
    } else {
        //NVMEV_INFO("Fail got %llu wanted %llu when checking offset %llu.\n", 
        //           *(uint64_t*) actual_key.key, *(uint64_t*) check_key.key,
        //           (uint64_t) ptr);

        /* retry */
        shard->stats.fp_collision_w++;
        h_params->cnt++;
        return 1;
    }
}

uint32_t get_hash_idx(struct cache *cache, void *_h_params) {
	struct hash_params *h_params = (struct hash_params *)_h_params;
again:
	h_params->lpa = PROBING_FUNC(h_params->hash, h_params->cnt) % 
                    (cache->nr_valid_tentries - 1) + 1;

    /*
     * Skip anything in mapping table entry 0 for now, as it complicates
     * things like OOB checking where 0 can be meaningful or not.
     */
    if(h_params->lpa <= EPP) {
        h_params->cnt++;
        goto again;
    }
    
	return h_params->lpa;
}


static inline uint32_t __vlen_from_value(uint8_t* mem) {
    uint8_t klen = *(uint8_t*) (mem);
    uint32_t vlen = *(uint32_t*) (mem + KLEN_MARKER_SZ + klen);
    NVMEV_ASSERT(vlen > 0);
    return vlen;
}

uint64_t __release_map(void *voidargs, uint64_t*, uint64_t*) {
    atomic_t *outgoing = (atomic_t*) voidargs;
    atomic_dec(outgoing);
    return 0;
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
    h.lpa = 0;

    uint64_t **oob = shard->oob;

    struct cache *cache = &shard->cache;
    while(cache_full(cache)) {
        nsecs_completed = __evict_one(shard, req, nsecs_latest, &credits);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        shard->stats.t_write_on_read += spp->pgsz;
    }

    consume_write_credit(shard, credits);
    check_and_refill_write_credit(shard);
    nsecs_completed = __get_wallclock();
    nsecs_latest = max(nsecs_latest, nsecs_completed);

lpa:
    lpa_t lpa = get_hash_idx(&shard->cache, &h);
    h.lpa = lpa;

    struct ht_section *ht = cache_get_ht(cache, lpa);
    uint32_t t_ppa = atomic_read(&ht->t_ppa);

    if (h.cnt > shard->max_try) {
        /*
         * max_try is the most we've hashed the same key to find an empty
         * LPA.
         */
        cmd->kv_retrieve.value_len = 0;
        cmd->kv_retrieve.rsvd = U64_MAX;

        if(nsecs_latest == nsecs_start) {
            nsecs_latest = local_clock();
        }

        __warn_not_found(cmd->kv_retrieve.key, klen);

        status = KV_ERR_KEY_NOT_EXIST;
        goto out;
    }

    if(t_ppa == UINT_MAX) {
        NVMEV_INFO("Key %s (%llu) tried to read missing CMT entry IDX %u.\n", 
                (char*) cmd->kv_store.key, *(uint64_t*) cmd->kv_store.key, ht->idx);
        h.cnt++;
        goto lpa;
    }

cache:
    if(cache_hit(ht)) { 
        struct h_to_g_mapping pte = cache_hidx_to_grain(ht, lpa, &pos);
        uint32_t g_from_pte = atomic_read(&pte.ppa);
        void* old_mem;

        //NVMEV_INFO("Read for key %llu (%llu) checks LPA %u PPA %u\n", 
        //            *(uint64_t*) (key.key), *(uint64_t*) &(cmd->kv_store.key), 
        //            lpa, pte.ppa);

        if (!IS_INITIAL_PPA(pte.ppa)) {
            shard->stats.d_read_on_read += spp->pgsz;
            old_mem = ht->pair_mem[OFFSET(lpa)];

            if(__read_and_compare(shard, g_from_pte, old_mem, &h, 
                                  cmd->kv_store.key, klen,
                                  nsecs_latest, &nsecs_completed)) {
                nsecs_latest = max(nsecs_latest, nsecs_completed);

                if (vlen <= KB(4)) {
                    nsecs_latest += spp->fw_4kb_rd_lat;
                } else {
                    nsecs_latest += spp->fw_rd_lat;
                }

                pos = UINT_MAX;
                atomic_set(&ht->outgoing, 0);
                goto lpa;
            }

            nsecs_latest = max(nsecs_latest, nsecs_completed);

            len = __vlen_from_value(old_mem);
            NVMEV_ASSERT(len > 0);

            if(!for_del) {
                cmd->kv_retrieve.value_len = len;
                cmd->kv_retrieve.rsvd = (uint64_t) old_mem;

				uint32_t _len = *(uint32_t*) (old_mem + 9);
                NVMEV_ASSERT(len > 0);

                //cmd->kv_retrieve.rsvd = ((uint64_t) pte.ppa) * GRAINED_UNIT;
            } else {
                cmd->kv_retrieve.rsvd = U64_MAX;
                mark_grain_invalid(shard, g_from_pte, 
                                   len % GRAINED_UNIT ? 
                                   (len / GRAINED_UNIT) + 1 :
                                   len / GRAINED_UNIT);
                atomic_set(&pte.ppa, UINT_MAX);
                __update_map(shard, ht, lpa, NULL, pte, pos);
            }

            if(!for_del && (vlen < len)) {
                NVMEV_ERROR("Buffer with size %u too small for value %u\n",
                             vlen, len);
                cmd->kv_retrieve.rsvd = U64_MAX;
                NVMEV_ASSERT(false);
                status = KV_ERR_BUFFER_SMALL;
            } else {
                status = NVME_SC_SUCCESS;
            }
        } else {
            NVMEV_ERROR("Miss LPA %u key %llu\n", 
                         lpa, *(uint64_t*) cmd->kv_retrieve.key);
            cmd->kv_retrieve.value_len = 0;
            cmd->kv_retrieve.rsvd = U64_MAX;
            status = KV_ERR_KEY_NOT_EXIST;
        }

        if(!missed) {
            shard->stats.cache_hit++;
        }

        goto out;
    } else if(t_ppa != UINT_MAX) {
        nsecs_completed = __get_one(shard, ht, false, nsecs_latest, &missed);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        shard->stats.t_read_on_read += spp->pgsz;
        goto cache;
    }

out:

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

    shard->stats.read_req_cnt++;

    nsecs_completed = __get_wallclock();
    nsecs_latest = max(nsecs_latest, nsecs_completed);

	ret->cb = __release_map;
	ret->args = &ht->outgoing;
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
inline bool __crossing_page(struct ssdparams *spp, uint64_t offset, uint32_t vlen) {
    if(offset % spp->pgsz == 0) {
        return true;
    }
    return !((offset % spp->pgsz) + vlen <= spp->pgsz);
}

static struct hash_params *make_hash_params(uint64_t hash) {
	struct hash_params *h_params = 
    (struct hash_params *)kzalloc_node(sizeof(struct hash_params), GFP_KERNEL, 
                                       numa_node_id());
    h_params->hash = hash;
	h_params->cnt = 0;
	h_params->lpa = 0;

	return h_params;
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

                shard->stats.d_write_on_write += spp->pgsz * spp->pgs_per_oneshotpg;
                shard->stats.data_w += spp->pgsz * spp->pgs_per_oneshotpg;
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
    }

    NVMEV_ASSERT(vlen > klen);
    NVMEV_ASSERT(vlen <= spp->pgsz);
    NVMEV_ASSERT(klen <= 16);

    glen = vlen / GRAINED_UNIT;
    grain = shard->offset / GRAINED_UNIT;
    page = G_IDX(grain);
    g_off = G_OFFSET(grain);

	if(vlen & (GRAINED_UNIT - 1)) {
		glen++;
	}

    mark_grain_valid(shard, grain, glen);
    credits += glen;

	shard->offset += vlen & ~(GRAINED_UNIT - 1);
	if(vlen & (GRAINED_UNIT - 1)) {
		shard->offset += GRAINED_UNIT;
	}

    if(need_new == true) {
        goto append;
    }

    struct hash_params h; 
    h.hash = hash;
    h.cnt = 0;
    h.lpa = 0;

    uint32_t pos = UINT_MAX;
    bool missed = false;
    bool first = false;
    struct h_to_g_mapping new_pte;
    atomic_set(&new_pte.ppa, grain);

lpa:
    lpa_t lpa = get_hash_idx(&shard->cache, &h);
    h.lpa = lpa;

#ifndef ORIGINAL
    new_pte.hidx = lpa;
#endif

    struct cache *cache = &shard->cache;
    while(cache_full(cache)) {
        nsecs_completed = __evict_one(shard, req, nsecs_latest, &credits);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        shard->stats.t_write_on_write += spp->pgsz;
    }

    consume_write_credit(shard, credits);
    check_and_refill_write_credit(shard);
    nsecs_completed = __get_wallclock();
    nsecs_latest = max(nsecs_latest, nsecs_completed);

    struct ht_section *ht = cache_get_ht(cache, lpa);
    uint32_t t_ppa = atomic_read(&ht->t_ppa);

    if(shard->fastmode) {
        if(ht->pair_mem[OFFSET(lpa)]) {
            h.cnt++;
            shard->max_try = (h.cnt > shard->max_try) ? h.cnt : 
                                   shard->max_try;
            atomic_dec(&ht->outgoing);
            goto lpa;
        } else {
            goto fm_out;
        }
    }

    if(t_ppa == UINT_MAX) {
        /*
         * Previously unused cached mapping table entry.
         */
skip:
        spin_lock(&ev_spin);
        struct ppa p = get_new_page(shard, MAP_IO);
        ppa_t ppa = ppa2pgidx(shard, &p);

        advance_write_pointer(shard, MAP_IO);

        mark_page_valid(shard, &p);
        spin_unlock(&ev_spin);

        mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        if(ppa == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
            goto skip;
        }

        atomic_set(&ht->t_ppa, ppa);
        t_ppa = ppa;

        if(!shard->fastmode) {
            ht->mappings = NULL;
        } else {
            ht->mappings = (struct h_to_g_mapping*) (nvmev_vdev->ns[0].mapped + 
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

#ifdef ORIGINAL
        oob[ppa][0] = ((uint64_t) GRAIN_PER_PAGE << 32) | (ht->idx * EPP);

        for(int i = 1; i < GRAIN_PER_PAGE; i++) {
            oob[ppa][i] = UINT_MAX;
        }

        ht->len_on_disk = GRAIN_PER_PAGE;
#else
        uint64_t glen = ORIG_GLEN;
        oob[ppa][0] = (glen << 32) | (ht->idx * EPP);

        for(int i = 1; i < GRAIN_PER_PAGE; i++) {
            oob[ppa][i] = UINT_MAX;
        }

        if(!shard->fastmode) {
            if(ORIG_GLEN < GRAIN_PER_PAGE) {
                mark_grain_invalid(shard, PPA_TO_PGA(ppa, ORIG_GLEN), 
                        GRAIN_PER_PAGE - ORIG_GLEN);
            }

            ht->len_on_disk = ORIG_GLEN;
        } else {
            ht->len_on_disk = 1;
            ht->g_off = 0;

            for(int i = 0; i < EPP; i++) {
                ht->mappings[i].hidx = UINT_MAX;
                atomic_set(&ht->mappings[i].ppa, UINT_MAX);
            }
        }
#endif

#ifndef ORIGINAL
        ht->cached_cnt = 0;
#endif
        /*
         * Will be marked dirty below in update().
         */

        ht->state = CLEAN;
        first = true;
	}

    if(shard->fastmode) {
        goto fm_out;
    }

cache:
    if(cache_hit(ht)) {
        struct h_to_g_mapping pte = cache_hidx_to_grain(ht, lpa, &pos);

        uint32_t g_from_pte = atomic_read(&pte.ppa);
        void* old_mem;

        if(!IS_INITIAL_PPA(pte.ppa)) {
            //NVMEV_INFO("Hit for LPA %u IDX %lu, got grain %u\n", lpa, IDX(lpa), pte.ppa);
            shard->stats.d_read_on_write += spp->pgsz;
            old_mem = ht->pair_mem[OFFSET(lpa)];

            if(__read_and_compare(shard, g_from_pte, old_mem, &h, 
                                  cmd->kv_retrieve.key, klen, 
                                  nsecs_latest, &nsecs_completed)) {
                nsecs_latest = max(nsecs_latest, nsecs_completed);
                missed = true;
                pos = UINT_MAX;
                atomic_set(&ht->outgoing, 0);
                goto lpa;
            }

            //NVMEV_INFO("Overwrite LPA %u PPA %u line %d\n", 
            //            lpa, pte.ppa, __grain2lineid(shard, pte.ppa));
            nsecs_latest = max(nsecs_latest, nsecs_completed);

            uint64_t old_g_off = G_OFFSET(g_from_pte);
            uint32_t len = __glen_from_oob(oob[G_IDX(g_from_pte)][old_g_off]);

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
                    atomic_set(&ht->outgoing, 0);
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
                    atomic_set(&new_pte.ppa, grain);
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
                uint64_t off = ((uint64_t) atomic_read(&pte.ppa)) * GRAINED_UNIT;
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
                NVMEV_INFO("Mismatch LPA %u grain %u: %u %llu OOB %llu\n", 
                            lpa, atomic_read(&pte.ppa), len, glen, 
                            oob[G_IDX(atomic_read(&pte.ppa))][old_g_off]);
                NVMEV_ASSERT(false);
            }

            NVMEV_ASSERT(len > 0);
            //NVMEV_INFO("OW\n");
            mark_grain_invalid(shard, g_from_pte, len);
        } else {
            pair_mem = kzalloc_node(glen * GRAINED_UNIT, GFP_KERNEL, numa_node_id());
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
            shard->stats.cache_hit++;
        }
    } else if(t_ppa != UINT_MAX) {
        nsecs_completed = __get_one(shard, ht, first, nsecs_latest, &missed);
        nsecs_latest = max(nsecs_latest, nsecs_completed);

        missed = true;

        shard->stats.t_read_on_write += spp->pgsz;
        goto cache;
    }

    oob[page][g_off] = ((uint64_t) glen << 32) | lpa;

    shard->max_try = (h.cnt > shard->max_try) ? h.cnt : shard->max_try;

    __update_map(shard, ht, lpa, pair_mem, new_pte, pos);

out:
    shard->stats.write_req_cnt++;

    //atomic_dec(&ht->outgoing);
    //NVMEV_ERROR("%s for key %llu (%llu) klen %u vlen %u grain %llu PPA %llu LPA %u\n",
    //            append ? "Append" : "Write",
    //            *(uint64_t*) (key.key), *(uint64_t*) &(cmd->kv_store.key),
    //            klen, vlen, grain, page, lpa);

    if(!shard->fastmode) {
        ret->cb = __release_map;
        ret->args = &ht->outgoing;
        ret->nsecs_target = nsecs_latest;
    } else {
fm_out:
        void* to;
        void* from = (void*) cmd->kv_store.dptr.prp1;

        oob[page][g_off] = ((uint64_t) glen << 32) | lpa;

        to = kzalloc_node(glen * GRAINED_UNIT, GFP_KERNEL, numa_node_id());
        memcpy(to, from, glen * GRAINED_UNIT);
        memmove(to + sizeof(uint8_t) + klen + sizeof(uint32_t),
                to + sizeof(uint8_t) + klen, vlen - sizeof(uint8_t) - klen);
        memcpy(to + sizeof(uint8_t) + klen, &vlen, sizeof(uint32_t));

        ht->pair_mem[OFFSET(lpa)] = to;
#ifndef ORIGINAL
        ht->fm_grains[OFFSET(lpa)] = PPA_TO_PGA(page, g_off);
#endif
        
        //NVMEV_ERROR("FM wrote LPA %u grain %llu\n", lpa, PPA_TO_PGA(page, g_off));
        atomic_dec(&ht->outgoing);
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
    //struct nvme_kv_command *cmd = (struct nvme_kv_command*) req->cmd;

    //uint8_t klen = 0;
    //uint32_t offset = 0, vlen = 0;
    //uint32_t remaining = cmd_value_length(cmd);

    ///*
    // * Not updated for new design yet.
    // */

    BUG_ON(true);

    //char* value = (char*) kzalloc_node(remaining, GFP_KERNEL, numa_node_id());
    ////__quick_copy(cmd, value, remaining);
    //
    //while(remaining > 0) {
    //    klen = *(uint8_t*) value + offset;
    //    offset += sizeof(klen);
    //    
    //    key.key = (char*)kzalloc_node(klen + 1, GFP_KERNEL, numa_node_id());     
    //    memcpy(key.key, value + offset, klen);
    //}

    //return true;
}

#ifndef ORIGINAL
#define REAP 4096
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
    struct cache* cache;
    struct ssdparams *spp;
    struct nvmev_ns *ns;
    uint64_t size;
    uint32_t vlen;
    uint32_t pairs;

    args = (struct fast_fill_args*) data;
    shard = __fast_fill_shard;
    cache = &shard->cache;
    spp = &shard->ssd->sp;
    ns = args->ns;
    size = args->size;
    vlen = args->vlen;
    pairs = args->pairs;

    shard->fastmode = true;

    vlen += sizeof(uint32_t);

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

    NVMEV_ERROR("Starting fastmode vlen %u pairs %u.\n", vlen, pairs);

    bufs = kzalloc_node(sizeof(char*) * REAP, GFP_KERNEL, numa_node_id());
    for(int i = 0; i < REAP; i++) {
        bufs[i] = kzalloc_node(vlen, GFP_KERNEL, numa_node_id());
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

        __store(ns, &req, &ret, false, false);

        if(i % REAP == 0) {
            __reclaim_completed_reqs();
        }

        if(i > 0 && ((i & 1048575) == 0)) {
            tend = ktime_get();
            uint64_t elapsed = ktime_to_ns(ktime_sub(tend, tstart)) / 1000000000;

            if(elapsed > 0) {
                uint64_t ops_s = i / elapsed;
                NVMEV_ERROR("%llu fastmode writes done. %llu elapsed. %llu ops/s\n", 
                             i, elapsed, ops_s);
            }

            cond_resched();
        }
    }

    NVMEV_ERROR("Before map.\n");

    uint64_t collision = 0;
    for(uint64_t i = 1; i < cache->nr_valid_tpages; i++) {
        struct ht_section *ht = cache->ht[i];

        if(!ht) {
            NVMEV_ERROR("CMT IDX %u hadn't been written to yet.\n", ht->idx);
            continue;
        }

        if(i % 1000 == 0) {
            NVMEV_ERROR("Finished %llu out of %u CMTs.\n", 
                         i, cache->nr_valid_tpages);
        }

        uint32_t t_ppa = atomic_read(&ht->t_ppa);
        uint32_t cnt = 0;
        uint64_t glen = ORIG_GLEN;
        uint64_t leaf_glen = ORIG_GLEN - ROOT_G;
        uint32_t cnt_bytes;

        for(int i = 0; i < EPP; i++) {
            if(ht->pair_mem[i]) {
                cnt++;
            }
        }

        cnt_bytes = cnt * sizeof(uint32_t) * 2;

        if(cnt_bytes > (leaf_glen * GRAINED_UNIT)) {
            glen = ROOT_G + (cnt_bytes / GRAINED_UNIT);

            if(cnt_bytes % GRAINED_UNIT) {
                glen++;
            }

            //NVMEV_ERROR("New glen %llu cnt_bytes %u cnt %u\n", 
            //             glen, cnt_bytes, cnt);
        }

        if(t_ppa == UINT_MAX) {
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

            atomic_set(&ht->t_ppa, ppa);

            oob[ppa][0] = (glen << 32) | (ht->idx * EPP);
            for(int i = 1; i < GRAIN_PER_PAGE; i++) {
                oob[ppa][i] = UINT_MAX;
            }

            ht->mappings = kzalloc_node(spp->pgsz, GFP_KERNEL, numa_node_id());
            ht->mem = ht->mappings;
            ht->len_on_disk = glen;
            ht->g_off = 0;

            if(glen < GRAIN_PER_PAGE) {
                mark_grain_invalid(shard, PPA_TO_PGA(ppa, glen), 
                                   GRAIN_PER_PAGE - glen);
            }

            struct root* root;
            btree_init((struct root*) ht->mappings);
            root = (struct root*) ht->mappings;
            for(int i = root->cnt; i < glen - ROOT_G; i++) {
                btree_expand(root);
            }
        }

        ht->state = CLEAN;

        struct root* root;
        root = (struct root*) ht->mappings;

        uint32_t leaf_e_idx = 0;

        for(int i = 0; i < EPP; i++) {
            if(ht->pair_mem[i]) {
                lpa_t lpa = (ht->idx * EPP) + i;
                e[leaf_e_idx].hidx = lpa;
                e[leaf_e_idx].ppa = ht->fm_grains[OFFSET(lpa)];
                leaf_e_idx++;
            }
        }

        btree_bulk_insert(root, e, leaf_e_idx);
        ht->cached_cnt += leaf_e_idx;
        ht->state = DIRTY;
        ht->mappings = NULL;
    }

    shard->fastmode = false;
    NVMEV_ERROR("Fast fill done. %llu collisions\n", collision);

    kfree(args);

    return 0;
}

void fast_fill(struct nvmev_ns *ns, uint64_t size, uint32_t vlen, uint32_t pairs) {
    struct fast_fill_args *args;

    args = kzalloc_node(sizeof(*args), GFP_KERNEL, numa_node_id());
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
