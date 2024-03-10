// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/kthread.h>
#include <linux/random.h>
#include <linux/sched/clock.h>
#include <linux/sort.h>
#include <linux/xarray.h>

#include "city.h"
#include "nvmev.h"
#include "demand_ftl.h"

#include "demand/cache.h"
#include "demand/coarse_old.h"
#include "demand/d_param.h"
#include "demand/demand.h"
#include "demand/utility.h"

#include <linux/kfifo.h>

extern int find(const void *table, uint32_t lpa, size_t count);

struct my_queue_node {
    void *data;
    struct list_head list;
};

struct my_queue {
    struct list_head head;
};

struct my_queue *q;

void my_queue_init(struct my_queue *queue) {
    INIT_LIST_HEAD(&queue->head);
}

void* my_queue_enqueue(struct my_queue *queue, void *data) {
    struct my_queue_node *new_node = kmalloc(sizeof(*new_node), GFP_KERNEL);
    new_node->data = data;
    list_add_tail(&new_node->list, &queue->head);
    return (void*) 0xDEADBEEF;
}

void *my_queue_dequeue(struct my_queue *queue) {
    if (list_empty(&queue->head))
        return NULL;

    struct my_queue_node *node = list_first_entry(&queue->head, struct my_queue_node, list);
    void *data = node->data;
    list_del(&node->list);
    kfree(node);
    return data;
}

void my_queue_destroy(struct my_queue *queue) {
    struct my_queue_node *cur, *temp;
    list_for_each_entry_safe(cur, temp, &queue->head, list) {
        list_del(&cur->list);
        kfree(cur);
    }
}

uint64_t offset = 0;

//bool are_all_non_zero_asm(uint32_t *array, size_t length) {
//	// Assuming array is 16-byte aligned and length is a multiple of 4
//	// for simplicity. Real code should handle unaligned data and lengths
//	// not divisible by 4.
//	char result = 1; // Assume all non-zero initially
//	size_t iterations = length / 4; // Process 4 uint32_t per iteration
//
//	asm volatile(
//			"1: \n\t" // Loop label
//			"movdqu (%[array]), %%xmm0 \n\t" // Load 4 uint32_t from array into xmm0
//			"ptest %%xmm0, %%xmm0 \n\t" // Test for all zeros (SSE4.1 instruction, use por+pcmpeqd for older SSE)
//			"jnz 2f \n\t" // If not all zeros, jump to label 2
//			"add $16, %[array] \n\t" // Move pointer to next 4 uint32_t
//			"dec %[iterations] \n\t" // Decrement iterations
//			"jnz 1b \n\t" // If iterations not zero, loop
//			"jmp 3f \n" // Jump to end
//			"2: \n\t" // Label for non-zero detection
//			"mov $0, %[result] \n" // Set result to 0 (false)
//			"3:" // End label
//			: [result] "+r" (result), [array] "+r" (array), [iterations] "+r" (iterations) // Output operands
//			: // No input operands
//			: "cc" // Clobbered registers
//			);
//
//	return result != 0;
//}

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
    NVMEV_ASSERT(oob_empty(shard, ppa2pgidx(shard, &ppa)));

	return ppa;
}

static void init_maptbl(struct demand_shard *demand_shard)
{
	int i;
	struct ssdparams *spp = &demand_shard->ssd->sp;

	demand_shard->maptbl = vmalloc(sizeof(struct ppa) * spp->tt_pgs);
	for (i = 0; i < spp->tt_pgs; i++) {
		demand_shard->maptbl[i].ppa = UNMAPPED_PPA;
	}
}

static void remove_maptbl(struct demand_shard *demand_shard)
{
	vfree(demand_shard->maptbl);
}

static void init_rmap(struct demand_shard *demand_shard)
{
	int i;
	struct ssdparams *spp = &demand_shard->ssd->sp;

	demand_shard->rmap = vmalloc(sizeof(uint64_t) * spp->tt_pgs);
	for (i = 0; i < spp->tt_pgs; i++) {
		demand_shard->rmap[i] = INVALID_LPN;
	}
}

static void remove_rmap(struct demand_shard *demand_shard)
{
	vfree(demand_shard->rmap);
}

extern struct algorithm __demand;
extern struct lower_info virt_info;

#ifndef GC_STANDARD
char **inv_mapping_bufs;
uint64_t *inv_mapping_offs;
uint8_t *pg_inv_cnt;
#endif

struct kfifo *gc_fifo;
int init_gc_fifo(void) {
	gc_fifo = kmalloc(sizeof(struct kfifo), GFP_KERNEL);
	if (!gc_fifo)
		return -ENOMEM;

	if (kfifo_alloc(gc_fifo, 2097152, GFP_KERNEL)) {
		NVMEV_ASSERT(false);
		//kfree(gc_fifo);
		return -ENOMEM;
	}

	return 0;
}

void free_gc_fifo(void) {
	kfifo_free(gc_fifo);
	kfree(gc_fifo);
}

struct task_struct *gcer;
static int __copy_thread(void *voidargs);
void demand_init(struct demand_shard *shard, uint64_t size, 
                 struct ssd* ssd) 
{
    struct ssdparams *spp = &ssd->sp;

    uint64_t tt_grains = spp->tt_pgs * GRAIN_PER_PAGE; 

	init_gc_fifo();
    q = kzalloc(sizeof(*q), GFP_KERNEL);
    my_queue_init(q);

    gcer = 
    kthread_create(__copy_thread, NULL, "__copy_thread");
	if (nvmev_vdev->config.cpu_nr_copier != -1)
		kthread_bind(gcer, nvmev_vdev->config.cpu_nr_copier);
    wake_up_process(gcer);

    spp->tt_map_pgs = tt_grains / EPP;
    spp->tt_data_pgs = spp->tt_pgs - spp->tt_map_pgs;

#ifndef GC_STANDARD
    pg_inv_cnt = (uint8_t*) vmalloc(spp->tt_pgs * sizeof(uint8_t));
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

	shard->oob = (uint32_t**)vmalloc(spp->tt_pgs * sizeof(uint32_t*));
	shard->oob_mem = 
	(uint32_t*) vmalloc(spp->tt_pgs * GRAIN_PER_PAGE * sizeof(uint32_t));

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

    print_demand_stat(&d_stat);
}

void demand_free(struct demand_shard *shard) {
    struct ssdparams *spp = &shard->ssd->sp;

	if (!IS_ERR_OR_NULL(gcer)) {
		kthread_stop(gcer);
        gcer = NULL;
	}

	free_gc_fifo();

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

uint64_t dsize = 0;
uint8_t* wb;
uint64_t wb_offs;
skiplist *tskip = NULL;

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

    tskip = skiplist_init();
    NVMEV_INFO("Skip init.\n");

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

    //NVMEV_INFO("Marking grain %llu valid length %u in PPA %llu\n", 
    //            grain, len, page);

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
    
    if(blk->igc >= spp->pgs_per_blk * GRAIN_PER_PAGE) {
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

    if(page_grains_invalid(shard, page)) {
        mark_page_invalid(shard, &ppa);
    }
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
DEFINE_HASHTABLE(inv_m_hash, 21); // 2^8 buckets = 256 buckets

#ifndef GC_STANDARD
static uint64_t __record_inv_mapping(struct demand_shard *shard, lpa_t lpa, 
                                     ppa_t ppa, uint64_t *credits) {
    struct ssdparams *spp = &shard->ssd->sp;
    struct gc_data *gcd = &shard->gcd;
    struct ppa p = ppa_to_struct(spp, G_IDX(ppa));
    struct line* l = get_line(shard, &p); 
    uint64_t line = (uint64_t) l->id;
    uint64_t nsecs_completed = 0;
    uint32_t **oob = shard->oob;

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
    uint32_t *start_address = shard->oob[start_pgidx];
    size_t total_size = sizeof(uint32_t) * GRAIN_PER_PAGE * spp->pgs_per_blk;
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

static int len_cmp(const void *a, const void *b)
{
    const struct lpa_len_ppa *da = a, *db = b;

    if (db->len < da->len) return -1;
    if (db->len > da->len) return 1;
    return 0;
}

bool oob_empty(struct demand_shard *shard, uint64_t pgidx) {
    return true;
    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        if(shard->oob[pgidx][i] == 1) {
            NVMEV_ERROR("Page %llu offset %d was %u\n", 
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

    void* xa_entry = xa_load(&gcd->gc_xa, key);
    if(xa_entry) {
        uint64_t xa_ppa = (xa_to_value(xa_entry) >> 32);
        if(xa_ppa == ppa) {
            /*
             * There was an invalid mapping that matched this LPA PPA
             * combination.
             */
            end = ktime_get();
            mapping_searches += ktime_to_us(end) - ktime_to_us(start);

			bool found = false;
			struct inv_entry *item;
			// Iterate over the hash table to find the item
			hash_for_each_possible(inv_m_hash, item, node, key) {
				if (item->key == key) {
                    found = true;
                    break;
				}
			} 

            NVMEV_ASSERT(found);
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

        bool found = false;
        struct inv_entry *item;
        // Iterate over the hash table to find the item
        hash_for_each_possible(inv_m_hash, item, node, key) {
            if (item->key == key) {
                found = true;
                break;
            }
        } 

        NVMEV_ASSERT(!found);
        end = ktime_get();
        mapping_searches += ktime_to_us(end) - ktime_to_us(start);
        return true;
    }
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

//void __load_cmt_entry(struct demand_shard *demand_shard, uint64_t idx) {
//    uint64_t lpa = idx * EPP;
//    struct ssdparams *spp = &demand_shard->ssd->sp;
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
atomic_t gc_rem;

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

struct gc_c_args {
    struct demand_shard *shard;
    uint64_t to;
    uint64_t from;
    uint32_t length;
    bool map;
    atomic_t *gc_rem;
    uint32_t idx;
    uint64_t old_grain;
};

static int __copy_thread(void *voidargs) {
    NVMEV_INFO("GC copy thread started.\n");
    struct generic_copy_args args;
    while(!kthread_should_stop()) {
        if (kfifo_out(gc_fifo, &args, sizeof(args)) == sizeof(args)) {
            args.func(args.args, 0, 0);
        } else {
            cond_resched();
        }
    }
    return 0;
}

//static int fifo_add(int id, const char *name) {
//	temp.id = id;
//	strncpy(temp.name, name, sizeof(temp.name));
//	temp.name[sizeof(temp.name) - 1] = '\0';
//
//	// Try to add the item to the FIFO
//	if (!kfifo_put(&my_fifo, temp)) {
//		NVMEV_ASSERT(false);
//		printk(KERN_INFO "FIFO is full\n");
//		return -ENOMEM;
//	}
//
//	return 0;
//}

bool __is_aligned(void* ptr, uint32_t alignment) {
    uintptr_t ptr_as_uint = (uintptr_t)ptr;
    return (ptr_as_uint & (alignment - 1)) == 0;
}

void __gc_copy_work(void *voidargs, uint64_t*, uint64_t*) {
    struct gc_c_args *args;
    struct demand_shard *shard;
    struct demand_cache *cache;
    struct ssdparams *spp;
    uint64_t to, from;
    uint32_t length;
    bool map;
    atomic_t *gc_rem;
    uint32_t sqid;
    uint32_t idx;
    uint64_t old_grain;

    args = (struct gc_c_args*) voidargs;
    shard = args->shard;
    cache = shard->cache;
    spp = &shard->ssd->sp;
    to = args->to;
    from = args->from;
    length = args->length;
    map = args->map;
    gc_rem = args->gc_rem;
    idx = args->idx;
    old_grain = args->old_grain;

    memcpy(nvmev_vdev->ns[0].mapped + to, 
           nvmev_vdev->ns[0].mapped + from, length);

    NVMEV_DEBUG("Copying %u bytes from %llu (G%llu) to %llu (G%llu) in %s.\n", 
                 length, from, from / GRAINED_UNIT, to, to / GRAINED_UNIT,
                 __func__);

    if(map && idx != UINT_MAX) {
        struct cmt_struct *c = cache->member.cmt[idx];

        NVMEV_ASSERT(c->t_ppa == G_IDX(old_grain));
        NVMEV_DEBUG("%s CMT IDX %u moving from PPA %llu to PPA %llu\n", 
                     __func__, idx, G_IDX(old_grain), to / spp->pgsz);
        c->t_ppa = to / spp->pgsz;
        c->g_off = (to / GRAINED_UNIT) % GRAIN_PER_PAGE;

        if(c->pt) {
            NVMEV_ASSERT(c->pt == (nvmev_vdev->ns[0].mapped + from));
            c->pt = (struct pt_struct*) (nvmev_vdev->ns[0].mapped + to);
        }

        atomic_dec(&c->outgoing);
    }

    atomic_dec(gc_rem);
    kfree(args);
}

char pg[8192];
uint32_t pg_idx = 0;

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
    uint32_t **oob = shard->oob;

    struct lpa_len_ppa *lpa_lens;
    uint64_t tt_rewrite = 0;
    bool mapping_line = gcd->map;
    //uint64_t idx;
    uint64_t lpa_len_idx = 0;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint32_t gcs = 0;
    uint64_t nsecs_completed = 0;

    uint64_t start = 0, end = 0;

    lpa_lens = gcd->lpa_lens;
    NVMEV_ASSERT(lpa_lens);

    start = ktime_get();
	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(shard->ssd, &ppa_copy);
        pgidx = ppa2pgidx(shard, &ppa_copy);
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
                continue;
            }

            if(offset / GRAINED_UNIT == grain) {
                /*
                 * Edge case where in __store we got the last
                 * page of a line for the offset, and GC thinks the
                 * line is available to garbage collect because all
                 * of the pages have been used.
                 */
                //uint32_t g_off = G_OFFSET(grain);
                //NVMEV_ASSERT(g_off < GRAIN_PER_PAGE);
                //mark_grain_valid(shard, PPA_TO_PGA(pgidx, g_off), 
                //                 GRAIN_PER_PAGE - g_off);
                //mark_grain_invalid(shard, PPA_TO_PGA(pgidx, g_off), 
                //                   GRAIN_PER_PAGE - g_off);
                offset = 0;
                NVMEV_INFO("Set offset to 0 grain %llu!!!\n",
                            grain);
            }

            NVMEV_DEBUG("Grain %llu (%d)\n", grain, i);
            if(mapping_line && i == 0 && oob[pgidx][i] == UINT_MAX) {
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

                uint32_t key_part_1 = oob[pgidx][1];
                uint32_t key_part_2 = oob[pgidx][2];
                unsigned long key = 
                ((unsigned long) key_part_1 << 32) | key_part_2;

                NVMEV_DEBUG("Got invalid mapping PPA %llu key %lu target line %lu in GC\n", 
                             pgidx, key, key >> 32);
                NVMEV_ASSERT(i == 0);
                NVMEV_ASSERT(mapping_line);

                //if(!__invalid_mapping_ppa(shard, G_IDX(grain), key)) {
                //    NVMEV_ASSERT(false);
                //    NVMEV_INFO("Mapping PPA %llu key %lu has since been rewritten. Skipping.\n",

                //            pgidx, key);

                //    i += GRAIN_PER_PAGE;
                //    continue;
                //} else {
                __clear_inv_mapping(shard, key);
                //}

                lpa_lens[lpa_len_idx++] =
                (struct lpa_len_ppa) {UINT_MAX, GRAIN_PER_PAGE, grain, 
                                      key >> 32 /* The line these invalid mappings target. */};
                //atomic_inc(&gc_rem);
                gcs++;

                d_stat.inv_m_w += spp->pgsz;
                d_stat.inv_m_r += spp->pgsz;

                mark_grain_invalid(shard, grain, GRAIN_PER_PAGE);
                cnt++;
                tt_rewrite += GRAIN_PER_PAGE * GRAINED_UNIT;
                i += GRAIN_PER_PAGE;
#endif
            } else if(mapping_line && valid_g) {
                /*
                 * A grain containing live LPA to PPA mapping information.
                 */

                if(oob[pgidx][i] == UINT_MAX) {
                    NVMEV_ASSERT(false);
                    continue;
                }

                struct cmt_struct *cmt = cache->get_cmt(cache, oob[pgidx][i]);
                uint32_t idx = IDX(oob[pgidx][i]);
                uint32_t g_len = 1;

                if(idx == 0) {
                    continue;
                }

#ifdef GC_STANDARD
                g_len = GRAIN_PER_PAGE;
#else
				while(i + g_len < GRAIN_PER_PAGE && oob[pgidx][i + g_len] == 0) {
				   g_len++;
				}
#endif

#ifdef GC_STANDARD
                NVMEV_ASSERT(cmt->g_off == 0 && i == 0);
#endif
                if(cmt->t_ppa != pgidx || cmt->g_off != i) {
                    NVMEV_DEBUG("CMT IDX %u moved from PPA %llu to PPA %u\n", 
                                 idx, pgidx, cmt->t_ppa);
                    continue;
                }

                NVMEV_DEBUG("CMT IDX %u was %u grains in size from grain %llu PPA %llu (%u)\n", 
                             idx, g_len, grain, G_IDX(grain + i), cmt->t_ppa);
                mark_grain_invalid(shard, grain, g_len);

                lpa_lens[lpa_len_idx++] = (struct lpa_len_ppa) {idx, g_len, 
                                           grain, UINT_MAX - 1, 
                                           UINT_MAX};
                //atomic_inc(&gc_rem);
                gcs++;

                i += g_len - 1;
                tt_rewrite += g_len * GRAINED_UNIT;
                cnt++;
            } else if(!mapping_line && valid_g && oob[pgidx][i] != UINT_MAX && 
                      oob[pgidx][i] != 2 && oob[pgidx][i] != 0 && 
                      __valid_mapping(shard, oob[pgidx][i], grain)) {
                NVMEV_DEBUG("Got regular PPA %llu grain %llu LPA %llu in GC grain %u\n", 
                             pgidx, grain, oob[pgidx][i], i);
#ifndef GC_STANDARD
                NVMEV_ASSERT(pg_inv_cnt[pgidx] <= GRAIN_PER_PAGE);
#endif
                NVMEV_ASSERT(!mapping_line);
                
                uint64_t lpa = oob[pgidx][i];

                len = 1;
                while(i + len < GRAIN_PER_PAGE && oob[pgidx][i + len] == 0) {
                    len++;
                }
                
                lpa_lens[lpa_len_idx++] =
                (struct lpa_len_ppa) {oob[pgidx][i], len, grain, UINT_MAX};

                gcs++;
                //int *v = (int *)&gc_rem.counter;
                //(*v)++;
                //atomic_inc(&gc_rem);

                i += len - 1;
                mark_grain_invalid(shard, grain, len);
                cnt++;
                tt_rewrite += len * GRAINED_UNIT;
            } 
        }

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

    end = ktime_get();
    clean_first_half += ktime_to_us(end) - ktime_to_us(start);

	if (cnt <= 0) {
		return;
    }

    start = ktime_get();
    atomic_set(&gc_rem, gcs);
    //sort(lpa_lens, lpa_len_idx, sizeof(struct lpa_len_ppa), &len_cmp, NULL);

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
        pgidx = ppa2pgidx(shard, &new_ppa);
        new_line = ppa2line(shard, &new_ppa);
        NVMEV_DEBUG("Picked up PPA %u %u remaining grains\n", 
                pgidx, GRAIN_PER_PAGE - offset);
    } else {
again:
        new_ppa = get_new_page(shard, mapping_line ? GC_MAP_IO : GC_IO);

        offset = 0;
        pgidx = ppa2pgidx(shard, &new_ppa);
        new_line = ppa2line(shard, &new_ppa);
        NVMEV_ASSERT(oob_empty(shard, ppa2pgidx(shard, &new_ppa)));
        advance_write_pointer(shard, mapping_line ? GC_MAP_IO : GC_IO);
        mark_page_valid(shard, &new_ppa);

        if(pgidx == 0) {
            NVMEV_ERROR("Got PPA 0 in GC. Skipping!\n");
            mark_grain_valid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
            mark_grain_invalid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
            goto again;
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
            mark_grain_valid(shard, PPA_TO_PGA(pgidx, offset), 
                    GRAIN_PER_PAGE - offset);
            mark_grain_invalid(shard, PPA_TO_PGA(pgidx, offset), 
                    GRAIN_PER_PAGE - offset);

            uint64_t to = shard_off + (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
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

                if (last_pg_in_wordline(shard, &new_ppa)) {
                    gcw.cmd = NAND_WRITE;
                    gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;

                    if(mapping_line) {
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
#ifndef GC_STANDARD
            NVMEV_ASSERT(remain >= INV_PAGE_SZ);
#endif
        }

        NVMEV_DEBUG("LPA/IDX %llu length %u going from PPA %llu (G%llu) to PPA %llu (G%llu)\n",
                     lpa, length, G_IDX(old_grain), old_grain, pgidx, grain);

        to = shard_off + (pgidx * spp->pgsz) + (offset * GRAINED_UNIT);
        from = shard_off + (G_IDX(old_grain) * spp->pgsz) + 
            (G_OFFSET(old_grain) * GRAINED_UNIT);

        if(pg_idx + length * GRAINED_UNIT > 8192) {
            pg_idx = 0;
        }

        /*
         * Emulate some copy-to-DRAM overhead.
         */
        memcpy(pg + pg_idx, nvmev_vdev->ns[0].mapped + from, length * GRAINED_UNIT);

        //memcpy(nvmev_vdev->ns[0].mapped + to, 
        //       nvmev_vdev->ns[0].mapped + from, length * GRAINED_UNIT);

        struct generic_copy_args c_args;

        struct gc_c_args* args = 
        (struct gc_c_args*) kmalloc(sizeof(*args), GFP_KERNEL);

        args->shard = shard;
        args->to = to;
        args->from = from;
        args->length = length * GRAINED_UNIT;
        args->map = mapping_line;
        args->gc_rem = &gc_rem;
        args->idx = lpa == UINT_MAX ? UINT_MAX: lpa;
        args->old_grain = old_grain;

        if(mapping_line && lpa != UINT_MAX) {
            struct cmt_struct *c = cache->get_cmt(cache, IDX2LPA(lpa));
            NVMEV_ASSERT(atomic_read(&c->outgoing) == 0);
            atomic_inc(&c->outgoing);
        }

        c_args.func = __gc_copy_work;
        c_args.args = args;
		while(kfifo_in(gc_fifo, &c_args, sizeof(c_args)) != sizeof(c_args)) {
			cpu_relax();
		}
        //schedule_internal_operation_cb(INT_MAX, reads_done,
        //        NULL, 0, 0, 
        //        (void*) __gc_copy_work, 
        //        (void*) args, 
        //        false, NULL);

        if(lpa == UINT_MAX) {
#ifdef GC_STANDARD
            NVMEV_ASSERT(false);
#endif
            oob[pgidx][offset] = lpa;
        } else {
            lpa_lens[grains_rewritten].new_ppa = PPA_TO_PGA(pgidx, offset);
            oob[pgidx][offset] = mapping_line ? IDX2LPA(lpa) : lpa;
        }

        for(int i = 1; i < length; i++) {
            oob[pgidx][offset + i] = 0;
        }

        mark_grain_valid(shard, grain, length);

        if(lpa == UINT_MAX) { 
#ifdef GC_STANDARD
            NVMEV_ASSERT(false);
#endif
            unsigned long target_line = lpa_lens[grains_rewritten].new_ppa;
            __update_mapping_ppa(shard, pgidx, target_line);
            NVMEV_DEBUG("Putting %u in the OOB for mapping PPA %u which targets line %lu\n",
                        (target_line << 32) | pgidx, pgidx, target_line);
            oob[pgidx][1] = target_line;
            oob[pgidx][2] = pgidx;
        }  
 
        offset += length;
        remain -= length * GRAINED_UNIT;
        grains_rewritten++;

        if(grains_rewritten % 4096 == 0) {
            __reclaim_completed_reqs();
        }

        NVMEV_ASSERT(offset <= GRAIN_PER_PAGE);

        if(offset == GRAIN_PER_PAGE && grains_rewritten < cnt) {
            NVMEV_ASSERT(remain > 0);
new_ppa:
            new_ppa = get_new_page(shard, mapping_line ? GC_MAP_IO : GC_IO);
            new_line = ppa2line(shard, &new_ppa);
            offset = 0;
            pgidx = ppa2pgidx(shard, &new_ppa);
            NVMEV_ASSERT(oob_empty(shard, ppa2pgidx(shard, &new_ppa)));
            advance_write_pointer(shard, mapping_line ? GC_MAP_IO : GC_IO);
            mark_page_valid(shard, &new_ppa);

            if(pgidx == 0) {
                NVMEV_ERROR("Got PPA 0 in GC. Skipping!\n");
                mark_grain_valid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
                mark_grain_invalid(shard, PPA_TO_PGA(pgidx, 0), GRAIN_PER_PAGE);
                goto new_ppa;
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

    //    if (last_pg_in_wordline(demand_shard, &new_ppa)) {
    //        gcw.cmd = NAND_WRITE;
    //        gcw.xfer_size = spp->pgsz * spp->pgs_per_oneshotpg;
    //    }

    //    /*
    //     * TODO are we skipping some writes because this isn't triggering?
    //     */

    //    ssd_advance_nand(demand_shard->ssd, &gcw);
    //}

    while(atomic_read(&gc_rem) > 0) {
        cpu_relax();
    }

    end = ktime_get();
    clean_second_half += ktime_to_us(end) - ktime_to_us(start);

    start = ktime_get();
    if(!mapping_line) {
        do_bulk_mapping_update_v(shard, lpa_lens, cnt, read_cmts, read_cmts_idx);
    }
    end = ktime_get();
    clean_third_half += ktime_to_us(end) - ktime_to_us(start);

    __reclaim_completed_reqs();

    read_cmts_idx = 0;
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

    atomic_set(&gc_rem, 0);

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
               " %llu time spent searching.", 
                user_pgs_this_gc, gc_pgs_this_gc, map_gc_pgs_this_gc, 
                spp->pgs_per_line, total, map, cleaning, freeing,
                clean_first_half, clean_second_half, clean_third_half, 
                mapping_searches);

    clean_first_half = clean_second_half = clean_third_half = mapping_searches = 0;

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

bool end_w(struct request *req) 
{
    return true;
}

uint32_t __get_glen(struct demand_shard *shard, uint64_t grain) {
    uint32_t ret = 1, i = G_OFFSET(grain);
    while(i + ret < GRAIN_PER_PAGE && shard->oob[G_IDX(grain)][i + ret] == 0) {
        ret++;
    }
    return ret;
}

uint32_t __get_vlen(struct demand_shard *shard, uint64_t grain) {
    uint32_t ret = 1, i = G_OFFSET(grain);
    while(i + ret < GRAIN_PER_PAGE && shard->oob[G_IDX(grain)][i + ret] == 0) {
        ret++;
    }
    return ret * GRAINED_UNIT;
}

bool end_d(struct request *req) 
{
    return true;
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
    return spp->pgsz / GRAINED_UNIT;
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
                               struct cmt_struct *cmt, struct pt_struct pte) {
    struct demand_cache *cache;
    struct ssdparams *spp;
    struct cache_member *cmbr;
    struct ppa p;
    uint64_t ppa;
    uint32_t **oob;

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

    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off = shard_off + ((uint64_t) ppa * spp->pgsz);
    uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;

    NVMEV_DEBUG("Expanded IDX %u from PPA %u grain %llu to PPA %llu new len %u %u cached\n", 
                 cmt->idx, cmt->t_ppa, cmt->g_off, ppa, cmt->len_on_disk + 1,
                 cmbr->nr_cached_tentries);

    struct pt_struct *old = cmt->pt;
    cmt->pt = (struct pt_struct*) ptr;

    uint32_t g_off = cmt->len_on_disk * GRAINED_UNIT;

    struct expand_w_args *args;

    args = (struct expand_w_args*) kmalloc(sizeof(*args), GFP_KERNEL);
    args->shard = shard;
    args->spp = spp;
    args->cmt = cmt;
    args->old = old;
    args->new = cmt->pt;
    args->pte = pte;
    args->cmbr = cmbr;
    args->len = g_off;

    NVMEV_ASSERT(atomic_read(&cmt->outgoing) == 0);
    atomic_inc(&cmt->outgoing);
    schedule_internal_operation_cb(UINT_MAX, 0,
            NULL, 0, 0, 
            (void*) __expand_work, 
            (void*) args, 
            false, NULL);

    cmt->t_ppa = ppa;
    cmt->g_off = 0;
    cmt->len_on_disk++;
    cmbr->nr_cached_tentries++;

    oob[ppa][0] = cmt->idx * EPP;
    for(int i = 1; i < cmt->len_on_disk; i++) {
        oob[ppa][i] = 0;
    }

    for(int i = cmt->len_on_disk; i < GRAIN_PER_PAGE; i++) {
        oob[ppa][i] = UINT_MAX;
    }

    if(GRAIN_PER_PAGE - cmt->len_on_disk > 0) {
        mark_grain_invalid(shard, PPA_TO_PGA(ppa, cmt->len_on_disk), 
                           GRAIN_PER_PAGE - cmt->len_on_disk);
    }
}
#endif

static void __update_map(struct demand_shard *shard, struct cmt_struct *cmt,
                         lpa_t lpa, struct pt_struct pte, uint32_t pos) {
    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;

    NVMEV_ASSERT(cmt->pt);
#ifdef GC_STANDARD
    cmt->pt[OFFSET(lpa)] = pte;
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
        cmt->pt[pos] = pte;
        return;
	} else if(cmt->cached_cnt == 0) {
		cmt->pt[0].lpa = lpa;
		cmt->pt[0].ppa = pte.ppa;
        cmt->cached_cnt++;
        //NVMEV_INFO("LPA %u added to pos %u\n", lpa, 0);
		return;
    } else if(!__need_expand(cmt)) {
        //NVMEV_INFO("LPA %u added to pos %u\n", lpa, cmt->cached_cnt);
        cmt->pt[cmt->cached_cnt++] = pte;
        return;
    }

	if(cmt->cached_cnt > 0 && __need_expand(cmt)) {
		__expand_map_entry(shard, cmt, pte);
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

uint32_t lpas[1024] __attribute__((aligned(32)));
struct pt_struct __lpa_to_pte(struct demand_shard *shard,
                              struct cmt_struct *cmt, lpa_t lpa,
                              uint32_t *pos) {
#ifdef GC_STANDARD
    return cmt->pt[OFFSET(lpa)];
#else
    uint32_t g_len = __entries_to_grains(shard, cmt);
    struct pt_struct pte;
    struct pt_struct *found_pte;

    pte.lpa = lpa;
	pte.ppa = UINT_MAX;

	//size_t count = ((g_len * GRAINED_UNIT) / ENTRY_SIZE) * 2;
    //if(count < 16) {
    //    count = 16;
    //}
	////for(int i = 0; i < count; i++) {
    ////    lpas[i] = cmt->pt[i].lpa;
    ////}

    //uint64_t start = get_cycles();
	//////NVMEV_INFO("Trying %lu items\n", (g_len * GRAINED_UNIT) / ENTRY_SIZE);

	//for(int i = 0; i < cmt->cached_cnt; i++) {
    //    if(cmt->pt[i].lpa != UINT_MAX) {
    //        NVMEV_INFO("Inside : %u\n", cmt->pt[i].lpa);
    //    }
	//}

    ////NVMEV_ASSERT(count % 8 == 0);

	//int idx = find(cmt->pt, lpa, count);
    //uint64_t end = get_cycles();

    //if(idx >= 0) {
	//	int orig = idx;
    //    idx /= 2;
    //    NVMEV_INFO("Found LPA %u at IDX %d orig %d took %llu cycles\n", 
    //                lpa, idx, orig, end - start);
    //    
    //    //if(cmt->pt[idx].lpa != lpa) {
    //    //    NVMEV_INFO("Cached cnt %u\n", cmt->cached_cnt);
    //    //}

    //    //if(cmt->pt[idx].lpa != lpa) {
    //    //    NVMEV_INFO("LPA was actually %u\n", cmt->pt[idx].lpa);
    //    //}

    //    NVMEV_ASSERT(cmt->pt[idx].lpa == lpa);
    //    pte.ppa = cmt->pt[idx].ppa;
    //    *pos = idx;
    //} else {
    //    NVMEV_INFO("CMT IDX %u idx %d couldn't find LPA %u took %llu cycles count %lu\n", 
    //                cmt->idx, idx, lpa, end - start, count);
    //    for(int i = 0; i < cmt->cached_cnt; i++) {
    //        if(cmt->pt[i].lpa != UINT_MAX) {
    //            NVMEV_INFO("2 Inside : %u %u\n", cmt->pt[i].lpa, cmt->pt[i].ppa);
    //        }
    //    }

    //    *pos = UINT_MAX;
    //}

    //return pte;

    //found_pte = search_item(cmt, lpa, (g_len * GRAINED_UNIT) / ENTRY_SIZE,
    //                        pos);

    //if(found_pte) {
    //    return *found_pte;
    //} else {
    //    return pte;
    //}

	//size_t idx = find_index(lpa, cmt->pt, (g_len * GRAINED_UNIT) / ENTRY_SIZE);
	//if(idx != UINT_MAX) {
    //    pte.lpa = lpa;
	//	pte.ppa = cmt->pt[idx].ppa;
	//	*pos = idx;
	//} else {
    //    *pos = UINT_MAX;
    //}
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

    //uint64_t end = get_cycles();
    //NVMEV_INFO("Found idx %lu\n", idx);
    //NVMEV_INFO("Took %llu cycles\n", 
    //            end - start);

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

	//victim = lru_peek(cmbr->lru);
	victim = my_queue_dequeue(q);

    for(int i = 0; i < MAX_SEARCH; i++) {
        if(!victim) {
            //NVMEV_INFO("Exiting eviction because no victim i %d.\n", i);
            break;
        }

        if(victim->state == DIRTY) {
            all_clean = false;
        }

        g_len = __entries_to_grains(shard, victim);

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
            my_queue_enqueue(q, victim);
            break;
        }

        if(count >= target_g) {
            break;
        }
    
        victim = my_queue_dequeue(q);
        //victim = (struct cmt_struct *)lru_prev(cmbr->lru, victim->lru_ptr);
    }

    //NVMEV_INFO("Removing %u entries from the LRU.\n", idx);

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
    uint32_t **oob;
    struct demand_cache *cache;
    struct cache_member *cmbr;
    struct ssdparams *spp;
    struct cache_stat *cstat;
    struct cmt_struct* victim;
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

            /*
             * The IDX is used during GC so that we know which CMT entry
             * to update.
             */

            oob[ppa][grain] = victim->idx * EPP;
            NVMEV_DEBUG("Set OOB PPA %u grain %u to IDX %u\n", ppa, grain, victim->idx);

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
            for(int j = 1; j < g_len; j++) {
                NVMEV_ASSERT(grain + j < GRAIN_PER_PAGE);
                oob[ppa][grain + j] = 0;
            }

            mark_grain_valid(shard, PPA_TO_PGA(ppa, grain), g_len);

            victim->t_ppa = ppa;
            victim->state = CLEAN;

            struct pte_e_args *args;
            args = (struct pte_e_args*) kzalloc(sizeof(*args), GFP_KERNEL);
            args->out_ppa = ppa;
            args->prev_ppa = prev_ppa;
            args->cmt = victim;
            args->idx = victim->idx;
            args->spp = spp;
            args->grain = grain;
            args->len = g_len;

            grain += g_len;

            struct ppa p = ppa_to_struct(spp, victim->t_ppa);
            struct nand_cmd swr = {
                .type = USER_IO,
                .cmd = NAND_WRITE,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            };

            if (last_pg_in_wordline(shard, &p)) {
                swr.stime = __stime_or_clock(stime);
                swr.ppa = &p;
                d_stat.trans_w += spp->pgsz * spp->pgs_per_oneshotpg;
                nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
            }

            struct generic_copy_args c_args;
            c_args.func = __pte_evict_work;
            c_args.args = args;

            //uint64_t nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
			atomic_inc(&victim->outgoing);
            while(kfifo_in(gc_fifo, &c_args, sizeof(c_args)) != sizeof(c_args)) {
                cpu_relax();
            }
            //schedule_internal_operation_cb(req->sq_id, 0,
			//		NULL, 0, 0, 
			//		(void*) __pte_evict_work, 
			//		(void*) args, 

			//		false, NULL);


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
    uint32_t **oob;

    cache = shard->cache;
    cmbr = &cache->member;
    spp = &shard->ssd->sp;
    cstat = &cache->stat;
    grain = cmt->g_off;
    oob = shard->oob;

    while(atomic_read(&cmt->outgoing) == 1) {
        cpu_relax();
    }

    uint64_t off = ((uint64_t) cmt->t_ppa * spp->pgsz) + (grain * GRAINED_UNIT);
    uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;
    cmt->pt = (struct pt_struct*) ptr;

    NVMEV_DEBUG("__get_one for IDX %u CMT PPA %u\n", cmt->idx, cmt->t_ppa);

    if(!first) {
        /*
         * If this wasn't the first access of this mapping
         * table page, we need to read it from disk.
         */

        if(cmt->t_ppa > spp->tt_pgs) {
            NVMEV_INFO("%s tried to convert PPA %u\n", __func__, cmt->t_ppa);
        }

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
        //NVMEV_INFO("Sent a read for CMT PPA %u\n", cmt->t_ppa);
    } else {
#ifdef GC_STANDARD
        cmt->len_on_disk = GRAIN_PER_PAGE;
        cmt->g_off = 0;
        
        for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
            cmt->pt[i].ppa = UINT_MAX;
        }
#else
        cmt->len_on_disk = ORIG_GLEN;
        cmt->g_off = 0;

        for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
            cmt->pt[i].lpa = UINT_MAX;
            cmt->pt[i].ppa = UINT_MAX;
        }
#endif
    }

	cmt->lru_ptr = my_queue_enqueue(q, (void*) cmt);
    //cmt->lru_ptr = lru_push(cmbr->lru, (void *)cmt);
    cmbr->nr_cached_tentries += cmt->len_on_disk;

    *missed = true;
    cstat->cache_miss++;

#ifndef GC_STANDARD
    if(!first && !cgo_is_full(cache)) {
        /*
         * Also bring in the remaining mapping entries on the page.
         */
        NVMEV_ASSERT(oob[cmt->t_ppa][0] != 2);
        struct cmt_struct *found_cmt;

        int brought = 0;
        for(int i = 0; i < GRAIN_PER_PAGE; i++) {
            uint32_t lpa_at_oob = oob[cmt->t_ppa][i];
            uint32_t idx_at_oob = IDX(lpa_at_oob);

			/*
			 * TODO must be a cleaner way to go about this.
			 */

			if(lpa_at_oob != UINT_MAX && lpa_at_oob != 0 && lpa_at_oob != 2 && 
					idx_at_oob != cmt->idx) {
				//NVMEV_INFO("Trying to bring in IDX %u LPA %u\n", 
				//		idx_at_oob, lpa_at_oob);
				found_cmt = cache->get_cmt(cache, IDX2LPA(idx_at_oob));

				if(found_cmt->t_ppa == cmt->t_ppa) {
					if(!found_cmt->pt) {
						//NVMEV_INFO("Bringing in CMT IDX %u PPA %u grain %u\n", 
						//        found_cmt->idx, found_cmt->t_ppa, i);
						brought++;

						while(atomic_read(&found_cmt->outgoing) == 1) {
							cpu_relax();
						}

						NVMEV_ASSERT(found_cmt->t_ppa != UINT_MAX);
						NVMEV_ASSERT(!found_cmt->pt);
						NVMEV_ASSERT(!found_cmt->lru_ptr);

						off = ((uint64_t) found_cmt->t_ppa * spp->pgsz) + (i * GRAINED_UNIT);
						ptr = nvmev_vdev->ns[0].mapped + off;
						found_cmt->pt = (struct pt_struct*) ptr;
						//found_cmt->lru_ptr = lru_push(cmbr->lru, (void *)found_cmt);
						found_cmt->lru_ptr = my_queue_enqueue(q, (void*) found_cmt);

						cmbr->nr_cached_tentries += found_cmt->len_on_disk;
						//NVMEV_ASSERT(cmbr->nr_cached_tentries == cmbr->lru->size);
						//NVMEV_INFO("Increased cached tentries to %u\n", 
						//        cmbr->nr_cached_tentries);
					}
				}

				i += cmt->len_on_disk - 1;
			}

            if(cgo_is_full(cache)) {
                break;
            }
        }

        //NVMEV_INFO("Bringing in %d entries to the LRU.\n", brought);
    } else {
        //NVMEV_INFO("Skipped because %s %s\n", first ? "first" : "NOT FIRST",
        //                                      cgo_is_full(cache) ? "FULL" : "NOT FULL");
    }
#endif

    return nsecs_completed;
}

static uint64_t __read_and_compare(struct demand_shard *shard, 
        ppa_t grain, struct hash_params *h_params, 
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

    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off = shard_off + (ppa * spp->pgsz) + (offset * GRAINED_UNIT);

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

    swr.ppa = &p;
    *nsecs = ssd_advance_nand(ssd, &swr);
    d_stat.data_r += spp->pgsz;
    //NVMEV_INFO("Read completed %llu\n", *nsecs);

    uint8_t* ptr = nvmev_vdev->ns[0].mapped + off;
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
        //NVMEV_INFO("Match %llu %llu.\n", 
        //*(uint64_t*) actual_key.key, *(uint64_t*) check_key.key);

        /* hash key found -> update */
        d_stat.fp_match_w++;
        return 0;
    } else {
        //NVMEV_INFO("Fail %llu %llu.\n", 
        //        *(uint64_t*) actual_key.key, *(uint64_t*) check_key.key);

        /* retry */
        d_stat.fp_collision_w++;
        h_params->cnt++;
        return 1;
    }
}

static inline uint32_t __vlen_from_value(uint64_t off) {
    uint8_t klen = *(uint8_t*) (nvmev_vdev->ns[0].mapped + off);
    uint32_t vlen = *(uint32_t*) (nvmev_vdev->ns[0].mapped + off + KLEN_MARKER_SZ + klen);
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
    key.key = NULL;
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

    uint32_t **oob = shard->oob;
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
        //NVMEV_INFO("Key %s (%llu) tried to read missing CMT entry.\n", 
        //        (char*) cmd->kv_store.key, *(uint64_t*) cmd->kv_store.key);
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

        NVMEV_DEBUG("Read for key %llu (%llu) checks LPA %u PPA %u\n", 
                    *(uint64_t*) (key.key), *(uint64_t*) &(cmd->kv_store.key), 
                    lpa, pte.ppa);

        if (!IS_INITIAL_PPA(pte.ppa)) {
            d_stat.d_read_on_read += spp->pgsz;
            if(__read_and_compare(shard, pte.ppa, &h, &key, 
                        nsecs_latest, 
                        &nsecs_completed)) {
                nsecs_latest = max(nsecs_latest, nsecs_completed);

                if (vlen <= KB(4)) {
                    nsecs_latest += spp->fw_4kb_rd_lat;
                } else {
                    nsecs_latest += spp->fw_rd_lat;
                }

                pos = UINT_MAX;
                missed = false;
                goto lpa;
            }

            nsecs_latest = max(nsecs_latest, nsecs_completed);

            len = __vlen_from_value((uint64_t) pte.ppa * GRAINED_UNIT);
            if(!for_del) {
                cmd->kv_retrieve.value_len = len;
                cmd->kv_retrieve.rsvd = ((uint64_t) pte.ppa) * GRAINED_UNIT;
            } else {
                cmd->kv_retrieve.rsvd = U64_MAX;
                mark_grain_invalid(shard, pte.ppa, 
                                   len % GRAINED_UNIT ? 
                                   (len / GRAINED_UNIT) + 1 :
                                   len / GRAINED_UNIT);
                pte.ppa = UINT_MAX;
                __update_map(shard, cmt, lpa, pte, pos);
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

    //if(status == 0) {
    //    NVMEV_INFO("Read for key %llu (%llu %u) finishes with LPA %u PPA %llu vlen %u"
    //                " count %u\n", 
    //                *(uint64_t*) (key.key), 
    //                *(uint64_t*) (key.key + 4), 
    //                *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), 
    //                lpa, cmd->kv_retrieve.rsvd == U64_MAX ? U64_MAX :
    //                cmd->kv_retrieve.rsvd / GRAINED_UNIT, 
    //                cmd->kv_retrieve.value_len, h.cnt);
    //} else {
    //    NVMEV_INFO("Read for %s key %llu (%llu %u) FAILS with LPA %u PPA %llu vlen %u"
    //                " count %u\n", 
    //                key.key[0] == 'L' ? "log" : "regular",
    //                *(uint64_t*) (key.key),
    //                *(uint64_t*) (key.key + 4), 
    //                *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), 
    //                lpa, cmd->kv_retrieve.rsvd == U64_MAX ? U64_MAX :
    //                cmd->kv_retrieve.rsvd / GRAINED_UNIT, 
    //                cmd->kv_retrieve.value_len, h.cnt);
    //}

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
inline bool __crossing_page(struct ssdparams *spp, uint64_t offset, uint32_t vlen) {
    if(offset % spp->pgsz == 0) {
        return true;
    }
    return !((offset % spp->pgsz) + vlen <= spp->pgsz);
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

    nsecs_xfer_completed = ssd_advance_write_buffer(shard->ssd, req->nsecs_start, 
                                                    vlen);
    nsecs_latest = nsecs_xfer_completed;

    KEYT key;
    key.key = NULL;
    key.key = cmd->kv_store.key;
    key.len = klen;

    //if(key.key[0] == 'L') {
    //    NVMEV_INFO("Log key write. Bid %llu log num %u vlen %u\n", 
    //                *(uint64_t*) (key.key + 4), 
    //                *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), vlen);
    //}

    uint32_t **oob = shard->oob;
    bool need_new = false;
newpage:
    if(offset == 0 || __crossing_page(spp, offset, vlen)) {
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

        if(offset % spp->pgsz) {
            uint64_t ppa = ppa2pgidx(shard, &cur_page);
            uint64_t g = offset / GRAINED_UNIT;
            uint64_t g_off = g % GRAIN_PER_PAGE;

            NVMEV_ASSERT(offset % GRAINED_UNIT == 0);

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

        offset = ((uint64_t) ppa2pgidx(shard, &cur_page)) * spp->pgsz;
        //NVMEV_INFO("1 Set offset to %llu PPA %llu\n", offset, G_IDX(offset / GRAINED_UNIT));
    }

    struct request d_req;
    d_req.ssd = shard->ssd;
    d_req.nsecs_start = nsecs_xfer_completed;
    d_req.hash_params = NULL;
    d_req.cmd = cmd;
    d_req.key = key;

    NVMEV_ASSERT(key.key);
    NVMEV_ASSERT(cmd->kv_store.key);
    NVMEV_ASSERT(cmd);
    NVMEV_ASSERT(vlen > klen);
    NVMEV_ASSERT(vlen <= spp->pgsz);
    NVMEV_ASSERT(klen <= 16);

    //if(key.key[0] == 'L') {
    //    NVMEV_INFO("Log key write. Bid %llu log num %u len %u\n", 
    //                *(uint64_t*) (key.key + 4), 
    //                *(uint16_t*) (key.key + 4 + sizeof(uint64_t)), vlen);
    //}

    glen = vlen / GRAINED_UNIT;
    grain = offset / GRAINED_UNIT;
    page = G_IDX(grain);
    g_off = G_OFFSET(grain);

    if(vlen % GRAINED_UNIT) {
        glen++;
    }

    cmd->kv_store.rsvd = offset;
    //NVMEV_INFO("Initial\n");
    mark_grain_valid(shard, grain, glen);
    credits += glen;

    __mark_early(grain, klen, cmd->kv_store.key);

    offset += (vlen / GRAINED_UNIT) * GRAINED_UNIT;
    //NVMEV_INFO("2 Set offset to %llu PPA %llu\n", offset, page);

    if(vlen % GRAINED_UNIT) {
        offset += GRAINED_UNIT;
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

        //NVMEV_INFO("Its CMT was empty. We're giving it PPA %u\n", ppa);

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

        oob[ppa][0] = cmt->idx * EPP;

        cmt->t_ppa = ppa;
        cmt->pt = NULL;

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
        cmt->len_on_disk = GRAIN_PER_PAGE;
        for(int i = 1; i < GRAIN_PER_PAGE; i++) {
            oob[ppa][i] = 0;
        }
#else
        //NVMEV_INFO("CMT remaining\n");

        if(ORIG_GLEN < GRAIN_PER_PAGE) {
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, ORIG_GLEN), 
                    GRAIN_PER_PAGE - ORIG_GLEN);
        }

        cmt->len_on_disk = ORIG_GLEN;

        for(int i = 1; i < ORIG_GLEN; i++) {
            oob[ppa][i] = 0;
        }

        for(int i = ORIG_GLEN; i < GRAIN_PER_PAGE; i++) {
            /*
             * We set to UINT_MAX here instead of 0 so that this mapping
             * page will be recognized as length 1 if the OOB needs
             * checked.
             */
            oob[ppa][i] = UINT_MAX;
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

    while(atomic_read(&cmt->outgoing) == 1) {
        cpu_relax();
    }

cache:
    if(__cache_hit(cache, lpa)) {
        struct pt_struct pte = __lpa_to_pte(shard, cmt, lpa, &pos);

        if(!IS_INITIAL_PPA(pte.ppa)) {
            //NVMEV_INFO("Hit for LPA %u IDX %lu, got grain %u\n", lpa, IDX(lpa), pte.ppa);
            d_stat.d_read_on_write += spp->pgsz;
            if(__read_and_compare(shard, pte.ppa, &h, &key, 
                        nsecs_latest, 
                        &nsecs_completed)) {
                nsecs_latest = max(nsecs_latest, nsecs_completed);
                missed = false;
                pos = UINT_MAX;
                goto lpa;
            }

            //NVMEV_INFO("Overwrite LPA %u PPA %u\n", lpa, pte.ppa);

            nsecs_latest = max(nsecs_latest, nsecs_completed);

            uint64_t g_off = G_OFFSET(pte.ppa);
            uint32_t len = 1;
            while(g_off + len < GRAIN_PER_PAGE && oob[G_IDX(pte.ppa)][g_off + len] == 0) {
                len++;
            }

            //NVMEV_INFO("Got len %u PPA %u\n", len, G_IDX(pte.ppa));

            if(append) {
                /*
                 * We are interested in the real value length this time,
                 * not just the length in grains, so that we can append
                 * within the same grain to save space.
                 */
                uint32_t prev_vlen;
                prev_vlen = __vlen_from_value((uint64_t) pte.ppa * GRAINED_UNIT);
                if(prev_vlen + vlen > spp->pgsz) {
                    NVMEV_INFO("Too big.\n");
                    mark_grain_invalid(shard, grain, glen);
                    cmd->kv_store.rsvd = U64_MAX;
                    status = KV_ERR_BUFFER_SMALL;
                    goto out;
                }

                vlen = prev_vlen + vlen;
                if(__crossing_page(spp, offset, vlen)) {
                    NVMEV_INFO("Crossing vlen %u offset %llu.\n", vlen, offset);
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
                        offset += (new_glen - glen) * GRAINED_UNIT;
                        glen = new_glen;

                        NVMEV_INFO("Set new glen to %llu\n", glen);
                    }

                    NVMEV_ASSERT((offset / spp->pgsz) == page);
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
            }

            NVMEV_ASSERT(len > 0);
            //NVMEV_INFO("OW\n");
            mark_grain_invalid(shard, pte.ppa, len);
        } else if(append) {
            /*
             * We store the length of the original KV pair that receives
             * the append in this field, to be copied later in io.c.
             *
             * Set it to 0 here to indicate that we don't need to copy
             * a previously existing KV pair, because this is an insert.
             */
            //NVMEV_INFO("Orig len set to 0.\n");
            cmd->kv_append.offset = 0;
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

    oob[page][g_off] = lpa;
    //NVMEV_INFO("Marking %llu grains from %llu page %llu\n", 
    //            glen + 1, g_off, page);
    for(int i = 1; i < glen; i++) {
        oob[page][g_off + i] = 0;
    }

    shard->ftl->max_try = (h.cnt > shard->ftl->max_try) ? h.cnt : 
                           shard->ftl->max_try;

    //NVMEV_INFO("%s for key %llu (%llu) klen %u vlen %u grain %llu PPA %llu LPA %u\n", 
    //            append ? "Append" : "Write",
    //            *(uint64_t*) (key.key), *(uint64_t*) &(cmd->kv_store.key), 
    //            klen, vlen, grain, page, lpa);

    __update_map(shard, cmt, lpa, new_pte, pos);
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

    atomic_inc(&cmt->outgoing);

    ret->cb = __release_map;
    ret->args = &cmt->outgoing;
    ret->nsecs_target = nsecs_latest;
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
