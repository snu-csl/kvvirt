// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/sched/clock.h>

#include "nvmev.h"
#include "demand_ftl.h"

#include "demand/demand.h"
#include "demand/utility.h"

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

static inline bool last_pg_in_wordline(struct conv_ftl *conv_ftl, struct ppa *ppa)
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

static uint64_t ppa2pgidx(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	uint64_t pgidx;

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
	return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
	((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
	return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
	((struct line *)a)->pos = pos;
}

static inline void consume_write_credit(struct conv_ftl *conv_ftl)
{
	conv_ftl->wfc.write_credits--;
}

static void forground_gc(struct conv_ftl *conv_ftl);

static inline void check_and_refill_write_credit(struct conv_ftl *conv_ftl)
{
	struct write_flow_control *wfc = &(conv_ftl->wfc);
	if (wfc->write_credits <= 0) {
		forground_gc(conv_ftl);

		wfc->write_credits += wfc->credits_to_refill;
	}
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
			.pos = 0,
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

	wfc->write_credits = spp->pgs_per_line;
	wfc->credits_to_refill = spp->pgs_per_line;
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
	} else if (io_type == GC_IO) {
		return &ftl->gc_wp;
	}

	NVMEV_ASSERT(0);
	return NULL;
}

static void prepare_write_pointer(struct conv_ftl *conv_ftl, uint32_t io_type)
{
	struct write_pointer *wp = __get_wp(conv_ftl, io_type);
	struct line *curline = get_next_free_line(conv_ftl);

	NVMEV_ASSERT(wp);
	NVMEV_ASSERT(curline);

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

static void advance_write_pointer(struct conv_ftl *conv_ftl, uint32_t io_type)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct write_pointer *wpp = __get_wp(conv_ftl, io_type);

	NVMEV_DEBUG_VERBOSE("current wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n",
			wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg);

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
	/* move current line to {victim,full} line list */
	if (wpp->curline->vpc == spp->pgs_per_line) {
		/* all pgs are still valid, move to full line list */
		NVMEV_ASSERT(wpp->curline->ipc == 0);
		list_add_tail(&wpp->curline->entry, &lm->full_line_list);
		lm->full_line_cnt++;
		NVMEV_DEBUG_VERBOSE("wpp: move line to full_line_list\n");
	} else {
		NVMEV_DEBUG_VERBOSE("wpp: line is moved to victim list\n");
		NVMEV_ASSERT(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
		/* there must be some invalid pages in this line */
		NVMEV_ASSERT(wpp->curline->ipc > 0);
		pqueue_insert(lm->victim_line_pq, wpp->curline);
		lm->victim_line_cnt++;
	}
	/* current line is used up, pick another empty line */
	check_addr(wpp->blk, spp->blks_per_pl);
	wpp->curline = get_next_free_line(conv_ftl);
	NVMEV_DEBUG_VERBOSE("wpp: got new clean line %d\n", wpp->curline->id);

	wpp->blk = wpp->curline->id;
	check_addr(wpp->blk, spp->blks_per_pl);

	/* make sure we are starting from page 0 in the super block */
	NVMEV_ASSERT(wpp->pg == 0);
	NVMEV_ASSERT(wpp->lun == 0);
	NVMEV_ASSERT(wpp->ch == 0);
	/* TODO: assume # of pl_per_lun is 1, fix later */
	NVMEV_ASSERT(wpp->pl == 0);
out:
	NVMEV_DEBUG_VERBOSE("advanced wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d (curline %d)\n",
			wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg, wpp->curline->id);
}

static struct ppa get_new_page(struct conv_ftl *conv_ftl, uint32_t io_type)
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
	//init_lines(conv_ftl);

	/* initialize write pointer, this is how we allocate new pages for writes */
	//prepare_write_pointer(conv_ftl, USER_IO);
	//prepare_write_pointer(conv_ftl, GC_IO);

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
	cpp->gc_thres_lines = 2; /* Need only two lines.(host write, gc)*/
	cpp->gc_thres_lines_high = 2; /* Need only two lines.(host write, gc)*/
	cpp->enable_gc_delay = 1;
	cpp->pba_pcent = (int)((1 + cpp->op_area_pcent) * 100);
}

extern struct algorithm __demand;
extern struct lower_info virt_info;
extern struct blockmanager pt_bm;

void demand_init(uint64_t size, struct ssd* ssd) 
{
    struct ssdparams *spp = &ssd->sp;
    spp->nr_segs = size / (_PPS * PAGESIZE);

    virt_info.NOB = size / (_PPS * PAGESIZE);
    virt_info.NOP = spp->tt_pgs;
    virt_info.SOB = (spp->pgs_per_blk * spp->secsz * spp->secs_per_pg) * BPS;
    virt_info.SOP = PAGESIZE;
    virt_info.PPB = spp->pgs_per_blk;
    virt_info.PPS = spp->pgs_per_blk * BPS;
    virt_info.TS = size;
    virt_info.DEV_SIZE = size;
    virt_info.all_pages_in_dev = size / PAGESIZE;

    virt_info.create(&virt_info, &pt_bm);

    int temp[PARTNUM];
    temp[MAP_S] = MAPPART_SEGS;
    temp[DATA_S] = spp->nr_segs - MAPPART_SEGS;
    pt_bm.pt_create(&pt_bm, PARTNUM, temp, &virt_info);

    //printk("Before demand create.\n");
    demand_create(&virt_info, &pt_bm, &__demand, ssd, size);
    print_demand_stat(&d_stat);

    printk("NOB %u\n", virt_info.NOB);
    printk("NOP %u\n", virt_info.NOP);
    printk("SOB %u\n", virt_info.SOB);
    printk("SOP %u\n", virt_info.SOP);
    printk("PPB %u\n", virt_info.PPB);
    printk("PPS %u\n", virt_info.PPS);
    printk("TS %llu\n", virt_info.TS);
    printk("DEV_SIZE %llu\n", virt_info.DEV_SIZE);
    printk("all_pages_in_dev %llu\n", virt_info.all_pages_in_dev);
    printk("DRAM SIZE %lu\n", spp->dram_size);
}

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

    size = 512LU << 20;
    dsize = size;

	ssd_init_params(&spp, size, nr_parts);
	conv_init_params(&cpp);

	conv_ftls = kmalloc(sizeof(struct conv_ftl) * nr_parts, GFP_KERNEL);

	for (i = 0; i < nr_parts; i++) {
		ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
		ssd_init(ssd, &spp, cpu_nr_dispatcher);
		conv_init_ftl(&conv_ftls[i], &cpp, ssd);
	}

	/* PCIe, Write buffer are shared by all instances*/
	for (i = 1; i < nr_parts; i++) {
		kfree(conv_ftls[i].ssd->pcie->perf_model);
		kfree(conv_ftls[i].ssd->pcie);
		kfree(conv_ftls[i].ssd->write_buffer);

		conv_ftls[i].ssd->pcie = conv_ftls[0].ssd->pcie;
		conv_ftls[i].ssd->write_buffer = conv_ftls[0].ssd->write_buffer;
	}

    demand_init(dsize, conv_ftls[0].ssd);

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

	for (i = 0; i < nr_parts; i++) {
		conv_remove_ftl(&conv_ftls[i]);
		ssd_remove(conv_ftls[i].ssd);
		kfree(conv_ftls[i].ssd);
	}

    print_demand_stat(&d_stat);

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

static inline struct line *get_line(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	return &(conv_ftl->lm.lines[ppa->g.blk]);
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct line *line;

	/* update corresponding page status */
	pg = get_pg(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);
	pg->status = PG_INVALID;

	/* update corresponding block status */
	blk = get_blk(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
	blk->ipc++;
	NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
	blk->vpc--;

	/* update corresponding line status */
	line = get_line(conv_ftl, ppa);
	NVMEV_ASSERT(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
	if (line->vpc == spp->pgs_per_line) {
		NVMEV_ASSERT(line->ipc == 0);
		was_full_line = true;
	}
	line->ipc++;
	NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
	/* Adjust the position of the victime line in the pq under over-writes */
	if (line->pos) {
		/* Note that line->vpc will be updated by this call */
		pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
	} else {
		line->vpc--;
	}

	if (was_full_line) {
		/* move line: "full" -> "victim" */
		list_del_init(&line->entry);
		lm->full_line_cnt--;
		pqueue_insert(lm->victim_line_pq, line);
		lm->victim_line_cnt++;
	}
}

static void mark_page_valid(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct line *line;

	/* update page status */
	pg = get_pg(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_FREE);
	pg->status = PG_VALID;

	/* update corresponding block status */
	blk = get_blk(conv_ftl->ssd, ppa);
	NVMEV_ASSERT(blk->vpc >= 0 && blk->vpc < spp->pgs_per_blk);
	blk->vpc++;

	/* update corresponding line status */
	line = get_line(conv_ftl, ppa);
	NVMEV_ASSERT(line->vpc >= 0 && line->vpc < spp->pgs_per_line);
	line->vpc++;
}

static void mark_block_free(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_block *blk = get_blk(conv_ftl->ssd, ppa);
	struct nand_page *pg = NULL;
	int i;

	for (i = 0; i < spp->pgs_per_blk; i++) {
		/* reset page status */
		pg = &blk->pg[i];
		NVMEV_ASSERT(pg->nsecs == spp->secs_per_pg);
		pg->status = PG_FREE;
	}

	/* reset block status */
	NVMEV_ASSERT(blk->npgs == spp->pgs_per_blk);
	blk->ipc = 0;
	blk->vpc = 0;
	blk->erase_cnt++;
}

static void gc_read_page(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
	/* advance conv_ftl status, we don't care about how long it takes */
	if (cpp->enable_gc_delay) {
		struct nand_cmd gcr = {
			.type = GC_IO,
			.cmd = NAND_READ,
			.stime = 0,
			.xfer_size = spp->pgsz,
			.interleave_pci_dma = false,
			.ppa = ppa,
		};
		ssd_advance_nand(conv_ftl->ssd, &gcr);
	}
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct conv_ftl *conv_ftl, struct ppa *old_ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
	struct ppa new_ppa;
	uint64_t lpn = get_rmap_ent(conv_ftl, old_ppa);

	NVMEV_ASSERT(valid_lpn(conv_ftl, lpn));
	new_ppa = get_new_page(conv_ftl, GC_IO);
	/* update maptbl */
	set_maptbl_ent(conv_ftl, lpn, &new_ppa);
	/* update rmap */
	set_rmap_ent(conv_ftl, lpn, &new_ppa);

	mark_page_valid(conv_ftl, &new_ppa);

	/* need to advance the write pointer here */
	advance_write_pointer(conv_ftl, GC_IO);

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

	/* advance per-ch gc_endtime as well */
#if 0
	new_ch = get_ch(conv_ftl, &new_ppa);
	new_ch->gc_endtime = new_ch->next_ch_avail_time;

	new_lun = get_lun(conv_ftl, &new_ppa);
	new_lun->gc_endtime = new_lun->next_lun_avail_time;
#endif

	return 0;
}

static struct line *select_victim_line(struct conv_ftl *conv_ftl, bool force)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *victim_line = NULL;

	victim_line = pqueue_peek(lm->victim_line_pq);
	if (!victim_line) {
		return NULL;
	}

	if (!force && (victim_line->vpc > (spp->pgs_per_line / 8))) {
		return NULL;
	}

	pqueue_pop(lm->victim_line_pq);
	victim_line->pos = 0;
	lm->victim_line_cnt--;

	/* victim_line is a danggling node now */
	return victim_line;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct nand_page *pg_iter = NULL;
	int cnt = 0;
	int pg;

	for (pg = 0; pg < spp->pgs_per_blk; pg++) {
		ppa->g.pg = pg;
		pg_iter = get_pg(conv_ftl->ssd, ppa);
		/* there shouldn't be any free page in victim blocks */
		NVMEV_ASSERT(pg_iter->status != PG_FREE);
		if (pg_iter->status == PG_VALID) {
			gc_read_page(conv_ftl, ppa);
			/* delay the maptbl update until "write" happens */
			gc_write_page(conv_ftl, ppa);
			cnt++;
		}
	}

	NVMEV_ASSERT(get_blk(conv_ftl->ssd, ppa)->vpc == cnt);
}

/* here ppa identifies the block we want to clean */
static void clean_one_flashpg(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct convparams *cpp = &conv_ftl->cp;
	struct nand_page *pg_iter = NULL;
	int cnt = 0, i = 0;
	uint64_t completed_time = 0;
	struct ppa ppa_copy = *ppa;

	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(conv_ftl->ssd, &ppa_copy);
		/* there shouldn't be any free page in victim blocks */
		NVMEV_ASSERT(pg_iter->status != PG_FREE);
		if (pg_iter->status == PG_VALID)
			cnt++;

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

	if (cnt <= 0)
		return;

	if (cpp->enable_gc_delay) {
		struct nand_cmd gcr = {
			.type = GC_IO,
			.cmd = NAND_READ,
			.stime = 0,
			.xfer_size = spp->pgsz * cnt,
			.interleave_pci_dma = false,
			.ppa = &ppa_copy,
		};
		completed_time = ssd_advance_nand(conv_ftl->ssd, &gcr);
	}

	for (i = 0; i < spp->pgs_per_flashpg; i++) {
		pg_iter = get_pg(conv_ftl->ssd, &ppa_copy);

		/* there shouldn't be any free page in victim blocks */
		if (pg_iter->status == PG_VALID) {
			/* delay the maptbl update until "write" happens */
			gc_write_page(conv_ftl, &ppa_copy);
		}

		ppa_copy.g.pg++;
	}
}

static void mark_line_free(struct conv_ftl *conv_ftl, struct ppa *ppa)
{
	struct line_mgmt *lm = &conv_ftl->lm;
	struct line *line = get_line(conv_ftl, ppa);
	line->ipc = 0;
	line->vpc = 0;
	/* move this line to free line list */
	list_add_tail(&line->entry, &lm->free_line_list);
	lm->free_line_cnt++;
}

static int do_gc(struct conv_ftl *conv_ftl, bool force)
{
	struct line *victim_line = NULL;
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	struct ppa ppa;
	int flashpg;

	victim_line = select_victim_line(conv_ftl, force);
	if (!victim_line) {
		return -1;
	}

	ppa.g.blk = victim_line->id;
	NVMEV_DEBUG_VERBOSE("GC-ing line:%d,ipc=%d(%d),victim=%d,full=%d,free=%d\n", ppa.g.blk,
		    victim_line->ipc, victim_line->vpc, conv_ftl->lm.victim_line_cnt,
		    conv_ftl->lm.full_line_cnt, conv_ftl->lm.free_line_cnt);

	conv_ftl->wfc.credits_to_refill = victim_line->ipc;

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
						ssd_advance_nand(conv_ftl->ssd, &gce);
					}

					lunp->gc_endtime = lunp->next_lun_avail_time;
				}
			}
		}
	}

	/* update line status */
	mark_line_free(conv_ftl, &ppa);

	return 0;
}

static void forground_gc(struct conv_ftl *conv_ftl)
{
	if (should_gc_high(conv_ftl)) {
		NVMEV_DEBUG_VERBOSE("should_gc_high passed");
		/* perform GC here until !should_gc(conv_ftl) */
		do_gc(conv_ftl, true);
	}
}

static bool is_same_flash_page(struct conv_ftl *conv_ftl, struct ppa ppa1, struct ppa ppa2)
{
	struct ssdparams *spp = &conv_ftl->ssd->sp;
	uint32_t ppa1_page = ppa1.g.pg / spp->pgs_per_flashpg;
	uint32_t ppa2_page = ppa2.g.pg / spp->pgs_per_flashpg;

	return (ppa1.h.blk_in_ssd == ppa2.h.blk_in_ssd) && (ppa1_page == ppa2_page);
}

/*
 * If we find a KV pair in the write buffer, we copy the data directly
 * to the buffer provided by the user here. We can't do it later in
 * __do_perform_io_kv in io.c because that copies from virt's
 * reserved disk memory, on which KV pairs in the write buffer don't
 * exist yet.
 */

static unsigned int __quick_copy(struct nvme_kv_command *cmd, void *buf, uint64_t buf_len)
{
	size_t offset;
	size_t length, remaining;
	int prp_offs = 0;
	int prp2_offs = 0;
	u64 paddr;
	u64 *paddr_list = NULL;
	size_t nsid = 0;  // 0-based

    bool read = cmd->common.opcode == nvme_cmd_kv_retrieve;
    //printk("In quick copy for a %s!\n", read ? "read" : "write");

    nsid = 0;

    if(read) {
        offset = cmd->kv_retrieve.rsvd2;
        length = cmd->kv_retrieve.value_len << 2;
    } else {
        offset = cmd->kv_store.rsvd2;
        length = cmd->kv_store.value_len << 2;
    }

	remaining = length;
    //printk("Length %lu\n", length);

	while (remaining) {
		size_t io_size;
		void *vaddr;
		size_t mem_offs = 0;

		prp_offs++;
		if (prp_offs == 1) {
            if(read) {
                paddr = cmd->kv_retrieve.dptr.prp1;
            } else {
                paddr = cmd->kv_store.dptr.prp1;
            }
		} else if (prp_offs == 2) {
            if(read) {
                paddr = cmd->kv_retrieve.dptr.prp2;
            } else {
                paddr = cmd->kv_store.dptr.prp2;
            }
			if (remaining > PAGE_SIZE) {
				paddr_list = kmap_atomic_pfn(PRP_PFN(paddr)) +
					     (paddr & PAGE_OFFSET_MASK);
				paddr = paddr_list[prp2_offs++];
			}
		} else {
			paddr = paddr_list[prp2_offs++];
		}

		vaddr = kmap_atomic_pfn(PRP_PFN(paddr));
		io_size = min_t(size_t, remaining, PAGE_SIZE);

		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

		if (!read) {
            //printk("Quick copying for write key %s size %lu data %s!\n", 
                    //cmd->kv_retrieve.key, io_size, (char*) buf);
			memcpy(buf, vaddr + mem_offs, io_size);
		} else {
            //printk("Quick copying for read key %s size %lu data %s!\n", 
                    //cmd->kv_retrieve.key, io_size, (char*) buf);
			memcpy(vaddr + mem_offs, buf, io_size);
		}

		kunmap_atomic(vaddr);

		remaining -= io_size;
		offset += io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);

    //printk("Done\n");
	return length;
}


bool end_w(struct request *req) 
{
    return true;
}

bool end_r(struct request *req) 
{
    if(req->ppa == UINT_MAX - 1) {
        return true;
    }

    uint64_t offset = G_OFFSET(req->ppa);
    //printk("%s ending read key %s offset %llu ppa %llu\n", __func__, req->key.key, offset, req->ppa);

    req->ppa = (G_IDX(req->ppa) * PAGESIZE) + (G_OFFSET(req->ppa) * GRAINED_UNIT);
    //printk("%s switching ppa to %llu\n", __func__, req->ppa);

    return true;
}

char read_buf[4096];
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

    memset(&d_req, 0x0, sizeof(d_req));

    d_req.ssd = conv_ftl->ssd;
    d_req.req = req;
    d_req.hash_params = NULL;

    key.key = (char*)kzalloc(16, GFP_KERNEL);

    BUG_ON(!key.key);
    BUG_ON(!cmd->kv_retrieve.key);
    BUG_ON(!cmd);

    uint8_t length = cmd_key_length(tmp);

    memcpy(key.key, cmd->kv_retrieve.key, length);
    key.len = cmd_key_length(*cmd);
    d_req.key = key;

    //printk("Read for key %s len %u\n", key.key, key.len);

    struct value_set *value;
    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->value = read_buf;
    value->ssd = conv_ftl->ssd;
    value->length = 1024;
    d_req.value = value;
    d_req.end_req = &end_r;
    nsecs_latest = nsecs_xfer_completed = __demand.read(&d_req);

    //printk("Demand passed for key %s len %u data %s\n", key.key, key.len, (char*) value->value);

    //if ((cmd->rw.control & NVME_RW_FUA) || (spp->write_early_completion == 0)) {
    //    /* Wait all flash operations */
    ret->nsecs_target = nsecs_latest;
    //} else {
    //    /* Early completion */
    //    ret->nsecs_target = nsecs_xfer_completed;
    //}
 
    cmd->kv_store.rsvd2 = d_req.ppa;
    //printk("Storing ppa %llu\n", d_req.ppa);

    if (d_req.ppa == UINT_MAX - 1) {
        __quick_copy(cmd, value->value, value->length);
    }

    if(d_req.ppa == UINT_MAX) {
        ret->status = KV_ERR_KEY_NOT_EXIST;
    } else {
        ret->nsecs_target = nsecs_latest;
        ret->status = NVME_SC_SUCCESS;
    }

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

static bool conv_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
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

    memset(&d_req, 0x0, sizeof(d_req));

    d_req.ssd = conv_ftl->ssd;
    d_req.req = req;
    d_req.hash_params = NULL;

    key.key = NULL;
    key.key = (char*)kzalloc(16, GFP_KERNEL);

    BUG_ON(!key.key);
    BUG_ON(!cmd->kv_store.key);
    BUG_ON(!cmd);

    uint8_t length = cmd_key_length(tmp);

    memcpy(key.key, cmd->kv_retrieve.key, length);
    key.len = cmd_key_length(*cmd);
    d_req.key = key;

    //printk("Write for key %s len %u\n", key.key, length);

    struct value_set *value;
    value = (struct value_set*)kzalloc(sizeof(*value), GFP_KERNEL);
    value->value = (char*)kzalloc(1024, GFP_KERNEL);
    value->ssd = conv_ftl->ssd;
    value->length = 1024;
    d_req.value = value;
    d_req.end_req = &end_w;
    d_req.sqid = req->sq_id;

    __quick_copy(cmd, value->value, value->length);
    nsecs_latest = nsecs_xfer_completed = __demand.write(&d_req);

	//if ((cmd->rw.control & NVME_RW_FUA) || (spp->write_early_completion == 0)) {
	//	/* Wait all flash operations */
	ret->nsecs_target = nsecs_latest;
	//} else {
	//	/* Early completion */
	//	ret->nsecs_target = nsecs_xfer_completed;
	//}
    
    /*
     * write() puts the KV pair in the memory buffer, which is flushed to
     * disk at a later time.
     *
     * We set rsvd2 to UINT_MAX here so that in __do_perform_io_kv we skip
     * a memory copy to virt's reserved disk memory (since this KV pair isn't
     * actually on the disk yet).
     *
     * Even if this pair causes a flush of the write buffer, that's done 
     * asynchronously and the copy to virt's reserved disk memory happens
     * in nvmev_io_worker().
     */
    
    cmd->kv_store.rsvd2 = UINT_MAX;
	ret->status = NVME_SC_SUCCESS;

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

	NVMEV_DEBUG_VERBOSE("%s: latency=%llu\n", __func__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
	return;
}

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
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

bool kv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;

    switch (cmd->common.opcode) {
        case nvme_cmd_kv_store:
            ret->nsecs_target = conv_write(ns, req, ret);
            //NVMEV_INFO("%d, %llu, %llu\n", cmd_value_length(*((struct nvme_kv_command *)cmd)),
            //        __get_wallclock(), ret->nsecs_target);
            break;
        case nvme_cmd_kv_retrieve:
            ret->nsecs_target = conv_read(ns, req, ret);
            //NVMEV_INFO("%d, %llu, %llu\n", cmd_value_length(*((struct nvme_kv_command *)cmd)),
            //        __get_wallclock(), ret->nsecs_target);
            break;
        case nvme_cmd_write:
        case nvme_cmd_read:
        case nvme_cmd_flush:
            ret->nsecs_target = __get_wallclock() + 10;
            break;
        default:
            NVMEV_ERROR("%s: command not implemented: %s (0x%x)\n", __func__,
                    nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
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
		if (!conv_write(ns, req, ret))
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
