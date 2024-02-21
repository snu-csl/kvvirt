/*
 * Dynamic Value GC Implementation
 */

#ifdef DVALUE

#include "cache.h"
#include "demand.h"
#include "utility.h"
#include "cache.h"
#include "./interface/interface.h"

#include <linux/sort.h>

extern struct algo_req *make_algo_req_default(uint8_t, value_set *);
static int lpa_cmp(const void *a, const void *b)
{
    const struct lpa_len_ppa *da = a, *db = b;

    if (db->lpa < da->lpa) return -1;
    if (db->lpa > da->lpa) return 1;
    return 0;
}

bool __read_cmt(lpa_t lpa, uint64_t *read_cmts, uint64_t cnt) {
    uint64_t idx = IDX(lpa);

    for(int i = 0; i < cnt; i++) {
        NVMEV_DEBUG("Checking %llu against %llu\n", idx, read_cmts[i]);
        if(read_cmts[i] == idx) {
            return true;
        }
    }

    return false;
}

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

struct copy_args {
    struct ssdparams *spp;
    uint32_t idx;
    uint8_t *src;
    uint8_t *dst;
    uint32_t g_len;
    atomic_t *rem;
};

void __copy_work(void *voidargs, uint64_t*, uint64_t*) {
    struct copy_args *args = (struct copy_args*) voidargs;
    struct ssdparams *spp;
    uint8_t *src;
    uint8_t *dst;
    uint32_t g_len;
    atomic_t *rem;

    spp = args->spp;
    src = args->src;
    dst = args->dst;
    g_len = args->g_len;
    rem = args->rem;

    memcpy(dst, src, g_len * GRAINED_UNIT);

    NVMEV_INFO("%s Copying IDX %u grain length %u\n", 
                __func__, args->idx, g_len);

    atomic_dec(rem);
    kfree(args);
}

#ifdef GC_STANDARD
//struct copy_args {
//    struct ssdparams *spp;
//    uint32_t idx;
//    uint8_t *src;
//    uint8_t *dst;
//    atomic_t *rem;
//};
//
//void __copy_work(void *voidargs, uint64_t*, uint64_t*) {
//    struct copy_args *args = (struct copy_args*) voidargs;
//    struct ssdparams *spp;
//    uint8_t *src;
//    uint8_t *dst;
//    atomic_t *rem;
//
//    spp = args->spp;
//    dst = args->dst;
//    src = args->src;
//    rem = args->rem;
//
//    memcpy(dst, src, spp->pgsz);
//
//    NVMEV_DEBUG("%s IDX %u\n", __func__, args->idx);
//
//    atomic_dec(rem);
//    kfree(args);
//}

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

int do_bulk_mapping_update_v(struct demand_shard *shard, 
                             struct lpa_len_ppa *ppas, int nr_valid_grains, 
                             uint64_t *read_cmts, uint64_t read_cmt_cnt) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct demand_cache *cache = shard->cache;
	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);
    bool skip_all = true;
    uint64_t **oob = shard->oob;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t nsecs_completed = 0, nsecs_latest = 0, reads_done = 0;

    sort(ppas, nr_valid_grains, sizeof(struct lpa_len_ppa), &lpa_cmp, NULL);

    uint64_t unique_cmts = 0;
    uint64_t prev = UINT_MAX;
    for(int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;
        uint64_t idx = IDX(lpa);
        if(idx != prev && ppas[i].lpa != UINT_MAX) {
            unique_cmts++;
            prev = idx;
        }
    }

    NVMEV_DEBUG("There are %llu unique CMTs to update with %u pairs. Read %llu already.\n", 
                 unique_cmts, nr_valid_grains, read_cmt_cnt);

    if(unique_cmts == 0) {
        kfree(skip_update);
        return 0;
    }

    uint8_t** pts = kmalloc(sizeof(uint8_t*) * unique_cmts, GFP_KERNEL);
    uint64_t cmts_loaded = 0;
	/* read mapping table which needs update */
    volatile int nr_update_tpages = 0;
	for (int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;

        if(lpa == UINT_MAX || lpa == UINT_MAX - 1) {
            skip_update[i] = true;
            continue;
        }

        NVMEV_DEBUG("%s updating mapping of LPA %u IDX %u to PPA %u\n", 
                __func__, lpa, IDX(ppas[i].lpa), ppas[i].new_ppa);

        struct cmt_struct *cmt = cache->get_cmt(cache, lpa);
		if (cache->is_hit(cache, lpa)) {
            //NVMEV_INFO("LPA %u PPA %u IDX %u cached update in %s.\n", 
            //            lpa, ppas[i].new_ppa, cmt->idx, __func__);
			struct pt_struct pte = cache->get_pte(shard, lpa);
			pte.ppa = ppas[i].new_ppa;
			cache->update(shard, lpa, pte);
			skip_update[i] = true;
		} else {
            //NVMEV_INFO("LPA %u PPA %u IDX %u not cached update in %s.\n", 
            //            lpa, ppas[i].new_ppa, cmt->idx, __func__);
            if (cmt->t_ppa == UINT_MAX) {
                NVMEV_DEBUG("%s But the CMT had already been read here.\n", __func__);
                continue;
            }

            skip_all = false;
            uint64_t off = shard_off + ((uint64_t) cmt->t_ppa * spp->pgsz);

            if(0 && __read_cmt(ppas[i].lpa, read_cmts, read_cmt_cnt)) {
                //NVMEV_DEBUG("%s skipping read PPA %u as it was read earlier.\n",
                //            __func__, cmt->t_ppa);
                //uint64_t off = shard_off + (cmt->t_ppa * spp->pgsz);
                //memcpy(pts[cmts_loaded++]->value, nvmev_vdev->ns[0].mapped + off, spp->pgsz);
            } else {
                NVMEV_DEBUG("Trying to read CMT PPA %u\n", cmt->t_ppa);
                pts[cmts_loaded++] = ((uint8_t*) nvmev_vdev->ns[0].mapped) + off;

                struct ppa p = ppa_to_struct(spp, cmt->t_ppa);
                struct nand_cmd swr = {
                    .type = GC_MAP_IO,
                    .cmd = NAND_READ,
                    .interleave_pci_dma = false,
                    .xfer_size = spp->pgsz,
                    .stime = 0,
                };

                swr.ppa = &p;
                nsecs_completed = ssd_advance_nand(ssd, &swr);
                nsecs_latest = max(nsecs_latest, nsecs_completed);

                NVMEV_DEBUG("%s marking mapping PPA %u invalid while it was being read during GC.\n",
                        __func__, cmt->t_ppa);
                mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, 0), GRAIN_PER_PAGE);
            }
            cmt->t_ppa = UINT_MAX;

            d_stat.trans_r_dgc++;
            nr_update_tpages++;
        }
    }

    if(skip_all) {
        kfree(pts); 
        kfree(skip_update);
        return 0;
    }

    reads_done = nsecs_latest;

    atomic_t rem;
    atomic_set(&rem, cmts_loaded);

    cmts_loaded = 0;
    /* write */
	for (int i = 0; i < nr_valid_grains; i++) {
		if (skip_update[i]) {
			continue;
		}

        uint64_t idx = IDX(ppas[i].lpa);
        lpa_t lpa = ppas[i].lpa;

        struct cmt_struct t_cmt;
        t_cmt.idx = idx;
        t_cmt.pt = (struct pt_struct*) pts[cmts_loaded]; 

        uint64_t offset = OFFSET(ppas[i].lpa);
        t_cmt.pt[offset].ppa = ppas[i].new_ppa;

        NVMEV_DEBUG("%s 1 setting LPA %u to PPA %u\n",
                     __func__, ppas[i].lpa, ppas[i].new_ppa);

        if(G_IDX(ppas[i].new_ppa) > spp->tt_pgs) {
            printk("Tried to convert PPA %u\n", G_IDX(ppas[i].new_ppa));
            NVMEV_ASSERT(false);
        }

        while(i + 1 < nr_valid_grains && IDX(ppas[i + 1].lpa) == idx) {
            offset = OFFSET(ppas[i + 1].lpa);
            t_cmt.pt[offset].ppa = ppas[i + 1].new_ppa;

            if(G_IDX(ppas[i].new_ppa) > spp->tt_pgs) {
                printk("Tried to convert PPA %u\n", G_IDX(ppas[i].new_ppa));
                NVMEV_ASSERT(false);
            }

            NVMEV_DEBUG("%s 2 setting LPA %u to PPA %u\n",
                         __func__, ppas[i + 1].lpa, ppas[i + 1].new_ppa);

            i++;
        }

        struct ppa p;
        ppa_t ppa;
again:
        p = get_new_page(shard, GC_MAP_IO);
        ppa = ppa2pgidx(shard, &p);

        advance_write_pointer(shard, GC_MAP_IO);
        mark_page_valid(shard, &p);
        mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        if(ppa == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
            goto again;
        }

        oob[ppa][0] = lpa;

        NVMEV_DEBUG("%s writing CMT IDX %llu back to PPA %u\n",
                    __func__, idx, ppa);

        uint64_t off = shard_off + ((uint64_t) ppa * spp->pgsz);
        uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;
    
        struct copy_args *args = 
        (struct copy_args*) kzalloc(sizeof(*args), GFP_KERNEL);
        
        args->spp = spp;
        args->idx = idx;
        args->dst = ptr;
        args->src = pts[cmts_loaded];
        args->rem = &rem;

        schedule_internal_operation_cb(INT_MAX, 0,
                                       NULL, 0, 0, 
                                       (void*) __copy_work, 
                                       (void*) args, 
                                       false, NULL);
        //memcpy(ptr, pts[cmts_loaded], spp->pgsz);

        if (last_pg_in_wordline(shard, &p)) {
            struct nand_cmd swr = {
                .type = GC_MAP_IO,
                .cmd = NAND_WRITE,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            };

            swr.stime = reads_done; // __get_wallclock();
            swr.ppa = &p;

            ssd_advance_nand(shard->ssd, &swr);
            //schedule_internal_operation(req->sq_id, nsecs_completed, wbuf,
            //        spp->pgs_per_oneshotpg * spp->pgsz);
        }

        d_stat.trans_w_dgc++;

        struct cmt_struct *cmt = cache->member.cmt[idx];
        NVMEV_ASSERT(atomic_read(&cmt->outgoing) == 0);

        cmt->state = CLEAN;

        NVMEV_ASSERT(!cmt->lru_ptr);
        NVMEV_ASSERT(!cmt->pt);
        cmt->t_ppa = ppa;
        cmts_loaded++;

        if(cmts_loaded % 4096 == 0) {
            __reclaim_completed_reqs();
        }
    }

    while(atomic_read(&rem) > 0) {
        cpu_relax();
    }

    kfree(pts); 
	kfree(skip_update);
	return 0;
}
#else
void __update_pt(struct demand_shard *shard, struct cmt_struct *cmt, 
                 lpa_t lpa, ppa_t ppa) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    uint64_t entry_sz = ENTRY_SIZE;

    NVMEV_INFO("Trying to update IDX %u LPA %u PPA %u\n",
                cmt->idx, lpa, ppa);

    for(int i = 0; i < cmt->cached_cnt; i++) {
        if(cmt->pt[i].lpa == lpa) {
            cmt->pt[i].ppa = ppa;
            return;
        }
    }

    NVMEV_INFO("IDX %u TPPA %u cached cnt %u\n", 
                cmt->idx, cmt->t_ppa, cmt->cached_cnt);
    for(int i = 0; i < cmt->cached_cnt; i++) {
        NVMEV_INFO("Had LPA %u PPA %u\n", cmt->pt[i].lpa, cmt->pt[i].ppa);
    }

    NVMEV_ASSERT(false);
}

uint64_t __already_read(uint64_t *read_ppas, uint64_t cnt, uint64_t pg) {
    for(int i = 0; i < cnt; i++) {
        if(read_ppas[cnt] == pg) {
            return i;
        }
    }

    return UINT_MAX;
}

int do_bulk_mapping_update_v(struct demand_shard *shard, 
                             struct lpa_len_ppa *ppas, int nr_valid_grains, 
                             uint64_t *read_cmts, uint64_t read_cmt_cnt) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct demand_cache *cache = shard->cache;
	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);
    bool skip_all = true;
    uint64_t **oob = shard->oob;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t nsecs_completed = 0, nsecs_latest = 0, reads_done = 0;

    sort(ppas, nr_valid_grains, sizeof(struct lpa_len_ppa), &lpa_cmp, NULL);

    uint64_t unique_cmts = 0;
    uint64_t prev = UINT_MAX;
    for(int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;
        uint64_t idx = IDX(lpa);
        if(idx != prev && ppas[i].lpa != UINT_MAX) {
            unique_cmts++;
            prev = idx;
        }
    }

    NVMEV_INFO("There are %llu unique CMTs to update with %u pairs. Read %llu already.\n", 
                 unique_cmts, nr_valid_grains, read_cmt_cnt);

    if(unique_cmts == 0) {
        kfree(skip_update);
        return 0;
    }

    uint8_t** pts = kmalloc(sizeof(uint8_t*) * unique_cmts, GFP_KERNEL);
    uint64_t *read_ppas = kmalloc(sizeof(uint64_t) * unique_cmts, GFP_KERNEL);
    uint64_t read_ppa_cnt = 0;
    uint64_t cmts_loaded = 0;
    uint64_t g_len_total = 0;

	/* read mapping table which needs update */
    volatile int nr_update_tpages = 0;
	for (int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;

        if(lpa == UINT_MAX || lpa == UINT_MAX - 1) {
            skip_update[i] = true;
            continue;
        }

        NVMEV_INFO("%s updating mapping of LPA %u IDX %lu to PPA %u\n", 
                __func__, lpa, IDX(ppas[i].lpa), ppas[i].new_ppa);

        struct cmt_struct *cmt = cache->get_cmt(cache, lpa);
		if (cache->is_hit(cache, lpa)) {
            NVMEV_INFO("LPA %u PPA %u IDX %u cached update in %s.\n", 
                        lpa, ppas[i].new_ppa, cmt->idx, __func__);
			//struct pt_struct pte = cache->get_pte(shard, lpa);
			//pte.ppa = ppas[i].new_ppa;
            __update_pt(shard, cmt, lpa, ppas[i].new_ppa);
			//cache->update(shard, lpa, pte);
			skip_update[i] = true;
		} else {
            //NVMEV_INFO("LPA %u PPA %u IDX %u not cached update in %s.\n", 
            //            lpa, ppas[i].new_ppa, cmt->idx, __func__);

            uint64_t off, g_off;

            g_len_total += cmt->len_on_disk;
            g_off = cmt->g_off * GRAINED_UNIT;

            prev = __already_read(read_ppas, read_ppa_cnt, cmt->t_ppa);
            if (cmt->t_ppa == UINT_MAX) {
                NVMEV_INFO("%s But the CMT had already been read here.\n", __func__);
                continue;
            } else if(prev != UINT_MAX) {
#ifdef GC_STANDARD
                /*
                 * In the original scheme, this cmt->t_ppa should have been
                 * set to UINT_MAX below.
                 */
                NVMEV_ASSERT(false);
#endif
                NVMEV_INFO("%s IDX %u PPA %u grain %llu had already been read here.\n", 
                            __func__, cmt->idx, cmt->t_ppa, cmt->g_off);
                off = shard_off + ((uint64_t) cmt->t_ppa * spp->pgsz) + g_off;
                pts[cmts_loaded++] = ((uint8_t*) nvmev_vdev->ns[0].mapped) + off;

                struct pt_struct *tmp = 
                (struct pt_struct*)(((uint8_t*) nvmev_vdev->ns[0].mapped) + off); 
                NVMEV_ASSERT(IDX(tmp[0].lpa) == cmt->idx);

                mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, cmt->g_off), 
                                   GRAIN_PER_PAGE);
                cmt->t_ppa = UINT_MAX;
                continue;
            }

            skip_all = false;
            off = shard_off + ((uint64_t) cmt->t_ppa * spp->pgsz) + g_off;

            NVMEV_DEBUG("Trying to read CMT PPA %u\n", cmt->t_ppa);
            pts[cmts_loaded++] = ((uint8_t*) nvmev_vdev->ns[0].mapped) + off;

            struct pt_struct *tmp = 
            (struct pt_struct*)(((uint8_t*) nvmev_vdev->ns[0].mapped) + off); 

            if(IDX(tmp[0].lpa) != cmt->idx) {
                NVMEV_INFO("Had IDX %lu instead of IDX %u TPPA %u\n", 
                            IDX(tmp[0].lpa), cmt->idx, cmt->t_ppa);
            }

            NVMEV_ASSERT(IDX(tmp[0].lpa) == cmt->idx);

            struct ppa p = ppa_to_struct(spp, cmt->t_ppa);
            struct nand_cmd swr = {
                .type = GC_MAP_IO,
                .cmd = NAND_READ,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz,
                .stime = 0,
            };

            swr.ppa = &p;
            nsecs_completed = ssd_advance_nand(ssd, &swr);
            nsecs_latest = max(nsecs_latest, nsecs_completed);

            NVMEV_INFO("%s marking PPA %u grain %llu invalid while it was being read during GC.\n",
                    __func__, cmt->t_ppa, cmt->g_off);
            mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, cmt->g_off), 
                               cmt->len_on_disk);
            cmt->t_ppa = UINT_MAX;

            read_ppas[read_ppa_cnt++] = cmt->t_ppa;

            d_stat.trans_r_dgc++;
            nr_update_tpages++;
        }
    }

    if(skip_all) {
        kfree(pts); 
        kfree(skip_update);
        return 0;
    }

    reads_done = nsecs_latest;

    atomic_t rem;
    atomic_set(&rem, cmts_loaded);

    uint32_t pg_cnt = g_len_total / GRAIN_PER_PAGE;
    uint32_t g_off = 0;
    struct ppa p;
    uint64_t ppa;
    uint64_t ppa_idx = 0;

    if(g_len_total % GRAIN_PER_PAGE) {
        pg_cnt++;
    }

    struct ppa *pgs = (struct ppa*) kzalloc(sizeof(struct ppa) * pg_cnt, GFP_KERNEL);
    for(int i = 0; i < pg_cnt; i++) {
again:
        pgs[i] = get_new_page(shard, GC_MAP_IO);
        ppa = ppa2pgidx(shard, &pgs[i]);
        advance_write_pointer(shard, GC_MAP_IO);
        mark_page_valid(shard, &pgs[i]);
        mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        if(ppa == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
            goto again;
        }
    }

    p = pgs[ppa_idx++];
    ppa = ppa2pgidx(shard, &p);

    cmts_loaded = 0;
    /* write */
	for (int i = 0; i < nr_valid_grains; i++) {
		if (skip_update[i]) {
			continue;
		}

        uint64_t idx = IDX(ppas[i].lpa);
        lpa_t lpa = ppas[i].lpa;

        struct cmt_struct *r_cmt = cache->get_cmt(cache, IDX2LPA(idx));
        struct cmt_struct t_cmt;

        t_cmt.t_ppa = r_cmt->t_ppa;
        t_cmt.cached_cnt = r_cmt->cached_cnt;
        t_cmt.len_on_disk = r_cmt->len_on_disk;
        t_cmt.idx = idx;
        t_cmt.pt = (struct pt_struct*) pts[cmts_loaded]; 

        __update_pt(shard, &t_cmt, ppas[i].lpa, ppas[i].new_ppa);
        //uint64_t offset = OFFSET(ppas[i].lpa);
        //t_cmt.pt[offset].ppa = ppas[i].new_ppa;

        NVMEV_INFO("%s 1 setting LPA %u to PPA %u\n",
                     __func__, ppas[i].lpa, ppas[i].new_ppa);

        if(G_IDX(ppas[i].new_ppa) > spp->tt_pgs) {
            printk("Tried to convert PPA %u\n", G_IDX(ppas[i].new_ppa));
            NVMEV_ASSERT(false);
        }

        while(i + 1 < nr_valid_grains && IDX(ppas[i + 1].lpa) == idx) {
            __update_pt(shard, &t_cmt, ppas[i + 1].lpa, ppas[i + 1].new_ppa);
            //offset = OFFSET(ppas[i + 1].lpa);
            //t_cmt.pt[offset].ppa = ppas[i + 1].new_ppa;

            if(G_IDX(ppas[i].new_ppa) > spp->tt_pgs) {
                printk("Tried to convert PPA %u\n", G_IDX(ppas[i].new_ppa));
                NVMEV_ASSERT(false);
            }

            NVMEV_INFO("%s 2 setting LPA %u to PPA %u\n",
                         __func__, ppas[i + 1].lpa, ppas[i + 1].new_ppa);

            i++;
        }

        if(g_off + t_cmt.len_on_disk > GRAIN_PER_PAGE) {
            if(g_off % GRAIN_PER_PAGE) {
                for(int i = g_off; i < GRAIN_PER_PAGE; i++) {
                    oob[ppa][i] = UINT_MAX;
                }

                mark_grain_invalid(shard, PPA_TO_PGA(ppa, g_off), 
                                   GRAIN_PER_PAGE - g_off);
            }

            p = pgs[ppa_idx++];
            ppa = ppa2pgidx(shard, &p);
            g_off = 0;
        }

        oob[ppa][g_off] = lpa;

        uint64_t off = shard_off + ((uint64_t) ppa * spp->pgsz) + 
                       (g_off * GRAINED_UNIT);
        uint8_t *ptr = nvmev_vdev->ns[0].mapped + off;

        NVMEV_INFO("%s writing CMT IDX %llu back to PPA %llu grain %u len %u off %llu\n",
                    __func__, idx, ppa, g_off, t_cmt.len_on_disk, off);
    
        struct copy_args *args = 
        (struct copy_args*) kzalloc(sizeof(*args), GFP_KERNEL);
        
        args->spp = spp;
        args->idx = idx;
        args->dst = ptr;
        args->g_len = t_cmt.len_on_disk;
        args->src = pts[cmts_loaded];
        args->rem = &rem;

        for(int i = 0; i < t_cmt.cached_cnt; i++) {
            if(IDX(t_cmt.pt[i].lpa) != t_cmt.idx) {
                NVMEV_INFO("Had LPA %u PPA %u IDX %u\n",
                            t_cmt.pt[i].lpa, t_cmt.pt[i].ppa, t_cmt.idx);
            }
            NVMEV_ASSERT(IDX(t_cmt.pt[i].lpa) == t_cmt.idx);
        }

        schedule_internal_operation_cb(INT_MAX, 0,
                                       NULL, 0, 0, 
                                       (void*) __copy_work, 
                                       (void*) args, 
                                       false, NULL);

        if (last_pg_in_wordline(shard, &p)) {
            struct nand_cmd swr = {
                .type = GC_MAP_IO,
                .cmd = NAND_WRITE,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            };

            swr.stime = reads_done; // __get_wallclock();
            swr.ppa = &p;

            ssd_advance_nand(shard->ssd, &swr);
            //schedule_internal_operation(req->sq_id, nsecs_completed, wbuf,
            //        spp->pgs_per_oneshotpg * spp->pgsz);
        }

        d_stat.trans_w_dgc++;

        struct cmt_struct *cmt = cache->member.cmt[idx];
        NVMEV_ASSERT(atomic_read(&cmt->outgoing) == 0);

        cmt->state = CLEAN;

        NVMEV_ASSERT(!cmt->lru_ptr);
        NVMEV_ASSERT(!cmt->pt);
        cmt->t_ppa = ppa;
        cmt->g_off = g_off;

        g_off += t_cmt.len_on_disk;
        cmts_loaded++;

        if(cmts_loaded % 4096 == 0) {
            __reclaim_completed_reqs();
        }
    }

    if(g_off % GRAIN_PER_PAGE) {
        for(int i = g_off; i < GRAIN_PER_PAGE; i++) {
            oob[ppa][i] = UINT_MAX;
        }

        mark_grain_invalid(shard, PPA_TO_PGA(ppa, g_off), 
                           GRAIN_PER_PAGE - g_off);
    }

    while(atomic_read(&rem) > 0) {
        cpu_relax();
    }

    kfree(pgs);
    kfree(pts); 
	kfree(skip_update);
	return 0;
//
//
//    struct ssd *ssd = shard->ssd;
//    struct ssdparams *spp = &ssd->sp;
//    struct demand_cache *cache = shard->cache;
//	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);
//    bool skip_all = true;
//    uint64_t **oob = shard->oob;
//    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
//    uint64_t nsecs_completed = 0, nsecs_latest = 0, reads_done = 0;
//
//    NVMEV_ASSERT(skip_update);
//
//    sort(ppas, nr_valid_grains, sizeof(struct lpa_len_ppa), &lpa_cmp, NULL);
//
//    uint64_t unique_cmts = 0;
//    uint64_t prev = UINT_MAX;
//    for(int i = 0; i < nr_valid_grains; i++) {
//        lpa_t lpa = ppas[i].lpa;
//        ppa_t idx = IDX(lpa);
//        if(idx != prev && ppas[i].lpa != UINT_MAX) {
//            unique_cmts++;
//            prev = idx;
//        }
//    }
//
//    read_ppa_cnt = 0;
//
//    NVMEV_DEBUG("There are %llu unique CMTs to update with %d pairs. Read %llu already.\n", 
//                 unique_cmts, nr_valid_grains, read_cmt_cnt);
//
//    if(unique_cmts == 0) {
//        kfree(skip_update);
//        return 0;
//    }
//
//    value_set **pts = (value_set**) kzalloc(sizeof(value_set*) * unique_cmts, GFP_KERNEL);
//    for(int i = 0; i < unique_cmts; i++) {
//        pts[i] = (value_set*) kzalloc(sizeof(value_set), GFP_KERNEL);
//        pts[i]->value = kzalloc(spp->pgsz, GFP_KERNEL);
//        NVMEV_ASSERT(pts[i]->value);
//        pts[i]->ssd = d_member.ssd;
//    }
//
//    uint64_t to_write = 0;
//    uint64_t cur;
//    uint64_t step = ENTRY_SIZE;
//    uint64_t cmts_loaded = 0;
//	/* read mapping table which needs update */
//    volatile int nr_update_tpages = 0;
//	for (int i = 0; i < nr_valid_grains; i++) {
//        lpa_t lpa = ppas[i].lpa;
//
//        if(lpa == UINT_MAX || lpa == UINT_MAX - 1) {
//            skip_update[i] = true;
//            continue;
//        }
//
//        NVMEV_DEBUG("%s updating mapping of LPA %u IDX %lu to PPA %u\n", 
//                __func__, lpa, IDX(ppas[i].lpa), ppas[i].new_ppa);
//
//        struct cache_stat *stat = get_cstat();
//		if (d_cache->is_hit(lpa)) {
//            NVMEV_DEBUG("%s It was cached.\n", __func__);
//			struct pt_struct pte = d_cache->get_pte(lpa);
//			pte.ppa = ppas[i].new_ppa;
//			d_cache->update(lpa, pte);
//			skip_update[i] = true;
//		} else {
//            struct cmt_struct *cmt = d_cache->get_cmt(lpa);
//            NVMEV_DEBUG("%s It wasn't cached PPA is %u.\n", __func__, cmt->t_ppa);
//            prev = __already_read(cmt->t_ppa);
//            if (cmt->t_ppa == UINT_MAX) {
//                continue;
//            } else if(prev != UINT_MAX) {
//                NVMEV_DEBUG("%s But the CMT had already been read here at %llu.\n", 
//                            __func__, prev);
//                value_set *already_read = read_ppas_v[prev];
//                __page_to_ptes(already_read, cmt->idx, false);
//                mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, cmt->grain), 
//                                   g_per_cg);
//                cmt->t_ppa = UINT_MAX;
//                continue;
//            }
//
//            skip_all = false;
//            NVMEV_ASSERT(cmt->pt == NULL);
//
//            value_set *_value_mr = pts[cmts_loaded++];
//            _value_mr->ssd = d_member.ssd;
//            __demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, NULL);
//
//            read_ppas_v[read_ppa_cnt] = _value_mr;
//            read_ppas[read_ppa_cnt++] = cmt->t_ppa;
//
//            NVMEV_DEBUG("Got CMT IDX %u\n", cmt->idx);
//            NVMEV_DEBUG("%s marking mapping PPA %u grain %llu invalid during GC read.\n",
//                    __func__, cmt->t_ppa, cmt->grain);
//            mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, cmt->grain), 
//                               cmt->len_on_disk);
//            cmt->t_ppa = UINT_MAX;
//            __page_to_ptes(_value_mr, cmt->idx, false);
//
//            cur = CACHE_GRAIN * ((cmt->cached_cnt / CACHE_GRAIN) + 1);
//            to_write += (cur * step) / GRAINED_UNIT;
//
//            d_stat.trans_r_dgc++;
//            nr_update_tpages++;
//        }
//    }
//
//    if(skip_all) {
//        for(int i = 0; i < unique_cmts; i++) {
//            kfree(pts[i]->value);
//            kfree(pts[i]);
//        }
//        kfree(pts); 
//
//        kfree(skip_update);
//        return 0;
//    }
//
//    NVMEV_DEBUG("Exiting loop.\n");
//
//    cmts_loaded = 0;
//    /* write */
//	for (int i = 0; i < nr_valid_grains; i++) {
//		if (skip_update[i]) {
//			continue;
//		}
//
//        uint64_t idx = IDX(ppas[i].lpa);
//        lpa_t lpa = ppas[i].lpa;
//        ppa_t ppa = ppas[i].new_ppa;
//
//        NVMEV_DEBUG("Updating CMT IDX %llu LPA %u PPA %u\n",
//                    idx, lpa, ppa);
//
//        struct cmt_struct *cmt = d_cache->member.cmt[idx];
//        BUG_ON(!cmt->pt);
//        __update_pt(cmt, lpa, ppa);
//    }
//
//    NVMEV_DEBUG("Exiting loop 2.\n");
//
//    struct ppa p = get_new_page(ftl, GC_MAP_IO);
//    ppa_t ppa = ppa2pgidx(ftl, &p);
//    uint64_t idx;
//    uint64_t offset;
//    uint64_t written;
//    uint64_t last_idx = UINT_MAX;
//    struct cmt_struct *cmt;
//
//    advance_write_pointer(ftl, GC_MAP_IO);
//    mark_page_valid(ftl, &p);
//    d_stat.trans_w_dgc++;
//
//    value_set *w = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
//    w->ssd = d_member.ssd;
//    memset(w->value, 0x0, spp->pgsz);
//
//    idx = 0;
//    offset = 0;
//    written = 0;
//
//    NVMEV_DEBUG("Before grains.\n");
//
//    while(written < nr_valid_grains) {
//        if (skip_update[written]) {
//            written++;
//            continue;
//        }
//
//        idx = IDX(ppas[written].lpa);
//        if(idx == last_idx) {
//            written++;
//            continue;
//        }
//
//        last_idx = idx;
//
//        cmt = d_cache->member.cmt[idx];
//        BUG_ON(!cmt->pt);
//
//        cur = (cmt->cached_cnt / CACHE_GRAIN) + 1;
//        uint64_t length = cur * g_per_cg;
//
//        if(length > GRAIN_PER_PAGE - offset) {
//            NVMEV_ASSERT(offset > 0);
//            mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), 
//                    GRAIN_PER_PAGE - offset);
//            mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), 
//                    GRAIN_PER_PAGE - offset);
//            uint64_t to = shard_off + ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
//            memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
//                    GRAINED_UNIT);
//            last_idx = UINT_MAX;
//            goto new_ppa;
//        }
//
//        cmt->state = CLEAN;
//        oob[ppa][offset] = IDX2LPA(cmt->idx);
//        NVMEV_ASSERT(oob[ppa][offset] < 33000000);
//
//        uint64_t cg = (cmt->cached_cnt / CACHE_GRAIN) + 1;
//        uint64_t cur = CACHE_GRAIN * cg;
//        for(int j = 1; j < (cur * step) / GRAINED_UNIT; j++) {
//            oob[ppa][offset + j] = UINT_MAX - 1; 
//        }
//
//        //BUG_ON(cur > CACHE_GRAIN);
//        mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), g_per_cg * cg);
//        cmt->grain = offset;
//
//        NVMEV_DEBUG("CMT IDX %llu gets grain %llu in GC\n", idx, cmt->grain);
//        NVMEV_DEBUG("Setting PPA %u offset %llu to LPA %lu in GC IDX %lld (%d).\n",
//                ppa, offset, IDX2LPA(cmt->idx), written, nr_valid_grains);
//
//        __pte_to_page(w, cmt, (GRAIN_PER_PAGE - offset) * GRAINED_UNIT);
//
//        lru_delete(d_cache->member.lru, cmt->lru_ptr);
//        cmt->lru_ptr = NULL;
//        cmt->cached_cnt = 0;
//        cmt->t_ppa = ppa;
//        kfree(cmt->pt);
//        cmt->pt = NULL;
//
//        offset += length;
//        written++;
//
//        if(offset == GRAIN_PER_PAGE && written <= nr_valid_grains) {
//new_ppa:
//            __demand.li->write(ppa, spp->pgsz, w, ASYNC, NULL);
//
//            if(offset < GRAIN_PER_PAGE) {
//                oob[ppa][offset] = UINT_MAX;
//            }
//
//            NVMEV_DEBUG("2 Re-wrote a mapping PPA to %u in GC. First few:\n", ppa);
//
//            memset(w->value, 0x0, spp->pgsz);
//            p = get_new_page(ftl, GC_MAP_IO);
//            offset = 0;
//            ppa = ppa2pgidx(ftl, &p);
//            NVMEV_ASSERT(oob_empty(ppa2pgidx(ftl, &p)));
//            mark_page_valid(ftl, &p);
//            advance_write_pointer(ftl, GC_MAP_IO);
//            d_stat.trans_w_dgc++;
//        }
//    }
//
//    if(offset < GRAIN_PER_PAGE) {
//        __demand.li->write(ppa, spp->pgsz, w, ASYNC, NULL);
//
//        BUG_ON(cmt->t_ppa == UINT_MAX);
//
//        oob[ppa][offset] = UINT_MAX;
//        NVMEV_DEBUG("1 PPA %u closed at grain %llu\n", ppa, offset);
//        mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), 
//                GRAIN_PER_PAGE - offset);
//        mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), 
//                GRAIN_PER_PAGE - offset);
//
//        uint64_t to = shard_off + ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
//        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
//                GRAINED_UNIT);
//
//        NVMEV_DEBUG("3 Re-wrote a mapping PPA to %u in GC.\n", ppa);
//        memset(w->value, 0x0, spp->pgsz);
//    }
//
//    for(int i = 0; i < unique_cmts; i++) {
//        kfree(pts[i]->value);
//        kfree(pts[i]);
//    }
//    kfree(pts); 
//
//	kfree(skip_update);
//	return 0;
//    return 0;
}
#endif
int dpage_gc_dvalue(blockmanager *bm) {
    NVMEV_ASSERT(false);
}

#endif
