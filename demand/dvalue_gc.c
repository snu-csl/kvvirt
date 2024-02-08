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

#ifdef GC_STANDARD
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

struct copy_args {
    struct ssdparams *spp;
    uint32_t idx;
    uint8_t *src;
    uint8_t *dst;
    atomic_t *rem;
};

void __copy_work(void *voidargs, uint64_t*, uint64_t*) {
    struct copy_args *args = (struct copy_args*) voidargs;
    struct ssdparams *spp;
    uint8_t *src;
    uint8_t *dst;
    atomic_t *rem;

    spp = args->spp;
    dst = args->dst;
    src = args->src;
    rem = args->rem;

    memcpy(dst, src, spp->pgsz);

    NVMEV_DEBUG("%s IDX %u\n", __func__, args->idx);

    atomic_dec(rem);
    kfree(args);
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
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

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

        schedule_internal_operation_cb(INT_MAX, __get_wallclock(),
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

            swr.stime = 0 ; // __get_wallclock();
            swr.ppa = &p;

            nsecs_completed = ssd_advance_nand(shard->ssd, &swr);
            nsecs_latest = max(nsecs_latest, nsecs_completed);

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
uint64_t __pte_to_page(value_set *value, struct cmt_struct *cmt, 
                       uint64_t remaining) {
    struct ssdparams *spp = &d_member.ssd->sp;
    struct pt_struct *pt = cmt->pt;
    uint64_t ret = 0;
    uint64_t step = ENTRY_SIZE;
    uint64_t start = (spp->pgsz - remaining) / (ENTRY_SIZE);

    uint64_t cur = CACHE_GRAIN * ((cmt->cached_cnt / CACHE_GRAIN) + 1);
    //if(cur * step > remaining) {
    //    return UINT_MAX;
    //}

    for(int i = start; i < (start + cur); i++) {
        lpa_t lpa = pt[i - start].lpa;
        ppa_t ppa = pt[i - start].ppa;
#ifdef STORE_KEY_FP
        fp_t fp = pt[i - start].key_fp;
#endif

        if(lpa != UINT_MAX) {
            NVMEV_DEBUG("LPA %u PPA %u IDX %u to %u in the memcpy ret %llu.\n",
                    lpa, ppa, cmt->idx, i, ret);
        }

        memcpy(value->value + (i * step), &lpa, sizeof(lpa));
        memcpy(value->value + (i * step) + sizeof(lpa), &ppa, sizeof(ppa));
#ifdef STORE_KEY_FP
        memcpy(value->value + (i * step) + sizeof(lpa) + sizeof(ppa), 
               &fp, sizeof(fp));
#endif
        ret += step;
    }

    NVMEV_ASSERT(ret <= spp->pgsz);
    return ret;
}

value_set* read_ppas_v[EPP];
uint64_t read_ppas[EPP];
uint64_t read_ppa_cnt;
uint64_t __already_read(ppa_t ppa) {
    for(int i = 0; i < read_ppa_cnt; i++) {
        if(read_ppas[i] == ppa) {
            return i;
        }
    }
    return UINT_MAX;
}

void __update_pt(struct cmt_struct *pt, lpa_t lpa, ppa_t ppa) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t entry_sz = ENTRY_SIZE;

    NVMEV_DEBUG("Trying to update IDX %u LPA %u PPA %u\n",
                pt->idx, lpa, ppa);

    for(int i = 0; i < pt->cached_cnt; i++) {
        if(pt->pt[i].lpa == lpa) {
            pt->pt[i].ppa = ppa;
            return;
        }
    }

    NVMEV_ASSERT(false);
}

int do_bulk_mapping_update_v(struct lpa_len_ppa *ppas, int nr_valid_grains, 
                             uint64_t *read_cmts, uint64_t read_cmt_cnt) {
    struct ssdparams *spp = &d_member.ssd->sp;
	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);
    uint64_t g_per_cg = (CACHE_GRAIN * ENTRY_SIZE) / GRAINED_UNIT;
    bool skip_all = true;

    NVMEV_ASSERT(skip_update);

    sort(ppas, nr_valid_grains, sizeof(struct lpa_len_ppa), &lpa_cmp, NULL);

    uint64_t unique_cmts = 0;
    uint64_t prev = UINT_MAX;
    for(int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;
        ppa_t idx = IDX(lpa);
        if(idx != prev && ppas[i].lpa != UINT_MAX) {
            unique_cmts++;
            prev = idx;
        }
    }

    read_ppa_cnt = 0;

    NVMEV_DEBUG("There are %llu unique CMTs to update with %d pairs. Read %llu already.\n", 
                 unique_cmts, nr_valid_grains, read_cmt_cnt);

    if(unique_cmts == 0) {
        kfree(skip_update);
        return 0;
    }

    value_set **pts = (value_set**) kzalloc(sizeof(value_set*) * unique_cmts, GFP_KERNEL);
    for(int i = 0; i < unique_cmts; i++) {
        pts[i] = (value_set*) kzalloc(sizeof(value_set), GFP_KERNEL);
        pts[i]->value = kzalloc(spp->pgsz, GFP_KERNEL);
        NVMEV_ASSERT(pts[i]->value);
        pts[i]->ssd = d_member.ssd;
    }

    uint64_t to_write = 0;
    uint64_t cur;
    uint64_t step = ENTRY_SIZE;
    uint64_t cmts_loaded = 0;
	/* read mapping table which needs update */
    volatile int nr_update_tpages = 0;
	for (int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;

        if(lpa == UINT_MAX || lpa == UINT_MAX - 1) {
            skip_update[i] = true;
            continue;
        }

        NVMEV_DEBUG("%s updating mapping of LPA %u IDX %lu to PPA %u\n", 
                __func__, lpa, IDX(ppas[i].lpa), ppas[i].new_ppa);

        struct cache_stat *stat = get_cstat();
		if (d_cache->is_hit(lpa)) {
            NVMEV_DEBUG("%s It was cached.\n", __func__);
			struct pt_struct pte = d_cache->get_pte(lpa);
			pte.ppa = ppas[i].new_ppa;
			d_cache->update(lpa, pte);
			skip_update[i] = true;
		} else {
            struct cmt_struct *cmt = d_cache->get_cmt(lpa);
            NVMEV_DEBUG("%s It wasn't cached PPA is %u.\n", __func__, cmt->t_ppa);
            prev = __already_read(cmt->t_ppa);
            if (cmt->t_ppa == UINT_MAX) {
                continue;
            } else if(prev != UINT_MAX) {
                NVMEV_DEBUG("%s But the CMT had already been read here at %llu.\n", 
                            __func__, prev);
                value_set *already_read = read_ppas_v[prev];
                __page_to_ptes(already_read, cmt->idx, false);
                mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, cmt->grain), 
                                   g_per_cg);
                cmt->t_ppa = UINT_MAX;
                continue;
            }

            skip_all = false;
            NVMEV_ASSERT(cmt->pt == NULL);

            value_set *_value_mr = pts[cmts_loaded++];
            _value_mr->ssd = d_member.ssd;
            __demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, NULL);

            read_ppas_v[read_ppa_cnt] = _value_mr;
            read_ppas[read_ppa_cnt++] = cmt->t_ppa;

            NVMEV_DEBUG("Got CMT IDX %u\n", cmt->idx);
            NVMEV_DEBUG("%s marking mapping PPA %u grain %llu invalid during GC read.\n",
                    __func__, cmt->t_ppa, cmt->grain);
            mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, cmt->grain), 
                               cmt->len_on_disk);
            cmt->t_ppa = UINT_MAX;
            __page_to_ptes(_value_mr, cmt->idx, false);

            cur = CACHE_GRAIN * ((cmt->cached_cnt / CACHE_GRAIN) + 1);
            to_write += (cur * step) / GRAINED_UNIT;

            d_stat.trans_r_dgc++;
            nr_update_tpages++;
        }
    }

    if(skip_all) {
        for(int i = 0; i < unique_cmts; i++) {
            kfree(pts[i]->value);
            kfree(pts[i]);
        }
        kfree(pts); 

        kfree(skip_update);
        return 0;
    }

    NVMEV_DEBUG("Exiting loop.\n");

    cmts_loaded = 0;
    /* write */
	for (int i = 0; i < nr_valid_grains; i++) {
		if (skip_update[i]) {
			continue;
		}

        uint64_t idx = IDX(ppas[i].lpa);
        lpa_t lpa = ppas[i].lpa;
        ppa_t ppa = ppas[i].new_ppa;

        NVMEV_DEBUG("Updating CMT IDX %llu LPA %u PPA %u\n",
                    idx, lpa, ppa);

        struct cmt_struct *cmt = d_cache->member.cmt[idx];
        BUG_ON(!cmt->pt);
        __update_pt(cmt, lpa, ppa);
    }

    NVMEV_DEBUG("Exiting loop 2.\n");

    struct ppa p = get_new_page(ftl, GC_MAP_IO);
    ppa_t ppa = ppa2pgidx(ftl, &p);
    uint64_t idx;
    uint64_t offset;
    uint64_t written;
    uint64_t last_idx = UINT_MAX;
    struct cmt_struct *cmt;

    advance_write_pointer(ftl, GC_MAP_IO);
    mark_page_valid(ftl, &p);
    d_stat.trans_w_dgc++;

    value_set *w = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
    w->ssd = d_member.ssd;
    memset(w->value, 0x0, spp->pgsz);

    idx = 0;
    offset = 0;
    written = 0;

    NVMEV_DEBUG("Before grains.\n");

    while(written < nr_valid_grains) {
        if (skip_update[written]) {
            written++;
            continue;
        }

        idx = IDX(ppas[written].lpa);
        if(idx == last_idx) {
            written++;
            continue;
        }

        last_idx = idx;

        cmt = d_cache->member.cmt[idx];
        BUG_ON(!cmt->pt);

        cur = (cmt->cached_cnt / CACHE_GRAIN) + 1;
        uint64_t length = cur * g_per_cg;

        if(length > GRAIN_PER_PAGE - offset) {
            NVMEV_ASSERT(offset > 0);
            mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), 
                    GRAIN_PER_PAGE - offset);
            mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), 
                    GRAIN_PER_PAGE - offset);
            uint64_t to = shard_off + ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
            memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                    GRAINED_UNIT);
            last_idx = UINT_MAX;
            goto new_ppa;
        }

        cmt->state = CLEAN;
        oob[ppa][offset] = IDX2LPA(cmt->idx);
        NVMEV_ASSERT(oob[ppa][offset] < 33000000);

        uint64_t cg = (cmt->cached_cnt / CACHE_GRAIN) + 1;
        uint64_t cur = CACHE_GRAIN * cg;
        for(int j = 1; j < (cur * step) / GRAINED_UNIT; j++) {
            oob[ppa][offset + j] = UINT_MAX - 1; 
        }

        //BUG_ON(cur > CACHE_GRAIN);
        mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), g_per_cg * cg);
        cmt->grain = offset;

        NVMEV_DEBUG("CMT IDX %llu gets grain %llu in GC\n", idx, cmt->grain);
        NVMEV_DEBUG("Setting PPA %u offset %llu to LPA %lu in GC IDX %lld (%d).\n",
                ppa, offset, IDX2LPA(cmt->idx), written, nr_valid_grains);

        __pte_to_page(w, cmt, (GRAIN_PER_PAGE - offset) * GRAINED_UNIT);

        lru_delete(d_cache->member.lru, cmt->lru_ptr);
        cmt->lru_ptr = NULL;
        cmt->cached_cnt = 0;
        cmt->t_ppa = ppa;
        kfree(cmt->pt);
        cmt->pt = NULL;

        offset += length;
        written++;

        if(offset == GRAIN_PER_PAGE && written <= nr_valid_grains) {
new_ppa:
            __demand.li->write(ppa, spp->pgsz, w, ASYNC, NULL);

            if(offset < GRAIN_PER_PAGE) {
                oob[ppa][offset] = UINT_MAX;
            }

            NVMEV_DEBUG("2 Re-wrote a mapping PPA to %u in GC. First few:\n", ppa);

            memset(w->value, 0x0, spp->pgsz);
            p = get_new_page(ftl, GC_MAP_IO);
            offset = 0;
            ppa = ppa2pgidx(ftl, &p);
            NVMEV_ASSERT(oob_empty(ppa2pgidx(ftl, &p)));
            mark_page_valid(ftl, &p);
            advance_write_pointer(ftl, GC_MAP_IO);
            d_stat.trans_w_dgc++;
        }
    }

    if(offset < GRAIN_PER_PAGE) {
        __demand.li->write(ppa, spp->pgsz, w, ASYNC, NULL);

        BUG_ON(cmt->t_ppa == UINT_MAX);

        oob[ppa][offset] = UINT_MAX;
        NVMEV_DEBUG("1 PPA %u closed at grain %llu\n", ppa, offset);
        mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), 
                GRAIN_PER_PAGE - offset);
        mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), 
                GRAIN_PER_PAGE - offset);

        uint64_t to = shard_off + ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
        memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                GRAINED_UNIT);

        NVMEV_DEBUG("3 Re-wrote a mapping PPA to %u in GC.\n", ppa);
        memset(w->value, 0x0, spp->pgsz);
    }

    for(int i = 0; i < unique_cmts; i++) {
        kfree(pts[i]->value);
        kfree(pts[i]);
    }
    kfree(pts); 

	kfree(skip_update);
	return 0;
}
#endif
int dpage_gc_dvalue(blockmanager *bm) {
    NVMEV_ASSERT(false);
}

#endif
