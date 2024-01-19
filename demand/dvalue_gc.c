/*
 * Dynamic Value GC Implementation
 */

#ifdef DVALUE

#include "cache.h"
#include "demand.h"
#include "page.h"
#include "utility.h"
#include "cache.h"
#include "./interface/interface.h"

#include <linux/sort.h>

extern algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

extern struct demand_cache *d_cache;

extern __segment *d_active;
extern __segment *d_reserve;

struct gc_bucket_node {
	PTR ptr;
	int32_t lpa;
	int32_t ppa;
};

struct gc_bucket {
	struct gc_bucket_node bucket[PAGESIZE/GRAINED_UNIT+1][_PPS*GRAIN_PER_PAGE];
	uint32_t idx[PAGESIZE/GRAINED_UNIT+1];
};

extern struct algo_req *make_algo_req_default(uint8_t, value_set *);
static int 
_do_bulk_read_pages_containing_valid_grain
(blockmanager *bm, struct gc_table_struct **bulk_table, __gsegment *target_seg) {

	__block *target_blk = NULL;
	int ppa = 0;
	int blk_idx = 0;
	int page_idx = 0;

	int i = 0;
	for_each_page_in_seg_blocks(target_seg, target_blk, ppa, blk_idx, page_idx) {
		if (contains_valid_grain(bm, ppa)) {
            //printk("%s valid grain in ppa %d\n", __func__, ppa);
			value_set *origin = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
            origin->ssd = d_member.ssd;
			__demand.li->read(ppa, PAGESIZE, origin, ASYNC, make_algo_req_default(GCDR, origin));

			bulk_table[i] = (struct gc_table_struct *)kzalloc(sizeof(struct gc_table_struct), GFP_KERNEL);
            BUG_ON(!bulk_table[i]);
			bulk_table[i]->origin   = origin;
			//bulk_table[i]->lpa_list = (lpa_t *)bm->get_oob(bm, ppa);
			bulk_table[i]->ppa      = ppa;

			i++;
		}
	}
	int nr_read_pages = i;
    printk("%s read %d pages\n", __func__, nr_read_pages);
	return nr_read_pages;
}

static void _do_wait_until_read_all(int target_number) {
	while (d_member.nr_valid_read_done != target_number) {}
	d_member.nr_valid_read_done = 0;
}

struct gc_bucket *gc_bucket;
struct gc_bucket_node *gcb_node_arr[_PPS*GRAIN_PER_PAGE];
static int 
_do_bulk_write_valid_grains
(blockmanager *bm, struct gc_table_struct **bulk_table, int nr_read_pages, page_t type) {
	gc_bucket = (struct gc_bucket *)vmalloc(sizeof(struct gc_bucket));
	for (int i = 0; i < PAGESIZE/GRAINED_UNIT+1; i++) gc_bucket->idx[i] = 0;

	int nr_valid_grains = 0;
	for (int i = 0; i < nr_read_pages; i++) {
		for (int j = 0; j < GRAIN_PER_PAGE; j++) {
			pga_t pga = bulk_table[i]->ppa * GRAIN_PER_PAGE + j;
			if (is_valid_grain(pga)) {
                //printk("%s found a valid grain %u (ppa %u)\n", __func__, pga, bulk_table[i]->ppa);

				int len = 1;
				lpa_t *lpa_list = get_oob(bm, pga/GRAIN_PER_PAGE);

				while (j+len < GRAIN_PER_PAGE && lpa_list[j+len] == 0) { len++; }

				struct gc_bucket_node *gcb_node = &gc_bucket->bucket[len][gc_bucket->idx[len]];
				gcb_node->ptr = bulk_table[i]->origin->value + j*GRAINED_UNIT;
				gcb_node->lpa = lpa_list[j];
				if (unlikely(lpa_list[j] == 0)) {
                    printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);
                }

				gc_bucket->idx[len]++;

				gcb_node_arr[nr_valid_grains++] = gcb_node;

				invalidate_page(bm, pga, DATA);
			}
		}
		if (unlikely(contains_valid_grain(bm, bulk_table[i]->ppa))) {
            printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);;
        }
	}

    printk("%s read valid grains.\n", __func__);

	int ordering_done = 0, copied_pages = 0;
	while (ordering_done < nr_valid_grains) {
		value_set *_value_dgcw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
		PTR page = _value_dgcw->value;
		int remain = PAGESIZE;
		ppa_t ppa = bm->get_page_num(bm, d_reserve);
		int offset = 0;

		while (remain > 0) {
			int target_length = remain / GRAINED_UNIT;
			while (gc_bucket->idx[target_length] == 0 && target_length != 0) { target_length--; }
			if (target_length == 0) break;

			struct gc_bucket_node *gcb_node = &gc_bucket->bucket[target_length][gc_bucket->idx[target_length]-1];
			gc_bucket->idx[target_length]--;
			gcb_node->ppa = PPA_TO_PGA(ppa, offset);

			/** VALID COPY HERE **/
			memcpy(&page[offset*GRAINED_UNIT], gcb_node->ptr, target_length*GRAINED_UNIT);

			//set_oob(bm, gcb_node->lpa, ppa, type);
			set_oob(bm, gcb_node->lpa, gcb_node->ppa, type);
			validate_grain(bm, gcb_node->ppa);

			offset += target_length;
			remain -= target_length * GRAINED_UNIT;

			ordering_done++;
		}
        _value_dgcw->ssd = d_member.ssd;
		__demand.li->write(ppa, PAGESIZE, _value_dgcw, ASYNC, make_algo_req_default(GCDW, _value_dgcw));

		copied_pages++;
	}
	printk("[valid grains: %d -> packed pages: %d]\n", nr_valid_grains, copied_pages);

	return nr_valid_grains;
}

static int lpa_compare(const void *a, const void *b) {
	lpa_t a_lpa = (*(struct gc_bucket_node **)a)->lpa;
	lpa_t b_lpa = (*(struct gc_bucket_node **)b)->lpa;

	if (a_lpa < b_lpa) return -1;
	else if (a_lpa == b_lpa) return 0;
	else return 1;
}

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
uint64_t __pte_to_page(value_set *value, struct cmt_struct *cmt) {
    struct ssdparams *spp = &d_member.ssd->sp;
    struct pt_struct *pt = cmt->pt;
    uint64_t ret = 0;
    uint64_t step = ENTRY_SIZE;
    uint64_t first_lpa = cmt->idx * EPP;

    for(int i = 0; i < spp->pgsz / step; i++) {
        ppa_t ppa = pt[i].ppa;

        if(ppa != UINT_MAX) {
            NVMEV_DEBUG("LPA %u PPA %u IDX %u to %u in the memcpy ret %u.\n",
                    first_lpa + i, ppa, cmt->idx, i, ret);
        }

        memcpy(value->value + (i * step), &ppa, sizeof(ppa));
        ret += step;
    }

    NVMEV_ASSERT(ret <= spp->pgsz);
    return ret;
}

int do_bulk_mapping_update_v(struct lpa_len_ppa *ppas, int nr_valid_grains, 
                             uint64_t *read_cmts, uint64_t read_cmt_cnt) {
    struct ssdparams *spp = &d_member.ssd->sp;
	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);
    bool skip_all = true;

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

    NVMEV_DEBUG("There are %u unique CMTs to update with %u pairs. Read %u already.\n", 
                 unique_cmts, nr_valid_grains, read_cmt_cnt);

    if(unique_cmts == 0) {
        kfree(skip_update);
        return 0;
    }

    value_set **pts = (value_set**) kzalloc(sizeof(value_set*) * unique_cmts, GFP_KERNEL);
    for(int i = 0; i < unique_cmts; i++) {
        pts[i] = (value_set*) kzalloc(sizeof(value_set), GFP_KERNEL);
        pts[i]->value = kzalloc(spp->pgsz, GFP_KERNEL);
        pts[i]->ssd = d_member.ssd;
    }

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

		if (d_cache->is_hit(lpa)) {
            NVMEV_DEBUG("%s It was cached.\n", __func__);
			struct pt_struct pte = d_cache->get_pte(lpa);
			pte.ppa = ppas[i].new_ppa;
			d_cache->update(lpa, pte);
			skip_update[i] = true;
		} else {
            NVMEV_DEBUG("%s It wasn't cached.\n", __func__);
            struct cmt_struct *cmt = d_cache->get_cmt(lpa);
            if (cmt->t_ppa == UINT_MAX) {
                NVMEV_DEBUG("%s But the CMT had already been read here.\n", __func__);
                continue;
            }

            skip_all = false;

            if(__read_cmt(ppas[i].lpa, read_cmts, read_cmt_cnt)) {
                NVMEV_DEBUG("%s skipping read PPA %u as it was read earlier.\n",
                            __func__, cmt->t_ppa);

                uint64_t off = cmt->t_ppa * spp->pgsz;
                memcpy(pts[cmts_loaded++]->value, nvmev_vdev->ns[0].mapped + off, spp->pgsz);
            } else {
                value_set *_value_mr = pts[cmts_loaded++];
                _value_mr->ssd = d_member.ssd;
                __demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, NULL);

                NVMEV_DEBUG("%s marking mapping PPA %u invalid while it was being read during GC.\n",
                        __func__, cmt->t_ppa);
                mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, 0), GRAIN_PER_PAGE);
            }
            cmt->t_ppa = UINT_MAX;

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

    cmts_loaded = 0;
    /* write */
	for (int i = 0; i < nr_valid_grains; i++) {
		if (skip_update[i]) {
			continue;
		}

        uint64_t idx = IDX(ppas[i].lpa);
        lpa_t lpa = ppas[i].lpa;

        struct cmt_struct t_cmt;
        t_cmt.pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
        __page_to_pte(pts[cmts_loaded], t_cmt.pt, t_cmt.idx);

        uint64_t offset = OFFSET(ppas[i].lpa);
        t_cmt.pt[offset].ppa = ppas[i].new_ppa;

        NVMEV_DEBUG("%s 1 setting LPA %u to PPA %u\n",
                __func__, ppas[i].lpa, ppas[i].new_ppa);

        while(i + 1 < nr_valid_grains && IDX(ppas[i + 1].lpa) == idx) {
            offset = OFFSET(ppas[i + 1].lpa);
            t_cmt.pt[offset].ppa = ppas[i + 1].new_ppa;

            NVMEV_DEBUG("%s 2 setting LPA %u to PPA %u\n",
                         __func__, ppas[i + 1].lpa, ppas[i + 1].new_ppa);

            i++;
        }

        __pte_to_page(pts[cmts_loaded], &t_cmt);
        kfree(t_cmt.pt);
 
        struct ppa p = get_new_page(ftl, GC_MAP_IO);
        ppa_t ppa = ppa2pgidx(ftl, &p);

        advance_write_pointer(ftl, GC_MAP_IO);
        mark_page_valid(ftl, &p);
        mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        oob[ppa][0] = lpa;

        NVMEV_DEBUG("%s writing CMT IDX %u back to PPA %u\n",
                    __func__, idx, ppa);

        __demand.li->write(ppa, spp->pgsz, pts[cmts_loaded], ASYNC, NULL);

        d_stat.trans_w_dgc++;

        struct cmt_struct *cmt = d_cache->member.cmt[idx];
        cmt->state = CLEAN;
        lru_delete(d_cache->member.lru, cmt->lru_ptr);
        cmt->lru_ptr = NULL;        
        //kfree(cmt->pt);
        cmt->t_ppa = ppa;
        cmt->pt = NULL;
        cmts_loaded++;
    }

    for(int i = 0; i < unique_cmts; i++) {
        kfree(pts[i]->value);
        kfree(pts[i]);
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
            uint64_t to = ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
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

        uint64_t to = ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
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

static int _do_bulk_mapping_update(blockmanager *bm, int nr_valid_grains, page_t type) {
	sort(gcb_node_arr, nr_valid_grains, sizeof(struct gc_bucket_node *), lpa_compare, NULL);

	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);

	/* read mapping table which needs update */
	volatile int nr_update_tpages = 0;
	for (int i = 0; i < nr_valid_grains; i++) {
		struct gc_bucket_node *gcb_node = gcb_node_arr[i];
		lpa_t lpa = gcb_node->lpa;

		if (d_cache->is_hit(lpa)) {
			struct pt_struct pte = d_cache->get_pte(lpa);
			pte.ppa = gcb_node->ppa;
			d_cache->update(lpa, pte);

			skip_update[i] = true;
		} else {
            BUG_ON(true);
			struct cmt_struct *cmt = d_cache->get_cmt(lpa);
			if (cmt->t_ppa == UINT_MAX) {
				continue;
			}

			value_set *_value_mr = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
            _value_mr->ssd = d_member.ssd;
			__demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, make_algo_req_default(GCMR_DGC, _value_mr));

			invalidate_page(bm, cmt->t_ppa, MAP);
			cmt->t_ppa = UINT_MAX;
			
			nr_update_tpages++;
		}
	}

	/* wait */
	while (d_member.nr_tpages_read_done != nr_update_tpages) {}
	d_member.nr_tpages_read_done = 0;

	/* write */
	for (int i = 0; i < nr_valid_grains; i++) {
        BUG_ON(true);
		//if (skip_update[i]) {
		//	continue;
		//}

		//struct cmt_struct *cmt = d_cache->member.cmt[IDX(gcb_node_arr[i]->lpa)];
		//struct pt_struct *pt = d_cache->member.mem_table[cmt->idx];

		//pt[OFFSET(gcb_node_arr[i]->lpa)].ppa = gcb_node_arr[i]->ppa;
		//while (i+1 < nr_valid_grains && IDX(gcb_node_arr[i+1]->lpa) == cmt->idx) {
		//	pt[OFFSET(gcb_node_arr[i+1]->lpa)].ppa = gcb_node_arr[i+1]->ppa;
		//	i++;
		//}

		//cmt->t_ppa = get_tpage(bm);
		//value_set *_value_mw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
        //_value_mw->ssd = d_member.ssd;
		//__demand.li->write(cmt->t_ppa, PAGESIZE, _value_mw, ASYNC, make_algo_req_default(GCMW_DGC, _value_mw));

		//set_oob(bm, cmt->idx, cmt->t_ppa, MAP);

		//cmt->state = CLEAN;
	}

	kfree(skip_update);
	return 0;
}

int dpage_gc_dvalue(blockmanager *bm) {
	d_stat.dgc_cnt++;

	struct gc_table_struct **bulk_table = 
    (struct gc_table_struct **)vmalloc(_PPS * sizeof(struct gc_table_struct *));
    printk("bulk table %p\n", bulk_table);

    BUG_ON(!bulk_table);
    memset(bulk_table, 0x0, _PPS * sizeof(struct gc_table_struct *));

	__gsegment *target_seg = bm->pt_get_gc_target(bm, DATA_S);

	int nr_read_pages = _do_bulk_read_pages_containing_valid_grain(bm, bulk_table, target_seg);

	_do_wait_until_read_all(nr_read_pages);
    printk("%s finished waiting.\n", __func__);

	int nr_valid_grains = _do_bulk_write_valid_grains(bm, bulk_table, nr_read_pages, DATA);

	_do_bulk_mapping_update(bm, nr_valid_grains, DATA);

	/* trim blocks on the gsegemnt */
	bm->pt_trim_segment(bm, DATA_S, target_seg, __demand.li);

	/* reserve -> active / target_seg -> reserve */
	d_active  = d_reserve;
	d_reserve = bm->change_pt_reserve(bm, DATA_S, d_reserve);

	for (int i = 0; i < nr_read_pages; i++) {
		inf_free_valueset(bulk_table[i]->origin, FS_MALLOC_R);
		kfree(bulk_table[i]);
	}

	vfree(bulk_table);
	vfree(gc_bucket);

	return nr_read_pages;
}

#endif
