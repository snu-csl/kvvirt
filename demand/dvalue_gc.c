/*
 * Dynamic Value GC Implementation
 */

#ifdef DVALUE

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

bool __read_cmt(uint64_t lpa, uint64_t *read_cmts, uint64_t cnt) {
    uint64_t idx = IDX(lpa);

    for(int i = 0; i < cnt; i++) {
        NVMEV_ERROR("Checking %llu against %llu\n", idx, read_cmts[i]);
        if(read_cmts[i] == idx) {
            return true;
        }
    }

    return false;
}

int do_bulk_mapping_update_v(struct lpa_len_ppa *ppas, int nr_valid_grains, 
                             uint64_t *read_cmts, uint64_t read_cmt_cnt) {
    struct ssdparams *spp = &d_member.ssd->sp;
	bool *skip_update = (bool *)kzalloc(nr_valid_grains * sizeof(bool), GFP_KERNEL);

    sort(ppas, nr_valid_grains, sizeof(struct lpa_len_ppa), &lpa_cmp, NULL);

    uint64_t unique_cmts = 0;
    uint64_t prev = U64_MAX;
    for(int i = 0; i < nr_valid_grains; i++) {
        uint64_t lpa = ppas[i].lpa;
        //uint64_t ppa = ppas[i].new_ppa;
        uint64_t idx = IDX(lpa);
        //struct pt_struct p;

        //if(lpa == U64_MAX || lpa == U64_MAX - 1) {
        //    continue;
        //}

        //p.ppa = ppa;

        //NVMEV_ERROR("*** LPA %llu rewrite to PPA %llu IDX %llu ***\n", lpa, ppa, idx);
        //if (d_cache->is_hit(lpa)) {
        //    NVMEV_ERROR("*** LPA %llu rewrite to PPA %llu IDX %llu HIT ***\n", lpa, ppa, idx);
        //    d_cache->update(lpa, p);
        //} else {
        //    NVMEV_ERROR("*** LPA %llu rewrite to PPA %llu IDX %llu MISS ***\n", lpa, ppa, idx);

        //    struct cmt_struct *cmt = d_cache->get_cmt(lpa);
        //    cmt->state = DIRTY;

        //    if(cmt->t_ppa != U64_MAX) {
        //        NVMEV_ERROR("%s marking mapping PPA %llu invalid while it was being read during GC.\n",
        //                    __func__, cmt->t_ppa);
        //        mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, 0), GRAIN_PER_PAGE);
        //        //cmt->t_ppa = U64_MAX;
        //    }

        //    struct request *r = (struct request*) kzalloc(sizeof(*r), GFP_KERNEL);;
        //    r->value = (value_set*) kzalloc(sizeof(value_set), GFP_KERNEL);
        //    r->value->value = kzalloc(spp->pgsz, GFP_KERNEL);

        //    NVMEV_ASSERT(r);
        //    NVMEV_ASSERT(r->value);
        //    NVMEV_ASSERT(r->value->value);

        //    d_cache->load(lpa, r, NULL, NULL);
        //    d_cache->list_up(lpa, r, NULL, NULL);
        //    d_cache->update(lpa, p);

        //    kfree(r->params);
        //    kfree(r->value->value);
        //    kfree(r->value);
        //    kfree(r);
        //}

        if(idx != prev && ppas[i].lpa != U64_MAX) {
            unique_cmts++;
            prev = idx;
        }
    }

    //kfree(skip_update);
    //return 0;

    NVMEV_ERROR("There are %llu unique CMTs to update with %u pairs. Read %llu already.\n", 
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

        if(lpa == U64_MAX || lpa == U64_MAX - 1) {
            skip_update[i] = true;
            continue;
        }

        NVMEV_ERROR("%s updating mapping of LPA %llu IDX %llu to PPA %llu\n", 
                __func__, lpa, IDX(ppas[i].lpa), ppas[i].new_ppa);

		if (d_cache->is_hit(lpa)) {
            NVMEV_ERROR("%s It was cached.\n", __func__);
			struct pt_struct pte = d_cache->get_pte(lpa);
			pte.ppa = ppas[i].new_ppa;
			d_cache->update(lpa, pte);
			skip_update[i] = true;
		} else {
            NVMEV_ERROR("%s It wasn't cached.\n", __func__);
            struct cmt_struct *cmt = d_cache->get_cmt(lpa);
            if (cmt->t_ppa == U64_MAX) {
                NVMEV_ERROR("%s But the CMT had already been read here.\n", __func__);
                continue;
            }

            if(__read_cmt(ppas[i].lpa, read_cmts, read_cmt_cnt)) {
                NVMEV_ERROR("%s skipping read PPA %llu as it was read earlier.\n",
                            __func__, cmt->t_ppa);

                uint64_t off = cmt->t_ppa * spp->pgsz;
                memcpy(pts[cmts_loaded++]->value, nvmev_vdev->ns[0].mapped + off, spp->pgsz);
            } else {
                value_set *_value_mr = pts[cmts_loaded++];
                _value_mr->ssd = d_member.ssd;
                __demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, NULL);

                NVMEV_ERROR("%s marking mapping PPA %llu invalid while it was being read during GC.\n",
                        __func__, cmt->t_ppa);
                mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, 0), GRAIN_PER_PAGE);
            }
            //invalidate_page(bm, cmt->t_ppa, MAP);
            cmt->t_ppa = U64_MAX;

            nr_update_tpages++;
        }
    }

    cmts_loaded = 0;
    /* write */
	for (int i = 0; i < nr_valid_grains; i++) {
		if (skip_update[i]) {
			continue;
		}

        uint64_t idx = IDX(ppas[i].lpa);
        uint64_t lpa = ppas[i].lpa;

        struct cmt_struct *cmt = d_cache->member.cmt[idx];

        BUG_ON(cmt->pt);

        if(!cmt->pt) {
            cmt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
            NVMEV_ASSERT(cmt->pt);

            for(int i = 0; i < EPP; i++) {
                cmt->pt[i].ppa = U64_MAX;
#ifdef STORE_KEY_FP
                BUG_ON(true);
#endif
            }
        }

        __page_to_pte(pts[cmts_loaded], cmt->pt);

        uint64_t offset = OFFSET(ppas[i].lpa);
        cmt->pt[offset].ppa = ppas[i].new_ppa;

        NVMEV_DEBUG("%s 1 setting LPA %llu to PPA %llu\n",
                __func__, ppas[i].lpa, ppas[i].new_ppa);

        while(i + 1 < nr_valid_grains && IDX(ppas[i + 1].lpa) == idx) {
            offset = OFFSET(ppas[i + 1].lpa);
            cmt->pt[offset].ppa = ppas[i + 1].new_ppa;

            NVMEV_DEBUG("%s 2 setting LPA %llu to PPA %llu\n",
                         __func__, ppas[i + 1].lpa, ppas[i + 1].new_ppa);

#ifdef STORE_KEY_FP
            BUG_ON(true);
#endif
            i++;
        }

        __pte_to_page(pts[cmts_loaded], cmt->pt);
 
        struct ppa p = get_new_page(ftl, MAP_IO);
        uint64_t ppa = ppa2pgidx(ftl, &p);

        advance_write_pointer(ftl, MAP_IO);
        mark_page_valid(ftl, &p);
        mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        oob[ppa][0] = U64_MAX - 1;
        oob[ppa][1] = lpa;

        NVMEV_DEBUG("%s writing CMT IDX %llu back to PPA %llu\n",
                    __func__, idx, ppa);

        __demand.li->write(ppa, spp->pgsz, pts[cmts_loaded], ASYNC, NULL);

        cmt->state = CLEAN;
        
        kfree(cmt->pt);
        cmt->t_ppa = ppa;
        cmt->pt = NULL;
        cmts_loaded++;
    }

	kfree(skip_update);
	return 0;
}

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
			if (cmt->t_ppa == U64_MAX) {
				continue;
			}

			value_set *_value_mr = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
            _value_mr->ssd = d_member.ssd;
			__demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, make_algo_req_default(GCMR_DGC, _value_mr));

			invalidate_page(bm, cmt->t_ppa, MAP);
			cmt->t_ppa = U64_MAX;
			
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
