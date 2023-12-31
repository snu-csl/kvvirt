/*
 * Coarse-grained Cache (default)
 */

#include "cache.h"
#include "coarse.h"
#include "utility.h"
#include "page.h"
#include "./interface/interface.h"

#include <linux/sched/clock.h>

extern struct algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

struct demand_cache cg_cache = {
	.create = cg_create,
	.destroy = cg_destroy,
	.load = cg_load,
	.list_up = cg_list_up,
	.wait_if_flying = cg_wait_if_flying,
	.touch = cg_touch,
	.update = cg_update,
	.get_pte = cg_get_pte,
	.get_cmt = cg_get_cmt,
/*	.get_ppa = cg_get_ppa,
#ifdef STORE_KEY_FP
	.get_fp = cg_get_fp,
#endif */
	.is_hit = cg_is_hit,
	.is_full = cg_is_full,
};

static struct cache_env *cenv;
static struct cache_member *cmbr;
static struct cache_stat *cstat;

static void print_cache_env(struct cache_env *const _env) {
    struct ssdparams spp = d_member.ssd->sp;

	printk("\n");
	printk(" |---------- Demand Cache Log: Coarse-grained Cache\n");
	printk(" | Total trans pages:        %d\n", _env->nr_valid_tpages);
	//printk(" | Caching Ratio:            %0.3f%%\n", _env->caching_ratio * 100);
	printk(" | Caching Ratio:            same as PFTL\n");
	printk(" |  - Max cached tpages:     %d (%lu pairs)\n", 
          _env->max_cached_tpages, _env->max_cached_tpages * EPP);
	//printk(" |  (PageFTL cached tpages:  %d)\n", _env->nr_tpages_optimal_caching);
	printk(" |---------- Demand Cache Log END\n");
	printk("\n");
}

static void cg_env_init(cache_t c_type, struct cache_env *const _env) {
    struct ssdparams spp = d_member.ssd->sp;

	_env->c_type = c_type;

	_env->nr_tpages_optimal_caching = d_env.nr_pages * 4 / spp.pgsz;
	_env->nr_valid_tpages = (d_env.nr_pages / EPP) + ((d_env.nr_pages % EPP) ? 1 : 0);
	_env->nr_valid_tentries = _env->nr_valid_tpages * EPP;

	//_env->caching_ratio = d_env.caching_ratio;
	//_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio;
    
    printk("Size is %llu %llu\n", d_env.size, (d_env.size) / K);

    _env->max_cached_tpages = 4096; //(d_env.size / K) / spp.pgsz;
	_env->max_cached_tentries = 0; // not used here

#ifdef DVALUE
	_env->nr_valid_tpages *= GRAIN_PER_PAGE / 2;
	_env->nr_valid_tentries *= GRAIN_PER_PAGE / 2;
#endif

	print_cache_env(_env);
}

static void cg_member_init(struct cache_member *const _member) {
	struct cmt_struct **cmt = 
    (struct cmt_struct **)vmalloc(cenv->nr_valid_tpages * sizeof(struct cmt_struct *));
	for (int i = 0; i < cenv->nr_valid_tpages; i++) {
		cmt[i] = (struct cmt_struct *)kzalloc(sizeof(struct cmt_struct), GFP_KERNEL);

        //struct ppa p = get_new_page(ftl, USER_IO);
        //uint64_t ppa = ppa2pgidx(ftl, &p);

        //advance_write_pointer(ftl, USER_IO);
        //mark_page_valid(ftl, &p);
        //mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        //oob[ppa][0] = U64_MAX - 1;
        //oob[ppa][1] = i * EPP;

		cmt[i]->t_ppa = U64_MAX;
		cmt[i]->idx = i;
		cmt[i]->pt = NULL;
		cmt[i]->lru_ptr = NULL;
		cmt[i]->state = CLEAN;
		cmt[i]->is_flying = false;

		q_init(&cmt[i]->retry_q, d_env.wb_flush_size);
		q_init(&cmt[i]->wait_q, d_env.wb_flush_size);

		cmt[i]->dirty_cnt = 0;
    }
    _member->cmt = cmt;

    lru_init(&_member->lru);

	_member->nr_cached_tpages = 0;
}

static void cg_stat_init(struct cache_stat *const _stat) {

}

int cg_create(cache_t c_type, struct demand_cache *dc) {
	cenv = &dc->env;
	cmbr = &dc->member;
	cstat = &dc->stat;

	cg_env_init(c_type, &dc->env);
	cg_member_init(&dc->member);
	cg_stat_init(&dc->stat);

	return 0;
}

static void cg_print_member(void) {
	//printk("=====================");
	//printk(" Cache Finish Status ");
	//printk("=====================");

	//printk("Max Cached tpages:     %d\n", cenv->max_cached_tpages);
	//printk("Current Cached tpages: %d\n", cmbr->nr_cached_tpages);
	//printk("\n");
}

static void cg_member_kfree(struct cache_member *_member) {
	for (int i = 0; i < cenv->nr_valid_tpages; i++) {
		q_free(_member->cmt[i]->retry_q);
		q_free(_member->cmt[i]->wait_q);
		kfree(_member->cmt[i]);
	}
	vfree(_member->cmt);

	//for (int i = 0; i < cenv->nr_valid_tpages; i++) {
	//	kfree(_member->mem_table[i]);
	//}
    //vfree(_member->mem_table);

	lru_kfree(_member->lru);
}

int cg_destroy(void) {
	print_cache_stat(cstat);

	cg_print_member();

	cg_member_kfree(cmbr);
	return 0;
}

int cg_load(lpa_t lpa, request *const req, snode *wb_entry, uint64_t *nsecs_completed) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct inflight_params *i_params;
    struct ssdparams spp = d_member.ssd->sp;
    uint64_t nsec = 0;

	if (IS_INITIAL_PPA(cmt->t_ppa)) {
        NVMEV_ERROR("Tried to load an unmapped PPA in %s.\n", __func__);
		return 0;
	}

	i_params = get_iparams(req, wb_entry);
	i_params->jump = GOTO_LIST;

	value_set *_value_mr = inf_get_valueset(NULL, FS_MALLOC_R, spp.pgsz);

    if(req) {
        NVMEV_ASSERT(!wb_entry);
        req->mapping_v = _value_mr;
    } else {
        NVMEV_ASSERT(!req);
        wb_entry->mapping_v = _value_mr;
    }

    NVMEV_ERROR("Reading a mapping PPA %llu in %s.\n", cmt->t_ppa, __func__);

    _value_mr->ssd = d_member.ssd;
	nsec = __demand.li->read(cmt->t_ppa, spp.pgsz, _value_mr, ASYNC, 
                                         make_algo_req_rw(MAPPINGR, _value_mr, 
                                         req, wb_entry));
    NVMEV_ERROR("Read returned.\n");

    if(nsecs_completed) {
        *nsecs_completed = nsec;
    }

	cmt->is_flying = true;
	return 1;
}

void __page_to_pte(value_set *value, struct pt_struct *pt) {
    struct ssdparams *spp = &d_member.ssd->sp;

    for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
        uint64_t ppa = *(uint64_t*) (value->value + (i * ENTRY_SIZE));
        pt[i].ppa = ppa;
#ifdef STORE_KEY_FP
        BUG_ON(true);
#endif

        if(ppa != U64_MAX) {
            //NVMEV_ERROR("IDX %u PPA %llu in %s.\n", i, ppa, __func__);
        }
    }
}

void __pte_to_page(value_set *value, struct pt_struct *pt) {
    struct ssdparams *spp = &d_member.ssd->sp;

    for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
        uint64_t ppa = pt[i].ppa;
#ifdef STORE_KEY_FP
        BUG_ON(true);
#endif

        memcpy(value->value + (i * ENTRY_SIZE), &ppa, sizeof(ppa));
        if(ppa != U64_MAX) {
            //NVMEV_ERROR("IDX %u PPA %llu in %s.\n", i, ppa, __func__);
        }
    }
}

void __reset_pt(struct pt_struct *pt) {
    struct ssdparams *spp = &d_member.ssd->sp;

    for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
        pt[i].ppa = U64_MAX;
#ifdef STORE_KEY_FP
        BUG_ON(true);
#endif
    }
}

int cg_list_up(lpa_t lpa, request *const req, snode *wb_entry, 
               uint64_t *nsecs_completed, uint64_t *credits) {
    int rc = 0;
	blockmanager *bm = __demand.bm;
    uint64_t nsecs_latest = 0, nsecs = 0;

	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct cmt_struct *victim = NULL;

    NVMEV_ERROR("Got CMT IDX %llu.\n", IDX(lpa));

	struct inflight_params *i_params;

	if (cg_is_full()) {
        NVMEV_DEBUG("%s lpa %llu translation cache full.\n", __func__, lpa);
		victim = (struct cmt_struct *)lru_pop(cmbr->lru);
		cmbr->nr_cached_tpages--;

        NVMEV_ASSERT(victim->idx != IDX(lpa));

		if (victim->state == DIRTY) {
			cstat->dirty_evict++;

            if(victim->t_ppa != U64_MAX) {
                //NVMEV_ERROR("Marking previous PPA %llu for IDX %llu invalid.\n",
                //        victim->t_ppa, victim->idx);
                //mark_grain_invalid(ftl, PPA_TO_PGA(victim->t_ppa, 0), GRAIN_PER_PAGE);
            }

			i_params = get_iparams(req, wb_entry);
			i_params->jump = GOTO_COMPLETE;
			//i_params->pte = cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];

            struct ppa p = get_new_page(ftl, MAP_IO);
            uint64_t ppa = ppa2pgidx(ftl, &p);

            advance_write_pointer(ftl, MAP_IO);
            mark_page_valid(ftl, &p);
            mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

            /*
             * U64_MAX - 1 for a mapping page, as opposed to U64_MAX which
             * is a page of invalid KV pair mappings.
             *
             * The IDX is used during GC so that we know which CMT entry
             * to update.
             */

            oob[ppa][0] = U64_MAX - 1;
            oob[ppa][1] = victim->idx * EPP;

			victim->t_ppa = ppa;
			victim->state = CLEAN;

            NVMEV_ERROR("Assigned PPA %llu to victim at IDX %u in %s.\n", 
                        ppa, victim->idx, __func__);

            //printk("%s dirty page %u.\n", __func__, victim->t_ppa);

			//struct pt_struct pte = cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];

			value_set *_value_mw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
            _value_mw->ssd = d_member.ssd;

            __pte_to_page(_value_mw, victim->pt);
			nsecs = __demand.li->write(victim->t_ppa, PAGESIZE, _value_mw, ASYNC, 
                                       make_algo_req_rw(MAPPINGW, _value_mw, 
                                       req, wb_entry));
			//set_oob(bm, victim->idx, victim->t_ppa, MAP);

			rc = 1;
            (*credits) += GRAIN_PER_PAGE;

            NVMEV_ERROR("Evicted DIRTY mapping entry IDX %u in %s.\n", 
                         victim->idx, __func__);
		} else {
            NVMEV_ERROR("Evicted CLEAN mapping entry IDX %u in %s.\n", 
                         victim->idx, __func__);
			cstat->clean_evict++;
		}

        victim->lru_ptr = NULL;
        //__reset_pt(victim->pt);
        kfree(victim->pt);
        victim->pt = NULL;
	}

    nsecs_latest = max(nsecs_latest, nsecs);

	//cmt->pt = cmbr->mem_table[IDX(lpa)];
    NVMEV_ERROR("Building mapping PPA %llu for LPA %llu\n", cmt->t_ppa, lpa);
	cmt->lru_ptr = lru_push(cmbr->lru, (void *)cmt);
	cmbr->nr_cached_tpages++;

	if (cmt->is_flying) {
        NVMEV_ERROR("Passed flying check in %s.\n", __func__);
		cmt->is_flying = false;

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

		if (req) {
            NVMEV_ERROR("Entered req branch in %s.\n", __func__);

            NVMEV_ASSERT(req->mapping_v);
            __page_to_pte(req->mapping_v, cmt->pt);
            kfree(req->mapping_v->value);
            kfree(req->mapping_v);
            req->mapping_v = NULL;

			request *retry_req;
			while ((retry_req = (request *)q_dequeue(cmt->retry_q))) {
				//lpa_t retry_lpa = get_lpa(retry_req->key, retry_req->hash_params);

				struct inflight_params *i_params = get_iparams(retry_req, NULL);
				i_params->jump = GOTO_COMPLETE;
                NVMEV_ERROR("Set i_params to GOTO_COMPLETE in %s.\n", __func__);
				//i_params->pte = cmt->pt[OFFSET(retry_lpa)];

                //printk("Should have called inf_assign_try in cg_list_up!\n");
				//inf_assign_try(retry_req);
			}
		} else if (wb_entry) {
            NVMEV_ASSERT(wb_entry->mapping_v);

            __page_to_pte(wb_entry->mapping_v, cmt->pt);
            kfree(wb_entry->mapping_v->value);
            kfree(wb_entry->mapping_v);
            wb_entry->mapping_v = NULL;

			snode *retry_wbe;
			while ((retry_wbe = (snode *)q_dequeue(cmt->retry_q))) {
				//lpa_t retry_lpa = get_lpa(retry_wbe->key, retry_wbe->hash_params);

				struct inflight_params *i_params = get_iparams(NULL, retry_wbe);
				i_params->jump = GOTO_COMPLETE;
				//i_params->pte = cmt->pt[OFFSET(retry_lpa)];

				q_enqueue((void *)retry_wbe, d_member.wb_retry_q);
			}
		}
	}

    if(nsecs_completed) {
        *nsecs_completed = nsecs_latest;
    }

	return rc;
}

int cg_wait_if_flying(lpa_t lpa, request *const req, snode *wb_entry) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("%s lpa %u\n", __func__, lpa);

	if (cmt->is_flying) {
		cstat->blocked_miss++;

		if (req) q_enqueue((void *)req, cmt->retry_q);
		else if (wb_entry) q_enqueue((void *)wb_entry, cmt->retry_q);
		else //printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);;

        //printk("%s returning 1\n", __func__);   
		return 1;
	}
    //printk("%s returning 0\n", __func__);   
	return 0;
}

int cg_touch(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	lru_update(cmbr->lru, cmt->lru_ptr);
	return 0;
}

int cg_update(lpa_t lpa, struct pt_struct pte) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("cg_update pte ppa %u for lpa %u\n", pte.ppa, lpa);

	if (cmt->pt) {
		cmt->pt[OFFSET(lpa)] = pte;

		if (!IS_INITIAL_PPA(cmt->t_ppa) && cmt->state == CLEAN) {
			//invalidate_page(__demand.bm, cmt->t_ppa, MAP);
            /*
             * Only safe if assuming battery-backed DRAM.
             */

            NVMEV_ERROR("Marking mapping PPA %llu invalid as it was dirtied in memory.\n",
                        cmt->t_ppa);
            mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, 0), GRAIN_PER_PAGE);
		}

		cmt->state = DIRTY;
		lru_update(cmbr->lru, cmt->lru_ptr);
	} else {
        BUG_ON(true);
		/* FIXME: to handle later update after evict */
		//cmbr->mem_table[IDX(lpa)][OFFSET(lpa)] = pte;

		//static int cnt = 0;
		//if (++cnt % 10240 == 0) //printk("cg_update %d\n", cnt);
		////printk("cg_update %d\n", ++cnt);
	}
	return 0;
}

bool cg_is_hit(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	if (cmt->pt != NULL) {
		cstat->cache_hit++;
		return 1;
	} else {
		cstat->cache_miss++;
		return 0;
	}
}

bool cg_is_full(void) {
	return (cmbr->nr_cached_tpages >= cenv->max_cached_tpages);
}

struct pt_struct cg_get_pte(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	if (cmt->pt) {
        //printk("%s returning %u for lpa %u\n", __func__, cmt->pt[OFFSET(lpa)].ppa, lpa);
		return cmt->pt[OFFSET(lpa)];
	} else {
        if(cmt->t_ppa == U64_MAX) {
            /*
             * Haven't used this CMT entry yet.
             */

            cmt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
            for(int i = 0; i < EPP; i++) {
                cmt->pt[i].ppa = U64_MAX;
#ifdef STORE_KEY_FP
                BUG_ON(true);
#endif
            }

            return cmt->pt[OFFSET(lpa)];
        } else {
            BUG_ON(true);
        }
		/* FIXME: to handle later update after evict */
		//return cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];
	}
/*	if (unlikely(cmt->pt == NULL)) {
		//printk("Should have aborted!!!! %s:%d
, __FILE__, __LINE__);;
	}
	return cmt->pt[OFFSET(lpa)]; */
}

struct cmt_struct *cg_get_cmt(lpa_t lpa) {
	return cmbr->cmt[IDX(lpa)];
}
