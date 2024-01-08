/*
 * Coarse-grained Cache (default)
 */

#include "cache.h"
#include "coarse_old.h"
#include "utility.h"
#include "page.h"
#include "./interface/interface.h"

#include <linux/sched/clock.h>

extern struct algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

struct demand_cache cgo_cache = {
    .create = cgo_create,
    .destroy = cgo_destroy,
    .load = cgo_load,
    .list_up = cgo_list_up,
    .wait_if_flying = cgo_wait_if_flying,
    .touch = cgo_touch,
    .update = cgo_update,
    .get_pte = cgo_get_pte,
    .get_cmt = cgo_get_cmt,
    /*      .get_ppa = cgo_get_ppa,
#ifdef STORE_KEY_FP
.get_fp = cgo_get_fp,
#endif */
    .is_hit = cgo_is_hit,
    .is_full = cgo_is_full,
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

static void cgo_env_init(cache_t c_type, struct cache_env *const _env) {
    struct ssdparams spp = d_member.ssd->sp;

    _env->c_type = c_type;

    _env->nr_tpages_optimal_caching = d_env.nr_pages * 4 / spp.pgsz;
    _env->nr_valid_tpages = (d_env.nr_pages / EPP) + ((d_env.nr_pages % EPP) ? 1 : 0);
    _env->nr_valid_tentries = _env->nr_valid_tpages * EPP;

    //_env->caching_ratio = d_env.caching_ratio;
    //_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio;

    printk("Size is %llu %llu\n", d_env.size, (d_env.size) / K);

    uint64_t capa = spp.tt_pgs * spp.pgsz;
    uint64_t dram = (uint64_t)((capa * 100) / 100000);
    printk("DRAM is %lluMB\n", dram >> 20);

    _env->max_cached_tpages = dram / spp.pgsz;
    _env->max_cached_tentries = 0;

#ifdef DVALUE
    _env->nr_valid_tpages *= GRAIN_PER_PAGE / 2;
    _env->nr_valid_tentries *= GRAIN_PER_PAGE / 2;
#endif

    print_cache_env(_env);
}

static void cgo_member_init(struct cache_member *const _member) {
    struct cmt_struct **cmt =
        (struct cmt_struct **)vmalloc(cenv->nr_valid_tpages * sizeof(struct cmt_struct *));
    for (int i = 0; i < cenv->nr_valid_tpages; i++) {
        cmt[i] = (struct cmt_struct *)kzalloc(sizeof(struct cmt_struct), GFP_KERNEL);

        cmt[i]->t_ppa = UINT_MAX;
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

static void cgo_stat_init(struct cache_stat *const _stat) {

}

int cgo_create(cache_t c_type, struct demand_cache *dc) {
    cenv = &dc->env;
    cmbr = &dc->member;
    cstat = &dc->stat;

    cgo_env_init(c_type, &dc->env);
    cgo_member_init(&dc->member);
    cgo_stat_init(&dc->stat);

    return 0;
}

static void cgo_print_member(void) {
    //printk("=====================");
    //printk(" Cache Finish Status ");
    //printk("=====================");

    //printk("Max Cached tpages:     %d\n", cenv->max_cached_tpages);
    //printk("Current Cached tpages: %d\n", cmbr->nr_cached_tpages);
    //printk("\n");
}

static void cgo_member_kfree(struct cache_member *_member) {
    for (int i = 0; i < cenv->nr_valid_tpages; i++) {
        q_free(_member->cmt[i]->retry_q);
        q_free(_member->cmt[i]->wait_q);
        kfree(_member->cmt[i]);
    }
    vfree(_member->cmt);

    //for (int i = 0; i < cenv->nr_valid_tpages; i++) {
    //      kfree(_member->mem_table[i]);
    //}
    //vfree(_member->mem_table);

    lru_kfree(_member->lru);
}

int cgo_destroy(void) {
    print_cache_stat(cstat);

    cgo_print_member();

    cgo_member_kfree(cmbr);
    return 0;
}

int cgo_load(lpa_t lpa, request *const req, snode *wb_entry, uint64_t *nsecs_completed) {
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

    NVMEV_DEBUG("Reading a mapping PPA %u in %s.\n", cmt->t_ppa, __func__);
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

void __page_to_pte(value_set *value, struct pt_struct *pt, uint64_t idx) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t start_lpa = idx * EPP;

    for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
        ppa_t ppa = *(ppa_t*) (value->value + (i * ENTRY_SIZE));
        pt[i].ppa = ppa;
#ifdef STORE_KEY_FP
        pt[i].key_fp = *(fp_t*) (value->value + (i * ENTRY_SIZE) + sizeof(ppa));
#endif

        if(ppa != UINT_MAX) {
            NVMEV_DEBUG("Bringing in LPA %u PPA %u in %s.\n", 
                         start_lpa + i, ppa, __func__);
        }
    }
}

void __cgo_pte_to_page(value_set *value, struct pt_struct *pt, uint64_t idx) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t start_lpa = idx * EPP;

    for(int i = 0; i < spp->pgsz / ENTRY_SIZE; i++) {
        ppa_t ppa = pt[i].ppa;
        memcpy(value->value + (i * ENTRY_SIZE), &ppa, sizeof(ppa));

#ifdef STORE_KEY_FP
        fp_t fp = pt[i].key_fp;
        memcpy(value->value + (i * ENTRY_SIZE) + sizeof(ppa), &fp, sizeof(fp));
#endif

        if(ppa != UINT_MAX) {
            NVMEV_DEBUG("LPA %u PPA %u in %s.\n", start_lpa + i, ppa, __func__);
        }
    }
}

int cgo_list_up(lpa_t lpa, request *const req, snode *wb_entry,
                uint64_t *nsecs_completed, uint64_t *credits) {
    int rc = 0;
    blockmanager *bm = __demand.bm;
    uint64_t nsecs_latest = 0, nsecs = 0;
    struct ssdparams *spp = &d_member.ssd->sp;

    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    struct cmt_struct *victim = NULL;

    NVMEV_ERROR("Got CMT IDX %u.\n", IDX(lpa));

    struct inflight_params *i_params;

    if (cgo_is_full()) {
        NVMEV_DEBUG("%s lpa %u translation cache full.\n", __func__, lpa);
        victim = (struct cmt_struct *)lru_pop(cmbr->lru);
        cmbr->nr_cached_tpages--;

        NVMEV_ASSERT(victim->idx != IDX(lpa));

        if (victim->state == DIRTY) {
            cstat->dirty_evict++;

            if(victim->t_ppa != UINT_MAX) {
                //NVMEV_ERROR("Marking previous PPA %u for IDX %u invalid.\n",
                //        victim->t_ppa, victim->idx);
                //mark_grain_invalid(ftl, PPA_TO_PGA(victim->t_ppa, 0), GRAIN_PER_PAGE);
            }

            i_params = get_iparams(req, wb_entry);
            i_params->jump = GOTO_COMPLETE;
            //i_params->pte = cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];

            struct ppa p = get_new_page(ftl, MAP_IO);
            ppa_t ppa = ppa2pgidx(ftl, &p);

            advance_write_pointer(ftl, MAP_IO);
            mark_page_valid(ftl, &p);
            mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

            /*
             * UINT_MAX - 1 for a mapping page, as opposed to UINT_MAX which
             * is a page of invalid KV pair mappings.
             *
             * The IDX is used during GC so that we know which CMT entry
             * to update.
             */

            oob[ppa][0] = victim->idx * EPP;

            victim->t_ppa = ppa;
            victim->state = CLEAN;

            NVMEV_DEBUG("Assigned PPA %u to victim at IDX %u in %s.\n",
                        ppa, victim->idx, __func__);

            value_set *_value_mw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
            _value_mw->ssd = d_member.ssd;

            __cgo_pte_to_page(_value_mw, victim->pt, victim->idx);
            nsecs = __demand.li->write(victim->t_ppa, PAGESIZE, _value_mw, ASYNC,
                                       make_algo_req_rw(MAPPINGW, _value_mw,
                                       req, wb_entry));

            rc = 1;
            (*credits) += GRAIN_PER_PAGE;

            NVMEV_DEBUG("Evicted DIRTY mapping entry IDX %u in %s.\n",
                         victim->idx, __func__);
        } else {
            NVMEV_DEBUG("Evicted CLEAN mapping entry IDX %u in %s.\n",
                    victim->idx, __func__);
            cstat->clean_evict++;
        }

        victim->lru_ptr = NULL;
        //__reset_pt(victim->pt);
        kfree(victim->pt);
        victim->pt = NULL;
    }

    nsecs_latest = max(nsecs_latest, nsecs);

    NVMEV_DEBUG("Building mapping PPA %u for LPA %u\n", cmt->t_ppa, lpa);
    cmt->lru_ptr = lru_push(cmbr->lru, (void *)cmt);
    cmbr->nr_cached_tpages++;

    if (cmt->is_flying) {
        NVMEV_ERROR("Passed flying check in %s.\n", __func__);
        cmt->is_flying = false;

        if(!cmt->pt) {
            cmt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
            NVMEV_ASSERT(cmt->pt);

            for(int i = 0; i < EPP; i++) {
                cmt->pt[i].ppa = UINT_MAX;
#ifdef STORE_KEY_FP
                cmt->pt[i].key_fp = FP_MAX;
#endif
            }
        }

        if (req) {
            NVMEV_ERROR("Entered req branch in %s.\n", __func__);

            NVMEV_ASSERT(req->mapping_v);
            __page_to_pte(req->mapping_v, cmt->pt, cmt->idx);
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

                //printk("Should have called inf_assign_try in cgo_list_up!\n");
                //inf_assign_try(retry_req);
            }
        } else if (wb_entry) {
            NVMEV_ASSERT(wb_entry->mapping_v);
            __page_to_pte(wb_entry->mapping_v, cmt->pt, cmt->idx);
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

int cgo_wait_if_flying(lpa_t lpa, request *const req, snode *wb_entry) {
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

int cgo_touch(lpa_t lpa) {
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    lru_update(cmbr->lru, cmt->lru_ptr);
    return 0;
}

int cgo_update(lpa_t lpa, struct pt_struct pte) {
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("cgo_update pte ppa %u for lpa %u\n", pte.ppa, lpa);

    if (cmt->pt) {
        NVMEV_DEBUG("Setting LPA %u to PPA %u FP %u in update.\n", lpa, pte.ppa, pte.key_fp);
        cmt->pt[OFFSET(lpa)] = pte;

        if (!IS_INITIAL_PPA(cmt->t_ppa) && cmt->state == CLEAN) {
            //invalidate_page(__demand.bm, cmt->t_ppa, MAP);
            /*
             * Only safe if assuming battery-backed DRAM.
             */

            NVMEV_ERROR("Marking mapping PPA %u invalid as it was dirtied in memory.\n",
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
        //if (++cnt % 10240 == 0) //printk("cgo_update %d\n", cnt);
        ////printk("cgo_update %d\n", ++cnt);
    }
    return 0;
}

bool cgo_is_hit(lpa_t lpa) {
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    if (cmt->pt != NULL) {
        cstat->cache_hit++;
        return 1;
    } else {
        cstat->cache_miss++;
        return 0;
    }
}

bool cgo_is_full(void) {
    return (cmbr->nr_cached_tpages >= cenv->max_cached_tpages);
}

struct pt_struct cgo_get_pte(lpa_t lpa) {
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    if (cmt->pt) {
        NVMEV_DEBUG("%s returning %u for LPA %u IDX %u\n", 
                    __func__, cmt->pt[OFFSET(lpa)].ppa, lpa, IDX(lpa));
        return cmt->pt[OFFSET(lpa)];
    } else {
        if(cmt->t_ppa == UINT_MAX) {
            NVMEV_DEBUG("%s CMT was NULL for LPA %u IDX %u\n", 
                        __func__, lpa, IDX(lpa));
            /*
             * Haven't used this CMT entry yet.
             */

            cmt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
            for(int i = 0; i < EPP; i++) {
                cmt->pt[i].ppa = UINT_MAX;
#ifdef STORE_KEY_FP
                cmt->pt[i].key_fp = FP_MAX;
#endif
            }

            return cmt->pt[OFFSET(lpa)];
        } else {
            BUG_ON(true);
        }
        /* FIXME: to handle later update after evict */
        //return cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];
    }
    /*      if (unlikely(cmt->pt == NULL)) {
    //printk("Should have aborted!!!! %s:%d
    , __FILE__, __LINE__);;
    }
    return cmt->pt[OFFSET(lpa)]; */
}

struct cmt_struct *cgo_get_cmt(lpa_t lpa) {
    return cmbr->cmt[IDX(lpa)];
}
