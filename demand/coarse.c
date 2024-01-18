/*
 * Coarse-grained Cache (default)
 */

#include "cache.h"
#include "coarse.h"
#include "utility.h"
#include "page.h"
#include "./interface/interface.h"

#include <linux/random.h>
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
    printk(" |  - Max cached tentries:     %d\n", 
          _env->max_cached_tentries);
	//printk(" |  (PageFTL cached tpages:  %d)\n", _env->nr_tpages_optimal_caching);
	printk(" |---------- Demand Cache Log END\n");
	printk("\n");
}

static void cg_env_init(cache_t c_type, struct cache_env *const _env) {
    struct ssdparams spp = d_member.ssd->sp;

	_env->c_type = c_type;

	_env->nr_tpages_optimal_caching = spp.tt_pgs * 4 / spp.pgsz;
	_env->nr_valid_tpages = (spp.tt_pgs / EPP) + ((spp.tt_pgs % EPP) ? 1 : 0);
	_env->nr_valid_tentries = _env->nr_valid_tpages * EPP;

	//_env->caching_ratio = d_env.caching_ratio;
	//_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio;

    uint64_t capa = spp.tt_pgs * spp.pgsz;
    uint64_t dram = (uint64_t)((capa * 100) / 100000);
    printk("DRAM is %lluMB\n", dram >> 20);

    _env->max_cached_tpages = 10; // dram / spp.pgsz;
    _env->max_cached_tentries = (dram / ENTRY_SIZE);
    //_env->max_cached_tentries = _env->max_cached_tentries % 512 > 0 ? 
    //    (_env->max_cached_tentries + 512) - (_env->max_cached_tentries % 512) : 
    //    _env->max_cached_tentries ;
    //_env->max_cached_tentries = 512 * 5;

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

		cmt[i]->t_ppa = UINT_MAX;
		cmt[i]->idx = i;
		cmt[i]->pt = NULL;
		cmt[i]->lru_ptr = NULL;
		cmt[i]->state = CLEAN;
		cmt[i]->is_flying = false;
        cmt[i]->cached_cnt = 0;
        cmt[i]->len_on_disk = 0;

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
        if(_member->cmt[i]->pt) {
            kfree(_member->cmt[i]->pt);
        }
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

bool _update_pt(struct cmt_struct *pt, lpa_t lpa, ppa_t ppa, fp_t fp) {
    uint64_t entry_sz = ENTRY_SIZE;

    for(int i = 0; i < pt->cached_cnt; i++) {
        if(pt->pt[i].lpa == lpa) {
            pt->pt[i].ppa = ppa;
#ifdef STORE_KEY_FP
            pt->pt[i].key_fp = fp;
#endif
            return true;
        }
    }

    pt->pt[pt->cached_cnt].lpa = lpa;
    pt->pt[pt->cached_cnt].ppa = ppa;
#ifdef STORE_KEY_FP
    pt->pt[pt->cached_cnt].key_fp = fp;
#endif

    NVMEV_DEBUG("%s IDX %u LPA %u PPA %u pos %u\n", 
               __func__, pt->idx, lpa, ppa, pt->cached_cnt); 
    pt->cached_cnt++;

    if(pt->cached_cnt % CACHE_GRAIN == 0) {
        //BUG_ON(true);
        //printk("Inside:\n");
        //for(int i = 0; i < pt->cached_cnt; i++) {
        //    printk("%u %u ", pt->pt[i].lpa, pt->pt[i].ppa);
        //}
        //printk("\n");
        uint32_t cnt = pt->cached_cnt / CACHE_GRAIN;
        void* lrup = pt->lru_ptr;
        BUG_ON(!lrup);
        //lru_delete(cmbr->lru, (void *) pt->lru_ptr);
        pt->lru_ptr = NULL;
        char *buf = (char*) pt->pt;
        char *new_buf = krealloc(buf, sizeof(struct pt_struct) * CACHE_GRAIN * (cnt + 1), 
                                 GFP_KERNEL);
        //if(new_buf != buf) {
        //    char* old = buf;
        buf = new_buf;
        //    kfree(old);
        //}

        pt->pt = (struct pt_struct*) buf;

        for(int i = pt->cached_cnt; i < pt->cached_cnt + CACHE_GRAIN; i++) {
            pt->pt[i].lpa = UINT_MAX;
            pt->pt[i].ppa = UINT_MAX;
#ifdef STORE_KEY_FP
            pt->pt[i].key_fp = FP_MAX;
#endif
        }

        pt->lru_ptr = lrup; //lru_push(cmbr->lru, (void *) pt);
        cmbr->nr_cached_tentries += CACHE_GRAIN;

        if(pt->t_ppa != UINT_MAX && pt->state == CLEAN) {
            NVMEV_DEBUG("Marking mapping PPA %u grain %llu IDX %u len %d (cached cnt %d)"
                    "invalid as it was resizing.\n",
                    pt->t_ppa, pt->grain, pt->idx, pt->len_on_disk, pt->cached_cnt);
            mark_grain_invalid(ftl, PPA_TO_PGA(pt->t_ppa, pt->grain), 
                               pt->len_on_disk);
            //NVMEV_DEBUG("Resizecaller is %pS\n", __builtin_return_address(0));
            //NVMEV_DEBUG("Resizecaller is %pS\n", __builtin_return_address(1));
            //printk("Resizecaller is %pS\n", __builtin_return_address(2));
            pt->state = DIRTY;
        }
        
        NVMEV_INFO("Resized IDX %u to %u\n", pt->idx, pt->cached_cnt + CACHE_GRAIN);
        NVMEV_ASSERT(new_buf);
    }

    return false;
}

int cg_load(lpa_t lpa, request *const req, snode *wb_entry, uint64_t *nsecs_completed) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct inflight_params *i_params;
    struct ssdparams spp = d_member.ssd->sp;
    uint64_t nsec = 0;

	if (IS_INITIAL_PPA(cmt->t_ppa)) {
        NVMEV_DEBUG("Tried to load an unmapped PPA in %s.\n", __func__);
		return 0;
	}

	i_params = get_iparams(req, wb_entry);
	i_params->jump = GOTO_LIST;

    if(cmt->pt) {
        cmt->is_flying = false;
        return 0;
    }

	value_set *_value_mr = inf_get_valueset(NULL, FS_MALLOC_R, spp.pgsz);

    if(req) {
        NVMEV_ASSERT(!wb_entry);
        req->mapping_v = _value_mr;
    } else {
        NVMEV_ASSERT(!req);
        wb_entry->mapping_v = _value_mr;
    }

    NVMEV_INFO("Reading a mapping PPA %u in %s IDX %lu.\n", cmt->t_ppa, __func__, IDX(lpa));

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

void __alloc_pt(uint64_t idx) {
    struct cmt_struct *pt = cmbr->cmt[idx];
    uint64_t max = UINT_MAX;
    uint64_t entry_sz = ENTRY_SIZE;
    
    char* pt_buf = kzalloc(sizeof(struct pt_struct) * CACHE_GRAIN, GFP_KERNEL);
    pt->pt = (struct pt_struct*) pt_buf;
    //pt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
    for(int i = 0; i < CACHE_GRAIN; i++) {
        pt->pt[i].lpa = UINT_MAX;
        pt->pt[i].ppa = UINT_MAX;
#ifdef STORE_KEY_FP
        pt->pt[i].key_fp = FP_MAX;
#endif
    }
    //uint64_t end = local_clock();
    //NVMEV_DEBUG("alloc takes %u ns\n", end - start);
}

bool __pt_contains(struct cmt_struct *pt, lpa_t lpa, ppa_t ppa) {
    for(int i = 0; i < pt->cached_cnt; i++) {
        if(ppa == UINT_MAX && pt->pt[i].lpa == lpa) {
            return true;
        } else if(pt->pt[i].ppa == ppa && pt->pt[i].lpa == lpa) {
            return true;
        }
    }

    return false;
}

struct pt_struct __get_pte(struct cmt_struct *pt, lpa_t lpa) {
    struct pt_struct ret = (struct pt_struct) { lpa, UINT_MAX };
    for(int i = 0; i < pt->cached_cnt; i++) {
        if(pt->pt[i].lpa == lpa) {
            return pt->pt[i];
        }
    }
    return ret;
}

void __page_to_ptes_wcmt(value_set *value, struct cmt_struct *cmt) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t entry_sz = ENTRY_SIZE;
    uint64_t total = 0;
#ifdef STORE_KEY_FP
    BUG_ON(true);
    fp_t fp = FP_MAX;
#else
    uint8_t fp = 255;
#endif

    uint64_t last_idx = UINT_MAX;
    for(int i = 0; i < spp->pgsz / entry_sz; i++) {
        lpa_t lpa = *(lpa_t*) (value->value + (i * entry_sz));
        ppa_t ppa = *(ppa_t*) (value->value + ((i * entry_sz) + sizeof(lpa)));
#ifdef STORE_KEY_FP
        fp_t fp = *(fp_t*) (value->value + (i * entry_sz) + sizeof(lpa) + 
                            sizeof(ppa));
#endif
        uint64_t idx = IDX(lpa);
        uint64_t offset = OFFSET(lpa);

        if(lpa == 0 || lpa == UINT_MAX) {
            continue;
        }

        if(idx != cmt->idx) {
            continue;
        }

        if(cmt->pt == NULL) {
            uint64_t max = UINT_MAX;
            uint64_t entry_sz = ENTRY_SIZE;

            char* pt_buf = kzalloc(sizeof(struct pt_struct) * CACHE_GRAIN, GFP_KERNEL);
            cmt->pt = (struct pt_struct*) pt_buf;
            //pt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
            for(int i = 0; i < CACHE_GRAIN; i++) {
                cmt->pt[i].lpa = UINT_MAX;
                cmt->pt[i].ppa = UINT_MAX;
#ifdef STORE_KEY_FP
                cmt->pt[i].key_fp = FP_MAX;
#endif
            }
        }

        _update_pt(cmt, lpa, ppa, fp);
    }
}

void __page_to_ptes(value_set *value, uint64_t idx_, bool up_cache) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t entry_sz = ENTRY_SIZE;
    uint64_t total = 0;
#ifdef STORE_KEY_FP
    BUG_ON(true);
    fp_t fp = FP_MAX;
#else
    uint8_t fp = 255;
#endif

    uint64_t last_idx = UINT_MAX;
    for(int i = 0; i < spp->pgsz / entry_sz; i++) {
        lpa_t lpa = *(lpa_t*) (value->value + (i * entry_sz));
        ppa_t ppa = *(ppa_t*) (value->value + ((i * entry_sz) + sizeof(lpa)));
#ifdef STORE_KEY_FP
        fp_t fp = *(fp_t*) (value->value + (i * entry_sz) + sizeof(lpa) + 
                            sizeof(ppa));
#endif
        uint64_t idx = IDX(lpa);
        uint64_t offset = OFFSET(lpa);

        if(lpa == 0 || lpa == UINT_MAX) {
            continue;
        }

        if(idx != last_idx) {
            //printk("Saw IDX %llu LPA %u\n", idx, lpa);
            total++;
        }

        last_idx = idx;

        //if(idx != idx_) {
        //    NVMEV_DEBUG("Skipping because IDX %u != IDX %u\n", idx, idx_);
        //    continue;
        //}

        if(idx >= cenv->nr_valid_tpages) {
            //printk("WAAAAAA %llu LPA %u %d\n", idx, lpa, i);
            //printk("Caller is %pS\n", __builtin_return_address(0));
            //printk("Caller is %pS\n", __builtin_return_address(1));
            //printk("Caller is %pS\n", __builtin_return_address(2));
            continue;
        }

        struct cmt_struct *pt = cmbr->cmt[idx];
        if(pt != NULL && idx_ != idx) { 
            NVMEV_DEBUG("PT was dirty for IDX %u\n", pt->idx);
            continue;
        }

        NVMEV_INFO("Adding LPA %u PPA %u IDX %llu offset %d\n", lpa, ppa, idx, i);
#ifdef STORE_KEY_FP
        //NVMEV_INFO("Adding LPA %u PPA %u FP %u IDX %llu offset %d\n", lpa, ppa, fp, idx, i);
#endif

        if(pt->pt == NULL) {
            BUG_ON(pt->cached_cnt > 0);

            __alloc_pt(idx);
            if(pt->lru_ptr == NULL)  {
                pt->lru_ptr = lru_push(cmbr->lru, (void *) pt);
            }

            if(up_cache) {
                /*
                 * Don't update this during GC, as the mappings
                 * we put in this PT temporarily will be
                 * freed after GC is done.
                 */
                cmbr->nr_cached_tentries += CACHE_GRAIN;
            }
        } else {
            //BUG_ON(pt->lru_ptr == NULL);
        }

        //BUG_ON(__pt_contains(pt, lpa, ppa));
        _update_pt(pt, lpa, ppa, fp);

        //pt->pt[offset].ppa = ppa;

        //if(cmbr->nr_cached_tentries > cenv->max_cached_tentries) {
        //    NVMEV_ERROR("WTF!!! %u %u\n", cmbr->nr_cached_tentries, cenv->max_cached_tentries);
        //}
        //BUG_ON(cmbr->nr_cached_tentries > cenv->max_cached_tentries);
    }

    //printk("Added %llu indexes.\n", total);
}

inline uint64_t __num_cached(struct cmt_struct* cmt) {
    uint64_t ret = CACHE_GRAIN * ((cmt->cached_cnt / CACHE_GRAIN) + 1);
    return ret;
}

inline uint64_t __cmt_real_size(struct cmt_struct *cmt) {
    return cmt->cached_cnt * ENTRY_SIZE;
}

bool __should_sample(void) {
    return false;
    uint32_t rand;
    get_random_bytes(&rand, sizeof(rand));
    if(rand % 100 > 95) {
        return true;
    }
    return false;
}

struct victim_entry {
    lpa_t lpa;
    ppa_t ppa;
#ifdef STORE_KEY_FP
    fp_t key_fp;
#endif
    struct cmt_struct *pt;
};

/*
 * We potentially have to bring in an entire page of cache entries for the same IDX,
 * so that's how much space we check.
 *
 * Would be good in the future to have perhaps a counter for each IDX, so we can know
 * exactly how much space we need upfront.
 */

inline bool __have_enough(void) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t entries_per_page = spp->pgsz / ENTRY_SIZE;

    if(cmbr->nr_cached_tentries == 0) {
        return true;
    } else {
        return (cmbr->nr_cached_tentries + entries_per_page + 1) <= cenv->max_cached_tentries;
    }
}

inline bool __dont_have_enough(void) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t entries_per_page = spp->pgsz / ENTRY_SIZE;

    if(cmbr->nr_cached_tentries == 0) {
        return false;
    } else {
        return (cmbr->nr_cached_tentries + entries_per_page + 1) > cenv->max_cached_tentries;
    }
}

struct victim_entry *victims;
void __collect_victims(struct conv_ftl *conv_ftl, LRU* lru, 
                       struct victim_entry **_v, uint64_t *cnt) { 
    struct ssdparams *spp = &d_member.ssd->sp;
    void* item = NULL;
    uint64_t count = 0, clean_count = 0;
    uint64_t loop_cnt = 0;
    uint64_t remaining = spp->pgsz;
    uint64_t cached;
    bool empty = true;

    uint64_t start = local_clock();
    uint64_t inner = 0;

    bool sample = false;
    if(__should_sample()) {
        sample = true;
    }

    while((item = lru_pop(lru)) != NULL) {
        struct cmt_struct *pt = (struct cmt_struct*) item;
        cached = __num_cached(pt);
        empty = false;

        if(pt->state == CLEAN) {
            //BUG_ON(pt->cached_cnt == 0);
            //BUG_ON(!pt->pt);

            NVMEV_INFO("2 Evicting %llu pairs for IDX %u\n", cached, pt->idx);
            clean_count += cached;
            cstat->clean_evict++;
            cmbr->nr_cached_tentries -= cached;
            pt->cached_cnt = 0;
            pt->lru_ptr = NULL;
            kfree((char*) pt->pt);
            pt->pt = NULL;

            if(__have_enough()) {
                goto end;
            } else {
                continue;
            }
        }

        uint64_t size = cached * (ENTRY_SIZE);
        uint64_t incr = ENTRY_SIZE;
        ppa_t ppa, lpa;
#ifdef STORE_KEY_FP
        fp_t fp;
#endif

        if(size <= remaining) {
            NVMEV_INFO("Evicting %llu pairs for IDX %u\n", cached, pt->idx);
            //BUG_ON(count >= EPP);
            cmbr->nr_cached_tentries -= cached;
            remaining -= incr * cached;

            uint64_t start_inner = local_clock();
            for(uint64_t i = 0; i < cached; i++) {
                if(i == 0) {
                    //printk("Caught in here for IDX %u\n", pt->idx);
                    NVMEV_ASSERT(pt->pt[i].lpa != UINT_MAX);
                }

                lpa = pt->pt[i].lpa;
                ppa = pt->pt[i].ppa;
#ifdef STORE_KEY_FP
                fp = pt->pt[i].key_fp;
#endif

                NVMEV_DEBUG("CHECK IDX %u LPA %u PPA %u pos %u\n", 
                        pt->idx, lpa, ppa, i);  

                ppa = ppa;
                //if(lpa != UINT_MAX && lpa > 30046504) {
                //    printk("Copying %u %u to victim list from idx %u.\n",
                //            lpa, ppa, pt->idx);
                //}
                victims[count + i].lpa = lpa;
                victims[count + i].ppa = ppa;
#ifdef STORE_KEY_FP
                victims[count + i].key_fp = fp;
#endif
            }

            count += cached;

            uint64_t end_inner = local_clock();
            inner += (end_inner - start_inner);

            pt->state = CLEAN;
            NVMEV_DEBUG("Marking PT IDX %u clean in victims.\n", pt->idx);
            pt->lru_ptr = NULL;
            kfree((char*) pt->pt);
            pt->pt = NULL;
        } else {
            pt->lru_ptr = lru_push(cmbr->lru, (void *) pt);
            NVMEV_INFO("Skipping IDX %u because it was bigger than what we had left (%llu %llu).\n", 
                        pt->idx, size, remaining);
            //break;
        }

        if(remaining == 0 || loop_cnt++ == 32) {
            goto end;
        }
    }

end:
    //if(empty) {
    //    for(int i = 0; i < cenv->nr_valid_tpages; i++) {
    //        NVMEV_ASSERT(cmbr->cmt[i]->pt == NULL);
    //        NVMEV_ASSERT(cmbr->cmt[i]->cached_cnt == 0);
    //        if(cmbr->cmt[i]->state != CLEAN) {
    //            NVMEV_INFO("DIRTY for IDX %d\n", i);
    //        }
    //        NVMEV_ASSERT(cmbr->cmt[i]->state == CLEAN);
    //        NVMEV_ASSERT(cmbr->cmt[i]->lru_ptr == NULL);
    //    }
    //}

    if(empty) {
        cmbr->nr_cached_tentries = 0;
    }

    //BUG_ON(count == 0 && clean_count == 0);
    NVMEV_INFO("Collected %llu (%llu) victims %llu loops.\n", 
                count, clean_count, loop_cnt);
    if(count == 0) {
        NVMEV_DEBUG("Had clean count %u\n", clean_count);
    }
    NVMEV_ASSERT(count * ENTRY_SIZE <= spp->pgsz);

    //if(sample) {
        //uint64_t end = local_clock();
        //NVMEV_DEBUG("Took %u ns to collect victims. %u ns spent in inner loops\n", 
        //        end - start, inner);
    //}

    *cnt = count;
}

int cg_list_up(lpa_t lpa, request *const req, snode *wb_entry, 
               uint64_t *nsecs_completed, uint64_t *credits) {
    int rc = 0;
	blockmanager *bm = __demand.bm;
    uint64_t nsecs_latest = 0, nsecs = 0;
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t cnt;
    uint64_t evicted = 0;
    bool wrote = false;

    uint64_t get = 0, alloc = 0, mcpy = 0, final = 0, start = 0, write = 0, queue = 0;

	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct cmt_struct *victim = NULL;

    NVMEV_INFO("list_up for IDX %lu.\n", IDX(lpa));

	struct inflight_params *i_params;

again:
	if (cg_is_full() || __dont_have_enough()) {
        NVMEV_INFO("%s lpa %u translation cache full because %s (cur %u max %u).\n", __func__, lpa,
                cg_is_full() ? "cg_is_full" : "cg_no_room", cmbr->nr_cached_tentries,
                cenv->max_cached_tentries);

        /*
         * For now, we pessimistically evict a page worth of items, because
         * it's possible that the translation mapping load after this
         * loads that many. We would do well to have some check of how
         * many items we strictly need to evict.
         */

        //victim = (struct cmt_struct *)lru_peek(cmbr->lru);
        if(0) { // victim->state == CLEAN) {
            while(1) {
                victim = (struct cmt_struct *)lru_pop(cmbr->lru);

                NVMEV_ASSERT(victim->pt);
                cmbr->nr_cached_tentries -= __num_cached(victim);
                evicted += __num_cached(victim);

                NVMEV_DEBUG("Evicting %u entries from CLEAN IDX %u\n", 
                        __num_cached(victim), victim->idx);

                victim->cached_cnt = 0;
                victim->lru_ptr = NULL;
                kfree((char*) victim->pt);
                victim->pt = NULL;

                victim = (struct cmt_struct *)lru_peek(cmbr->lru);       
                if(!victim || victim->state != CLEAN || __have_enough()) {
                    break;
                }
            }

            if(__dont_have_enough()) {
                NVMEV_DEBUG("But we still need to evict more. %u remaining. %u evicted\n",
                            cmbr->nr_cached_tentries, evicted);
                goto again;
            }
        } else {
            start = local_clock();
            victims = (struct victim_entry*) 
                      kzalloc(EPP * sizeof(struct victim_entry), GFP_KERNEL);
            __collect_victims(ftl, cmbr->lru, &victims, &cnt);

            if(cnt == 0) {
                /*
                 * We evicted all clean items.... or we have a terrible bug.
                 */
                //printk("All clean!\n");
                kfree(victims);

                if(__dont_have_enough()) {
                    goto again;
                } else {
                    goto out;
                }
            }

            i_params = get_iparams(req, wb_entry);
            i_params->jump = GOTO_COMPLETE;

            struct ppa p = get_new_page(ftl, MAP_IO);
            ppa_t ppa = ppa2pgidx(ftl, &p);
            uint64_t grains = (cnt * ENTRY_SIZE) / GRAIN_PER_PAGE;

            advance_write_pointer(ftl, MAP_IO);
            mark_page_valid(ftl, &p);
            mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), grains);

            if(grains != GRAIN_PER_PAGE) {
                uint64_t offset = grains;
                mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), GRAIN_PER_PAGE - offset);
                mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), GRAIN_PER_PAGE - offset);

                uint64_t to = ((uint64_t) ppa * spp->pgsz) + (offset * GRAINED_UNIT);
                memset(nvmev_vdev->ns[0].mapped + to, 0x0, (GRAIN_PER_PAGE - offset) *
                       GRAINED_UNIT);
            }

            value_set *_value_mw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
            _value_mw->ssd = d_member.ssd;

            uint64_t step = ENTRY_SIZE;
            uint8_t *ptr = _value_mw->value;
            for(int i = 0; i < cnt; i++) {
                lpa_t lpa = victims[i].lpa;
                ppa_t m_ppa = victims[i].ppa;
#ifdef STORE_KEY_FP
                fp_t fp = victims[i].key_fp;
#endif

                if(lpa != UINT_MAX) {
                    NVMEV_INFO("In memcpy for LPA %u PPA %u IDX %lu going to PPA %u\n", 
                               lpa, m_ppa, IDX(lpa), ppa);
                }

                memcpy(ptr + (i * step), &lpa, sizeof(lpa));
                memcpy(ptr + (i * step) + sizeof(lpa), &m_ppa, sizeof(m_ppa));
#ifdef STORE_KEY_FP
                if(lpa != UINT_MAX) {
                    //NVMEV_INFO("LPA %u PPA %u gets FP %u\n", lpa, ppa, fp);
                }
                memcpy(ptr + (i * step) + sizeof(lpa) + sizeof(ppa), &fp, 
                        sizeof(fp));
#endif
            }

            NVMEV_DEBUG("Passed memcpy.\n");

            if(!wrote) {
                nsecs = __demand.li->write(ppa, PAGESIZE, _value_mw, ASYNC, 
                        make_algo_req_rw(MAPPINGW, _value_mw, 
                            req, wb_entry));
                wrote = true;
            } else {
                nsecs = __demand.li->write(ppa, PAGESIZE, _value_mw, ASYNC, NULL);
                kfree(_value_mw->value);
                kfree(_value_mw);
                d_stat.trans_w++;

                if(req) {
                    d_stat.t_write_on_read++;
                } else {
                    d_stat.t_write_on_write++;
                }
            }

            uint8_t oob_idx = 0;
            uint64_t last_idx = UINT_MAX;
            for(int i = 0; i < cnt; i++) {
                if(victims[i].lpa == UINT_MAX) {
                    continue;
                }

                //NVMEV_DEBUG("IDX %u LPA %u gets PPA %u\n", 
                //            victims[i].lpa / EPP, victims[i].lpa, ppa);

                uint64_t idx = victims[i].lpa / EPP;
                struct cmt_struct *cmt = cmbr->cmt[idx];
                //cstat->dirty_evict++;

                if(idx == last_idx) {
                    continue;
                }

                cmt->grain = oob_idx;
                oob[ppa][oob_idx++] = victims[i].lpa;
                NVMEV_INFO("IDX %llu (CC %u) gets grain %u PPA %u LPA %u\n", 
                            idx, cmt->cached_cnt, oob_idx - 1, ppa, victims[i].lpa);

                uint64_t g_per_cg = (CACHE_GRAIN * ENTRY_SIZE) / GRAINED_UNIT;
                for(int i = 1; i < ((cmt->cached_cnt / CACHE_GRAIN) + 1) * g_per_cg; i++) {
                    oob[ppa][oob_idx++] = UINT_MAX - 1;
                }

                NVMEV_DEBUG("IDX %llu is leaving memory with a cached_cnt of %u\n",
                        idx, cmt->cached_cnt);

                cmt->cached_cnt = 0;
                cmt->len_on_disk = ((cmt->cached_cnt / CACHE_GRAIN) + 1) * g_per_cg;
                cmt->t_ppa = ppa;
                last_idx = idx;
            }

            if(oob_idx < GRAIN_PER_PAGE) {
                oob[ppa][oob_idx] = UINT_MAX;
            }

            kfree(victims);

            rc = 1;
            (*credits) += GRAIN_PER_PAGE;

            //printk("Evicting %llu items. %u remaining (%u max).\n", 
            //        cnt, cmbr->nr_cached_tentries, cenv->max_cached_tentries);

            cstat->dirty_evict++;
            evicted += cnt;

            if(__dont_have_enough()) {
                NVMEV_INFO("But we still need to evict more. %u remaining. %llu evicted\n",
                            cmbr->nr_cached_tentries, evicted);
                goto again;
            }
        }
        NVMEV_DEBUG("Exiting victim loop.\n");
	} 

out:
    nsecs_latest = max(nsecs_latest, nsecs);

    NVMEV_ASSERT(cmt->idx == IDX(lpa));

    if(!cmt->lru_ptr) {
        cmt->lru_ptr = lru_push(cmbr->lru, (void *) cmt);
    }

    if(cmt->t_ppa != UINT_MAX && cmt->pt != NULL) {
        NVMEV_ASSERT(!cmt->is_flying);
    }

    //if(cmt->t_ppa != UINT_MAX && cmt->pt == NULL) {
    //    if(wb_entry && wb_entry->mapping_v) {
    //        inf_free_valueset(wb_entry->mapping_v, FS_MALLOC_W);
    //    } else if(req && req->mapping_v) {
    //        inf_free_valueset(req->mapping_v, FS_MALLOC_W);
    //    }

    //    if(cmt->pt == NULL) {
    //        NVMEV_DEBUG("Building mapping PPA %u for LPA %u IDX %u\n", 
    //                cmt->t_ppa, lpa, IDX(lpa));
    //        /*
    //         * This CMT was written above.
    //         */

    //        value_set *v = inf_get_valueset(NULL, FS_MALLOC_R, spp->pgsz);
    //        v->ssd = d_member.ssd;
    //        nsecs = __demand.li->read(cmt->t_ppa, PAGESIZE, v, ASYNC, NULL);
    //        cmt->is_flying = true;
    //        d_stat.trans_r++;

    //        if(req) {
    //            req->mapping_v = v;
    //            d_stat.t_read_on_read++;
    //        } else {
    //            wb_entry->mapping_v = v;
    //            d_stat.t_read_on_write++;
    //        }

    //        if(sample) {
    //            write = local_clock();
    //        }
    //    }
    //} 
    
    //else {
    //    NVMEV_ASSERT(!cmt->is_flying);

    //    /*
    //     * This CMT entry hasn't been written yet. We won't add a group of CMT
    //     * entries because there is no page of CMT entries to read. Just
    //     * add one entry.
    //     */

    //}

    //cmbr->nr_cached_tentries++;

	if (cmt->is_flying) {
        //NVMEV_INFO("Passed flying check LPA %u PPA %u IDX %lu %s.\n", 
        //            lpa, cmt->t_ppa, IDX(lpa), __func__);
		cmt->is_flying = false;

		if (req) {
            NVMEV_ASSERT(req->mapping_v);
            __page_to_ptes(req->mapping_v, IDX(lpa), true);
            NVMEV_DEBUG("IDX %u entered memory with a cached_cnt of %u\n",
                    cmt->idx, cmt->cached_cnt);

            //if(__pt_contains(cmt, lpa, UINT_MAX)) {
            //    cmbr->nr_cached_tentries--;
            //    NVMEV_DEBUG("1Decr IDX %u.\n", IDX(lpa));
            //}

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
            NVMEV_DEBUG("pagetoptes triggered for IDX %u PPA %u\n", IDX(lpa), cmt->t_ppa);
            __page_to_ptes(wb_entry->mapping_v, IDX(lpa), true);
            NVMEV_DEBUG("IDX %u entered memory with a cached_cnt of %u\n",
                    cmt->idx, cmt->cached_cnt);

            //if(__pt_contains(cmt, lpa, UINT_MAX)) {
            //    cmbr->nr_cached_tentries--;
            //    NVMEV_DEBUG("1Decr IDX %u.\n", IDX(lpa));
            //}

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

    uint64_t end = local_clock();

	return rc;
}

int cg_wait_if_flying(lpa_t lpa, request *const req, snode *wb_entry) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

	if (cmt->is_flying) {
		cstat->blocked_miss++;

		if (req) q_enqueue((void *)req, cmt->retry_q);
		else if (wb_entry) q_enqueue((void *)wb_entry, cmt->retry_q);
		else printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);;

		return 1;
	}

	return 0;
}

int cg_touch(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    //NVMEV_DEBUG("Update for IDX %u\n", IDX(lpa));
	lru_update(cmbr->lru, cmt->lru_ptr);
	return 0;
}

static struct ppa ppa_to_struct(const struct ssdparams *spp, ppa_t ppa_)
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

int cg_update(lpa_t lpa, struct pt_struct pte) {
    struct ssdparams *spp = &d_member.ssd->sp;
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("cg_update pte ppa %u for lpa %u\n", pte.ppa, lpa);

	if (cmt->pt) {
        NVMEV_DEBUG("cg_update %u %u\n", lpa, pte.ppa);
#ifdef STORE_KEY_FP
        _update_pt(cmt, lpa, pte.ppa, pte.key_fp);
#else
        _update_pt(cmt, lpa, pte.ppa, 255);
#endif
		//cmt->pt[OFFSET(lpa)] = pte;

		if (!IS_INITIAL_PPA(cmt->t_ppa) && cmt->state == CLEAN) {
            /*
             * Only safe if assuming battery-backed DRAM.
             */

            /* update corresponding page status */
            NVMEV_DEBUG("Marking mapping PPA %u grain %llu IDX %lu len %d (cached cnt %d)"
                    "invalid as it was dirtied in memory.\n",
                    cmt->t_ppa, cmt->grain, IDX(lpa), cmt->len_on_disk, cmt->cached_cnt);

            uint64_t g_per_cg = (CACHE_GRAIN * ENTRY_SIZE) / GRAINED_UNIT;
            mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, cmt->grain), 
                               cmt->len_on_disk);
        }

		cmt->state = DIRTY;
        //NVMEV_DEBUG("1 Update for IDX %u\n", IDX(lpa));
		lru_update(cmbr->lru, cmt->lru_ptr);
	} else {
        NVMEV_DEBUG("2 Hit this case for IDX %u!!\n", IDX(lpa));
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
        NVMEV_INFO("t_ppa for IDX %lu is %u\n", IDX(lpa), cmt->t_ppa);
        if(__pt_contains(cmt, lpa, UINT_MAX)) {
            NVMEV_INFO("Hit for IDX %lu! %u cached! %d cached total! PPA is %u!\n", 
                       IDX(lpa), cmt->cached_cnt, cmbr->nr_cached_tentries, __get_pte(cmt, lpa).ppa);
            //BUG_ON(__get_pte(cmt, lpa).ppa == UINT_MAX);
            //BUG_ON(cmt->lru_ptr == NULL);
            cstat->cache_hit++;
            return 1;
        } else if (1 || cmt->cached_cnt % CACHE_GRAIN) {
            /*
             * There's room for another entry.
             */
            cstat->cache_hit++;
            return 1;
        } else {
            printk("Hit this for IDX %lu\n", IDX(lpa));
            cstat->cache_miss++;
            return 0;
        }
	} else {
		cstat->cache_miss++;
		return 0;
	}
}

bool cg_is_full(void) {
	return (cmbr->nr_cached_tentries >= cenv->max_cached_tentries);
}

struct pt_struct cg_get_pte(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    struct ssdparams *spp = &d_member.ssd->sp;

    NVMEV_DEBUG("Getting PTE for IDX %u\n", IDX(lpa));
	if (cmt->pt) {
        return __get_pte(cmt, lpa);
        //printk("%s returning %u for lpa %u\n", __func__, cmt->pt[OFFSET(lpa)].ppa, lpa);
		return cmt->pt[OFFSET(lpa)];
	} else {
        if(cmt->t_ppa == UINT_MAX) {
            /*
             * Haven't used this CMT entry yet.
             */

            __alloc_pt(IDX(lpa));
           
            cmbr->nr_cached_tentries += CACHE_GRAIN;
            return __get_pte(cmt, lpa);
            //return cmt->pt[OFFSET(lpa)];
        } else {
            BUG_ON(true);
            NVMEV_DEBUG("Hit this case for IDX %u reading %u!!\n", 
                        IDX(lpa), cmt->t_ppa);

            value_set *v = inf_get_valueset(NULL, FS_MALLOC_R, spp->pgsz);
            v->ssd = d_member.ssd;
            __demand.li->read(cmt->t_ppa, PAGESIZE, v, ASYNC, NULL);
            __page_to_ptes(v, IDX(lpa), true);
            d_stat.trans_r++;

            NVMEV_ASSERT(cmt->pt);
            return __get_pte(cmt, lpa);
            //BUG_ON(true);
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
