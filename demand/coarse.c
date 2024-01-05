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

	_env->nr_tpages_optimal_caching = d_env.nr_pages * 4 / spp.pgsz;
	_env->nr_valid_tpages = (d_env.nr_pages / EPP) + ((d_env.nr_pages % EPP) ? 1 : 0);
	_env->nr_valid_tentries = _env->nr_valid_tpages * EPP;

	//_env->caching_ratio = d_env.caching_ratio;
	//_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio;

    uint64_t capa = spp.tt_pgs * spp.pgsz;
    uint64_t dram = (uint64_t)((capa * 100) / 100000);
    printk("DRAM is %lluMB\n", dram >> 20);

    _env->max_cached_tpages = 10; // dram / spp.pgsz;
	_env->max_cached_tentries = dram / (sizeof(uint64_t) * 2); // 10000; // not used here

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

		cmt[i]->t_ppa = U64_MAX;
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

bool _update_pt(struct cmt_struct *pt, uint64_t lpa, uint64_t ppa) {
    uint64_t entry_sz = sizeof(uint64_t) * 2;

    for(int i = 0; i < pt->cached_cnt; i++) {
        if(pt->pt[i].lpa == lpa) {
            pt->pt[i].ppa = ppa;
            return true;
        }
    }

    pt->pt[pt->cached_cnt].lpa = lpa;
    pt->pt[pt->cached_cnt].ppa = ppa;
    NVMEV_INFO("%s IDX %u LPA %llu PPA %llu pos %u\n", 
                __func__, pt->idx, lpa, ppa, pt->cached_cnt); 
    pt->cached_cnt++;

    if(pt->cached_cnt % CACHE_GRAIN == 0) {
        uint32_t cnt = pt->cached_cnt / CACHE_GRAIN;
        char *buf = (char*) pt->pt;
        char *new_buf = krealloc(buf, sizeof(struct pt_struct) * CACHE_GRAIN * (cnt + 1), 
                       GFP_KERNEL);

        if(new_buf != buf) {
            char* old = buf;
            buf = new_buf;
            kfree(old);
        }

        pt->pt = (struct pt_struct*) buf;

        for(int i = pt->cached_cnt; i < pt->cached_cnt + CACHE_GRAIN; i++) {
            pt->pt[i].lpa = U64_MAX;
            pt->pt[i].ppa = U64_MAX;
        }

        cmbr->nr_cached_tentries += CACHE_GRAIN;

        NVMEV_INFO("Resizing IDX %u\n", pt->idx);
        NVMEV_ASSERT(buf);
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

    NVMEV_DEBUG("Reading a mapping PPA %llu in %s IDX %llu.\n", cmt->t_ppa, __func__, IDX(lpa));

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
    uint64_t max = U64_MAX;
    uint64_t entry_sz = sizeof(uint64_t) * 2;
    
    char* pt_buf = kzalloc(sizeof(struct pt_struct) * CACHE_GRAIN, GFP_KERNEL);
    pt->pt = (struct pt_struct*) pt_buf;
    //pt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
    for(int i = 0; i < CACHE_GRAIN; i++) {
        pt->pt[i].lpa = U64_MAX;
        pt->pt[i].ppa = U64_MAX;
#ifdef STORE_KEY_FP
        BUG_ON(true);
#endif
    }
    //uint64_t end = local_clock();
    //NVMEV_DEBUG("alloc takes %llu ns\n", end - start);
}

bool __pt_contains(struct cmt_struct *pt, uint64_t lpa, uint64_t ppa) {
    for(int i = 0; i < pt->cached_cnt; i++) {
        if(ppa == U64_MAX && pt->pt[i].lpa == lpa) {
            return true;
        } else if(pt->pt[i].ppa == ppa && pt->pt[i].lpa == lpa) {
            return true;
        }
    }

    return false;
}

struct pt_struct __get_pte(struct cmt_struct *pt, uint64_t lpa) {
    struct pt_struct ret = (struct pt_struct) { lpa, U64_MAX };
    for(int i = 0; i < pt->cached_cnt; i++) {
        if(pt->pt[i].lpa == lpa) {
            return pt->pt[i];
        }
    }
    return ret;
}

void __page_to_ptes(value_set *value, uint64_t idx_, bool up_cache) {
    struct ssdparams *spp = &d_member.ssd->sp;
    uint64_t entry_sz = sizeof(uint64_t) * 2;
    uint64_t total = 0;

    for(int i = 0; i < spp->pgsz / entry_sz; i++) {
        uint64_t lpa = *(uint64_t*) (value->value + (i * entry_sz));
        uint64_t ppa = *(uint64_t*) (value->value + ((i * entry_sz) + sizeof(uint64_t)));
        uint64_t idx = IDX(lpa);
        uint64_t offset = OFFSET(lpa);
        struct cmt_struct *pt = cmbr->cmt[idx];

        if(lpa == 0 || lpa == U64_MAX) {
            continue;
        }

        if(idx != idx_) {
            NVMEV_INFO("Skipping because IDX %llu != IDX %llu\n", idx, idx_);
            continue;
        }

        if(pt->state == DIRTY) {
            NVMEV_INFO("PT was dirty for IDX %u\n", pt->idx);
            continue;
        }

        NVMEV_INFO("Adding LPA %llu PPA %llu IDX %llu offset %d\n", lpa, ppa, idx, i);

        if(pt->pt == NULL) {
            uint64_t start = local_clock();
            __alloc_pt(idx);
            uint64_t end = local_clock();
            total += end - start;
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
            BUG_ON(pt->lru_ptr == NULL);
        }
        
        //BUG_ON(__pt_contains(pt, lpa, ppa));
        _update_pt(pt, lpa, ppa);

        //pt->pt[offset].ppa = ppa;

        if(cmbr->nr_cached_tentries > cenv->max_cached_tentries) {
            NVMEV_DEBUG("WTF!!! %u %u\n", cmbr->nr_cached_tentries, cenv->max_cached_tentries);
        }
        //BUG_ON(cmbr->nr_cached_tentries > cenv->max_cached_tentries);

#ifdef STORE_KEY_FP
        BUG_ON(true);
#endif
    }

    NVMEV_DEBUG("allocs took %llu ns\n", total);
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

inline uint64_t __num_cached(struct cmt_struct* cmt) {
    uint64_t ret = CACHE_GRAIN * ((cmt->cached_cnt / CACHE_GRAIN) + 1);
    return ret;
    //uint64_t start = local_clock();
    //for(int i = 0; i < EPP; i++) {
    //    if(cmt->pt[i].ppa != U64_MAX) {
    //        ret++;
    //    }
    //}
    //uint64_t end = local_clock();
    //NVMEV_DEBUG("num_cached took %llu ns\n", end - start);
    //return ret;
}

inline uint64_t __cmt_real_size(struct cmt_struct *cmt) {
    //NVMEV_DEBUG("cached_cnt IDX %u %u\n", cmt->idx, cmt->cached_cnt);
    return cmt->cached_cnt * (sizeof(uint64_t) * 2);
    uint64_t ret = 0;

    BUG_ON(!cmt);
    BUG_ON(!cmt->pt);

    uint64_t start = local_clock();
    for(int i = 0; i < EPP; i++) {
        if(cmt->pt[i].ppa != U64_MAX) {
            ret += sizeof(uint64_t) * 2;
        }
    }
    uint64_t end = local_clock();
    NVMEV_DEBUG("real_size took %llu ns\n", end - start);

    return ret;
}

bool __should_sample(void) {
    uint32_t rand;
    get_random_bytes(&rand, sizeof(rand));
    if(rand % 100 > 95) {
        return true;
    }
    return false;
}



struct victim_entry {
    uint64_t lpa;
    uint64_t ppa;
    struct cmt_struct *pt;
};

struct victim_entry *victims;
void __collect_victims(struct conv_ftl *conv_ftl, LRU* lru, 
                       struct victim_entry **_v, uint64_t *cnt) { 
    struct ssdparams *spp = &d_member.ssd->sp;
    void* item = NULL;
    uint64_t count = 0, clean_count = 0;
    uint64_t loop_cnt = 0;
    uint64_t remaining = spp->pgsz;
    uint64_t cached;

    uint64_t start = local_clock();
    uint64_t inner = 0;

    bool sample = false;
    if(__should_sample()) {
        sample = true;
    }

    while((item = lru_pop(lru)) != NULL) {
        struct cmt_struct *pt = (struct cmt_struct*) item;
        cached = __num_cached(pt);

        if(pt->state == CLEAN) {
            NVMEV_DEBUG("1 Cached cnt of IDX %u %llu\n", pt->idx, cached);
            clean_count += cached;
            cmbr->nr_cached_tentries -= cached;
            pt->cached_cnt = 0;
            pt->lru_ptr = NULL;
            kfree((char*) pt->pt);
            pt->pt = NULL;
            continue;
        }

        uint64_t size = cached * (sizeof(uint64_t) * 2);
        uint64_t incr = sizeof(uint64_t) * 2;
        uint64_t start_lpa = pt->idx * EPP;
        uint64_t ppa, lpa;

        if(size <= remaining) {
            NVMEV_DEBUG("Cached cnt of IDX %u %llu\n", pt->idx, cached);
            BUG_ON(count >= EPP);
            cmbr->nr_cached_tentries -= cached;
            remaining -= incr * cached;

            uint64_t start_inner = local_clock();
            for(uint64_t i = 0; i < cached; i++) {
                if(i == 0) {
                    NVMEV_ASSERT(pt->pt[i].lpa != U64_MAX);
                }

                lpa = pt->pt[i].lpa;
                ppa = pt->pt[i].ppa;

                NVMEV_DEBUG("CHECK IDX %u LPA %llu PPA %llu pos %llu\n", 
                        pt->idx, lpa, ppa, i);  

                //BUG_ON(ppa == U64_MAX);
                //if(ppa != U64_MAX) {
                ppa = ppa;
                NVMEV_INFO("Copying %llu %llu to victim list from idx %u.\n",
                        start_lpa + i, ppa, pt->idx);
                victims[count + i].lpa = lpa;
                victims[count + i].ppa = ppa;
                //}
            }

            count += cached;

            uint64_t end_inner = local_clock();
            inner += (end_inner - start_inner);

            pt->cached_cnt = 0;
            pt->state = CLEAN;
            NVMEV_INFO("Marking PT IDX %u clean in victims.\n", pt->idx);
            pt->lru_ptr = NULL;
            kfree((char*) pt->pt);
            pt->pt = NULL;
        } else {
            pt->lru_ptr = lru_push(cmbr->lru, (void *) pt);
            //NVMEV_DEBUG("Skipping IDX %u because it was bigger than what we had left (%llu %llu).\n", 
            //            pt->idx, size, remaining);
            //break;
        }

        if(remaining == 0 || loop_cnt++ == 32) {
            goto end;
        }
    }

end:
    BUG_ON(count == 0 && clean_count == 0);
    NVMEV_DEBUG("Collected %llu (%llu) victims %llu loops.\n", 
                count, clean_count, loop_cnt);
    if(count == 0) {
        NVMEV_DEBUG("Had clean count %llu\n", clean_count);
    }
    NVMEV_ASSERT(count * (sizeof(uint64_t*) * 2) <= spp->pgsz);

    //if(sample) {
        //uint64_t end = local_clock();
        //NVMEV_DEBUG("Took %llu ns to collect victims. %llu ns spent in inner loops\n", 
        //        end - start, inner);
    //}

    *cnt = count;
}

bool cg_no_room(void) {
    if(cmbr->nr_cached_tentries == 0) {
        return false;
    } else {
        NVMEV_DEBUG("%s we had %u entries, one more page and one more entry  will be "
                    "%u entries (max %u).\n", 
                    __func__, cmbr->nr_cached_tentries, cmbr->nr_cached_tentries + (4096 / 16) + CACHE_GRAIN,
                    cenv->max_cached_tentries);
        return ((cmbr->nr_cached_tentries + ((4096 * 1) / 16) + CACHE_GRAIN) > cenv->max_cached_tentries);
    }
}

inline bool __dont_have_enough(void) {
    struct ssdparams *spp = &d_member.ssd->sp;
    return (cmbr->nr_cached_tentries + (((spp->pgsz * CACHE_GRAIN) / (sizeof(uint64_t) * 2)) + 1)) >
           cenv->max_cached_tentries;
}

inline bool __have_enough(void) {
    struct ssdparams *spp = &d_member.ssd->sp;
    return (cmbr->nr_cached_tentries + (((spp->pgsz * CACHE_GRAIN) / (sizeof(uint64_t) * 2)) + 1)) <
           cenv->max_cached_tentries;
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
    bool special = false;

    bool sample = false;
    uint64_t get = 0, alloc = 0, mcpy = 0, final = 0, start = 0, write = 0, queue = 0;

    uint32_t rand;
    get_random_bytes(&rand, sizeof(rand));
    if(rand % 100 > 95) {
        sample = true;
    }

	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct cmt_struct *victim = NULL;

    NVMEV_DEBUG("list_up for IDX %llu.\n", IDX(lpa));

	struct inflight_params *i_params;

    if(sample) {
        start = local_clock();
    }

again:
	if (cg_is_full() || cg_no_room()) {
        NVMEV_DEBUG("%s lpa %llu translation cache full because %s.\n", __func__, lpa,
                   cg_is_full() ? "cg_is_full" : "cg_no_room");

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

                NVMEV_DEBUG("Evicting %llu entries from CLEAN IDX %u\n", 
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
                NVMEV_DEBUG("But we still need to evict more. %u remaining. %llu evicted\n",
                            cmbr->nr_cached_tentries, evicted);
                goto again;
            }
        } else {
            start = local_clock();
            victims = (struct victim_entry*) 
                      kzalloc(EPP * sizeof(struct victim_entry), GFP_KERNEL);
            //NVMEV_ASSERT(victims);
            __collect_victims(ftl, cmbr->lru, &victims, &cnt);

            if(cnt == 0) {
                /*
                 * We evicted all clean items.... or we have a terrible bug.
                 */
                kfree(victims);
                goto again;
            }

            if(sample) {
                get = local_clock();
            }

            i_params = get_iparams(req, wb_entry);
            i_params->jump = GOTO_COMPLETE;

            struct ppa p = get_new_page(ftl, MAP_IO);
            uint64_t ppa = ppa2pgidx(ftl, &p);
            uint64_t grains = (cnt * sizeof(uint64_t) * 2) / GRAIN_PER_PAGE;

            advance_write_pointer(ftl, MAP_IO);
            mark_page_valid(ftl, &p);
            mark_grain_valid(ftl, PPA_TO_PGA(ppa, 0), grains);

            if(grains != GRAIN_PER_PAGE) {
                uint64_t offset = grains;
                mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), GRAIN_PER_PAGE - offset);
                mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), GRAIN_PER_PAGE - offset);
            }

            value_set *_value_mw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
            _value_mw->ssd = d_member.ssd;

            if(sample) {
                alloc = local_clock();
            }

            uint64_t step = sizeof(uint64_t) * 2;
            uint8_t *ptr = _value_mw->value;
            for(int i = 0; i < cnt; i++) {
                uint64_t lpa = victims[i].lpa;
                uint64_t m_ppa = victims[i].ppa;

                if(lpa != U64_MAX) {
                    NVMEV_INFO("In memcpy for LPA %llu PPA %llu IDX %llu going to PPA %llu\n", 
                            lpa, m_ppa, IDX(lpa), ppa);
                }
 
                memcpy(ptr + (i * step), &lpa, sizeof(lpa));
                memcpy(ptr + (i * step) + sizeof(uint64_t), &m_ppa, sizeof(m_ppa));
            }

            NVMEV_DEBUG("Passed memcpy.\n");

            if(sample) {
                mcpy = local_clock();
            }

            if(!wrote) {
                nsecs = __demand.li->write(ppa, PAGESIZE, _value_mw, ASYNC, 
                        make_algo_req_rw(MAPPINGW, _value_mw, 
                            req, wb_entry));
                wrote = true;
            } else {
                nsecs = __demand.li->write(ppa, PAGESIZE, _value_mw, ASYNC, NULL);
                kfree(_value_mw->value);
                kfree(_value_mw);
                NVMEV_DEBUG("Loop 2\n");
            }

            NVMEV_DEBUG("Passed write.\n");

            uint8_t oob_idx = 0;
            uint64_t last_idx = U64_MAX;
            for(int i = 0; i < cnt; i++) {
                if(victims[i].lpa == U64_MAX) {
                    continue;
                }

                //NVMEV_INFO("IDX %llu LPA %llu gets PPA %llu\n", 
                //            victims[i].lpa / EPP, victims[i].lpa, ppa);

                uint64_t idx = victims[i].lpa / EPP;
                struct cmt_struct *cmt = cmbr->cmt[idx];
                //cstat->dirty_evict++;

                if(idx == last_idx) {
                    continue;
                }

                cmt->grain = oob_idx;
                oob[ppa][oob_idx++] = victims[i].lpa;
                NVMEV_INFO("IDX %llu (CC %u) gets grain %u PPA %llu LPA %llu\n", 
                            idx, cmt->cached_cnt, oob_idx - 1, ppa, victims[i].lpa);

                uint64_t g_per_cg = (CACHE_GRAIN * sizeof(uint64_t) * 2) / GRAINED_UNIT;
                for(int i = 1; i < ((cmt->cached_cnt / CACHE_GRAIN) + 1) * g_per_cg; i++) {
                    oob[ppa][oob_idx++] = U64_MAX - 1;
                }

                cmt->len_on_disk = ((cmt->cached_cnt / CACHE_GRAIN) + 1) * g_per_cg;
                cmt->t_ppa = ppa;
                last_idx = idx;
            }

            if(oob_idx < GRAIN_PER_PAGE) {
                oob[ppa][oob_idx] = U64_MAX;
            }

            kfree(victims);

            rc = 1;
            (*credits) += GRAIN_PER_PAGE;

            NVMEV_DEBUG("Evicting %llu items. %u remaining.\n", cnt, cmbr->nr_cached_tentries);

            cstat->dirty_evict++;
            evicted += cnt;

            if(sample) {
                final = local_clock();
            }

            final = local_clock();

            if(__dont_have_enough()) {
                NVMEV_DEBUG("But we still need to evict more. %u remaining. %llu evicted\n",
                            cmbr->nr_cached_tentries, evicted);
                goto again;
            }
        }
        NVMEV_DEBUG("Exiting victim loop.\n");
	} 

    nsecs_latest = max(nsecs_latest, nsecs);

    NVMEV_ASSERT(cmt->idx == IDX(lpa));

    if(!cmt->lru_ptr) {
        cmt->lru_ptr = lru_push(cmbr->lru, (void *) cmt);
    }

    if(cmt->t_ppa != U64_MAX && cmt->pt != NULL) {
        NVMEV_ASSERT(!cmt->is_flying);
    }

    if(cmt->t_ppa != U64_MAX && cmt->pt == NULL) {
        if(wb_entry && wb_entry->mapping_v) {
            inf_free_valueset(wb_entry->mapping_v, FS_MALLOC_W);
        } else if(req && req->mapping_v) {
            inf_free_valueset(req->mapping_v, FS_MALLOC_W);
        }

        if(cmt->pt == NULL) {
            NVMEV_DEBUG("Building mapping PPA %llu for LPA %llu IDX %llu\n", 
                    cmt->t_ppa, lpa, IDX(lpa));
            /*
             * This CMT was written above.
             */

            value_set *v = inf_get_valueset(NULL, FS_MALLOC_R, spp->pgsz);
            v->ssd = d_member.ssd;
            nsecs = __demand.li->read(cmt->t_ppa, PAGESIZE, v, ASYNC, NULL);
            cmt->is_flying = true;

            if(req) {
                req->mapping_v = v;
            } else {
                wb_entry->mapping_v = v;
            }

            if(sample) {
                write = local_clock();
            }
        }
    } 
    
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
        NVMEV_DEBUG("Passed flying check LPA %llu PPA %llu IDX %llu %s.\n", 
                    lpa, cmt->t_ppa, IDX(lpa), __func__);
		cmt->is_flying = false;

		if (req) {
            NVMEV_ASSERT(req->mapping_v);
            __page_to_ptes(req->mapping_v, IDX(lpa), true);

            //if(__pt_contains(cmt, lpa, U64_MAX)) {
            //    cmbr->nr_cached_tentries--;
            //    NVMEV_DEBUG("1Decr IDX %llu.\n", IDX(lpa));
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
            NVMEV_INFO("pagetoptes triggered for IDX %llu PPA %llu\n", IDX(lpa), cmt->t_ppa);
            __page_to_ptes(wb_entry->mapping_v, IDX(lpa), true);

            //if(__pt_contains(cmt, lpa, U64_MAX)) {
            //    cmbr->nr_cached_tentries--;
            //    NVMEV_DEBUG("1Decr IDX %llu.\n", IDX(lpa));
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

    if(sample) {
        queue = local_clock();
    }

    if(nsecs_completed) {
        *nsecs_completed = nsecs_latest;
    }

    uint64_t end = local_clock();

    if(final - start > 0 && final - start < 10000000) {
        NVMEV_DEBUG("list_up inner part took %llu ns\n", final - start);
    }

    if(sample) {
        NVMEV_DEBUG("list_up took %llu ns (%llu %llu %llu %llu %llu %llu)\n", 
                   end - start, get - start, alloc - get, mcpy - alloc,
                   final - mcpy, write - final, queue - write);
        if((nsecs_latest > 0) && (nsecs_latest < queue)) {
            NVMEV_DEBUG("!!!!!!! list_up took longer than the flash IO !!!!!!!\n");
        }
    }

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
    //NVMEV_DEBUG("Update for IDX %llu\n", IDX(lpa));
	lru_update(cmbr->lru, cmt->lru_ptr);
	return 0;
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

    //printk("%s: For PPA %llu we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

int cg_update(lpa_t lpa, struct pt_struct pte) {
    struct ssdparams *spp = &d_member.ssd->sp;
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("cg_update pte ppa %u for lpa %u\n", pte.ppa, lpa);

	if (cmt->pt) {
        NVMEV_DEBUG("cg_update %llu %llu\n", lpa, pte.ppa);
        _update_pt(cmt, lpa, pte.ppa);
		//cmt->pt[OFFSET(lpa)] = pte;

		if (!IS_INITIAL_PPA(cmt->t_ppa) && cmt->state == CLEAN) {
            /*
             * Only safe if assuming battery-backed DRAM.
             */

            /* update corresponding page status */
            NVMEV_INFO("Marking mapping PPA %llu grain %llu IDX %llu "
                    "invalid as it was dirtied in memory.\n",
                    cmt->t_ppa, cmt->grain, IDX(lpa));

            uint64_t g_per_cg = (CACHE_GRAIN * sizeof(uint64_t) * 2) / GRAINED_UNIT;

            mark_grain_invalid(ftl, PPA_TO_PGA(cmt->t_ppa, cmt->grain), 
                               cmt->len_on_disk);
        }

		cmt->state = DIRTY;
        //NVMEV_DEBUG("1 Update for IDX %llu\n", IDX(lpa));
		lru_update(cmbr->lru, cmt->lru_ptr);
	} else {
        NVMEV_INFO("2 Hit this case for IDX %llu!!\n", IDX(lpa));
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
        if(__pt_contains(cmt, lpa, U64_MAX)) {
            NVMEV_DEBUG("Hit for IDX %llu! %u cached! %u cached total! PPA is %llu!\n", 
                    IDX(lpa), cmt->cached_cnt, cmbr->nr_cached_tentries, __get_pte(cmt, lpa).ppa);
            BUG_ON(__get_pte(cmt, lpa).ppa == U64_MAX);
            BUG_ON(cmt->lru_ptr == NULL);
            cstat->cache_hit++;
            return 1;
        } else if (1 || cmt->cached_cnt % CACHE_GRAIN) {
            /*
             * There's room for another entry.
             */
            cstat->cache_hit++;
            return 1;
        } else {
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

    NVMEV_INFO("Getting PTE for IDX %llu\n", IDX(lpa));
	if (cmt->pt) {
        return __get_pte(cmt, lpa);
        //printk("%s returning %u for lpa %u\n", __func__, cmt->pt[OFFSET(lpa)].ppa, lpa);
		return cmt->pt[OFFSET(lpa)];
	} else {
        if(cmt->t_ppa == U64_MAX) {
            /*
             * Haven't used this CMT entry yet.
             */

            __alloc_pt(IDX(lpa));
           
            cmbr->nr_cached_tentries += CACHE_GRAIN;
            return __get_pte(cmt, lpa);
            //return cmt->pt[OFFSET(lpa)];
        } else {
            BUG_ON(true);
            NVMEV_INFO("Hit this case for IDX %llu reading %llu!!\n", 
                        IDX(lpa), cmt->t_ppa);

            value_set *v = inf_get_valueset(NULL, FS_MALLOC_R, spp->pgsz);
            v->ssd = d_member.ssd;
            __demand.li->read(cmt->t_ppa, PAGESIZE, v, ASYNC, NULL);
            __page_to_ptes(v, IDX(lpa), true);

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
