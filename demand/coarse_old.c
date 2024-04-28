/*
 * Coarse-grained Cache (default)
 */

#include "cache.h"
#include "coarse_old.h"
#include "../fifo.h"
#include "utility.h"

#include "./interface/interface.h"

#include <linux/jiffies.h> 
#include <linux/sched/clock.h>

extern struct algorithm __demand;
extern struct demand_stat d_stat;

struct demand_cache *cgo_cache[SSD_PARTITIONS];
static struct cache_stat *cstat;

static void print_cache_env(struct demand_shard const *shard) {
    struct cache_env *const _env = &shard->cache->env;

    NVMEV_DEBUG("\n");
    NVMEV_DEBUG(" |---------- Demand Cache Log: Coarse-grained Cache\n");
    NVMEV_DEBUG(" | Total trans pages:        %d\n", _env->nr_valid_tpages);
    //NVMEV_DEBUG(" | Caching Ratio:            %0.3f%%\n", _env->caching_ratio * 100);
    NVMEV_DEBUG(" | Caching Ratio:            same as PFTL\n");
    NVMEV_DEBUG(" |  - Max cached tpages:     %d (%lu pairs)\n",
            _env->max_cached_tpages, _env->max_cached_tpages * EPP);
    //NVMEV_DEBUG(" |  (PageFTL cached tpages:  %d)\n", _env->nr_tpages_optimal_caching);
    NVMEV_DEBUG(" |---------- Demand Cache Log END\n");
    NVMEV_DEBUG("\n");
}

static void cgo_env_init(struct demand_shard const *shard, cache_t c_type,
                         struct cache_env *const _env) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct demand_env *d_env = shard->env;
    uint64_t capa, dram;

    _env->c_type = c_type;

    _env->nr_tpages_optimal_caching = d_env->nr_pages * 4 / spp->pgsz;

    _env->max_cached_tpages = shard->dram / spp->pgsz;
    _env->max_cached_tentries = shard->dram / GRAINED_UNIT;

    _env->nr_valid_tpages = (d_env->nr_pages * GRAIN_PER_PAGE) / EPP;
    _env->nr_valid_tentries = (d_env->nr_pages * GRAIN_PER_PAGE);

#ifndef GC_STANDARD
    if((d_env->nr_pages * GRAIN_PER_PAGE) % EPP) {
        _env->nr_valid_tpages++;
        _env->nr_valid_tentries += GRAIN_PER_PAGE;
    }
#endif

    NVMEV_ERROR("nr pages %u Valid tpages %u tentries %u\n", 
            d_env->nr_pages, _env->nr_valid_tpages, _env->nr_valid_tentries);

    print_cache_env(shard);
}

static void cgo_member_init(struct demand_shard *shard) { 
    struct demand_cache *cache = shard->cache;
    struct cache_member *_member = &cache->member;
    struct cache_env *cenv = &cache->env;
    struct demand_env *d_env = shard->env;
    struct cmt_struct **cmt =
    (struct cmt_struct **)vmalloc(cenv->nr_valid_tpages * sizeof(struct cmt_struct *));

    NVMEV_ERROR("Allocated CMT %p member %p cache %p %u pages\n", 
                 cmt, _member, cache, cenv->nr_valid_tpages);

    for (int i = 0; i < cenv->nr_valid_tpages; i++) {
        cmt[i] = (struct cmt_struct *)kzalloc(sizeof(struct cmt_struct), GFP_KERNEL);
        NVMEV_ASSERT(cmt[i]);

        atomic_set(&cmt[i]->t_ppa, UINT_MAX);
        atomic_set(&cmt[i]->outgoing, 0);

        cmt[i]->idx = i;
        cmt[i]->pt = NULL;
        cmt[i]->lru_ptr = NULL;
        cmt[i]->state = CLEAN;
        cmt[i]->mems = kzalloc(sizeof(void*) * EPP, GFP_KERNEL);
        cmt[i]->len_on_disk = 0;

        NVMEV_ASSERT(cmt[i]->mems);
    }
    _member->cmt = cmt;

    fifo_init(&_member->fifo);
    _member->nr_cached_tpages = 0;
}

static void cgo_stat_init(struct cache_stat *const _stat) {

}

int cgo_create(struct demand_shard *shard, cache_t c_type) {
    struct demand_cache *dc = shard->cache;

    dc->create = cgo_create;
    dc->destroy = cgo_destroy;
    dc->touch = cgo_touch;
    dc->update = cgo_update;
    dc->get_pte = cgo_get_pte;
    dc->get_cmt = cgo_get_cmt;
    dc->is_hit = cgo_is_hit;
    dc->is_full = cgo_is_full;

    cstat = &dc->stat;

    cgo_env_init(shard, c_type, &dc->env);
    cgo_member_init(shard);
    cgo_stat_init(&dc->stat);

    return 0;
}

static void cgo_print_member(void) {
    //NVMEV_DEBUG("=====================");
    //NVMEV_DEBUG(" Cache Finish Status ");
    //NVMEV_DEBUG("=====================");

    //NVMEV_DEBUG("Max Cached tpages:     %d\n", cenv->max_cached_tpages);
    //NVMEV_DEBUG("Current Cached tpages: %d\n", cmbr->nr_cached_tpages);
    //NVMEV_DEBUG("\n");
}

static void cgo_member_kfree(struct demand_cache *cache) {
    struct cache_member *_member = &cache->member;
    struct cache_env *cenv = &cache->env;

    for (int i = 0; i < cache->env.nr_valid_tpages; i++) {
        struct cmt_struct *cmt = _member->cmt[i];
        for(int i = 0; i < EPP; i++) {
            if(cmt->mems[i]) {
                kfree(cmt->mems[i]);
                cmt->mems[i] = NULL;
            }
        }

        if(cmt->pt_mem) {
            kfree(cmt->pt_mem);
        }

        kfree(cmt->mems);
        kfree(cmt);
    }
    vfree(_member->cmt);
    fifo_destroy(_member->fifo);
}

int cgo_destroy(struct demand_cache *cache) {
    print_cache_stat(cstat);
    cgo_print_member();
    cgo_member_kfree(cache);
    return 0;
}

int cgo_touch(struct demand_cache *cache, lpa_t lpa) {
    return 0;
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    //lru_update(cmbr->lru, cmt->lru_ptr);
    return 0;
}

int cgo_update(struct demand_shard *shard, lpa_t lpa, struct pt_struct pte) {
    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    if (cmt->pt) {
        NVMEV_DEBUG("Setting LPA %u to PPA %u FP %u in update.\n", lpa, pte.ppa, pte.key_fp);
        cmt->pt[OFFSET(lpa)] = pte;
        cmt->state = DIRTY;
        lru_update(cmbr->lru, cmt->lru_ptr);
    } else {
        BUG_ON(true);
        /* FIXME: to handle later update after evict */
        //cmbr->mem_table[IDX(lpa)][OFFSET(lpa)] = pte;

        //static int cnt = 0;
        //if (++cnt % 10240 == 0) //NVMEV_DEBUG("cgo_update %d\n", cnt);
        ////NVMEV_DEBUG("cgo_update %d\n", ++cnt);
    }
    return 0;
}

bool cgo_is_hit(struct demand_cache *cache, lpa_t lpa) {
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    if (cmt->pt != NULL) {
        return 1;
    } else {
        return 0;
    }
}

bool cgo_is_full(struct demand_cache* cache) {
    struct cache_member *cmbr = &cache->member;

    //NVMEV_ERROR("CMT %u cached %u max\n", 
    //            cmbr->nr_cached_tentries, cache->env.max_cached_tentries);

    return (cmbr->nr_cached_tentries >= cache->env.max_cached_tentries);
}

struct pt_struct cgo_get_pte(struct demand_shard *shard, lpa_t lpa) {
    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    while(atomic_read(&cmt->outgoing) > 0) {
        cpu_relax();
    }

    if (cmt->pt) {
        NVMEV_DEBUG("%s returning %u for LPA %u IDX %lu shard %llu cmt %p\n", 
                    __func__, cmt->pt[OFFSET(lpa)].ppa, lpa, IDX(lpa), shard->id,
                    cmt);
        return cmt->pt[OFFSET(lpa)];
    } else {
        NVMEV_ASSERT(false);
    }
}

struct cmt_struct *cgo_get_cmt(struct demand_cache *cache, lpa_t lpa) {
    struct cache_member *cmbr = &cache->member;

    if(IDX(lpa) >= cache->env.nr_valid_tpages) {
        NVMEV_ERROR("WTF!!! IDX %lu LPA %u v %u\n", 
                     IDX(lpa), lpa, cache->env.nr_valid_tpages);
    }
    NVMEV_ASSERT(IDX(lpa) < cache->env.nr_valid_tpages);

	struct cmt_struct *c = cmbr->cmt[IDX(lpa)];
    unsigned long start = jiffies;

    if(!c) {
        NVMEV_ERROR("WTF!!! IDX %u LPA %u\n", c->idx, lpa);
    }

	while (atomic_cmpxchg(&c->outgoing, 0, 1) != 0) {
		cpu_relax();

		if (time_after(jiffies, start + HZ)) {
			NVMEV_ERROR("Stuck waiting for CMT IDX %u\n", 
						 c->idx);
			start = jiffies;
		}
	}

    return c;
}
