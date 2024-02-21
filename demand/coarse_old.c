/*
 * Coarse-grained Cache (default)
 */

#include "cache.h"
#include "coarse_old.h"
#include "utility.h"
#include "./interface/interface.h"

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
    _env->nr_valid_tpages = (d_env->nr_pages / EPP) + ((d_env->nr_pages % EPP) ? 1 : 0);
    _env->nr_valid_tentries = _env->nr_valid_tpages * EPP;

    _env->max_cached_tpages = shard->dram / spp->pgsz;
    _env->max_cached_tentries = 10;//shard->dram / GRAINED_UNIT;

#ifdef DVALUE
    _env->nr_valid_tpages *= GRAIN_PER_PAGE / 2;
    _env->nr_valid_tentries *= GRAIN_PER_PAGE / 2;
#endif

    NVMEV_DEBUG("nr pages %u Valid tpages %u tentries %u\n", 
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

    NVMEV_DEBUG("Allocated CMT %p member %p cache %p %u pages\n", 
            cmt, _member, cache, cenv->nr_valid_tpages);

    for (int i = 0; i < cenv->nr_valid_tpages; i++) {
        cmt[i] = (struct cmt_struct *)kzalloc(sizeof(struct cmt_struct), GFP_KERNEL);

        cmt[i]->t_ppa = UINT_MAX;
        cmt[i]->idx = i;
        cmt[i]->pt = NULL;
        cmt[i]->lru_ptr = NULL;
        cmt[i]->state = CLEAN;
    }
    _member->cmt = cmt;

    lru_init(&_member->lru);

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
    for (int i = 0; i < cache->env.nr_valid_tpages; i++) {
        kfree(_member->cmt[i]);
    }
    vfree(_member->cmt);

    //for (int i = 0; i < cenv->nr_valid_tpages; i++) {
    //      kfree(_member->mem_table[i]);
    //}
    //vfree(_member->mem_table);

    lru_kfree(_member->lru);
}

int cgo_destroy(struct demand_cache *cache) {
    print_cache_stat(cstat);

    cgo_print_member();

    cgo_member_kfree(cache);
    return 0;
}

int cgo_touch(struct demand_cache *cache, lpa_t lpa) {
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
    lru_update(cmbr->lru, cmt->lru_ptr);
    return 0;
}

int cgo_update(struct demand_shard *shard, lpa_t lpa, struct pt_struct pte) {
    struct demand_cache *cache = shard->cache;
    struct cache_member *cmbr = &cache->member;
    struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    printk("cgo_update pte ppa %u for lpa %u\n", pte.ppa, lpa);

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

    //NVMEV_INFO("CMT %u cached %u max\n", 
    //            cmbr->nr_cached_tentries, cache->env.max_cached_tentries);

    return (cmbr->nr_cached_tentries >= cache->env.max_cached_tentries);
    return (cmbr->nr_cached_tpages >= cache->env.max_cached_tpages);
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
        if(cmt->t_ppa == UINT_MAX) {
            NVMEV_ASSERT(false);
            printk("%s CMT was NULL for LPA %u IDX %lu\n", 
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
            NVMEV_INFO("Failing for LPA %u IDX %lu\n", lpa, IDX(lpa));
            BUG_ON(true);
        }
        /* FIXME: to handle later update after evict */
        //return cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];
    }
    /*      if (unlikely(cmt->pt == NULL)) {
    //NVMEV_DEBUG("Should have aborted!!!! %s:%d
    , __FILE__, __LINE__);;
    }
    return cmt->pt[OFFSET(lpa)]; */
}

struct cmt_struct *cgo_get_cmt(struct demand_cache *cache, lpa_t lpa) {
    struct cache_member *cmbr = &cache->member;

    struct cmt_struct *c = cmbr->cmt[IDX(lpa)];
    while(atomic_read(&c->outgoing) > 0) {
        cpu_relax();
    }

    return c;
}
