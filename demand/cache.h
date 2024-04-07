/*
 * Header for Cache module
 */

#ifndef __DEMAND_CACHE_H__
#define __DEMAND_CACHE_H__

#include "./include/data_struct/lru_list.h"
#include "demand.h"

#define CACHE_GRAIN 128

/* Structures */
struct cache_env {
	cache_t c_type;

	int nr_tpages_optimal_caching;
	int nr_valid_tpages;
	int nr_valid_tentries;

	float caching_ratio;
	int max_cached_tpages;
	int max_cached_tentries;

    int pftl_memory;

	/* add attributes here */
};

struct cache_member {
	struct cmt_struct **cmt;
	LRU *lru;
    struct fifo *fifo;

	int nr_cached_tpages;
	int nr_cached_tentries;
};

struct cache_stat {
	/* cache performance */
	uint64_t cache_hit;
	uint64_t cache_miss;
	uint64_t clean_evict;
	uint64_t dirty_evict;
	uint64_t blocked_miss;

	/* add attributes here */
};


struct demand_cache {
	int (*create) (struct demand_shard*, cache_t c_type);
	int (*destroy) (struct demand_cache*);

	int (*load) (struct demand_shard*, lpa_t lpa, request *const req, 
                 snode *wb_entry, uint64_t*, uint64_t);
	int (*list_up) (struct demand_shard*,lpa_t lpa, request *const req, 
                    snode *wb_entry, uint64_t*, uint64_t*, uint64_t);
	int (*wait_if_flying) (lpa_t lpa, request *const req, snode *wb_entry);

	int (*touch) (struct demand_cache*, lpa_t lpa);
	int (*update) (struct demand_shard*, lpa_t lpa, struct pt_struct pte);

	struct pt_struct (*get_pte) (struct demand_shard*, lpa_t lpa);
    struct cmt_struct *(*get_cmt_x) (struct demand_cache*, lpa_t lpa);
	struct cmt_struct *(*get_cmt) (struct demand_cache*, lpa_t lpa);

	bool (*is_hit) (struct demand_cache*, lpa_t lpa);
	bool (*is_full) (struct demand_cache*);

	struct cache_env env;
	struct cache_member member;
	struct cache_stat stat;
};

extern struct demand_cache cg_cache;
extern struct demand_cache *cgo_cache[SSD_PARTITIONS];
extern struct demand_cache fg_cache;

/* Functions */
struct demand_cache *select_cache(struct demand_shard *shard, cache_t type);
void clear_cache_stat(uint32_t id);
uint32_t get_cache_stat(uint32_t id, char* out);
void print_cache_stat(struct cache_stat *_stat);
struct cache_stat* get_cstat(uint32_t id);

#endif
