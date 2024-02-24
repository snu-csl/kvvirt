/*
 * Demand-based FTL Main Header
 */

#ifndef __DEMAND_H__
#define __DEMAND_H__

#include "../nvmev.h"
#include "../ssd.h"
#include "../demand_ftl.h"

#include "d_type.h"
#include "d_param.h"
#include "./interface/queue.h"
#include "./include/container.h"
#include "./include/dl_sync.h"
#include "./Lsmtree/skiplist.h"
#include "./include/data_struct/lru_list.h"

#include <linux/highmem.h>
#include <linux/sched/clock.h>
#include <linux/vmalloc.h>

extern uint64_t user_pgs_this_gc;
extern uint64_t gc_pgs_this_gc;
extern uint64_t map_pgs_this_gc;
extern uint64_t map_gc_pgs_this_gc;
extern uint64_t pgs_this_flush;

struct lpa_len_ppa {
    lpa_t lpa; /* the lpa we find in the reverse map. */
    uint32_t len; /* the length of the key-value pair. */
    ppa_t prev_ppa; /* to copy from during GC */
    ppa_t new_ppa; /* the new ppa we're writing this key-value pair to. */
    ppa_t cmt_ppa; /* which CMT ppa does this item belong to? */
};

void clear_oob(struct demand_shard *shard, uint64_t pgidx);
bool oob_empty(struct demand_shard *shard, uint64_t pgidx);
struct ppa get_new_page(struct demand_shard *demand_shard, uint32_t io_type);
ppa_t ppa2pgidx(struct demand_shard *demand_shard, struct ppa *ppa);
bool advance_write_pointer(struct demand_shard *demand_shard, uint32_t io_type);
void mark_page_valid(struct demand_shard *demand_shard, struct ppa *ppa);
void mark_page_invalid(struct demand_shard *demand_shard, struct ppa *ppa);
void mark_grain_valid(struct demand_shard *demand_shard, uint64_t grain, uint32_t len);
void mark_grain_invalid(struct demand_shard *demand_shard, uint64_t grain, uint32_t len);
inline void consume_write_credit(struct demand_shard *demand_shard, uint32_t len);
inline uint64_t check_and_refill_write_credit(struct demand_shard *demand_shard);
int do_bulk_mapping_update_v(struct demand_shard*, struct lpa_len_ppa*, int, 
                             uint64_t*, uint64_t);
inline struct line *get_line(struct demand_shard *demand_shard, struct ppa *ppa);
inline bool last_pg_in_wordline(struct demand_shard *demand_shard, struct ppa *ppa);

extern struct demand_stat d_stat;

/* Structures */
// Page table entry
struct pt_struct {
#ifndef GC_STANDARD
    lpa_t lpa;
#endif
	ppa_t ppa; // Index = lpa
#ifdef STORE_KEY_FP
	fp_t key_fp;
#endif
};

// Cached mapping table
struct cmt_struct {
	int32_t idx;
	struct pt_struct *pt;
	NODE *lru_ptr;
	ppa_t t_ppa;

	cmt_state_t state;

    uint64_t g_off;
    uint16_t len_on_disk;
#ifndef GC_STANDARD
	uint32_t cached_cnt;
#endif

    atomic_t outgoing;
};

struct hash_params {
	uint32_t hash;
#ifdef STORE_KEY_FP
	fp_t key_fp;
#endif
	int cnt;
	int find;
	lpa_t lpa;

#ifdef DVALUE
	int fl_idx;
#endif
};

struct demand_params{
    struct demand_shard *shard;
	value_set *value;
	snode *wb_entry;
	//struct cmt_struct *cmt;
	dl_sync *sync_mutex;
	int offset;
};

struct inflight_params{
	jump_t jump;
	//struct pt_struct pte;
};

struct flush_node {
    ppa_t ppa;
	value_set *value;
};

struct flush_list {
	int size;
	struct flush_node *list;
};


/* Wrapper structures */
struct demand_env {
	int nr_pages;
	int nr_blocks;

	int nr_tblks;
	int nr_tpages;
	int nr_dblks;
	int nr_dpages;

	volatile uint64_t wb_flush_size;

#if defined(HASH_KVSSD) && defined(DVALUE)
	int nr_grains;
	int nr_dgrains;
#endif

	int cache_id;
	float caching_ratio;

    unsigned long long size;
};

struct demand_member {
	struct mutex op_lock;

	LRU *lru;
	skiplist *write_buffer;
	snode **sorted_list;

	queue *flying_q;
	queue *blocked_q;
	queue *wb_master_q;
	queue *wb_retry_q;

	queue *range_q;

	struct flush_list *flush_list;

	volatile int nr_valid_read_done;
	volatile int nr_tpages_read_done;

	struct d_htable *hash_table;

#ifdef HASH_KVSSD
	int max_try;
#endif

#ifdef GC_STANDARD
    bool* grain_bitmap;
#endif
};

struct demand_stat {
	/* device traffic */
	uint64_t data_r;
	uint64_t data_w;
	uint64_t trans_r;
	uint64_t trans_w;
	uint64_t data_r_dgc;
	uint64_t data_w_dgc;
	uint64_t trans_r_dgc;
    uint64_t trans_r_dgc_2;
	uint64_t trans_w_dgc;
	uint64_t trans_r_tgc;
	uint64_t trans_w_tgc;

	/* gc trigger count */
	uint64_t dgc_cnt;
	uint64_t tgc_cnt;
	uint64_t tgc_by_read;
	uint64_t tgc_by_write;

	/* r/w specific traffic */
	uint64_t read_req_cnt;
	uint64_t write_req_cnt;

	uint64_t d_read_on_read;
	uint64_t d_write_on_read;
	uint64_t t_read_on_read;
	uint64_t t_write_on_read;
	uint64_t d_read_on_write;
	uint64_t d_write_on_write;
	uint64_t t_read_on_write;
	uint64_t t_write_on_write;

	/* write buffer */
	uint64_t wb_hit;

    /* gc reads and writes */
    uint64_t gc_pair_copy;
    uint64_t gc_invm_copy;
    uint64_t gc_cmt_copy;

#ifdef HASH_KVSSD
	uint64_t w_hash_collision_cnt[MAX_HASH_COLLISION];
	uint64_t r_hash_collision_cnt[MAX_HASH_COLLISION];

	uint64_t fp_match_r;
	uint64_t fp_match_w;
	uint64_t fp_collision_r;
	uint64_t fp_collision_w;

    uint64_t inv_w;
    uint64_t inv_r;
#endif

};

/* Functions */
uint32_t demand_argument_set(int argc, char **argv);
uint32_t demand_create(struct demand_shard *shard, lower_info*, blockmanager*, 
                       algorithm*, struct ssd*, uint64_t size);
void demand_destroy(struct demand_shard *shard, lower_info*, algorithm*);
uint32_t demand_read(void*, uint64_t*, uint64_t*);
uint64_t demand_write(void*, uint64_t*, uint64_t*); 
uint32_t demand_remove(void*, uint64_t*, uint64_t*);
uint64_t demand_append(struct demand_shard *shard, request *const);

uint64_t __demand_read(struct demand_shard *shard, request *const, 
                       bool for_del, uint64_t stime);
uint64_t __demand_write(struct demand_shard *shard, request *const, 
                        uint64_t stime);
uint32_t __demand_remove(struct demand_shard *shard, request *const);
void *demand_end_req(algo_req*);

void clear_demand_stat(void);
char* get_demand_stat(struct demand_stat *const _stat);
void print_demand_stat(struct demand_stat *const _stat);

int range_create(void);
uint32_t demand_range_query(request *const);
bool range_end_req(request *);

struct value_set* get_vs(struct ssdparams *spp);
void put_vs(struct value_set *vs);

extern algorithm __demand;
extern struct demand_stat d_stat;
//extern struct demand_member d_member[D_SHARDS];
//extern struct demand_cache *d_cache[D_SHARDS];

#ifdef DVALUE
int grain_create(void);
int is_valid_grain(pga_t);
int contains_valid_grain(blockmanager *, ppa_t);
int validate_grain(blockmanager *, pga_t);
int invalidate_grain(blockmanager *, pga_t);
#endif

#ifdef GC_STANDARD
void __page_to_pte(value_set*, struct pt_struct*, uint64_t, struct ssdparams*,
                   uint64_t shard_id);
#else
void __page_to_ptes_wcmt(value_set *value, struct cmt_struct *cmt);
void __page_to_ptes(value_set*, uint64_t, bool);
#endif

#endif
