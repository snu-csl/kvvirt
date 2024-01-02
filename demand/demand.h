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
#include "d_htable.h"
#include "./interface/queue.h"
#include "./include/container.h"
#include "./include/dl_sync.h"
#include "./Lsmtree/skiplist.h"
#include "./include/data_struct/lru_list.h"

#include <linux/highmem.h>
#include <linux/vmalloc.h>

extern uint64_t user_pgs_this_gc;
extern uint64_t gc_pgs_this_gc;
extern uint64_t map_pgs_this_gc;
extern uint64_t pgs_this_flush;

struct lpa_len_ppa {
    uint64_t lpa; /* the lpa we find in the reverse map. */
    uint32_t len; /* the length of the key-value pair. */
    uint64_t prev_ppa; /* to copy from during GC */
    uint64_t new_ppa; /* the new ppa we're writing this key-value pair to. */
    bool skip_read; /* skip the read for this page when updating mappings. */
#ifndef GC_STANDARD
    uint64_t cmt_ppa; /* which CMT ppa does this item belong to? */
#endif
};

void clear_oob(uint64_t pgidx);
bool oob_empty(uint64_t pgidx);
struct ppa get_new_page(struct conv_ftl *conv_ftl, uint32_t io_type);
uint64_t ppa2pgidx(struct conv_ftl *conv_ftl, struct ppa *ppa);
bool advance_write_pointer(struct conv_ftl *conv_ftl, uint32_t io_type);
void mark_page_valid(struct conv_ftl *conv_ftl, struct ppa *ppa);
void mark_page_invalid(struct conv_ftl *conv_ftl, struct ppa *ppa);
void mark_grain_valid(struct conv_ftl *conv_ftl, uint64_t grain, uint32_t len);
void mark_grain_invalid(struct conv_ftl *conv_ftl, uint64_t grain, uint32_t len);
inline void consume_write_credit(struct conv_ftl *conv_ftl, uint32_t len);
inline uint64_t check_and_refill_write_credit(struct conv_ftl *conv_ftl);
void clean_one_flashpg(struct conv_ftl *conv_ftl, struct ppa *ppa);
int do_bulk_mapping_update_v(struct lpa_len_ppa *ppas, int nr_valid_grains, 
                             uint64_t *read_cmts, uint64_t read_cmt_cnt);
inline struct line *get_line(struct conv_ftl *conv_ftl, struct ppa *ppa);

extern struct demand_stat d_stat;

extern bool FAIL_MODE;

/* Structures */
// Page table entry
struct pt_struct {
    lpa_t lpa;
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
    ppa_t grain;

	cmt_state_t state;
	bool is_flying;

	queue *retry_q;
	queue *wait_q;

    bool *is_cached;
	uint32_t cached_cnt;
	uint32_t dirty_cnt;
};

struct hash_params {
	uint64_t hash;
#ifdef STORE_KEY_FP
	fp_t key_fp;
#endif
	int cnt;
	int find;
	uint64_t lpa;

#ifdef DVALUE
	int fl_idx;
#endif
};

struct demand_params{
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

    struct ssd *ssd;
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
#endif

};

/* Functions */
uint32_t demand_argument_set(int argc, char **argv);
uint32_t demand_create(lower_info*, blockmanager*, algorithm*, struct ssd*, 
                       uint64_t size);
void demand_destroy(lower_info*, algorithm*);
uint32_t demand_read(request *const);
uint64_t demand_write(request *const);
uint32_t demand_remove(request *const);
uint64_t demand_append(request *const);

uint64_t __demand_read(request *const, bool for_del);
uint64_t __demand_write(request *const);
uint32_t __demand_remove(request *const);
void *demand_end_req(algo_req*);

void print_demand_stat(struct demand_stat *const _stat);

int range_create(void);
uint32_t demand_range_query(request *const);
bool range_end_req(request *);

#ifdef DVALUE
int grain_create(void);
int is_valid_grain(pga_t);
int contains_valid_grain(blockmanager *, ppa_t);
int validate_grain(blockmanager *, pga_t);
int invalidate_grain(blockmanager *, pga_t);
#endif

void __page_to_pte(value_set *value, struct pt_struct *pt, uint64_t idx);
void __pte_to_page(value_set *value, struct pt_struct *pt);

#endif
