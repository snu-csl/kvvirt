// SPDX-License-Identifier: GPL-2.0-only

#ifndef _NVMEVIRT_CONV_FTL_H
#define _NVMEVIRT_CONV_FTL_H

#include <linux/hashtable.h> 
#include <linux/kfifo.h>
#include <linux/slab.h>
#include <linux/slub_def.h>
#include <linux/spinlock.h>
#include <linux/types.h>
#include <linux/xarray.h>

#include "cache.h"
#include "pqueue/pqueue.h"
#include "ssd_config.h"
#include "ssd.h"

#include "nvme_kv.h"

#define is_kv_append_cmd(opcode) ((opcode) == nvme_cmd_kv_append)
#define is_kv_store_cmd(opcode) ((opcode) == nvme_cmd_kv_store)
#define is_kv_retrieve_cmd(opcode) ((opcode) == nvme_cmd_kv_retrieve)
#define is_kv_delete_cmd(opcode) ((opcode) == nvme_cmd_kv_delete)
#define is_kv_iter_req_cmd(opcode) ((opcode) == nvme_cmd_kv_iter_req)
#define is_kv_iter_read_cmd(opcode) ((opcode) == nvme_cmd_kv_iter_read)
#define is_kv_exist_cmd(opcode) ((opcode) == nvme_cmd_kv_exist)
#define is_kv_batch_cmd(opcode) ((opcode) == nvme_cmd_kv_batch)

#define is_kv_cmd(opcode)                                                                         \
	(is_kv_append_cmd(opcode) || is_kv_store_cmd(opcode) || is_kv_retrieve_cmd(opcode) ||     \
	 is_kv_delete_cmd(opcode) || is_kv_iter_req_cmd(opcode) || is_kv_iter_read_cmd(opcode) || \
	 is_kv_exist_cmd(opcode)) ||                                                              \
		is_kv_batch_cmd(opcode)

/*
 * For DFTL.
 */

typedef enum {
	// generic command status
	KV_SUCCESS = 0, // success
	KV_ERR_KEY_NOT_EXIST = 0x310,
    KV_ERR_BUFFER_SMALL=0x301
} kvs_result;

struct convparams {
	uint32_t gc_thres_lines;
	uint32_t gc_thres_lines_high;
	bool enable_gc_delay;

	double op_area_pcent;
	int pba_pcent; /* (physical space / logical space) * 100*/

    uint64_t max_ppa;
    uint64_t num_segments;
    uint64_t real_num_segments;
};

struct line {
	int id; /* line id, the same as corresponding block id */
	int ipc; /* invalid page count in this line */
	int vpc; /* valid page count in this line */
    int vgc; /* valid grains in this line */
    int igc; /* invalid grain count in this line */
	struct list_head entry;
	/* position in the priority queue for victim lines */
	size_t pos;
    bool map;
};

/* wp: record next write addr */
struct write_pointer {
	struct line *curline;
	uint32_t ch;
	uint32_t lun;
	uint32_t pg;
	uint32_t blk;
	uint32_t pl;
};

struct line_mgmt {
	struct line *lines;

	/* free line list, we only need to maintain a list of blk numbers */
	struct list_head free_line_list;
	pqueue_t *victim_line_pq;
	struct list_head full_line_list;

	uint32_t tt_lines;
	uint32_t free_line_cnt;
	uint32_t victim_line_cnt;
	uint32_t full_line_cnt;
};

struct write_flow_control {
	uint32_t write_credits;
	uint32_t credits_to_refill;
};

struct gc_data {
    /*
     * When we copy grains of data during GC, parts of
     * the page which we allocated for the copies
     * can be unused at the end of the series
     * of copies in clean_one_flashpg. We record
     * the page we used during the previous call to
     * clean_one_flashpg and various other information
     * so that we can copy to that page in the next
     * clean_one_flashpg call.
     */

    struct ppa gc_ppa;
    uint64_t remain;
    uint64_t pgidx;
    uint32_t offset;
    bool last;
    bool map;
    struct xarray inv_mapping_xa;
    struct xarray gc_xa;
    struct lpa_len_ppa *lpa_lens; 
    atomic_t gc_rem;
};

#define MAX_HASH_COLLISION 1024
struct stats {
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
    uint64_t inv_m_r;
    uint64_t inv_m_w;

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

	uint64_t w_hash_collision_cnt[MAX_HASH_COLLISION];
	uint64_t r_hash_collision_cnt[MAX_HASH_COLLISION];

	uint64_t fp_match_r;
	uint64_t fp_match_w;
	uint64_t fp_collision_r;
	uint64_t fp_collision_w;

	uint64_t cache_hit;
	uint64_t cache_miss;
	uint64_t clean_evict;
	uint64_t dirty_evict;
};

struct demand_shard {
    uint64_t id;

    /*
     * Hash index to grain mapping cache.
     */
    struct cache cache;

	struct ssd *ssd;

	struct convparams cp;
	struct ppa *maptbl; /* page level mapping table */
	uint64_t *rmap; /* reverse mapptbl, assume it's stored in OOB */
	struct write_pointer wp;
    struct write_pointer map_wp;
	struct write_pointer gc_wp;
    struct write_pointer map_gc_wp;
	struct line_mgmt lm;
	struct write_flow_control wfc;
    struct gc_data gcd;

    uint64_t offset; /* current offset on disk */

    uint64_t **oob;
    uint64_t *oob_mem;
    bool *grain_bitmap;

    uint32_t max_try;

    uint64_t dram; /* in bytes */
    bool fastmode; /* skip timings and build map later */

    struct task_struct *bg_gc_t;
    struct task_struct *bg_ev_t;

    atomic_t candidates;
    atomic_t have_victims;

    struct stats stats;
};

void conv_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
			 uint32_t cpu_nr_dispatcher);
void conv_remove_namespace(struct nvmev_ns *ns);
bool conv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req,
			   struct nvmev_result *ret);
bool kv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, 
                         struct nvmev_result *ret);
void demand_warmup(struct nvmev_ns *ns);

void mark_grain_invalid(struct demand_shard *shard, uint64_t grain, uint32_t len);
void mark_page_valid(struct demand_shard *demand_shard, struct ppa *ppa);
void mark_grain_valid(struct demand_shard *shard, uint64_t grain, uint32_t len);

#ifndef ORIGINAL
#define INV_PAGE_SZ PAGESIZE
#define INV_ENTRY_SZ (sizeof(lpa_t) + sizeof(ppa_t))

extern uint8_t *pg_inv_cnt;
extern char** inv_mapping_bufs;
extern uint64_t* inv_mapping_offs;
#endif

extern uint64_t user_pgs_this_gc;
extern uint64_t gc_pgs_this_gc;
extern uint64_t map_pgs_this_gc;
extern uint64_t map_gc_pgs_this_gc;

struct hash_params {
	uint32_t hash;
	int cnt;
	int find;
	uint32_t lpa;
};

#define QUADRATIC_PROBING(h,c) ((h)+(c)+(c)*(c))
#define LINEAR_PROBING(h,c) (h+c)

#define PROBING_FUNC(h,c) QUADRATIC_PROBING(h,c)

#define IS_INITIAL_PPA(x) ((atomic_read(&x)) == UINT_MAX)

#define IDX2LPA(x) ((x) * EPP)
#define IDX(x) ((x) / EPP)

#ifdef ORIGINAL
#define OFFSET(x) ((x) & (EPP - 1))
#else
#define OFFSET(x) ((x) % EPP)
#endif

#define PPA_TO_PGA(_ppa_, _offset_) ( ((_ppa_) * GRAIN_PER_PAGE) + (_offset_) )
#define G_IDX(x) ((x) / GRAIN_PER_PAGE)
#define G_OFFSET(x) ((x) & (GRAIN_PER_PAGE - 1))

#endif
