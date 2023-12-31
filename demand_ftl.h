// SPDX-License-Identifier: GPL-2.0-only

#ifndef _NVMEVIRT_CONV_FTL_H
#define _NVMEVIRT_CONV_FTL_H

#include <linux/hashtable.h> 
#include <linux/types.h>
#include <linux/spinlock.h>

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

extern struct conv_ftl *ftl;

typedef enum {
	// generic command status
	KV_SUCCESS = 0, // success
	KV_ERR_KEY_NOT_EXIST = 0x310,
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
    uint64_t** inv_mappings; 
    uint64_t* idxs;
    
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
};

struct conv_ftl {
	struct ssd *ssd;

	struct convparams cp;
	struct ppa *maptbl; /* page level mapping table */
	uint64_t *rmap; /* reverse mapptbl, assume it's stored in OOB */
	struct write_pointer wp;
    struct write_pointer map_wp;
	struct write_pointer gc_wp;
	struct line_mgmt lm;
	struct write_flow_control wfc;
    struct gc_data gcd;
};

void conv_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
			 uint32_t cpu_nr_dispatcher);

void conv_remove_namespace(struct nvmev_ns *ns);

bool conv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req,
			   struct nvmev_result *ret);
bool kv_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, 
                         struct nvmev_result *ret);


#ifdef GC_STANDARD
extern bool *grain_bitmap;
#else
extern uint64_t *pg_inv_cnt;
extern uint64_t *pg_v_cnt;
#define INV_PAGE_SZ 4096
#define INV_ENTRY_SZ (sizeof(uint64_t) + sizeof(uint64_t))
extern char** inv_mapping_bufs;
extern uint64_t* inv_mapping_offs;
extern uint64_t **inv_mapping_ppas;
extern uint64_t *inv_mapping_cnts;
#endif

extern uint64_t **oob;

extern DECLARE_HASHTABLE(mapping_ht, 20);
struct ht_mapping {
    uint64_t lpa;
    uint64_t ppa;
    struct hlist_node node;
};

#endif
