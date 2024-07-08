#ifndef _NVMEVIRT_DCACHE_H
#define _NVMEVIRT_DCACHE_H

/*
 * See cache.c for more comments.
 */

#include <linux/types.h>

#include "fifo.h"
#include "nvmev.h"
#include "ssd.h"
#include "ssd_config.h"

#ifdef ORIGINAL
#define OFFSET(x) ((x) & (EPP - 1))

#define ENTRY_SIZE sizeof(ppa_t)

#define EPP (PAGESIZE / ENTRY_SIZE)

#else
#define OFFSET(x) ((x) % EPP)
#define ENTRY_SIZE (sizeof(ppa_t) + sizeof(lpa_t))

#define ROOT_G 4
#define ROOT_G_BYTES (ROOT_G * GRAINED_UNIT)

#define ORIG_GLEN (ROOT_G + 2)
#define ORIG_GLEN_BYTES (ORIG_GLEN * GRAINED_UNIT)

#define EPP (((GRAIN_PER_PAGE - ROOT_G) * GRAINED_UNIT) / ENTRY_SIZE)

#define IN_LEAF ((GRAINED_UNIT) / ENTRY_SIZE)
#define IN_ROOT (EPP / ENTRY_SIZE)

#endif

#define IDX2LPA(x) ((x) * EPP)
#define IDX(x) ((x) / EPP)

#define VICTIM_RB_SZ 131072
struct victim_buffer {
    struct ht_section* hts[VICTIM_RB_SZ];
    int head, tail;
    spinlock_t lock;
};
static struct victim_buffer vb;

struct h_to_g_mapping {
#ifndef ORIGINAL
    uint32_t hidx;
#endif
    atomic_t ppa;
};

typedef enum {
	CLEAN, DIRTY, C_CANDIDATE, D_CANDIDATE
} mapping_state;

struct ht_section {
    uint32_t idx;
    atomic_t t_ppa;
    mapping_state state;

    struct h_to_g_mapping *mappings;

    /*
     * We wrote the FTL with the assumption that pt == NULL means the
     * CMT isn't cached, which is fine. But when moving to using Linux's
     * allocator instead of tying PT address to the PPA on disk, if we
     * NULL pt we lose the memory location of the PT. Use pt_mem to keep
     * that location around.
     */
    void *mem;

    uint64_t g_off;
    uint16_t len_on_disk;
#ifndef ORIGINAL
	uint32_t cached_cnt;
#endif

    atomic_t outgoing;
    /*
     * Mem is where this pair is located in-memory, which is not
     * necessarily the same as its ppa. Consider that we don't need to
     * re-write pairs to a new memory location if they're the same length,
     * we can just overwrite, saving some work. We will still get
     * flash timings based on the PPA on the SSD and use those for the
     * completion times.
     */
    void *pair_mem[EPP];

#ifndef ORIGINAL
    /*
     * Somewhere to store LPA -> grain mappings for use in fast filling.
     * Only works with Plus for now.
     */
    uint32_t fm_grains[EPP];
#endif

    /*
     * For avoiding reads of the first page of an append buffer.
     */
    char* keys[EPP];
};
extern struct ht_section *ht_mem;

struct cache {
    int nr_valid_tpages;
    int nr_valid_tentries;
    int nr_cached_tentries;
	int max_cached_tentries;

    struct ht_section **ht;
    struct ht_section *ht_mem;
    struct fifo *fifo;
};

uint64_t init_cache(struct cache*, uint64_t tt_pgs, uint64_t dram_bytes);

void destroy_cache(struct cache*);

bool cache_full(struct cache*);

bool cache_hit(struct ht_section *ht);

void cache_get(struct cache *cache, struct ht_section *ht, bool *missed);

struct h_to_g_mapping cache_hidx_to_grain(struct ht_section *ht, uint32_t hidx, 
                                          uint32_t *pos);

struct ht_section *cache_get_ht(struct cache*, uint32_t);

#include "twolevel.h"
#endif
