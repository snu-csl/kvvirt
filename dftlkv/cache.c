#include "cache.h"

/*
 * Cache functions.
 *
 * You will notice that evict and get are in demand_ftl.c.
 * Optimism led me to refactor the cache code into a file of its own that
 * wouldn't need to import everything else from virt.
 *
 * That optimism soon met reality when I realised how mixed
 * the cache, SSD, and virt code are for gets and eviction.
 * There are only so many hours in a day, and I need to move on to other work 
 * to graduate!
 *
 * Full refactor TBD (TM).
 */

void init_cache(struct cache* c, uint64_t tt_pgs, uint64_t dram_bytes) 
{
    struct ht_section **ht;

    c->nr_valid_tpages = (tt_pgs * GRAIN_PER_PAGE) / EPP;
    c->nr_valid_tentries = (tt_pgs * GRAIN_PER_PAGE);
    c->max_cached_tentries = dram_bytes / GRAINED_UNIT;

    fifo_init(&c->fifo);

    /*
     * This is a constantly available list of pointers to every hash table
     * section, whether that section be on flash or DRAM.
     */
    ht = (struct ht_section**) vmalloc(c->nr_valid_tpages * 
                                       sizeof(struct ht_section*));

    for (int i = 0; i < c->nr_valid_tpages; i++) {
        /*
         * This is an actual hash table section.
         * Whether or not ->mem is NULL determines if the hash table
         * section is cached in DRAM or not.
         */
        ht[i] = (struct ht_section*) kzalloc(sizeof(struct ht_section), 
                                             GFP_KERNEL);
        NVMEV_ASSERT(ht[i]);

        atomic_set(&ht[i]->t_ppa, UINT_MAX);
        atomic_set(&ht[i]->outgoing, 0);

        /*
         * This index is the ID of the hash table section.
         * For example, if we have LPA 50 and each hash table section holds
         * 1024 items, this IDX will be 0.
         */
        ht[i]->idx = i; 
        ht[i]->mem = NULL;
        ht[i]->state = CLEAN;

        /*
         * The actual hash index to grain mappings.
         */
        ht[i]->mappings = NULL;

        /*
         * In the current iteration of this FTL, we use Linux's kernel
         * allocator to allocate KV pairs, instead of virt's internal
         * memory. 
         *
         * If we were using virt's internal memory, we would use the grain
         * and take GRAINED_UNIT * grain to find the pair. However,
         * since we are allocating pairs by kzalloc, we can't do that.
         *
         * pair_mem contains the locations of KV pairs in memory.
         * When we look for a hash index to grain mapping,
         * we get its KV pair data from here.
         */
        ht[i]->pair_mem = kzalloc(sizeof(void*) * EPP, GFP_KERNEL);
        ht[i]->len_on_disk = 0;

        NVMEV_ASSERT(ht[i]->pair_mem);
    }
    
    c->ht = ht;
    c->nr_cached_tentries = 0;
}

void destroy_cache(struct cache* c) 
{
    for (int i = 0; i < c->nr_valid_tpages; i++) {
        struct ht_section *ht = c->ht[i];
        for(int i = 0; i < EPP; i++) {
            if(ht->pair_mem[i]) {
                kfree(ht->pair_mem[i]);
                ht->pair_mem[i] = NULL;
            }
        }

        if(ht->mem) {
            kfree(ht->mem);
        }

        kfree(ht->pair_mem);
        kfree(ht);
    }

    vfree(c->ht);
    fifo_destroy(c->fifo);
}

bool cache_full(struct cache *c) {
    return (c->nr_cached_tentries >= c->max_cached_tentries);
}

bool cache_hit(struct ht_section *ht) {
    /*
     * cache_hit means "is this hash table section in memory", not
     * "is this hash index to grain mapping in memory".
     *
     * In Original, a hash index to grain mapping that hasn't been mapped
     * yet can still be in memory, because we load the whole hash table section
     * and not just live entries. In this case, we return true here for hit.
     * After in demand_ftl.c, we will check if the hash index to grain mapping
     * contains a valid mapping (IS_INITIAL_PPA). If it doesn't, we just
     * continue and fill the mapping. If we returned false here when the
     * hash table section was in memory but the specific mapping we were looking
     * for didn't exist, we would re-read the hash table section from disk,
     * which is false.
     *
     * In Plus, we can return true here but the hash index to grain mapping may
     * not exist at all in the hash table section. "mappings" is the two level
     * table that Plus uses to find hash index to grain mappings. If a search
     * through the table can't find the mapping, we continue and add it.
     */
    return ht->mappings == NULL;
}

struct h_to_g_mapping cache_hidx_to_grain(struct ht_section *ht, uint32_t hidx, 
                                          uint32_t *pos) {
    struct h_to_g_mapping ret;
#ifdef ORIGINAL
    /*
     * Original's offset calculation scheme.
     */

    if(pos) {
        *pos = OFFSET(hidx);
    }

    return ht->mappings[OFFSET(hidx)];
#else
    /*
     * Plus's two level table scheme.
     */

    struct root *root;

    root = (struct root*) ht->mappings;
    ret.hidx = hidx;
    atomic_set(&ret.ppa, twolevel_find(root, hidx, pos));

    return ret;
#endif
}

struct ht_section* cache_get_ht(struct cache* c, uint32_t hidx) 
{
    struct ht_section *ht;

    /*
     * IDX means the index of this hash table section in the overall hash table.
     * The IDX for a certain hash table section never changes throughout the 
     * FTL's lifetime.
     */
    ht = c->ht[IDX(hidx)];

    while (atomic_cmpxchg(&ht->outgoing, 0, 1) != 0) {
        cpu_relax();
    }

    return ht;
}

