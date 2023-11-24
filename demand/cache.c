/*
 * Demand-based FTL Cache
 */

#include "cache.h"

/* Declare cache structure first */
extern struct demand_cache cg_cache;
extern struct demand_cache fg_cache;
//extern struct demand_cache pt_cache;

struct demand_cache *select_cache(cache_t type) {
	/*
	 * This Fucntion returns selected cache module pointer with create() it
	 */

	struct demand_cache *ret = NULL;

	switch (type) {
	case COARSE_GRAINED:
		ret = &cg_cache;
		break;
	case FINE_GRAINED:
		ret = &fg_cache;
		break;
#if 0
	case PARTED:
		ret = &pt_cache;
		break;
#endif

	/* if you implemented any other cache module, add here */

	default:
		printk("[ERROR] No cache type found, at %s:%d\n", __FILE__, __LINE__);
        printk("Should be aborting here!\n");
	}

	if (ret) ret->create(type, ret);

	return ret;
}

void print_cache_stat(struct cache_stat *_stat) {
	printk("===================");
	printk(" Cache Performance ");
	printk("===================");

	printk("Cache_Hit:\t%lld\n", _stat->cache_hit);
	printk("Cache_Miss:\t%lld\n", _stat->cache_miss);
	//printk("Hit ratio:\t%.2f%%\n", (float)(_stat->cache_hit)/(_stat->cache_hit+_stat->cache_miss)*100);
    printk("Hit ratio:FIXME\n");
	printk("\n");

	printk("Blocked miss:\t%lld\n", _stat->blocked_miss);
	printk("\n");

	printk("Clean evict:\t%lld\n", _stat->clean_evict);
	printk("Dirty evict:\t%lld\n", _stat->dirty_evict);
	printk("\n");
}
