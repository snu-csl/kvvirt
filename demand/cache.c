/*
 * Demand-based FTL Cache
 */

#include "cache.h"

/* Declare cache structure first */
extern struct demand_cache cg_cache;
extern struct demand_cache cgo_cache;
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
	case OLD_COARSE_GRAINED:
        printk("OLD_COARSE\n");
		ret = &cgo_cache;
		break;
	case FINE_GRAINED:
        BUG_ON(true);
		//ret = &fg_cache;
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

struct cache_stat* get_cstat(void) {
#ifdef GC_STANDARD
    return &(cgo_cache.stat);
#else
    return &(cg_cache.stat);
#endif
}

void clear_cache_stat(void) {
#ifdef GC_STANDARD
    cgo_cache.stat.cache_hit = 0;
    cgo_cache.stat.cache_miss = 0;
    cgo_cache.stat.clean_evict = 0;
    cgo_cache.stat.dirty_evict = 0;
    cgo_cache.stat.blocked_miss = 0;
#else
    cg_cache.stat.cache_hit = 0;
    cg_cache.stat.cache_miss = 0;
    cg_cache.stat.clean_evict = 0;
    cg_cache.stat.dirty_evict = 0;
    cg_cache.stat.blocked_miss = 0;
#endif
}

uint32_t get_cache_stat(char* out) {
    struct cache_stat _stat = *get_cstat();
    char *buf = (char*) kzalloc(4096, GFP_KERNEL);
    int off = 0;

	sprintf(buf + off, "===================\n");
    off = strlen(buf);
	sprintf(buf + off, " Cache Performance \n");
    off = strlen(buf);
	sprintf(buf + off, "===================\n");
    off = strlen(buf);

	sprintf(buf + off, "Cache_Hit:\t%lld\n", _stat.cache_hit);
    off = strlen(buf);
	sprintf(buf + off, "Cache_Miss:\t%lld\n", _stat.cache_miss);
    off = strlen(buf);

    sprintf(buf + off, "Hit ratio:FIXME\n");
    off = strlen(buf);
	sprintf(buf + off, "\n");
    off = strlen(buf);

	sprintf(buf + off, "Blocked miss:\t%lld\n", _stat.blocked_miss);
    off = strlen(buf);
	sprintf(buf + off, "\n");
    off = strlen(buf);

	sprintf(buf + off, "Clean evict:\t%lld\n", _stat.clean_evict);
    off = strlen(buf);
	sprintf(buf + off, "Dirty evict:\t%lld\n", _stat.dirty_evict);
    off = strlen(buf);
	sprintf(buf + off, "\n");
    off = strlen(buf);

    memcpy(out, buf, strlen(buf));
    kfree(buf);
    return off;
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
