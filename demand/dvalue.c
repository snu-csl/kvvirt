/*
 * Demand-based FTL Grain Implementation For Supporting Dynamic Value Size
 *
 * Note that, I use the term "grain" to substitute the term "piece" which represents dynamic value unit
 *
 */

/*
 * TODO clean this up so we don't need to do ifdef everywhere.
 */

#ifdef DVALUE

#include "demand.h"
#include "../demand_ftl.h"

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

lpa_t **oob = NULL;
uint64_t* pg_inv_cnt = NULL;
uint64_t* pg_v_cnt = NULL;

#ifdef GC_STANDARD
bool* grain_bitmap = NULL;
#endif

int grain_create(void) {
#ifdef GC_STANDARD
	grain_bitmap = (bool *)vmalloc(d_env.nr_grains * sizeof(bool));
	if (!grain_bitmap) return 1;
#endif

	return 0;
}

int is_valid_grain(pga_t pga) {
#ifdef GC_STANDARD
	return grain_bitmap[pga];
#else
    BUG_ON(true);
#endif
}

int contains_valid_grain(blockmanager *bm, ppa_t ppa) {
#ifdef GC_STANDARD
	pga_t pga = ppa * GRAIN_PER_PAGE;
	for (int i = 0; i < GRAIN_PER_PAGE; i++) {
		if (is_valid_grain(pga+i)) return 1;
	}

	/* no valid grain */
	if (unlikely(bm->is_valid_page(bm, ppa))) printk("Should have aborted!!!! %s:%d\n" , __FILE__, __LINE__);;

	return 0;
#else
BUG_ON(true);
#endif
}

int validate_grain(blockmanager *bm, pga_t pga) {
#ifdef GC_STANDARD
	int rc = 0;

	if (grain_bitmap[pga] == 1) rc = 1;
	grain_bitmap[pga] = 1;

	return rc;
#else
BUG_ON(true);
#endif
}

int invalidate_grain(blockmanager *bm, pga_t pga) {
#ifdef GC_STANDARD
	int rc = 0;

	if (grain_bitmap[pga] == 0) rc = 1;
	grain_bitmap[pga] = 0;

	return rc;
#else
BUG_ON(true);
#endif
}

#endif
