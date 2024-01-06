/*
 * Header for coarse.c
 */

#ifndef __COARSE_H__
#define __COARSE_H__

#include "demand.h"

int cgo_create(cache_t, struct demand_cache *);
int cgo_destroy(void);
int cgo_load(lpa_t, request *const, snode *wb_entry, uint64_t*);
int cgo_list_up(lpa_t, request *const, snode *wb_entry, uint64_t*, uint64_t*);
int cgo_wait_if_flying(lpa_t, request *const, snode *wb_entry);
int cgo_touch(lpa_t);
int cgo_update(lpa_t, struct pt_struct pte);
bool cgo_is_hit(lpa_t);
bool cgo_is_full(void);
struct pt_struct cgo_get_pte(lpa_t lpa);
struct cmt_struct *cgo_get_cmt(lpa_t lpa);

#endif
