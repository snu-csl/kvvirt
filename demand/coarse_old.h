/*
 * Header for coarse.c
 */

#ifndef __COARSE_H__
#define __COARSE_H__

#include "demand.h"

int cgo_create(struct demand_shard*, cache_t);
int cgo_destroy(struct demand_cache *);
int cgo_load(struct demand_shard*, lpa_t, request *const, snode *wb_entry, uint64_t*);
int cgo_list_up(struct demand_shard*, lpa_t, request *const, snode *wb_entry, uint64_t*, uint64_t*);
int cgo_wait_if_flying(lpa_t, request *const, snode *wb_entry);
int cgo_touch(struct demand_cache*, lpa_t);
int cgo_update(struct demand_shard*, lpa_t, struct pt_struct pte);
bool cgo_is_hit(struct demand_cache*, lpa_t);
bool cgo_is_full(struct demand_cache*);
struct pt_struct cgo_get_pte(struct demand_shard*, lpa_t lpa);
struct cmt_struct *cgo_get_cmt(struct demand_cache*, lpa_t lpa);

#endif
