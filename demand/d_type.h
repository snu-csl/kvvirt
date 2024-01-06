/*
 * Demand-based FTL Type definitions with including basic headers
 */

#ifndef __DEMAND_TYPE_H__
#define __DEMAND_TYPE_H__

#include <linux/types.h>

#include "./include/settings.h"
#include "./include/demand_settings.h"
#include "./include/types.h"

typedef uint64_t lpa_t;
typedef uint64_t ppa_t;
typedef ppa_t pga_t;

#if (FP_SIZE<=8)
typedef uint8_t fp_t;
#define FP_MAX U64_MAX

#elif (FP_SIZE<=16)
typedef uint16_t fp_t;
#define FP_MAX UINT16_MAX

#elif (FP_SIZE<=32) 
typedef uint32_t fp_t;
#define FP_MAX U64_MAX

#elif (FP_SIZE<=64) 
typedef uint64_t fp_t;
#define FP_MAX UINT64_MAX
#endif


typedef enum {
    /*
     * Renamed from READ, WRITE because READ is taken in the kernel
     * somewhere.
     */

	DREAD, DWRITE
} rw_t;

typedef enum {
	HASH_KEY_INITIAL,
	HASH_KEY_NONE,
	HASH_KEY_SAME,
	HASH_KEY_DIFF,
} hash_cmp_t;

typedef enum {
	CLEAN, DIRTY
} cmt_state_t;

typedef enum {
	GOTO_LOAD, GOTO_LIST, GOTO_EVICT, 
	GOTO_COMPLETE, GOTO_READ, GOTO_WRITE,
	GOTO_UPDATE,
} jump_t;

typedef enum {
	COARSE_GRAINED,
	FINE_GRAINED,
	PARTED,
    OLD_COARSE_GRAINED
} cache_t;

#endif
