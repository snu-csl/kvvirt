#ifndef __DEMAND_SETTINGS_H__
#define __DEMAND_SETTINGS_H__

#include "../d_param.h"
#include "settings.h"

#ifdef KVSSD
#define HASH_KVSSD

/* Storing the key(or fingerprint(hash) of the key) in the mapping entry */

#if (FP_SIZE > 0)
#define STORE_KEY_FP
#endif

#ifdef STORE_KEY_FP

#ifdef GC_STANDARD
#define ENTRY_SIZE (sizeof(ppa_t) + (FP_SIZE / 8))
#else
#define ENTRY_SIZE (sizeof(ppa_t) + sizeof(lpa_t) + (FP_SIZE / 8))
#endif

#else

#ifdef GC_STANDARD
#define ENTRY_SIZE (sizeof(ppa_t))
#else
#define ENTRY_SIZE (sizeof(lpa_t) + sizeof(ppa_t))
#endif

#endif

#define EPP (PAGESIZE / ENTRY_SIZE) // Entry Per Page

/* Support variable-sized value. Grain entries of the mapping table as GRAINED_UNIT */
#define GRAINED_UNIT ( PIECE )
#define VAR_VALUE_MIN ( MINVALUE )
#define VAR_VALUE_MAX ( PAGESIZE )

/*
 * EPP -> mapping entries per page
 * 
 * We multiply mapping entries per page by the pagesize to get the amount
 * of bytes we need to map via grains. Then, we divide by the grain size
 * (GRAINED_UNIT) to get the grains per mapping page.
 */

#define GRAIN_PER_PAGE (PAGESIZE / GRAINED_UNIT)
//#define BITS_PER_PAGE GRAIN_PER_PAGE
//static_assert((GRAIN_PER_PAGE * GRAINED_UNIT) == (EPP * PAGESIZE));
//static_assert((BITS_PER_PAGE * GRAINED_UNIT) == (EPP * PAGESIZE));
//
///*
// * We need to store as many value lengths as the worst case, which
// * is one value length per grain.
// *
// * In reality, we might be storing less than that, because values
// * can be more than one grain in size.
// */
//
//typedef uint16_t vlen_t;
//#define VLEN_S sizeof(vlen_t)
//#define VLENS_PER_PAGE GRAIN_PER_PAGE
//static_assert((VLENS_PER_PAGE * VLEN_S) <= ((EPP * PAGESIZE) / 4));

/* Max hash collision count to logging ( refer utility.c:hash_collision_logging() ) */
#define MAX_HASH_COLLISION 1024
#endif

#define DEMAND_WARNING 1

#if DEMAND_WARNING
/* Warning options here */
#define WARNING_NOTFOUND
#endif


#define PART_RATIO 0.5

#define WRITE_BACK
#define MAX_WRITE_BUF 256

#define STRICT_CACHING

#define PRINT_GC_STATUS

#endif
