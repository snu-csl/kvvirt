#include <linux/sort.h>

#include "demand/cache.h"
#include "nvmev.h"

#define BITS_TO_REPRESENT(x) ((x) <= 1 ? 1 : \
                              (x) <= 2 ? 2 : \
                              (x) <= 4 ? 3 : \
                              (x) <= 8 ? 4 : \
                              (x) <= 16 ? 5 : \
                              (x) <= 32 ? 6 : \
                              (x) <= 64 ? 7 : \
                              (x) <= 128 ? 8 : \
                              (x) <= 256 ? 9 : \
                              (x) <= 512 ? 10 : \
                              (x) <= 1024 ? 11 : \
                              (x) <= 2048 ? 12 : \
                              (x) <= 4096 ? 13 : \
                              (x) <= 8192 ? 14 : \
                              (x) <= 16384 ? 15 : \
                              (x) <= 32768 ? 16 : \
                              (x) <= 65536 ? 17 : 32) 

//#define PAGESIZE 4096
//#define ENTRY_SIZE 8
//
//#define PIECE 64
//#define GRAINED_UNIT ( PIECE )
//#define GRAIN_PER_PAGE (PAGESIZE / GRAINED_UNIT)
//
//#define ROOT_G 4
//#define ROOT_G_BYTES (ROOT_G * GRAINED_UNIT)
//
//#define ORIG_GLEN (ROOT_G + 2)
//#define ORIG_GLEN_BYTES (ORIG_GLEN * GRAINED_UNIT)
//
//#define IN_LEAF ((GRAINED_UNIT - sizeof(uint32_t)) / (sizeof(uint32_t) * 2))
//#define IN_ROOT ((ROOT_G_BYTES - sizeof(uint32_t)) / sizeof(uint32_t))
//
//#define TOTAL_CNT_BYTES (IN_ROOT * sizeof(uint32_t))
//#define TOTAL_MAP_BYTES (IN_ROOT * IN_LEAF * sizeof(uint32_t) * 2)
//
//#define EPP (IN_ROOT * IN_LEAF)

struct root {
    uint32_t cnt;
    uint32_t entries[IN_ROOT];
};

struct leaf {
    uint32_t lpa[IN_LEAF];
    uint32_t ppa[IN_LEAF];
};

struct leaf_e {
    uint32_t lpa;
    uint32_t ppa;
};

void btree_init(struct root *root);
void btree_insert(struct cmt_struct *cmt, struct root *root, 
                  uint32_t lpa, uint32_t ppa, uint32_t pos);
uint32_t btree_find(struct root *root, uint32_t lpa, uint32_t *pos);
void btree_expand(struct root *root);
void btree_reload(struct cmt_struct *cmt, struct root *root, 
                  uint32_t lpa, uint32_t ppa);
void btree_direct_read(struct root *root, uint32_t pos, 
                       void* out, uint32_t len);
void btree_bulk_insert(struct root* root, struct leaf_e *e, uint32_t cnt);
