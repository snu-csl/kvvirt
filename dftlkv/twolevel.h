#ifndef _NVMEVIRT_TWOLEVEL_H
#define _NVMEVIRT_TWOLEVEL_H

#include <linux/sort.h>

#include "cache.h"
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

struct root {
    uint32_t cnt;
    uint32_t entries[IN_ROOT];
};

struct leaf {
    uint32_t hidx[IN_LEAF];
    uint32_t ppa[IN_LEAF];
};

struct leaf_e {
    uint32_t hidx;
    uint32_t ppa;
};

void btree_init(struct root *root);
void btree_insert(struct ht_section *ht, struct root *root, 
                  uint32_t hidx, uint32_t ppa, uint32_t pos);
uint32_t btree_find(struct root *root, uint32_t hidx, uint32_t *pos);
void btree_expand(struct root *root);
void btree_reload(struct ht_section *ht, struct root *root, 
                  uint32_t hidx, uint32_t ppa);
void btree_direct_read(struct root *root, uint32_t pos, 
                       void* out, uint32_t len);
void btree_bulk_insert(struct root* root, struct leaf_e *e, uint32_t cnt);

#endif
