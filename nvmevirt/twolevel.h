#ifndef _NVMEVIRT_TWOLEVEL_H
#define _NVMEVIRT_TWOLEVEL_H

#include <linux/sort.h>

#include "cache.h"
#include "nvmev.h"

#ifndef ORIGINAL

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

void twolevel_init(struct root *root);
void twolevel_insert(struct ht_section *ht, struct root *root, 
                  uint32_t hidx, uint32_t ppa, uint32_t pos);
uint32_t twolevel_find(struct root *root, uint32_t hidx, uint32_t *pos);
void twolevel_expand(struct root *root);
void twolevel_reload(struct ht_section *ht, struct root *root, 
                  uint32_t hidx, uint32_t ppa);
void twolevel_direct_read(struct root *root, uint32_t pos, 
                       void* out, uint32_t len);
void twolevel_bulk_insert(struct root* root, struct leaf_e *e, uint32_t cnt);

#endif

#endif
