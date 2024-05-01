#ifndef _HASHSET_H
#define _HASHSET_H

#include <linux/random.h>
#include <linux/types.h>   // For uint32_t
#include <linux/slab.h>    // For kmalloc, kfree
#include <linux/list.h>    // For linked list operations
#include <linux/spinlock.h> // For locking

#include "nvmev.h"

struct hashset_item {
    uint32_t key;
    struct hlist_node node;
};

struct hashset {
    struct hlist_head bins[HSIZE];
    spinlock_t lock;
	int count;
};

struct hashset *hashset_create(void);
void hashset_destroy(struct hashset *set);
bool hashset_insert(struct hashset *set, uint32_t key);
bool hashset_delete(struct hashset *set, uint32_t key);
bool hashset_contains(struct hashset *set, uint32_t key);
void hashset_evict_random(struct hashset *set);

#endif
