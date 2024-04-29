#include "hashset.h"
#include <linux/hash.h>    // For hash function

/* Hash function */
static inline int hash(uint32_t key) {
    return hash_32(key, 8);  // Use kernel's built-in hash function
}

/* Create a new hash set */
struct hashset *hashset_create(void) {
    struct hashset *set = kmalloc(sizeof(struct hashset), GFP_KERNEL);
    if (!set)
        return NULL;
    
    int i;
    for (i = 0; i < HSIZE; i++) {
        INIT_HLIST_HEAD(&set->bins[i]);
    }
    spin_lock_init(&set->lock);
	set->count = 0;

    return set;
}

/* Destroy the hash set */
void hashset_destroy(struct hashset *set) {
    int i;
    struct hashset_item *item;
    struct hlist_node *tmp;

    if (!set)
        return;

    for (i = 0; i < HSIZE; i++) {
        hlist_for_each_entry_safe(item, tmp, &set->bins[i], node) {
            hlist_del(&item->node);
            kfree(item);
        }
    }
    kfree(set);
}

/* Insert a new key into the hash set */
bool hashset_insert(struct hashset *set, uint32_t key) {
    int bin = hash(key);
    struct hashset_item *item;
    
    //spin_lock(&set->lock);
    if (set->count >= MAX_ITEMS) {  // MAX_ITEMS is the threshold for eviction
        hashset_evict_random(set);  // Evict a random item before inserting
    }

    hlist_for_each_entry(item, &set->bins[bin], node) {
        if (item->key == key) {
            spin_unlock(&set->lock);
            return false; // Key already exists
        }
    }

    item = kmalloc(sizeof(struct hashset_item), GFP_KERNEL);
    if (!item) {
        spin_unlock(&set->lock);
        return false;
    }

	set->count++;
    item->key = key;
    hlist_add_head(&item->node, &set->bins[bin]);
    //spin_unlock(&set->lock);
    return true;
}

/* Delete a key from the hash set */
bool hashset_delete(struct hashset *set, uint32_t key) {
    int bin = hash(key);
    struct hashset_item *item;

    spin_lock(&set->lock);
    hlist_for_each_entry(item, &set->bins[bin], node) {
        if (item->key == key) {
            hlist_del(&item->node);
            kfree(item);
            spin_unlock(&set->lock);
            return true;
        }
    }
    spin_unlock(&set->lock);
    return false; // Key not found
}

/* Check if the hash set contains a key */
bool hashset_contains(struct hashset *set, uint32_t key) {
    int bin = hash(key);
    struct hashset_item *item;

    spin_lock(&set->lock);
    hlist_for_each_entry(item, &set->bins[bin], node) {
        if (item->key == key) {
            spin_unlock(&set->lock);
            return true;
        }
    }
    spin_unlock(&set->lock);
    return false;
}

void hashset_evict_random(struct hashset *set) {
    int idx = prandom_u32_max(HSIZE);  // Generate a random bin index
    struct hashset_item *item;
    struct hlist_node *tmp;
    bool found = false;

    // Try to find a non-empty bin starting from a random index
    for (int i = 0; i < HSIZE; i++) {
        int bin = (idx + i) % HSIZE;
        hlist_for_each_entry_safe(item, tmp, &set->bins[bin], node) {
            hlist_del(&item->node);
            kfree(item);
            set->count--;
            found = true;
            break;
        }
        if (found) break;
    }
}
