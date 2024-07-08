#include <linux/slab.h>
#include <linux/types.h>

#include "nvmev.h"
#include "lru.h"

void lru_cache_init(struct lru_cache *cache, uint32_t capacity) {
    cache->head = NULL;
    cache->tail = NULL;
    cache->capacity = capacity;
    cache->size = 0;
}

struct item *lru_cache_get(struct lru_cache *cache, uint32_t id) {
    if (cache->size == 0 || id >= MAX_CAPACITY) {
        return NULL;
    }

    struct item *item = cache->items[id];
    if (item == NULL) {
        NVMEV_ASSERT(false);
        return NULL;
    }

    NVMEV_DEBUG("%s moving %u to head of the list in update.\n", 
                 __func__, id);

    // Move the item to the head of the list
    if (item != cache->head) {
        if (item == cache->tail) {
            cache->tail = item->prev;
            cache->tail->next = NULL;
        } else {
            item->prev->next = item->next;
            item->next->prev = item->prev;
        }

        item->prev = NULL;
        item->next = cache->head;
        cache->head->prev = item;
        cache->head = item;
    }

    return item;
}

void lru_cache_set(struct lru_cache *cache, struct item *item) {
    uint32_t id = item->id;
    
    NVMEV_DEBUG("Adding buf %u to LRU.\n", id);
    if (cache->size == cache->capacity) {
        // Remove the least recently used item
        struct item *lru = cache->tail;
        NVMEV_DEBUG("We were at capacity. Dropping %u.\n", lru->id);
        cache->tail = lru->prev;
        cache->tail->next = NULL;
        cache->items[lru->id] = NULL;
        //kfree(lru);
        cache->size--;
    }

    // Add the new item to the head of the list
    item->prev = NULL;
    item->next = cache->head;
    if (cache->head != NULL) {
        cache->head->prev = item;
    } else {
        cache->tail = item;
    }
    cache->head = item;
    cache->items[id] = item;
    cache->size++;
    NVMEV_DEBUG("LRU cache head is now %u\n", item->id);
}

void lru_cache_remove(struct lru_cache *cache, uint32_t id) {
    if (cache->size == 0 || id >= MAX_CAPACITY) {
        NVMEV_ASSERT(false);
        return;
    }

    NVMEV_DEBUG("Trying to remove item with ID %u\n", id);
    struct item *item = cache->items[id];
    if (item == NULL) {
        NVMEV_ASSERT(false);
        return;
    }

    if (item == cache->head) {
        cache->head = item->next;
        if (cache->head != NULL) {
            cache->head->prev = NULL;
        } else {
            cache->tail = NULL;
        }
    } else if (item == cache->tail) {
        cache->tail = item->prev;
        cache->tail->next = NULL;
    } else {
        item->prev->next = item->next;
        item->next->prev = item->prev;
    }

    cache->items[id] = NULL;
    //kfree(item);
    cache->size--;
}

struct item *lru_cache_get_oldest(struct lru_cache *cache) {
	if (cache->size == 0) {
		return NULL;
	}

	struct item *oldest = cache->tail;

	// Remove the oldest item from the list
	if (oldest == cache->head) {
		cache->head = NULL;
		cache->tail = NULL;
	} else {
		cache->tail = oldest->prev;
		cache->tail->next = NULL;
	}

	cache->items[oldest->id] = NULL;
	cache->size--;

    NVMEV_DEBUG("Removed buf %u from cache in get_oldest.\n", oldest->id);
	return oldest;
}
