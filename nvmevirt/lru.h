#define MAX_CAPACITY 128

struct item {
    uint32_t id;
    struct item *prev;
    struct item *next;
};

struct lru_cache {
    struct item *head;
    struct item *tail;
    uint32_t capacity;
    uint32_t size;
    struct item *items[MAX_CAPACITY];
};

void lru_cache_init(struct lru_cache *cache, uint32_t capacity);
struct item *lru_cache_get(struct lru_cache *cache, uint32_t id);
void lru_cache_set(struct lru_cache *cache, struct item *item);
void lru_cache_remove(struct lru_cache *cache, uint32_t id);
struct item *lru_cache_get_oldest(struct lru_cache *cache);
