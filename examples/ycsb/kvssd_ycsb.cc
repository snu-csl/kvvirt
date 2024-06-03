#include <shared_mutex>

#include "kvssd.h"
#include "lrucache.hpp"

const uint32_t NUM_CACHE_SHARDS = 31;

KVSSD kvssd("/dev/nvme0n1");
std::shared_mutex cache_locks[NUM_CACHE_SHARDS];
cache::lru_cache<uint64_t, std::pair<char*, uint64_t>> *value_cache[NUM_CACHE_SHARDS];

std::mutex open_lock;
bool opened = false;
uint32_t open_count = 0;
bool have_cache = false;

int open(const char* dir, uint64_t vlen, uint64_t cache_size_mb, bool create) {
    open_lock.lock();

    if(!opened) {
        kvssd.SetPath(dir);
        kvssd.Open();
        kvssd.SetBufferLen(vlen);

        if(cache_size_mb > 0) {
            uint64_t cache_size_b = cache_size_mb << 20;
            for(int i = 0; i < NUM_CACHE_SHARDS; i++) {
                value_cache[i] = 
                    new cache::lru_cache<uint64_t, std::pair<char*, uint64_t>>(cache_size_b / NUM_CACHE_SHARDS);
                cache_locks[i].lock();
                cache_locks[i].unlock();
            }
            have_cache = true;
        } else {
            have_cache = false;
        }

        opened = true;
    }

    open_count++;
    open_lock.unlock();
    return 0;
}

int close() {
    open_lock.lock();
    if(--open_count == 0) {
        kvssd.Close();
        opened = false;
    }
    open_lock.unlock();
    return 0;
}

static std::atomic<uint64_t> total;
static std::atomic<uint64_t> hits;
bool get_from_cache(uint64_t key, char* out, uint64_t *vlen_out) {
	if(!have_cache) {
		return 0;
	}
    bool found = false;
    uint64_t shard = key % NUM_CACHE_SHARDS;

    total++;

    cache_locks[shard].lock_shared();
    auto res = value_cache[shard]->get(key);
    if(res.first != NULL) {
        memcpy(out, res.first, res.second);
        if(vlen_out) {
            *vlen_out = res.second;
        }
        hits++;
        found = true;
    }
    cache_locks[shard].unlock_shared();

    return found;
}

void add_to_cache(uint64_t key, char* buf, uint64_t vlen) {
	if(!have_cache) {
		return;
	}
    uint64_t shard = key % NUM_CACHE_SHARDS;
    char* v = (char*) malloc(vlen);
    memcpy(v, buf, vlen);

    cache_locks[shard].lock();
    value_cache[shard]->put(key, std::make_pair(v, vlen));
    cache_locks[shard].unlock();
}

int put(uint64_t k, char* v, uint64_t vlen, bool append) {
    if(kvssd.Store(k, v, vlen, append)) {
        return 1;
    } else {
        add_to_cache(k, v, vlen);
        return 0;
    }
}

int get(uint64_t k, char *out, uint64_t *vlen_out) {
    if(!get_from_cache(k, out, vlen_out)) {
        if(kvssd.Retrieve(k, out, vlen_out)) {
            return 1;
        }

        add_to_cache(k, out, *vlen_out);
    }

    return 0;
}

int rmw(uint64_t k, uint64_t vlen) {
    uint32_t sz = vlen + (4096 - (vlen % 4096));
    char* out = (char*) aligned_alloc(4096, sz);
    uint64_t vlen_out = UINT64_MAX;

    if(get(k, out, &vlen_out)) {
        return 1;
    }

    out[vlen / 2] = 'X';
    assert(!put(k, out, vlen, false));
    free(out);

    return 0;
}

int scan(uint64_t, uint64_t, char*) {
    return 0;
}

int empty_cache() {
    for(int i = 0; i < NUM_CACHE_SHARDS; i++) {
        if(value_cache[i]) {
            value_cache[i]->free_all();
        }
    }

    return 0;
}
