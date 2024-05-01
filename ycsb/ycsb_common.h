#ifdef __NEWSTORE
int open(const char* dir, uint64_t vlen, uint64_t cache_size_mb, bool create, uint64_t chunk_len, uint64_t wal_write_size, uint64_t num_pairs, uint64_t wal_size_mb);
#else
int open(const char* dir, uint64_t vlen, uint64_t cache_size_mb, bool create);
#endif
int close();
int put(uint64_t k, char* v, uint64_t vlen, bool sync);
int get(uint64_t k, char *out, uint64_t *vlen_out);
int scan(uint64_t start_k, uint64_t len, char* out);
int rmw(uint64_t k, uint64_t vlen);
int empty_cache();
