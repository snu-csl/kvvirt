int midas_put(void* k, char* v, uint64_t vlen);
int midas_get(const void* k, char* v_out, int *vlen_out);
int midas_open();
int midas_close();
