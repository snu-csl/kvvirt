#include "../include/container.h"

uint32_t virt_create(lower_info*, blockmanager *bm);
void *virt_destroy(lower_info*);
uint64_t virt_push_data(uint32_t ppa, uint32_t size, value_set *value,bool async, algo_req * const req);
uint64_t virt_pull_data(uint32_t ppa, uint32_t size, value_set* value,bool async,algo_req * const req);
void* virt_trim_block(uint32_t ppa,bool async);
void *virt_refresh(lower_info*);
void virt_stop(void);
void virt_flying_req_wait(void);
