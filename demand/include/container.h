#ifndef __H_CONTAINER__
#define __H_CONTAINER__
 
#include "../../nvme_kv.h"
#include "../../nvmev.h"
#include "../../ssd.h"
#include "sem_lock.h"
#include "settings.h"
#include "types.h"
#include "utils.h"

#include <linux/mutex.h>
#include <stdarg.h>

struct demand_shard;

typedef struct lower_info lower_info;
typedef struct algorithm algorithm;
typedef struct algo_req algo_req;
typedef struct request request;
typedef struct blockmanager blockmanager;

typedef struct value_set{
    struct demand_shard *shard;
	PTR value;
	uint32_t length;
	int dmatag; //-1 == not dma_alloc, others== dma_alloc
	ppa_t ppa;
	bool from_app;
	PTR rmw_value;
	uint8_t status;
	uint32_t len;
	uint32_t offset;
}value_set;

struct request {
	FSTYPE type;
	KEYT key;
	ppa_t ppa;/*it can be the iter_idx*/
	uint32_t seq;
	volatile int num; /*length of requests*/
	volatile int cpl; /*number of completed requests*/
	int not_found_cnt;
	value_set *value;
	value_set **multi_value;
	char **app_result;

	KEYT *multi_key;
	bool (*end_req)(struct request *const);
	void *(*special_func)(void *);
	bool (*added_end_req)(struct request *const);
	bool isAsync;
	void *p_req;
	void (*p_end_req)(uint32_t,uint32_t,void*);
	void *params;
	void *__hash_node;
	//struct mutex async_mutex;
	fdriver_lock_t sync_lock;
	int mark;

/*s:for application req*/
	char *target_buf;
	uint32_t inter_offset;
	uint32_t target_len;
	char istophalf;
	FSTYPE org_type;
/*e:for application req*/

	uint8_t type_ftl;
	uint8_t type_lower;
	uint8_t before_type_lower;
	bool isstart;
	//MeasureTime latency_checker;

	/* HASH_KVSSD */
	void *hash_params;
	struct request *parents;

    /* NVMeVirt */
    struct demand_shard *shard;
    uint64_t nsecs_start;
    struct ssd *ssd;
    uint64_t sqid;
    struct nvme_kv_command *cmd;
    value_set *mapping_v;
};

struct algo_req {
	ppa_t ppa;
	request * parents;
	//MeasureTime latency_lower;
	uint8_t type;
	bool rapid;
	uint8_t type_lower;
	//0: normal, 1 : no tag, 2: read delay 4:write delay
	void *(*end_req)(struct algo_req *const);
	void *params;

    /*
     * NVMeVirt.
     */

    uint64_t sqid;
    bool need_retry;
};

struct lower_info {
	uint32_t (*create)(struct lower_info*, blockmanager *bm);
	void* (*destroy)(struct lower_info*);
	uint64_t (*write)(ppa_t ppa, uint32_t size, 
                      value_set *value, bool async,
                      algo_req * const req);
	uint64_t (*read)(ppa_t ppa, uint32_t size, 
                  value_set *value, bool async,
                  algo_req * const req);
	void* (*read_hw)(ppa_t ppa, char *key,uint32_t key_len, value_set *value,bool async,algo_req * const req);
	void* (*device_badblock_checker)(ppa_t ppa,uint32_t size,void *(*process)(uint64_t, uint8_t));
	void* (*trim_block)(ppa_t ppa,bool async);
	void* (*trim_a_block)(ppa_t ppa,bool async);
	void* (*refresh)(struct lower_info*);
	void (*stop)(void);
	int (*lower_alloc) (int type, char** buf);
	void (*lower_free) (int type, int dmaTag);
	void (*lower_flying_req_wait) (void);
	void (*lower_show_info)(void);
	uint32_t (*lower_tag_num)(void);
	uint32_t (*hw_do_merge)(uint32_t lp_num, ppa_t *lp_array, uint32_t hp_num,ppa_t *hp_array,ppa_t *tp_array, uint32_t* ktable_num, uint32_t *invliadate_num);
	char *(*hw_get_kt)(void);
	char *(*hw_get_inv)(void);
	struct blockmanager *bm;

	lower_status (*statusOfblock)(BLOCKT);
	
	uint64_t write_op;
	uint64_t read_op;
	uint64_t trim_op;

	uint32_t NOB;
	uint32_t NOP;
	uint32_t SOK;
	uint32_t SOB;
	uint32_t SOP;
	uint32_t PPB;
	uint32_t PPS;
	uint64_t TS;
	uint64_t DEV_SIZE;//for sacle up test
	uint64_t all_pages_in_dev;//for scale up test

	uint64_t req_type_cnt[LREQ_TYPE_NUM];
	//anything
};

struct algorithm{
	/*interface*/
	uint32_t (*argument_set) (int argc, char**argv);
	uint32_t (*create) (struct demand_shard*, lower_info*, blockmanager *bm, 
                        struct algorithm *, struct ssd*, uint64_t size);
	void (*destroy) (struct demand_shard*, lower_info*, struct algorithm *);
	uint32_t (*read)(void*, uint64_t*, uint64_t*);
	uint64_t (*write)(void*, uint64_t*, uint64_t*);
	uint32_t (*remove)(void*, uint64_t*, uint64_t*);
#ifdef KVSSD
	uint32_t (*iter_create)(request *const);
	uint32_t (*iter_next)(request *const);
	uint32_t (*iter_next_with_value)(request *const);
	uint32_t (*iter_release)(request *const);
	uint32_t (*iter_all_key)(request *const);
	uint32_t (*iter_all_value)(request *const);
	uint32_t (*multi_set)(request *const,int num);
	uint32_t (*multi_get)(request *const,int num);
	uint32_t (*range_query)(request *const);
    uint64_t (*append)(struct demand_shard*, request *const);
#endif
	lower_info* li;
	struct blockmanager *bm;
	void *algo_body;
};

#endif
