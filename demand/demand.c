/*
 * Demand-based FTL Interface
 */

#include "demand.h"
#include "page.h"
#include "cache.h"

#define _PPS (_PPB*BPS)

#include "./interface/interface.h"
#ifdef HASH_KVSSD
#include "./include/utils/sha256.h"
#endif

struct algorithm __demand = {
	.argument_set = demand_argument_set,
	.create = demand_create,
	.destroy = demand_destroy,
	.read = demand_read,
	.write = demand_write,
	.remove  = demand_remove,
	.iter_create = NULL,
	.iter_next = NULL,
	.iter_next_with_value = NULL,
	.iter_release = NULL,
	.iter_all_key = NULL,
	.iter_all_value = NULL,
	.multi_set = NULL,
	.multi_get = NULL,
	.range_query =NULL 
};

struct demand_env d_env;
struct demand_member d_member;
struct demand_stat d_stat;

struct demand_cache *d_cache;

#ifdef HASH_KVSSD
KEYT key_max, key_min;
#endif


uint32_t demand_argument_set(int argc, char **argv) {
    printk("demand_argument_set FIXME.\n");

	//int c;
	//bool ci_flag = false;
	//bool cr_flag = false;

	//while ((c=getopt(argc, argv, "cr")) != -1) {
	//	switch (c) {
	//	case 'c':
	//		ci_flag = true;
	//		printk("cache id:%s\n", argv[optind]);
	//		d_env.cache_id = atoi(argv[optind]);
	//		break;
	//	case 'r':
	//		cr_flag = true;
	//		printk("caching ratio:%s\n", argv[optind]);
	//		d_env.caching_ratio = (float)atoi(argv[optind])/100;
	//		break;
	//	}
	//}

	//if (!ci_flag) d_env.cache_id = COARSE_GRAINED;
	//if (!cr_flag) d_env.caching_ratio = 0.25;

	return 0;
}


static void print_demand_env(const struct demand_env *_env) {
	printk("\n");

#ifdef HASH_KVSSD
	printk(" |---------- algorithm_log : Hash-based Demand KV FTL\n");
#else
	printk(" |---------- algorithm_log : Demand-based FTL\n");
#endif
	printk(" | Total Blocks:         %d\n",   _env->nr_blocks);
	printk(" |  -Translation Blocks: %d\n", _env->nr_tblks);
	printk(" |  -Data Blocks:        %d\n", _env->nr_dblks);
	printk(" | Total Pages:            %d\n", _env->nr_pages);
	printk(" |  -Translation Pages:    %d\n", _env->nr_tpages);
	printk(" |  -Data Pages:           %d\n", _env->nr_dpages);
#ifdef DVALUE
	printk(" |    -Data Grains:        %d\n", _env->nr_dgrains);
#endif
	printk(" |  -Page per Segment:     %d\n", _PPS);
/*	printk(" | Total cache pages:      %d\n", _env->nr_valid_tpages);
	printk(" |  -Mixed Cache pages:    %d\n", _env->max_cached_tpages);
	printk(" |  -Cache Percentage:     %0.3f%%\n", _env->caching_ratio * 100); */
	printk(" | WriteBuffer flush size: %lld\n", _env->wb_flush_size);
	printk(" |\n");
	printk(" | ! Assume no Shadow buffer\n");
	printk(" |---------- algorithm_log END\n");

	printk("\n");
}

static void demand_env_init(struct demand_env *const _env, const struct ssdparams *spp,
                            uint64_t size) {
	_env->nr_pages = spp->tt_pgs;
	_env->nr_blocks = spp->tt_blks;

	_env->nr_tblks = spp->tt_map_blks;
	_env->nr_tpages = spp->tt_map_blks * spp->pgs_per_blk;
	_env->nr_dblks = spp->tt_data_blks;
	_env->nr_dpages = spp->tt_data_blks * spp->pgs_per_blk;

/*	_env->caching_ratio = CACHING_RATIO;
	_env->nr_tpages_optimal_caching = _env->nr_pages * 4 / PAGESIZE;
	_env->nr_valid_tpages = _env->nr_pages * ENTRY_SIZE / PAGESIZE;
	_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio; */

#ifdef WRITE_BACK
	_env->wb_flush_size = MAX_WRITE_BUF;
#else
	_env->wb_flush_size = 1;
#endif

#ifdef PART_CACHE
	_env->part_ratio = PART_RATIO;
	_env->max_clean_tpages = _env->max_cached_tpages * _env->part_ratio;
	_env->max_dirty_tentries = (_env->max_cached_tpages - _env->max_clean_tpages) * PAGESIZE / (ENTRY_SIZE + 4); // (Dirty cache size) / (Entry size)
#endif

#ifdef DVALUE
	_env->nr_grains = _env->nr_pages * GRAIN_PER_PAGE;
	_env->nr_dgrains = _env->nr_dpages * GRAIN_PER_PAGE;
	//_env->nr_valid_tpages *= GRAIN_PER_PAGE;
#endif

    _env->size = size;

	print_demand_env(_env);
}

static int demand_member_init(struct demand_member *const _member, const struct ssd *ssd) {

#ifdef HASH_KVSSD
	key_max.key = (char *)kzalloc(sizeof(char) * MAXKEYSIZE, GFP_KERNEL);
	key_max.len = MAXKEYSIZE;
	memset(key_max.key, -1, sizeof(char) * MAXKEYSIZE);

	key_min.key = (char *)kzalloc(sizeof(char) * MAXKEYSIZE, GFP_KERNEL);
	key_min.len = MAXKEYSIZE;
	memset(key_min.key, 0, sizeof(char) * MAXKEYSIZE);
#endif

	mutex_init(&_member->op_lock);

	_member->write_buffer = skiplist_init();

	q_init(&_member->flying_q, d_env.wb_flush_size);
	q_init(&_member->blocked_q, d_env.wb_flush_size);
	//q_init(&_member->wb_cmt_load_q, d_env.wb_flush_size);
	q_init(&_member->wb_master_q, d_env.wb_flush_size);
	q_init(&_member->wb_retry_q, d_env.wb_flush_size);

	struct flush_list *fl = (struct flush_list *)kzalloc(sizeof(struct flush_list), GFP_KERNEL);
	fl->size = 0;
	fl->list = (struct flush_node *)kzalloc(d_env.wb_flush_size * sizeof(struct flush_node), GFP_KERNEL);
	_member->flush_list = fl;

#ifdef HASH_KVSSD
	_member->max_try = 0;
#endif

	_member->hash_table = d_htable_init(d_env.wb_flush_size * 2);
    _member->ssd = ssd;

	return 0;
}

static void demand_stat_init(struct demand_stat *const _stat) {

}

uint32_t demand_create(lower_info *li, blockmanager *bm, 
                       algorithm *algo, const struct ssd *ssd,
                       uint64_t size) {

	/* map modules */
	algo->li = li;
	algo->bm = bm;

	/* init env */
	demand_env_init(&d_env, &ssd->sp, size);
	/* init member */
	demand_member_init(&d_member, ssd);
	/* init stat */
	demand_stat_init(&d_stat);

	d_cache = select_cache((cache_t)d_env.cache_id);

	///* create() for range query */
	//range_create();

	/* create() for page allocation module */
	page_create(bm);

#ifdef DVALUE
	/* create() for grain functions */
	grain_create();
#endif

	return 0;
}

static int count_filled_entry(void) {
	int ret = 0;
	for (int i = 0; i < d_cache->env.nr_valid_tpages; i++) {
		struct pt_struct *pt = d_cache->member.mem_table[i];
		for (int j = 0; j < EPP; j++) {
			if (pt[j].ppa != UINT_MAX) {
				ret++;
			}
		}
	}
	return ret;
}

static void print_hash_collision_cdf(uint64_t *hc) {

	int total = 0;
	for (int i = 0; i < MAX_HASH_COLLISION; i++) {
		total += hc[i];
	}

    printk("Total HC this time %d\n", total);

	int _cdf = 0;
	for (int i = 0; i < MAX_HASH_COLLISION; i++) {
		if (hc[i]) {
			_cdf += 100 * hc[i] / total;
			printk("%d,%lld,%d\n", i, hc[i], _cdf);
		}
	}
}

void print_demand_stat(struct demand_stat *const _stat) {
	/* device traffic */
	printk("================");
	printk(" Device Traffic ");
	printk("================");

	printk("Data_Read:  \t%lld\n", _stat->data_r);
	printk("Data_Write: \t%lld\n", _stat->data_w);
	printk("\n");
	printk("Trans_Read: \t%lld\n", _stat->trans_r);
	printk("Trans_Write:\t%lld\n", _stat->trans_w);
	printk("\n");
	printk("DataGC cnt: \t%lld\n", _stat->dgc_cnt);
	printk("DataGC_DR:  \t%lld\n", _stat->data_r_dgc);
	printk("DataGC_DW:  \t%lld\n", _stat->data_w_dgc);
	printk("DataGC_TR:  \t%lld\n", _stat->trans_r_dgc);
	printk("DataGC_TW:  \t%lld\n", _stat->trans_w_dgc);
	printk("\n");
	printk("TransGC cnt:\t%lld\n", _stat->tgc_cnt);
	printk("TransGC_TR: \t%lld\n", _stat->trans_r_tgc);
	printk("TransGC_TW: \t%lld\n", _stat->trans_w_tgc);
	printk("\n");

	int amplified_read = _stat->trans_r + _stat->data_r_dgc + _stat->trans_r_dgc + _stat->trans_r_tgc;
	int amplified_write = _stat->trans_w + _stat->data_w_dgc + _stat->trans_w_dgc + _stat->trans_w_tgc;

    if(_stat->data_r > 0) {
        printk("RAF: %lld\n", 100 * (_stat->data_r + amplified_read) /_stat->data_r);
    }

    if(_stat->data_w > 0) {
        printk("WAF: %lld\n",  100 * (_stat->data_w + amplified_write)/_stat->data_w);
    }
	printk("\n");

	/* r/w specific traffic */
	printk("==============");
	printk(" R/W analysis ");
	printk("==============");

	printk("[Read]");
	printk("*Read Reqs: \t%lld\n", _stat->read_req_cnt);
	printk("Data read:  \t%lld (+%lld Write-buffer hits)\n", _stat->d_read_on_read, _stat->wb_hit);
	printk("Data write: \t%lld\n", _stat->d_write_on_read);
	printk("Trans read: \t%lld\n", _stat->t_read_on_read);
	printk("Trans write:\t%lld\n", _stat->t_write_on_read);
	printk("\n");

	printk("[Write]");
	printk("*Write Reqs:\t%lld\n", _stat->write_req_cnt);
	printk("Data read:  \t%lld\n", _stat->d_read_on_write);
	printk("Data write: \t%lld\n", _stat->d_write_on_write);
	printk("Trans read: \t%lld\n", _stat->t_read_on_write);
	printk("Trans write:\t%lld\n", _stat->t_write_on_write);
	printk("\n");

	/* write buffer */
	printk("==============");
	printk(" Write Buffer ");
	printk("==============");

	printk("Write-buffer Hit cnt: %lld\n", _stat->wb_hit);
	printk("\n");


#ifdef HASH_KVSSD
	printk("================");
	printk(" Hash Collision ");
	printk("================");

	printk("[Overall Hash-table Load Factor]");
	int filled_entry_cnt = count_filled_entry();
	int total_entry_cnt = d_cache->env.nr_valid_tentries;
	printk("Total entry:  %d\n", total_entry_cnt);
	printk("Filled entry: %d\n", filled_entry_cnt);
	printk("Load factor: %d%%\n", 100 * (filled_entry_cnt/total_entry_cnt*100));
	printk("\n");

	printk("[write(insertion)]");
	print_hash_collision_cdf(_stat->w_hash_collision_cnt);

	printk("[read]");
	print_hash_collision_cdf(_stat->r_hash_collision_cnt);
	printk("\n");

	printk("=======================");
	printk(" Fingerprint Collision ");
	printk("=======================");

	printk("[Read]");
	printk("fp_match:     %lld\n", _stat->fp_match_r);
	printk("fp_collision: %lld\n", _stat->fp_collision_r);

    if(_stat->fp_match_r + _stat->fp_collision_r > 0) {
        printk("rate: %llu\n", 100 * _stat->fp_collision_r/(_stat->fp_match_r+_stat->fp_collision_r)*100);
    }
    printk("\n");

	printk("[Write]");
	printk("fp_match:     %lld\n", _stat->fp_match_w);
	printk("fp_collision: %lld\n", _stat->fp_collision_w);

    if(_stat->fp_match_w + _stat->fp_collision_w > 0) {
        printk("rate: %lld\n", 100 * _stat->fp_collision_w/(_stat->fp_match_w+_stat->fp_collision_w)*100);
    }
	printk("\n");
#endif
}

static void demand_member_kfree(struct demand_member *const _member) {
/*	for (int i = 0; i < d_env.nr_valid_tpages; i++) {
		q_free(_member->cmt[i]->blocked_q);
		q_free(_member->cmt[i]->wait_q);
		kfree(_member->cmt[i]);
	}
	kfree(_member->cmt);

	for(int i=0;i<d_env.nr_valid_tpages;i++) {
		kfree(_member->mem_table[i]);
	}
	kfree(_member->mem_table);

	lru_kfree(_member->lru); */
	skiplist_kfree(_member->write_buffer);

	q_free(_member->flying_q);
	q_free(_member->blocked_q);
	//q_free(_member->wb_cmt_load_q);
	q_free(_member->wb_master_q);
	q_free(_member->wb_retry_q);

#ifdef PART_CACHE
	q_free(&_member->wait_q);
	q_free(&_member->write_q);
	q_free(&_member->flying_q);
#endif
}

void demand_destroy(lower_info *li, algorithm *algo){

	/* print stat */
	print_demand_stat(&d_stat);

	/* free member */
	demand_member_kfree(&d_member);

	/* cleanup cache */
	d_cache->destroy();
}

#ifdef HASH_KVSSD
static uint32_t hashing_key(char* key,uint8_t len) {
	char* string;
	Sha256Context ctx;
	SHA256_HASH hash;
	int bytes_arr[8];
	uint32_t hashkey;

	string = key;

	Sha256Initialise(&ctx);
	Sha256Update(&ctx, (unsigned char*)string, len);
	Sha256Finalise(&ctx, &hash);

	for(int i=0; i<8; i++) {
		bytes_arr[i] = ((hash.bytes[i*4] << 24) | (hash.bytes[i*4+1] << 16) | \
				(hash.bytes[i*4+2] << 8) | (hash.bytes[i*4+3]));
	}

	hashkey = bytes_arr[0];
	for(int i=1; i<8; i++) {
		hashkey ^= bytes_arr[i];
	}

	return hashkey;
}

static uint32_t hashing_key_fp(char* key,uint8_t len) {
	char* string;
	Sha256Context ctx;
	SHA256_HASH hash;
	int bytes_arr[8];
	uint32_t hashkey;

	string = key;

	Sha256Initialise(&ctx);
	Sha256Update(&ctx, (unsigned char*)string, len);
	Sha256Finalise(&ctx, &hash);

	for(int i=0; i<8; i++) {
		bytes_arr[i] = ((hash.bytes[i*4]) | (hash.bytes[i*4+1] << 8) | \
				(hash.bytes[i*4+2] << 16) | (hash.bytes[i*4+3] << 24));
	}

	hashkey = bytes_arr[0];
	for(int i=1; i<8; i++) {
		hashkey ^= bytes_arr[i];
	}

	return (hashkey & ((1<<FP_SIZE)-1));
}

static struct hash_params *make_hash_params(request *const req) {
	struct hash_params *h_params = (struct hash_params *)kzalloc(sizeof(struct hash_params), GFP_KERNEL);
	h_params->hash = hashing_key(req->key.key, req->key.len);
#ifdef STORE_KEY_FP
	h_params->key_fp = hashing_key_fp(req->key.key, req->key.len);
#endif
	h_params->cnt = 0;
	h_params->find = HASH_KEY_INITIAL;
	h_params->lpa = 0;

	return h_params;
}
#endif

uint32_t demand_read(request *const req){
	uint32_t rc;
	mutex_lock(&d_member.op_lock);
#ifdef HASH_KVSSD
	if (!req->hash_params) {
		d_stat.read_req_cnt++;
		req->hash_params = (void *)make_hash_params(req);
	}
#endif
	rc = __demand_read(req);
	if (rc == UINT_MAX) {
		req->type = FS_NOTFOUND_T;
		req->end_req(req);
	}
	mutex_unlock(&d_member.op_lock);
	return 0;
}

uint64_t demand_write(request *const req) {
	uint32_t rc;
	mutex_lock(&d_member.op_lock);
#ifdef HASH_KVSSD
	if (!req->hash_params) {
		d_stat.write_req_cnt++;
		req->hash_params = (void *)make_hash_params(req);
	}
#endif
	rc = __demand_write(req);
	mutex_unlock(&d_member.op_lock);
	return rc;
}

uint32_t demand_remove(request *const req) {
	int rc;
	rc = __demand_remove(req);
	req->end_req(req);
	return 0;
}

