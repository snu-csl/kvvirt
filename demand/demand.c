/*
 * Demand-based FTL Interface
 */

#include "cache.h"
#include "../city.h"
#include "demand.h"

#include "./interface/interface.h"
#ifdef HASH_KVSSD
#include "./include/utils/sha256.h"
#endif

struct algorithm __demand = {
	.argument_set = demand_argument_set,
	.create = demand_create,
	.destroy = demand_destroy,
};

struct demand_stat d_stat;

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
/*	printk(" | Total cache pages:      %d\n", _env->nr_valid_tpages);
	printk(" |  -Mixed Cache pages:    %d\n", _env->max_cached_tpages);
	printk(" |  -Cache Percentage:     %0.3f%%\n", _env->caching_ratio * 100); */
	printk(" | WriteBuffer flush size: %lld\n", _env->wb_flush_size);
	printk(" |\n");
	printk(" | ! Assume no Shadow buffer\n");
    printk(" | We can write %u 1024B pairs.\n", _env->nr_dgrains / (1024 / GRAIN_PER_PAGE));
	printk(" |---------- algorithm_log END\n");

	printk("\n");
}

static void demand_env_init(struct demand_env *const _env, const struct ssdparams *spp,
                            uint64_t size) {
	_env->nr_pages = spp->tt_pgs;
	_env->nr_blocks = spp->tt_blks;

	_env->nr_tblks = spp->tt_map_pgs / spp->pgs_per_blk;
	_env->nr_tpages = spp->tt_map_pgs;
	_env->nr_dblks = spp->tt_data_pgs / spp->pgs_per_blk;
	_env->nr_dpages = spp->tt_data_pgs;

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

static int demand_member_init(struct demand_shard *shard, struct ssd *ssd) {
    struct demand_env *env = shard->env;
    struct demand_member *const _member = shard->ftl;
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

	q_init(&_member->flying_q, env->wb_flush_size);
	q_init(&_member->blocked_q, env->wb_flush_size);
	//q_init(&_member->wb_cmt_load_q, d_env.wb_flush_size);
	q_init(&_member->wb_master_q, env->wb_flush_size);
	q_init(&_member->wb_retry_q, env->wb_flush_size);

	struct flush_list *fl = (struct flush_list *)kzalloc(sizeof(struct flush_list), GFP_KERNEL);
	fl->size = 0;
	fl->list = (struct flush_node *)kzalloc(env->wb_flush_size * 
                                    sizeof(struct flush_node), GFP_KERNEL);
	_member->flush_list = fl;

#ifdef HASH_KVSSD
	_member->max_try = 0;
#endif

	return 0;
}

static void demand_stat_init(struct demand_stat *const _stat) {

}

uint32_t demand_create(struct demand_shard *shard, lower_info *li, 
                       blockmanager *bm, algorithm *algo, struct ssd *ssd,
                       uint64_t size) {

	/* map modules */
	algo->li = li;

	/* init env */
	demand_env_init(shard->env, &ssd->sp, size);
	/* init member */
	demand_member_init(shard, ssd);
	/* init stat */
	demand_stat_init(&d_stat);

    shard->env->cache_id = OLD_COARSE_GRAINED;

	//shard->cache = select_cache(shard, (cache_t)d_env.cache_id);

	return 0;
}

static int count_filled_entry(struct demand_cache *cache) {
    return 0;
//	int ret = 0;
//    value_set *v = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
//
//    for (int i = 0; i < cache->env.nr_valid_tpages; i++) {
//        struct cmt_struct *cmt = cache->member.cmt[i];
//#ifdef GC_STANDARD
//        if(cmt->t_ppa != UINT_MAX) {
//            if(cmt->pt == NULL) {
//                cmt->pt = kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
//                for(int i = 0; i < EPP; i++) {
//                    cmt->pt[i].ppa = UINT_MAX;
//                }
//
//                BUG_ON(!v->value);
//                memcpy(v->value, nvmev_vdev->ns[0].mapped + (cmt->t_ppa * PAGESIZE), 
//                        PAGESIZE);
//                __page_to_pte(v, cmt->pt, cmt->idx);
//            }
//
//            for (int j = 0; j < EPP; j++) {
//                if (cmt->pt[j].ppa != UINT_MAX) {
//                    ret++;
//                }
//            }
//        }
//#else
//        if(cmt->t_ppa != UINT_MAX) {
//            if(cmt->pt == NULL) {
//                memcpy(v->value, nvmev_vdev->ns[0].mapped + (cmt->t_ppa * PAGESIZE), 
//                        PAGESIZE);
//                __page_to_ptes(v, cmt->idx, false);
//            }
//
//            for (int j = 0; j < EPP; j++) {
//                if (cmt->pt[j].ppa != UINT_MAX) {
//                    ret++;
//                }
//            }
//        }
//#endif
//    }
//	return ret;
}

static unsigned int __buf_copy(struct nvme_kv_command *cmd, void *buf, 
                               uint64_t buf_len)
{
	size_t offset;
	size_t length, remaining;
	int prp_offs = 0;
	int prp2_offs = 0;
	u64 paddr;
	u64 *paddr_list = NULL;
	size_t nsid = 0;  // 0-based

    bool read = cmd->common.opcode == nvme_cmd_kv_retrieve;

    nsid = 0;

    if(read) {
        length = buf_len;
    } else {
        length = cmd->kv_store.value_len << 2;
    }

	remaining = length;
	while (remaining) {
		size_t io_size;
		void *vaddr;
		size_t mem_offs = 0;

		prp_offs++;
		if (prp_offs == 1) {
            if(read) {
                paddr = cmd->kv_retrieve.dptr.prp1;
            } else {
                paddr = cmd->kv_store.dptr.prp1;
            }
		} else if (prp_offs == 2) {
            if(read) {
                paddr = cmd->kv_retrieve.dptr.prp2;
            } else {
                paddr = cmd->kv_store.dptr.prp2;
            }
			if (remaining > PAGE_SIZE) {
				paddr_list = kmap_atomic_pfn(PRP_PFN(paddr)) +
					     (paddr & PAGE_OFFSET_MASK);
				paddr = paddr_list[prp2_offs++];
			}
		} else {
			paddr = paddr_list[prp2_offs++];
		}

		vaddr = kmap_atomic_pfn(PRP_PFN(paddr));
		io_size = min_t(size_t, remaining, PAGE_SIZE);

		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

		if (!read) {
			memcpy(buf, vaddr + mem_offs, io_size);
		} else {
			memcpy(vaddr + mem_offs, buf, io_size);
		}

		kunmap_atomic(vaddr);

		remaining -= io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);

	return length;
}

static uint32_t get_hash_collision_cdf(uint64_t *hc, char* out) {
    char *buf = (char*) kzalloc(4096, GFP_KERNEL);
    int off = 0;

	int total = 0;
	for (int i = 0; i < MAX_HASH_COLLISION; i++) {
		total += hc[i];
	}

    sprintf(buf, "Total HC this time %d\n", total);
    off = strlen(buf);

	int _cdf = 0;
	for (int i = 0; i < MAX_HASH_COLLISION; i++) {
		if (hc[i]) {
			_cdf += 100 * hc[i] / total;
			sprintf(buf + off, "%d,%lld,%d\n", i, hc[i], _cdf);
            off = strlen(buf);
		}
	}

    memcpy(out, buf, strlen(buf));
    kfree(buf);
    return strlen(buf);
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

void clear_demand_stat(void) {
    for(int i = 0; i < MAX_HASH_COLLISION; i++) {
        d_stat.w_hash_collision_cnt[i] = 0;
        d_stat.r_hash_collision_cnt[i] = 0;
    }

    /* device traffic */
    d_stat.data_r = 0;
    d_stat.data_w = 0;
    d_stat.trans_r = 0;
    d_stat.trans_w = 0;
    d_stat.data_r_dgc = 0;
    d_stat.data_w_dgc = 0;
    d_stat.trans_r_dgc = 0;
    d_stat.trans_r_dgc_2 = 0;
    d_stat.trans_w_dgc = 0;
    d_stat.trans_r_tgc = 0;
    d_stat.trans_w_tgc = 0;
    d_stat.dgc_cnt = 0;
    d_stat.tgc_cnt = 0;
    d_stat.tgc_by_read = 0;
    d_stat.tgc_by_write = 0;
    d_stat.read_req_cnt = 0;
    d_stat.write_req_cnt = 0;
    d_stat.d_read_on_read = 0;
    d_stat.d_write_on_read = 0;
    d_stat.t_read_on_read = 0;
    d_stat.t_write_on_read = 0;
    d_stat.d_read_on_write = 0;
    d_stat.d_write_on_write = 0;
    d_stat.t_read_on_write = 0;
    d_stat.t_write_on_write = 0;
    d_stat.wb_hit = 0;
    d_stat.gc_pair_copy = 0;
    d_stat.gc_invm_copy = 0;
    d_stat.gc_cmt_copy = 0;
    d_stat.fp_match_r = 0;
    d_stat.fp_match_w = 0;
    d_stat.fp_collision_r = 0;
    d_stat.fp_collision_w = 0;
    d_stat.inv_m_w = 0;
    d_stat.inv_m_r = 0;
}

char* get_demand_stat(struct demand_stat *const _stat) {
    uint32_t buf_size = 16384;
    uint32_t length = 0;
    char *ret = kzalloc(buf_size, GFP_KERNEL);
    char tmp_buf[4096];
    memset(ret, 0x0, buf_size);

	/* device traffic */
	length += snprintf(ret + length, buf_size - length, "================\n");
	length += snprintf(ret + length, buf_size - length, " Device Traffic \n");
	length += snprintf(ret + length, buf_size - length, "================\n");
    length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "Data_Read:\t%lld MB\n", 
                       _stat->data_r >> 20);
	length += snprintf(ret + length, buf_size - length, "Data_Write:\t%lld MB\n", 
                        _stat->data_w >> 20);
	length += snprintf(ret + length, buf_size - length, "\n");
	length += snprintf(ret + length, buf_size - length, "Trans_Read:\t%lld MB\n", 
                       _stat->trans_r >> 20);
	length += snprintf(ret + length, buf_size - length, "Trans_Write:\t%lld MB\n", 
                       _stat->trans_w >> 20);
	length += snprintf(ret + length, buf_size - length, "\n");
	length += snprintf(ret + length, buf_size - length, "DataGC cnt:\t%lld\n", _stat->dgc_cnt);
	length += snprintf(ret + length, buf_size - length, "DataGC_DR:\t%lld MB\n", 
                       _stat->data_r_dgc >> 20);
	length += snprintf(ret + length, buf_size - length, "DataGC_DW:\t%lld\n", 
                       _stat->data_w_dgc >> 20);
	length += snprintf(ret + length, buf_size - length, "DataGC_TR:\t%lld MB (%lld MB)\n", 
                        _stat->trans_r_dgc >> 20, _stat->trans_r_dgc_2 >> 20);
	length += snprintf(ret + length, buf_size - length, "DataGC_TW:\t%lld MB\n", 
                       _stat->trans_w_dgc >> 20);
	length += snprintf(ret + length, buf_size - length, "\n");
	length += snprintf(ret + length, buf_size - length, "TransGC cnt:\t%lld\n", _stat->tgc_cnt);
	length += snprintf(ret + length, buf_size - length, "TransGC_TR: \t%lld MB\n", 
                       _stat->trans_r_tgc >> 20);
	length += snprintf(ret + length, buf_size - length, "TransGC_TW: \t%lld MB\n", 
                       _stat->trans_w_tgc >> 20);
	length += snprintf(ret + length, buf_size - length, "\n");

	uint64_t amplified_read = _stat->trans_r + _stat->data_r_dgc + _stat->trans_r_dgc + _stat->trans_r_tgc;
	uint64_t amplified_write = _stat->trans_w + _stat->data_w_dgc + _stat->trans_w_dgc + _stat->trans_w_tgc;

    if(_stat->data_r > 0) {
        length += snprintf(ret + length, buf_size - length,
                          "AR %llu RAF: %lld\n", amplified_read,
                          (100 * (_stat->data_r + amplified_read)) /_stat->data_r);
    }

    if(_stat->data_w > 0) {
        length += snprintf(ret + length, buf_size - length,
                          "AW %llu WAF: %lld\n", amplified_write,
                          (100 * (_stat->data_w + amplified_write)) /_stat->data_w);
    }
	length += snprintf(ret + length, buf_size - length, "\n");

	/* r/w specific traffic */
	length += snprintf(ret + length, buf_size - length, "==============\n");
	length += snprintf(ret + length, buf_size - length, " R/W analysis \n");
	length += snprintf(ret + length, buf_size - length, "==============\n");
    length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "[Read]\n");
	length += snprintf(ret + length, buf_size - length, "*Read Reqs:\t%lld\n", _stat->read_req_cnt);
	length += snprintf(ret + length, buf_size - length, "Data read:\t%lld MB (+%lld Write-buffer hits)\n",                      _stat->d_read_on_read >> 20, _stat->wb_hit);
	length += snprintf(ret + length, buf_size - length, "Data write:\t%lld MB\n", 
                       _stat->d_write_on_read >> 20);
	length += snprintf(ret + length, buf_size - length, "Trans read:\t%lld MB\n", 
                       _stat->t_read_on_read >> 20);
	length += snprintf(ret + length, buf_size - length, "Trans write:\t%lld MB\n", 
                       _stat->t_write_on_read >> 20);
	length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "[Write]\n");
	length += snprintf(ret + length, buf_size - length, "*Write Reqs:\t%lld\n", _stat->write_req_cnt);
	length += snprintf(ret + length, buf_size - length, "Data read:\t%lld MB\n", 
                       _stat->d_read_on_write >> 20);
	length += snprintf(ret + length, buf_size - length, "Data write:\t%lld MB\n", 
                       _stat->d_write_on_write >> 20);
	length += snprintf(ret + length, buf_size - length, "Trans read:\t%lld MB\n", 
                       _stat->t_read_on_write >> 20);
	length += snprintf(ret + length, buf_size - length, "Trans write:\t%lld MB\n", 
                       _stat->t_write_on_write >> 20);
	length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "================\n");
	length += snprintf(ret + length, buf_size - length, " Hash Collision \n");
	length += snprintf(ret + length, buf_size - length, "================\n");
    length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "[Overall Hash-table Load Factor]\n");
	int filled_entry_cnt = count_filled_entry(NULL);
	int total_entry_cnt = 0; //d_cache->env.nr_valid_tentries;
	length += snprintf(ret + length, buf_size - length, "Total entry:  %d\n", total_entry_cnt);
	length += snprintf(ret + length, buf_size - length, "Filled entry: %d\n", filled_entry_cnt);
	//length += snprintf(ret + length, buf_size - length, "Load factor: %d%%\n", 100 * (filled_entry_cnt/total_entry_cnt*100));
	length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "[write(insertion)]\n");
    length += get_hash_collision_cdf(_stat->w_hash_collision_cnt, ret + length);

	length += snprintf(ret + length, buf_size - length, "[read]\n");
    length += get_hash_collision_cdf(_stat->r_hash_collision_cnt, ret + length);
	//length += snprintf(ret + length, buf_size - length, print_hash_collision_cdf(_stat->r_hash_collision_cnt));
	length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "=======================\n");
	length += snprintf(ret + length, buf_size - length, " Fingerprint Collision \n");
	length += snprintf(ret + length, buf_size - length, "=======================\n");
    length += snprintf(ret + length, buf_size - length, "\n");

	length += snprintf(ret + length, buf_size - length, "[Read]\n");
	length += snprintf(ret + length, buf_size - length, "fp_match:     %lld\n", _stat->fp_match_r);
	length += snprintf(ret + length, buf_size - length, "fp_collision: %lld\n", _stat->fp_collision_r);

    if(_stat->fp_match_r + _stat->fp_collision_r > 0) {
        length += snprintf(ret+length, buf_size - length, "rate: %llu\n", 
                  100 * _stat->fp_collision_r/(_stat->fp_match_r+_stat->fp_collision_r) * 100);
    }

	length += snprintf(ret + length, buf_size - length, "[Write]\n");
	length += snprintf(ret + length, buf_size - length, "fp_match:     %lld\n", _stat->fp_match_w);
	length += snprintf(ret + length, buf_size - length, "fp_collision: %lld\n", _stat->fp_collision_w);

    if(_stat->fp_match_w + _stat->fp_collision_w > 0) {
        length += snprintf(ret + length, buf_size - length, "rate: %lld\n", 
                  100 * _stat->fp_collision_w/(_stat->fp_match_w+_stat->fp_collision_w)*100);
    }

#ifndef GC_STANDARD
	length += snprintf(ret + length, buf_size - length, "=======================\n");
	length += snprintf(ret + length, buf_size - length, " Invalid Mappings \n");
	length += snprintf(ret + length, buf_size - length, "=======================\n");
    length += snprintf(ret + length, buf_size - length, "\n");

    length += snprintf(ret + length, buf_size - length, "Write: %lld MB\n", 
                       _stat->inv_m_w >> 20);
	length += snprintf(ret + length, buf_size - length, "Read: %lld MB\n\n", 
                       _stat->inv_m_r >> 20);
#endif

    return ret;
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
	printk("DataGC_TR:  \t%lld (%lld)\n", _stat->trans_r_dgc, _stat->trans_r_dgc_2);
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
	int filled_entry_cnt = 0; //count_filled_entry();
	int total_entry_cnt = 0; //d_cache->env.nr_valid_tentries;
	printk("Total entry:  %d\n", total_entry_cnt);
	printk("Filled entry: %d\n", filled_entry_cnt);
	//printk("Load factor: %d%%\n", 100 * (filled_entry_cnt/total_entry_cnt*100));
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
        printk("rate: %llu\n", 
                100 * _stat->fp_collision_r/(_stat->fp_match_r+_stat->fp_collision_r)*100);
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

void demand_destroy(struct demand_shard *shard, lower_info *li, algorithm *algo){

	/* print stat */
	print_demand_stat(&d_stat);

	/* free member */
	demand_member_kfree(shard->ftl);

	/* cleanup cache */
	shard->cache->destroy(shard->cache);
}

#ifdef HASH_KVSSD
static uint64_t hashing_key(char* key,uint8_t len, fp_t *fp) {
    //uint64_t ret = 0;
    //ret = CityHash64(key, len); 
    //return ret;

	char* string;
	Sha256Context ctx;
	SHA256_HASH hash;
	int bytes_arr[8];
	uint64_t hashkey;

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

    if(fp) {
        *fp = (hashkey & ((1<<FP_SIZE)-1));
    }

	return hashkey;
}

static uint64_t hashing_key_fp(char* key,uint8_t len) {
    //uint64_t ret = 0;
    //ret = CityHash64(data, 4096); 
    //return ret;

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
	struct hash_params *h_params = 
    (struct hash_params *)kzalloc(sizeof(struct hash_params), GFP_KERNEL);
#ifdef STORE_KEY_FP
    fp_t fp;
    h_params->hash = hashing_key(req->key.key, req->key.len, &h_params->key_fp);
#else
    h_params->hash = hashing_key(req->key.key, req->key.len, NULL);
#endif
	h_params->cnt = 0;
	h_params->find = HASH_KEY_INITIAL;
	h_params->lpa = 0;

	return h_params;
}
#endif
