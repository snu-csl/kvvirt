/*
 * Demand-based FTL Internal Implementation
 */

#include "demand.h"
#include "utility.h"
#include "cache.h"
#include "./interface/interface.h"

#include <linux/random.h>
#include <linux/sched/clock.h>

extern algorithm __demand;
extern struct demand_stat d_stat;

static uint64_t __get_wallclock(void) {
    return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

void d_set_oob(struct demand_shard *shard, lpa_t lpa, ppa_t ppa, 
               uint64_t offset, uint32_t len) {
    NVMEV_DEBUG("Setting OOB for PPA %u offset %llu LPA %u len %u\n", 
                ppa, offset, lpa, len);

    shard->oob[ppa][offset] = lpa;
    for(int i = 1; i < len; i++) {
        shard->oob[ppa][offset + i] = 0;
    }
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
        offset = cmd->kv_retrieve.rsvd;
        length = buf_len;
    } else {
        offset = cmd->kv_store.rsvd;
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
		offset += io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);

	return length;
}

static uint32_t do_wb_check(skiplist *wb, request *const req) {
	snode *wb_entry = skiplist_find(wb, req->key);
	if (WB_HIT(wb_entry)) {
        NVMEV_DEBUG("WB hit for key %s! Length %u!\n", 
                    req->key.key, wb_entry->value->length);
		d_stat.wb_hit++;
#ifdef HASH_KVSSD
		kfree(req->hash_params);
#endif
        //uint32_t vlen = wb_entry->value->length * GRAINED_UNIT;
        ///*
        // * Here, req->value->length represents the buffer size provided
        // * by the user. Later we should handle this by returning a
        // * BUFFER_TOO_SMALL or similar.
        // */
        //NVMEV_ASSERT(req->value->length >= vlen);

        //__buf_copy(req->cmd, wb_entry->value->value, vlen);
        copy_value(req->value, wb_entry->value, wb_entry->value->length * GRAINED_UNIT);

		req->type_ftl = 0;
		req->type_lower = 0;
        req->value->length = wb_entry->value->length * GRAINED_UNIT;
		return 1;
	}
	return 0;
}

static uint32_t do_wb_delete(skiplist *wb, request *const req) {
    NVMEV_ASSERT(!skiplist_delete(wb, req->key));
	return 0;
}

static struct ppa ppa_to_struct(const struct ssdparams *spp, ppa_t ppa_)
{
    struct ppa ppa;

    ppa.ppa = 0;
    ppa.g.ch = (ppa_ / spp->pgs_per_ch) % spp->pgs_per_ch;
    ppa.g.lun = (ppa_ % spp->pgs_per_ch) / spp->pgs_per_lun;
    ppa.g.pl = 0 ; //ppa_ % spp->tt_pls; // (ppa_ / spp->pgs_per_pl) % spp->pls_per_lun;
    ppa.g.blk = (ppa_ % spp->pgs_per_lun) / spp->pgs_per_blk;
    ppa.g.pg = ppa_ % spp->pgs_per_blk;

    //printk("%s: For PPA %u we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

#ifndef GC_STANDARD
static uint64_t _record_inv_mapping(lpa_t lpa, ppa_t ppa, uint64_t *credits) {
    struct ssdparams spp = shard->ssd->sp;
    struct gc_data *gcd = &ftl->gcd;
    struct ppa p = ppa_to_struct(&spp, ppa);
    struct line* l = get_line(ftl, &p); 
    uint64_t line = (uint64_t) l->id;
    uint64_t nsecs_completed = 0;

    NVMEV_DEBUG("Got an invalid LPA %u PPA %u mapping line %llu (%llu)\n", 
                 lpa, ppa, line, inv_mapping_offs[line]);

    BUG_ON(lpa == UINT_MAX);
    BUG_ON(!credits);
 
    if((inv_mapping_offs[line] + sizeof(lpa) + sizeof(ppa)) > INV_PAGE_SZ) {
        /*
         * Anything bigger complicates implementation. Keep to pgsz for now.
         */

        NVMEV_ASSERT(INV_PAGE_SZ == spp.pgsz);

        struct ppa n_p = get_new_page(ftl, MAP_IO);
        uint64_t pgidx = ppa2pgidx(ftl, &n_p);

        NVMEV_ASSERT(pg_inv_cnt[pgidx] == 0);
        NVMEV_ASSERT(advance_write_pointer(ftl, MAP_IO));
        mark_page_valid(ftl, &n_p);
        mark_grain_valid(ftl, PPA_TO_PGA(ppa2pgidx(ftl, &n_p), 0), GRAIN_PER_PAGE);
    
		ppa_t w_ppa = ppa2pgidx(ftl, &n_p);
        NVMEV_DEBUG("Flushing an invalid mapping page for line %llu off %llu to PPA %u\n", 
                    line, inv_mapping_offs[line], w_ppa);

        oob[w_ppa][0] = UINT_MAX;
        oob[w_ppa][1] = (line << 32) | (w_ppa);

        struct value_set value;
        value.value = inv_mapping_bufs[line];
        value.ssd = shard->ssd;
        value.length = INV_PAGE_SZ;

        nsecs_completed = 
        __demand.li->write(w_ppa, INV_PAGE_SZ, &value, ASYNC, NULL);
        nvmev_vdev->space_used += INV_PAGE_SZ;

        NVMEV_DEBUG("Added %u (%u %u) to XA.\n", (line << 32) | w_ppa, line, w_ppa);
        xa_store(&gcd->inv_mapping_xa, (line << 32) | w_ppa, xa_mk_value(w_ppa), GFP_KERNEL);
        
        memset(inv_mapping_bufs[line], 0x0, INV_PAGE_SZ);
        inv_mapping_offs[line] = 0;

        (*credits) += GRAIN_PER_PAGE;
        d_stat.inv_w++;
    }

    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &lpa, sizeof(lpa));
    inv_mapping_offs[line] += sizeof(lpa);
    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &ppa, sizeof(ppa));
    inv_mapping_offs[line] += sizeof(ppa);

    return nsecs_completed;
}
#endif

uint8_t __klen_from_value(uint8_t *ptr) {
    return *(uint8_t*) ptr;
}

char* __key_from_value(uint8_t *ptr) {
    return (char*) (ptr + 1);
}

uint32_t get_vlen(struct demand_shard *shard, ppa_t ppa, uint64_t offset) {
    NVMEV_DEBUG("Checking PPA %u offset %u\n", ppa, offset);

    uint32_t len = 1;
    while(offset + len < GRAIN_PER_PAGE && shard->oob[ppa][offset + len] == 0) {
        len++;
    }

    NVMEV_DEBUG("%s returning a vlen of %u\n", __func__, len * GRAINED_UNIT);
    return len * GRAINED_UNIT;
}

static uint64_t read_actual_dpage(struct demand_shard *shard, ppa_t ppa, 
                                  request *const req, uint64_t *nsecs_completed,
                                  uint64_t stime) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct hash_params *h_params = (struct hash_params*) req->hash_params;
    uint64_t nsecs = 0;
    uint64_t offset;
    uint64_t local;
    bool retry;

    NVMEV_ASSERT(nsecs_completed);

	if (IS_INITIAL_PPA(ppa)) {
        if(nsecs_completed) {
            *nsecs_completed = 0;
        }

		return UINT_MAX;
	}

#ifdef DVALUE
    offset = G_OFFSET(ppa);
    ppa = G_IDX(ppa);
#endif

    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off = shard_off + ((uint64_t) ppa * spp->pgsz);

    local = __get_wallclock();

    struct nand_cmd swr = {
        .type = USER_IO,
        .cmd = NAND_READ,
        .interleave_pci_dma = false,
        .xfer_size = spp->pgsz,
        .stime = local > stime ? local : stime,
    };

    struct ppa p = ppa_to_struct(spp, ppa);
    swr.ppa = &p;
    *nsecs_completed = ssd_advance_nand(ssd, &swr);

    uint8_t* ptr = nvmev_vdev->ns[0].mapped + off;
    uint8_t klen = __klen_from_value(ptr);
    char* key = __key_from_value(ptr);

    KEYT check_key;
    KEYT actual_key;

    check_key.len = req->key.len;
    check_key.key = req->key.key;

    actual_key.len = klen;
    actual_key.key = key;

    if (KEYCMP(actual_key, check_key) == 0) {
        d_stat.fp_match_r++;

        hash_collision_logging(h_params->cnt, DREAD);
        kfree(h_params);

        req->value->length = get_vlen(shard, G_IDX(req->ppa), offset);
        req->shard = shard;
        req->end_req(req);
        retry = false;
    } else {
        d_stat.fp_collision_r++;

        h_params->find = HASH_KEY_DIFF;
        h_params->cnt++;

        kfree(check_key.key);
        retry = true;
    }

    if(retry) {
        return 1;
    } else {
        return 0;
    }

//    struct algo_req *a_req = make_algo_req_rw(shard, DATAR, NULL, req, NULL);
//    a_req->parents = req;
//    a_req->parents->ppa = ppa;
//    a_req->stime = 0;
//    //printk("%s ppa %u offset %u\n", 
//            //__func__, ppa, ((struct demand_params *)a_req->params)->offset);
//#ifdef DVALUE
//	((struct demand_params *)a_req->params)->offset = G_OFFSET(ppa);
//	ppa = G_IDX(ppa);
//#endif
//
//    req->ssd = shard->ssd;
//    req->value->shard = shard;
//	nsecs = __demand.li->read(ppa, spp->pgsz, req->value, false, a_req);

}

static uint64_t read_for_data_check(struct demand_shard *shard, 
                                    ppa_t ppa, snode *wb_entry, 
                                    uint64_t stime) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct hash_params *h_params = (struct hash_params*) wb_entry->hash_params;
    struct inflight_params *i_params;
	//value_set *_value_dr_check = inf_get_valueset(NULL, FS_MALLOC_R, spp->pgsz);
	//struct algo_req *a_req = make_algo_req_rw(shard, DATAR, _value_dr_check, NULL, wb_entry);
    uint64_t nsecs_completed;
    uint64_t offset;
    uint64_t local;

#ifdef DVALUE
	offset = G_OFFSET(ppa);
	ppa = G_IDX(ppa);
#endif

    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off = shard_off + ((uint64_t) ppa * spp->pgsz);

    struct ppa p = ppa_to_struct(spp, ppa);
    //local = __get_wallclock();

    struct nand_cmd swr = {
        .type = USER_IO,
        .cmd = NAND_READ,
        .interleave_pci_dma = false,
        .xfer_size = spp->pgsz,
        .stime = stime,
    };

    swr.ppa = &p;
    nsecs_completed = ssd_advance_nand(ssd, &swr);
    printk("Read completed %llu\n", nsecs_completed);

    uint8_t* ptr = nvmev_vdev->ns[0].mapped + off;
    uint8_t klen = __klen_from_value(ptr);
    char* key = __key_from_value(ptr);

    KEYT check_key;
    KEYT actual_key;

    check_key.len = wb_entry->key.len;
    check_key.key = wb_entry->key.key;

    actual_key.len = klen;
    actual_key.key = key;

    BUG_ON(!h_params);

    //printk("Got key %llu at offset %llu\n", *(uint64_t*) ptr, off);

    if (KEYCMP(actual_key, check_key) == 0) {
        //printk("Match in read for writes.\n");
        /* hash key found -> update */
        d_stat.fp_match_w++;

        h_params->find = HASH_KEY_SAME;
        i_params = get_iparams(NULL, wb_entry);
        BUG_ON(!i_params);
        i_params->jump = GOTO_UPDATE;

        q_enqueue((void *)wb_entry, shard->ftl->wb_retry_q);
    } else {
        /* retry */
        d_stat.fp_collision_w++;

        h_params->find = HASH_KEY_DIFF;
        h_params->cnt++;

        q_enqueue((void *)wb_entry, shard->ftl->wb_master_q);
    }

//    a_req->ppa = ppa;
//    a_req->stime = stime;
//    _value_dr_check->shard = shard;
//	nsecs_completed = __demand.li->read(ppa, spp->pgsz, _value_dr_check, ASYNC, a_req);
//
//    kfree(a_req);
	return nsecs_completed;
}

uint64_t __demand_read(struct demand_shard *shard,
                       request *const req, bool for_del,
                       uint64_t stime) {
	uint64_t rc = 0;
    uint64_t nsecs_completed = 0, nsecs_latest = stime;
    uint64_t credits = 0;
	struct hash_params *h_params = (struct hash_params *)req->hash_params;
    uint64_t **oob = shard->oob;
    uint64_t local;

	lpa_t lpa;
	struct pt_struct pte;

read_retry:
	lpa = get_lpa(shard->cache, req->key, req->hash_params);
    //NVMEV_INFO("Got LPA %u for key %s (%llu)!\n", 
    //            lpa, req->key.key, *(uint64_t*) req->key.key);
	pte.ppa = UINT_MAX;
#ifdef STORE_KEY_FP
	pte.key_fp = FP_MAX;
#endif

#ifdef HASH_KVSSD
	if (h_params->cnt > shard->ftl->max_try) {
		rc = UINT_MAX;
        req->ppa = UINT_MAX;
        req->value->length = 0;
        free_iparams(req, NULL);
        kfree(h_params);

        if(req->key.key[0] == 'L') {
            NVMEV_DEBUG("3 Log key not found. Bid %llu log num %u\n", 
                         *(uint64_t*) (req->key.key + 4), 
                         *(uint16_t*) (req->key.key + 4 + sizeof(uint64_t)));
        }

		warn_notfound(__FILE__, __LINE__);
		goto read_ret;
	}
#endif

	/* inflight request */
	if (IS_INFLIGHT(req->params)) {
		struct inflight_params *i_params = (struct inflight_params *)req->params;
		jump_t jump = i_params->jump;
		free_iparams(req, NULL);

		switch (jump) {
		case GOTO_LOAD:
			goto cache_load;
		case GOTO_LIST:
		case GOTO_EVICT:
			goto cache_list_up;
		case GOTO_COMPLETE:
			//pte = i_params->pte;
			goto cache_check_complete;
		case GOTO_READ:
			goto data_read;
		default:
			//printk("[ERROR] No jump type found, at %s:%d\n", __FILE__, __LINE__);
			printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);;
		}
	}

    nsecs_latest = max(nsecs_completed, nsecs_latest);

	/* 1. check write buffer first */
	rc = do_wb_check(shard->ftl->write_buffer, req);
	if (rc) {
        req->ppa = UINT_MAX - 1;
        if(for_del) {
            do_wb_delete(shard->ftl->write_buffer, req);
        } else {
            nsecs_latest =
            ssd_advance_pcie(req->ssd, nsecs_latest, req->value->length);
            req->end_req(req);
        }
        free_iparams(req, NULL);
		goto read_ret;
	}

	/* 2. check cache */
	if (shard->cache->is_hit(shard->cache, lpa)) {
		shard->cache->touch(shard->cache, lpa);
        NVMEV_DEBUG("Cache hit for LPA %u!\n", lpa);
	} else {
cache_load:
		rc = shard->cache->wait_if_flying(lpa, req, NULL);
		if (rc) {
			goto read_ret;
		}
		rc = shard->cache->load(shard, lpa, req, NULL, &nsecs_completed, 
                                nsecs_latest);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
		if (!rc) {
            req->ppa = UINT_MAX;
            req->value->length = 0;
			rc = UINT_MAX;

            if(req->key.key[0] == 'L') {
                NVMEV_DEBUG("1 Log key not found. Bid %llu log num %u\n", 
                            *(uint64_t*) (req->key.key + 4), 
                            *(uint16_t*) (req->key.key + 4 + sizeof(uint64_t)));
            }

			warn_notfound(__FILE__, __LINE__);
            goto read_ret;
		}
cache_list_up:
		rc = shard->cache->list_up(shard, lpa, req, NULL, 
                                   &nsecs_completed, &credits,
                                   nsecs_latest);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
	}

cache_check_complete:
	free_iparams(req, NULL);

	pte = shard->cache->get_pte(shard, lpa);
#ifdef STORE_KEY_FP
	/* fast fingerprint compare */
	if (h_params->key_fp != pte.key_fp) {
        NVMEV_DEBUG("The fingerprints didn't match.\n");
		h_params->cnt++;
		goto read_retry;
	}
#endif
data_read:
	/* 3. read actual data */
    NVMEV_DEBUG("Got PPA %u for LPA %u\n", pte.ppa, lpa);

    rc = read_actual_dpage(shard, pte.ppa, req, &nsecs_completed, nsecs_latest);
    nsecs_latest = max(nsecs_latest, nsecs_completed);

    if(rc) {
        //d_stat.fp_collision_r++;

        if(req->key.key[0] == 'L') {
            NVMEV_DEBUG("2 Log key retry. Bid %llu log num %u\n", 
                        *(uint64_t*) (req->key.key + 4), 
                        *(uint16_t*) (req->key.key + 4 + sizeof(uint64_t)));
        }

        //h_params->find = HASH_KEY_DIFF;
        //h_params->cnt++;
        goto read_retry;
    }
    //} else if(rc == 1) {
    //    NVMEV_DEBUG("Retrying a read for key %s cnt %u\n", req->key.key, h_params->cnt);
    //    goto read_retry;
    //}

    if(for_del) {
        NVMEV_ASSERT(!IS_INITIAL_PPA(pte.ppa));

        uint64_t offset = G_OFFSET(pte.ppa);
        uint32_t len = 1;

        while(offset + len < GRAIN_PER_PAGE && oob[G_IDX(pte.ppa)][offset + len] == 0) {
            len++;
        }

        NVMEV_DEBUG("Deleting a pair of length %u (%u) grain %u PPA %u\n", 
                    len, len * GRAINED_UNIT, pte.ppa, G_IDX(pte.ppa));

        oob[G_IDX(pte.ppa)][offset] = 2;
        mark_grain_invalid(shard, pte.ppa, len);
#ifndef GC_STANDARD
        _record_inv_mapping(lpa, G_IDX(pte.ppa), &credits);
#endif
        req->ppa = UINT_MAX - 2;

        pte.ppa = UINT_MAX;
        shard->cache->update(shard, lpa, pte);
        d_htable_insert(shard->ftl->hash_table, UINT_MAX, lpa);

        nvmev_vdev->space_used -= len * GRAINED_UNIT;
    }

    if(credits > 0) {
        consume_write_credit(shard, credits);
        check_and_refill_write_credit(shard);
    }

read_ret:
    //printk("Read done. %p\n", shard);
	return nsecs_latest;
}


static bool wb_is_full(struct demand_shard *shard, skiplist *wb) { 
    return (wb->size == shard->env->wb_flush_size); 
}

uint32_t cnt = 0;
static bool _do_wb_assign_ppa(struct demand_shard *shard, skiplist *wb) {
	blockmanager *bm = __demand.bm;
	struct flush_list *fl = shard->ftl->flush_list;
    struct ssdparams *spp = &shard->ssd->sp;
    uint64_t **oob = shard->oob;

	snode *wb_entry;
	sk_iter *iter = skiplist_get_iterator(wb);

#ifdef DVALUE
	l_bucket *wb_bucket = (l_bucket *)kzalloc(sizeof(l_bucket), GFP_KERNEL);
	for (int i = 1; i <= GRAIN_PER_PAGE; i++) {
		wb_bucket->bucket[i] = (snode **)kzalloc(shard->env->wb_flush_size * sizeof(snode *), GFP_KERNEL);
		wb_bucket->idx[i] = 0;
	}

	for (size_t i = 0; i < shard->env->wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		int val_len = wb_entry->value->length;

		wb_bucket->bucket[val_len][wb_bucket->idx[val_len]] = wb_entry;
		wb_bucket->idx[val_len]++;
	}

	int ordering_done = 0;
	while (ordering_done < shard->env->wb_flush_size) {
		struct value_set *new_vs = get_vs(spp);

		PTR page = new_vs->value;
		int remain = spp->pgsz;
        uint32_t credits = 0;

        struct ppa ppa_s = get_new_page(shard, USER_IO);
        if(!advance_write_pointer(shard, USER_IO)) {
            NVMEV_ERROR("Failing wb flush because we had no available pages!\n");
            /*
             * TODO
             * Leak here of remaining wb_entries.
             */

            return false;
        }

        mark_page_valid(shard, &ppa_s);
		ppa_t ppa = ppa2pgidx(shard, &ppa_s);
        NVMEV_DEBUG("%s assigning PPA %u (%u)\n", __func__, ppa, cnt++);

		int offset = 0;

		fl->list[fl->size].ppa = ppa;
		fl->list[fl->size].value = new_vs;

		while (remain > 0) {
			int target_length = remain / GRAINED_UNIT;
            //printk("%s target_length %d remain %u\n", __func__, target_length, remain);

			while(wb_bucket->idx[target_length]==0 && target_length!=0) --target_length;
			if (target_length==0) {
				break;
			}

			wb_entry = wb_bucket->bucket[target_length][wb_bucket->idx[target_length]-1];
			wb_bucket->idx[target_length]--;
			wb_entry->ppa = PPA_TO_PGA(ppa, offset);

            //printk("PGA %u for PPA %u\n", wb_entry->ppa, ppa);

            //printk("%s key %s going to ppa %u (%u) offset %u\n", __func__, 
                    //wb_entry->key.key, wb_entry->ppa, ppa, offset*GRAINED_UNIT);

			// FIXME: copy only key?
			//memcpy(&page[offset*GRAINED_UNIT], 
            //       wb_entry->value->value, 
            //       wb_entry->value->length * GRAINED_UNIT);
            //char tmp[128];
            //memcpy(tmp, wb_entry->value->value + 1, wb_entry->key.len);
            //tmp[wb_entry->key.len] = '\0';
            //NVMEV_DEBUG("%s writing %s length %u (%u %llu) to %u (%u)\n", 
            //            __func__, tmp, *(uint8_t*) wb_entry->value->value, 
            //            wb_entry->value->length, 
            //            *(uint64_t*) (wb_entry->value->value + sizeof(uint8_t)),
            //            ppa, wb_entry->ppa);

            put_vs(wb_entry->value);
			wb_entry->value = NULL;

			//validate_grain(bm, wb_entry->ppa);
            mark_grain_valid(shard, wb_entry->ppa, target_length);

            credits += target_length;
			offset += target_length;
			remain -= target_length * GRAINED_UNIT;

			ordering_done++;
		}

        if(remain > 0) {
            NVMEV_DEBUG("Had %u bytes leftover PPA %u offset %u.\n", remain, ppa, offset);
            NVMEV_DEBUG("Ordering %s.\n", ordering_done < shard->env->wb_flush_size ? "NOT DONE" : "DONE");
            mark_grain_valid(shard, PPA_TO_PGA(ppa, offset), 
                    GRAIN_PER_PAGE - offset);
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, offset), 
                    GRAIN_PER_PAGE - offset);
            oob[ppa][offset] = 2;
            nvmev_vdev->space_used += (GRAIN_PER_PAGE - offset) * GRAINED_UNIT;
        }

		fl->size++;
	}

    //NVMEV_ERROR("Exited loop.\n");

	for (int i = 1; i<= GRAIN_PER_PAGE; i++) {
		kfree(wb_bucket->bucket[i]);
	}
	kfree(wb_bucket);
#else
	for (int i = 0; i < shard->env->wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		wb_entry->ppa = get_dpage(bm);

		fl->list[i]->ppa = wb_entry->ppa;
		//fl->list[i]->value = wb_entry->value;
		wb_entry->value = NULL;

#ifndef HASH_KVSSD
		set_oob(bm, wb_entry->lpa, wb_entry->ppa, DATA);
#endif
	}
#endif
	kfree(iter);

    return true;
}

static uint64_t _do_wb_mapping_update(struct demand_shard *shard, skiplist *wb, 
                                      uint64_t* credits, uint64_t stime) {
	int rc = 0;
    uint64_t nsecs_completed = 0, nsecs_latest = stime;
	blockmanager *bm = __demand.bm;
    uint64_t **oob = shard->oob;
    uint64_t local;

	snode *wb_entry;
	struct hash_params *h_params;

	lpa_t lpa;
	struct pt_struct pte, new_pte;

	/* push all the wb_entries to queue */
	sk_iter *iter = skiplist_get_iterator(wb);
	for (int i = 0; i < shard->env->wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		q_enqueue((void *)wb_entry, shard->ftl->wb_master_q);
	}
	kfree(iter);

	/* mapping update */
	volatile int updated = 0;
	while (updated < shard->env->wb_flush_size) {
		wb_entry = (snode *)q_dequeue(shard->ftl->wb_retry_q);
		if (!wb_entry) {
			wb_entry = (snode *)q_dequeue(shard->ftl->wb_master_q);
		}
		if (!wb_entry) continue;
wb_retry:
		h_params = (struct hash_params *)wb_entry->hash_params;

		lpa = get_lpa(shard->cache, wb_entry->key, wb_entry->hash_params);
		new_pte.ppa = wb_entry->ppa;

#ifdef STORE_KEY_FP
		new_pte.key_fp = h_params->key_fp;
#endif
		/* inflight wb_entries */
		if (IS_INFLIGHT(wb_entry->params)) {
			struct inflight_params *i_params = (struct inflight_params *)wb_entry->params;
			jump_t jump = i_params->jump;
			free_iparams(NULL, wb_entry);

			switch (jump) {
			case GOTO_LOAD:
				goto wb_cache_load;
			case GOTO_LIST:
				goto wb_cache_list_up;
			case GOTO_COMPLETE:
				goto wb_data_check;
			case GOTO_UPDATE:
				goto wb_update;
			default:
				//printk("[ERROR] No jump type found, at %s:%d\n", __FILE__, __LINE__);
				printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);;
			}
		}

		if (shard->cache->is_hit(shard->cache, lpa)) {
            NVMEV_DEBUG("%s hit for LPA %u\n", __func__, lpa);
		} else {
            //printk("%s miss for LPA %u\n", __func__, lpa);
wb_cache_load:
			rc = shard->cache->wait_if_flying(lpa, NULL, wb_entry);
			if (rc) continue; /* pending */

            //printk("%s passed wait_if_flying for LPA %u\n", __func__, lpa);
            
			rc = shard->cache->load(shard, lpa, NULL, wb_entry, 
                                    &nsecs_completed, nsecs_latest);
            nsecs_latest = max(nsecs_latest, nsecs_completed);
			if (rc) continue; /* mapping read */

            //printk("%s passed load for LPA %u\n", __func__, lpa);
wb_cache_list_up:
            /*
             * TODO time here.
             */

			rc = shard->cache->list_up(shard, lpa, NULL, wb_entry, 
                                       &nsecs_completed, credits, 
                                       nsecs_latest);
            nsecs_latest = max(nsecs_latest, nsecs_completed);
			if (rc) continue; /* mapping write */
		}

wb_data_check:
		/* get page_table entry which contains {ppa, key_fp} */
        pte = shard->cache->get_pte(shard, lpa);
        NVMEV_DEBUG("Grain %u for LPA %u\n", pte.ppa, lpa);
        //printk("%s passed get_pte for LPA %u\n", __func__, lpa);

#ifdef HASH_KVSSD
		/* direct update at initial case */
		if (IS_INITIAL_PPA(pte.ppa)) {
            //printk("%s entered initial_ppa for LPA %u\n", __func__, lpa);
			nvmev_vdev->space_used += wb_entry->len * GRAINED_UNIT;
            goto wb_direct_update;
		}
        //printk("%s passed initial_ppa for LPA %u\n", __func__, lpa);
#ifdef STORE_KEY_FP
		/* fast fingerprint compare */
		if (h_params->key_fp != pte.key_fp) {
			h_params->find = HASH_KEY_DIFF;
			h_params->cnt++;

			goto wb_retry;
		}
#endif
		/* hash_table lookup to filter same wb element */
		rc = d_htable_find(shard->ftl->hash_table, pte.ppa, lpa);
		if (rc) {
			h_params->find = HASH_KEY_DIFF;
			h_params->cnt++;

			goto wb_retry;
		}
        //printk("%s passed hash table check for LPA %u\n", __func__, lpa);

		/* data check is necessary before update */
		nsecs_completed = read_for_data_check(shard, pte.ppa, wb_entry, 
                                              stime);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
		continue;
#endif

wb_update:
        NVMEV_DEBUG("1 %s LPA %u PPA %u update in cache.\n", __func__, lpa, new_pte.ppa);
		pte = shard->cache->get_pte(shard, lpa);
		if (!IS_INITIAL_PPA(pte.ppa)) {
            uint64_t offset = G_OFFSET(pte.ppa);
            uint32_t len = 1;
            while(offset + len < GRAIN_PER_PAGE && oob[G_IDX(pte.ppa)][offset + len] == 0) {
                len++;
            }

            NVMEV_DEBUG("%s LPA %u old PPA %u overwrite old len %u.\n", 
                        __func__, lpa, pte.ppa, len);

            mark_grain_invalid(shard, pte.ppa, len);
#ifndef GC_STANDARD
            nsecs_completed = _record_inv_mapping(lpa, G_IDX(pte.ppa), credits);
            nsecs_latest = max(nsecs_latest, nsecs_completed);
#endif

			static int over_cnt = 0; over_cnt++;
			if (over_cnt % 100000 == 0) printk("overwrite: %d\n", over_cnt);
		} else {
            NVMEV_ERROR("INSERT: Key %s\n", wb_entry->key.key);
            nvmev_vdev->space_used += wb_entry->len * GRAINED_UNIT;
        }
wb_direct_update:
		shard->cache->update(shard, lpa, new_pte);
        NVMEV_DEBUG("2 %s LPA %u PPA %u update in cache.\n", __func__, lpa, new_pte.ppa);

		updated++;
		d_htable_insert(shard->ftl->hash_table, new_pte.ppa, lpa);

#ifdef HASH_KVSSD
		shard->ftl->max_try = (h_params->cnt > shard->ftl->max_try) ? h_params->cnt : shard->ftl->max_try;
		hash_collision_logging(h_params->cnt, DWRITE);
		d_set_oob(shard, lpa, G_IDX(new_pte.ppa), G_OFFSET(new_pte.ppa), 
                  wb_entry->len);
#endif
	}

	if (unlikely(shard->ftl->wb_master_q->size + shard->ftl->wb_retry_q->size > 0)) {
		//printk("[ERROR] wb_entry still remains in queues, at %s:%d\n", __FILE__, __LINE__);
		printk("Should have aborted!!!! %s:%d MQ size RQ size %u %u\n", 
                __FILE__, __LINE__, shard->ftl->wb_master_q->size, shard->ftl->wb_retry_q->size);
        wb_entry = (snode *)q_dequeue(shard->ftl->wb_master_q);
        printk("Last one was LPA %u PPA %u key %s\n", wb_entry->lpa, wb_entry->ppa,
                wb_entry->key.key);
        BUG_ON(true);
	}

	iter = skiplist_get_iterator(wb);
	for (size_t i = 0; i < shard->env->wb_flush_size; i++) {
		snode *wb_entry = skiplist_get_next(iter);
        //(*credits) += wb_entry->len;
		if (wb_entry->hash_params) kfree(wb_entry->hash_params);
		free_iparams(NULL, wb_entry);
	}

	kfree(iter);
    return nsecs_latest;
}

uint64_t _do_wb_flush(struct demand_shard *shard, skiplist *wb, 
                      uint64_t credits, uint64_t stime) {
	struct flush_list *fl = shard->ftl->flush_list;
    struct ssdparams spp = shard->ssd->sp;
    uint64_t nsecs_completed = 0, nsecs_latest = stime;

	//for (int i = 0; i < fl->size; i++) {
	//	ppa_t ppa = fl->list[i].ppa;
	//	value_set *value = fl->list[i].value;
    //    value->shard = shard;

    //    struct algo_req *a = make_algo_req_rw(shard, DATAW, value, NULL, NULL);
    //    a->stime = nsecs_latest;

	//	nsecs_completed = 
    //    __demand.li->write(ppa, spp.pgsz, value, ASYNC, a);
    //    nsecs_latest = max(nsecs_latest, nsecs_completed);
    //    credits += GRAIN_PER_PAGE;
    //}

	fl->size = 0;
	memset(fl->list, 0, shard->env->wb_flush_size * sizeof(struct flush_node));

	d_htable_kfree(shard->ftl->hash_table);
	shard->ftl->hash_table = d_htable_init(shard->env->wb_flush_size * 2);

	skiplist_kfree(wb);

    /*
     * CAUTION, only consume credits after both the OOBs and grains
     * have been set for the writes you want to consume the credits for.
     * Otherwise, GC will be incorrect because it will see pages
     * with valid grains, but invalid OOBs, or vice versa.
     */

    consume_write_credit(shard, credits);
    nsecs_completed = check_and_refill_write_credit(shard);
    nsecs_latest = max(nsecs_latest, nsecs_completed);

	return nsecs_latest;
}

uint64_t pgs_this_flush = 0;
static uint32_t _do_wb_insert(struct demand_shard *shard, skiplist *wb, 
                              request *const req) {
    bool ow = false;
	snode *wb_entry = skiplist_insert(wb, req->key, req->value, true, 
                                      req->sqid, &ow, req->wb_off);
#ifdef HASH_KVSSD
    if(!ow) {
        wb_entry->hash_params = (void *)req->hash_params;
    } else {
        kfree(req->hash_params);
    }
#endif
	req->value = NULL;

	if (wb_is_full(shard, wb)) return 1;
	else return 0;
}

uint64_t __demand_write(struct demand_shard *shard, request *const req,
                        uint64_t stime) {
	uint64_t rc = 0;
    uint64_t nsecs_latest = stime, nsecs_completed = 0;
    uint64_t length = req->value->length;
    uint64_t credits = 0;
	skiplist *wb = shard->ftl->write_buffer;
    struct ssdparams *spp = &shard->ssd->sp;

	/* flush the buffer if full */
	if (wb_is_full(shard, wb)) {
		///* assign ppa first */
        //_do_wb_assign_ppa(shard, wb);

		/* mapping update [lpa, origin]->[lpa, new] */
		nsecs_completed = _do_wb_mapping_update(shard, wb, &credits, 
                                                nsecs_latest);
        printk("Gap 1 %llu (%llu %llu)\n", nsecs_completed - nsecs_latest, nsecs_completed,
                nsecs_latest);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
		
		/* flush the buffer */
		_do_wb_flush(shard, wb, credits, nsecs_latest);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        wb = shard->ftl->write_buffer = skiplist_init();
    }

	/* default: insert to the buffer */
	rc = _do_wb_insert(shard, wb, req); // rc: is the write buffer is full? 1 : 0
	req->end_req(req);
	return nsecs_latest;
}

uint32_t __demand_remove(struct demand_shard *shard, request *const req) {
	//printk("Hello! remove() is not implemented yet! lol!");
	return 0;
}

void *demand_end_req(algo_req *a_req) {
	struct demand_params *d_params = (struct demand_params *)a_req->params;
	request *req = a_req->parents;
	snode *wb_entry = d_params->wb_entry;
    BUG_ON(!d_params->shard);
    struct demand_shard *shard = d_params->shard;

	struct hash_params *h_params;
	struct inflight_params *i_params;
	KEYT check_key;

	dl_sync *sync_mutex = d_params->sync_mutex;

	int offset = d_params->offset;

	switch (a_req->type) {
	case DATAR:
		d_stat.data_r++;
#ifdef HASH_KVSSD
		if (IS_READ(req)) {
			d_stat.d_read_on_read++;
			req->type_ftl++;

            BUG_ON(!req->hash_params);
			h_params = (struct hash_params *)req->hash_params;

            BUG_ON(!req->key.key);
            BUG_ON(!req->value);
            BUG_ON(!a_req);

			copy_key_from_value(&check_key, req->value, offset);
            BUG_ON(!check_key.key);

            //NVMEV_INFO("Passed check %p %p %llu\n", 
            //            req, &(req->key), *(uint64_t*) req->key.key);
            //NVMEV_INFO("Passed check 2 %p %p %llu\n", 
            //            req, &(req->key), *(uint64_t*) check_key.key);

            if(check_key.key[0] == 'L') {
                NVMEV_DEBUG("end_req Log key comparing %llu %u and %llu %u\n", 
                        *(uint64_t*) (check_key.key + 4), 
                        *(uint16_t*) (check_key.key + 4 + sizeof(uint64_t)),
                        *(uint64_t*) (req->key.key + 4), 
                        *(uint16_t*) (req->key.key + 4 + sizeof(uint64_t)));
            } else {
                NVMEV_DEBUG("Comparing %s (%llu) and %s (%llu)\n", 
                        check_key.key, *(uint64_t*) check_key.key,
                        req->key.key, *(uint64_t*) req->key.key);
            }
			if (KEYCMP(req->key, check_key) == 0) {
                NVMEV_DEBUG("Match %llu and %llu PPA was %u.\n", 
                           *(uint64_t*) check_key.key, 
                           *(uint64_t*) (req->key.key), req->ppa);
				d_stat.fp_match_r++;

                a_req->need_retry = false;
				hash_collision_logging(h_params->cnt, DREAD);
				kfree(h_params);

                req->value->length = get_vlen(shard, G_IDX(req->ppa), offset);
                req->shard = shard;
				req->end_req(req);
			} else {
                NVMEV_DEBUG("Passed cmp 2.\n");
                NVMEV_DEBUG("Mismatch %llu and %llu.\n", 
                           *(uint64_t*) check_key.key,
                           *(uint64_t*) (req->key.key));
				d_stat.fp_collision_r++;

				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;

                kfree(check_key.key);
                a_req->need_retry = true;
                return NULL;

				//insert_retry_read(req);
				//inf_assign_try(req);
			}
		} else {
			d_stat.d_read_on_write++;
			h_params = (struct hash_params *)wb_entry->hash_params;

			copy_key_from_value(&check_key, d_params->value, offset);
			if (KEYCMP(wb_entry->key, check_key) == 0) {
                //printk("Match in read for writes.\n");
				/* hash key found -> update */
				d_stat.fp_match_w++;

				h_params->find = HASH_KEY_SAME;
				i_params = get_iparams(NULL, wb_entry);
				i_params->jump = GOTO_UPDATE;

				q_enqueue((void *)wb_entry, shard->ftl->wb_retry_q);
			} else {
				/* retry */
				d_stat.fp_collision_w++;

				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;

				q_enqueue((void *)wb_entry, shard->ftl->wb_master_q);
			}
			inf_free_valueset(d_params->value, FS_MALLOC_R);
		}
		kfree(check_key.key);
#else
		req->end_req(req);
#endif
        return NULL;
		break;
	case DATAW:
		d_stat.data_w++;
		d_stat.d_write_on_write++;
        put_vs(d_params->value);
#ifndef DVALUE
		kfree(wb_entry->hash_params);
#endif
		break;
	case MAPPINGR:
        NVMEV_DEBUG("In MAPPINGR.\n");
		d_stat.trans_r++;
		//inf_free_valueset(d_params->value, FS_MALLOC_R);
		if (sync_mutex) {
			if (IS_READ(req)) d_stat.t_read_on_read++;
			else d_stat.t_read_on_write++;

			dl_sync_arrive(sync_mutex);
			//kfree(sync_mutex);
			break;
		} else if (IS_READ(req)) {
			d_stat.t_read_on_read++;
			req->type_ftl++;
            return NULL;
			//insert_retry_read(req);
			//inf_assign_try(req);
		} else {
			d_stat.t_read_on_write++;
			q_enqueue((void *)wb_entry, shard->ftl->wb_retry_q);
            return NULL;
		}
		break;
	case MAPPINGW:
		d_stat.trans_w++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		if (IS_READ(req)) {
			d_stat.t_write_on_read++;
			req->type_ftl+=100;
            free_algo_req(a_req);
            return NULL;
            //printk("Re-queuing read-type req in MAPPINGW.\n");
			//insert_retry_read(req);
			//inf_assign_try(req);
		} else {
			d_stat.t_write_on_write++;
			q_enqueue((void *)wb_entry, shard->ftl->wb_retry_q);
		}
		break;
	case GCDR:
		d_stat.data_r_dgc++;
		shard->ftl->nr_valid_read_done++;
		break;
	case GCDW:
		d_stat.data_w_dgc++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	case GCMR_DGC:
		d_stat.trans_r_dgc++;
		shard->ftl->nr_tpages_read_done++;
		//inf_free_valueset(d_params->value, FS_MALLOC_R);
		break;
	case GCMW_DGC:
		d_stat.trans_w_dgc++;
		//inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	case GCMR:
		d_stat.trans_r_tgc++;
		shard->ftl->nr_valid_read_done++;
		break;
	case GCMW:
		d_stat.trans_w_tgc++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	default:
		printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);
	}

	free_algo_req(a_req);
	return NULL;
}

