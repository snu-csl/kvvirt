/*
 * Demand-based FTL Internal Implementation
 */

#include "demand.h"
#include "page.h"
#include "utility.h"
#include "cache.h"
#include "./interface/interface.h"

#include <linux/random.h>
#include <linux/sched/clock.h>

extern algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

extern struct demand_cache *d_cache;

struct conv_ftl *ftl;

bool FAIL_MODE = false;

void __hash_remove(uint64_t lpa) {
    return;
    struct ht_mapping *cur = NULL;
    bool found = false;

    hash_for_each_possible(mapping_ht, cur, node, lpa) {
        found = true;
        cur = cur;
        hash_del(&cur->node);
    }

    //if(cur) {
    //    NVMEV_ERROR("Removing old LPA PPA mapping %llu %llu\n",
    //            cur->lpa, cur->ppa);
    //    hash_del(&cur->node);
    //}
    NVMEV_ASSERT(found);
}

void d_set_oob(uint64_t lpa, uint64_t ppa, uint64_t offset, uint32_t len) {
    BUG_ON(!oob);

    NVMEV_DEBUG("Trying to set OOB for PPA %llu offset %llu LPA %llu\n", 
                ppa, offset, lpa);
    oob[ppa][offset] = lpa;

    for(int i = 1; i < len; i++) {
        oob[ppa][offset + i] = 0;
    }
}

void print_oob(uint64_t ppa) {
    return;
    NVMEV_DEBUG("OOB for PPA %llu is: \n", ppa);
    for(int i = 0; i < GRAIN_PER_PAGE; i++) {
        NVMEV_DEBUG("%llu ", oob[ppa][i]);
    }
    NVMEV_DEBUG("\n");
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
	//snode *wb_entry = skiplist_delete(wb, req->key);
	//if (WB_HIT(wb_entry)) {
	//	d_stat.wb_hit++;
//#ifdef HASH_KVSSD
	//	kfree(req->hash_params);
//#endif
    //    copy_value(req->value, wb_entry->value, wb_entry->value->length * GRAINED_UNIT);
	//	req->type_ftl = 0;
	//	req->type_lower = 0;
    //    req->value->length = wb_entry->value->length * GRAINED_UNIT;
	//	return 1;
	//}
	return 0;
}

static struct ppa ppa_to_struct(const struct ssdparams *spp, uint64_t ppa_)
{
    struct ppa ppa;

    ppa.ppa = 0;
    ppa.g.ch = (ppa_ / spp->pgs_per_ch) % spp->pgs_per_ch;
    ppa.g.lun = (ppa_ % spp->pgs_per_ch) / spp->pgs_per_lun;
    ppa.g.pl = 0 ; //ppa_ % spp->tt_pls; // (ppa_ / spp->pgs_per_pl) % spp->pls_per_lun;
    ppa.g.blk = (ppa_ % spp->pgs_per_lun) / spp->pgs_per_blk;
    ppa.g.pg = ppa_ % spp->pgs_per_blk;

    //printk("%s: For PPA %llu we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

#ifndef GC_STANDARD
static uint64_t _record_inv_mapping(uint64_t lpa, ppa_t ppa, uint64_t *credits) {
    struct ssdparams spp = d_member.ssd->sp;
    struct gc_data *gcd = &ftl->gcd;
    struct ppa p = ppa_to_struct(&spp, ppa);
    struct line* l = get_line(ftl, &p); 
    uint64_t line = (uint64_t) l->id;
    uint64_t nsecs_completed = 0;

    NVMEV_DEBUG("Got an invalid LPA to PPA mapping %llu %llu line %d (%llu)\n", 
                 lpa, ppa, line, inv_mapping_offs[line]);

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
        NVMEV_DEBUG("Flushing an invalid mapping page for line %d off %llu to PPA %llu\n", 
                     line, inv_mapping_offs[line], w_ppa);

        oob[w_ppa][0] = U64_MAX;
        oob[w_ppa][1] = (line << 32) | w_ppa;

        struct value_set value;
        value.value = inv_mapping_bufs[line];
        value.ssd = d_member.ssd;
        value.length = INV_PAGE_SZ;

        nsecs_completed = 
        __demand.li->write(w_ppa, INV_PAGE_SZ, &value, ASYNC, NULL);
        nvmev_vdev->space_used += INV_PAGE_SZ;

        NVMEV_DEBUG("Added %llu (%llu %llu) to XA.\n", (line << 32) | w_ppa, line, w_ppa);
        xa_store(&gcd->inv_mapping_xa, (line << 32) | w_ppa, xa_mk_value(w_ppa), GFP_KERNEL);
        //inv_mapping_ppas[line][inv_mapping_cnts[line]++] = w_ppa;
        
        memset(inv_mapping_bufs[line], 0x0, INV_PAGE_SZ);
        inv_mapping_offs[line] = 0;

        (*credits) += GRAIN_PER_PAGE;
    }

    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &lpa, sizeof(lpa));
    inv_mapping_offs[line] += sizeof(lpa);
    memcpy(inv_mapping_bufs[line] + inv_mapping_offs[line], &ppa, sizeof(ppa));
    inv_mapping_offs[line] += sizeof(ppa);

    return nsecs_completed;
}
#endif

static uint64_t read_actual_dpage(ppa_t ppa, request *const req, uint64_t *nsecs_completed) {
    struct ssdparams spp = d_member.ssd->sp;
    uint64_t nsecs = 0;

	if (IS_INITIAL_PPA(ppa)) {
        //printk("%s IS_INITIAL_PPA failure.\n", __func__);
		warn_notfound(__FILE__, __LINE__);
		return U64_MAX;
	}

    struct algo_req *a_req = make_algo_req_rw(DATAR, NULL, req, NULL);
    a_req->parents = req;
    a_req->parents->ppa = ppa;
    //printk("%s ppa %u offset %u\n", 
            //__func__, ppa, ((struct demand_params *)a_req->params)->offset);
#ifdef DVALUE
	((struct demand_params *)a_req->params)->offset = G_OFFSET(ppa);
	ppa = G_IDX(ppa);
#endif

    //printk("%s ppa %u offset %u\n", 
            //__func__, ppa, ((struct demand_params *)a_req->params)->offset);
    req->value->ssd = d_member.ssd;
	nsecs = __demand.li->read(ppa, spp.pgsz, req->value, false, a_req);

    if(nsecs_completed) {
        *nsecs_completed = nsecs;
    }

    if(nsecs == U64_MAX - 1) {
        return 1;
    } else {
        return 0;
    }
}

static uint64_t read_for_data_check(ppa_t ppa, snode *wb_entry) {
    struct ssdparams spp = d_member.ssd->sp;
	value_set *_value_dr_check = inf_get_valueset(NULL, FS_MALLOC_R, spp.pgsz);
	struct algo_req *a_req = make_algo_req_rw(DATAR, _value_dr_check, NULL, wb_entry);
    uint64_t nsecs_completed;

    a_req->ppa = ppa;
#ifdef DVALUE
	((struct demand_params *)a_req->params)->offset = G_OFFSET(ppa);
	ppa = G_IDX(ppa);
#endif
    _value_dr_check->ssd = d_member.ssd;
	nsecs_completed = __demand.li->read(ppa, spp.pgsz, _value_dr_check, ASYNC, a_req);
	return nsecs_completed;
}

uint64_t __demand_read(request *const req, bool for_del) {
	uint64_t rc = 0;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;
    uint64_t credits = 0;

	struct hash_params *h_params = (struct hash_params *)req->hash_params;

	lpa_t lpa;
	struct pt_struct pte;

read_retry:
	lpa = get_lpa(req->key, req->hash_params);
    //printk("Got LPA %u for key %s!\n", lpa, req->key.key);
	pte.ppa = U64_MAX;
#ifdef STORE_KEY_FP
	pte.key_fp = FP_MAX;
#endif

#ifdef HASH_KVSSD
	if (h_params->cnt > d_member.max_try) {
		rc = U64_MAX;
        req->value->length = 0;
		warn_notfound(__FILE__, __LINE__);
		goto read_ret;
	}
#endif

    nsecs_latest = max(nsecs_completed, nsecs_latest);

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

	/* 1. check write buffer first */
	rc = do_wb_check(d_member.write_buffer, req);
	if (rc) {
        req->ppa = U64_MAX - 1;
        if(for_del) {
            do_wb_delete(d_member.write_buffer, req);
        } else {
            nsecs_completed =
            ssd_advance_pcie(req->ssd, req->req->nsecs_start, 1024);
            req->end_req(req);
        }
		goto read_ret;
	}

	/* 2. check cache */
	if (d_cache->is_hit(lpa)) {
		d_cache->touch(lpa);
        NVMEV_DEBUG("Cache hit for LPA %llu!\n", lpa);
	} else {
cache_load:
		rc = d_cache->wait_if_flying(lpa, req, NULL);
		if (rc) {
			goto read_ret;
		}
		rc = d_cache->load(lpa, req, NULL, &nsecs_completed);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
		if (!rc) {
            req->ppa = U64_MAX;
            req->value->length = 0;
			rc = U64_MAX;
			warn_notfound(__FILE__, __LINE__);
            goto read_ret;
		}
cache_list_up:
		rc = d_cache->list_up(lpa, req, NULL, &nsecs_completed, &credits);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
		//if (rc) {
        //    req->value->length = 0;
		//	goto read_ret;
		//}
	}

cache_check_complete:
	free_iparams(req, NULL);

	pte = d_cache->get_pte(lpa);
#ifdef STORE_KEY_FP
	/* fast fingerprint compare */
	if (h_params->key_fp != pte.key_fp) {
		h_params->cnt++;
		goto read_retry;
	}
#endif
data_read:
	/* 3. read actual data */
    NVMEV_DEBUG("Got PPA %lluu for LPA %llu %llu %llu\n", pte.ppa, lpa, nsecs_latest, nsecs_completed);
    rc = read_actual_dpage(pte.ppa, req, &nsecs_completed);
    nsecs_latest = nsecs_latest == U64_MAX - 1 ? nsecs_completed :  max(nsecs_latest, nsecs_completed);

    if(rc == U64_MAX) {
        req->ppa = U64_MAX;
        req->value->length = 0;
        warn_notfound(__FILE__, __LINE__);
        goto read_ret;
    } else if(nsecs_latest == U64_MAX - 1) {
        NVMEV_DEBUG("Retrying a read for key %s cnt %u\n", req->key.key, h_params->cnt);
        goto read_retry;
    }

    if(h_params->cnt > 0) {
        //printk("Eventually finished a retried read cnt %u\n", h_params->cnt);
    }

    if(for_del) {
        NVMEV_ASSERT(!IS_INITIAL_PPA(pte.ppa));

        uint64_t offset = G_OFFSET(pte.ppa);
        uint32_t len = 1;

        while(offset + len < GRAIN_PER_PAGE && oob[G_IDX(pte.ppa)][offset + len] == 0) {
            len++;
        }

        NVMEV_DEBUG("Deleting a pair of length %u (%u) grain %llu PPA %llu\n", 
                    len, len * GRAINED_UNIT, pte.ppa, G_IDX(pte.ppa));

        oob[G_IDX(pte.ppa)][offset] = 2;
        mark_grain_invalid(ftl, pte.ppa, len);
#ifndef GC_STANDARD
        _record_inv_mapping(lpa, G_IDX(pte.ppa), &credits);
#endif
        req->ppa = U64_MAX - 2;

        pte.ppa = U64_MAX;
        d_cache->update(lpa, pte);
        d_htable_insert(d_member.hash_table, U64_MAX, lpa);

        nvmev_vdev->space_used -= len * GRAINED_UNIT;
    }

    if(credits > 0) {
        consume_write_credit(ftl, credits);
        check_and_refill_write_credit(ftl);
    }

read_ret:
	return nsecs_latest;
}


static bool wb_is_full(skiplist *wb) { return (wb->size == d_env.wb_flush_size); }

uint32_t cnt = 0;
static bool _do_wb_assign_ppa(skiplist *wb) {
	blockmanager *bm = __demand.bm;
	struct flush_list *fl = d_member.flush_list;
    struct ssdparams spp = d_member.ssd->sp;

	snode *wb_entry;
	sk_iter *iter = skiplist_get_iterator(wb);

#ifdef DVALUE
	l_bucket *wb_bucket = (l_bucket *)kzalloc(sizeof(l_bucket), GFP_KERNEL);
	for (int i = 1; i <= GRAIN_PER_PAGE; i++) {
		wb_bucket->bucket[i] = (snode **)kzalloc(d_env.wb_flush_size * sizeof(snode *), GFP_KERNEL);
		wb_bucket->idx[i] = 0;
	}

	for (size_t i = 0; i < d_env.wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		int val_len = wb_entry->value->length;

		wb_bucket->bucket[val_len][wb_bucket->idx[val_len]] = wb_entry;
		wb_bucket->idx[val_len]++;
	}

	int ordering_done = 0;
	while (ordering_done < d_env.wb_flush_size) {
		value_set *new_vs = inf_get_valueset(NULL, FS_MALLOC_W, spp.pgsz);
		PTR page = new_vs->value;
		int remain = spp.pgsz;
        uint32_t credits = 0;

        struct ppa ppa_s = get_new_page(ftl, USER_IO);
        if(!advance_write_pointer(ftl, USER_IO)) {
            NVMEV_ERROR("Failing wb flush because we had no available pages!\n");
            /*
             * TODO
             * Leak here of remaining wb_entries.
             */

            /*
             * So we can just rmmod nvmev instead of rebooting the VM.
             */

            FAIL_MODE = true;
            return false;
        }

        mark_page_valid(ftl, &ppa_s);
		ppa_t ppa = ppa2pgidx(ftl, &ppa_s);

        struct ppa tmp_ppa = ppa_to_struct(&d_member.ssd->sp, ppa);
        NVMEV_DEBUG("%s assigning PPA %llu (%u)\n", __func__, ppa, cnt++);

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
			memcpy(&page[offset*GRAINED_UNIT], 
                   wb_entry->value->value, 
                   wb_entry->value->length * GRAINED_UNIT);
            char tmp[128];
            memcpy(tmp, wb_entry->value->value + 1, wb_entry->key.len);
            tmp[wb_entry->key.len] = '\0';
            NVMEV_DEBUG("%s writing %s (%u %s) to %llu (%llu)\n", 
                        __func__, tmp, wb_entry->key.len, wb_entry->key.key,
                        ppa + (offset * GRAINED_UNIT), wb_entry->ppa);

			inf_free_valueset(wb_entry->value, FS_MALLOC_W);
			wb_entry->value = NULL;

			//validate_grain(bm, wb_entry->ppa);
            mark_grain_valid(ftl, wb_entry->ppa, target_length);

            credits += target_length;
			offset += target_length;
			remain -= target_length * GRAINED_UNIT;

			ordering_done++;
		}

        if(remain > 0) {
            //NVMEV_ERROR("Had %u bytes leftover PPA %llu offset %u.\n", remain, ppa, offset);
            //NVMEV_ERROR("Ordering %s.\n", ordering_done < d_env.wb_flush_size ? "NOT DONE" : "DONE");
            mark_grain_valid(ftl, PPA_TO_PGA(ppa, offset), 
                    GRAIN_PER_PAGE - offset);
            mark_grain_invalid(ftl, PPA_TO_PGA(ppa, offset), 
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
	for (int i = 0; i < d_env.wb_flush_size; i++) {
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

static uint64_t _do_wb_mapping_update(skiplist *wb) {
	int rc = 0;
    uint64_t credits = 0;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;
	blockmanager *bm = __demand.bm;

    bool sample = false;
    uint64_t touch = 0, wait = 0, load = 0, list_up = 0, get = 0, update = 0;
    uint64_t start, end;

    uint32_t rand;
    get_random_bytes(&rand, sizeof(rand));
    if(rand % 100 > 75) {
        start = local_clock();
        sample = true;
    }

	snode *wb_entry;
	struct hash_params *h_params;

	lpa_t lpa;
	struct pt_struct pte, new_pte;

	/* push all the wb_entries to queue */
	sk_iter *iter = skiplist_get_iterator(wb);
	for (int i = 0; i < d_env.wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		q_enqueue((void *)wb_entry, d_member.wb_master_q);
	}
	kfree(iter);

	/* mapping update */
	volatile int updated = 0;
	while (updated < d_env.wb_flush_size) {
		wb_entry = (snode *)q_dequeue(d_member.wb_retry_q);
		if (!wb_entry) {
			wb_entry = (snode *)q_dequeue(d_member.wb_master_q);
		}
		if (!wb_entry) continue;
wb_retry:
		h_params = (struct hash_params *)wb_entry->hash_params;

		lpa = get_lpa(wb_entry->key, wb_entry->hash_params);
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

		if (d_cache->is_hit(lpa)) {
            NVMEV_DEBUG("%s hit for LPA %llu\n", __func__, lpa);
            if(sample) {
                start = local_clock();
                d_cache->touch(lpa);
                end = local_clock();
                touch += end - start;
            }
		} else {
            //printk("%s miss for LPA %u\n", __func__, lpa);
wb_cache_load:
            start = local_clock();
			rc = d_cache->wait_if_flying(lpa, NULL, wb_entry);
            end = local_clock();
            wait += end - start;
			if (rc) continue; /* pending */

            //printk("%s passed wait_if_flying for LPA %u\n", __func__, lpa);
            
            start = local_clock();
			rc = d_cache->load(lpa, NULL, wb_entry, NULL);
            end = local_clock();
            load += end - start;
			if (rc) continue; /* mapping read */

            //printk("%s passed load for LPA %u\n", __func__, lpa);
wb_cache_list_up:
            /*
             * TODO time here.
             */

            start = local_clock();
			rc = d_cache->list_up(lpa, NULL, wb_entry, NULL, &credits);
            end = local_clock();
            list_up += end - start;
			if (rc) continue; /* mapping write */

            //printk("%s passed list_up for LPA %u\n", __func__, lpa);
		}

wb_data_check:
		/* get page_table entry which contains {ppa, key_fp} */
		start = local_clock();
        pte = d_cache->get_pte(lpa);
        end = local_clock();
        get += end - start;
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
		rc = d_htable_find(d_member.hash_table, pte.ppa, lpa);
		if (rc) {
			h_params->find = HASH_KEY_DIFF;
			h_params->cnt++;

			goto wb_retry;
		}
        //printk("%s passed hash table check for LPA %u\n", __func__, lpa);

		/* data check is necessary before update */
		nsecs_completed = read_for_data_check(pte.ppa, wb_entry);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        //printk("%s passed data check for LPA %u\n", __func__, lpa);
		continue;
#endif

wb_update:
        NVMEV_DEBUG("1 %s LPA %llu PPA %llu update in cache.\n", __func__, lpa, new_pte.ppa);
		pte = d_cache->get_pte(lpa);
		if (!IS_INITIAL_PPA(pte.ppa)) {
            __hash_remove(lpa);

            uint64_t offset = G_OFFSET(pte.ppa);
            uint32_t len = 1;
            while(offset + len < GRAIN_PER_PAGE && oob[G_IDX(pte.ppa)][offset + len] == 0) {
                len++;
            }

            NVMEV_DEBUG("%s LPA %llu old PPA %llu overwrite old len %u.\n", 
                        __func__, lpa, pte.ppa, len);

            mark_grain_invalid(ftl, pte.ppa, wb_entry->len);
#ifndef GC_STANDARD
            nsecs_completed = _record_inv_mapping(lpa, G_IDX(pte.ppa), &credits);
#endif
            nsecs_latest = max(nsecs_latest, nsecs_completed);

			static int over_cnt = 0; over_cnt++;
			if (over_cnt % 100000 == 0) printk("overwrite: %d\n", over_cnt);
		} else {
            NVMEV_ERROR("INSERT: Key %s\n", wb_entry->key.key);
            nvmev_vdev->space_used += wb_entry->len * GRAINED_UNIT;
        }
wb_direct_update:
        start = local_clock();
		d_cache->update(lpa, new_pte);
        end = local_clock();
        update += end - start;
        NVMEV_DEBUG("2 %s LPA %llu PPA %llu update in cache.\n", __func__, lpa, new_pte.ppa);

		updated++;
		//inflight--;

        //struct ht_mapping *m = (struct ht_mapping*) kzalloc(sizeof(*m), GFP_KERNEL); 
        //m->lpa = lpa;
        //m->ppa = new_pte.ppa;
        //hash_add(mapping_ht, &m->node, lpa);

		d_htable_insert(d_member.hash_table, new_pte.ppa, lpa);

#ifdef HASH_KVSSD
		d_member.max_try = (h_params->cnt > d_member.max_try) ? h_params->cnt : d_member.max_try;
		hash_collision_logging(h_params->cnt, DWRITE);

		d_set_oob(lpa, G_IDX(new_pte.ppa), G_OFFSET(new_pte.ppa), wb_entry->len);
        print_oob(G_IDX(new_pte.ppa));
#endif
	}

	if (unlikely(d_member.wb_master_q->size + d_member.wb_retry_q->size > 0)) {
		//printk("[ERROR] wb_entry still remains in queues, at %s:%d\n", __FILE__, __LINE__);
		printk("Should have aborted!!!! %s:%d MQ size RQ size %u %u\n", 
                __FILE__, __LINE__, d_member.wb_master_q->size, d_member.wb_retry_q->size);
        wb_entry = (snode *)q_dequeue(d_member.wb_master_q);
        printk("Last one was LPA %llu PPA %llu key %s\n", wb_entry->lpa, wb_entry->ppa,
                wb_entry->key.key);
        BUG_ON(true);
	}

	iter = skiplist_get_iterator(wb);
	for (size_t i = 0; i < d_env.wb_flush_size; i++) {
		snode *wb_entry = skiplist_get_next(iter);
        credits += wb_entry->len;
		if (wb_entry->hash_params) kfree(wb_entry->hash_params);
		free_iparams(NULL, wb_entry);
	}

    /*
     * CAUTION, only consume credits after both the OOBs and grains
     * have been set for the writes you want to consume the credits for.
     * Otherwise, GC will be incorrect because it will see pages
     * with valid grains, but invalid OOBs, or vice versa.
     */

    consume_write_credit(ftl, credits);
    nsecs_completed = check_and_refill_write_credit(ftl);
    nsecs_latest = max(nsecs_latest, nsecs_completed);

	kfree(iter);

    if(sample) {
        //NVMEV_INFO("BREAKDOWN: touch %lluns wait %lluns load %lluns list_up %llu get %llu update %llu.\n",
        //        touch, wait, load, list_up, get, update);
    }

    return nsecs_latest;
}

uint64_t _do_wb_flush(skiplist *wb) {
	struct flush_list *fl = d_member.flush_list;
    struct ssdparams spp = d_member.ssd->sp;
    uint64_t nsecs_completed = 0, nsecs_latest = 0;

	for (int i = 0; i < fl->size; i++) {
		ppa_t ppa = fl->list[i].ppa;
		value_set *value = fl->list[i].value;
        value->ssd = d_member.ssd;

		nsecs_completed = 
        __demand.li->write(ppa, spp.pgsz, value, ASYNC, 
                           make_algo_req_rw(DATAW, value, NULL, NULL));
        nsecs_latest = max(nsecs_latest, nsecs_completed);
    }

	fl->size = 0;
	memset(fl->list, 0, d_env.wb_flush_size * sizeof(struct flush_node));

	d_htable_kfree(d_member.hash_table);
	d_member.hash_table = d_htable_init(d_env.wb_flush_size * 2);

	/* wait until device traffic clean */
	__demand.li->lower_flying_req_wait();

	skiplist_kfree(wb);

	return nsecs_latest;
}

uint64_t pgs_this_flush = 0;
static uint32_t _do_wb_insert(skiplist *wb, request *const req) {
	snode *wb_entry = skiplist_insert(wb, req->key, req->value, true, req->sqid);
    //NVMEV_DEBUG("%s K1 %s KLEN %u K2 %s\n", __func__,
    //        (char*) wb_entry->key.key, *(uint8_t*) (wb_entry->value->value), 
    //        (char*) (wb_entry->value->value + 1));
#ifdef HASH_KVSSD
	wb_entry->hash_params = (void *)req->hash_params;
#endif
	req->value = NULL;

	if (wb_is_full(wb)) return 1;
	else return 0;
}

uint64_t __demand_write(request *const req) {
	uint32_t rc = 0;
    uint64_t nsecs_latest = 0, nsecs_completed = 0;
    uint64_t nsecs_start = req->req->nsecs_start;
    uint64_t length = req->value->length;
	skiplist *wb = d_member.write_buffer;
    struct ssdparams *spp = &d_member.ssd->sp;

	/* flush the buffer if full */
	if (wb_is_full(wb)) {
		/* assign ppa first */
        
        uint64_t start = ktime_get_ns();
        nsecs_completed = _do_wb_assign_ppa(wb);
        nsecs_latest = nsecs_completed;
        uint64_t end = ktime_get_ns();
        NVMEV_DEBUG("_do_wb_assign_ppa took %lluns\n", end - start);

		/* mapping update [lpa, origin]->[lpa, new] */
        start = ktime_get_ns();
		nsecs_completed = _do_wb_mapping_update(wb);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        end = ktime_get_ns();
        NVMEV_DEBUG("_do_wb_mapping_update took %lluns\n", end - start);
		
		/* flush the buffer */
        start = ktime_get_ns();
		nsecs_latest = _do_wb_flush(wb);
        nsecs_latest = max(nsecs_latest, nsecs_completed);
        wb = d_member.write_buffer =  skiplist_init();
        end = ktime_get_ns();
        NVMEV_DEBUG("_do_wb_flush took %lluns\n", end - start);
        NVMEV_DEBUG("Assigned new WB.\n");

        //NVMEV_INFO("%llu pages this flush. %lu pgs_per_line.", 
        //            pgs_this_flush, spp->pgs_per_line);
    }

    pgs_this_flush = 0;

    ////printk("Advancing WB %llu start %llu length.\n", nsecs_start, length);

	/* default: insert to the buffer */
	rc = _do_wb_insert(wb, req); // rc: is the write buffer is full? 1 : 0
   
    nsecs_completed =
    ssd_advance_write_buffer(req->ssd, nsecs_start, length);

    //printk("%llu %llu\n", nsecs_completed, nsecs_latest);
    nsecs_latest = max(nsecs_completed, nsecs_latest);

	req->end_req(req);
	return nsecs_latest;
}

uint32_t __demand_remove(request *const req) {
	//printk("Hello! remove() is not implemented yet! lol!");
	return 0;
}

uint32_t get_vlen(uint64_t ppa, uint64_t offset) {
    NVMEV_DEBUG("Checking PPA %llu offset %llu\n", ppa, offset);

    uint32_t len = 1;
    while(offset + len < GRAIN_PER_PAGE && oob[ppa][offset + len] == 0) {
        len++;
    }

    NVMEV_DEBUG("%s returning a vlen of %u\n", __func__, len * GRAINED_UNIT);
    return len * GRAINED_UNIT;
}

void *demand_end_req(algo_req *a_req) {
	struct demand_params *d_params = (struct demand_params *)a_req->params;
	request *req = a_req->parents;
	snode *wb_entry = d_params->wb_entry;

    //printk("Entered demand_end_req ppa %u\n", a_req->ppa);

	struct hash_params *h_params;
	struct inflight_params *i_params;
	KEYT check_key;

	dl_sync *sync_mutex = d_params->sync_mutex;

    BUG_ON(!d_params);
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

            BUG_ON(!req->value);

			copy_key_from_value(&check_key, req->value, offset);

            NVMEV_DEBUG("Comparing %s and %s\n", check_key.key, req->key.key);
			if (KEYCMP(req->key, check_key) == 0) {
                NVMEV_DEBUG("Match %s and %s.\n", check_key.key, req->key.key);
				d_stat.fp_match_r++;

                a_req->need_retry = false;
				hash_collision_logging(h_params->cnt, DREAD);
				kfree(h_params);
                req->value->length = get_vlen(G_IDX(req->ppa), offset);
				req->end_req(req);
			} else {
                NVMEV_DEBUG("Mismatch %s and %s.\n", check_key.key, req->key.key);
				d_stat.fp_collision_r++;

				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;

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

                //wb_entry->len = get_vlen(G_IDX(a_req->ppa), offset) / GRAINED_UNIT;
				q_enqueue((void *)wb_entry, d_member.wb_retry_q);
			} else {
				/* retry */
				d_stat.fp_collision_w++;

				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;

				q_enqueue((void *)wb_entry, d_member.wb_master_q);
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

		inf_free_valueset(d_params->value, FS_MALLOC_W);
#ifndef DVALUE
		kfree(wb_entry->hash_params);
#endif
		break;
	case MAPPINGR:
        NVMEV_ERROR("In MAPPINGR.\n");
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
			q_enqueue((void *)wb_entry, d_member.wb_retry_q);
            return NULL;
		}
		break;
	case MAPPINGW:
		d_stat.trans_w++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		if (IS_READ(req)) {
			d_stat.t_write_on_read++;
			req->type_ftl+=100;
            return NULL;
            //printk("Re-queuing read-type req in MAPPINGW.\n");
			//insert_retry_read(req);
			//inf_assign_try(req);
		} else {
			d_stat.t_write_on_write++;
			q_enqueue((void *)wb_entry, d_member.wb_retry_q);
		}
		break;
	case GCDR:
		d_stat.data_r_dgc++;
		d_member.nr_valid_read_done++;
		break;
	case GCDW:
		d_stat.data_w_dgc++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	case GCMR_DGC:
		d_stat.trans_r_dgc++;
		d_member.nr_tpages_read_done++;
		//inf_free_valueset(d_params->value, FS_MALLOC_R);
		break;
	case GCMW_DGC:
		d_stat.trans_w_dgc++;
		//inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	case GCMR:
		d_stat.trans_r_tgc++;
		d_member.nr_valid_read_done++;
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

