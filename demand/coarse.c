/*
 * Coarse-grained Cache (default)
 */

#include "cache.h"
#include "coarse.h"
#include "utility.h"
#include "page.h"
#include "./interface/interface.h"

extern struct algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

struct demand_cache cg_cache = {
	.create = cg_create,
	.destroy = cg_destroy,
	.load = cg_load,
	.list_up = cg_list_up,
	.wait_if_flying = cg_wait_if_flying,
	.touch = cg_touch,
	.update = cg_update,
	.get_pte = cg_get_pte,
	.get_cmt = cg_get_cmt,
/*	.get_ppa = cg_get_ppa,
#ifdef STORE_KEY_FP
	.get_fp = cg_get_fp,
#endif */
	.is_hit = cg_is_hit,
	.is_full = cg_is_full,
};

static struct cache_env *cenv;
static struct cache_member *cmbr;
static struct cache_stat *cstat;


static void print_cache_env(struct cache_env *const _env) {
	printk("\n");
	printk(" |---------- Demand Cache Log: Coarse-grained Cache\n");
	printk(" | Total trans pages:        %d\n", _env->nr_valid_tpages);
	//printk(" | Caching Ratio:            %0.3f%%\n", _env->caching_ratio * 100);
	printk(" | Caching Ratio:            same as PFTL\n");
	printk(" |  - Max cached tpages:     %d (%u bytes)\n", 
          _env->max_cached_tpages, _env->max_cached_tpages * PAGESIZE);
	//printk(" |  (PageFTL cached tpages:  %d)\n", _env->nr_tpages_optimal_caching);
	printk(" |---------- Demand Cache Log END\n");
	printk("\n");
}
static void cg_env_init(cache_t c_type, struct cache_env *const _env) {
	_env->c_type = c_type;

	_env->nr_tpages_optimal_caching = d_env.nr_pages * 4 / PAGESIZE;
	_env->nr_valid_tpages = d_env.nr_pages / EPP + ((d_env.nr_pages % EPP) ? 1 : 0);
	_env->nr_valid_tentries = _env->nr_valid_tpages * EPP;

	//_env->caching_ratio = d_env.caching_ratio;
	//_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio;
	_env->max_cached_tpages = _env->nr_valid_tpages; // (d_env.size / K) / PAGESIZE;
	_env->max_cached_tentries = 0; // not used here

#ifdef DVALUE
	_env->nr_valid_tpages *= GRAIN_PER_PAGE / 2;
	_env->nr_valid_tentries *= GRAIN_PER_PAGE / 2;
#endif

	print_cache_env(_env);
}

static void cg_member_init(struct cache_member *const _member) {
	struct cmt_struct **cmt = 
    (struct cmt_struct **)kzalloc(cenv->nr_valid_tpages * sizeof(struct cmt_struct *), GFP_KERNEL);
	for (int i = 0; i < cenv->nr_valid_tpages; i++) {
		cmt[i] = (struct cmt_struct *)kzalloc(sizeof(struct cmt_struct), GFP_KERNEL);

		cmt[i]->t_ppa = UINT_MAX;
		cmt[i]->idx = i;
		cmt[i]->pt = NULL;
		cmt[i]->lru_ptr = NULL;
		cmt[i]->state = CLEAN;
		cmt[i]->is_flying = false;

		q_init(&cmt[i]->retry_q, d_env.wb_flush_size);
		q_init(&cmt[i]->wait_q, d_env.wb_flush_size);

		cmt[i]->dirty_cnt = 0;
	}
	_member->cmt = cmt;

	_member->mem_table = 
    (struct pt_struct **)kzalloc(cenv->nr_valid_tpages * sizeof(struct pt_struct *), GFP_KERNEL);
	for (int i = 0; i < cenv->nr_valid_tpages; i++) {
		_member->mem_table[i] = (struct pt_struct *)kzalloc(EPP * sizeof(struct pt_struct), GFP_KERNEL);
		for (int j = 0; j < EPP; j++) {
			_member->mem_table[i][j].ppa = UINT_MAX;
#ifdef STORE_KEY_FP
			_member->mem_table[i][j].key_fp = 0;
#endif
		}
	}

	lru_init(&_member->lru);

	_member->nr_cached_tpages = 0;
}

static void cg_stat_init(struct cache_stat *const _stat) {

}

int cg_create(cache_t c_type, struct demand_cache *dc) {
	cenv = &dc->env;
	cmbr = &dc->member;
	cstat = &dc->stat;

	cg_env_init(c_type, &dc->env);
	cg_member_init(&dc->member);
	cg_stat_init(&dc->stat);

	return 0;
}

static void cg_print_member(void) {
	//printk("=====================");
	//printk(" Cache Finish Status ");
	//printk("=====================");

	//printk("Max Cached tpages:     %d\n", cenv->max_cached_tpages);
	//printk("Current Cached tpages: %d\n", cmbr->nr_cached_tpages);
	//printk("\n");
}

static void cg_member_kfree(struct cache_member *_member) {
	for (int i = 0; i < cenv->nr_valid_tpages; i++) {
		q_free(_member->cmt[i]->retry_q);
		q_free(_member->cmt[i]->wait_q);
		kfree(_member->cmt[i]);
	}
	kfree(_member->cmt);

	for (int i = 0; i < cenv->nr_valid_tpages; i++) {
		kfree(_member->mem_table[i]);
	}

	lru_kfree(_member->lru);
}

int cg_destroy(void) {
	print_cache_stat(cstat);

	cg_print_member();

	cg_member_kfree(cmbr);
	return 0;
}

int cg_load(lpa_t lpa, request *const req, snode *wb_entry, uint64_t *nsecs_completed) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct inflight_params *i_params;
    uint64_t nsec = 0;

    //printk("%s lpa %u\n", __func__, lpa);
	if (IS_INITIAL_PPA(cmt->t_ppa)) {
        //printk("Unmapped PPA for LPA %u\n", lpa);
		return 0;
	}

	i_params = get_iparams(req, wb_entry);
	i_params->jump = GOTO_LIST;

	value_set *_value_mr = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
    _value_mr->ssd = d_member.ssd;
	nsec = __demand.li->read(cmt->t_ppa, PAGESIZE, _value_mr, ASYNC, 
                                         make_algo_req_rw(MAPPINGR, _value_mr, 
                                         req, wb_entry));

    if(nsecs_completed) {
        *nsecs_completed = nsec;
    }

	cmt->is_flying = true;

	return 1;
}

int cg_list_up(lpa_t lpa, request *const req, snode *wb_entry, 
               uint64_t *nsecs_completed) {
	int rc = 0;
	blockmanager *bm = __demand.bm;
    uint64_t nsecs_latest = 0, nsecs = 0;

	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	struct cmt_struct *victim = NULL;

	struct inflight_params *i_params;

	if (cg_is_full()) {
        //printk("%s lpa %u translation cache full.\n", __func__, lpa);
        BUG_ON(true);
		victim = (struct cmt_struct *)lru_pop(cmbr->lru);
		cmbr->nr_cached_tpages--;

		victim->lru_ptr = NULL;
		victim->pt = NULL;

		if (victim->state == DIRTY) {
			cstat->dirty_evict++;

			i_params = get_iparams(req, wb_entry);
			i_params->jump = GOTO_COMPLETE;
			//i_params->pte = cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];

			victim->t_ppa = get_tpage(bm);
			victim->state = CLEAN;

            //printk("%s dirty page %u.\n", __func__, victim->t_ppa);

			//struct pt_struct pte = cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];

			value_set *_value_mw = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
            _value_mw->ssd = d_member.ssd;
			nsecs = __demand.li->write(victim->t_ppa, PAGESIZE, _value_mw, ASYNC, 
                                       make_algo_req_rw(MAPPINGW, _value_mw, 
                                       req, wb_entry));
			set_oob(bm, victim->idx, victim->t_ppa, MAP);

			rc = 1;
		} else {
			cstat->clean_evict++;
		}
	}

    nsecs_latest = max(nsecs_latest, nsecs);

	cmt->pt = cmbr->mem_table[IDX(lpa)];
    //printk("Mapping PPA %u for LPA %u\n", cmt->t_ppa, lpa);
	cmt->lru_ptr = lru_push(cmbr->lru, (void *)cmt);
	cmbr->nr_cached_tpages++;

	if (cmt->is_flying) {
		cmt->is_flying = false;

		if (req) {
			request *retry_req;
			while ((retry_req = (request *)q_dequeue(cmt->retry_q))) {
				//lpa_t retry_lpa = get_lpa(retry_req->key, retry_req->hash_params);

				struct inflight_params *i_params = get_iparams(retry_req, NULL);
				i_params->jump = GOTO_COMPLETE;
				//i_params->pte = cmt->pt[OFFSET(retry_lpa)];

                //printk("Should have called inf_assign_try in cg_list_up!\n");
				//inf_assign_try(retry_req);
			}
		} else if (wb_entry) {
			snode *retry_wbe;
			while ((retry_wbe = (snode *)q_dequeue(cmt->retry_q))) {
				//lpa_t retry_lpa = get_lpa(retry_wbe->key, retry_wbe->hash_params);

				struct inflight_params *i_params = get_iparams(NULL, retry_wbe);
				i_params->jump = GOTO_COMPLETE;
				//i_params->pte = cmt->pt[OFFSET(retry_lpa)];

				q_enqueue((void *)retry_wbe, d_member.wb_retry_q);
			}
		}
	}

    if(nsecs_completed) {
        *nsecs_completed = nsecs_latest;
    }

	return rc;
}

int cg_wait_if_flying(lpa_t lpa, request *const req, snode *wb_entry) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("%s lpa %u\n", __func__, lpa);

	if (cmt->is_flying) {
		cstat->blocked_miss++;

		if (req) q_enqueue((void *)req, cmt->retry_q);
		else if (wb_entry) q_enqueue((void *)wb_entry, cmt->retry_q);
		else //printk("Should have aborted!!!! %s:%d\n", __FILE__, __LINE__);;

        //printk("%s returning 1\n", __func__);   
		return 1;
	}
    //printk("%s returning 0\n", __func__);   
	return 0;
}

int cg_touch(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	lru_update(cmbr->lru, cmt->lru_ptr);
	return 0;
}

int cg_update(lpa_t lpa, struct pt_struct pte) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];

    //printk("cg_update pte ppa %u for lpa %u\n", pte.ppa, lpa);

	if (cmt->pt) {
		cmt->pt[OFFSET(lpa)] = pte;

		if (!IS_INITIAL_PPA(cmt->t_ppa) && cmt->state == CLEAN) {
			invalidate_page(__demand.bm, cmt->t_ppa, MAP);
		}

		cmt->state = DIRTY;
		lru_update(cmbr->lru, cmt->lru_ptr);
	} else {
		/* FIXME: to handle later update after evict */
		cmbr->mem_table[IDX(lpa)][OFFSET(lpa)] = pte;

		//static int cnt = 0;
		//if (++cnt % 10240 == 0) //printk("cg_update %d\n", cnt);
		////printk("cg_update %d\n", ++cnt);
	}
	return 0;
}

bool cg_is_hit(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	if (cmt->pt != NULL) {
		cstat->cache_hit++;
		return 1;
	} else {
		cstat->cache_miss++;
		return 0;
	}
}

bool cg_is_full(void) {
    return false;
	return (cmbr->nr_cached_tpages >= cenv->max_cached_tpages);
}

struct pt_struct cg_get_pte(lpa_t lpa) {
	struct cmt_struct *cmt = cmbr->cmt[IDX(lpa)];
	if (cmt->pt) {
        //printk("%s returning %u for lpa %u\n", __func__, cmt->pt[OFFSET(lpa)].ppa, lpa);
		return cmt->pt[OFFSET(lpa)];
	} else {
		/* FIXME: to handle later update after evict */
		return cmbr->mem_table[IDX(lpa)][OFFSET(lpa)];
	}
/*	if (unlikely(cmt->pt == NULL)) {
		//printk("Should have aborted!!!! %s:%d
, __FILE__, __LINE__);;
	}
	return cmt->pt[OFFSET(lpa)]; */
}

struct cmt_struct *cg_get_cmt(lpa_t lpa) {
	return cmbr->cmt[IDX(lpa)];
}
