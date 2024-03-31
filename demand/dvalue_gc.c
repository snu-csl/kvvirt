/*
 * Dynamic Value GC Implementation
 */

#ifdef DVALUE

#include "cache.h"
#include "demand.h"
#include "utility.h"
#include "cache.h"
#include "./interface/interface.h"

#include <linux/kfifo.h>
#include <linux/sort.h>

static int lpa_cmp(const void *a, const void *b)
{
    const struct lpa_len_ppa *da = a, *db = b;

    if (db->lpa < da->lpa) return -1;
    if (db->lpa > da->lpa) return 1;
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

    //printk("%s: For PPA %u we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

void __update_pt(struct demand_shard *shard, struct cmt_struct *cmt, 
                 lpa_t lpa, ppa_t ppa) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    uint64_t entry_sz = ENTRY_SIZE;

    NVMEV_DEBUG("Trying to update IDX %u LPA %u PPA %u\n",
                 cmt->idx, lpa, ppa);

#ifdef GC_STANDARD
    struct pt_struct pte;
    pte.ppa = ppa;

    cmt->pt[OFFSET(lpa)] = pte;
    return;
#else
    for(int i = 0; i < cmt->cached_cnt; i++) {
        if(cmt->pt[i].lpa == lpa) {
            cmt->pt[i].ppa = ppa;
            return;
        }
    }

    NVMEV_INFO("IDX %u TPPA %u cached cnt %u\n", 
                cmt->idx, cmt->t_ppa, cmt->cached_cnt);
    for(int i = 0; i < cmt->cached_cnt; i++) {
        //NVMEV_INFO("Had LPA %u PPA %u\n", cmt->pt[i].lpa, cmt->pt[i].ppa);
    }

    NVMEV_ASSERT(false);
#endif
}

uint64_t __already_read(uint64_t *read_ppas, uint64_t cnt, uint64_t pg) {
    for(int i = 0; i < cnt; i++) {
        if(read_ppas[cnt] == pg) {
            return i;
        }
    }

    return UINT_MAX;
}

bool allocated = false;
bool *skip_update = NULL;
uint64_t *read_ppas = NULL;
struct ppa *pgs = NULL;

int do_bulk_mapping_update_v(struct demand_shard *shard, 
                             struct lpa_len_ppa *ppas, int nr_valid_grains) {
    struct ssd *ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct demand_cache *cache = shard->cache;
    bool skip_all = true;
    uint64_t **oob = shard->oob;
    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t nsecs_completed = 0, nsecs_latest = 0, reads_done = 0;

    sort(ppas, nr_valid_grains, sizeof(struct lpa_len_ppa), &lpa_cmp, NULL);

    uint64_t prev = UINT_MAX;
    NVMEV_DEBUG("There are %llu unique CMTs to update with %u pairs. Read %llu already.\n", 
                 unique_cmts, nr_valid_grains, read_cmt_cnt);

    if(!allocated) {
        skip_update = (bool*) kzalloc(spp->pgs_per_flashpg * GRAIN_PER_PAGE * sizeof(bool), GFP_KERNEL);
        read_ppas = kzalloc(spp->pgs_per_flashpg * GRAIN_PER_PAGE * sizeof(uint64_t), GFP_KERNEL);
        pgs = 
        (struct ppa*) vmalloc(sizeof(struct ppa) * spp->pgs_per_flashpg * 
                              GRAIN_PER_PAGE);
		memset(pgs, 0x0, sizeof(struct ppa) * spp->pgs_per_flashpg * 
                         GRAIN_PER_PAGE);

        NVMEV_ASSERT(nr_valid_grains <= spp->pgs_per_flashpg * GRAIN_PER_PAGE);
        NVMEV_ASSERT(skip_update);
        NVMEV_ASSERT(read_ppas);
        NVMEV_ASSERT(pgs);
        allocated = true;
    }

    uint64_t read_ppa_cnt = 0;
    uint64_t cmts_loaded = 0;
    uint64_t cur_g_len = 0;
    uint32_t pg_cnt = 1;

	/* read mapping table which needs update */
    volatile int nr_update_tpages = 0;
	for (int i = 0; i < nr_valid_grains; i++) {
        lpa_t lpa = ppas[i].lpa;

        if(lpa == UINT_MAX || lpa == UINT_MAX - 1) {
            skip_update[i] = true;
            continue;
        }

        NVMEV_DEBUG("%s updating mapping of LPA %u IDX %lu to PPA %u\n", 
                     __func__, lpa, IDX(ppas[i].lpa), ppas[i].new_ppa);

        struct cmt_struct *cmt = cache->get_cmt(cache, lpa);
		if (cache->is_hit(cache, lpa)) {
            NVMEV_DEBUG("LPA %u PPA %u IDX %u cached update in %s.\n", 
                         lpa, ppas[i].new_ppa, cmt->idx, __func__);
            __update_pt(shard, cmt, lpa, ppas[i].new_ppa);
			skip_update[i] = true;
		} else {
            skip_update[i] = false;
            NVMEV_DEBUG("LPA %u PPA %u IDX %u not cached update in %s.\n", 
                         lpa, ppas[i].new_ppa, cmt->idx, __func__);

            uint64_t off, g_off;
            g_off = cmt->g_off * GRAINED_UNIT;

#ifdef GC_STANDARD
            prev = UINT_MAX;
#else
            prev = __already_read(read_ppas, read_ppa_cnt, cmt->t_ppa);
#endif
            if (cmt->t_ppa == UINT_MAX) {
                NVMEV_DEBUG("%s But the CMT had already been read here.\n", __func__);
                continue;
            } else if(prev != UINT_MAX) {
#ifdef GC_STANDARD
                /*
                 * In the original scheme, this cmt->t_ppa should have been
                 * set to UINT_MAX below.
                 */
                NVMEV_INFO("Failing for PPA %u\n", cmt->t_ppa);
                NVMEV_ASSERT(false);
#endif
                NVMEV_DEBUG("%s IDX %u PPA %u grain %llu had already been read here.\n", 
                            __func__, cmt->idx, cmt->t_ppa, cmt->g_off);
                mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, cmt->g_off), 
                                   cmt->len_on_disk);

                NVMEV_DEBUG("1 Saw CMT IDX %u with len %u\n", 
                        cmt->idx, cmt->len_on_disk);
                if(cur_g_len + cmt->len_on_disk > GRAIN_PER_PAGE) {
                    pg_cnt++;
                    NVMEV_DEBUG("Inc pg cnt to %u\n", pg_cnt);
                    cur_g_len = 0;
                }

                cur_g_len += cmt->len_on_disk;
                cmt->t_ppa = UINT_MAX;
                continue;
            }

            skip_all = false;
            NVMEV_DEBUG("Trying to read CMT PPA %u IDX %u grain %llu\n", 
                         cmt->t_ppa, cmt->idx, cmt->g_off);

            struct ppa p = ppa_to_struct(spp, cmt->t_ppa);
            struct nand_cmd swr = {
                .type = GC_MAP_IO,
                .cmd = NAND_READ,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz,
                .stime = 0,
            };

            swr.ppa = &p;
            nsecs_completed = ssd_advance_nand(ssd, &swr);
            nsecs_latest = max(nsecs_latest, nsecs_completed);

            d_stat.trans_r_dgc += spp->pgsz;

            NVMEV_DEBUG("%s marking PPA %u grain %llu invalid while it was being read during GC.\n",
                         __func__, cmt->t_ppa, cmt->g_off);
            mark_grain_invalid(shard, PPA_TO_PGA(cmt->t_ppa, cmt->g_off), 
                               cmt->len_on_disk);
            cmt->t_ppa = UINT_MAX;

            NVMEV_DEBUG("2 Saw CMT IDX %u with len %u\n", 
                    cmt->idx, cmt->len_on_disk);
            if(cur_g_len + cmt->len_on_disk > GRAIN_PER_PAGE) {
                pg_cnt++;
                NVMEV_DEBUG("Inc pg cnt to %u\n", pg_cnt);
                cur_g_len = 0;
            }

            cur_g_len += cmt->len_on_disk;

            read_ppas[read_ppa_cnt++] = cmt->t_ppa;

            nr_update_tpages++;
        }
    }

    if(skip_all) {
        //memset(read_ppas, 0x0, spp->pgs_per_flashpg * GRAIN_PER_PAGE * sizeof(uint64_t));
        return 0;
    }

	reads_done = nsecs_latest;

    atomic_t rem;
    atomic_set(&rem, cmts_loaded);

    uint32_t g_off = 0;
    struct ppa p;
    uint64_t ppa;
    uint64_t ppa_idx = 0;

    for(int i = 0; i < pg_cnt; i++) {
again:
        pgs[i] = get_new_page(shard, GC_MAP_IO);
        ppa = ppa2pgidx(shard, &pgs[i]);
        advance_write_pointer(shard, GC_MAP_IO);
        mark_page_valid(shard, &pgs[i]);
        mark_grain_valid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);

        if(ppa == 0) {
            mark_grain_invalid(shard, PPA_TO_PGA(ppa, 0), GRAIN_PER_PAGE);
            goto again;
        }

        NVMEV_DEBUG("Got page %llu\n", ppa);
    }

    p = pgs[ppa_idx++];
    ppa = ppa2pgidx(shard, &p);

    cmts_loaded = 0;
    /* write */
	for (int i = 0; i < nr_valid_grains; i++) {
		if (skip_update[i]) {
			continue;
		}

        uint64_t idx = IDX(ppas[i].lpa);
        lpa_t lpa = ppas[i].lpa;

        struct cmt_struct *cmt = cache->get_cmt(cache, IDX2LPA(idx));
        NVMEV_ASSERT(cmt->pt_mem);

        cmt->pt = cmt->pt_mem;
        __update_pt(shard, cmt, ppas[i].lpa, ppas[i].new_ppa);

        NVMEV_DEBUG("%s 1 setting LPA %u to PPA %u\n",
                     __func__, ppas[i].lpa, ppas[i].new_ppa);

        if(G_IDX(ppas[i].new_ppa) > spp->tt_pgs) {
            printk("Tried to convert PPA %u\n", G_IDX(ppas[i].new_ppa));
            NVMEV_ASSERT(false);
        }

        while(i + 1 < nr_valid_grains && IDX(ppas[i + 1].lpa) == idx) {
            __update_pt(shard, cmt, ppas[i + 1].lpa, ppas[i + 1].new_ppa);
            //offset = OFFSET(ppas[i + 1].lpa);
            //t_cmt.pt[offset].ppa = ppas[i + 1].new_ppa;

            if(G_IDX(ppas[i].new_ppa) > spp->tt_pgs) {
                printk("Tried to convert PPA %u\n", G_IDX(ppas[i].new_ppa));
                NVMEV_ASSERT(false);
            }

            NVMEV_DEBUG("%s 2 setting LPA %u to PPA %u\n",
                         __func__, ppas[i + 1].lpa, ppas[i + 1].new_ppa);

            i++;
        }
        cmt->pt = NULL;

        if(g_off + cmt->len_on_disk > GRAIN_PER_PAGE) {
            if(g_off % GRAIN_PER_PAGE) {
                //for(int i = g_off; i < GRAIN_PER_PAGE; i++) {
                //    oob[ppa][i] = UINT_MAX;
                //}

                mark_grain_invalid(shard, PPA_TO_PGA(ppa, g_off), 
                                   GRAIN_PER_PAGE - g_off);
            }

            p = pgs[ppa_idx++];

            if(ppa_idx == pg_cnt) {
                NVMEV_DEBUG("g_off %u len_on_disk %u\n", 
                            g_off, cmt->len_on_disk);
            }

            ppa = ppa2pgidx(shard, &p);
            g_off = 0;

            NVMEV_DEBUG("Took page %llu\n", ppa);
        }

        oob[ppa][g_off] = ((uint64_t) cmt->len_on_disk << 32) | lpa;
        //for(int i = 1; i < cmt->len_on_disk; i++) {
        //    oob[ppa][g_off + i] = 0;
        //}

        if (last_pg_in_wordline(shard, &p)) {
            struct nand_cmd swr = {
                .type = GC_MAP_IO,
                .cmd = NAND_WRITE,
                .interleave_pci_dma = false,
                .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            };

            swr.stime = 0;//reads_done; // __get_wallclock();
            swr.ppa = &p;

            ssd_advance_nand(shard->ssd, &swr);
            d_stat.trans_w_dgc += spp->pgsz * spp->pgs_per_oneshotpg;
            //schedule_internal_operation(req->sq_id, nsecs_completed, wbuf,
            //        spp->pgs_per_oneshotpg * spp->pgsz);
        }

        cmt->state = CLEAN;
        NVMEV_ASSERT(!cmt->lru_ptr);
        NVMEV_ASSERT(!cmt->pt);
        cmt->t_ppa = ppa;
        cmt->g_off = g_off;

        g_off += cmt->len_on_disk;
        cmts_loaded++;
    }

    if(g_off % GRAIN_PER_PAGE) {
#ifdef GC_STANDARD
        NVMEV_ASSERT(false);
#endif
        //for(int i = g_off; i < GRAIN_PER_PAGE; i++) {
        //    oob[ppa][i] = UINT_MAX;
        //}

        mark_grain_invalid(shard, PPA_TO_PGA(ppa, g_off), 
                           GRAIN_PER_PAGE - g_off);
    }

    if(ppa_idx != pg_cnt) {
        NVMEV_DEBUG("ppa_idx %llu pg_cnt %u\n", ppa_idx, pg_cnt);
    }

    NVMEV_ASSERT(ppa_idx == pg_cnt);

    //memset(read_ppas, 0x0, spp->pgs_per_flashpg * GRAIN_PER_PAGE * sizeof(uint64_t));
	return 0;
}

int dpage_gc_dvalue(blockmanager *bm) {
    NVMEV_ASSERT(false);
}

#endif
