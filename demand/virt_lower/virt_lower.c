#define _LARGEFILE64_SOURCE

#include "../demand.h"
#include "../../nvmev.h"
#include "../include/settings.h"
#include "virt_lower.h"

lower_info virt_info = {
	.create=virt_create,
	.destroy=virt_destroy,
	.write=virt_push_data,
	.read=virt_pull_data,
	.device_badblock_checker=NULL,
	.trim_block=virt_trim_block,
	.trim_a_block=virt_trim_block,
	.refresh=virt_refresh,
	.stop=virt_stop,
	.lower_alloc=NULL,
	.lower_free=NULL,
	.lower_flying_req_wait=virt_flying_req_wait
};

uint32_t virt_create(lower_info *li,blockmanager *bm){
	li->SOK=sizeof(uint32_t);
	li->write_op=li->read_op=li->trim_op=0;

	return 1;
}

void *virt_refresh(lower_info *li){
	li->write_op = li->read_op = li->trim_op = 0;
	return NULL;
}
void *virt_destroy(lower_info *li){
	return NULL;
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

    //NVMEV_DEBUG("%s: For PPA %u we got ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", 
    //        __func__, ppa_, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);

    if(ppa_ > spp->tt_pgs) {
        NVMEV_INFO("Tried %u\n", ppa_);
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(0));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(1));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(2));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(3));
        NVMEV_INFO("Caller is %pS\n", __builtin_return_address(4));
    }

	NVMEV_ASSERT(ppa_ < spp->tt_pgs);

	return ppa;
}

void print_kvs(uint64_t off) {
    int64_t remain = 4096;

    uint8_t klen = 0;
    uint32_t vlen = 0, g_len = 0;

    while(remain > 0) {
        klen = *(uint8_t*) (nvmev_vdev->ns[0].mapped + off);

        if(klen == 0) {
            break;
        }

        vlen = *(uint32_t*) (nvmev_vdev->ns[0].mapped + off + sizeof(uint8_t) + klen);
        //if(vlen % 512 == 0) {
        //    off += vlen;
        //} else {
        g_len = (((vlen + sizeof(uint8_t) + klen + sizeof(uint32_t)) / 512 ) + 1) * 512;
        off += g_len;
        //}

        remain -= g_len;
        NVMEV_DEBUG("Klen %u vlen %u off %u remain %lld ", klen, vlen, off, remain);
    }
    NVMEV_DEBUG("\n");
}

uint64_t virt_push_data(ppa_t PPA, uint32_t size, 
                        value_set* value, bool async,
                        algo_req *const req){
    BUG_ON(async);
    BUG_ON(!value);
    BUG_ON(!value->shard);

    struct demand_shard *shard = value->shard;
    struct ssd* ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;
    uint64_t nsecs_completed = 0;
    struct ppa ppa;
    uint64_t local;

    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off = shard_off + ((uint64_t) PPA * spp->pgsz);

    NVMEV_DEBUG("Writing PPA %u (%llu) size %u pagesize %u in virt_push_datas\n", 
                PPA, off, size, ssd->sp.pgsz);

    local = cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);

    ppa = ppa_to_struct(spp, PPA);
    if (last_pg_in_wordline(value->shard, &ppa)) {
        struct nand_cmd swr = {
            .type = USER_IO,
            .cmd = NAND_WRITE,
            .interleave_pci_dma = false,
            .xfer_size = spp->pgsz * spp->pgs_per_oneshotpg,
            .stime = local > req->stime ? local : req->stime,
        };

        swr.ppa = &ppa;
        nsecs_completed = ssd_advance_nand(ssd, &swr);
    }

    memcpy(nvmev_vdev->ns[0].mapped + off, value->value, size);

    if(req) {
        req->end_req(req);
    }

	return nsecs_completed;
}

uint64_t virt_pull_data(ppa_t PPA, uint32_t size, 
                     value_set* value, bool async,
                     algo_req *const req) {	
    uint64_t nsecs_completed, nsecs_latest;
    struct ppa ppa;

    uint64_t now = cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
    struct nand_cmd swr = {
        .type = USER_IO,
        .cmd = NAND_READ,
        .interleave_pci_dma = true,
        .xfer_size = size,
        .stime = req->stime,
    };

    BUG_ON(async);
    BUG_ON(!value);
    BUG_ON(!value->shard);

    struct demand_shard *shard = value->shard;
    struct ssd* ssd = shard->ssd;
    struct ssdparams *spp = &ssd->sp;

    uint64_t shard_off = shard->id * spp->tt_pgs * spp->pgsz;
    uint64_t off = shard_off + ((uint64_t) PPA * spp->pgsz);

    NVMEV_DEBUG("Reading PPA %u (%llu) size %u sqid %u %s in virt_dev\n", 
                 PPA, off, size, nvmev_vdev->sqes[1]->qid, 
                 async ? "ASYNCHRONOUSLY" : "SYNCHRONOUSLY");

    ppa = ppa_to_struct(spp, PPA);
    swr.ppa = &ppa;
    nsecs_completed = ssd_advance_nand(ssd, &swr);

    memcpy(value->value, nvmev_vdev->ns[0].mapped + off, size);

    if(req) {
        req->end_req(req);
        kfree(req->params);
    }

    return nsecs_completed;
}

void *virt_trim_block(ppa_t PPA, bool async){
	virt_info.req_type_cnt[TRIM]++;
	uint64_t range[2];
	//range[0]=PPA*virt_info.SOP;
	//range[0]=offset_hooker((uint64_t)PPA*virt_info.SOP,TRIM);
	//range[1]=_PPB*virt_info.SOP;
	return NULL;
}

void virt_stop(void){}

void virt_flying_req_wait(void){
	return ;
}
