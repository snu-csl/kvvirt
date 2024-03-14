// SPDX-License-Identifier: GPL-2.0-only

#include <linux/highmem.h>
#include <linux/kthread.h>
#include <linux/ktime.h>
#include <linux/random.h>
#include <linux/sched/clock.h>

#include "demand/include/demand_settings.h"
#include "demand/d_type.h"
#include "nvmev.h"
#include "nvme_kv.h"
#include "dma.h"

#if (SUPPORTED_SSD_TYPE(CONV) || SUPPORTED_SSD_TYPE(ZNS))
#include "ssd.h"
#else
struct buffer;
#endif

#undef PERF_DEBUG

#define sq_entry(entry_id) sq->sq[SQ_ENTRY_TO_PAGE_NUM(entry_id)][SQ_ENTRY_TO_PAGE_OFFSET(entry_id)]
#define cq_entry(entry_id) cq->cq[CQ_ENTRY_TO_PAGE_NUM(entry_id)][CQ_ENTRY_TO_PAGE_OFFSET(entry_id)]

extern bool io_using_dma;

static inline unsigned int __get_io_worker(int sqid)
{
#ifdef CONFIG_NVMEV_IO_WORKER_BY_SQ
	return (sqid - 1) % nvmev_vdev->config.nr_io_workers;
#else
	return nvmev_vdev->io_worker_turn;
#endif
}

static inline unsigned long long __get_wallclock(void)
{
	return cpu_clock(nvmev_vdev->config.cpu_nr_dispatcher);
}

#if (BASE_SSD == SAMSUNG_970PRO_HASH_DFTL)
/*
 * Copy from the disk to an in-memory buffer outside of virt's reserved memory.
 * For example, in a demand-based FTL we may need to copy
 * mapping entries from the disk into a cache somewhere.
 */

static unsigned int __do_perform_internal_copy(ppa_t ppa, void* dst, 
                                               uint32_t len, bool read)
{
    uint64_t offset = ppa;

    if(read) {
        printk("Performing an internal read from ppa %u len %u\n", ppa, len);
        memcpy(dst, nvmev_vdev->ns[0].mapped + offset, len);
    } else {
        memcpy(nvmev_vdev->ns[0].mapped + offset, dst, len);
        printk("Performing an internal write to ppa %u len %u data %s\n", 
                ppa, len, (char*) nvmev_vdev->ns[0].mapped + offset);
    }

    return 0;
}

int is_aligned(void *ptr, size_t alignment) {
    uintptr_t ptr_as_uint = (uintptr_t)ptr;
    return (ptr_as_uint & (alignment - 1)) == 0;
}

static unsigned int __do_perform_io_kv(int sqid, int sq_entry)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	struct nvme_command *b_cmd = &sq_entry(sq_entry);
    struct nvme_kv_command *cmd = (struct nvme_kv_command*) b_cmd;

	size_t offset;
	size_t length, remaining, orig, orig_len = 0;
    uint32_t real_vlen;
	int prp_offs = 0;
	int prp2_offs = 0;
	u64 paddr;
	u64 *paddr_list = NULL;
	size_t nsid = 0;  // 0-based

    bool read = cmd->common.opcode == nvme_cmd_kv_retrieve;
    bool write = cmd->common.opcode == nvme_cmd_kv_store;
    bool delete = cmd->common.opcode == nvme_cmd_kv_delete;
    bool append = cmd->common.opcode == nvme_cmd_kv_append;

    if(delete) {
        return 0;
    }

    nsid = 0;

    if(read) {
        offset = cmd->kv_retrieve.rsvd;
        if(offset != U64_MAX) {
            uint8_t *ptr = (uint8_t*) offset;
            uint8_t klen = *(uint8_t*) ptr;
            real_vlen = *(uint32_t*) (ptr + 
                    sizeof(uint8_t) + klen);
            length = real_vlen + sizeof(uint32_t);
        }
    } else if(write) {
        offset = cmd->kv_store.rsvd;
        length = (cmd->kv_store.value_len << 2) - cmd->kv_store.invalid_byte;
    } else if(append) {
        offset = cmd->kv_append.rsvd;
        length = (cmd->kv_append.value_len << 2) - cmd->kv_append.invalid_byte;
        orig = ((uint64_t) cmd->kv_append.rsvd2 << 32) | cmd->kv_append.nsid;
        orig_len = cmd->kv_append.offset == 0 ? 
                   0 : cmd->kv_append.offset + sizeof(uint32_t);
    } else {
        NVMEV_ASSERT(false);
    }

    if(offset == UINT_MAX - 1) {
        return length;
    } else if (offset == U64_MAX) {
        //NVMEV_INFO("Failing command.\n");
        return 0;
    }

	remaining = length;

    if(append && orig_len > 0) {
        /*
         * Copy the existing KV pair data first.
         */
        //char v[8];
        //memcpy(v, nvmev_vdev->ns[nsid].mapped + orig + orig_len - 16, 8);
        //char v2[8];
        //memcpy(v2, nvmev_vdev->ns[nsid].mapped + orig, 8);

        //NVMEV_INFO("Copying orig len %lu to offset %lu from %lu first key %s last key %s\n", 
        //            orig_len, offset, orig, v2, v);
        memcpy(nvmev_vdev->ns[nsid].mapped + offset, 
               nvmev_vdev->ns[nsid].mapped + orig, orig_len);
        offset += orig_len;
    }

    void *vaddr;
	while (remaining) {
		size_t io_size;
		size_t mem_offs = 0;

		prp_offs++;
		if (prp_offs == 1) {
            if(read) {
                paddr = cmd->kv_retrieve.dptr.prp1;
            } else if(write) {
                paddr = cmd->kv_store.dptr.prp1;
            } else if(append) {
                paddr = cmd->kv_append.dptr.prp1;
            }
		} else if (prp_offs == 2) {
            if(read) {
                paddr = cmd->kv_retrieve.dptr.prp2;
            } else if(write) {
                paddr = cmd->kv_store.dptr.prp2;
            } else if(append) {
                paddr = cmd->kv_append.dptr.prp2;
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

        if(!is_aligned(vaddr, 4096)) {
            NVMEV_ERROR("Trying to perform IO on unaligned buffer!\n");
        }

		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

        if(write || (append && orig_len == 0)) {
            memcpy((void*) offset, vaddr + mem_offs, io_size);
            //NVMEV_INFO("Wrote key %s to offset %lu\n",
            //            (char*) (((char*) offset) + 1), offset);
            //char v[9];
            //memcpy(v, nvmev_vdev->ns[nsid].mapped + offset + (io_size - 16), 8);
            //v[8] = '\0';
            //NVMEV_INFO("Copying write length %lu (%s) to %lu last key %s\n", 
            //            io_size, (char*) (vaddr + mem_offs + 9), offset, v);
        } else if(read) {
            uint8_t *ptr = (uint8_t*) offset;
            uint8_t klen = *(uint8_t*) ptr;
            //char v1[9];
            //char v[9];

            //int off = prp_offs == 1 ? 13 : 0;
            //memcpy(v, nvmev_vdev->ns[nsid].mapped + offset + io_size - 16, 8);
            //memcpy(v1, nvmev_vdev->ns[nsid].mapped + offset + off, 8);
            //v[8] = '\0';
            //v1[8] = '\0';
            //NVMEV_INFO("Copying %lu bytes from offset %lu to mem_offs %lu first key %s last key %s\n",
            //            io_size, offset, mem_offs, v1, v);
            memcpy(vaddr + mem_offs, (void*) offset, io_size);

            if(prp_offs == 1) {
                /*
                 * The first copy contains the first grain, which
                 * contains the value length. We don't copy the value length
                 * back to the user inside the buffer.
                 */
                uint32_t length = *(uint32_t*) (vaddr + mem_offs + sizeof(uint8_t) + klen);
                real_vlen = length;
                //NVMEV_INFO("Got a value length of %u from offset %lu\n",
                //            (uint32_t) length, offset + 
                //            sizeof(uint8_t) + klen);
                memmove(vaddr + mem_offs + sizeof(uint8_t) + klen, 
                        vaddr + mem_offs + sizeof(uint8_t) + klen + sizeof(uint32_t),
                        io_size - sizeof(uint32_t) - sizeof(uint8_t) - klen);

                //memcpy(v, vaddr + mem_offs + io_size - 16 - sizeof(uint32_t), 8);
                //NVMEV_INFO("Moved %lu bytes from offset %lu to offset %lu new last key %s\n",
                //            io_size - sizeof(uint32_t) - sizeof(uint8_t) - klen,
                //            offset + sizeof(u_int8_t) + klen + sizeof(uint32_t),
                //            offset + sizeof(u_int8_t) + klen, v);
                memcpy(vaddr + mem_offs + io_size - sizeof(uint32_t), 
                       ((char*) offset) + io_size, 
                       sizeof(uint32_t));
                //char v2[9];
                //memcpy(v2, vaddr + mem_offs + io_size - 16, 8);
                //v2[8] = '\0';
                //NVMEV_INFO("Copying %lu extra bytes from offset %lu to offset %lu last key %s\n",
                //            sizeof(uint32_t), offset + io_size,
                //            mem_offs + io_size - sizeof(uint32_t), v2);
                offset += sizeof(uint32_t);
            }
        } else if(append) {
            uint8_t *ptr = nvmev_vdev->ns[nsid].mapped + orig;
            uint8_t klen = *(uint8_t*) ptr;
            if(prp_offs == 1) {
                /*
                 * This is the start of the copy, and We're appending to 
                 * an existing KV pair, don't include the key length and 
                 * key.
                 */
                mem_offs += sizeof(uint8_t) + klen;
                //NVMEV_INFO("Skipped key data (%u) and copying %lu bytes to offset %lu\n",
                //            klen, io_size - sizeof(uint8_t) - klen, 
                //            offset);
                memcpy(nvmev_vdev->ns[nsid].mapped + offset, 
                       vaddr + mem_offs, io_size - sizeof(uint8_t) - klen);
                io_size = io_size - sizeof(uint8_t) - klen;
            } else {
                memcpy(nvmev_vdev->ns[nsid].mapped + offset, 
                       vaddr + mem_offs, io_size);
                //NVMEV_INFO("Copied %lu bytes to offset %lu\n",
                //            io_size, offset);
            }
        }

		kunmap_atomic(vaddr);

		remaining -= io_size;
		offset += io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);

    uint8_t *ptr;
    uint8_t klen;

    if(read) {
        //ptr = nvmev_vdev->ns[nsid].mapped + cmd->kv_retrieve.rsvd;

        //klen = *(uint8_t*) ptr;
        //uint8_t *kPtr = (uint8_t*) nvmev_vdev->ns[nsid].mapped + cmd->kv_retrieve.rsvd + 1;

        //if(klen > 16) {
        //    NVMEV_ERROR("WTF!!!! klen %u\n", klen);
        //}

        //char k[16];
        //memcpy(k, kPtr, klen);
        //k[klen] = '\0';

        //if(k[0] == 'L') {
        //    uint64_t ts = *(uint64_t*) (((uint8_t*) vaddr) + real_vlen - 8);

        //    if(ts > 2009533252106612185) {
        //        NVMEV_INFO("Log key read returns length %u. Bid %llu log num %u timestamp %llu\n", 
        //                real_vlen,
        //                *(uint64_t*) (k + 4), 
        //                *(uint16_t*) (k + 4 + sizeof(uint64_t)), ts);
        //    }
        //} else {
        //    //NVMEV_INFO("Returning length %u in io_kv\n", real_vlen);
        //}
        return real_vlen;
    } else if(write || orig_len == 0) {
        ptr = (void*) cmd->kv_store.rsvd;
        klen = *(uint8_t*) ptr;

        //char v2[8];
        //memcpy(v2, nvmev_vdev->ns[nsid].mapped + cmd->kv_store.rsvd + length - 16, 8);
        //NVMEV_INFO("Last key before move %s\n", v2);
        memmove(ptr + sizeof(uint8_t) + klen + sizeof(uint32_t),
                ptr + sizeof(uint8_t) + klen, length - sizeof(uint8_t) - klen);
        //memcpy(v2, 
        //nvmev_vdev->ns[nsid].mapped + cmd->kv_store.rsvd + length + sizeof(uint32_t) - 16, 8);
        //NVMEV_INFO("Moved %lu bytes from offset %llu to offset %llu last key %s\n",
        //            length - sizeof(uint8_t) - klen, 
        //            cmd->kv_store.rsvd + sizeof(uint8_t) + klen,
        //            cmd->kv_store.rsvd + sizeof(uint8_t) + klen + sizeof(uint32_t),
        //            v2);
        memcpy(ptr + sizeof(uint8_t) + klen, &length, sizeof(uint32_t));
        //memcpy(v2, 
        //nvmev_vdev->ns[nsid].mapped + cmd->kv_store.rsvd + length + sizeof(uint32_t) - 16, 8);
        //NVMEV_INFO("Wrote a value length of %u to offset %llu last key %s\n",
        //            (uint32_t) length, cmd->kv_store.rsvd + 
        //            sizeof(uint8_t) + klen, v2);
        return 0;
    } else if(append) {
        ptr = nvmev_vdev->ns[nsid].mapped + cmd->kv_append.rsvd;
        klen = *(uint8_t*) ptr;
        /*
         * We are appending to a previously existing KV pair (orig_len > 0).
         * Update the length of the KV pair to the result of the append +
         * original KV pair length.
         */
        length += orig_len - sizeof(uint32_t) - sizeof(uint8_t) - klen;
        memcpy(nvmev_vdev->ns[nsid].mapped + cmd->kv_store.rsvd + 
               sizeof(uint8_t) + klen,
               &length, sizeof(uint32_t));
        //NVMEV_INFO("Append result : %s\n", 
        //            (char*) nvmev_vdev->ns[nsid].mapped + cmd->kv_store.rsvd + 13);
        return 0;
    } else {
        NVMEV_ASSERT(false);
    }

    return 0;
}
#endif

static unsigned int __do_perform_io(int sqid, int sq_entry)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
    struct nvme_rw_command *cmd = &sq_entry(sq_entry).rw;
	size_t offset;
	size_t length, remaining;
	int prp_offs = 0;
	int prp2_offs = 0;
	u64 paddr;
	u64 *paddr_list = NULL;
	size_t nsid = cmd->nsid - 1;  // 0-based

    offset = cmd->slba << 9;
	length = (cmd->length + 1) << 9;
	remaining = length;

	while (remaining) {
		size_t io_size;
		void *vaddr;
		size_t mem_offs = 0;

		prp_offs++;
		if (prp_offs == 1) {
			paddr = cmd->prp1;
		} else if (prp_offs == 2) {
			paddr = cmd->prp2;
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

		if (cmd->opcode == nvme_cmd_write ||
		    cmd->opcode == nvme_cmd_zone_append) {
			memcpy(nvmev_vdev->ns[nsid].mapped + offset, vaddr + mem_offs, io_size);
		} else if (cmd->opcode == nvme_cmd_read) {
			memcpy(vaddr + mem_offs, nvmev_vdev->ns[nsid].mapped + offset, io_size);
		}

		kunmap_atomic(vaddr);

		remaining -= io_size;
		offset += io_size;
	}

	if (paddr_list != NULL)
		kunmap_atomic(paddr_list);

	return length;
}

static u64 paddr_list[513] = {
	0,
}; // Not using index 0 to make max index == num_prp
static unsigned int __do_perform_io_using_dma(int sqid, int sq_entry)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	struct nvme_rw_command *cmd = &sq_entry(sq_entry).rw;
	size_t offset;
	size_t length, remaining;
	int prp_offs = 0;
	int prp2_offs = 0;
	int num_prps = 0;
	u64 paddr;
	u64 *tmp_paddr_list = NULL;
	size_t io_size;
	size_t mem_offs = 0;

	offset = cmd->slba << 9;
	length = (cmd->length + 1) << 9;
	remaining = length;

	memset(paddr_list, 0, sizeof(paddr_list));
	/* Loop to get the PRP list */
	while (remaining) {
		io_size = 0;

		prp_offs++;
		if (prp_offs == 1) {
			paddr_list[prp_offs] = cmd->prp1;
		} else if (prp_offs == 2) {
			paddr_list[prp_offs] = cmd->prp2;
			if (remaining > PAGE_SIZE) {
				tmp_paddr_list = kmap_atomic_pfn(PRP_PFN(paddr_list[prp_offs])) +
						 (paddr_list[prp_offs] & PAGE_OFFSET_MASK);
				paddr_list[prp_offs] = tmp_paddr_list[prp2_offs++];
			}
		} else {
			paddr_list[prp_offs] = tmp_paddr_list[prp2_offs++];
		}

		io_size = min_t(size_t, remaining, PAGE_SIZE);

		if (paddr_list[prp_offs] & PAGE_OFFSET_MASK) {
			mem_offs = paddr_list[prp_offs] & PAGE_OFFSET_MASK;
			if (io_size + mem_offs > PAGE_SIZE)
				io_size = PAGE_SIZE - mem_offs;
		}

		remaining -= io_size;
	}
	num_prps = prp_offs;

	if (tmp_paddr_list != NULL)
		kunmap_atomic(tmp_paddr_list);

	remaining = length;
	prp_offs = 1;

	/* Loop for data transfer */
	while (remaining) {
		size_t page_size;
		mem_offs = 0;
		io_size = 0;
		page_size = 0;

		paddr = paddr_list[prp_offs];
		page_size = min_t(size_t, remaining, PAGE_SIZE);

		/* For non-page aligned paddr, it will never be between continuous PRP list (Always first paddr)  */
		if (paddr & PAGE_OFFSET_MASK) {
			mem_offs = paddr & PAGE_OFFSET_MASK;
			if (page_size + mem_offs > PAGE_SIZE) {
				page_size = PAGE_SIZE - mem_offs;
			}
		}

		for (prp_offs++; prp_offs <= num_prps; prp_offs++) {
			if (paddr_list[prp_offs] == paddr_list[prp_offs - 1] + PAGE_SIZE)
				page_size += PAGE_SIZE;
			else
				break;
		}

		io_size = min_t(size_t, remaining, page_size);

		if (cmd->opcode == nvme_cmd_write ||
		    cmd->opcode == nvme_cmd_zone_append) {
			ioat_dma_submit(paddr, nvmev_vdev->config.storage_start + offset, io_size);
		} else if (cmd->opcode == nvme_cmd_read) {
			ioat_dma_submit(nvmev_vdev->config.storage_start + offset, paddr, io_size);
		}

		remaining -= io_size;
		offset += io_size;
	}

	return length;
}

static void __insert_req_sorted(unsigned int entry, struct nvmev_io_worker *worker,
				                unsigned long nsecs_target)
{
	/**
	 * Requests are placed in @work_queue sorted by their target time.
	 * @work_queue is statically allocated and the ordered list is
	 * implemented by chaining the indexes of entries with @prev and @next.
	 * This implementation is nasty but we do this way over dynamically
	 * allocated linked list to minimize the influence of dynamic memory allocation.
	 * Also, this O(n) implementation can be improved to O(logn) scheme with
	 * e.g., red-black tree but....
	 */
	if (worker->io_seq == -1) {
		worker->io_seq = entry;
		worker->io_seq_end = entry;
	} else {
		unsigned int curr = worker->io_seq_end;

		while (curr != -1) {
			if (worker->work_queue[curr].nsecs_target <= worker->latest_nsecs)
				break;

			if (worker->work_queue[curr].nsecs_target <= nsecs_target)
				break;

			curr = worker->work_queue[curr].prev;
		}

		if (curr == -1) { /* Head inserted */
			worker->work_queue[worker->io_seq].prev = entry;
			worker->work_queue[entry].next = worker->io_seq;
			worker->io_seq = entry;
		} else if (worker->work_queue[curr].next == -1) { /* Tail */
			worker->work_queue[entry].prev = curr;
			worker->io_seq_end = entry;
			worker->work_queue[curr].next = entry;
		} else { /* In between */
			worker->work_queue[entry].prev = curr;
			worker->work_queue[entry].next = worker->work_queue[curr].next;

			worker->work_queue[worker->work_queue[entry].next].prev = entry;
			worker->work_queue[curr].next = entry;
		}
	}
}

static struct nvmev_io_worker *__allocate_work_queue_entry(int sqid, unsigned int *entry)
{
	unsigned int io_worker_turn = __get_io_worker(sqid);
	struct nvmev_io_worker *worker = &nvmev_vdev->io_workers[io_worker_turn];
	unsigned int e = worker->free_seq;
	struct nvmev_io_work *w = worker->work_queue + e;

	if (w->next >= NR_MAX_PARALLEL_IO) {
		WARN_ON_ONCE("IO queue is almost full");
		return NULL;
	}

	if (++io_worker_turn == nvmev_vdev->config.nr_io_workers)
		io_worker_turn = 0;
	nvmev_vdev->io_worker_turn = io_worker_turn;

	worker->free_seq = w->next;
	BUG_ON(worker->free_seq >= NR_MAX_PARALLEL_IO);
	*entry = e;

	return worker;
}

static struct nvmev_io_work* __enqueue_io_req(int sqid, int cqid, int sq_entry, 
                                              unsigned long long nsecs_start,
                                              struct nvmev_result *ret)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	struct nvmev_io_worker *worker;
	struct nvmev_io_work *w;
	unsigned int entry;

	worker = __allocate_work_queue_entry(sqid, &entry);
	if (!worker)
		return NULL;

	w = worker->work_queue + entry;

	NVMEV_DEBUG_VERBOSE("%s/%u[%d], sq %d cq %d, entry %d, %llu + %llu\n", worker->thread_name, entry,
		    sq_entry(sq_entry).rw.opcode, sqid, cqid, sq_entry, nsecs_start,
		    ret->nsecs_target - nsecs_start);

	/////////////////////////////////
	w->sqid = sqid;
	w->cqid = cqid;
	w->sq_entry = sq_entry;
	w->command_id = sq_entry(sq_entry).common.command_id;
	w->nsecs_start = nsecs_start;
	w->nsecs_enqueue = local_clock();
	w->nsecs_target = ret->nsecs_target;
	w->status = ret->status;
	w->is_completed = false;
	w->is_copied = false;
	w->prev = -1;
	w->next = -1;
    w->cb = ret->cb;
    w->args = ret->args;

	w->is_internal = false;
	mb(); /* IO worker shall see the updated w at once */

	__insert_req_sorted(entry, worker, ret->nsecs_target);
    return w;
}

void schedule_internal_operation(int sqid, unsigned long long nsecs_target,
				 struct buffer *write_buffer, size_t buffs_to_release)
{
	struct nvmev_io_worker *worker;
	struct nvmev_io_work *w;
	unsigned int entry;

	if(sqid == INT_MAX) {
		uint16_t sqid_r;
		get_random_bytes(&sqid_r, sizeof(sqid_r));
		sqid = sqid_r % nvmev_vdev->config.nr_io_workers;
	}

	worker = __allocate_work_queue_entry(sqid, &entry);
	if (!worker)
		return;

	w = worker->work_queue + entry;

	NVMEV_DEBUG_VERBOSE("%s/%u, internal sq %d, %llu + %llu\n", worker->thread_name, entry, sqid,
		    local_clock(), nsecs_target - local_clock());

	/////////////////////////////////
	w->sqid = sqid;
	w->nsecs_start = w->nsecs_enqueue = local_clock();
	w->nsecs_target = nsecs_target;
	w->is_completed = false;
	w->is_copied = true;
	w->prev = -1;
	w->next = -1;

	w->is_internal = true;
	w->write_buffer = write_buffer;
	w->buffs_to_release = buffs_to_release;
	mb(); /* IO worker shall see the updated w at once */

	__insert_req_sorted(entry, worker, nsecs_target);
}

void schedule_internal_operation_cb(int sqid, unsigned long long nsecs_start,
                                    void* mem, uint64_t ppa, uint64_t len,
                                    uint64_t (*cb)(void*, uint64_t*, uint64_t*), 
                                    void *args, bool read,
                                    struct nvmev_io_work *cb_w)
{
	struct nvmev_io_worker *worker;
	struct nvmev_io_work *w;
	unsigned int entry;

    if(sqid == INT_MAX) {
        uint16_t sqid_r;
        get_random_bytes(&sqid_r, sizeof(sqid_r));
        sqid = sqid_r % nvmev_vdev->config.nr_io_workers;
    }

	worker = __allocate_work_queue_entry(sqid, &entry);
    BUG_ON(!worker);
	if (!worker)
		return;

	w = worker->work_queue + entry;

	NVMEV_DEBUG_VERBOSE("%s/%u, internal sq %d, %llu + %llu\n", worker->thread_name, entry, sqid,
		    local_clock(), nsecs_start - local_clock());

	/////////////////////////////////
	w->sqid = sqid;
	w->nsecs_start = w->nsecs_enqueue = nsecs_start;
	w->nsecs_target = ppa;
	w->is_completed = false;
	w->is_copied = false;
	w->prev = -1;
	w->next = -1;
    w->cb = cb;
    w->args = args;
    w->cb_w = cb_w;

    w->read = read;
    w->mem = mem;
    w->ppa = ppa;
    w->len = len;

	w->is_internal = true;
	w->write_buffer = NULL;
	w->buffs_to_release = 0;
	mb(); /* IO worker shall see the updated w at once */

	__insert_req_sorted(entry, worker, w->nsecs_target);
}

static void __reclaim_completed_reqs(void)
{
	unsigned int turn;

	for (turn = 0; turn < nvmev_vdev->config.nr_io_workers; turn++) {
		struct nvmev_io_worker *worker;
		struct nvmev_io_work *w;

		unsigned int first_entry = -1;
		unsigned int last_entry = -1;
		unsigned int curr;
		int nr_reclaimed = 0;

		worker = &nvmev_vdev->io_workers[turn];

		first_entry = worker->io_seq;
		curr = first_entry;

		while (curr != -1) {
			w = &worker->work_queue[curr];
			if (w->is_completed == true && w->is_copied == true &&
			    w->nsecs_target <= worker->latest_nsecs) {
				last_entry = curr;
				curr = w->next;
				nr_reclaimed++;
			} else {
				break;
			}
		}

		if (last_entry != -1) {
			w = &worker->work_queue[last_entry];
			worker->io_seq = w->next;
			if (w->next != -1) {
				worker->work_queue[w->next].prev = -1;
			}
			w->next = -1;

			w = &worker->work_queue[first_entry];
			w->prev = worker->free_seq_end;

			w = &worker->work_queue[worker->free_seq_end];
			w->next = first_entry;

			worker->free_seq_end = last_entry;
			NVMEV_DEBUG_VERBOSE("%s: %u -- %u, %d\n", __func__,
					first_entry, last_entry, nr_reclaimed);
		}
	}
}

static size_t __nvmev_proc_io(int sqid, int sq_entry, size_t *io_size)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	unsigned long long nsecs_start = __get_wallclock();
	struct nvme_command *cmd = &sq_entry(sq_entry);
#if (BASE_SSD == KV_PROTOTYPE) || (BASE_SSD == SAMSUNG_970PRO_HASH_DFTL)
	uint32_t nsid = 0; // Some KVSSD programs give 0 as nsid for KV IO
#else
	uint32_t nsid = cmd->common.nsid - 1;
#endif
	struct nvmev_ns *ns = &nvmev_vdev->ns[nsid];

	struct nvmev_request req = {
		.cmd = cmd,
		.sq_id = sqid,
		.nsecs_start = nsecs_start,
	};
	struct nvmev_result ret = {
//#if (BASE_SSD != SAMSUNG_970PRO_HASH_DFTL)
        .nsecs_target = nsecs_start,
//#else
//        .nsecs_target = U64_MAX,
//#endif
		.status = NVME_SC_SUCCESS,
        .cb = NULL,
        .args = NULL,
	};

#ifdef PERF_DEBUG
	unsigned long long prev_clock = local_clock();
	unsigned long long prev_clock2 = 0;
	unsigned long long prev_clock3 = 0;
	unsigned long long prev_clock4 = 0;
	static unsigned long long clock1 = 0;
	static unsigned long long clock2 = 0;
	static unsigned long long clock3 = 0;
	static unsigned long long counter = 0;
#endif

//#if (BASE_SSD == SAMSUNG_970PRO_HASH_DFTL)
//    /*
//     * In the DFTLKV FTL, key-value work (store, retrieve, etc)
//     * is done in the worker threads. When these operations complete,
//     * we need to switch the target time of the work entry to the
//     * finish time of the key-value operation. To do that, we need
//     * the nvmev_io_work item first.
//     *
//     * Note that it doesn't matter that we insert and get the work
//     * here before proc_io_cmd below, because we have already
//     * set the target time of the work to U64_MAX above.
//     */
//
//    bool kv_cmd = false;
//    if(ns->identify_io_cmd(ns, sq_entry(sq_entry))) {
//        //struct nvme_command *b_cmd = &sq_entry(sq_entry);
//        //struct nvme_kv_command *cmd = (struct nvme_kv_command*) b_cmd;
//
//        //if(cmd->common.opcode == nvme_cmd_kv_store) {
//        //    uint32_t vlen = cmd->kv_store.value_len << 2;
//        //    cmd->kv_store.rsvd = wb_offs;
//        //    wb_offs += vlen;
//        //}
//
//        struct nvmev_io_work *w = __enqueue_io_req(sqid, sq->cqid, sq_entry, 
//                nsecs_start, &ret);
//        req.w = w;
//        kv_cmd = true;
//    }
//#endif

	if (!ns->proc_io_cmd(ns, &req, &ret))
		return false;
	*io_size = (cmd->rw.length + 1) << 9;

#ifdef PERF_DEBUG
	prev_clock2 = local_clock();
#endif

//#if (BASE_SSD != SAMSUNG_970PRO_HASH_DFTL)
    __enqueue_io_req(sqid, sq->cqid, sq_entry, nsecs_start, &ret);
//#else
//    if(!kv_cmd) {
//        __enqueue_io_req(sqid, sq->cqid, sq_entry, nsecs_start, &ret);
//    }
//#endif

#ifdef PERF_DEBUG
	prev_clock3 = local_clock();
#endif

	__reclaim_completed_reqs();

#ifdef PERF_DEBUG
	prev_clock4 = local_clock();

	clock1 += (prev_clock2 - prev_clock);
	clock2 += (prev_clock3 - prev_clock2);
	clock3 += (prev_clock4 - prev_clock3);
	counter++;

	if (counter > 1000) {
		NVMEV_DEBUG("LAT: %llu, ENQ: %llu, CLN: %llu\n", clock1 / counter, clock2 / counter,
			    clock3 / counter);
		clock1 = 0;
		clock2 = 0;
		clock3 = 0;
		counter = 0;
	}
#endif
	return true;
}

int nvmev_proc_io_sq(int sqid, int new_db, int old_db)
{
	struct nvmev_submission_queue *sq = nvmev_vdev->sqes[sqid];
	int num_proc = new_db - old_db;
	int seq;
	int sq_entry = old_db;
	int latest_db;

	if (unlikely(!sq))
		return old_db;
	if (unlikely(num_proc < 0))
		num_proc += sq->queue_size;

	for (seq = 0; seq < num_proc; seq++) {
		size_t io_size;
		if (!__nvmev_proc_io(sqid, sq_entry, &io_size))
			break;

		if (++sq_entry == sq->queue_size) {
			sq_entry = 0;
		}
		sq->stat.nr_dispatched++;
		sq->stat.nr_in_flight++;
		sq->stat.total_io += io_size;
	}
	sq->stat.nr_dispatch++;
	sq->stat.max_nr_in_flight = max_t(int, sq->stat.max_nr_in_flight, sq->stat.nr_in_flight);

	latest_db = (old_db + seq) % sq->queue_size;
	return latest_db;
}

void nvmev_proc_io_cq(int cqid, int new_db, int old_db)
{
	struct nvmev_completion_queue *cq = nvmev_vdev->cqes[cqid];
	int i;
	for (i = old_db; i != new_db; i++) {
		int sqid = cq_entry(i).sq_id;
		if (i >= cq->queue_size) {
			i = -1;
			continue;
		}

		/* Should check the validity here since SPDK deletes SQ immediately
		 * before processing associated CQes */
		if (!nvmev_vdev->sqes[sqid]) continue;

		nvmev_vdev->sqes[sqid]->stat.nr_in_flight--;
	}

	cq->cq_tail = new_db - 1;
	if (new_db == -1)
		cq->cq_tail = cq->queue_size - 1;
}

static void __fill_cq_result(struct nvmev_io_work *w)
{
	int sqid = w->sqid;
	int cqid = w->cqid;
	int sq_entry = w->sq_entry;
	unsigned int command_id = w->command_id;
	unsigned int status = w->status;
	unsigned int result0 = w->result0;
	unsigned int result1 = w->result1;

	struct nvmev_completion_queue *cq = nvmev_vdev->cqes[cqid];
	int cq_head = cq->cq_head;
	struct nvme_completion *cqe = &cq_entry(cq_head);

	spin_lock(&cq->entry_lock);
	cqe->command_id = command_id;
	cqe->sq_id = sqid;
	cqe->sq_head = sq_entry;
	cqe->status = cq->phase | (status << 1);
	cqe->result0 = result0;
	cqe->result1 = result1;

	if (++cq_head == cq->queue_size) {
		cq_head = 0;
		cq->phase = !cq->phase;
	}

	cq->cq_head = cq_head;
	cq->interrupt_ready = true;
	spin_unlock(&cq->entry_lock);
}

static int nvmev_io_worker(void *data)
{
	struct nvmev_io_worker *worker = (struct nvmev_io_worker *)data;
	struct nvmev_ns *ns;

#ifdef PERF_DEBUG
	static unsigned long long intr_clock[NR_MAX_IO_QUEUE + 1];
	static unsigned long long intr_counter[NR_MAX_IO_QUEUE + 1];

	unsigned long long prev_clock;
#endif

	NVMEV_INFO("%s started on cpu %d (node %d)\n", worker->thread_name, smp_processor_id(),
		   cpu_to_node(smp_processor_id()));

	while (!kthread_should_stop()) {
		unsigned long long curr_nsecs_wall = __get_wallclock();
		unsigned long long curr_nsecs_local = local_clock();
		long long delta = curr_nsecs_wall - curr_nsecs_local;

		volatile unsigned int curr = worker->io_seq;
		int qidx;

		while (curr != -1) {
			struct nvmev_io_work *w = &worker->work_queue[curr];
			unsigned long long curr_nsecs = local_clock() + delta;
			worker->latest_nsecs = curr_nsecs;

			if (w->is_completed == true) {
				curr = w->next;
				continue;
			}

			if (w->is_copied == false) {
                NVMEV_DEBUG_VERBOSE("%s: picked up %u, %d %d %d\n", worker->thread_name, curr,
                            w->sqid, w->cqid, w->sq_entry);

#ifdef PERF_DEBUG
				w->nsecs_copy_start = local_clock() + delta;
#endif
				if (w->is_internal) {
#if (BASE_SSD == SAMSUNG_970PRO_HASH_DFTL)
                    if(w->cb) {
                        NVMEV_ASSERT(w->args);

                        uint64_t target = U64_MAX;
                        uint64_t result = U64_MAX;
                        uint64_t status = U64_MAX;
                        target = w->cb(w->args, &result, &status);

                        if(w->cb_w) {
                            w->cb_w->result0 = w->cb_w->result1 = result;
                            w->cb_w->status = status;
                            w->cb_w->nsecs_target = target;
                        }
                    }
#endif
				} else if (io_using_dma) {
					__do_perform_io_using_dma(w->sqid, w->sq_entry);
				} else {
#if (BASE_SSD == KV_PROTOTYPE)
					struct nvmev_submission_queue *sq =
						nvmev_vdev->sqes[w->sqid];
					ns = &nvmev_vdev->ns[0];
					if (ns->identify_io_cmd(ns, sq_entry(w->sq_entry))) {
						w->result0 = ns->perform_io_cmd(
							ns, &sq_entry(w->sq_entry), &(w->status));
					} else {
						__do_perform_io(w->sqid, w->sq_entry);
					}
#endif
#if (BASE_SSD == SAMSUNG_970PRO_HASH_DFTL)
					struct nvmev_submission_queue *sq =
						nvmev_vdev->sqes[w->sqid];
					ns = &nvmev_vdev->ns[0];
                    if (ns->identify_io_cmd(ns, sq_entry(w->sq_entry))) {
                        w->result0 = __do_perform_io_kv(w->sqid, w->sq_entry);
                        if(w->cb) {
                            w->cb(w->args, 0, 0);
                        }
                    } else {
                        __do_perform_io(w->sqid, w->sq_entry);
                    }
#else
					__do_perform_io(w->sqid, w->sq_entry);
#endif
				}

#ifdef PERF_DEBUG
				w->nsecs_copy_done = local_clock() + delta;
#endif
				w->is_copied = true;

				NVMEV_DEBUG_VERBOSE("%s: copied %u, %d %d %d\n", worker->thread_name, curr,
					    w->sqid, w->cqid, w->sq_entry);
			}

			if (w->nsecs_target <= curr_nsecs) {
				if (w->is_internal) {
#if (SUPPORTED_SSD_TYPE(CONV) || SUPPORTED_SSD_TYPE(ZNS))
                    if(w->write_buffer) {
                        buffer_release((struct buffer *)w->write_buffer,
                                w->buffs_to_release);
                    }
#endif
				} else {
                    __fill_cq_result(w);
				}

				NVMEV_DEBUG_VERBOSE("%s: completed %u, %d %d %d\n", worker->thread_name, curr,
					    w->sqid, w->cqid, w->sq_entry);

#ifdef PERF_DEBUG
				w->nsecs_cq_filled = local_clock() + delta;
				trace_printk("%llu %llu %llu %llu %llu %llu\n", w->nsecs_start,
					     w->nsecs_enqueue - w->nsecs_start,
					     w->nsecs_copy_start - w->nsecs_start,
					     w->nsecs_copy_done - w->nsecs_start,
					     w->nsecs_cq_filled - w->nsecs_start,
					     w->nsecs_target - w->nsecs_start);
#endif
				mb(); /* Reclaimer shall see after here */
				w->is_completed = true;
			}

			curr = w->next;
		}

		for (qidx = 1; qidx <= nvmev_vdev->nr_cq; qidx++) {
			struct nvmev_completion_queue *cq = nvmev_vdev->cqes[qidx];

#ifdef CONFIG_NVMEV_IO_WORKER_BY_SQ
			if ((worker->id) != __get_io_worker(qidx))
				continue;
#endif
			if (cq == NULL || !cq->irq_enabled)
				continue;

			if (spin_trylock(&cq->irq_lock)) {
				if (cq->interrupt_ready == true) {
#ifdef PERF_DEBUG
					prev_clock = local_clock();
#endif
					cq->interrupt_ready = false;
					nvmev_signal_irq(cq->irq_vector);

#ifdef PERF_DEBUG
					intr_clock[qidx] += (local_clock() - prev_clock);
					intr_counter[qidx]++;

					if (intr_counter[qidx] > 1000) {
						NVMEV_DEBUG("Intr %d: %llu\n", qidx,
							    intr_clock[qidx] / intr_counter[qidx]);
						intr_clock[qidx] = 0;
						intr_counter[qidx] = 0;
					}
#endif
				}
				spin_unlock(&cq->irq_lock);
			}
		}
		cond_resched();
	}

	return 0;
}

void NVMEV_IO_WORKER_INIT(struct nvmev_dev *nvmev_vdev)
{
	unsigned int i, worker_id;

	nvmev_vdev->io_workers =
		kcalloc(sizeof(struct nvmev_io_worker), nvmev_vdev->config.nr_io_workers, GFP_KERNEL);
	nvmev_vdev->io_worker_turn = 0;

	for (worker_id = 0; worker_id < nvmev_vdev->config.nr_io_workers; worker_id++) {
		struct nvmev_io_worker *worker = &nvmev_vdev->io_workers[worker_id];

		worker->work_queue =
			kzalloc(sizeof(struct nvmev_io_work) * NR_MAX_PARALLEL_IO, GFP_KERNEL);
		for (i = 0; i < NR_MAX_PARALLEL_IO; i++) {
			worker->work_queue[i].next = i + 1;
			worker->work_queue[i].prev = i - 1;
		}
		worker->work_queue[NR_MAX_PARALLEL_IO - 1].next = -1;
		worker->id = worker_id;
		worker->free_seq = 0;
		worker->free_seq_end = NR_MAX_PARALLEL_IO - 1;
		worker->io_seq = -1;
		worker->io_seq_end = -1;

		snprintf(worker->thread_name, sizeof(worker->thread_name), "nvmev_io_worker_%d", worker_id);

		worker->task_struct = kthread_create(nvmev_io_worker, worker, "%s", worker->thread_name);

		kthread_bind(worker->task_struct, nvmev_vdev->config.cpu_nr_io_workers[worker_id]);
		wake_up_process(worker->task_struct);
	}
}

void NVMEV_IO_WORKER_FINAL(struct nvmev_dev *nvmev_vdev)
{
	unsigned int i;

	for (i = 0; i < nvmev_vdev->config.nr_io_workers; i++) {
		struct nvmev_io_worker *worker = &nvmev_vdev->io_workers[i];

		if (!IS_ERR_OR_NULL(worker->task_struct)) {
			kthread_stop(worker->task_struct);
		}

		kfree(worker->work_queue);
	}

	kfree(nvmev_vdev->io_workers);
}
