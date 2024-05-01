# A Hash-Based Key-Value SSD FTL With Efficient Small-Value Support

Welcome to the repository for the work described in the above paper. This work is not a part of the
official NVMeVirt repository, located here: https://github.com/snu-csl/NVMeVirt.

Included in this repository is the following:

- The *Original* FTL, based on the good work at https://github.com/dgist-datalab/PinK/.
- The *Plus* FTL.
- The YCSB code used in the paper.
- Installation Instructions

## Prerequisites

A kernel that is both supported by the KVSSD drivers and NVMeVirt is required. As of now, that's v5.10.37. SPDK works with disks exposed by NVMeVirt, and Samsung originally provided an SPDK driver for their KVSSDs here: https://github.com/OpenMPDK/KVSSD. It may be possible use NVMeVirt with a KVSSD FTL that way, but it hasn't been tested yet. 

## Installation

The build.sh script will install libgflags (necessary for YCSB), a concurrent queue library we use for KVSSD async I/O (see [Acknowledgements](#ack)), and build YCSB, the KVSSD driver, and NVMeVirt with either Original or Plus.  The *drivers* folder is taken from a modified version of Samsung's official KVSSD repository, located here: https://github.com/snu-csl/KVSSD_5.10.37.

```
./build.sh rel # Release
./build.sh debug # Debug (-O0 for YCSB, nothing right now for virt.)
```

Switching between Original and Plus is a compile time define, they are both implemented
in *demand\_ftl.c*, but the define is in *ssd_config.h*. Delete the line or undefine
it to use Plus.

```
# ssd_config.h
#define ORIGINAL # to use Original
#undef ORIGINAL  # to use Plus
```

First, insert the KVSSD kernel module:

```
rmmod nvme nvme_core # if you already have the NVMe module loaded
insmod drivers/kernel_v5.10.37/nvme-core.ko
insmod drivers/kernel_v5.10.37/nvme.ko
```

Next, insert the NVMeVirt the kernel module with the a command similar to the following:

`insmod nvmev.ko memmap_start=32G memmap_size=124G cpus=35,36 gccpu=37 evictcpu=38 cache_dram_mb=8`

memmap\_start and memmap\_size refer to an area of memory reserved at boot time.
You can reserve memory by adding the following to /etc/default/grub

`memmap=128G\\\$600G # Reserve 128GB from 600GB`

A good guide to figuring out which memory you can reserve is here: https://pmem.io/blog/2016/02/how-to-emulate-persistent-memory/. The current versions of Original and Plus don't actually use this memory right now (they use the kernel allocator), but reserving is still required.

The parameters cpus, gccpu, and evictcpu refer to cores on which to pin the as-named threads.
cpus=35,36 means NVMeVirt's dispatcher thread will run on CPU 35, and the IO worker thread
will run on CPU 36. You can specify multiple IO worker threads with cpus=35,36,37...

The design assumes one background GC and one eviction thread for now.

After you run the insmod command above, you should see a new NVMe SSD in your system

```
sudo nvme list
Node             SN                   Model                                    Namespace Usage                      Format           FW Rev
---------------- -------------------- ---------------------------------------- --------- -------------------------- ---------------- --------
/dev/nvme0n1     CSL_Virt_SN_01       CSL_Virt_MN_01                           1          124  GB /  124  GB    512   B +  0 B   CSL_002
```

If not, check dmesg for errors. You may run into an error that has been previously reported
in the official NVMeVirt repository at https://github.com/snu-csl/NVMeVirt, so check the
issues there too.

## Changing Parameters

In ssd\_config.h you can modify both the timing parameters for things like flash accesses in NVMeVirt, and parameters like the grain size
for the KVSSD FTLs.

```
#define SSD_PARTITIONS (1)
#define NAND_CHANNELS (8)
#define LUNS_PER_NAND_CH (8)
#define PLNS_PER_LUN (1)
#define FLASH_PAGE_SIZE KB(32)
#define ONESHOT_PAGE_SIZE (FLASH_PAGE_SIZE * 1)
#define BLKS_PER_PLN (0)
#define BLK_SIZE KB(128) /*BLKS_PER_PLN should not be 0 */
static_assert((ONESHOT_PAGE_SIZE % FLASH_PAGE_SIZE) == 0);

#define PIECE 512 # 512B grain size
```

## YCSB

Running a YCSB benchmark can be done with the following command:

`./ycsb/build/ycsb_kvssd --num_pairs=100000000 --vlen=1000 --threads=20 --num_ops=500000 --duration=0 --cache_size_mb=0 --pop=true --benchmarks=a --store_name=KVSSD --uniform=true --warmup_time=0`

*pop* refers to population. To run YCSB B next, change *pop* to false and
*--benchmarks=a* to *--benchmarks=b* .
A log file will be created in the directory from which the command is
run. There are other options available, see the top of main.cc in the  ycsb
folder for details (although not all of them
are tested!). You can run multiple benchmarks with
*--benchmarks=a,b,c,d,f*, but it is less
convenient as there won't be a separate log file for each benchmark as of now.

## Code Notes

How FTLs are written in NVMeVirt can be confusing at first. The general theory is this; for writes/stores, IO workers will copy to an address in memory that we give them, as a result of the mapping logic in the __store function. We *won't* perform any copies in the foreground. The foreground dispatcher performs mapping logic only. If we overwrite a KV pair, the IO worker will copy to the same address as before unless the pair size changes. This isn't how flash works in real life, but the underlying flash model ultimately determines how long the store will take.

In GC, we only update mapping information, and don't actually copy any KV data to new locations.
This reduces the work the foreground dispatcher and background garbage collector have to perform dramatically. 

This doesn't result in "free" or unrealistically fast IO; the flash timings we generate from the dispatcher are ultimately what decides when the completion event gets placed onto the disk's completion queue.

Originally, hash indexes were called LPAs (logical page addresses). If you see LPA in the code, assume hash index. Likewise, hash table sections (ht_section) were called CMT.

## License

NVMeVirt is offered under the terms of the GNU General Public License version 2 as published by the Free Software Foundation. More information about this license can be found [here](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html).

Priority queue implementation [`pqueue/`](pqueue/) is offered under the terms of the BSD 2-clause license (GPL-compatible). (Copyright (c) 2014, Volkan Yazıcı <volkan.yazici@gmail.com>. All rights reserved.)

<a id="ack"></a>
## Acknowledgements

We thank the author of https://github.com/cameron314/concurrentqueue and https://github.com/atbarker/cityhash-kernel, whose work we use throughout.
