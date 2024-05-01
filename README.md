# A Hash-Based Key-Value SSD FTL With Efficient Small-Value Support

Welcome to the repository for the work described in the above paper. This work is not a part of the
official NVMeVirt repository, located here: https://github.com/snu-csl/NVMeVirt.

Included in this repository is the following:

- The *Original* FTL, based on the good work at from https://github.com/dgist-datalab/PinK/.
- The *Plus* FTL.
- The YCSB code used in the paper.
- [Installation Instructions](#installation)

<a id="installation"></a>
## Installation

The build.sh script will build both YCSB and NVMeVirt with either Original or Plus.
Invoke it as follows:

```
./build.sh rel # Release
./build.sh debug # Debug (-O0 for YCSB, nothing right now for virt.)
```

Switching between Original and Plus is a compile time define, they are both implemented
in *demand\_ftl.c*. Delete the line or undefine
it to use Plus.

```
#define ORIGINAL # in ssd_config.h
#undef ORIGINAL # to use Plus
```

After building NVMeVirt, you can insert the kernel module with the a command similar to the following:

`insmod /home/username/virtkv/nvmev.ko memmap_start=32G memmap_size=124G cpus=35,36 gccpu=37 evictcpu=38 cache_dram_mb=8`

memmap\_start and memmap\_size refer to an area of memory reserved at boot time.
You can reserve memory by adding the following to /etc/default/grub

`memmap=128G\\\$600G # Reserve 128GB from 600GB`

A good guide to figuring out which memory you can reserve is here: https://pmem.io/blog/2016/02/how-to-emulate-persistent-memory/

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

`./build/ycsb_kvssd --num_pairs=100000000 --vlen=1000 --threads=20 --num_ops=500000 --duration=0 --cache_size_mb=0 --pop=true --benchmarks=a --store_name=KVSSD --uniform=true --warmup_time=0`

*pop* refers to population. To run YCSB B next, change *pop* to false and
*--benchmarks=a* to *--benchmarks=b* .
A log file will be created in the directory from which the command is
run. There are other options available, see the top of main.cc in the  ycsb
folder for details (although not all of them
are tested!). You can run multiple benchmarks with
*--benchmarks=a,b,c,d,f*, but it is less
convenient as there won't be a separate log file for each benchmark as of now.

## License

NVMeVirt is offered under the terms of the GNU General Public License version 2 as published by the Free Software Foundation. More information about this license can be found [here](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html).

Priority queue implementation [`pqueue/`](pqueue/) is offered under the terms of the BSD 2-clause license (GPL-compatible). (Copyright (c) 2014, Volkan Yazıcı <volkan.yazici@gmail.com>. All rights reserved.)
