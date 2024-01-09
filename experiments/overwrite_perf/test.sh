#!/bin/bash
source ../common.sh

TS=$( date +%s )
DIR=${TS}_logdir
mkdir -p ${DIR}

NUM=100000
OPS=1000
THREADS=20
VLEN=1024
KLEN=8
CACHE=1
U=true
WARMUP=1

greenecho "Running overwrite test..."

for dist in zipf uniform; do
    if [ "${dist}" = "zipf" ]; then
        U=false
    else
        U=true
    fi

    for ftl in old new; do
        if [ "${ftl}" = "old" ]; then
            greenecho "This is the old FTL ${dist} test."
            vm_build_old
        else
            greenecho "This is the new FTL ${dist} test."
            vm_build_new
        fi

        vm_clear_dmesg
        greenecho "Loading NVMeVirt..."
        if vm_renvmev ${TS}; then
            redecho "Test failed! Moving on to the next."
            continue
        else
            greenecho "NVMeVirt loaded."
        fi

        greenecho "Running benchmark..."

        if ! vm_send_cmd "sudo ${YCSB_DIR}/build/ycsb_kvssd \
            --num_pairs=${NUM} \
            --vlen=${VLEN} \
            --threads=${THREADS} \
            --num_ops=${OPS} \
            --duration=0 \
            --cache_size_mb=${CACHE} \
            --pop=true \
            --benchmarks=o \
            --store_name=KVSSD \
            --uniform=${U} \
            --warmup_time=${WARMUP} > ow_${TS}_${dist}_${ftl}_results.log"; then
             vm_get_dmesg ${DIR}/failed_dmesg.log
             redecho "FAILED"
             tail -1000 ${DIR}/failed_dmesg.log
         fi
         vm_get_file ow_${TS}_${dist}_${ftl}_results.log ${DIR}/ow_${TS}_${dist}_${ftl}_results.log
         vm_rem_nvmev
         vm_get_dmesg ${DIR}/ow_${TS}_${dist}_${ftl}_dmesg.log
         greenecho "Benchmark done."
         exit
    done
done

greenecho "Overwrite test done."
