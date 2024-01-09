#!/bin/bash
source ../common.sh

TS=$( date +%s )
DIR=${TS}_logdir
mkdir -p ${DIR}

NUM=750000
THREADS=20
OPS=$( echo "10000000 / ${THREADS}" | bc )
VLEN=1024
KLEN=8
CACHE=1
U=true
WARMUP=1
SYNC=false

greenecho "Running overwrite test..."

for dist in zipf uniform; do
    if [ "${dist}" = "zipf" ]; then
        U=false
    else
        U=true
    fi

    for ftl in old new; do
        vm_start_sync_and_wait

        greenecho "VM started."
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
             redecho "This test failed!"
             echo "ow_${TS}_${dist}_${ftl}" >> ${DIR}/failures
             tail -500 ${DIR}/failed_dmesg.log
         fi

         vm_get_file ow_${TS}_${dist}_${ftl}_results.log ${DIR}/ow_${TS}_${dist}_${ftl}_results.log
         vm_rem_nvmev
         vm_get_dmesg ${DIR}/ow_${TS}_${dist}_${ftl}_dmesg.log
         greenecho "Benchmark done."
         vm_sync
         vm_shutdown
    done
done

greenecho "Overwrite test done."
