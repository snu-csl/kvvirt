#!/bin/bash
source ../common.sh

TS=$( date +%s )
DIR=${TS}_logdir
mkdir -p ${DIR}

DISK_SIZE_G="32G"
NUM=22000000
THREADS=20
OPS=$( echo "10000000 / ${THREADS}" | bc )
VLEN=1024
KLEN=8
CACHE=0
U=true
WARMUP=1
BENCH="a b c d e f"
PRECON=$(( ${NUM} * 14 )) 
PRECON_T=$( echo "${PRECON} / ${THREADS}" | bc )

greenecho "Running YCSB tests..."

for dist in zipf uniform; do
    if [ "${dist}" = "zipf" ]; then
        U=false
    else
        U=true
    fi

    for ftl in old new; do
        vm_start_sync_and_wait

        if [ "${ftl}" = "old" ]; then
            greenecho "This is the old FTL ${dist} test."
            vm_build_old
        else
            greenecho "This is the new FTL ${dist} test."
            vm_build_new
        fi

        vm_clear_dmesg
        greenecho "Loading NVMeVirt..."
        if vm_renvmev ${DISK_SIZE_G} ${TS}; then
            echo "ow_${TS}_${dist}_${ftl}" >> ${DIR}/failures
            continue
        else
            greenecho "NVMeVirt loaded."
        fi

        greenecho "Preconditioning with ${PRECON} overwrites..."
        FNAME="precon_${TS}_${dist}_${ftl}"
        if ! vm_send_cmd "sudo ${YCSB_DIR}/build/ycsb_kvssd \
            --num_pairs=${NUM} \
            --vlen=${VLEN} \
            --threads=${THREADS} \
            --num_ops=${PRECON_T} \
            --duration=0 \
            --cache_size_mb=${CACHE} \
            --pop=true \
            --benchmarks=o \
            --store_name=KVSSD \
            --uniform=${U} \
            --warmup_time=${WARMUP} > ${FNAME}_results.log"; then
                    vm_get_dmesg ${DIR}/failed_dmesg.log
                    redecho "This test failed!"
                    echo "ow_${TS}_${dist}_${ftl}" >> ${DIR}/failures
                    tail -500 ${DIR}/failed_dmesg.log
        fi
        vm_get_file ${FNAME}_results.log ${DIR}/${FNAME}_results.log
        vm_get_dmesg ${DIR}/${FNAME}_dmesg.log
        vm_clear_dmesg
        greenecho "Precondition done."

        for bench in ${BENCH}; do
            greenecho "Running YCSB ${bench}..."
            FNAME="${bench}_${TS}_${dist}_${ftl}"
            if ! vm_send_cmd "sudo ${YCSB_DIR}/build/ycsb_kvssd \
                --num_pairs=${NUM} \
                --vlen=${VLEN} \
                --threads=${THREADS} \
                --num_ops=${OPS} \
                --duration=0 \
                --cache_size_mb=${CACHE} \
                --pop=false \
                --benchmarks=${bench} \
                --store_name=KVSSD \
                --uniform=${U} \
                --warmup_time=${WARMUP} > ${FNAME}_results.log"; then
                            vm_get_dmesg ${DIR}/failed_dmesg.log
                            redecho "This test failed!"
                            echo "ow_${TS}_${dist}_${ftl}" >> ${DIR}/failures
                            tail -500 ${DIR}/failed_dmesg.log
            fi
            vm_get_file ${FNAME}_results.log ${DIR}/${FNAME}_results.log
            vm_get_dmesg ${DIR}/${FNAME}_dmesg.log
            vm_clear_dmesg
            greenecho "YCSB ${bench} done."
         done

         vm_rem_nvmev
         greenecho "Moving on to next."
         vm_sync
         vm_shutdown
    done
done

greenecho "YCSB test done."
