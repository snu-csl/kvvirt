#!/bin/bash

HOST_VIRT=/home/move/MyVirt/
NVMEV_DIR=/home/virt/nvmevirt # on the VM
YCSB_DIR=/home/virt/ycsb # on the VM
VM_USER=virt
VM_NAME=ubuntuvirt
VM_IP=192.168.123.41
NVMEV_CORES="36,37,38,39"
SYNC="/home/move/MyVirt/sync.sh"
MEMMAP_START="32G"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

function vm_start {
    virsh start ${VM_NAME}
}

function vm_shutdown {
    virsh destroy ${VM_NAME}
}

function vm_send_cmd {
    ssh ${VM_USER}@${VM_IP} "${1}"
}

function greenecho {
    printf "${GREEN}$1${NC}\n"
}

function redecho {
    printf "${RED}$1${NC}\n"
}

function vm_build_old {
    vm_send_cmd "make clean -C ${NVMEV_DIR} > /dev/null"
    vm_send_cmd "sed -i 's/#undef GC_STANDARD/#define GC_STANDARD/g' ${NVMEV_DIR}/ssd_config.h > /dev/null"
    vm_send_cmd "make -j -C ${NVMEV_DIR} > /dev/null"
}

function vm_build_new {
    vm_send_cmd "make clean -C ${NVMEV_DIR} > /dev/null"
    vm_send_cmd "sed -i 's/#define GC_STANDARD/#undef GC_STANDARD/g' ${NVMEV_DIR}/ssd_config.h > /dev/null"
    vm_send_cmd "make -j -C ${NVMEV_DIR} > /dev/null"
}

function vm_clear_dmesg {
    vm_send_cmd "sudo -n dmesg -c > /dev/null"
}

function vm_get_dmesg {
    vm_send_cmd "dmesg > /tmp/dmesg.log"
    scp ${VM_USER}@${VM_IP}:/tmp/dmesg.log $1 > /dev/null
}

function vm_get_file {
    scp ${VM_USER}@${VM_IP}:$1 $2
}

function vm_wait_ssh {
    until ping -c1 ${VM_IP} > /dev/null 2>&1; do :; done
}

function vm_renvmev {
    vm_send_cmd "sudo -n rmmod nvmev > /dev/null"
    vm_send_cmd "sudo -n insmod ${NVMEV_DIR}/nvmev.ko memmap_start=${MEMMAP_START} memmap_size=$1 cpus=${NVMEV_CORES}"
    vm_send_cmd "dmesg > /tmp/dmesg.log"
    scp ${VM_USER}@${VM_IP}:/tmp/dmesg.log /tmp/dmesg.log > /dev/null

    if grep -q "cut here" /tmp/dmesg.log; then
        FNAME="/tmp/$2_failed_dmesg.log"
        redecho "NVMeVirt module load for this test failed. Copying dmesg output to ${tmp}"
        mv /tmp/dmesg.log ${FNAME}
        return 0
    fi 

    sleep 3
    return 1
}

function vm_sync {
    vm_send_cmd "sync"
}

function vm_rem_nvmev {
    vm_send_cmd "sudo rmmod nvmev > /dev/null"
}

function nvmev_sync {
    DIR=$( pwd )
    cd ${HOST_VIRT}
    ./sync.sh
    vm_send_cmd "sync"
    cd ${DIR}
}

function vm_start_sync_and_wait {
    greenecho "Starting VM..."
    vm_shutdown > /dev/null
    vm_start 

    greenecho "Waiting for VM to start..."
    vm_wait_ssh

    if [ "${SYNC}" = "false" ]; then
        greenecho "Syncing NVMeVirt source..."
        nvmev_sync
    fi

    greenecho "VM started."
}

trap ctrl_c INT
function ctrl_c() {
    exit
}
