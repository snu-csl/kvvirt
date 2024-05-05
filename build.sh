#!/bin/bash

DIR=${PWD}
export USER=$(whoami)
export Q_DIR="${DIR}/ycsb/concurrentqueue"
export HIST_DIR="${DIR}/ycsb/hdrhistogram_c"

TYPE=$1
if [[ ! $TYPE =~ ^(debug|rel|clean)$ ]]; then 
    echo "Usage : build.sh debug/rel/clean"
    exit
fi

#sudo apt-get install libgflags-dev cmake make zlib1g-dev

if [[ $TYPE == "clean" ]]; then
    cd drivers/kernel_v5.10.37
    make clean
    cd ${DIR}
    rm -r ycsb/build
    rm -r ${Q_DIR}
    rm -r ${HIST_DIR}
    make clean
    exit
fi

#if [ ! -d ${Q_DIR}  ]; then
#    echo "Cloning concurrent queue library."
#    git clone https://github.com/cameron314/concurrentqueue.git ${Q_DIR}
#fi
#
#if [ ! -d ${HIST_DIR}  ]; then
#    echo "Cloning histogram library."
#    git clone https://github.com/HdrHistogram/HdrHistogram_c ${HIST_DIR}
#    echo "Building histogram library."
#    cd ${HIST_DIR}
#    mkdir build
#    cd build
#    cmake ../
#    make -j
#    cd ${DIR}
#fi
#
#echo "Building KVSSD driver."
#cd drivers/kernel_v5.10.37
#make -j
#cd ../../
#
#echo "Building YCSB in ${TYPE} mode."
#cd ycsb
#./make.sh kvssd ${TYPE}
#cd ..

echo "Building NVMeVirt."
make -j
