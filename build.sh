#!/bin/bash

DIR=${PWD}
export USER=$(whoami)
export Q_DIR="${DIR}/ycsb/concurrentqueue"

TYPE=$1
if [[ ! $TYPE =~ ^(debug|rel|clean)$ ]]; then 
    echo "Usage : build.sh debug/rel/clean"
    exit
fi

sudo apt-get install libgflags-dev

if [[ $TYPE == "clean" ]]; then
    cd drivers/kernel_v5.10.37
    make clean
    cd ${DIR}
    rm -r ycsb/build
    rm -r ${Q_DIR}
    make clean
    exit
fi

if [ ! -d ${Q_DIR}  ]; then
    echo "Cloning concurrent queue library."
    git clone https://github.com/cameron314/concurrentqueue.git ${Q_DIR}
fi

echo "Building KVSSD driver."
cd drivers/kernel_v5.10.37
make -j
cd ../../

echo "Building YCSB in ${TYPE} mode."
cd ycsb
./make.sh kvssd ${TYPE}

echo "Building NVMeVirt."
cd ..
make -j
