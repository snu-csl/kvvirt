#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage : make.sh kvssd debug/rel"
    exit
fi

STORE=$1
if [[ ! $STORE =~ ^(kvssd)$ ]]; then 
    echo "Usage : make.sh kvssd debug/rel"
    exit
fi

TYPE=$2
if [[ ! $TYPE =~ ^(debug|rel)$ ]]; then 
    echo "Usage : make.sh kvssd debug/rel"
    exit
fi

if [[ $TYPE == rel ]]; then
    TYPE="-O3"
elif [[ $TYPE == debug ]]; then
    TYPE="-O0"
fi

WRAP=""
LIB=""
INC=""
DEF=""
if [[ $STORE == kvssd ]]; then
    WRAP="kvssd.cc kvssd_ycsb.cc"
fi

mkdir -p build
g++-11 latest-generator.cc zipf.cc main.cc ${WRAP} -o build/ycsb_${STORE} ${DEF} ${TYPE} -g -std=c++17 -lpthread -lhdr_histogram -lgflags ${LIB} ${INC}
