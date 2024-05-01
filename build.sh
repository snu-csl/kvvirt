DIR=${PWD}

TYPE=$1
if [[ ! $TYPE =~ ^(debug|rel|clean)$ ]]; then 
    echo "Usage : build.sh debug/rel/clean"
    exit
fi

echo "Building YCSB in ${TYPE} mode."
cd ycsb

if [[ $TYPE == "clean" ]]; then
    rm -r build
else
    ./make.sh kvssd ${TYPE}
fi

echo "Building NVMeVirt."
cd ..

if [[ $TYPE == "clean" ]]; then
    make clean
else
    make -j
fi
