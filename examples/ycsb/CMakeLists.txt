cmake_minimum_required(VERSION 3.10)

# Set project name and version
project(ycsb VERSION 1.0)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Include directories
include_directories(${Q_DIR}/src/q_repo/ ${HIST_DIR}/src/hist_repo/include ../include)

# Source files
set(SOURCES
    latest-generator.cc
    zipf.cc
    main.cc
    kvssd.cc
    kvssd_ycsb.cc
)

# Add executable
add_executable(ycsb ${SOURCES})
add_dependencies(ycsb hist_repo q_repo)

# Link libraries
target_link_libraries(ycsb
    ${HIST_DIR}/build/src/libhdr_histogram_static.a
    pthread
    gflags
)
