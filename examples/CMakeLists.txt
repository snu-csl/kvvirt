cmake_minimum_required(VERSION 3.10)

# Add ycsb subdirectory
add_subdirectory(hello_world)
add_subdirectory(ycsb)

# Pass directories to subdirectories
set(Q_DIR ${Q_DIR} PARENT_SCOPE)
set(HIST_DIR ${HIST_DIR} PARENT_SCOPE)
