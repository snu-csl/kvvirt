cmake_minimum_required(VERSION 3.10)

# Set project name and version
project(hello_world VERSION 1.0)

include_directories(../include)

# Source files
set(SOURCES
    main.c
)

# Add executable
add_executable(hello_world ${SOURCES})

