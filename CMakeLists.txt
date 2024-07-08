cmake_minimum_required(VERSION 3.10)
project(kvvirt)

include(ExternalProject)

set(BUILD_DIR "${CMAKE_BINARY_DIR}")

# Define the path to the nvmevirt directory containing the Makefile
set(DRIVER_DIR "${CMAKE_SOURCE_DIR}/nvmevirt/drivers/kernel_v5.10.37")
set(DRIVER_OUT "${CMAKE_BINARY_DIR}/driver")

set(NVMEVIRT_DIR "${CMAKE_SOURCE_DIR}/nvmevirt")
set(VIRT_OUT "${CMAKE_BINARY_DIR}/nvmevirt")

file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/obj")

# Define the custom command to build the Makefile project in nvmevirt
add_custom_command(
    OUTPUT ${DRIVER_DIR}/nvme-core.ko ${DRIVER_DIR}/nvme.ko
    COMMAND ${CMAKE_COMMAND} -E echo "Building KVSSD driver for kernel 5.10.37..."
    COMMAND ${CMAKE_MAKE_PROGRAM} -C ${DRIVER_DIR}
    WORKING_DIRECTORY ${NVMEVIRT_DIR}
    COMMENT "Running make in ${DRIVER_DIR}"
    VERBATIM
    )

# Define the custom command to build the Makefile project in nvmevirt
add_custom_command(
    OUTPUT ${NVMEVIRT_DIR}/nvmev.ko
    COMMAND ${CMAKE_COMMAND} -E echo "Building NVMeVirt..."
    COMMAND ${CMAKE_MAKE_PROGRAM} -C ${NVMEVIRT_DIR}
    WORKING_DIRECTORY ${NVMEVIRT_DIR}
    COMMENT "Running make in ${NVMEVIRT_DIR}"
    VERBATIM
    )

# Define a custom target that depends on the custom command
add_custom_target(
    driver ALL
    DEPENDS ${DRIVER_DIR}/nvme-core.ko ${DRIVER_DIR}/nvme.ko
    )

# Define a custom target that depends on the custom command
add_custom_target(
    nvmevirt ALL
    DEPENDS ${NVMEVIRT_DIR}/nvmev.ko
    )

# Define the custom command to copy nvmev.ko to the build folder
add_custom_command(
    TARGET nvmevirt POST_BUILD
    COMMENT "Copying nvmev.ko to nvmevirt/"
    COMMAND ${CMAKE_COMMAND} -E copy ${NVMEVIRT_DIR}/nvmev.ko ${VIRT_OUT}/nvmev.ko
    COMMAND ${CMAKE_COMMAND} -E copy ${NVMEVIRT_DIR}/*.o ${BUILD_DIR}/obj/
	COMMAND ${CMAKE_COMMAND} -E remove ${NVMEVIRT_DIR}/nvmev.ko
    COMMAND ${CMAKE_COMMAND} -E remove ${NVMEVIRT_DIR}/*.o
    )

add_custom_command(
    TARGET driver POST_BUILD
    COMMENT "Copying KVSSD driver to driver/"
    COMMAND ${CMAKE_COMMAND} -E copy ${DRIVER_DIR}/nvme-core.ko ${DRIVER_OUT}/nvme-core.ko
    COMMAND ${CMAKE_COMMAND} -E copy ${DRIVER_DIR}/nvme.ko ${DRIVER_OUT}/nvme.ko
    COMMAND ${CMAKE_COMMAND} -E remove ${DRIVER_DIR}/nvme-core.ko
    COMMAND ${CMAKE_COMMAND} -E remove ${DRIVER_DIR}/nvme.ko
    COMMAND ${CMAKE_COMMAND} -E remove ${DRIVER_DIR}/*.o
    )

# Used in YCSB
set(Q_DIR ${CMAKE_BINARY_DIR}/q_dir)
set(HIST_DIR ${CMAKE_BINARY_DIR}/hist_dir)

# Clone the repositories
ExternalProject_Add(
    q_repo
    GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
    PREFIX ${CMAKE_BINARY_DIR}/q_dir
    UPDATE_DISCONNECTED 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
)

ExternalProject_Add(
    hist_repo
    GIT_REPOSITORY https://github.com/HdrHistogram/HdrHistogram_c.git
	GIT_TAG main
    PREFIX ${HIST_DIR}
	UPDATE_DISCONNECTED 1
	SOURCE_DIR ${HIST_DIR}/src/hist_repo
	BINARY_DIR ${HIST_DIR}/build
	CONFIGURE_COMMAND ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR} ${HIST_DIR}/src/hist_repo
	BUILD_COMMAND ${CMAKE_COMMAND} --build ${HIST_DIR}/build
	INSTALL_COMMAND ""
	LOG_DOWNLOAD ON
)

# Add subdirectory
add_subdirectory(examples)

# Pass directories to subdirectory CMakeLists
set(Q_DIR ${Q_DIR} PARENT_SCOPE)
set(HIST_DIR ${HIST_DIR} PARENT_SCOPE)

# Custom clean commands
set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_BINARY_DIR}/q_repo;${CMAKE_BINARY_DIR}/hist_repo")

add_custom_target(extra_clean
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/q_repo
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/hist_repo
)

# Custom clean target to remove the external project directories
add_custom_target(clean_all
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_BINARY_DIR}/clean_all.cmake
)

# Generate a script to clean up the external project directories
file(WRITE ${CMAKE_BINARY_DIR}/clean_all.cmake
    "file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/q_dir)\n"
    "file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/hist_dir)\n"
	"file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/driver)\n"
	"file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/nvmevirt)\n"
	"file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/obj)\n"
	"file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/examples)\n"
)

