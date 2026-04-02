cmake_minimum_required(VERSION 3.14)
project(oro-db
    VERSION 0.1.0
    LANGUAGES C CXX
    DESCRIPTION "Standalone in-memory transactional storage engine extracted from openGauss MOT"
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

# Options
option(ORO_BUILD_BENCH "Build the micro-benchmark" ON)
option(ORO_WITH_NUMA "Enable NUMA support (requires libnuma)" OFF)

# =============================================================================
# Masstree
# =============================================================================
# The upstream kohler/masstree-beta has incompatible API with MOT's wrappers.
# MOT requires the openGauss-patched masstree (from openGauss-third_party repo).
# Until that is integrated, we use StubPrimaryIndex (std::map-based).
# TODO: integrate openGauss-patched masstree for real MassTree index support.
#
# If you have the patched masstree, set MASSTREE_DIR:
#   cmake .. -DMASSTREE_DIR=/path/to/patched/masstree
option(MASSTREE_DIR "Path to openGauss-patched masstree (optional)" "")
if(MASSTREE_DIR AND EXISTS ${MASSTREE_DIR}/config.h)
    message(STATUS "Using masstree from: ${MASSTREE_DIR}")
    include_directories(BEFORE ${MASSTREE_DIR})
    set(ORO_HAS_MASSTREE ON)
else()
    message(STATUS "Masstree not available — using StubPrimaryIndex")
    set(ORO_HAS_MASSTREE OFF)
endif()

# =============================================================================
# Stubs (replace openGauss kernel headers)
# =============================================================================
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/src/stubs)

# =============================================================================
# MOT Core source collection
# =============================================================================
set(MOT_CORE_DIR ${CMAKE_SOURCE_DIR}/src/mot_core)

# Include ALL subdirectories of MOT core
file(GLOB_RECURSE MOT_ALL_DIRS LIST_DIRECTORIES true ${MOT_CORE_DIR}/*)
set(MOT_INCLUDE_DIRS ${MOT_CORE_DIR})
foreach(dir ${MOT_ALL_DIRS})
    if(IS_DIRECTORY ${dir})
        list(APPEND MOT_INCLUDE_DIRS ${dir})
    endif()
endforeach()
include_directories(${MOT_INCLUDE_DIRS})

# Collect all .cpp files from MOT core
file(GLOB_RECURSE MOT_CORE_SOURCES ${MOT_CORE_DIR}/*.cpp)

# Exclude masstree-dependent source files — using StubPrimaryIndex until
# the openGauss-patched masstree library is available.
# (Upstream kohler/masstree-beta has incompatible API.)
list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*mot_masstree_kvthread\\.cpp$")
list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*masstree_index\\.cpp$")
list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*index_factory\\.cpp$")
# Recovery manager has heavy openGauss memory context dependencies
list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*mtls_recovery_manager\\.cpp$")

# Stubs source (openGauss kernel replacements + stub index)
set(STUB_SOURCES
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_stubs.cpp
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_index_factory.cpp
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_recovery_stub.cpp
)

# =============================================================================
# liboro - the core MOT library
# =============================================================================
add_library(oro STATIC
    ${MOT_CORE_SOURCES}
    ${STUB_SOURCES}
)

target_compile_definitions(oro PUBLIC
    MOT_STANDALONE=1
    ENABLE_MOT=1
)

# Suppress warnings for vendored code
target_compile_options(oro PRIVATE
    -Wno-unused-variable
    -Wno-unused-parameter
    -Wno-sign-compare
    -Wno-missing-field-initializers
    -Wno-write-strings
    -Wno-unused-but-set-variable
    -Wno-deprecated-declarations
    -fpermissive
)

# Note: real masstree not linked yet (using StubPrimaryIndex)
target_link_libraries(oro PUBLIC
    pthread
    atomic
)

if(ORO_WITH_NUMA)
    target_compile_definitions(oro PUBLIC HAVE_NUMA=1)
    target_link_libraries(oro PUBLIC numa)
endif()

# =============================================================================
# Benchmark executable
# =============================================================================
if(ORO_BUILD_BENCH)
    add_executable(oro_bench
        ${CMAKE_SOURCE_DIR}/bench/oro_bench.cpp
    )
    target_link_libraries(oro_bench PRIVATE oro)
    target_include_directories(oro_bench PRIVATE
        ${CMAKE_SOURCE_DIR}/src/stubs
        ${MOT_INCLUDE_DIRS}
    )
endif()

message(STATUS "")
message(STATUS "=== oro-db configuration ===")
message(STATUS "  Build type:    ${CMAKE_BUILD_TYPE}")
message(STATUS "  Masstree:      ${ORO_HAS_MASSTREE}")
message(STATUS "  NUMA support:  ${ORO_WITH_NUMA}")
message(STATUS "  Benchmark:     ${ORO_BUILD_BENCH}")
message(STATUS "============================")
message(STATUS "")
