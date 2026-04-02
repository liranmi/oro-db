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
# Check for patched masstree in third_party/masstree (with config.h from ./configure)
set(MASSTREE_SRC_DIR "${CMAKE_SOURCE_DIR}/third_party/masstree")
if(EXISTS "${MASSTREE_SRC_DIR}/config.h")
    message(STATUS "Using patched masstree from: ${MASSTREE_SRC_DIR}")
    set(ORO_HAS_MASSTREE ON)
else()
    message(STATUS "Masstree not available — using StubPrimaryIndex")
    message(STATUS "  To enable: cd third_party/masstree && autoreconf -i && ./configure --enable-max-key-len=256")
    set(ORO_HAS_MASSTREE OFF)
endif()

# =============================================================================
# Stubs (replace openGauss kernel headers)
# =============================================================================
# Stubs dir must come first so our postgres.h etc. override system/openGauss headers
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/src/stubs)
# Masstree include must come before MOT core so masstree_config.h resolves to our stub bridge
if(ORO_HAS_MASSTREE)
    include_directories(BEFORE ${MASSTREE_SRC_DIR})
endif()

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

# Recovery manager has heavy openGauss memory context dependencies
list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*mtls_recovery_manager\\.cpp$")

if(ORO_HAS_MASSTREE)
    # With real masstree: exclude the original index_factory.cpp (use our stub),
    # and exclude MOT's mot_masstree_kvthread.cpp (we use upstream threadinfo with our adapter)
    list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*/index_factory\\.cpp$")
    list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*mot_masstree_kvthread\\.cpp$")
else()
    # Without masstree: exclude masstree-dependent files
    list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*mot_masstree_kvthread\\.cpp$")
    list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*masstree_index\\.cpp$")
    list(FILTER MOT_CORE_SOURCES EXCLUDE REGEX ".*/index_factory\\.cpp$")
endif()

# Stubs source (openGauss kernel replacements)
set(STUB_SOURCES
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_stubs.cpp
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_index_factory.cpp
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_recovery_stub.cpp
    ${CMAKE_SOURCE_DIR}/src/stubs/oro_masstree_adapter.cpp
)

# Masstree source files (from upstream kohler/masstree-beta)
if(ORO_HAS_MASSTREE)
    set(MASSTREE_SOURCES
        ${MASSTREE_SRC_DIR}/kvthread.cc
        ${MASSTREE_SRC_DIR}/straccum.cc
        ${MASSTREE_SRC_DIR}/str.cc
        ${MASSTREE_SRC_DIR}/string.cc
        ${MASSTREE_SRC_DIR}/compiler.cc
    )
endif()

# =============================================================================
# liboro - the core MOT library
# =============================================================================
add_library(oro STATIC
    ${MOT_CORE_SOURCES}
    ${STUB_SOURCES}
    ${MASSTREE_SOURCES}
)

target_compile_definitions(oro PUBLIC
    MOT_STANDALONE=1
    ENABLE_MOT=1
)

if(ORO_HAS_MASSTREE)
    target_compile_definitions(oro PUBLIC ORO_HAS_MASSTREE=1)
endif()

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

# =============================================================================
# Test executable
# =============================================================================
option(ORO_BUILD_TESTS "Build the index test suite" ON)

if(ORO_BUILD_TESTS)
    add_executable(oro_test_index
        ${CMAKE_SOURCE_DIR}/test/test_index.cpp
    )
    target_link_libraries(oro_test_index PRIVATE oro)
    target_include_directories(oro_test_index PRIVATE
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
message(STATUS "  Tests:         ${ORO_BUILD_TESTS}")
message(STATUS "============================")
message(STATUS "")
