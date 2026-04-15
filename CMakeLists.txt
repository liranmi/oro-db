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
option(ORO_MEMORY_DEBUG "Enable memory debug instrumentation (canaries, bounds checks, stats)" OFF)

# =============================================================================
# Masstree
# =============================================================================
# Check for patched masstree in third_party/masstree
set(MASSTREE_SRC_DIR "${CMAKE_SOURCE_DIR}/third_party/masstree")
if(EXISTS "${MASSTREE_SRC_DIR}/masstree_config.h")
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

# Masstree source files (openEuler patched masstree)
if(ORO_HAS_MASSTREE)
    set(MASSTREE_SOURCES
        ${MASSTREE_SRC_DIR}/kvthread.cpp
        ${MASSTREE_SRC_DIR}/straccum.cpp
        ${MASSTREE_SRC_DIR}/string.cpp
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

if(ORO_MEMORY_DEBUG)
    target_compile_definitions(oro PUBLIC ORO_MEMORY_DEBUG=1)
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
# Micro-benchmark executable (original)
# =============================================================================
if(ORO_BUILD_BENCH)
    add_executable(oro_micro_bench
        ${CMAKE_SOURCE_DIR}/bench/oro_bench.cpp
    )
    target_link_libraries(oro_micro_bench PRIVATE oro)
    target_include_directories(oro_micro_bench PRIVATE
        ${CMAKE_SOURCE_DIR}/src/stubs
        ${MOT_INCLUDE_DIRS}
    )

    # =========================================================================
    # TPC-C / YCSB benchmark executable
    # =========================================================================
    set(BENCH_INCLUDE_DIRS
        ${CMAKE_SOURCE_DIR}/bench/framework
        ${CMAKE_SOURCE_DIR}/bench/tpcc
        ${CMAKE_SOURCE_DIR}/bench/ycsb
    )

    add_executable(oro_bench
        ${CMAKE_SOURCE_DIR}/bench/main.cpp
        ${CMAKE_SOURCE_DIR}/bench/tpcc/tpcc_workload.cpp
        ${CMAKE_SOURCE_DIR}/bench/tpcc/tpcc_txn.cpp
        ${CMAKE_SOURCE_DIR}/bench/tpcc/tpcc_consistency.cpp
        ${CMAKE_SOURCE_DIR}/bench/ycsb/ycsb_workload.cpp
        ${CMAKE_SOURCE_DIR}/bench/ycsb/ycsb_txn.cpp
    )
    target_link_libraries(oro_bench PRIVATE oro)
    target_include_directories(oro_bench PRIVATE
        ${CMAKE_SOURCE_DIR}/src/stubs
        ${MOT_INCLUDE_DIRS}
        ${BENCH_INCLUDE_DIRS}
    )
    target_compile_options(oro_bench PRIVATE
        -Wno-unused-variable
        -Wno-unused-parameter
        -fpermissive
    )
    # Copy mot.conf next to the binary so it is found automatically
    add_custom_command(TARGET oro_bench POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config/mot.conf
            $<TARGET_FILE_DIR:oro_bench>/mot.conf
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

# =============================================================================
# SQLite virtual table interface
# =============================================================================
option(ORO_BUILD_SQLITE "Build SQLite virtual table interface" OFF)

if(ORO_BUILD_SQLITE)
    # Bundled SQLite amalgamation in third_party/sqlite
    set(SQLITE3_SRC_DIR "${CMAKE_SOURCE_DIR}/third_party/sqlite")
    if(NOT EXISTS "${SQLITE3_SRC_DIR}/sqlite3.h")
        message(FATAL_ERROR "SQLite amalgamation not found at ${SQLITE3_SRC_DIR}\n"
                "Download from https://www.sqlite.org/download.html and extract into third_party/sqlite/")
    endif()

    add_library(sqlite3_lib STATIC ${SQLITE3_SRC_DIR}/sqlite3.c)
    target_include_directories(sqlite3_lib PUBLIC ${SQLITE3_SRC_DIR})
    target_compile_definitions(sqlite3_lib PUBLIC
        SQLITE_ENABLE_COLUMN_METADATA=1
    )
    target_compile_options(sqlite3_lib PRIVATE -Wno-unused-parameter)
    target_link_libraries(sqlite3_lib PUBLIC dl)

    # MOT adapter: separate library with C linkage that sqlite3_lib will call into.
    # Built before oro_sqlite_vtab so it can be linked between sqlite3_lib and the executable.
    add_library(oro_mot_adapter STATIC
        ${CMAKE_SOURCE_DIR}/sqlite/oro_mot_adapter.cpp
    )
    target_link_libraries(oro_mot_adapter PUBLIC oro)
    target_include_directories(oro_mot_adapter PUBLIC
        ${CMAKE_SOURCE_DIR}/sqlite
        ${CMAKE_SOURCE_DIR}/src/stubs
        ${CMAKE_SOURCE_DIR}/bench/framework
        ${MOT_INCLUDE_DIRS}
        ${SQLITE3_SRC_DIR}
    )
    target_compile_options(oro_mot_adapter PRIVATE
        -Wno-unused-parameter
        -fpermissive
    )

    # Make sqlite3_lib link against the adapter (forward declarations resolve here)
    target_link_libraries(sqlite3_lib PUBLIC oro_mot_adapter)

    # Test executable (CREATE MOT TABLE / engine integration path)
    add_executable(oro_test_mot_engine
        ${CMAKE_SOURCE_DIR}/test/test_mot_engine.cpp
    )
    target_link_libraries(oro_test_mot_engine PRIVATE oro_mot_adapter sqlite3_lib)
    target_include_directories(oro_test_mot_engine PRIVATE
        ${CMAKE_SOURCE_DIR}/sqlite
        ${SQLITE3_SRC_DIR}
    )
    add_custom_command(TARGET oro_test_mot_engine POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config/mot.conf
            $<TARGET_FILE_DIR:oro_test_mot_engine>/mot.conf
    )

    # Interactive shell: oro_shell (SQLite CLI + oro-db engine)
    # Compile shell.c with main renamed to sqlite3_shell_main
    add_library(sqlite3_shell_lib STATIC ${SQLITE3_SRC_DIR}/shell.c)
    target_include_directories(sqlite3_shell_lib PUBLIC ${SQLITE3_SRC_DIR})
    target_compile_definitions(sqlite3_shell_lib PRIVATE
        main=sqlite3_shell_main
        SQLITE_ENABLE_COLUMN_METADATA=1
    )

    # Optional readline support for line editing
    find_library(READLINE_LIB readline)
    if(NOT READLINE_LIB)
        target_compile_definitions(sqlite3_shell_lib PRIVATE HAVE_READLINE=0)
    endif()

    target_compile_options(sqlite3_shell_lib PRIVATE
        -Wno-unused-parameter
        -Wno-unused-function
        -Wno-sign-compare
    )
    target_link_libraries(sqlite3_shell_lib PUBLIC sqlite3_lib)

    add_executable(oro_shell
        ${CMAKE_SOURCE_DIR}/sqlite/oro_shell.cpp
    )
    target_link_libraries(oro_shell PRIVATE oro_mot_adapter sqlite3_shell_lib)
    if(READLINE_LIB)
        target_link_libraries(oro_shell PRIVATE readline)
    endif()
    target_include_directories(oro_shell PRIVATE
        ${CMAKE_SOURCE_DIR}/sqlite
        ${SQLITE3_SRC_DIR}
        ${MOT_INCLUDE_DIRS}
    )
    target_compile_options(oro_shell PRIVATE
        -Wno-unused-parameter
        -fpermissive
    )
    add_custom_command(TARGET oro_shell POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config/mot.conf
            $<TARGET_FILE_DIR:oro_shell>/mot.conf
    )
endif()

message(STATUS "")
message(STATUS "=== oro-db configuration ===")
message(STATUS "  Build type:    ${CMAKE_BUILD_TYPE}")
message(STATUS "  Masstree:      ${ORO_HAS_MASSTREE}")
message(STATUS "  NUMA support:  ${ORO_WITH_NUMA}")
message(STATUS "  Benchmark:     ${ORO_BUILD_BENCH}")
message(STATUS "  Tests:         ${ORO_BUILD_TESTS}")
message(STATUS "  SQLite vtab:   ${ORO_BUILD_SQLITE}")
message(STATUS "  Memory debug:  ${ORO_MEMORY_DEBUG}")
message(STATUS "============================")
message(STATUS "")
