# CLAUDE.md

## Project Overview

oro-db is a standalone in-memory transactional storage engine extracted from
[openGauss MOT](https://opengauss.org). It provides a C++ library for
in-memory row storage with OCC transactions, MVCC, MassTree indexing, and
NUMA-aware allocation. Named after the Oro Jackson (Pirate King's ship).

## Quick Reference

```bash
# Build (debug, just the benchmark)
make debug -sj

# Build (release, all targets)
make release-all -sj

# Run tests
./build/debug/oro_test_index

# Run TPC-C benchmark
./build/debug/oro_bench --workload tpcc --warehouses 4 --threads 8 --duration 30

# Run YCSB benchmark
./build/debug/oro_bench --workload ycsb --profile A --records 1000000 --threads 16

# Full CMake build (what CI does)
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo && cmake --build build -j$(nproc)
```

## Build System

**CMake 3.14+** with C++17 and C11. Default build type: `RelWithDebInfo`.

### Makefile Targets (convenience wrapper)

| Target | What it builds |
|---|---|
| `make debug` | Debug build of `oro_bench` only |
| `make release` | Release build of `oro_bench` only |
| `make debug-all` | Debug build of all targets |
| `make release-all` | Release build of all targets |
| `make clean` | Remove `build/` directory |

Always pass `-sj` for silent parallel builds: `make debug -sj`

Debug builds go to `build/debug/`, release to `build/release/`.

### CMake Options

| Option | Default | Description |
|---|---|---|
| `ORO_BUILD_BENCH` | ON | Build benchmarks (micro + TPC-C/YCSB) |
| `ORO_BUILD_TESTS` | ON | Build index test suite |
| `ORO_WITH_NUMA` | OFF | Enable NUMA support (requires libnuma-dev) |
| `ORO_MEMORY_DEBUG` | OFF | Memory debug instrumentation (canaries, bounds checks) |

### Build Outputs

| Binary | Description |
|---|---|
| `liboro.a` | Static library (MOT core + stubs + MassTree) |
| `oro_bench` | Unified TPC-C/YCSB benchmark CLI |
| `oro_micro_bench` | Simple insert throughput micro-benchmark |
| `oro_test_index` | Index test suite (18 tests) |

`mot.conf` is auto-copied next to binaries by a CMake post-build step.

## Testing

### Index Tests (18 tests)

```bash
./build/oro_test_index               # Uses auto-discovered mot.conf
./build/oro_test_index config/mot.conf  # Explicit config path
```

Tests cover two levels:
- **Part 1**: Low-level index API (direct insert/read/iterate on MassTree)
- **Part 2**: Transactional API (InsertRow, commit, concurrent multi-thread access)

Custom test framework with `TEST_ASSERT`, `TEST_ASSERT_RC`, and `RUN_TEST` macros.

### Benchmark Consistency Checks

```bash
# YCSB with post-run consistency validation
./build/oro_bench --workload ycsb --profile A --threads 2 --records 100000 --check

# TPC-C with consistency checks
./build/oro_bench --workload tpcc --warehouses 1 --threads 2 --check
```

### What CI Runs

Triggered on push/PR to `main`. Runs on `ubuntu-latest` with 15-min timeout:
1. Build with CMake (`RelWithDebInfo`)
2. `oro_test_index` (18 tests)
3. YCSB profiles A through F (2 threads, 100K records, `--check`)

## Repository Structure

```
oro-db/
├── CMakeLists.txt          # Main build configuration
├── Makefile                # Convenience wrapper for CMake
├── config/mot.conf         # MOT engine configuration (standalone mode)
├── src/
│   ├── mot_core/           # MOT engine from openGauss (~136 cpp, ~176 headers)
│   │   ├── concurrency_control/  # OCC transaction manager
│   │   ├── infra/                # Config, containers, stats, synchronization
│   │   ├── memory/               # NUMA-aware allocation, garbage collector
│   │   ├── storage/              # Tables, rows, columns, indexes, MassTree
│   │   ├── system/               # Engine, transactions, checkpoint, recovery
│   │   └── utils/                # Helpers and logging
│   └── stubs/              # Replacement headers for openGauss kernel deps
│       ├── oro_stubs.cpp          # Thread-local/global context setup
│       ├── oro_index_factory.cpp  # MassTree/StubIndex selection
│       ├── oro_masstree_adapter.cpp
│       ├── oro_recovery_stub.cpp
│       ├── oro_stub_index.h       # std::map fallback index
│       ├── postgres.h, securec.h, libintl.h
│       ├── knl/                   # Kernel context stubs
│       └── utils/                 # elog, memutils stubs
├── bench/
│   ├── main.cpp            # Unified TPC-C/YCSB benchmark CLI
│   ├── oro_bench.cpp       # Micro-benchmark
│   ├── framework/          # Benchmark infrastructure (config, stats, utils)
│   ├── tpcc/               # TPC-C workload (9 tables, 5 transaction types)
│   └── ycsb/               # YCSB workload (profiles A-F)
├── test/
│   └── test_index.cpp      # Index test suite (18 tests)
└── third_party/
    └── masstree/           # Bundled openEuler-patched MassTree
```

## Architecture

### Key Layers

1. **MOT Engine** (`MOTEngine`): Singleton entry point. Manages tables, sessions, transactions.
2. **Storage**: `Table` -> `Row` -> `Column`. Each table has a primary `Index` (MassTree or std::map fallback).
3. **Concurrency Control**: OCC with MVCC. CSN-based snapshot isolation. Per-transaction access sets.
4. **Memory**: NUMA-aware allocators, memory pools, garbage collector for deferred reclamation.
5. **Stubs**: Replace openGauss kernel dependencies (`postgres.h`, `knl_*` contexts, `elog`, `securec`, etc.) with minimal standalone implementations.

### MassTree Index

The primary index is a MassTree (trie of B+ trees) from `third_party/masstree/`.
If MassTree is unavailable (missing `masstree_config.h`), the build falls back
to `StubIndex` (mutex-guarded `std::map`). Selection happens in `oro_index_factory.cpp`.

### Stubs Layer

The stubs directory must be included **before** standard headers (see `CMakeLists.txt`
`include_directories(BEFORE ...)`). This ensures `postgres.h`, `securec.h`, etc.
resolve to our lightweight stubs instead of any system headers.

Key stubs:
- `oro_stubs.cpp`: Provides `thread_local t_thrd`, `u_sess`, `g_instance` globals
- `oro_index_factory.cpp`: Routes index creation to MassTree or StubIndex
- `oro_recovery_stub.cpp`: No-op recovery manager for standalone mode

## Code Conventions

- **Language**: C++17, C11
- **Class names**: CamelCase (`MOTEngine`, `SessionContext`, `OccTransactionManager`)
- **Variables/functions**: snake_case in new code; MOT core uses mixed styles from openGauss
- **License headers**: Mulan PSL v2 on MOT core files; MIT on MassTree
- **No code formatter** configured (`.clang-format` does not exist; `editor.formatOnSave: false` in VS Code)
- **Compile flags**: `-fpermissive` and `-Wno-*` flags suppress warnings in vendored MOT code. New code in `bench/`, `test/`, `stubs/` should aim for clean compilation.
- **Custom test macros**: `TEST_ASSERT(cond, msg)`, `TEST_ASSERT_RC(rc, msg)`, `RUN_TEST(fn)` in `test/test_index.cpp`
- **Defines**: `MOT_STANDALONE=1` and `ENABLE_MOT=1` are always set. `ORO_HAS_MASSTREE=1` when MassTree is available.

## Important Notes for AI Assistants

### Build Considerations
- Always verify changes compile: `make debug -sj` is the fastest feedback loop
- After modifying core engine files, run `./build/debug/oro_test_index` to validate
- The stubs include order matters: stubs must resolve before MOT core headers
- `mtls_recovery_manager.cpp` is deliberately excluded from the build (heavy openGauss deps)

### Working with MOT Core (`src/mot_core/`)
- This is vendored code from openGauss. Modifications should be minimal and targeted.
- New functionality should go in `src/stubs/`, `bench/`, or `test/` when possible.
- MOT core has deep interdependencies; changing one header can cascade widely.

### Working with Benchmarks (`bench/`)
- TPC-C implements all 5 standard transactions: NewOrder, Payment, Delivery, OrderStatus, StockLevel
- YCSB supports profiles A-F with configurable distributions (uniform, zipfian)
- The `--check` flag enables post-run consistency validation
- `bench/framework/` contains shared infrastructure (config parsing, stats, utilities)

### Configuration
- `config/mot.conf` is the engine configuration file
- Key settings: memory limits, NUMA, logging level
- Automatically copied next to binaries during build
- CI uses default settings; adjust `max_mot_global_memory` if running on constrained systems

### Dependencies
- **Required**: cmake (3.14+), C++17 compiler, pthread, atomic
- **Optional**: libnuma-dev (for NUMA support)
- **Bundled**: MassTree in `third_party/masstree/` (openEuler-patched `kohler/masstree-beta`)
- No external package manager (no vcpkg, conan, etc.)

### Common Pitfalls
- Forgetting to run tests after MOT core changes (can cause subtle concurrency bugs)
- Key format for LONG columns is 9 bytes (1 sign + 8 big-endian); must use `Column::PackKey` path
- Transaction operations require proper session context setup via `oro_stubs.cpp` globals
- MassTree `threadinfo` must be initialized per-thread before index operations
