# oro-db

Standalone in-memory transactional storage engine extracted from
[openGauss MOT](https://opengauss.org), with MassTree indexing.

Named after the **Oro Jackson** — the ship of the Pirate King, built from
the Treasure Tree Adam.

## What is this?

oro-db extracts the MOT (Memory-Optimized Tables) core engine from openGauss
and makes it usable as a standalone C++ library, without the full database
server. It provides:

- **In-memory row storage** with NUMA-aware allocation
- **MassTree index** (trie of B+ trees) for ordered key-value access
- **OCC transactions** with MVCC and CSN-based snapshot isolation
- **Direct C++ API** for insert / select / update / delete
- **TPC-C and YCSB benchmarks** for performance evaluation
- **TPC-C consistency checks** (9 conditions from Clause 3.3, inline or post-run)
- **MVCC snapshot testing** mode for verifying isolation under concurrency
- **Comprehensive index test suite** (18 tests covering low-level and transactional APIs)

## Building

### Quick start (Makefile)

```bash
make debug -sj          # Debug build of oro_bench only (fastest iteration)
make debug-all -sj      # Debug build of all targets
make release -sj        # Release build of oro_bench only
make release-all -sj    # Release build of all targets
make clean              # Remove all build artifacts
```

Debug builds go to `build/debug/`, release to `build/release/`.

### Full CMake build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j$(nproc)
```

### CMake options

| Option | Default | Description |
|---|---|---|
| `ORO_BUILD_BENCH` | ON | Build benchmarks (micro + TPC-C/YCSB) |
| `ORO_BUILD_TESTS` | ON | Build the index test suite |
| `ORO_WITH_NUMA` | OFF | Enable NUMA support (requires libnuma-dev) |
| `ORO_MEMORY_DEBUG` | OFF | Memory debug instrumentation (canaries, bounds checks) |

### Build outputs

| Binary | Description |
|---|---|
| `liboro.a` | Static library (MOT core + stubs + MassTree) |
| `oro_bench` | Unified TPC-C/YCSB benchmark CLI |
| `oro_micro_bench` | Simple insert throughput micro-benchmark |
| `oro_test_index` | Index test suite (18 tests) |

`mot.conf` is auto-copied next to binaries by a CMake post-build step.

### Dependencies

- **Required**: CMake 3.14+, C++17 compiler, pthread, atomic
- **Optional**: libnuma-dev (for NUMA support)
- **Bundled**: MassTree in `third_party/masstree/` (openEuler-patched `kohler/masstree-beta`)

## Running the benchmarks

### TPC-C

Full TPC-C implementation with all 5 transaction types (NewOrder, Payment,
Delivery, OrderStatus, StockLevel) and 9 tables:

```bash
# Full mix, 4 warehouses, 8 threads, 30 seconds
./oro_bench --workload tpcc --warehouses 4 --threads 8 --duration 30

# NewOrder/Payment only (DBx1000-style): 80% NewOrder, 20% Payment
./oro_bench --workload tpcc -Tp 80 --warehouses 4 --threads 8

# Reduced schema for quick testing
./oro_bench --workload tpcc --small --warehouses 1 --threads 2

# Stop after a fixed number of transactions instead of time
./oro_bench --workload tpcc --warehouses 2 --threads 4 --max-txns 100000
```

### YCSB

Supports profiles A-F with configurable key distributions:

```bash
# Profile A (50% read, 50% update) with zipfian distribution
./oro_bench --workload ycsb --profile A --records 1000000 --threads 16

# Profile E (95% scan, 5% insert) with custom scan length
./oro_bench --workload ycsb --profile E --records 100000 --threads 4 --scan-length 100

# Uniform distribution, custom field layout
./oro_bench --workload ycsb --profile B --distribution uniform --fields 5 --field-length 200

# Custom operations per transaction
./oro_bench --workload ycsb --profile F --ops-per-txn 8 --threads 8
```

| Profile | Read | Update | Insert | Scan | RMW | Description |
|---------|------|--------|--------|------|-----|-------------|
| A | 50% | 50% | — | — | — | Update heavy |
| B | 95% | 5% | — | — | — | Read mostly |
| C | 100% | — | — | — | — | Read only |
| D | 95% | — | 5% | — | — | Read latest |
| E | — | — | 5% | 95% | — | Short ranges |
| F | 50% | — | — | — | 50% | Read-modify-write |

### Micro-benchmark

Simple insert throughput test:

```bash
./oro_micro_bench [num_rows] [num_threads]
./oro_micro_bench 100000 1
```

### Common options

| Flag | Description |
|---|---|
| `--threads N` | Worker threads (default: 1) |
| `--duration N` | Benchmark duration in seconds (default: 10) |
| `--max-txns N` | Stop after N total transactions (overrides --duration) |
| `--json FILE` | Write results to JSON file |
| `--config FILE` | Override mot.conf path |
| `--check` | Run post-benchmark data consistency checks |

## Consistency checks and MVCC testing

### Post-run checks (`--check`)

Runs data consistency validation after the benchmark completes:

```bash
# TPC-C: runs all 9 consistency conditions (Clause 3.3)
./oro_bench --workload tpcc --warehouses 2 --threads 4 --duration 30 --check

# YCSB: verifies commits > 0 and abort rate < 50%
./oro_bench --workload ycsb --profile A --threads 2 --records 100000 --check
```

### Inline consistency checks (`--consistency-pct`)

Mixes read-only consistency checks into the TPC-C transaction workload across
all threads, testing MVCC snapshot correctness under concurrency:

```bash
# 10% of transactions on every thread are consistency checks
./oro_bench --workload tpcc --warehouses 4 --threads 8 --consistency-pct 10
```

### MVCC test mode (`--mvcc-test`)

Dedicated MVCC snapshot-correctness mode. Thread 0 runs 5% consistency checks
under REPEATABLE_READ isolation while all other threads run the normal TPC-C
mix as concurrent writers:

```bash
./oro_bench --workload tpcc --warehouses 4 --threads 8 --duration 60 --mvcc-test
```

Requires at least 2 threads. Mutually exclusive with `--consistency-pct`.

## Index tests

18 tests covering low-level index API and transactional API:

```bash
./build/oro_test_index                  # Uses auto-discovered mot.conf
./build/oro_test_index config/mot.conf  # Explicit config path
```

## CI

Triggered on push/PR to `main` (GitHub Actions, `ubuntu-latest`, 15-min timeout):

1. Build with CMake (`RelWithDebInfo`)
2. Index tests (18/18)
3. YCSB profiles A-F (2 threads, 100K records, `--check`)

## Project structure

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
│       ├── oro_stub_index.h       # std::map fallback index
│       ├── oro_masstree_adapter.cpp
│       ├── oro_recovery_stub.cpp
│       ├── postgres.h, securec.h, libintl.h
│       ├── knl/                   # Kernel context stubs
│       └── utils/                 # elog, memutils stubs
├── bench/
│   ├── main.cpp            # Unified TPC-C/YCSB benchmark CLI
│   ├── oro_bench.cpp       # Micro-benchmark
│   ├── framework/          # Benchmark infrastructure (config, stats, utils)
│   ├── tpcc/               # TPC-C workload (9 tables, 5 transaction types)
│   │   └── tpcc_consistency.cpp  # 9 consistency conditions (Clause 3.3)
│   └── ycsb/               # YCSB workload (profiles A-F)
├── test/
│   └── test_index.cpp      # Index test suite (18 tests)
└── third_party/
    └── masstree/           # Bundled openEuler-patched MassTree
```

## Status

The MOT core compiles standalone with stub headers replacing openGauss kernel
dependencies. MassTree is bundled in `third_party/` (openEuler-patched
`kohler/masstree-beta`). Without MassTree, the engine falls back to a
mutex-guarded `std::map` index.

All index tests pass (18/18). TPC-C and YCSB benchmarks are functional.

## License

The MOT core engine is licensed under the Mulan PSL v2 license (from openGauss).
MassTree is MIT licensed. See individual source files for details.
