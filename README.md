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
- **Comprehensive index test suite** (18 tests covering low-level and transactional APIs)

## Building

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Options

| CMake Option | Default | Description |
|---|---|---|
| `ORO_BUILD_BENCH` | ON | Build the benchmarks (micro-benchmark + TPC-C/YCSB) |
| `ORO_BUILD_TESTS` | ON | Build the index test suite |
| `ORO_WITH_NUMA` | OFF | Enable NUMA support (requires libnuma-dev) |

## Running the benchmarks

### Micro-benchmark

Validates basic engine functionality (insert throughput):

```bash
./oro_micro_bench [num_rows] [num_threads]
./oro_micro_bench 100000 1
```

### TPC-C

Full TPC-C implementation with all 5 transaction types (NewOrder, Payment,
Delivery, OrderStatus, StockLevel):

```bash
./oro_bench --workload tpcc --warehouses 4 --threads 8 --duration 30
```

### YCSB

Supports profiles A–F (read-heavy, write-heavy, scan, read-modify-write, etc.):

```bash
./oro_bench --workload ycsb --profile A --records 1000000 --threads 16
```

### Index tests

```bash
./oro_test_index
```

## Project structure

```
oro-db/
├── CMakeLists.txt
├── README.md
├── bench/
│   ├── oro_bench.cpp              # Micro-benchmark (basic insert throughput)
│   ├── main.cpp                   # Unified TPC-C / YCSB benchmark CLI
│   ├── framework/                 # Benchmark infrastructure (config, stats, utils)
│   ├── tpcc/                      # TPC-C workload (9 tables, 5 transactions)
│   └── ycsb/                      # YCSB workload (profiles A–F)
├── config/
│   └── mot.conf                   # MOT configuration file
├── test/
│   └── test_index.cpp             # Index test suite (low-level + transactional)
├── third_party/
│   └── masstree/                  # Bundled MassTree (openEuler-patched)
└── src/
    ├── mot_core/                  # MOT core engine (from openGauss)
    │   ├── concurrency_control/   # OCC transaction manager
    │   ├── infra/                 # Config, containers, stats, synchronization
    │   ├── memory/                # NUMA-aware allocation, garbage collector
    │   ├── storage/               # Tables, rows, columns, indexes
    │   ├── system/                # Engine, transactions, checkpoint, recovery
    │   └── utils/                 # Helpers and logging
    └── stubs/                     # Replacement headers for openGauss deps
        ├── postgres.h
        ├── securec.h
        ├── libintl.h
        ├── oro_stubs.cpp          # Thread-local / global context setup
        ├── oro_index_factory.cpp  # MassTree / StubIndex selection
        ├── oro_stub_index.h       # std::map fallback when MassTree unavailable
        ├── oro_masstree_adapter.cpp
        ├── oro_recovery_stub.cpp
        ├── knl/                   # Kernel context stubs
        └── utils/                 # elog, memutils stubs
```

## Status

The MOT core compiles standalone with stub headers replacing openGauss kernel
dependencies. MassTree is bundled in `third_party/` (openEuler-patched
`kohler/masstree-beta`). Without MassTree, the engine falls back to a
mutex-guarded `std::map` index.

All index tests pass (18/18). TPC-C and YCSB benchmarks are functional.

## License

The MOT core engine is licensed under the Mulan PSL v2 license (from openGauss).
See individual source files for details.
