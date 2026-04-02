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
- **Micro-benchmark harness** for performance testing

## Building

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Options

| CMake Option | Default | Description |
|---|---|---|
| `ORO_BUILD_BENCH` | ON | Build the micro-benchmark |
| `ORO_WITH_NUMA` | OFF | Enable NUMA support (requires libnuma-dev) |

## Running the benchmark

```bash
./oro_bench [num_rows] [num_threads]
./oro_bench 100000 1
```

## Project structure

```
oro-db/
├── CMakeLists.txt
├── README.md
├── bench/
│   └── oro_bench.cpp          # Micro-benchmark harness
├── config/
│   └── mot.conf               # MOT configuration file
└── src/
    ├── mot_core/              # MOT core engine (from openGauss)
    │   ├── concurrency_control/
    │   ├── infra/
    │   ├── memory/
    │   ├── storage/
    │   ├── system/
    │   └── utils/
    └── stubs/                 # Replacement headers for openGauss deps
        ├── postgres.h
        ├── securec.h
        ├── libintl.h
        ├── knl/
        │   ├── knl_thread.h
        │   ├── knl_session.h
        │   └── knl_instance.h
        └── utils/
            ├── elog.h
            └── memutils.h
```

## Status

**Work in progress.** The MOT core compiles standalone with stub headers
replacing openGauss kernel dependencies. The MassTree index requires the
external `libmasstree` library (from `kohler/masstree-beta` or the openGauss
third-party build).

## License

The MOT core engine is licensed under the Mulan PSL v2 license (from openGauss).
See individual source files for details.
