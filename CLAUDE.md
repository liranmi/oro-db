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

## Engine API Guidelines

The bench code talks directly to MOT engine APIs (no SQL layer, no FDW).

### Transaction Lifecycle

```
txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
// ... operations ...
rc = txn->Commit();   // or txn->Rollback();
txn->EndTransaction();
```

### Row Lookup

- **Point lookup (primary index):** Use `Lookup()` in `tpcc_helper.h`. Builds a key from a packed uint64, calls `txn->RowLookupByKey()`, destroys the key.
- **From iterator:** `txn->RowLookup(AccessType, sentinel, rc)` — the only way to get a **visible** row. Raw `sentinel->GetData()` bypasses MVCC.
- **AccessType:**
  - `RD` — read-only, no lock
  - `RD_FOR_UPDATE` — read with intent to update (acquires row-level lock for OCC). Required before `UpdateRow`.

### Updating Rows

```cpp
rc = txn->UpdateRow(row, COLUMN_ID, new_value);  // RD_FOR_UPDATE → WR, creates MVCC draft
Row* draft = txn->GetLastAccessedDraft();
// Read original values from `row`, write additional columns to `draft`:
draft->SetValue<T>(COLUMN_ID, value);
```

- First `UpdateRow` creates the draft. Subsequent columns can be written directly to the draft.
- Do NOT call `row->SetValue()` on the original global row — bypasses MVCC entirely.

### String Columns

- **Write:** `SetStringValue(row, COL_ID, str)` — copies raw bytes up to column's declared size (friend function in `row.h`).
- **Read:** `(const char*)row->GetValue(COL_ID)` — raw column bytes (no length prefix). Use `strnlen(ptr, col_size)` for actual length.
- VARCHAR columns store raw bytes, NOT `ColumnVARCHAR::Pack/Unpack` format.

### Primary Index Keys

All primary indexes use a single packed `uint64_t` column as the key (the last column, e.g., `C_KEY`). Composite keys are encoded via shift+or (e.g., `PackCustKey(w_id, d_id, c_id)`). Stored via `row->SetInternalKey(lastCol, value)`, and `BuildKey` copies it directly (the "InternalKey path").

### Key Encoding — Big-Endian

MassTree compares keys as byte strings (lexicographic via `htonq`). All `Pack*` functions return **big-endian** uint64 values (via `ToBE()`) so byte-string comparison matches numerical ordering.

- `BuildSearchKey` stores BE value directly via `FillValue`
- `SetInternalKey` stores BE value via `SetValue<uint64_t>`
- Do NOT use native integer comparison (`>=`, `<=`) on BE-encoded packed keys — wrong on little-endian
- Helpers in `bench_util.h`: `ToBE(val)`, `EncodeU64BE(dst, val)`, `KeyFillU64(key, offset, val)`

### Secondary Indexes

The engine's `BuildKey` takes an InternalKey shortcut for all bench rows, so secondary indexes with different key compositions **cannot** use `BuildKey` or auto-population via `InsertRow`. They must be **manually managed**:

```cpp
Key* key = ix->CreateNewSearchKey();
key->FillPattern(0x00, key->GetKeyLength(), 0);
EncodeU64BE(buf, value); key->FillValue(buf, 8, offset);  // uint64 fields
key->FillValue((const uint8_t*)str, len, offset);          // string fields
// Non-unique indexes: append rowId at offset = user_key_len
key->FillValue((uint8_t*)&rowId, 8, user_key_len);         // native endian
ix->DestroyKey(key);
```

- Non-unique indexes: engine adds `NON_UNIQUE_INDEX_SUFFIX_LEN` (8 bytes) internally. Pass only user key length to `CreateIndexEx`.
- Safe for TPC-C since customer/item rows are never inserted/deleted during the benchmark.

### Index Iteration (Cursors)

```cpp
Key* searchKey = BuildOrCreateKey(...);
bool found = false;
IndexIterator* it = ix->Search(searchKey, true/*matchKey*/, true/*forward*/, pid, found);
while (it != nullptr && it->IsValid()) {
    const Key* cur = (const Key*)it->GetKey();
    if (memcmp(cur->GetKeyBuf(), searchKey->GetKeyBuf(), prefixLen) != 0) break;
    Sentinel* s = it->GetPrimarySentinel();
    Row* r = txn->RowLookup(accessType, s, rc);
    if (rc != RC_OK) break;
    if (r) { /* visible row */ }
    it->Next();
}
if (it) it->Destroy();
ix->DestroyKey(searchKey);
```

Helpers: `ScanRange()`, `ScanPrefix()` in `tpcc_helper.h` (for uint64 packed keys).

### Range Scans — Two-Cursor Pattern

Based on the FDW `IsScanEnd` pattern. Use two cursors: one walks forward, one marks end boundary.

```cpp
auto [lo, hi] = PrefixKeyRange(PackOlKey(w,d,o_lo,0), PackOlKey(w,d,o_hi,0), OL_SUFFIX_BITS);
Key* lo_key = BuildSearchKey(txn, ix, lo);
Key* hi_key = BuildSearchKey(txn, ix, hi);
bool found = false;
IndexIterator* cursor_0 = ix->Search(lo_key, true, true, pid, found);
IndexIterator* cursor_1 = ix->Search(hi_key, true, true, pid, found);

while (cursor_0 != nullptr && cursor_0->IsValid()) {
    if (cursor_1 != nullptr && cursor_1->IsValid()) {
        const Key* k0 = reinterpret_cast<const Key*>(cursor_0->GetKey());
        const Key* k1 = reinterpret_cast<const Key*>(cursor_1->GetKey());
        if (k0 && k1 &&
            memcmp(k0->GetKeyBuf(), k1->GetKeyBuf(), ix->GetKeySizeNoSuffix()) >= 0)
            break;
    }
    Sentinel* s = cursor_0->GetPrimarySentinel();
    Row* row = txn->RowLookup(AccessType::RD, s, rc);
    if (rc != RC_OK) break;
    if (row) { /* process */ }
    cursor_0->Next();
}
if (cursor_0) cursor_0->Destroy();
if (cursor_1) cursor_1->Destroy();
txn->DestroyTxnKey(lo_key);
txn->DestroyTxnKey(hi_key);
```

**Critical: use `>= 0`, not `> 0`** in the memcmp end check. `cursor_1` lands on the first key AT OR AFTER `hi_key`; `>= 0` stops BEFORE reaching that position.

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

- **Do not make code changes that were not discussed or requested.** If you identify a potential issue, report it and wait for confirmation before modifying code.

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
