/*
 * coro_bench.cpp — Microbenchmark for coroutine-interleaved index probing.
 *
 * Builds a real MassTree primary index, loads N rows so the working set spills
 * out of cache, then issues random point lookups two ways on identical data:
 *
 *   seq  — IndexReadBatchSeq:  one lookup at a time (baseline).
 *   coro — IndexReadBatchCoro: a batch of lookups driven by a round-robin
 *          coroutine scheduler that prefetches each lookup's next node and
 *          switches to another lookup while the line loads, overlapping the
 *          DRAM stalls of independent probes.
 *
 * Both paths run the same MassTree descent (reach_leaf + leaf/value reads);
 * the only difference is interleaving. Prints throughput and speedup per batch
 * size so the improvement is directly observable.
 *
 * Usage:
 *   oro_coro_bench [config] [--records N] [--lookups K] [--batches 1,8,16,32]
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <libgen.h>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

#include "mot_engine.h"
#include "table.h"
#include "row.h"
#include "txn.h"
#include "index.h"
#include "index_factory.h"
#include "key.h"
#include "session_context.h"
#include "session_manager.h"
#include "catalog_column_types.h"
#include "mot_configuration.h"
#include "masstree_index.h"
#include "knl/knl_thread.h"

using namespace MOT;

namespace {

MOTEngine* g_engine = nullptr;
SessionContext* g_session = nullptr;
TxnManager* g_txn = nullptr;
Table* g_table = nullptr;

bool SetupEngine(const char* cfgPath)
{
    g_engine = MOTEngine::GetInstance();
    if (!g_engine) {
        g_engine = MOTEngine::CreateInstance(cfgPath);
    }
    return g_engine != nullptr;
}

// (val LONG, id LONG PK) — key column last, for the InternalKey path.
bool CreateTable(const char* name)
{
    g_session = g_engine->GetSessionManager()->CreateSessionContext(false, 4 * 1024);
    if (!g_session) {
        return false;
    }
    g_txn = g_session->GetTxnManager();

    g_txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    g_table = new Table();
    if (!g_table->Init(name, (std::string("public.") + name).c_str(), 2)) {
        return false;
    }
    g_table->AddColumn("val", sizeof(int64_t), MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, false);
    g_table->AddColumn("id", sizeof(uint64_t), MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);
    if (!g_table->InitRowPool() || !g_table->InitTombStonePool()) {
        return false;
    }
    if (g_txn->CreateTable(g_table) != RC_OK) {
        return false;
    }

    IndexTreeFlavor flavor = GetGlobalConfiguration().m_indexTreeFlavor;
    Index* idx = IndexFactory::CreateIndex(
        IndexOrder::INDEX_ORDER_PRIMARY, IndexingMethod::INDEXING_METHOD_TREE, flavor);
    if (!idx) {
        return false;
    }
    idx->SetUnique(true);
    if (!idx->SetNumTableFields(g_table->GetFieldCount())) {
        return false;
    }
    idx->SetNumIndexFields(1);
    uint32_t keyLen = g_table->GetFieldSize(1);
    idx->SetLenghtKeyFields(0, 1, keyLen);
    idx->SetTable(g_table);
    if (idx->IndexInit(keyLen, true, std::string(name) + "_pkey", nullptr) != RC_OK) {
        delete idx;
        return false;
    }
    idx->SetIsCommited(true);
    if (g_txn->CreateIndex(g_table, idx, true) != RC_OK) {
        return false;
    }
    if (g_txn->Commit() != RC_OK) {
        return false;
    }
    g_txn->EndTransaction();
    return true;
}

RC InsertRow(uint64_t id, int64_t val)
{
    g_txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    Row* row = g_table->CreateNewRow();
    if (!row) {
        g_txn->Rollback();
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    row->SetValue<int64_t>(0, val);
    row->SetInternalKey(1, id);
    RC rc = g_table->InsertRow(row, g_txn);
    if (rc != RC_OK) {
        g_txn->Rollback();
        return rc;
    }
    rc = g_txn->Commit();
    g_txn->EndTransaction();
    return rc;
}

// Build a primary search key for `id` (InternalKey path: last column).
Key* MakeKey(Index* idx, uint64_t id)
{
    Key* key = idx->CreateNewKey();
    Row* tmp = g_table->CreateNewRow();
    int lastCol = g_table->GetFieldCount() - 1;
    tmp->SetInternalKey(lastCol, id);
    idx->BuildKey(g_table, tmp, key);
    g_table->DestroyRow(tmp);
    return key;
}

double NowSec()
{
    using namespace std::chrono;
    return duration<double>(steady_clock::now().time_since_epoch()).count();
}

}  // namespace

int main(int argc, char* argv[])
{
    knl_thread_mot_init();

    const char* cfgPath = nullptr;
    char cfgBuf[PATH_MAX];
    uint64_t records = 10'000'000;
    uint64_t lookups = 4'000'000;
    std::vector<uint32_t> batchSizes = {8, 16, 32, 64};

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            records = strtoull(argv[++i], nullptr, 10);
        } else if (strcmp(argv[i], "--lookups") == 0 && i + 1 < argc) {
            lookups = strtoull(argv[++i], nullptr, 10);
        } else if (strcmp(argv[i], "--batches") == 0 && i + 1 < argc) {
            batchSizes.clear();
            char* s = argv[++i];
            for (char* tok = strtok(s, ","); tok; tok = strtok(nullptr, ",")) {
                batchSizes.push_back((uint32_t)atoi(tok));
            }
        } else if (argv[i][0] != '-') {
            cfgPath = argv[i];
        }
    }
    if (!cfgPath) {
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len > 0) {
            exePath[len] = '\0';
            snprintf(cfgBuf, sizeof(cfgBuf), "%s/mot.conf", dirname(exePath));
            cfgPath = cfgBuf;
        }
    }

    printf("=== oro-db coroutine-interleaved probe benchmark ===\n");
    printf("records=%llu lookups=%llu\n\n",
           (unsigned long long)records, (unsigned long long)lookups);

    if (!SetupEngine(cfgPath) || !CreateTable("coro_bench")) {
        fprintf(stderr, "FATAL: engine/table setup failed\n");
        return 1;
    }

    Index* baseIdx = g_table->GetPrimaryIndex();
    MasstreePrimaryIndex* idx = dynamic_cast<MasstreePrimaryIndex*>(baseIdx);
    if (!idx) {
        fprintf(stderr, "FATAL: primary index is not a MassTree (StubIndex fallback?)\n");
        return 1;
    }

    // Load.
    printf("[1] Loading %llu rows...\n", (unsigned long long)records);
    double t0 = NowSec();
    for (uint64_t id = 0; id < records; ++id) {
        if (InsertRow(id, (int64_t)id) != RC_OK) {
            fprintf(stderr, "FATAL: insert failed at id=%llu\n", (unsigned long long)id);
            return 1;
        }
    }
    printf("    loaded in %.2fs\n\n", NowSec() - t0);

    // Random lookup ids (uniform → cold misses).
    std::mt19937_64 rng(0xC0FFEE);
    std::uniform_int_distribution<uint64_t> dist(0, records - 1);
    std::vector<Key*> keys(lookups);
    for (uint64_t i = 0; i < lookups; ++i) {
        keys[i] = MakeKey(idx, dist(rng));
    }
    std::vector<const Key*> keyPtrs(lookups);
    for (uint64_t i = 0; i < lookups; ++i) {
        keyPtrs[i] = keys[i];
    }

    std::vector<Sentinel*> out(*std::max_element(batchSizes.begin(), batchSizes.end()));

    // Checksum to prevent the compiler eliding the lookups.
    auto runSeq = [&](uint32_t bs) -> std::pair<double, uint64_t> {
        uint64_t sum = 0;
        double s = NowSec();
        for (uint64_t i = 0; i + bs <= lookups; i += bs) {
            idx->IndexReadBatchSeq(&keyPtrs[i], out.data(), bs, 0);
            for (uint32_t j = 0; j < bs; ++j) {
                sum += reinterpret_cast<uintptr_t>(out[j]);
            }
        }
        return {NowSec() - s, sum};
    };
    auto runCoro = [&](uint32_t bs) -> std::pair<double, uint64_t> {
        uint64_t sum = 0;
        double s = NowSec();
        for (uint64_t i = 0; i + bs <= lookups; i += bs) {
            idx->IndexReadBatchCoro(&keyPtrs[i], out.data(), bs, 0);
            for (uint32_t j = 0; j < bs; ++j) {
                sum += reinterpret_cast<uintptr_t>(out[j]);
            }
        }
        return {NowSec() - s, sum};
    };

    // Warm up (touches the structure once).
    (void)runSeq(batchSizes.front());

    printf("[2] Random point lookups (lower time = better)\n");
    printf("    %-8s %14s %14s %12s %10s\n", "batch", "seq Mlookup/s", "coro Mlookup/s", "speedup", "match");
    for (uint32_t bs : batchSizes) {
        auto [seqT, seqSum] = runSeq(bs);
        auto [coroT, coroSum] = runCoro(bs);
        uint64_t done = (lookups / bs) * bs;
        double seqMps = done / seqT / 1e6;
        double coroMps = done / coroT / 1e6;
        printf("    %-8u %14.2f %14.2f %11.2fx %10s\n",
               bs, seqMps, coroMps, coroMps / seqMps,
               (seqSum == coroSum) ? "ok" : "MISMATCH");
    }
    printf("\n");

    return 0;
}
