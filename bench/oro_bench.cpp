/*
 * oro-db micro-benchmark (stub version)
 *
 * Validates that the MOT engine initializes and basic table operations work.
 * Full benchmarking requires the real MassTree index.
 *
 * Usage: ./oro_bench
 */

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>

// MOT core headers
#include "mot_engine.h"
#include "table.h"
#include "row.h"
#include "txn.h"
#include "session_context.h"
#include "session_manager.h"
#include "catalog_column_types.h"

using Clock = std::chrono::high_resolution_clock;
using Ms = std::chrono::duration<double, std::milli>;

static void Report(const char* label, uint64_t ops, double ms)
{
    double sec = ms / 1000.0;
    double opsPerSec = (sec > 0) ? (double)ops / sec : 0;
    printf("  %-20s %8lu ops  %8.2f ms  %12.0f ops/sec\n",
           label, (unsigned long)ops, ms, opsPerSec);
}

int main(int argc, char* argv[])
{
    uint64_t numRows = 10000;
    if (argc > 1) numRows = (uint64_t)atol(argv[1]);

    printf("=== oro-db micro-benchmark ===\n");
    printf("  Rows:  %lu\n", (unsigned long)numRows);
    printf("  Index: StubPrimaryIndex (std::map, mutex-based)\n");
    printf("  Note:  Real MassTree not yet integrated\n\n");

    // 1. Initialize MOT engine
    printf("[1] Initializing MOT engine...\n");
    MOT::MOTEngine* engine = MOT::MOTEngine::CreateInstance();
    if (!engine) {
        fprintf(stderr, "FATAL: Failed to create MOT engine\n");
        return 1;
    }
    printf("    Engine initialized.\n\n");

    // 2. Create a session
    printf("[2] Creating session...\n");
    MOT::SessionContext* session = engine->GetSessionManager()->CreateSessionContext();
    if (!session) {
        fprintf(stderr, "FATAL: Failed to create session\n");
        MOT::MOTEngine::DestroyInstance();
        return 1;
    }
    MOT::TxnManager* txn = session->GetTxnManager();
    printf("    Session created (id=%u).\n\n", session->GetSessionId());

    // 3. Create table
    printf("[3] Creating table 'bench_kv'...\n");
    txn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);

    MOT::Table* table = new MOT::Table();
    if (!table->Init("bench_kv", "public.bench_kv", 3)) {
        fprintf(stderr, "FATAL: Table init failed\n");
        return 1;
    }

    table->AddColumn("id", sizeof(uint64_t), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);
    table->AddColumn("name", 64, MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR, false);
    table->AddColumn("value", sizeof(int64_t), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, false);

    if (!table->InitRowPool()) {
        fprintf(stderr, "FATAL: Failed to init row pool\n");
        return 1;
    }

    MOT::RC rc = txn->CreateTable(table);
    if (rc != MOT::RC_OK) {
        fprintf(stderr, "FATAL: CreateTable failed: rc=%u (%s)\n", (unsigned)rc, MOT::RcToString(rc));
        return 1;
    }
    rc = txn->Commit();
    if (rc != MOT::RC_OK) {
        fprintf(stderr, "FATAL: Commit after CreateTable failed: rc=%u\n", (unsigned)rc);
        return 1;
    }
    printf("    Table created with 3 columns.\n\n");

    // 4. Benchmark INSERT
    printf("[4] Running benchmarks...\n");
    auto t0 = Clock::now();
    uint64_t inserted = 0;

    for (uint64_t i = 1; i <= numRows; ++i) {
        txn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
        MOT::Row* row = table->CreateNewRow();
        if (!row) break;

        row->SetValue<uint64_t>(0, i);
        row->SetValue<int64_t>(2, (int64_t)(i * 10));

        rc = txn->InsertRow(row);
        if (rc != MOT::RC_OK) {
            table->DestroyRow(row);
            txn->Rollback();
            continue;
        }
        rc = txn->Commit();
        if (rc == MOT::RC_OK) ++inserted;
    }
    double insertMs = std::chrono::duration_cast<Ms>(Clock::now() - t0).count();
    Report("INSERT", inserted, insertMs);

    // 5. Cleanup
    printf("\n[5] Cleaning up...\n");
    engine->GetSessionManager()->DestroySessionContext(session);
    MOT::MOTEngine::DestroyInstance();
    printf("    Done.\n");

    return 0;
}
