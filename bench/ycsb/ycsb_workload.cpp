/*
 * YCSB Schema Creation and Data Population for oro-db.
 *
 * Single table "usertable" with YCSB_KEY (uint64) + N varchar fields,
 * following Cooper et al., "Benchmarking Cloud Serving Systems with YCSB"
 * (SoCC 2010).
 */

#include "ycsb_workload.h"

#include <cstdio>
#include <cstring>

using namespace MOT;

namespace oro::ycsb {

// ======================================================================
// Schema creation
// ======================================================================
bool CreateSchema(TxnManager* txn, YcsbTables& tables,
                  uint32_t field_count, uint32_t field_length)
{
    using F = MOT_CATALOG_FIELD_TYPES;
    RC rc;

    // Total columns: 1 (YCSB_KEY) + field_count value fields
    uint32_t numCols = 1 + field_count;

    tables.usertable = new Table();
    if (!tables.usertable->Init("usertable", "public.usertable", numCols)) {
        fprintf(stderr, "ERROR: Table init failed for 'usertable'\n");
        return false;
    }

    // Column 0: YCSB_KEY (uint64)
    tables.usertable->AddColumn("YCSB_KEY", sizeof(uint64_t),
                                F::MOT_TYPE_LONG, true);

    // Columns 1..N: FIELD_0 .. FIELD_(N-1) (varchar)
    for (uint32_t i = 0; i < field_count; i++) {
        char colName[32];
        snprintf(colName, sizeof(colName), "FIELD_%u", i);
        tables.usertable->AddColumn(colName, field_length,
                                    F::MOT_TYPE_VARCHAR, false);
    }

    if (!tables.usertable->InitRowPool()) {
        fprintf(stderr, "ERROR: InitRowPool failed for 'usertable'\n");
        return false;
    }

    rc = txn->CreateTable(tables.usertable);
    if (rc != RC_OK) {
        fprintf(stderr, "ERROR: CreateTable 'usertable': %s\n", RcToString(rc));
        return false;
    }

    // Primary index on YCSB_KEY
    rc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY,
        IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR,
        true,          // unique
        YCSB_KEY_LEN,  // key length = 8 bytes
        "ix_usertable_pk",
        rc, nullptr);
    if (ix == nullptr || rc != RC_OK) {
        fprintf(stderr, "ERROR: Failed to create index 'ix_usertable_pk': %s\n",
                RcToString(rc));
        return false;
    }

    if (!ix->SetNumTableFields(tables.usertable->GetFieldCount())) {
        fprintf(stderr, "ERROR: SetNumTableFields failed for 'ix_usertable_pk'\n");
        delete ix;
        return false;
    }
    ix->SetNumIndexFields(1);
    ix->SetLenghtKeyFields(0, Col::YCSB_KEY, YCSB_KEY_LEN);  // 9: matches PackKey output
    ix->SetTable(tables.usertable);

    rc = txn->CreateIndex(tables.usertable, ix, true /* isPrimary */);
    if (rc != RC_OK) {
        fprintf(stderr, "ERROR: CreateIndex 'ix_usertable_pk' failed: %s\n",
                RcToString(rc));
        delete ix;
        return false;
    }
    tables.ix_primary = ix;

    return true;
}

// ======================================================================
// Data population
// ======================================================================

static bool InsertAndCommit(TxnManager* txn, Table* table, Row* row)
{
    RC rc = txn->InsertRow(row);
    if (rc != RC_OK) {
        table->DestroyRow(row);
        txn->Rollback();
        return false;
    }
    rc = txn->Commit();
    return rc == RC_OK;
}

bool PopulateData(MOTEngine* engine, YcsbTables& tables,
                  uint64_t record_count, uint32_t field_count,
                  uint32_t field_length)
{
    printf("[YCSB Population] Loading %lu records (%u fields x %u bytes)...\n",
           (unsigned long)record_count, field_count, field_length);

    SessionContext* session = engine->GetSessionManager()->CreateSessionContext();
    if (!session) {
        fprintf(stderr, "ERROR: Failed to create session for YCSB population\n");
        return false;
    }
    TxnManager* txn = session->GetTxnManager();
    oro::FastRandom rng(42);

    // Allocate a temporary buffer for random field values
    char* fieldBuf = new char[field_length + 1];

    uint64_t inserted = 0;
    for (uint64_t i = 0; i < record_count; i++) {
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

        Row* row = tables.usertable->CreateNewRow();
        if (!row) {
            txn->Rollback();
            fprintf(stderr, "WARN: CreateNewRow failed at key %lu\n",
                    (unsigned long)i);
            continue;
        }

        // Column 0: YCSB_KEY
        row->SetValue<uint64_t>(Col::YCSB_KEY, i);

        // Columns 1..N: random alphanumeric strings
        for (uint32_t f = 0; f < field_count; f++) {
            rng.RandomString(fieldBuf, field_length, field_length);
            SetStringValue(row, Col::FIELD_0 + f, fieldBuf);
        }

        if (InsertAndCommit(txn, tables.usertable, row)) {
            inserted++;
        } else {
            fprintf(stderr, "WARN: Failed to insert YCSB key %lu\n",
                    (unsigned long)i);
        }

        if ((i + 1) % 100000 == 0) {
            printf("    ... %lu / %lu rows inserted\n",
                   (unsigned long)(i + 1), (unsigned long)record_count);
        }
    }

    delete[] fieldBuf;
    engine->GetSessionManager()->DestroySessionContext(session);

    printf("[YCSB Population] Done. Inserted %lu / %lu rows.\n",
           (unsigned long)inserted, (unsigned long)record_count);
    return inserted == record_count;
}

}  // namespace oro::ycsb
