/*
 * YCSB Transaction Execution for oro-db.
 *
 * Implements the six workload profiles defined in:
 *   Cooper et al., "Benchmarking Cloud Serving Systems with YCSB", SoCC 2010.
 *
 * Profile  Read%   Update%  Insert%  Scan%  RMW%   Description
 * -------  ------  -------  -------  -----  -----  ------------------
 *   A       50       50       --       --     --    Update heavy
 *   B       95        5       --       --     --    Read mostly
 *   C      100       --       --       --     --    Read only
 *   D       95       --        5       --     --    Read latest
 *   E       --       --        5       95     --    Short ranges
 *   F       50       --       --       --     50    Read-modify-write
 */

#include "ycsb_txn.h"

#include <atomic>
#include <cstdio>
#include <cstring>

using namespace MOT;

namespace oro::ycsb {

// ---------------------------------------------------------------------------
// Global monotonic counter for INSERT key generation (workloads D and E).
// Keys start above the pre-loaded record_count so they never collide with
// existing rows.
// ---------------------------------------------------------------------------
static std::atomic<uint64_t> g_insert_key_counter{0};

// Initialize the counter once from the loader; subsequent calls are no-ops.
static uint64_t NextInsertKey(uint64_t record_count)
{
    return record_count + g_insert_key_counter.fetch_add(1, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Key helper: pick a key from the configured distribution.
// ---------------------------------------------------------------------------
static uint64_t PickKey(UniformGenerator* uniform_gen,
                        ZipfianGenerator* zipfian_gen,
                        oro::Distribution dist,
                        oro::FastRandom& rng)
{
    if (dist == oro::Distribution::ZIPFIAN) {
        return zipfian_gen->Next(rng);
    }
    return uniform_gen->Next(rng);
}

// ---------------------------------------------------------------------------
// Operation types within a single YCSB transaction.
// ---------------------------------------------------------------------------
enum class OpType { READ, UPDATE, INSERT, SCAN, RMW };

// Decide one operation based on the profile mix using a uniform random draw.
static OpType PickOperation(oro::YcsbProfile profile, oro::FastRandom& rng)
{
    uint32_t r = (uint32_t)rng.NextUniform(0, 99);  // [0, 99]

    switch (profile) {
        case oro::YcsbProfile::A:
            // 50% Read, 50% Update
            return (r < 50) ? OpType::READ : OpType::UPDATE;

        case oro::YcsbProfile::B:
            // 95% Read, 5% Update
            return (r < 95) ? OpType::READ : OpType::UPDATE;

        case oro::YcsbProfile::C:
            // 100% Read
            return OpType::READ;

        case oro::YcsbProfile::D:
            // 95% Read, 5% Insert
            return (r < 95) ? OpType::READ : OpType::INSERT;

        case oro::YcsbProfile::E:
            // 95% Scan, 5% Insert
            return (r < 95) ? OpType::SCAN : OpType::INSERT;

        case oro::YcsbProfile::F:
            // 50% Read, 50% Read-Modify-Write
            return (r < 50) ? OpType::READ : OpType::RMW;

        default:
            return OpType::READ;
    }
}

// ---------------------------------------------------------------------------
// Individual operation implementations.
// ---------------------------------------------------------------------------

// SQL: SELECT * FROM usertable WHERE YCSB_KEY = ?
static RC DoRead(TxnManager* txn, const YcsbTables& tables,
                 uint64_t ycsb_key, uint32_t field_count)
{
    RC rc = RC_OK;
    Index* ix = tables.ix_primary;
    Key* key = ix->CreateNewSearchKey();
    KeyFillU64(key, 0, ycsb_key);

    Row* row = txn->RowLookupByKey(tables.usertable, AccessType::RD, key, rc);
    ix->DestroyKey(key);

    if (rc != RC_OK || row == nullptr) {
        return (rc != RC_OK) ? rc : RC_OK;  // key-not-found is benign
    }

    // Read all value fields to simulate the full-row fetch.
    for (uint32_t f = 0; f < field_count; f++) {
        (void)row->GetValue(Col::FIELD_0 + (int)f);
    }
    return RC_OK;
}

// SQL: UPDATE usertable SET FIELD_x = ? WHERE YCSB_KEY = ?
static RC DoUpdate(TxnManager* txn, const YcsbTables& tables,
                   uint64_t ycsb_key, uint32_t field_count,
                   uint32_t field_length, oro::FastRandom& rng)
{
    RC rc = RC_OK;
    Index* ix = tables.ix_primary;
    Key* key = ix->CreateNewSearchKey();
    KeyFillU64(key, 0, ycsb_key);

    Row* row = txn->RowLookupByKey(tables.usertable, AccessType::RD_FOR_UPDATE, key, rc);
    ix->DestroyKey(key);

    if (rc != RC_OK || row == nullptr) {
        return (rc != RC_OK) ? rc : RC_OK;
    }

    // Pick a random field and write random data into it.
    int target_field = Col::FIELD_0 + (int)rng.NextUniform(0, field_count - 1);
    char buf[256];
    uint32_t len = (field_length < sizeof(buf)) ? field_length : (uint32_t)(sizeof(buf) - 1);
    rng.RandomString(buf, len, len);
    row->SetValue(target_field, buf);

    return RC_OK;
}

// SQL: INSERT INTO usertable (YCSB_KEY, FIELD_0, ...) VALUES (?, ?, ...)
static RC DoInsert(TxnManager* txn, const YcsbTables& tables,
                   uint64_t record_count, uint32_t field_count,
                   uint32_t field_length, oro::FastRandom& rng)
{
    RC rc = RC_OK;
    uint64_t new_key = NextInsertKey(record_count);

    Row* row = tables.usertable->CreateNewRow();
    if (row == nullptr) {
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    row->SetValue<uint64_t>(Col::YCSB_KEY, new_key);

    char buf[256];
    uint32_t len = (field_length < sizeof(buf)) ? field_length : (uint32_t)(sizeof(buf) - 1);
    for (uint32_t f = 0; f < field_count; f++) {
        rng.RandomString(buf, len, len);
        row->SetValue(Col::FIELD_0 + (int)f, buf);
    }

    rc = txn->InsertRow(row);
    if (rc != RC_OK) {
        tables.usertable->DestroyRow(row);
    }
    return rc;
}

// SQL: SELECT * FROM usertable WHERE YCSB_KEY >= ? LIMIT scan_length
static RC DoScan(TxnManager* txn, const YcsbTables& tables,
                 uint64_t ycsb_key, uint32_t scan_length,
                 uint32_t field_count, uint32_t thread_id)
{
    RC rc = RC_OK;
    Index* ix = tables.ix_primary;
    Key* search_key = ix->CreateNewSearchKey();
    KeyFillU64(search_key, 0, ycsb_key);

    bool found = false;
    IndexIterator* it = ix->Search(search_key, true, true, thread_id, found);

    uint32_t scanned = 0;
    while (it != nullptr && it->IsValid() && scanned < scan_length) {
        Sentinel* sentinel = it->GetPrimarySentinel();
        Row* row = txn->RowLookup(AccessType::RD, sentinel, rc);
        if (rc != RC_OK) {
            break;
        }
        if (row != nullptr) {
            // Read all value fields.
            for (uint32_t f = 0; f < field_count; f++) {
                (void)row->GetValue(Col::FIELD_0 + (int)f);
            }
        }
        scanned++;
        it->Next();
    }

    if (it != nullptr) {
        it->Destroy();
    }
    ix->DestroyKey(search_key);
    return rc;
}

// SQL: SELECT * FROM usertable WHERE YCSB_KEY = ?
//      UPDATE usertable SET FIELD_x = ? WHERE YCSB_KEY = ?
// (Read-Modify-Write: read the full row, modify one field, commit atomically.)
static RC DoReadModifyWrite(TxnManager* txn, const YcsbTables& tables,
                            uint64_t ycsb_key, uint32_t field_count,
                            uint32_t field_length, oro::FastRandom& rng)
{
    RC rc = RC_OK;
    Index* ix = tables.ix_primary;
    Key* key = ix->CreateNewSearchKey();
    KeyFillU64(key, 0, ycsb_key);

    // Acquire the row with RD_FOR_UPDATE so we hold a write lock.
    Row* row = txn->RowLookupByKey(tables.usertable, AccessType::RD_FOR_UPDATE, key, rc);
    ix->DestroyKey(key);

    if (rc != RC_OK || row == nullptr) {
        return (rc != RC_OK) ? rc : RC_OK;
    }

    // Read phase: read all value fields.
    for (uint32_t f = 0; f < field_count; f++) {
        (void)row->GetValue(Col::FIELD_0 + (int)f);
    }

    // Modify phase: pick a random field, write random data.
    int target_field = Col::FIELD_0 + (int)rng.NextUniform(0, field_count - 1);
    char buf[256];
    uint32_t len = (field_length < sizeof(buf)) ? field_length : (uint32_t)(sizeof(buf) - 1);
    rng.RandomString(buf, len, len);
    row->SetValue(target_field, buf);

    return RC_OK;
}

// ---------------------------------------------------------------------------
// Public entry point.
// ---------------------------------------------------------------------------
RC RunYcsbTxn(TxnManager* txn, const YcsbTables& tables,
              oro::YcsbProfile profile, uint32_t ops_per_txn,
              uint32_t field_count, uint32_t field_length,
              uint32_t scan_length, uint64_t record_count,
              UniformGenerator* uniform_gen,
              ZipfianGenerator* zipfian_gen,
              oro::Distribution dist,
              oro::FastRandom& rng,
              uint32_t thread_id)
{
    RC rc = RC_OK;

    // Begin a new transaction.
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    for (uint32_t i = 0; i < ops_per_txn; i++) {
        OpType op = PickOperation(profile, rng);

        switch (op) {
            case OpType::READ: {
                uint64_t key = PickKey(uniform_gen, zipfian_gen, dist, rng);
                rc = DoRead(txn, tables, key, field_count);
                break;
            }
            case OpType::UPDATE: {
                uint64_t key = PickKey(uniform_gen, zipfian_gen, dist, rng);
                rc = DoUpdate(txn, tables, key, field_count, field_length, rng);
                break;
            }
            case OpType::INSERT: {
                rc = DoInsert(txn, tables, record_count, field_count, field_length, rng);
                break;
            }
            case OpType::SCAN: {
                uint64_t key = PickKey(uniform_gen, zipfian_gen, dist, rng);
                rc = DoScan(txn, tables, key, scan_length, field_count, thread_id);
                break;
            }
            case OpType::RMW: {
                uint64_t key = PickKey(uniform_gen, zipfian_gen, dist, rng);
                rc = DoReadModifyWrite(txn, tables, key, field_count, field_length, rng);
                break;
            }
        }

        // Abort early if any operation within the transaction fails.
        if (rc != RC_OK) {
            txn->Rollback();
            return rc;
        }
    }

    // All operations succeeded -- commit.
    rc = txn->Commit();
    return rc;
}

}  // namespace oro::ycsb
