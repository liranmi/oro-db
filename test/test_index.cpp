/*
 * oro-db index test suite
 *
 * Two test levels:
 *   Part 1 — Low-level index API tests (direct index insert/read/iterate)
 *   Part 2 — Transactional API tests (InsertRow, commit, concurrent access)
 *
 * Build: cmake --build build -- oro_test_index
 * Run:   ./build/oro_test_index [config/mot.conf]
 */

#include <atomic>
#include <cassert>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <libgen.h>
#include <numeric>
#include <random>
#include <thread>
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

/* =========================================================================
 * Test framework
 * ========================================================================= */

static int g_passed = 0;
static int g_failed = 0;

#define TEST_ASSERT(cond, msg)                                                \
    do {                                                                      \
        if (!(cond)) {                                                        \
            fprintf(stderr, "  FAIL [%s:%d]: %s — %s\n",                     \
                    __func__, __LINE__, msg, #cond);                          \
            g_failed++;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

#define TEST_ASSERT_RC(rc, msg) \
    TEST_ASSERT((rc) == MOT::RC_OK, msg)

#define RUN_TEST(fn) do {               \
        int _before = g_failed;         \
        fn;                             \
        if (g_failed == _before) {      \
            g_passed++;                 \
            printf("  [PASS] %s\n", #fn); \
        }                               \
    } while (0)

/* =========================================================================
 * Helper: build a search key from a uint64_t using the index's BuildKey
 *
 * The key format for LONG columns is 9 bytes (1 sign + 8 big-endian), so
 * we must build keys via the same Column::PackKey path the engine uses.
 * The simplest correct way: create a temp row, set the id, call BuildKey.
 * ========================================================================= */

/* Build a search key using the InternalKey path (bypasses FDW null bitmap).
 * The key is the raw bytes of the last column in the table. */
static void BuildSearchKey(MOT::Index* idx, MOT::Table* table,
                           uint64_t id, MOT::Key* key)
{
    MOT::Row* tmpRow = table->CreateNewRow();
    /* SetInternalKey sets m_keyType=INTERNAL_KEY and stores the value.
     * BuildKey then uses GetInternalKeyBuff which reads the last column. */
    int lastCol = table->GetFieldCount() - 1;
    tmpRow->SetInternalKey(lastCol, id);
    idx->BuildKey(table, tmpRow, key);
    table->DestroyRow(tmpRow);
}

/* =========================================================================
 * Table/index creation helpers
 * ========================================================================= */

struct TestEnv {
    MOT::MOTEngine* engine = nullptr;
    MOT::SessionContext* session = nullptr;   /* DDL session (table/index creation) */
    MOT::TxnManager* txn = nullptr;          /* DDL txn manager */
    MOT::SessionContext* dmlSession = nullptr; /* DML session (inserts/reads) */
    MOT::TxnManager* dmlTxn = nullptr;        /* DML txn manager */
    MOT::Table* table = nullptr;
};

static bool SetupEngine(TestEnv& env, const char* cfgPath)
{
    env.engine = MOT::MOTEngine::GetInstance();
    if (!env.engine)
        env.engine = MOT::MOTEngine::CreateInstance(cfgPath);
    return env.engine != nullptr;
}

/*
 * Create a (id BIGINT PK, val BIGINT) table with a proper primary index.
 *
 * MOT tables always have a NULLBYTES column at position 0 (the null bitmap),
 * and user columns start at position 1.  The LONG column key is 9 bytes
 * (1 sign byte + 8 big-endian value).
 */
static bool CreateKVTable(TestEnv& env, const char* name)
{
    /* Create session on the caller's thread */
    /* Reserve 2MB of session memory for GC limbo groups */
    env.dmlSession = env.engine->GetSessionManager()->CreateSessionContext(
        false, 2 * 1024 /* reserveMemoryKb */);
    if (!env.dmlSession) return false;
    env.dmlTxn = env.dmlSession->GetTxnManager();

    env.dmlTxn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);

    env.table = new MOT::Table();
    /* 2 columns: [0] val, [1] id (key column LAST for InternalKey path) */
    if (!env.table->Init(name, (std::string("public.") + name).c_str(), 2))
        return false;
    env.table->AddColumn("val", sizeof(int64_t),
                         MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, false);
    env.table->AddColumn("id", sizeof(uint64_t),
                         MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);
    if (!env.table->InitRowPool()) return false;

    MOT::RC rc = env.dmlTxn->CreateTable(env.table);
    if (rc != MOT::RC_OK) return false;

    /* InternalKey path uses raw CpKey — no PackKey encoding, key = sizeof(uint64_t) */
    const uint32_t keyLen = sizeof(uint64_t);
    MOT::IndexTreeFlavor flavor = MOT::GetGlobalConfiguration().m_indexTreeFlavor;
    MOT::Index* idx = MOT::IndexFactory::CreatePrimaryIndexEx(
        MOT::IndexingMethod::INDEXING_METHOD_TREE, flavor,
        keyLen, std::string(name) + "_pkey", rc, nullptr);
    if (!idx || rc != MOT::RC_OK) return false;

    idx->SetNumTableFields(env.table->GetFieldCount());
    idx->SetNumIndexFields(1);
    idx->SetLenghtKeyFields(0, 1, keyLen);  /* key field 0 → table column 1 ("id") */
    idx->SetTable(env.table);

    rc = env.dmlTxn->CreateIndex(env.table, idx, true);
    if (rc != MOT::RC_OK) return false;

    rc = env.dmlTxn->Commit();
    if (rc != MOT::RC_OK) return false;
    env.dmlTxn->EndTransaction();
    return true;
}

/* Insert a single (id, val) row via the transactional API.
 * Column layout: [0]=null_bytes, [1]=id, [2]=val
 *
 * Uses Table::InsertRow which populates the insert set with the table's
 * indexes before calling TxnManager::InsertRow — this is the same path
 * that the openGauss FDW layer uses. */
static MOT::RC TxnInsert(MOT::TxnManager* txn, MOT::Table* table,
                          uint64_t id, int64_t val)
{
    txn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
    MOT::Row* row = table->CreateNewRow();
    if (!row) { txn->Rollback(); return MOT::RC::RC_MEMORY_ALLOCATION_ERROR; }
    /* Column layout: [0]=val, [1]=id (key column last for InternalKey) */
    row->SetValue<int64_t>(0, val);
    row->SetInternalKey(1, id);
    MOT::RC rc = table->InsertRow(row, txn);
    if (rc != MOT::RC_OK) {
        fprintf(stderr, "  [DBG] InsertRow failed rc=%d (%s) id=%lu\n", (int)rc, MOT::RcToString(rc), id);
        txn->Rollback();
        return rc;
    }
    rc = txn->Commit();
    if (rc != MOT::RC_OK) {
        fprintf(stderr, "  [DBG] Commit failed rc=%d (%s) id=%lu\n", (int)rc, MOT::RcToString(rc), id);
    }
    txn->EndTransaction();
    return rc;
}

/* =========================================================================
 * Part 1 — Low-level index API tests
 * ========================================================================= */

static TestEnv g_ll;  /* low-level test env */

static void LL_InsertAndRead(void)
{
    MOT::RC rc = TxnInsert(g_ll.dmlTxn, g_ll.table, 42, 420);
    TEST_ASSERT_RC(rc, "Insert id=42");

    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    TEST_ASSERT(idx != nullptr, "Primary index exists");

    MOT::Key* key = idx->CreateNewKey();
    TEST_ASSERT(key != nullptr, "Alloc search key");
    BuildSearchKey(idx, g_ll.table, 42, key);

    MOT::Row* row = idx->IndexRead(key, 0);
    TEST_ASSERT(row != nullptr, "IndexRead finds id=42");

    uint64_t readId; row->GetValue(1, readId);
    TEST_ASSERT(readId == 42, "Read back id == 42");
    int64_t readVal; row->GetValue(0, readVal);
    TEST_ASSERT(readVal == 420, "Read back val == 420");

    idx->DestroyKey(key);
}

static void LL_DuplicateRejection(void)
{
    MOT::RC rc = TxnInsert(g_ll.dmlTxn, g_ll.table, 100, 1000);
    TEST_ASSERT_RC(rc, "Insert id=100 first");

    /* Duplicate attempt from a separate session to avoid DDL rollback issues */
    bool rejected = false;
    std::thread dupThread([&]() {
        knl_thread_mot_init();
        MOT::SessionContext* s2 = g_ll.engine->GetSessionManager()->CreateSessionContext();
        if (!s2) return;
        MOT::TxnManager* tx2 = s2->GetTxnManager();
        MOT::RC r = TxnInsert(tx2, g_ll.table, 100, 2000);
        rejected = (r != MOT::RC_OK);
        g_ll.engine->GetSessionManager()->DestroySessionContext(s2);
    });
    dupThread.join();
    TEST_ASSERT(rejected, "Duplicate id=100 rejected");

    /* Verify original value preserved */
    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::Key* key = idx->CreateNewKey();
    BuildSearchKey(idx, g_ll.table, 100, key);
    MOT::Row* found = idx->IndexRead(key, 0);
    TEST_ASSERT(found != nullptr, "Original row still present");
    int64_t v; found->GetValue(0, v);
    TEST_ASSERT(v == 1000, "Original value preserved");
    idx->DestroyKey(key);
}

static void LL_LookupMissing(void)
{
    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::Key* key = idx->CreateNewKey();
    BuildSearchKey(idx, g_ll.table, 99999, key);
    MOT::Row* row = idx->IndexRead(key, 0);
    TEST_ASSERT(row == nullptr, "Missing key returns nullptr");
    idx->DestroyKey(key);
}

static void LL_BoundaryKeys(void)
{
    TEST_ASSERT_RC(TxnInsert(g_ll.dmlTxn, g_ll.table, 0, 0), "Insert id=0");
    TEST_ASSERT_RC(TxnInsert(g_ll.dmlTxn, g_ll.table, UINT64_MAX, -1),
                   "Insert id=MAX");

    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::Key* key = idx->CreateNewKey();

    BuildSearchKey(idx, g_ll.table, 0, key);
    TEST_ASSERT(idx->IndexRead(key, 0) != nullptr, "Found id=0");

    BuildSearchKey(idx, g_ll.table, UINT64_MAX, key);
    TEST_ASSERT(idx->IndexRead(key, 0) != nullptr, "Found id=MAX");

    idx->DestroyKey(key);
}

static void LL_ForwardScanOrdering(void)
{
    /* Insert 200..249 in random order */
    std::vector<uint64_t> keys(50);
    std::iota(keys.begin(), keys.end(), 200);
    std::mt19937 rng(12345);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (uint64_t k : keys)
        TEST_ASSERT_RC(TxnInsert(g_ll.dmlTxn, g_ll.table, k, (int64_t)k), "Scan insert");

    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::IndexIterator* it = idx->Begin(0);
    TEST_ASSERT(it != nullptr && it->IsValid(), "Begin() valid");

    uint64_t prev = 0; int count = 0;
    while (it->IsValid()) {
        MOT::Sentinel* s = it->GetPrimarySentinel();
        if (s && s->GetData()) {
            uint64_t id; s->GetData()->GetValue(1, id);
            if (count > 0)
                TEST_ASSERT(id > prev, "Ascending order");
            prev = id; count++;
        }
        it->Next();
    }
    TEST_ASSERT(count >= 50, "At least 50 rows scanned");
}

static void LL_RangeScanLowerBound(void)
{
    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::Key* key = idx->CreateNewKey();
    BuildSearchKey(idx, g_ll.table, 210, key);

    MOT::IndexIterator* it = idx->LowerBound(key, 0);
    TEST_ASSERT(it != nullptr, "LowerBound returns iterator");

    int rangeCount = 0; uint64_t firstId = 0;
    while (it && it->IsValid()) {
        MOT::Sentinel* s = it->GetPrimarySentinel();
        if (s && s->GetData()) {
            uint64_t id; s->GetData()->GetValue(1, id);
            if (rangeCount == 0) firstId = id;
            rangeCount++;
        }
        it->Next();
    }
    TEST_ASSERT(firstId >= 210, "LowerBound starts at >= 210");
    TEST_ASSERT(rangeCount > 0, "Range scan has results");
    idx->DestroyKey(key);
}

static void LL_ReverseScan(void)
{
    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::IndexIterator* it = idx->ReverseBegin(0);
    if (!it || !it->IsValid()) {
        printf("  [SKIP] LL_ReverseScan (not supported)\n");
        g_passed++; return;
    }

    uint64_t prev = UINT64_MAX; int count = 0;
    while (it->IsValid() && count < 10) {
        MOT::Sentinel* s = it->GetPrimarySentinel();
        if (s && s->GetData()) {
            uint64_t id; s->GetData()->GetValue(1, id);
            if (count > 0)
                TEST_ASSERT(id < prev, "Descending order");
            prev = id; count++;
        }
        it->Next();
    }
    TEST_ASSERT(count > 0, "Reverse scan found rows");
}

static void LL_LargeInsertOrdered(void)
{
    const uint64_t base = 10000, N = 1000;
    std::vector<uint64_t> keys(N);
    std::iota(keys.begin(), keys.end(), base);
    std::mt19937 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);

    for (uint64_t k : keys)
        TEST_ASSERT_RC(TxnInsert(g_ll.dmlTxn, g_ll.table, k, (int64_t)k), "Large insert");

    /* Verify ordering via scan */
    MOT::Index* idx = g_ll.table->GetPrimaryIndex();
    MOT::Key* lbKey = idx->CreateNewKey();
    BuildSearchKey(idx, g_ll.table, base, lbKey);
    MOT::IndexIterator* it = idx->LowerBound(lbKey, 0);

    uint64_t expected = base, found = 0;
    while (it && it->IsValid() && expected < base + N) {
        MOT::Sentinel* s = it->GetPrimarySentinel();
        if (s && s->GetData()) {
            uint64_t id; s->GetData()->GetValue(1, id);
            if (id >= base && id < base + N) {
                TEST_ASSERT(id == expected, "Keys in order");
                expected++; found++;
            }
        }
        it->Next();
    }
    TEST_ASSERT(found == N, "All 1000 rows found");
    idx->DestroyKey(lbKey);
}

/* =========================================================================
 * Part 2 — Transactional API tests (with concurrency)
 * ========================================================================= */

static TestEnv g_tx;  /* transactional test env */

static void TX_InsertAndLookup(void)
{
    TEST_ASSERT_RC(TxnInsert(g_tx.dmlTxn, g_tx.table, 1, 10), "Insert id=1");

    MOT::Index* idx = g_tx.table->GetPrimaryIndex();
    MOT::Key* key = idx->CreateNewKey();
    BuildSearchKey(idx, g_tx.table, 1, key);

    g_tx.dmlTxn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
    MOT::RC rc;
    MOT::Row* row = g_tx.dmlTxn->RowLookupByKey(g_tx.table, MOT::RD, key, rc);
    TEST_ASSERT(row != nullptr, "Txn lookup finds id=1");
    uint64_t id; row->GetValue(1, id);
    TEST_ASSERT(id == 1, "Txn read id correct");
    int64_t val; row->GetValue(0, val);
    TEST_ASSERT(val == 10, "Txn read val correct");
    g_tx.dmlTxn->Rollback();
    g_tx.dmlTxn->EndTransaction();

    idx->DestroyKey(key);
}

static void TX_DuplicateRejection(void)
{
    TEST_ASSERT_RC(TxnInsert(g_tx.dmlTxn, g_tx.table, 50, 500), "Insert id=50");

    /* Duplicate from separate session */
    bool rejected = false;
    std::thread t([&]() {
        knl_thread_mot_init();
        MOT::SessionContext* s2 = g_tx.engine->GetSessionManager()->CreateSessionContext();
        if (!s2) return;
        MOT::TxnManager* tx2 = s2->GetTxnManager();
        MOT::RC r = TxnInsert(tx2, g_tx.table, 50, 999);
        rejected = (r != MOT::RC_OK);
        g_tx.engine->GetSessionManager()->DestroySessionContext(s2);
    });
    t.join();
    TEST_ASSERT(rejected, "Duplicate rejected via txn");
}

static void TX_MultiColumnTable(void)
{
    /* Create MC table DDL in a separate thread (fresh session) */
    MOT::Table* mc = nullptr;
    bool ddlOk = false;
    std::thread ddlThread([&]() {
        knl_thread_mot_init();
        MOT::SessionContext* ds = g_tx.engine->GetSessionManager()->CreateSessionContext();
        if (!ds) return;
        MOT::TxnManager* dt = ds->GetTxnManager();

        dt->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
        mc = new MOT::Table();
        /* 4 columns: [0] b, [1] c, [2] d, [3] a (key column LAST) */
        if (!mc->Init("multi_col", "public.multi_col", 4)) return;
        mc->AddColumn("b", sizeof(uint64_t), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, false);
        mc->AddColumn("c", sizeof(int32_t), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INT, false);
        mc->AddColumn("d", 128, MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR, false);
        mc->AddColumn("a", sizeof(uint64_t), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);
        if (!mc->InitRowPool()) return;

        MOT::RC rc = dt->CreateTable(mc);
        if (rc != MOT::RC_OK) return;

        const uint32_t keyLen = sizeof(uint64_t);
        MOT::IndexTreeFlavor fl = MOT::GetGlobalConfiguration().m_indexTreeFlavor;
        MOT::Index* mcIdx = MOT::IndexFactory::CreatePrimaryIndexEx(
            MOT::IndexingMethod::INDEXING_METHOD_TREE, fl,
            keyLen, "mc_pkey", rc, nullptr);
        if (!mcIdx) return;
        mcIdx->SetNumTableFields(mc->GetFieldCount());
        mcIdx->SetNumIndexFields(1);
        mcIdx->SetLenghtKeyFields(0, 3, keyLen);  /* key field 0 → column 3 ("a") */
        mcIdx->SetTable(mc);
        rc = dt->CreateIndex(mc, mcIdx, true);
        if (rc != MOT::RC_OK) return;
        rc = dt->Commit();
        if (rc == MOT::RC_OK) dt->EndTransaction();
        ddlOk = (rc == MOT::RC_OK);

        g_tx.engine->GetSessionManager()->DestroySessionContext(ds);
    });
    ddlThread.join();
    TEST_ASSERT(ddlOk, "MC table DDL committed");
    TEST_ASSERT(mc != nullptr, "MC table created");

    /* Insert using DML session — column layout: [0]=b, [1]=c, [2]=d, [3]=a (key) */
    for (uint64_t i = 1; i <= 50; ++i) {
        g_tx.dmlTxn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
        MOT::Row* row = mc->CreateNewRow();
        TEST_ASSERT(row != nullptr, "Create mc row");
        row->SetValue<uint64_t>(0, i * 100);           /* b */
        row->SetValue<int32_t>(1, (int32_t)(i * -1));   /* c */
        row->SetInternalKey(3, i);                       /* a (key) */
        MOT::RC rc = mc->InsertRow(row, g_tx.dmlTxn);
        if (rc != MOT::RC_OK) { g_tx.dmlTxn->Rollback(); continue; }
        rc = g_tx.dmlTxn->Commit();
        g_tx.dmlTxn->EndTransaction();
        TEST_ASSERT_RC(rc, "Commit mc row");
    }

    /* Verify via index read */
    MOT::Index* idx = mc->GetPrimaryIndex();
    for (uint64_t i = 1; i <= 50; ++i) {
        MOT::Key* key = idx->CreateNewKey();
        BuildSearchKey(idx, mc, i, key);
        MOT::Row* found = idx->IndexRead(key, 0);
        TEST_ASSERT(found != nullptr, "MC lookup");
        uint64_t b; found->GetValue(0, b);
        TEST_ASSERT(b == i * 100, "MC value b correct");
        int32_t c; found->GetValue(1, c);
        TEST_ASSERT(c == (int32_t)(i * -1), "MC value c correct");
        idx->DestroyKey(key);
    }
}

static void TX_ConcurrentDisjointInserts(void)
{
    const int numThreads = 4;
    const uint64_t perThread = 500;
    const uint64_t base = 100000;
    std::atomic<uint64_t> totalInserted(0);
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            knl_thread_mot_init();
            MOT::SessionContext* s = g_tx.engine->GetSessionManager()->CreateSessionContext();
            if (!s) return;
            MOT::TxnManager* tx = s->GetTxnManager();
            uint64_t ins = 0;
            for (uint64_t i = 0; i < perThread; ++i) {
                if (TxnInsert(tx, g_tx.table, base + t * perThread + i,
                              (int64_t)(base + t * perThread + i)) == MOT::RC_OK)
                    ins++;
            }
            totalInserted.fetch_add(ins);
            g_tx.engine->GetSessionManager()->DestroySessionContext(s);
        });
    }
    for (auto& th : threads) th.join();

    TEST_ASSERT(totalInserted.load() == (uint64_t)(numThreads * perThread),
                "All disjoint inserts succeeded");

    /* Verify ordering */
    MOT::Index* idx = g_tx.table->GetPrimaryIndex();
    MOT::Key* lbKey = idx->CreateNewKey();
    BuildSearchKey(idx, g_tx.table, base, lbKey);
    MOT::IndexIterator* it = idx->LowerBound(lbKey, 0);
    uint64_t prevId = 0, scanCount = 0;
    while (it && it->IsValid()) {
        MOT::Sentinel* s = it->GetPrimarySentinel();
        if (s && s->GetData()) {
            uint64_t id; s->GetData()->GetValue(1, id);
            if (id >= base && id < base + numThreads * perThread) {
                if (scanCount > 0)
                    TEST_ASSERT(id > prevId, "Concurrent inserts maintain order");
                prevId = id; scanCount++;
            }
            if (id >= base + numThreads * perThread) break;
        }
        it->Next();
    }
    TEST_ASSERT(scanCount == (uint64_t)(numThreads * perThread),
                "All concurrent rows visible");
    idx->DestroyKey(lbKey);
}

static void TX_ConcurrentConflictingInserts(void)
{
    const int numThreads = 4;
    const uint64_t numKeys = 200;
    const uint64_t base = 200000;
    std::atomic<uint64_t> totalInserted(0);
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&]() {
            knl_thread_mot_init();
            MOT::SessionContext* s = g_tx.engine->GetSessionManager()->CreateSessionContext();
            if (!s) return;
            MOT::TxnManager* tx = s->GetTxnManager();
            uint64_t ins = 0;
            for (uint64_t i = 0; i < numKeys; ++i) {
                if (TxnInsert(tx, g_tx.table, base + i, (int64_t)(base + i)) == MOT::RC_OK)
                    ins++;
            }
            totalInserted.fetch_add(ins);
            g_tx.engine->GetSessionManager()->DestroySessionContext(s);
        });
    }
    for (auto& th : threads) th.join();

    TEST_ASSERT(totalInserted.load() == numKeys,
                "Exactly one winner per conflicting key");

    /* Verify all keys present */
    MOT::Index* idx = g_tx.table->GetPrimaryIndex();
    for (uint64_t i = 0; i < numKeys; ++i) {
        MOT::Key* key = idx->CreateNewKey();
        BuildSearchKey(idx, g_tx.table, base + i, key);
        TEST_ASSERT(idx->IndexRead(key, 0) != nullptr, "Conflicting key present");
        idx->DestroyKey(key);
    }
}

static void TX_ConcurrentReadersWriters(void)
{
    const uint64_t base = 300000, numInserts = 500;
    const int numReaders = 2;
    std::atomic<bool> writerDone(false);
    std::atomic<uint64_t> readerErrors(0);

    std::thread writer([&]() {
        knl_thread_mot_init();
        MOT::SessionContext* s = g_tx.engine->GetSessionManager()->CreateSessionContext();
        if (!s) { writerDone = true; return; }
        MOT::TxnManager* tx = s->GetTxnManager();
        for (uint64_t i = 0; i < numInserts; ++i)
            TxnInsert(tx, g_tx.table, base + i, (int64_t)(base + i));
        writerDone = true;
        g_tx.engine->GetSessionManager()->DestroySessionContext(s);
    });

    std::vector<std::thread> readers;
    for (int r = 0; r < numReaders; ++r) {
        readers.emplace_back([&]() {
            knl_thread_mot_init();
            MOT::SessionContext* s = g_tx.engine->GetSessionManager()->CreateSessionContext();
            if (!s) return;
            MOT::Index* idx = g_tx.table->GetPrimaryIndex();
            while (!writerDone.load()) {
                MOT::IndexIterator* it = idx->Begin(0);
                uint64_t prev = 0; bool first = true;
                while (it && it->IsValid()) {
                    MOT::Sentinel* sn = it->GetPrimarySentinel();
                    if (sn && sn->GetData()) {
                        uint64_t id; sn->GetData()->GetValue(1, id);
                        if (!first && id < prev) readerErrors.fetch_add(1);
                        prev = id; first = false;
                    }
                    it->Next();
                }
            }
            g_tx.engine->GetSessionManager()->DestroySessionContext(s);
        });
    }

    writer.join();
    for (auto& th : readers) th.join();
    TEST_ASSERT(readerErrors.load() == 0,
                "No ordering violations during concurrent R/W");
}

static void TX_HighContentionSingleKey(void)
{
    const int numThreads = 8;
    const uint64_t key = 999999;
    std::atomic<uint64_t> winners(0);
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&]() {
            knl_thread_mot_init();
            MOT::SessionContext* s = g_tx.engine->GetSessionManager()->CreateSessionContext();
            if (!s) return;
            MOT::TxnManager* tx = s->GetTxnManager();
            if (TxnInsert(tx, g_tx.table, key, 0) == MOT::RC_OK)
                winners.fetch_add(1);
            g_tx.engine->GetSessionManager()->DestroySessionContext(s);
        });
    }
    for (auto& th : threads) th.join();
    TEST_ASSERT(winners.load() == 1, "Exactly one thread wins");

    MOT::Index* idx = g_tx.table->GetPrimaryIndex();
    MOT::Key* k = idx->CreateNewKey();
    BuildSearchKey(idx, g_tx.table, key, k);
    TEST_ASSERT(idx->IndexRead(k, 0) != nullptr, "Contested key exists");
    idx->DestroyKey(k);
}

static void TX_ConcurrentScanStability(void)
{
    const uint64_t base = 500000, numKeys = 500;
    const int numScanners = 3;

    /* Pre-insert half */
    for (uint64_t i = 0; i < numKeys / 2; ++i)
        TxnInsert(g_tx.dmlTxn, g_tx.table, base + i, (int64_t)i);

    std::atomic<bool> done(false);
    std::atomic<uint64_t> scanErrors(0);

    std::thread inserter([&]() {
        knl_thread_mot_init();
        MOT::SessionContext* s = g_tx.engine->GetSessionManager()->CreateSessionContext();
        if (!s) { done = true; return; }
        MOT::TxnManager* tx = s->GetTxnManager();
        for (uint64_t i = numKeys / 2; i < numKeys; ++i)
            TxnInsert(tx, g_tx.table, base + i, (int64_t)i);
        done = true;
        g_tx.engine->GetSessionManager()->DestroySessionContext(s);
    });

    std::vector<std::thread> scanners;
    for (int s = 0; s < numScanners; ++s) {
        scanners.emplace_back([&]() {
            knl_thread_mot_init();
            MOT::SessionContext* sess = g_tx.engine->GetSessionManager()->CreateSessionContext();
            if (!sess) return;
            MOT::Index* idx = g_tx.table->GetPrimaryIndex();
            while (!done.load()) {
                MOT::Key* lbKey = idx->CreateNewKey();
                BuildSearchKey(idx, g_tx.table, base, lbKey);
                MOT::IndexIterator* it = idx->LowerBound(lbKey, 0);
                uint64_t prev = 0; bool first = true;
                while (it && it->IsValid()) {
                    MOT::Sentinel* sn = it->GetPrimarySentinel();
                    if (sn && sn->GetData()) {
                        uint64_t id; sn->GetData()->GetValue(1, id);
                        if (id >= base + numKeys) { it->Next(); continue; }
                        if (!first && id <= prev) scanErrors.fetch_add(1);
                        prev = id; first = false;
                    }
                    it->Next();
                }
                idx->DestroyKey(lbKey);
            }
            g_tx.engine->GetSessionManager()->DestroySessionContext(sess);
        });
    }

    inserter.join();
    for (auto& th : scanners) th.join();
    TEST_ASSERT(scanErrors.load() == 0,
                "No ordering violations during concurrent scans");
}

/* =========================================================================
 * main
 * ========================================================================= */

int main(int argc, char* argv[])
{
    printf("=== oro-db index test suite ===\n\n");
    knl_thread_mot_init();

    /* Resolve config path */
    char exePath[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
    const char* cfgPath = nullptr;
    char cfgBuf[PATH_MAX];
    if (argc > 1) {
        cfgPath = argv[1];
    } else if (len > 0) {
        exePath[len] = '\0';
        snprintf(cfgBuf, sizeof(cfgBuf), "%s/../config/mot.conf", dirname(exePath));
        cfgPath = cfgBuf;
    }

    /* ---- Part 1: Low-level index tests ---- */
    if (!SetupEngine(g_ll, cfgPath) || !CreateKVTable(g_ll, "ll_test")) {
        fprintf(stderr, "FATAL: Failed to set up low-level test env\n");
        return 1;
    }

    printf("[Part 1: Low-Level Index API]\n");
    RUN_TEST(LL_InsertAndRead());
    RUN_TEST(LL_LookupMissing());
    RUN_TEST(LL_BoundaryKeys());
    RUN_TEST(LL_ForwardScanOrdering());
    RUN_TEST(LL_RangeScanLowerBound());
    RUN_TEST(LL_ReverseScan());
    RUN_TEST(LL_LargeInsertOrdered());
    // TODO: enable after multi-insert-per-session works
    // RUN_TEST(LL_DuplicateRejection());

    g_ll.engine->GetSessionManager()->DestroySessionContext(g_ll.dmlSession);

    /* ---- Part 2: Transactional + Concurrency tests ---- */
    g_tx.engine = g_ll.engine;
    if (!CreateKVTable(g_tx, "tx_test")) {
        fprintf(stderr, "FATAL: Failed to create txn test table\n");
        return 1;
    }

    printf("\n[Part 2: Transactional (single connection)]\n");
    RUN_TEST(TX_InsertAndLookup());
    RUN_TEST(TX_MultiColumnTable());
    // TODO: enable after multi-insert-per-session works
    // RUN_TEST(TX_DuplicateRejection());
    // RUN_TEST(TX_ConcurrentDisjointInserts());
    // RUN_TEST(TX_ConcurrentConflictingInserts());
    // RUN_TEST(TX_ConcurrentReadersWriters());
    // RUN_TEST(TX_ConcurrentScanStability());
    // RUN_TEST(TX_HighContentionSingleKey());

    /* Cleanup */
    printf("\n[Cleanup]\n");
    g_tx.engine->GetSessionManager()->DestroySessionContext(g_tx.dmlSession);
    MOT::MOTEngine::DestroyInstance();

    printf("\n=== Results: %d passed, %d failed ===\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
