/*
 * oro_mot_adapter.cpp - Implementation of SQLite ↔ MOT bridge
 *
 * Stores SQLite serialized records as opaque BLOBs in MOT, keyed by rowid.
 * The MassTree primary index uses rowid (big-endian uint64) as the key.
 *
 * Internal MOT table layout (per SQLite MOT table):
 *   col 0: data    (BLOB, max 16KB) - the SQLite serialized record bytes
 *   col 1: rowid   (LONG)            - the rowid (also InternalKey)
 */

#include "oro_mot_adapter.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <endian.h>
#include <mutex>
#include <unordered_map>
#include <string>
#include <atomic>

// MOT engine headers
#include "mot_engine.h"
#include "session_context.h"
#include "session_manager.h"
#include "txn.h"
#include "table.h"
#include "row.h"
#include "index.h"
#include "index_factory.h"
#include "index_iterator.h"
#include "catalog_column_types.h"

using namespace MOT;

// =====================================================================
// Constants
// =====================================================================

// Max size for the SQLite record blob stored in MOT
static constexpr uint32_t MAX_RECORD_SIZE = 4096;

// Key length for the primary index (8 bytes uint64 + padding)
static constexpr uint16_t MOT_KEY_LEN = 8;

// MOT column indices for the internal layout
enum MotColIdx : int {
    MOT_COL_DATA = 0,    // BLOB - SQLite record bytes
    MOT_COL_ROWID = 1    // LONG - rowid (also InternalKey)
};

// =====================================================================
// Global state
// =====================================================================

static MOTEngine* g_engine = nullptr;
static std::atomic<bool> g_initialized{false};

// Thread-local MOT init flag
static thread_local bool tl_mot_initialized = false;

// Per-connection state (one per sqlite3*)
struct OroMotConn {
    SessionContext* session = nullptr;
    TxnManager*    txn      = nullptr;
    bool           in_txn   = false;
};

// Table registry: (iDb, table_name) → MOT::Table*
struct TableKey {
    int iDb;
    std::string name;
    bool operator==(const TableKey& o) const { return iDb==o.iDb && name==o.name; }
};
struct TableKeyHash {
    size_t operator()(const TableKey& k) const noexcept {
        return std::hash<std::string>()(k.name) ^ std::hash<int>()(k.iDb);
    }
};

struct OroGlobals {
    std::mutex                                                    mu;
    std::unordered_map<TableKey, Table*, TableKeyHash>            tables;
    std::unordered_map<void*, OroMotConn*>                        conns;
};

static OroGlobals& globals() {
    static OroGlobals g;
    return g;
}

// =====================================================================
// Cursor structure
// =====================================================================

struct OroMotCursor {
    Table*           table       = nullptr;
    Index*           index       = nullptr;
    OroMotConn*      conn        = nullptr;
    IndexIterator*   iter        = nullptr;
    Row*             current_row = nullptr;  // MVCC-visible row from RowLookup
    int64_t          current_rowid = 0;
    bool             at_eof      = true;
    bool             write_mode  = false;
};

// =====================================================================
// Thread context helper
// =====================================================================

static void EnsureThreadCtx(OroMotConn* conn) {
    if (!tl_mot_initialized) {
        knl_thread_mot_init();
        tl_mot_initialized = true;
    }
    if (conn && conn->session) {
        u_sess->mot_cxt.session_context = conn->session;
        u_sess->mot_cxt.txn_manager = conn->txn;
    }
}

// =====================================================================
// Engine lifecycle
// =====================================================================

extern "C" int oroMotInit(const char* config_path) {
    if (g_initialized.load()) return 0;
    g_engine = MOTEngine::CreateInstance(config_path);
    if (!g_engine) return 1;
    g_initialized.store(true);
    return 0;
}

extern "C" void oroMotShutdown(void) {
    if (!g_initialized.load()) return;
    // Clean up all connections first
    {
        auto& g = globals();
        std::lock_guard<std::mutex> lock(g.mu);
        for (auto& kv : g.conns) {
            EnsureThreadCtx(kv.second);
            if (kv.second->in_txn) {
                kv.second->txn->Rollback();
                kv.second->txn->EndTransaction();
            }
            g_engine->GetSessionManager()->DestroySessionContext(kv.second->session);
            delete kv.second;
        }
        g.conns.clear();
        g.tables.clear();
    }
    MOTEngine::DestroyInstance();
    g_engine = nullptr;
    g_initialized.store(false);
}

extern "C" int oroMotIsInit(void) {
    return g_initialized.load() ? 1 : 0;
}

// =====================================================================
// Connection management
// =====================================================================

static OroMotConn* GetOrCreateConn(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);

    auto it = g.conns.find(pDb);
    if (it != g.conns.end()) return it->second;

    EnsureThreadCtx(nullptr);
    SessionContext* sess = g_engine->GetSessionManager()->CreateSessionContext();
    if (!sess) return nullptr;

    auto* c = new OroMotConn();
    c->session = sess;
    c->txn = sess->GetTxnManager();
    g.conns[pDb] = c;

    EnsureThreadCtx(c);
    return c;
}

extern "C" int oroMotConnAttach(void* pDb) {
    return GetOrCreateConn(pDb) ? 0 : 1;
}

extern "C" int oroMotConnDetach(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);

    auto it = g.conns.find(pDb);
    if (it == g.conns.end()) return 0;

    OroMotConn* c = it->second;
    EnsureThreadCtx(c);
    if (c->in_txn) {
        c->txn->Rollback();
        c->txn->EndTransaction();
        c->in_txn = false;
    }
    g_engine->GetSessionManager()->DestroySessionContext(c->session);
    delete c;
    g.conns.erase(it);
    return 0;
}

extern "C" int oroMotBegin(void* pDb) {
    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;
    EnsureThreadCtx(c);
    if (!c->in_txn) {
        c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
        c->in_txn = true;
    }
    return 0;
}

extern "C" int oroMotCommit(void* pDb) {
    auto& g = globals();
    OroMotConn* c;
    {
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.conns.find(pDb);
        if (it == g.conns.end()) return 0;
        c = it->second;
    }
    EnsureThreadCtx(c);
    if (c->in_txn) {
        RC rc = c->txn->Commit();
        c->txn->EndTransaction();
        c->in_txn = false;
        return (rc == RC_OK) ? 0 : 1;
    }
    return 0;
}

extern "C" int oroMotRollback(void* pDb) {
    auto& g = globals();
    OroMotConn* c;
    {
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.conns.find(pDb);
        if (it == g.conns.end()) return 0;
        c = it->second;
    }
    EnsureThreadCtx(c);
    if (c->in_txn) {
        c->txn->Rollback();
        c->txn->EndTransaction();
        c->in_txn = false;
    }
    return 0;
}

extern "C" int oroMotAutoCommit(void* pDb) {
    // Same as oroMotCommit but safely no-ops if no connection or no active txn.
    auto& g = globals();
    OroMotConn* c;
    {
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.conns.find(pDb);
        if (it == g.conns.end()) return 0;
        c = it->second;
    }
    if (!c->in_txn) return 0;
    EnsureThreadCtx(c);
    RC rc = c->txn->Commit();
    c->txn->EndTransaction();
    c->in_txn = false;
    return (rc == RC_OK) ? 0 : 1;
}

extern "C" int oroMotHasActiveTxn(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    auto it = g.conns.find(pDb);
    if (it == g.conns.end()) return 0;
    return it->second->in_txn ? 1 : 0;
}

// =====================================================================
// Table registry
// =====================================================================

extern "C" int oroMotTableCreate(int iDb, const char* table_name) {
    if (!g_initialized.load() || !table_name) return 1;

    auto& g = globals();
    {
        std::lock_guard<std::mutex> lock(g.mu);
        TableKey key{iDb, table_name};
        if (g.tables.count(key)) return 0;  // already exists
    }

    // We need a session/transaction to create the table. Use a temporary
    // session bound to the current thread.
    EnsureThreadCtx(nullptr);
    SessionContext* sess = g_engine->GetSessionManager()->CreateSessionContext();
    if (!sess) return 1;
    TxnManager* txn = sess->GetTxnManager();

    // Build a unique internal MOT table name (avoid collisions across iDb)
    char name_buf[128];
    snprintf(name_buf, sizeof(name_buf), "mot_%d_%s", iDb, table_name);
    std::string long_name = std::string("public.") + name_buf;

    Table* t = new Table();
    if (!t->Init(name_buf, long_name.c_str(), 2)) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // col 0: data BLOB (variable, up to MAX_RECORD_SIZE)
    t->AddColumn("data", MAX_RECORD_SIZE, MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB, false);
    // col 1: rowid LONG (also serves as the InternalKey)
    t->AddColumn("rowid", sizeof(uint64_t), MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);

    if (!t->InitRowPool() || !t->InitTombStonePool()) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    txn->StartTransaction(txn->GetTransactionId(), READ_COMMITED);

    RC rc = txn->CreateTable(t);
    if (rc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // Primary index on rowid (col 1)
    RC irc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY,
        IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR,
        true,  // unique
        MOT_KEY_LEN,
        std::string("ix_") + name_buf + "_pk",
        irc, nullptr);

    if (!ix || irc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    ix->SetNumTableFields(t->GetFieldCount());
    ix->SetNumIndexFields(1);
    ix->SetLenghtKeyFields(0, MOT_COL_ROWID, MOT_KEY_LEN);
    ix->SetTable(t);

    rc = txn->CreateIndex(t, ix, true);
    if (rc != RC_OK) {
        delete ix;
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    rc = txn->Commit();
    txn->EndTransaction();
    g_engine->GetSessionManager()->DestroySessionContext(sess);

    if (rc != RC_OK) {
        delete t;
        return 1;
    }

    {
        std::lock_guard<std::mutex> lock(g.mu);
        g.tables[TableKey{iDb, table_name}] = t;
    }
    return 0;
}

extern "C" int oroMotTableDrop(int iDb, const char* table_name) {
    if (!table_name) return 1;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    g.tables.erase(TableKey{iDb, table_name});
    // Note: actual MOT::Table destruction would need a transaction; for now
    // we just remove from registry and let MOT engine cleanup handle it.
    return 0;
}

extern "C" int oroMotTableExists(int iDb, const char* table_name) {
    if (!table_name) return 0;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    return g.tables.count(TableKey{iDb, table_name}) ? 1 : 0;
}

static Table* LookupTable(int iDb, const char* table_name) {
    if (!table_name) return nullptr;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    auto it = g.tables.find(TableKey{iDb, table_name});
    return (it != g.tables.end()) ? it->second : nullptr;
}

// =====================================================================
// Cursor operations
// =====================================================================

extern "C" int oroMotCursorOpen(void* pDb, int iDb, const char* table_name,
                                int wrFlag, OroMotCursor** ppCursor) {
    Table* t = LookupTable(iDb, table_name);
    if (!t) return 1;

    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;

    EnsureThreadCtx(c);

    // Auto-start a transaction if none active (read transaction)
    if (!c->in_txn) {
        c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
        c->in_txn = true;
    }

    OroMotCursor* cur = new OroMotCursor();
    cur->table = t;
    cur->index = t->GetPrimaryIndex();
    cur->conn = c;
    cur->write_mode = (wrFlag != 0);
    cur->at_eof = true;

    *ppCursor = cur;
    return 0;
}

extern "C" void oroMotCursorClose(OroMotCursor* pCur) {
    if (!pCur) return;
    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }
    delete pCur;
}

// Advance iter to next MVCC-visible row, or EOF
static void CursorAdvance(OroMotCursor* cur) {
    EnsureThreadCtx(cur->conn);
    while (cur->iter && cur->iter->IsValid()) {
        Sentinel* s = cur->iter->GetPrimarySentinel();
        if (s) {
            RC rc = RC_OK;
            Row* r = cur->conn->txn->RowLookup(AccessType::RD, s, rc);
            if (rc != RC_OK) {
                cur->at_eof = true;
                return;
            }
            if (r) {
                cur->current_row = r;
                // Rowid column stores htobe64(rowid) (set via SetInternalKey for the index).
                // Convert back to native endian for the SQLite rowid.
                uint64_t rid_be;
                r->GetValue(MOT_COL_ROWID, rid_be);
                cur->current_rowid = (int64_t)be64toh(rid_be);
                cur->at_eof = false;
                return;
            }
        }
        cur->iter->Next();
    }
    cur->at_eof = true;
}

extern "C" int oroMotFirst(OroMotCursor* pCur, int* pEof) {
    EnsureThreadCtx(pCur->conn);
    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }
    pCur->iter = pCur->index->Begin(0);
    CursorAdvance(pCur);
    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}

extern "C" int oroMotLast(OroMotCursor* pCur, int* pEof) {
    // Simple implementation: not used for primary scan, can be added later
    *pEof = 1;
    return 0;
}

extern "C" int oroMotNext(OroMotCursor* pCur, int* pEof) {
    if (pCur->iter) {
        pCur->iter->Next();
        CursorAdvance(pCur);
    } else {
        pCur->at_eof = true;
    }
    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}

extern "C" int oroMotPrev(OroMotCursor* pCur, int* pEof) {
    *pEof = 1;
    return 0;
}

extern "C" int oroMotSeekRowid(OroMotCursor* pCur, int64_t rowid, int* pRes) {
    EnsureThreadCtx(pCur->conn);

    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }

    // Build the search key: htobe64(rowid)
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint64_t be_val = htobe64((uint64_t)rowid);
    key->FillValue(reinterpret_cast<const uint8_t*>(&be_val), sizeof(uint64_t), 0);

    RC rc = RC_OK;
    Row* r = pCur->conn->txn->RowLookupByKey(pCur->table, AccessType::RD, key, rc);
    pCur->index->DestroyKey(key);

    if (rc == RC_OK && r) {
        pCur->current_row = r;
        pCur->current_rowid = rowid;
        pCur->at_eof = false;
        *pRes = 0;
    } else {
        pCur->current_row = nullptr;
        pCur->at_eof = true;
        *pRes = -1;
    }
    return 0;
}

extern "C" int oroMotRowid(OroMotCursor* pCur, int64_t* pRowid) {
    if (pCur->at_eof || !pCur->current_row) return 1;
    *pRowid = pCur->current_rowid;
    return 0;
}

extern "C" int oroMotPayloadSize(OroMotCursor* pCur, uint32_t* pSize) {
    if (pCur->at_eof || !pCur->current_row) {
        *pSize = 0;
        return 1;
    }
    // First 4 bytes of the BLOB column store the actual record size
    const uint8_t* p = pCur->current_row->GetValue(MOT_COL_DATA);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    *pSize = sz;
    return 0;
}

extern "C" int oroMotRowData(OroMotCursor* pCur, uint32_t offset, uint32_t amount,
                             void* pBuf) {
    if (pCur->at_eof || !pCur->current_row) return 1;
    const uint8_t* p = pCur->current_row->GetValue(MOT_COL_DATA);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    if (offset + amount > sz) return 1;
    memcpy(pBuf, p + sizeof(uint32_t) + offset, amount);
    return 0;
}

extern "C" const void* oroMotPayloadFetch(OroMotCursor* pCur, uint32_t* pAmt) {
    if (pCur->at_eof || !pCur->current_row) {
        *pAmt = 0;
        return nullptr;
    }
    const uint8_t* p = pCur->current_row->GetValue(MOT_COL_DATA);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    *pAmt = sz;
    return p + sizeof(uint32_t);
}

extern "C" int oroMotInsert(OroMotCursor* pCur, int64_t rowid,
                            const void* pData, int nData) {
    EnsureThreadCtx(pCur->conn);

    if ((uint32_t)nData + sizeof(uint32_t) > MAX_RECORD_SIZE) {
        return 1;  // record too large
    }

    Row* row = pCur->table->CreateNewRow();
    if (!row) return 1;

    // Pack: [4-byte size][record bytes...]
    // Use SetStringValue (friend helper) to write to the column at the correct
    // offset. SetValueVariable has a known bug in MOT (writes at fieldSize
    // instead of field offset) so we avoid it.
    char buf[MAX_RECORD_SIZE];
    memset(buf, 0, sizeof(buf));
    uint32_t sz = (uint32_t)nData;
    memcpy(buf, &sz, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t), pData, nData);
    SetStringValue(row, MOT_COL_DATA, buf);

    // Set rowid column AND InternalKey (with htobe64 for MassTree byte ordering)
    row->SetValue<uint64_t>(MOT_COL_ROWID, (uint64_t)rowid);
    row->SetInternalKey(MOT_COL_ROWID, htobe64((uint64_t)rowid));

    RC rc = pCur->table->InsertRow(row, pCur->conn->txn);
    if (rc != RC_OK) {
        pCur->table->DestroyRow(row);
        return (rc == RC_UNIQUE_VIOLATION) ? 2 : 1;
    }
    return 0;
}

extern "C" int oroMotDelete(OroMotCursor* pCur) {
    EnsureThreadCtx(pCur->conn);
    if (pCur->at_eof || !pCur->current_row) return 1;

    // Need to look up via RD_FOR_UPDATE to mark for deletion
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint64_t be_val = htobe64((uint64_t)pCur->current_rowid);
    key->FillValue(reinterpret_cast<const uint8_t*>(&be_val), sizeof(uint64_t), 0);

    RC rc = RC_OK;
    Row* r = pCur->conn->txn->RowLookupByKey(pCur->table,
                                             AccessType::RD_FOR_UPDATE, key, rc);
    pCur->index->DestroyKey(key);

    if (rc != RC_OK || !r) return 1;

    rc = pCur->conn->txn->DeleteLastRow();
    return (rc == RC_OK) ? 0 : 1;
}

extern "C" int oroMotCount(OroMotCursor* pCur, int64_t* pCount) {
    EnsureThreadCtx(pCur->conn);
    int64_t n = 0;
    IndexIterator* it = pCur->index->Begin(0);
    while (it && it->IsValid()) {
        Sentinel* s = it->GetPrimarySentinel();
        if (s) {
            RC rc = RC_OK;
            Row* r = pCur->conn->txn->RowLookup(AccessType::RD, s, rc);
            if (rc == RC_OK && r) n++;
        }
        it->Next();
    }
    if (it) it->Destroy();
    *pCount = n;
    return 0;
}

extern "C" int oroMotEof(OroMotCursor* pCur) {
    return pCur->at_eof ? 1 : 0;
}
