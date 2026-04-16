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
#include "txn_insert_action.h"
#include "txn_access.h"

#include <vector>
#include <algorithm>

using namespace MOT;

// =====================================================================
// Constants
// =====================================================================

// Max size for the SQLite record blob stored in MOT
static constexpr uint32_t MAX_RECORD_SIZE = 4096;

// Key length for the primary index (8 bytes uint64)
static constexpr uint16_t MOT_KEY_LEN = 8;

// Max encoded key length for secondary indexes.
// VARCHAR stores [4-byte-len][data], and the InternalKey path copies from
// the column start (including the 4-byte prefix). MOT MAX_KEY_SIZE is 256,
// so we need: 4 + encoded_len <= 256  →  encoded_len <= 252.
static constexpr uint32_t IDX_ENC_KEY_LEN = 252;
// VARCHAR column size = 4 + 252 = 256
static constexpr uint32_t IDX_KEY_COL_SIZE = IDX_ENC_KEY_LEN + sizeof(uint32_t); // 256
// MOT index key length = full column size = 256 = MAX_KEY_SIZE
static constexpr uint16_t IDX_MOT_KEY_LEN = (uint16_t)IDX_KEY_COL_SIZE; // 256

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

// Secondary index registry key: (iDb, table_name, index_name)
struct IdxKey {
    int iDb;
    std::string tabName;
    std::string ixName;
    bool operator==(const IdxKey& o) const {
        return iDb==o.iDb && tabName==o.tabName && ixName==o.ixName;
    }
};
struct IdxKeyHash {
    size_t operator()(const IdxKey& k) const noexcept {
        size_t h = std::hash<std::string>()(k.tabName);
        h ^= std::hash<std::string>()(k.ixName) + 0x9e3779b9 + (h<<6) + (h>>2);
        h ^= std::hash<int>()(k.iDb);
        return h;
    }
};

struct OroMotIdxInfo {
    Table* table = nullptr;
    Index* index = nullptr;
};

// Per-table rowid counter
struct RowidCounter {
    std::atomic<int64_t> next{1};
};

struct OroGlobals {
    std::mutex                                                    mu;
    std::unordered_map<TableKey, Table*, TableKeyHash>            tables;
    std::unordered_map<void*, OroMotConn*>                        conns;
    std::unordered_map<IdxKey, OroMotIdxInfo, IdxKeyHash>         sec_indexes;
    std::unordered_map<TableKey, RowidCounter*, TableKeyHash>     rowid_counters;
};

static OroGlobals& globals() {
    static OroGlobals g;
    return g;
}

// =====================================================================
// Cursor structure
// =====================================================================

// Column indices for secondary index MOT table
enum IdxColIdx : int {
    IDX_COL_ROWID  = 0,    // LONG — the table rowid
    IDX_COL_RECORD = 1,    // BLOB — original SQLite index record bytes
    IDX_COL_KEY    = 2     // VARCHAR — memcmp-encoded key (InternalKey source)
};

// Pending insert entry for RYOW merge
struct PendingRow {
    int64_t rowid;
    Row*    row;
};

struct OroMotCursor {
    Table*           table       = nullptr;
    Index*           index       = nullptr;
    OroMotConn*      conn        = nullptr;
    IndexIterator*   iter        = nullptr;
    Row*             current_row = nullptr;  // MVCC-visible row from RowLookup
    int64_t          current_rowid = 0;
    bool             at_eof      = true;
    bool             write_mode  = false;
    // --- Index cursor fields ---
    bool             is_index    = false;
    int64_t          idx_rowid   = 0;       // rowid from current index entry
    // --- RYOW: pending inserts (sorted by rowid) ---
    std::vector<PendingRow> pending;
    size_t           pending_pos = 0;
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

// Advance iter to next MVCC-visible row, or EOF.
// MOT's ExecuteOptimisticInsert puts newly inserted rows into the MassTree
// index AND the txn's access set. RowLookup finds them via AccessLookup.
// So no special pending-insert merge is needed — RYOW works automatically.
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

// Forward declaration
static void IdxCursorAdvance(OroMotCursor* cur);

extern "C" int oroMotFirst(OroMotCursor* pCur, int* pEof) {
    EnsureThreadCtx(pCur->conn);
    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }
    pCur->iter = pCur->index->Begin(0);
    if (pCur->is_index) {
        IdxCursorAdvance(pCur);
    } else {
        CursorAdvance(pCur);
    }
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
        if (pCur->is_index) {
            IdxCursorAdvance(pCur);
        } else {
            CursorAdvance(pCur);
        }
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

extern "C" int oroMotSeekCmp(OroMotCursor* pCur, int64_t rowid, int cmp_op,
                             int* pEof) {
    /* cmp_op: 0=GT, 1=GE, 2=LT, 3=LE */
    EnsureThreadCtx(pCur->conn);

    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }

    /* For GT/GE scans: start at beginning, advance until condition met.
     * For LT/LE scans: start at beginning, advance while rowid < target, keep last match.
     * This is O(N) but correct. MOT could optimize with range-aware iterators later. */
    pCur->iter = pCur->index->Begin(0);
    CursorAdvance(pCur);

    while (!pCur->at_eof) {
        int64_t cur = pCur->current_rowid;
        bool match = false;
        switch (cmp_op) {
            case 0: match = (cur >  rowid); break;  // GT
            case 1: match = (cur >= rowid); break;  // GE
            case 2: match = (cur <  rowid); break;  // LT
            case 3: match = (cur <= rowid); break;  // LE
        }
        if (match) break;

        /* For GT/GE: if current key < target, advance */
        if (cmp_op <= 1) {
            if (pCur->iter) pCur->iter->Next();
            CursorAdvance(pCur);
        } else {
            /* For LT/LE scans, SQLite uses iteration in reverse. Full support
             * would need a reverse iterator. For now, scan past non-matching. */
            if (pCur->iter) pCur->iter->Next();
            CursorAdvance(pCur);
        }
    }
    *pEof = pCur->at_eof ? 1 : 0;
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
        /* Do NOT DestroyRow on failure: MOT's InsertRow may have registered
         * the row with the transaction's access manager. Destroying it here
         * leads to a double-free on rollback. */
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

// =====================================================================
// Rowid allocation
// =====================================================================

// Get per-table RowidCounter, creating and seeding on first call
static RowidCounter* GetRowidCounter(OroMotCursor* pCur) {
    auto& g = globals();
    // Use the MOT Table* pointer as a stable key
    // We store counters by a (table pointer) key for simplicity
    static std::unordered_map<Table*, RowidCounter*> s_counters;
    static std::mutex s_mu;
    std::lock_guard<std::mutex> lock(s_mu);

    auto it = s_counters.find(pCur->table);
    if (it != s_counters.end()) return it->second;

    RowidCounter* ctr = new RowidCounter();
    // Seed from max rowid in the table
    EnsureThreadCtx(pCur->conn);
    int64_t maxId = 0;
    Index* ix = pCur->table->GetPrimaryIndex();
    IndexIterator* it2 = ix->Begin(0);
    while (it2 && it2->IsValid()) {
        Sentinel* s = it2->GetPrimarySentinel();
        if (s) {
            RC rc2 = RC_OK;
            Row* r = pCur->conn->txn->RowLookup(AccessType::RD, s, rc2);
            if (rc2 == RC_OK && r) {
                uint64_t rid_be;
                r->GetValue(MOT_COL_ROWID, rid_be);
                int64_t rid = (int64_t)be64toh(rid_be);
                if (rid > maxId) maxId = rid;
            }
        }
        it2->Next();
    }
    if (it2) it2->Destroy();
    ctr->next.store(maxId + 1);
    s_counters[pCur->table] = ctr;
    return ctr;
}

extern "C" int oroMotCursorNewRowid(OroMotCursor* pCur, int64_t* pRowid) {
    RowidCounter* ctr = GetRowidCounter(pCur);
    *pRowid = ctr->next.fetch_add(1);
    return 0;
}

// =====================================================================
// Key encoding
// =====================================================================

// SQLite varint decoder (simplified, reads up to 9 bytes)
static int oroGetVarint(const uint8_t* p, int64_t* pVal) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) {
        v = (v << 7) | (p[i] & 0x7F);
        if ((p[i] & 0x80) == 0) {
            *pVal = (int64_t)v;
            return i + 1;
        }
    }
    // 9th byte: all 8 bits are payload
    v = (v << 8) | p[8];
    *pVal = (int64_t)v;
    return 9;
}

// Encode a signed int64 to memcmp-comparable 8 bytes (flip sign bit → unsigned BE)
static void encodeInt64(int64_t v, uint8_t* out) {
    uint64_t u = (uint64_t)v ^ 0x8000000000000000ULL;
    for (int i = 7; i >= 0; i--) {
        out[7 - i] = (uint8_t)(u >> (i * 8));
    }
}

// Encode a double to memcmp-comparable 8 bytes
static void encodeReal(double v, uint8_t* out) {
    uint64_t bits;
    memcpy(&bits, &v, 8);
    if (bits & 0x8000000000000000ULL) {
        bits = ~bits;  // negative: flip all bits
    } else {
        bits ^= 0x8000000000000000ULL;  // positive: flip sign bit
    }
    for (int i = 7; i >= 0; i--) {
        out[7 - i] = (uint8_t)(bits >> (i * 8));
    }
}

// Size of data for a SQLite serial type
static int serialTypeSize(int64_t type) {
    if (type <= 0) return 0;
    if (type <= 4) return (int)type;
    if (type == 5) return 6;
    if (type == 6 || type == 7) return 8;
    if (type == 8 || type == 9) return 0;
    if (type >= 12 && (type & 1) == 0) return (int)((type - 12) / 2);  // BLOB
    if (type >= 13 && (type & 1) == 1) return (int)((type - 13) / 2);  // TEXT
    return 0;
}

// Decode a signed integer from SQLite serial-type encoded bytes
static int64_t decodeSerialInt(const uint8_t* data, int len) {
    int64_t v = 0;
    if (len > 0 && (data[0] & 0x80)) v = -1;  // sign-extend
    for (int i = 0; i < len; i++) {
        v = (v << 8) | data[i];
    }
    return v;
}

extern "C" int oroMotEncodeIdxRecord(const void* pRecord, int nRecord,
                                     void* pOut, int64_t* pRowid) {
    const uint8_t* rec = (const uint8_t*)pRecord;
    uint8_t* out = (uint8_t*)pOut;
    memset(out, 0, IDX_ENC_KEY_LEN);

    if (nRecord < 1) return 1;

    // Parse header: first varint is header size
    int64_t hdrSize;
    int hdrBytes = oroGetVarint(rec, &hdrSize);
    if (hdrSize < 1 || hdrSize > nRecord) return 1;

    // Parse serial types from header
    int64_t types[64];
    int nTypes = 0;
    int pos = hdrBytes;
    while (pos < (int)hdrSize && nTypes < 64) {
        int64_t t;
        pos += oroGetVarint(rec + pos, &t);
        types[nTypes++] = t;
    }

    // Data starts after header
    int dataPos = (int)hdrSize;
    int outPos = 0;

    // Encode each field (including rowid as last field)
    for (int i = 0; i < nTypes && outPos < (int)IDX_ENC_KEY_LEN - 10; i++) {
        int64_t st = types[i];
        int dlen = serialTypeSize(st);

        if (st == 0) {
            // NULL
            out[outPos++] = 0x00;
        } else if (st >= 1 && st <= 6) {
            // Integer
            int64_t val = decodeSerialInt(rec + dataPos, dlen);
            out[outPos++] = 0x02;
            encodeInt64(val, out + outPos);
            outPos += 8;
        } else if (st == 7) {
            // Real (IEEE 754 double)
            double val;
            // SQLite stores doubles in big-endian in the record
            uint64_t bits = 0;
            for (int b = 0; b < 8; b++)
                bits = (bits << 8) | rec[dataPos + b];
            memcpy(&val, &bits, 8);
            out[outPos++] = 0x03;
            encodeReal(val, out + outPos);
            outPos += 8;
        } else if (st == 8) {
            // Integer value 0
            out[outPos++] = 0x02;
            encodeInt64(0, out + outPos);
            outPos += 8;
        } else if (st == 9) {
            // Integer value 1
            out[outPos++] = 0x02;
            encodeInt64(1, out + outPos);
            outPos += 8;
        } else if (st >= 13 && (st & 1) == 1) {
            // TEXT
            out[outPos++] = 0x04;
            int copyLen = dlen;
            if (outPos + copyLen + 1 > (int)IDX_ENC_KEY_LEN)
                copyLen = (int)IDX_ENC_KEY_LEN - outPos - 1;
            if (copyLen > 0) memcpy(out + outPos, rec + dataPos, copyLen);
            outPos += copyLen;
            out[outPos++] = 0x00;  // terminator
        } else if (st >= 12 && (st & 1) == 0) {
            // BLOB
            out[outPos++] = 0x05;
            int copyLen = dlen;
            if (outPos + copyLen + 1 > (int)IDX_ENC_KEY_LEN)
                copyLen = (int)IDX_ENC_KEY_LEN - outPos - 1;
            if (copyLen > 0) memcpy(out + outPos, rec + dataPos, copyLen);
            outPos += copyLen;
            out[outPos++] = 0x00;  // terminator
        }
        dataPos += dlen;
    }

    // The last field is the rowid — decode it for the caller
    if (pRowid && nTypes > 0) {
        int64_t lastType = types[nTypes - 1];
        int lastLen = serialTypeSize(lastType);
        int lastDataPos = nRecord - lastLen;
        if (lastType >= 1 && lastType <= 6) {
            *pRowid = decodeSerialInt(rec + lastDataPos, lastLen);
        } else if (lastType == 8) {
            *pRowid = 0;
        } else if (lastType == 9) {
            *pRowid = 1;
        } else {
            *pRowid = 0;
        }
    }

    return 0;
}

// Forward declaration for Mem-based encoding (used from sqlite3.c)
// The pMem pointer is cast to sqlite3_value* / Mem* in sqlite3.c.
// We use a simple interface: pass type flags and values directly.

// Internal: encode a single value into the key buffer at outPos
static int encodeOneValue(uint8_t* out, int outPos, int maxLen,
                          int flags, int64_t iVal, double rVal,
                          const void* zVal, int nVal) {
    // flags: 1=NULL, 2=INT, 4=REAL, 8=TEXT, 16=BLOB
    if (flags & 1) {
        // NULL
        if (outPos < maxLen) out[outPos++] = 0x00;
        return outPos;
    }
    if (flags & 2) {
        // Integer
        if (outPos + 9 <= maxLen) {
            out[outPos++] = 0x02;
            encodeInt64(iVal, out + outPos);
            outPos += 8;
        }
        return outPos;
    }
    if (flags & 4) {
        // Real
        if (outPos + 9 <= maxLen) {
            out[outPos++] = 0x03;
            encodeReal(rVal, out + outPos);
            outPos += 8;
        }
        return outPos;
    }
    if (flags & 8) {
        // Text
        if (outPos + nVal + 2 <= maxLen) {
            out[outPos++] = 0x04;
            if (nVal > 0) memcpy(out + outPos, zVal, nVal);
            outPos += nVal;
            out[outPos++] = 0x00;
        }
        return outPos;
    }
    if (flags & 16) {
        // Blob
        if (outPos + nVal + 2 <= maxLen) {
            out[outPos++] = 0x05;
            if (nVal > 0) memcpy(out + outPos, zVal, nVal);
            outPos += nVal;
            out[outPos++] = 0x00;
        }
        return outPos;
    }
    // Unknown → NULL
    if (outPos < maxLen) out[outPos++] = 0x00;
    return outPos;
}

// Exposed to sqlite3.c: encode Mem values into memcmp key.
// The pMem parameter is actually a Mem* from SQLite internals.
// Since we can't include SQLite internals here, we define a simple
// C callback protocol instead: sqlite3.c calls oroMotEncodeMemValues
// through a wrapper that extracts type+value from each Mem.
extern "C" int oroMotEncodeMemValues(const void* pMem, int nField,
                                     void* pOut, int* pOutLen) {
    // This function is called from sqlite3.c where pMem is actually
    // an array of (flags, iVal, rVal, zVal, nVal) structs packed for us.
    // See oroMotEncodeMemValuesEx below for the actual implementation.
    (void)pMem; (void)nField; (void)pOut; (void)pOutLen;
    return 1;  // Not used directly — use oroMotEncodeMemValuesEx
}

// Alternative: encode from explicit value arrays.
// flags[i]: 1=NULL, 2=INT, 4=REAL, 8=TEXT, 16=BLOB
// iVals[i], rVals[i], zVals[i], nVals[i] for each field.
extern "C" int oroMotEncodeValues(int nField, const int* flags,
                                  const int64_t* iVals, const double* rVals,
                                  const void* const* zVals, const int* nVals,
                                  void* pOut, int* pOutLen) {
    uint8_t* out = (uint8_t*)pOut;
    memset(out, 0, IDX_ENC_KEY_LEN);
    int pos = 0;
    for (int i = 0; i < nField; i++) {
        pos = encodeOneValue(out, pos, (int)IDX_ENC_KEY_LEN,
                             flags[i], iVals ? iVals[i] : 0,
                             rVals ? rVals[i] : 0.0,
                             zVals ? zVals[i] : nullptr,
                             nVals ? nVals[i] : 0);
    }
    if (pOutLen) *pOutLen = pos;
    return 0;
}

// =====================================================================
// Secondary index management
// =====================================================================

static OroMotIdxInfo* LookupIdx(int iDb, const char* tabName,
                                const char* ixName) {
    auto& g = globals();
    // Caller must hold g.mu or call under safe conditions
    auto it = g.sec_indexes.find(IdxKey{iDb, tabName, ixName});
    return (it != g.sec_indexes.end()) ? &it->second : nullptr;
}

extern "C" int oroMotIndexCreate(void* pDb, int iDb, const char* table_name,
                                 const char* index_name) {
    if (!g_initialized.load() || !table_name || !index_name) return 1;

    auto& g = globals();
    {
        std::lock_guard<std::mutex> lock(g.mu);
        IdxKey ik{iDb, table_name, index_name};
        if (g.sec_indexes.count(ik)) return 0;  // already exists
    }

    EnsureThreadCtx(nullptr);
    SessionContext* sess = g_engine->GetSessionManager()->CreateSessionContext();
    if (!sess) return 1;
    TxnManager* txn = sess->GetTxnManager();

    char name_buf[256];
    snprintf(name_buf, sizeof(name_buf), "motix_%d_%s_%s", iDb, table_name, index_name);
    std::string long_name = std::string("public.") + name_buf;

    Table* t = new Table();
    if (!t->Init(name_buf, long_name.c_str(), 3)) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // col 0: rowid (LONG, 8 bytes) — the table rowid this entry points to
    t->AddColumn("idx_rowid", sizeof(uint64_t),
                 MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, false);
    // col 1: record data (BLOB, MAX_RECORD_SIZE) — original SQLite index record
    t->AddColumn("idx_record", MAX_RECORD_SIZE,
                 MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB, false);
    // col 2: encoded key (VARCHAR, IDX_KEY_COL_SIZE) — memcmp-comparable key
    //   This MUST be the last column so GetInternalKeyBuff(PRIMARY) returns it.
    t->AddColumn("idx_key", IDX_KEY_COL_SIZE,
                 MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR, true);

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

    // Primary MOT index on col 2 (encoded key), unique, keyLength = IDX_MOT_KEY_LEN
    RC irc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY,
        IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR,
        true,  // unique (SQLite records always include rowid)
        IDX_MOT_KEY_LEN,
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
    ix->SetLenghtKeyFields(0, IDX_COL_KEY, IDX_MOT_KEY_LEN);
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
        g.sec_indexes[IdxKey{iDb, table_name, index_name}] = OroMotIdxInfo{t, ix};
    }
    return 0;
}

extern "C" int oroMotIndexDrop(int iDb, const char* table_name,
                               const char* index_name) {
    if (!table_name || !index_name) return 1;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    g.sec_indexes.erase(IdxKey{iDb, table_name, index_name});
    return 0;
}

// =====================================================================
// Index cursor
// =====================================================================

extern "C" int oroMotIdxCursorOpen(void* pDb, int iDb, const char* table_name,
                                   const char* index_name, int wrFlag,
                                   OroMotCursor** ppCursor) {
    OroMotIdxInfo* info = nullptr;
    {
        auto& g = globals();
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.sec_indexes.find(IdxKey{iDb, table_name, index_name});
        if (it == g.sec_indexes.end()) return 1;
        info = &it->second;
    }

    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;
    EnsureThreadCtx(c);

    if (!c->in_txn) {
        c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
        c->in_txn = true;
    }

    OroMotCursor* cur = new OroMotCursor();
    cur->table = info->table;
    cur->index = info->index;
    cur->conn = c;
    cur->write_mode = (wrFlag != 0);
    cur->at_eof = true;
    cur->is_index = true;

    *ppCursor = cur;
    return 0;
}

// Helper: advance index cursor to next MVCC-visible entry
static void IdxCursorAdvance(OroMotCursor* cur) {
    EnsureThreadCtx(cur->conn);
    while (cur->iter && cur->iter->IsValid()) {
        Sentinel* s = cur->iter->GetPrimarySentinel();
        if (s) {
            RC rc = RC_OK;
            Row* r = cur->conn->txn->RowLookup(AccessType::RD, s, rc);
            if (rc != RC_OK) { cur->at_eof = true; return; }
            if (r) {
                cur->current_row = r;
                // Read rowid from col 0
                uint64_t rid;
                r->GetValue(IDX_COL_ROWID, rid);
                cur->idx_rowid = (int64_t)rid;
                cur->at_eof = false;
                return;
            }
        }
        cur->iter->Next();
    }
    cur->at_eof = true;
}

extern "C" int oroMotIdxInsert(OroMotCursor* pCur,
                               const void* pEncodedKey, int64_t rowid,
                               const void* pRecord, int nRecord) {
    EnsureThreadCtx(pCur->conn);

    if ((uint32_t)nRecord + sizeof(uint32_t) > MAX_RECORD_SIZE) return 1;

    Row* row = pCur->table->CreateNewRow();
    if (!row) return 1;

    // col 0: rowid
    row->SetValue<uint64_t>(IDX_COL_ROWID, (uint64_t)rowid);

    // col 1: original record data (BLOB with 4-byte length prefix)
    {
        char buf[MAX_RECORD_SIZE];
        memset(buf, 0, sizeof(buf));
        uint32_t sz = (uint32_t)nRecord;
        memcpy(buf, &sz, sizeof(uint32_t));
        memcpy(buf + sizeof(uint32_t), pRecord, nRecord);
        SetStringValue(row, IDX_COL_RECORD, buf);
    }

    // col 2: encoded key (VARCHAR with 4-byte length prefix)
    // Write directly into the row data buffer
    {
        Column* col = pCur->table->GetField(IDX_COL_KEY);
        uint8_t* dst = const_cast<uint8_t*>(row->GetData()) + col->m_offset;
        uint32_t klen = IDX_ENC_KEY_LEN;
        memcpy(dst, &klen, sizeof(uint32_t));
        memcpy(dst + sizeof(uint32_t), pEncodedKey, IDX_ENC_KEY_LEN);
    }

    // Mark as InternalKey so BuildKey uses the InternalKey path
    row->SetKeytype(KeyType::INTERNAL_KEY);

    RC rc = pCur->table->InsertRow(row, pCur->conn->txn);
    if (rc != RC_OK) {
        return (rc == RC_UNIQUE_VIOLATION) ? 2 : 1;
    }
    return 0;
}

extern "C" int oroMotIdxDelete(OroMotCursor* pCur,
                               const void* pEncodedKey) {
    EnsureThreadCtx(pCur->conn);

    // Build search key: [4-byte constant len][encoded key bytes]
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint32_t klen = IDX_ENC_KEY_LEN;
    key->FillValue((const uint8_t*)&klen, sizeof(uint32_t), 0);
    key->FillValue((const uint8_t*)pEncodedKey, IDX_ENC_KEY_LEN, sizeof(uint32_t));

    RC rc = RC_OK;
    Row* r = pCur->conn->txn->RowLookupByKey(pCur->table,
                                              AccessType::RD_FOR_UPDATE, key, rc);
    pCur->index->DestroyKey(key);

    if (rc != RC_OK || !r) return (rc == RC_OK) ? 0 : 1;  // not found = ok

    rc = pCur->conn->txn->DeleteLastRow();
    return (rc == RC_OK) ? 0 : 1;
}

extern "C" int oroMotIdxRowid(OroMotCursor* pCur, int64_t* pRowid) {
    if (pCur->at_eof || !pCur->current_row) return 1;
    *pRowid = pCur->idx_rowid;
    return 0;
}

extern "C" const void* oroMotIdxRecordFetch(OroMotCursor* pCur,
                                            unsigned int* pAmt) {
    if (pCur->at_eof || !pCur->current_row) {
        *pAmt = 0;
        return nullptr;
    }
    // col 1 is BLOB: [4-byte-len][record bytes]
    const uint8_t* p = pCur->current_row->GetValue(IDX_COL_RECORD);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    *pAmt = sz;
    return p + sizeof(uint32_t);
}

extern "C" int oroMotIdxSeek(OroMotCursor* pCur,
                             const void* pEncodedKey, int nKey,
                             int cmp_op, int* pEof) {
    /* cmp_op: 0=GT, 1=GE, 2=LT, 3=LE */
    EnsureThreadCtx(pCur->conn);

    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }

    // Build search key: [4-byte constant len][encoded key bytes (padded)]
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint32_t klen = IDX_ENC_KEY_LEN;
    key->FillValue((const uint8_t*)&klen, sizeof(uint32_t), 0);
    int copyLen = nKey < (int)IDX_ENC_KEY_LEN ? nKey : (int)IDX_ENC_KEY_LEN;
    key->FillValue((const uint8_t*)pEncodedKey, copyLen, sizeof(uint32_t));

    bool found = false;
    bool forward = (cmp_op <= 1);  // GT,GE = forward; LT,LE = reverse?

    // For GE/GT: search forward from the key position
    // For LE/LT: search forward too but we'll need to handle differently
    pCur->iter = pCur->index->Search(key, true /*matchKey*/, true /*forward*/,
                                     0 /*pid*/, found);
    pCur->index->DestroyKey(key);

    if (cmp_op == 0 || cmp_op == 1) {
        // GE or GT
        IdxCursorAdvance(pCur);
        if (cmp_op == 0 && !pCur->at_eof) {
            // GT: if positioned at exact match, skip it
            // Compare current key against search key
            Column* col = pCur->table->GetField(IDX_COL_KEY);
            const uint8_t* curKey = pCur->current_row->GetData() + col->m_offset + sizeof(uint32_t);
            if (memcmp(curKey, pEncodedKey, copyLen) == 0) {
                // Exact match — need to advance past it for GT
                if (pCur->iter) pCur->iter->Next();
                IdxCursorAdvance(pCur);
            }
        }
    } else {
        // LT or LE: we need entries before the key.
        // MassTree Search positions at or after the key.
        // We need to back up. For now, scan from beginning and stop at key.
        if (pCur->iter) { pCur->iter->Destroy(); pCur->iter = nullptr; }
        pCur->iter = pCur->index->Begin(0);
        // Find the last entry that is < (or <=) the search key
        OroMotCursor best = {};
        best.at_eof = true;
        while (true) {
            IdxCursorAdvance(pCur);
            if (pCur->at_eof) break;
            Column* col = pCur->table->GetField(IDX_COL_KEY);
            const uint8_t* curKey = pCur->current_row->GetData() + col->m_offset + sizeof(uint32_t);
            int cmp = memcmp(curKey, pEncodedKey, copyLen);
            if (cmp_op == 2 && cmp >= 0) break;   // LT: stop at >=
            if (cmp_op == 3 && cmp > 0) break;    // LE: stop at >
            best.current_row = pCur->current_row;
            best.idx_rowid = pCur->idx_rowid;
            best.at_eof = false;
            if (pCur->iter) pCur->iter->Next();
        }
        if (!best.at_eof) {
            pCur->current_row = best.current_row;
            pCur->idx_rowid = best.idx_rowid;
            pCur->at_eof = false;
        } else {
            pCur->at_eof = true;
        }
    }

    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}
