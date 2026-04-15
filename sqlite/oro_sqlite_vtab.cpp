/*
 * oro_sqlite_vtab.cpp - Core SQLite virtual table implementation for oro-db
 *
 * Implements the sqlite3_module callbacks that bridge SQLite's SQL engine
 * to the oro-db in-memory transactional storage engine (MOT).  This is the
 * direct analogue of the openGauss FDW adapter layer.
 */

#include "oro_sqlite.h"
#include "oro_sqlite_conn.h"

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <string>
#include <vector>

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
#include "column.h"

// Key encoding helpers from bench framework
#include "bench_util.h"

using namespace MOT;

// =====================================================================
// Global state
// =====================================================================

static MOTEngine* g_engine = nullptr;

static OroGlobal g_oro_global;

OroGlobal& OroGetGlobal()
{
    return g_oro_global;
}

// =====================================================================
// Engine lifecycle (C API)
// =====================================================================

extern "C" int oro_engine_init(const char* config_path)
{
    if (g_engine)
        return SQLITE_OK;  // already initialized

    g_engine = MOTEngine::CreateInstance(config_path);
    return g_engine ? SQLITE_OK : SQLITE_ERROR;
}

extern "C" void oro_engine_shutdown(void)
{
    if (g_engine) {
        MOTEngine::DestroyInstance();
        g_engine = nullptr;
    }
}

// =====================================================================
// Thread context management
// =====================================================================

// Thread-local flag: has this thread been initialized for MOT?
static thread_local bool tl_mot_initialized = false;

void OroEnsureThreadContext(OroConnection* conn)
{
    if (!tl_mot_initialized) {
        // ScopedSessionManager normally handles this, but we need to do it
        // manually since our lifecycle doesn't match a simple scope.
        knl_thread_mot_init();
        tl_mot_initialized = true;
    }
    // Bind the session to this thread's u_sess
    if (conn && conn->session) {
        u_sess->mot_cxt.session_context = conn->session;
        u_sess->mot_cxt.txn_manager = conn->txn;
    }
}

// =====================================================================
// Transaction helpers
// =====================================================================

void OroEnsureReadTxn(OroConnection* conn)
{
    if (conn->txn_state == OroTxnState::IDLE) {
        OroEnsureThreadContext(conn);
        conn->txn->StartTransaction(conn->txn->GetTransactionId(),
                                    READ_COMMITED);
        conn->txn_state = OroTxnState::READ_ACTIVE;
    }
    // If a write txn is active, that's fine — reads happen within the write txn
}

void OroMaybeEndReadTxn(OroConnection* conn)
{
    if (conn->open_cursors == 0 && conn->txn_state == OroTxnState::READ_ACTIVE) {
        conn->txn->EndTransaction();
        conn->txn_state = OroTxnState::IDLE;
    }
}

// =====================================================================
// Connection registry
// =====================================================================

OroConnection* OroGetConnection(sqlite3* db)
{
    auto& g = OroGetGlobal();
    std::lock_guard<std::mutex> lock(g.mu);

    auto it = g.connections.find(db);
    if (it != g.connections.end())
        return it->second;

    // Create new connection
    OroEnsureThreadContext(nullptr);

    SessionContext* session = g_engine->GetSessionManager()->CreateSessionContext();
    if (!session)
        return nullptr;

    auto* conn = new OroConnection();
    conn->session = session;
    conn->txn = session->GetTxnManager();
    g.connections[db] = conn;

    // Bind session to this thread
    OroEnsureThreadContext(conn);

    return conn;
}

void OroReleaseConnection(sqlite3* db, OroConnection* conn)
{
    auto& g = OroGetGlobal();
    std::lock_guard<std::mutex> lock(g.mu);

    if (conn->vtab_count > 0)
        return;  // other vtabs still using it

    // End any active transaction
    if (conn->txn_state != OroTxnState::IDLE) {
        OroEnsureThreadContext(conn);
        if (conn->txn_state == OroTxnState::WRITE_ACTIVE) {
            conn->txn->Rollback();
        }
        conn->txn->EndTransaction();
        conn->txn_state = OroTxnState::IDLE;
    }

    g_engine->GetSessionManager()->DestroySessionContext(conn->session);
    g_engine->OnCurrentThreadEnding();
    g.connections.erase(db);
    delete conn;
}

// =====================================================================
// Schema parsing: CREATE VIRTUAL TABLE t USING oro(col1 TYPE, ...)
// =====================================================================

static std::string ToUpper(const std::string& s)
{
    std::string r = s;
    for (auto& c : r) c = (char)toupper((unsigned char)c);
    return r;
}

static std::string Trim(const std::string& s)
{
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// Parse a single column definition like "id INTEGER PRIMARY KEY" or "name VARCHAR(64)"
static bool ParseColumnDef(const std::string& def, OroColMeta& meta)
{
    // Tokenize by whitespace
    std::vector<std::string> tokens;
    std::string cur;
    for (char c : def) {
        if (isspace((unsigned char)c)) {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
        } else {
            cur += c;
        }
    }
    if (!cur.empty()) tokens.push_back(cur);

    if (tokens.size() < 2) return false;

    meta.name = tokens[0];
    std::string type_str = ToUpper(tokens[1]);
    meta.is_pk = false;
    meta.size = 0;

    // Check for PRIMARY KEY in remaining tokens
    for (size_t i = 2; i + 1 < tokens.size(); i++) {
        if (ToUpper(tokens[i]) == "PRIMARY" && ToUpper(tokens[i + 1]) == "KEY") {
            meta.is_pk = true;
            break;
        }
    }

    // Parse type with optional size: VARCHAR(64), BLOB(256), etc.
    uint32_t explicit_size = 0;
    auto paren = type_str.find('(');
    if (paren != std::string::npos) {
        auto end_paren = type_str.find(')', paren);
        if (end_paren != std::string::npos) {
            explicit_size = (uint32_t)atoi(type_str.substr(paren + 1, end_paren - paren - 1).c_str());
        }
        type_str = type_str.substr(0, paren);
    }

    // Map SQLite/SQL type names to MOT types
    if (type_str == "INTEGER" || type_str == "INT" || type_str == "BIGINT" || type_str == "LONG") {
        meta.mot_type = (int)MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG;
        meta.size = sizeof(uint64_t);
    } else if (type_str == "REAL" || type_str == "DOUBLE" || type_str == "FLOAT") {
        meta.mot_type = (int)MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE;
        meta.size = sizeof(double);
    } else if (type_str == "TEXT" || type_str == "VARCHAR") {
        meta.mot_type = (int)MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
        meta.size = explicit_size > 0 ? explicit_size : 256;
    } else if (type_str == "BLOB") {
        meta.mot_type = (int)MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB;
        meta.size = explicit_size > 0 ? explicit_size : 256;
    } else if (type_str == "SMALLINT" || type_str == "SHORT") {
        meta.mot_type = (int)MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT;
        meta.size = sizeof(int16_t);
    } else {
        // Default to VARCHAR
        meta.mot_type = (int)MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
        meta.size = explicit_size > 0 ? explicit_size : 256;
    }

    return true;
}

// Parse all column definitions from argv[3..argc-1]
// SQLite passes: argv[0]=module, argv[1]=dbname, argv[2]=tablename, argv[3..]=args
static bool ParseSchema(int argc, const char* const* argv,
                        std::string& table_name,
                        std::vector<OroColMeta>& columns)
{
    table_name = argv[2];

    // argv[3..argc-1] are the arguments inside the parentheses of CREATE VIRTUAL TABLE.
    // They're split by commas by SQLite already.
    for (int i = 3; i < argc; i++) {
        std::string arg = Trim(argv[i]);
        if (arg.empty()) continue;

        OroColMeta meta;
        if (!ParseColumnDef(arg, meta))
            return false;
        columns.push_back(std::move(meta));
    }

    return !columns.empty();
}

// Build the sqlite3_declare_vtab SQL string
static std::string BuildDeclareVtabSql(const std::vector<OroColMeta>& columns)
{
    std::string sql = "CREATE TABLE x(";
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) sql += ", ";
        sql += columns[i].name;
        // Map MOT type back to SQLite affinity
        auto mt = (MOT_CATALOG_FIELD_TYPES)columns[i].mot_type;
        switch (mt) {
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT:
                sql += " INTEGER";
                break;
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE:
                sql += " REAL";
                break;
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR:
                sql += " TEXT";
                break;
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB:
                sql += " BLOB";
                break;
            default:
                sql += " TEXT";
                break;
        }
    }
    sql += ")";
    return sql;
}

// Create the MOT table and primary index. Returns the Table* or nullptr.
static Table* CreateMotTable(OroConnection* conn, const std::string& table_name,
                             const std::vector<OroColMeta>& columns,
                             int& pk_sqlite_col, int& pk_mot_col)
{
    OroEnsureThreadContext(conn);

    // Find PK column
    pk_sqlite_col = -1;
    for (size_t i = 0; i < columns.size(); i++) {
        if (columns[i].is_pk) {
            pk_sqlite_col = (int)i;
            break;
        }
    }
    // Default: first INTEGER column, or column 0
    if (pk_sqlite_col < 0) {
        for (size_t i = 0; i < columns.size(); i++) {
            if ((MOT_CATALOG_FIELD_TYPES)columns[i].mot_type == MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG) {
                pk_sqlite_col = (int)i;
                break;
            }
        }
        if (pk_sqlite_col < 0) pk_sqlite_col = 0;
    }

    // MOT column layout: columns 0..N-1 = user columns, column N = hidden key column.
    // The hidden key column stores the packed BE primary key for the InternalKey path
    // in BuildKey/GetInternalKeyBuff (which always reads the LAST column).
    uint32_t field_count = (uint32_t)columns.size() + 1;  // +1 for hidden key column

    Table* table = new Table();
    std::string long_name = "public." + table_name;
    if (!table->Init(table_name.c_str(), long_name.c_str(), field_count)) {
        delete table;
        return nullptr;
    }

    // Add user columns
    for (size_t i = 0; i < columns.size(); i++) {
        table->AddColumn(columns[i].name.c_str(), columns[i].size,
                         (MOT_CATALOG_FIELD_TYPES)columns[i].mot_type,
                         columns[i].is_pk /* isNotNull */);
    }

    // Add hidden key column (uint64, last position)
    table->AddColumn("_oro_key", sizeof(uint64_t),
                     MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);

    if (!table->InitRowPool()) {
        delete table;
        return nullptr;
    }

    if (!table->InitTombStonePool()) {
        delete table;
        return nullptr;
    }

    // pk_mot_col points to the hidden key column (last column) for SetInternalKey
    pk_mot_col = (int)columns.size();  // index of the hidden _oro_key column

    // Create table in a DDL transaction
    TxnManager* txn = conn->txn;
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);

    RC rc = txn->CreateTable(table);
    if (rc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete table;
        return nullptr;
    }

    // Create primary index
    // Key length: 8 bytes for InternalKey path (raw htobe64 uint64)
    static constexpr uint16_t LONG_KEY_LEN = 8;
    RC irc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY,
        IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR,
        true,          // unique
        LONG_KEY_LEN,
        "ix_" + table_name + "_pk",
        irc, nullptr);

    if (!ix || irc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete table;
        return nullptr;
    }

    if (!ix->SetNumTableFields(table->GetFieldCount())) {
        delete ix;
        txn->Rollback();
        txn->EndTransaction();
        delete table;
        return nullptr;
    }
    ix->SetNumIndexFields(1);
    ix->SetLenghtKeyFields(0, (uint16_t)pk_mot_col, LONG_KEY_LEN);  // key column = hidden _oro_key
    ix->SetTable(table);

    rc = txn->CreateIndex(table, ix, true /* isPrimary */);
    if (rc != RC_OK) {
        delete ix;
        txn->Rollback();
        txn->EndTransaction();
        delete table;
        return nullptr;
    }

    rc = txn->Commit();
    if (rc != RC_OK) {
        txn->EndTransaction();
        delete table;
        return nullptr;
    }
    txn->EndTransaction();

    return table;
}

// =====================================================================
// xCreate / xConnect
// =====================================================================

static int OroCreate(sqlite3* db, void* /*pAux*/, int argc, const char* const* argv,
                     sqlite3_vtab** ppVtab, char** pzErr)
{
    if (!g_engine) {
        *pzErr = sqlite3_mprintf("oro: engine not initialized (call oro_engine_init first)");
        return SQLITE_ERROR;
    }

    std::string table_name;
    std::vector<OroColMeta> columns;
    if (!ParseSchema(argc, argv, table_name, columns)) {
        *pzErr = sqlite3_mprintf("oro: failed to parse column definitions");
        return SQLITE_ERROR;
    }

    // Declare the schema to SQLite
    std::string decl = BuildDeclareVtabSql(columns);
    int rc = sqlite3_declare_vtab(db, decl.c_str());
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("oro: sqlite3_declare_vtab failed: %s", sqlite3_errmsg(db));
        return rc;
    }

    // Get or create the connection
    OroConnection* conn = OroGetConnection(db);
    if (!conn) {
        *pzErr = sqlite3_mprintf("oro: failed to create MOT session");
        return SQLITE_ERROR;
    }

    // Create the MOT table
    int pk_sqlite_col, pk_mot_col;
    Table* table = CreateMotTable(conn, table_name, columns, pk_sqlite_col, pk_mot_col);
    if (!table) {
        *pzErr = sqlite3_mprintf("oro: failed to create MOT table '%s'", table_name.c_str());
        return SQLITE_ERROR;
    }

    // Allocate the vtab struct
    OroVtab* vtab = new OroVtab();
    memset(static_cast<sqlite3_vtab*>(vtab), 0, sizeof(sqlite3_vtab));
    vtab->conn = conn;
    vtab->table = table;
    vtab->primary_ix = table->GetPrimaryIndex();
    vtab->n_columns = (int)columns.size();
    vtab->pk_col = pk_sqlite_col;
    vtab->pk_mot_col = pk_mot_col;
    vtab->col_meta = new OroColMeta[columns.size()];
    for (size_t i = 0; i < columns.size(); i++)
        vtab->col_meta[i] = columns[i];

    conn->vtab_count++;

    *ppVtab = vtab;
    return SQLITE_OK;
}

static int OroConnect(sqlite3* db, void* pAux, int argc, const char* const* argv,
                      sqlite3_vtab** ppVtab, char** pzErr)
{
    // For now, xConnect == xCreate (in-memory, no persistent backing store)
    return OroCreate(db, pAux, argc, argv, ppVtab, pzErr);
}

// =====================================================================
// xDisconnect / xDestroy
// =====================================================================

static int OroDisconnect(sqlite3_vtab* pVtab)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    OroConnection* conn = vtab->conn;

    delete[] vtab->col_meta;

    conn->vtab_count--;
    if (conn->vtab_count <= 0) {
        // Find the db handle — we need it to remove from registry.
        // Walk the global registry to find the matching connection.
        auto& g = OroGetGlobal();
        std::lock_guard<std::mutex> lock(g.mu);
        for (auto it = g.connections.begin(); it != g.connections.end(); ++it) {
            if (it->second == conn) {
                // End any active transaction
                if (conn->txn_state != OroTxnState::IDLE) {
                    OroEnsureThreadContext(conn);
                    if (conn->txn_state == OroTxnState::WRITE_ACTIVE) {
                        conn->txn->Rollback();
                    }
                    conn->txn->EndTransaction();
                    conn->txn_state = OroTxnState::IDLE;
                }
                g_engine->GetSessionManager()->DestroySessionContext(conn->session);
                g_engine->OnCurrentThreadEnding();
                g.connections.erase(it);
                delete conn;
                break;
            }
        }
    }

    delete vtab;
    return SQLITE_OK;
}

static int OroDestroy(sqlite3_vtab* pVtab)
{
    // TODO: could drop the MOT table here with txn->DropTable()
    // For now, just disconnect
    return OroDisconnect(pVtab);
}

// =====================================================================
// xOpen / xClose — cursor lifecycle
// =====================================================================

static int OroOpen(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);

    OroCursor* cursor = new OroCursor();
    memset(static_cast<sqlite3_vtab_cursor*>(cursor), 0, sizeof(sqlite3_vtab_cursor));
    cursor->vtab = vtab;
    cursor->at_eof = true;

    vtab->conn->open_cursors++;

    *ppCursor = cursor;
    return SQLITE_OK;
}

static void CursorCleanup(OroCursor* cursor)
{
    if (cursor->iterator) {
        cursor->iterator->Destroy();
        cursor->iterator = nullptr;
    }
    if (cursor->end_iter) {
        cursor->end_iter->Destroy();
        cursor->end_iter = nullptr;
    }
    if (cursor->lo_key) {
        cursor->vtab->primary_ix->DestroyKey(cursor->lo_key);
        cursor->lo_key = nullptr;
    }
    if (cursor->hi_key) {
        cursor->vtab->primary_ix->DestroyKey(cursor->hi_key);
        cursor->hi_key = nullptr;
    }
    cursor->current_row = nullptr;
    cursor->at_eof = true;
}

static int OroClose(sqlite3_vtab_cursor* pCursor)
{
    OroCursor* cursor = static_cast<OroCursor*>(pCursor);
    OroConnection* conn = cursor->vtab->conn;

    CursorCleanup(cursor);

    conn->open_cursors--;
    OroMaybeEndReadTxn(conn);

    delete cursor;
    return SQLITE_OK;
}

// =====================================================================
// xFilter — position the cursor at the start of a scan
// =====================================================================

// Advance the iterator to the next MVCC-visible row
static void CursorAdvance(OroCursor* cursor)
{
    OroVtab* vtab = cursor->vtab;
    OroConnection* conn = vtab->conn;

    OroEnsureThreadContext(conn);

    while (cursor->iterator && cursor->iterator->IsValid()) {
        // For range scans: check if we've passed the end boundary
        if (cursor->end_iter && cursor->end_iter->IsValid()) {
            const Key* k0 = reinterpret_cast<const Key*>(cursor->iterator->GetKey());
            const Key* k1 = reinterpret_cast<const Key*>(cursor->end_iter->GetKey());
            if (k0 && k1) {
                int cmp = memcmp(k0->GetKeyBuf(), k1->GetKeyBuf(),
                                 vtab->primary_ix->GetKeySizeNoSuffix());
                if (cmp >= 0) {
                    cursor->at_eof = true;
                    return;
                }
            }
        }

        Sentinel* sentinel = cursor->iterator->GetPrimarySentinel();
        if (sentinel) {
            RC rc = RC_OK;
            Row* row = conn->txn->RowLookup(AccessType::RD, sentinel, rc);
            if (rc != RC_OK) {
                cursor->at_eof = true;
                return;
            }
            if (row) {
                cursor->current_row = row;
                cursor->at_eof = false;
                return;
            }
        }
        // Row not visible (deleted or not yet committed) — skip
        cursor->iterator->Next();
    }

    cursor->at_eof = true;
}

static Key* BuildKeyFromInt64(Index* ix, int64_t val)
{
    Key* key = ix->CreateNewSearchKey();
    if (!key) return nullptr;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    // Match the InternalKey format: htobe64 stored as raw 8 bytes
    uint64_t be_val = htobe64((uint64_t)val);
    key->FillValue(reinterpret_cast<const uint8_t*>(&be_val), sizeof(uint64_t), 0);
    return key;
}

static int OroFilter(sqlite3_vtab_cursor* pCursor, int idxNum,
                     const char* /*idxStr*/, int argc, sqlite3_value** argv)
{
    OroCursor* cursor = static_cast<OroCursor*>(pCursor);
    OroVtab* vtab = cursor->vtab;
    OroConnection* conn = vtab->conn;

    // Clean up any previous scan
    CursorCleanup(cursor);
    cursor->scan_type = idxNum;

    OroEnsureThreadContext(conn);
    OroEnsureReadTxn(conn);

    Index* ix = vtab->primary_ix;
    uint32_t pid = 0;  // thread slot (0 for single-threaded SQLite)

    if (idxNum == 1 && argc >= 1) {
        // Point lookup: EQ on primary key
        int64_t pk_val = sqlite3_value_int64(argv[0]);
        Key* key = BuildKeyFromInt64(ix, pk_val);
        if (!key) {
            cursor->at_eof = true;
            return SQLITE_OK;
        }

        RC rc = RC_OK;
        Row* row = conn->txn->RowLookupByKey(vtab->table, AccessType::RD, key, rc);
        ix->DestroyKey(key);

        if (rc == RC_OK && row) {
            cursor->current_row = row;
            cursor->at_eof = false;
        } else {
            cursor->at_eof = true;
        }
        return SQLITE_OK;

    } else if (idxNum == 2 && argc >= 2) {
        // Range scan: lower AND upper bound
        int64_t lo_val = sqlite3_value_int64(argv[0]);
        int64_t hi_val = sqlite3_value_int64(argv[1]);

        cursor->lo_key = BuildKeyFromInt64(ix, lo_val);
        // End cursor: hi_val + 1 so the two-cursor pattern includes hi_val
        cursor->hi_key = BuildKeyFromInt64(ix, hi_val + 1);
        if (!cursor->lo_key || !cursor->hi_key) {
            cursor->at_eof = true;
            return SQLITE_OK;
        }

        bool found = false;
        cursor->iterator = ix->Search(cursor->lo_key, true, true, pid, found);
        cursor->end_iter = ix->Search(cursor->hi_key, true, true, pid, found);

        CursorAdvance(cursor);
        return SQLITE_OK;

    } else if (idxNum == 3 && argc >= 1) {
        // Lower-bound scan (GE/GT)
        int64_t lo_val = sqlite3_value_int64(argv[0]);
        cursor->lo_key = BuildKeyFromInt64(ix, lo_val);
        if (!cursor->lo_key) {
            cursor->at_eof = true;
            return SQLITE_OK;
        }

        bool found = false;
        cursor->iterator = ix->Search(cursor->lo_key, true, true, pid, found);
        CursorAdvance(cursor);
        return SQLITE_OK;

    } else if (idxNum == 4 && argc >= 1) {
        // Upper-bound scan (LE/LT) — full scan with end boundary
        int64_t hi_val = sqlite3_value_int64(argv[0]);
        cursor->hi_key = BuildKeyFromInt64(ix, hi_val);
        if (!cursor->hi_key) {
            cursor->at_eof = true;
            return SQLITE_OK;
        }

        bool found = false;
        cursor->iterator = ix->Begin(pid);
        cursor->end_iter = ix->Search(cursor->hi_key, true, true, pid, found);
        CursorAdvance(cursor);
        return SQLITE_OK;

    } else {
        // Full table scan (idxNum == 0)
        cursor->iterator = ix->Begin(pid);
        CursorAdvance(cursor);
        return SQLITE_OK;
    }
}

// =====================================================================
// xNext / xEof
// =====================================================================

static int OroNext(sqlite3_vtab_cursor* pCursor)
{
    OroCursor* cursor = static_cast<OroCursor*>(pCursor);

    if (cursor->scan_type == 1) {
        // Point lookup: only one row
        cursor->at_eof = true;
        return SQLITE_OK;
    }

    if (cursor->iterator) {
        cursor->iterator->Next();
        CursorAdvance(cursor);
    } else {
        cursor->at_eof = true;
    }

    return SQLITE_OK;
}

static int OroEof(sqlite3_vtab_cursor* pCursor)
{
    OroCursor* cursor = static_cast<OroCursor*>(pCursor);
    return cursor->at_eof ? 1 : 0;
}

// =====================================================================
// xColumn — read a column value from the current row
// =====================================================================

static int OroColumn(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col)
{
    OroCursor* cursor = static_cast<OroCursor*>(pCursor);
    Row* row = cursor->current_row;

    if (!row) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    OroVtab* vtab = cursor->vtab;
    if (col < 0 || col >= vtab->n_columns) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    // In standalone mode, MOT column index == sqlite column index
    int mot_col = col;
    auto mot_type = (MOT_CATALOG_FIELD_TYPES)vtab->col_meta[col].mot_type;

    switch (mot_type) {
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG: {
            uint64_t val;
            row->GetValue(mot_col, val);
            sqlite3_result_int64(ctx, (sqlite3_int64)val);
            break;
        }
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT: {
            int16_t val;
            row->GetValue(mot_col, val);
            sqlite3_result_int(ctx, (int)val);
            break;
        }
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE: {
            double val;
            row->GetValue(mot_col, val);
            sqlite3_result_double(ctx, val);
            break;
        }
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR: {
            const char* ptr = (const char*)row->GetValue(mot_col);
            uint32_t col_size = vtab->col_meta[col].size;
            size_t len = strnlen(ptr, col_size);
            sqlite3_result_text(ctx, ptr, (int)len, SQLITE_TRANSIENT);
            break;
        }
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB: {
            const uint8_t* ptr = row->GetValue(mot_col);
            uint32_t col_size = vtab->col_meta[col].size;
            sqlite3_result_blob(ctx, ptr, (int)col_size, SQLITE_TRANSIENT);
            break;
        }
        default:
            sqlite3_result_null(ctx);
            break;
    }

    return SQLITE_OK;
}

// =====================================================================
// xRowid — return the primary key as the SQLite rowid
// =====================================================================

static int OroRowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid)
{
    OroCursor* cursor = static_cast<OroCursor*>(pCursor);
    Row* row = cursor->current_row;

    if (!row) {
        *pRowid = 0;
        return SQLITE_OK;
    }

    OroVtab* vtab = cursor->vtab;
    uint64_t pk_val;
    row->GetValue(vtab->pk_col, pk_val);  // read from user PK column, not hidden key col
    *pRowid = (sqlite3_int64)pk_val;
    return SQLITE_OK;
}

// =====================================================================
// sqlite3_module definition
// =====================================================================

static sqlite3_module oro_module = {
    /* iVersion    */ 2,
    /* xCreate     */ OroCreate,
    /* xConnect    */ OroConnect,
    /* xBestIndex  */ OroBestIndex,
    /* xDisconnect */ OroDisconnect,
    /* xDestroy    */ OroDestroy,
    /* xOpen       */ OroOpen,
    /* xClose      */ OroClose,
    /* xFilter     */ OroFilter,
    /* xNext       */ OroNext,
    /* xEof        */ OroEof,
    /* xColumn     */ OroColumn,
    /* xRowid      */ OroRowid,
    /* xUpdate     */ OroUpdate,
    /* xBegin      */ OroBegin,
    /* xSync       */ OroSync,
    /* xCommit     */ OroCommit,
    /* xRollback   */ OroRollback,
    /* xFindFunction */ nullptr,
    /* xRename     */ nullptr,
    /* xSavepoint  */ nullptr,
    /* xRelease    */ nullptr,
    /* xRollbackTo */ nullptr,
};

// =====================================================================
// Module registration
// =====================================================================

extern "C" int oro_sqlite_register(sqlite3* db)
{
    return sqlite3_create_module(db, "oro", &oro_module, nullptr);
}
