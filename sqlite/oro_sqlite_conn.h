/*
 * oro_sqlite_conn.h - Per-connection state shared across all oro virtual tables
 *
 * Each sqlite3* connection gets one OroConnection, which owns a single MOT
 * session and transaction.  This mirrors the openGauss FDW model where one
 * postgres transaction spans all MOT tables.
 */

#ifndef ORO_SQLITE_CONN_H
#define ORO_SQLITE_CONN_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <mutex>

// Forward-declare MOT types to avoid pulling engine headers into every TU
namespace MOT {
class MOTEngine;
class SessionContext;
class TxnManager;
class Table;
class Index;
class IndexIterator;
class Row;
class Key;
class Sentinel;
}  // namespace MOT

#include "sqlite3.h"

// Transaction state machine
enum class OroTxnState : uint8_t {
    IDLE,            // no active transaction
    READ_ACTIVE,     // auto-started for SELECT (no xBegin)
    WRITE_ACTIVE     // xBegin was called (INSERT/UPDATE/DELETE)
};

// Per-connection context (one per sqlite3*)
struct OroConnection {
    MOT::SessionContext* session = nullptr;
    MOT::TxnManager*    txn     = nullptr;
    OroTxnState          txn_state = OroTxnState::IDLE;
    uint32_t             open_cursors = 0;    // refcount for auto read txns
    bool                 sync_done = false;   // xSync already called this txn
    bool                 commit_done = false;  // xCommit already called this txn
    int                  vtab_count = 0;      // number of connected vtabs
};

// Column metadata parsed from CREATE VIRTUAL TABLE
struct OroColMeta {
    std::string name;
    int         mot_type;     // MOT_CATALOG_FIELD_TYPES value
    uint32_t    size;         // byte size of the column
    bool        is_pk;        // true for the PRIMARY KEY column
};

// Virtual table instance (extends sqlite3_vtab)
struct OroVtab : public sqlite3_vtab {
    OroConnection*  conn      = nullptr;
    MOT::Table*     table     = nullptr;
    MOT::Index*     primary_ix = nullptr;
    int             n_columns = 0;      // user columns (excluding null bitmap col 0)
    int             pk_col    = -1;     // sqlite column index of the PRIMARY KEY
    int             pk_mot_col = -1;    // MOT column index of the PK (1-based, after nullbytes)
    OroColMeta*     col_meta  = nullptr;
    uint64_t        est_rows  = 1000;   // estimated row count for xBestIndex
};

// Cursor instance (extends sqlite3_vtab_cursor)
struct OroCursor : public sqlite3_vtab_cursor {
    OroVtab*             vtab      = nullptr;
    MOT::IndexIterator*  iterator  = nullptr;  // current scan cursor
    MOT::IndexIterator*  end_iter  = nullptr;   // end sentinel for range scans
    MOT::Key*            lo_key    = nullptr;
    MOT::Key*            hi_key    = nullptr;
    MOT::Row*            current_row = nullptr; // MVCC-visible row from RowLookup
    bool                 at_eof    = true;
    int                  scan_type = 0;         // idxNum from xBestIndex
};

// ---- Internal helpers (implemented in oro_sqlite_vtab.cpp) ----

// Ensure MOT thread-local context is bound for the current thread/session.
void OroEnsureThreadContext(OroConnection* conn);

// Start a read transaction if none is active.
void OroEnsureReadTxn(OroConnection* conn);

// End a read transaction if cursor refcount drops to zero and no write txn.
void OroMaybeEndReadTxn(OroConnection* conn);

// Global connection registry (protected by mutex)
struct OroGlobal {
    std::mutex                                         mu;
    std::unordered_map<sqlite3*, OroConnection*>       connections;
};

OroGlobal& OroGetGlobal();

// Get or create the OroConnection for a sqlite3 handle.
OroConnection* OroGetConnection(sqlite3* db);

// Release the OroConnection when vtab_count drops to 0.
void OroReleaseConnection(sqlite3* db, OroConnection* conn);

// ---- Implemented in oro_sqlite_bestindex.cpp ----
int OroBestIndex(sqlite3_vtab* vtab, sqlite3_index_info* info);

// ---- Implemented in oro_sqlite_update.cpp ----
int OroUpdate(sqlite3_vtab* vtab, int argc, sqlite3_value** argv, sqlite3_int64* rowid);
int OroBegin(sqlite3_vtab* vtab);
int OroSync(sqlite3_vtab* vtab);
int OroCommit(sqlite3_vtab* vtab);
int OroRollback(sqlite3_vtab* vtab);

#endif /* ORO_SQLITE_CONN_H */
