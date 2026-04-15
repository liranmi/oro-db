/*
 * oro_sqlite_update.cpp - DML and transaction callbacks for oro virtual tables
 *
 * xUpdate handles INSERT, UPDATE, DELETE through a single entry point.
 * xBegin/xSync/xCommit/xRollback coordinate OCC transactions with SQLite's
 * two-phase commit protocol.
 */

#include "oro_sqlite_conn.h"

#include "mot_engine.h"
#include "session_context.h"
#include "session_manager.h"
#include "txn.h"
#include "table.h"
#include "row.h"
#include "index.h"
#include "catalog_column_types.h"

#include "bench_util.h"  // KeyFillU64, EncodeU64BE
#include <endian.h>      // htobe64

using namespace MOT;

// =====================================================================
// Helper: write a sqlite3_value into a MOT row column
// =====================================================================

static void WriteColumnValue(Row* row, int mot_col, int sqlite_col,
                             const OroColMeta& meta, sqlite3_value* val)
{
    auto mot_type = (MOT_CATALOG_FIELD_TYPES)meta.mot_type;

    if (sqlite3_value_type(val) == SQLITE_NULL) {
        // Zero-fill the column for NULL
        // (MOT doesn't have native NULL support via the bench API;
        //  we just store zero/empty as a placeholder)
        switch (mot_type) {
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG:
                row->SetValue<uint64_t>(mot_col, 0);
                break;
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT:
                row->SetValue<int16_t>(mot_col, 0);
                break;
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE:
                row->SetValue<double>(mot_col, 0.0);
                break;
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB:
                SetStringValue(row, mot_col, "");
                break;
            default:
                break;
        }
        return;
    }

    switch (mot_type) {
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG:
            row->SetValue<uint64_t>(mot_col, (uint64_t)sqlite3_value_int64(val));
            break;
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT:
            row->SetValue<int16_t>(mot_col, (int16_t)sqlite3_value_int(val));
            break;
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE:
            row->SetValue<double>(mot_col, sqlite3_value_double(val));
            break;
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR: {
            const char* text = (const char*)sqlite3_value_text(val);
            if (text)
                SetStringValue(row, mot_col, text);
            else
                SetStringValue(row, mot_col, "");
            break;
        }
        case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB: {
            const void* blob = sqlite3_value_blob(val);
            int blen = sqlite3_value_bytes(val);
            if (blob && blen > 0)
                row->SetValueVariable(mot_col, blob, (uint32_t)blen);
            break;
        }
        default:
            break;
    }
}

// =====================================================================
// xUpdate — single entry point for INSERT, UPDATE, DELETE
//
// argc == 1, argv[0] != NULL           → DELETE  (rowid in argv[0])
// argc > 1, argv[0] == NULL            → INSERT  (new rowid in argv[1], cols in argv[2..])
// argc > 1, argv[0] != NULL            → UPDATE  (old rowid argv[0], new rowid argv[1], cols argv[2..])
// =====================================================================

int OroUpdate(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv,
              sqlite3_int64* pRowid)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    OroConnection* conn = vtab->conn;

    OroEnsureThreadContext(conn);

    // Ensure write transaction is active
    if (conn->txn_state == OroTxnState::IDLE) {
        conn->txn->StartTransaction(conn->txn->GetTransactionId(),
                                    READ_COMMITED);
        conn->txn_state = OroTxnState::WRITE_ACTIVE;
    } else if (conn->txn_state == OroTxnState::READ_ACTIVE) {
        // Promote read → write. End the read txn and start fresh.
        conn->txn->EndTransaction();
        conn->txn->StartTransaction(conn->txn->GetTransactionId(),
                                    READ_COMMITED);
        conn->txn_state = OroTxnState::WRITE_ACTIVE;
    }

    TxnManager* txn = conn->txn;
    Table* table = vtab->table;
    Index* ix = vtab->primary_ix;

    // --- DELETE ---
    if (argc == 1) {
        int64_t del_rowid = sqlite3_value_int64(argv[0]);
        Key* key = ix->CreateNewSearchKey();
        if (!key) return SQLITE_NOMEM;
        key->FillPattern(0x00, key->GetKeyLength(), 0);
        uint64_t be_val = htobe64((uint64_t)del_rowid);
        key->FillValue(reinterpret_cast<const uint8_t*>(&be_val), sizeof(uint64_t), 0);

        RC rc = RC_OK;
        Row* row = txn->RowLookupByKey(table, AccessType::RD_FOR_UPDATE, key, rc);
        ix->DestroyKey(key);

        if (rc != RC_OK) {
            vtab->zErrMsg = sqlite3_mprintf("oro: lookup for delete failed: %s",
                                            RcToString(rc));
            return SQLITE_ERROR;
        }
        if (!row) {
            // Row not found — not an error in SQLite semantics
            return SQLITE_OK;
        }

        rc = txn->DeleteLastRow();
        if (rc != RC_OK) {
            vtab->zErrMsg = sqlite3_mprintf("oro: delete failed: %s", RcToString(rc));
            return SQLITE_ERROR;
        }

        return SQLITE_OK;
    }

    // --- INSERT ---
    if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        Row* row = table->CreateNewRow();
        if (!row) return SQLITE_NOMEM;

        // Determine the primary key value
        int64_t pk_val;
        if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
            // SQLite didn't provide a rowid — auto-generate
            // Use the PK column value if provided
            int pk_argv_idx = 2 + vtab->pk_col;
            if (pk_argv_idx < argc && sqlite3_value_type(argv[pk_argv_idx]) != SQLITE_NULL) {
                pk_val = sqlite3_value_int64(argv[pk_argv_idx]);
            } else {
                // Auto-generate a rowid (simple counter)
                static std::atomic<int64_t> s_next_rowid{1};
                pk_val = s_next_rowid.fetch_add(1);
            }
        } else {
            pk_val = sqlite3_value_int64(argv[1]);
        }

        // Set user column values
        for (int i = 0; i < vtab->n_columns && (i + 2) < argc; i++) {
            int mot_col = i;
            if (i == vtab->pk_col) {
                row->SetValue<uint64_t>(mot_col, (uint64_t)pk_val);
            } else {
                WriteColumnValue(row, mot_col, i, vtab->col_meta[i], argv[i + 2]);
            }
        }

        // Set the hidden key column (last column) with BE-packed key via SetInternalKey.
        // GetInternalKeyBuff reads the last column; htobe64 ensures MassTree byte-order.
        row->SetInternalKey(vtab->pk_mot_col, htobe64((uint64_t)pk_val));

        RC rc = table->InsertRow(row, txn);
        if (rc != RC_OK) {
            table->DestroyRow(row);
            if (rc == RC_UNIQUE_VIOLATION) {
                vtab->zErrMsg = sqlite3_mprintf("oro: UNIQUE constraint failed");
                return SQLITE_CONSTRAINT;
            }
            vtab->zErrMsg = sqlite3_mprintf("oro: insert failed: %s", RcToString(rc));
            return SQLITE_ERROR;
        }

        *pRowid = pk_val;
        vtab->est_rows++;
        return SQLITE_OK;
    }

    // --- UPDATE ---
    if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        int64_t old_rowid = sqlite3_value_int64(argv[0]);
        int64_t new_rowid = sqlite3_value_int64(argv[1]);

        // Rowid change is not supported (would require delete + insert)
        if (old_rowid != new_rowid) {
            vtab->zErrMsg = sqlite3_mprintf("oro: changing primary key is not supported");
            return SQLITE_CONSTRAINT;
        }

        // Look up the row for update
        Key* key = ix->CreateNewSearchKey();
        if (!key) return SQLITE_NOMEM;
        key->FillPattern(0x00, key->GetKeyLength(), 0);
        uint64_t be_key = htobe64((uint64_t)old_rowid);
        key->FillValue(reinterpret_cast<const uint8_t*>(&be_key), sizeof(uint64_t), 0);

        RC rc = RC_OK;
        Row* row = txn->RowLookupByKey(table, AccessType::RD_FOR_UPDATE, key, rc);
        ix->DestroyKey(key);

        if (rc != RC_OK || !row) {
            vtab->zErrMsg = sqlite3_mprintf("oro: lookup for update failed");
            return SQLITE_ERROR;
        }

        // Update each non-PK column
        // First UpdateRow call creates the MVCC draft; subsequent writes go to draft
        bool first_update = true;
        Row* draft = nullptr;

        for (int i = 0; i < vtab->n_columns && (i + 2) < argc; i++) {
            if (i == vtab->pk_col)
                continue;  // skip PK column (can't change)

            int mot_col = i;
            auto mot_type = (MOT_CATALOG_FIELD_TYPES)vtab->col_meta[i].mot_type;

            if (first_update) {
                // First UpdateRow creates the MVCC draft (use any numeric column for the call)
                // Then write the actual value to the draft
                uint64_t dummy = 0;
                rc = txn->UpdateRow(row, mot_col, dummy);
                if (rc != RC_OK) {
                    vtab->zErrMsg = sqlite3_mprintf("oro: update failed: %s", RcToString(rc));
                    return SQLITE_ERROR;
                }
                draft = txn->GetLastAccessedDraft();
                if (draft) {
                    WriteColumnValue(draft, mot_col, i, vtab->col_meta[i], argv[i + 2]);
                }
                first_update = false;
            } else if (draft) {
                WriteColumnValue(draft, mot_col, i, vtab->col_meta[i], argv[i + 2]);
            }
        }

        return SQLITE_OK;
    }

    return SQLITE_ERROR;
}

// =====================================================================
// Transaction callbacks
// =====================================================================

int OroBegin(sqlite3_vtab* pVtab)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    OroConnection* conn = vtab->conn;

    OroEnsureThreadContext(conn);

    if (conn->txn_state == OroTxnState::IDLE) {
        conn->txn->StartTransaction(conn->txn->GetTransactionId(),
                                    READ_COMMITED);
        conn->txn_state = OroTxnState::WRITE_ACTIVE;
    } else if (conn->txn_state == OroTxnState::READ_ACTIVE) {
        // Promote to write
        conn->txn->EndTransaction();
        conn->txn->StartTransaction(conn->txn->GetTransactionId(),
                                    READ_COMMITED);
        conn->txn_state = OroTxnState::WRITE_ACTIVE;
    }

    conn->sync_done = false;
    conn->commit_done = false;
    return SQLITE_OK;
}

int OroSync(sqlite3_vtab* pVtab)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    OroConnection* conn = vtab->conn;

    if (conn->sync_done)
        return SQLITE_OK;  // already validated (shared txn, multiple vtabs)

    OroEnsureThreadContext(conn);

    if (conn->txn_state == OroTxnState::WRITE_ACTIVE) {
        RC rc = conn->txn->ValidateCommit();
        if (rc != RC_OK) {
            // OCC conflict — tell SQLite to retry or abort
            vtab->zErrMsg = sqlite3_mprintf("oro: OCC validation failed: %s",
                                            RcToString(rc));
            return SQLITE_BUSY;
        }
    }

    conn->sync_done = true;
    return SQLITE_OK;
}

int OroCommit(sqlite3_vtab* pVtab)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    OroConnection* conn = vtab->conn;

    if (conn->commit_done)
        return SQLITE_OK;

    OroEnsureThreadContext(conn);

    if (conn->txn_state == OroTxnState::WRITE_ACTIVE) {
        conn->txn->RecordCommit();
        conn->txn->EndTransaction();
        conn->txn_state = OroTxnState::IDLE;
    }

    conn->commit_done = true;
    return SQLITE_OK;
}

int OroRollback(sqlite3_vtab* pVtab)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    OroConnection* conn = vtab->conn;

    OroEnsureThreadContext(conn);

    if (conn->txn_state == OroTxnState::WRITE_ACTIVE) {
        conn->txn->Rollback();
        conn->txn->EndTransaction();
        conn->txn_state = OroTxnState::IDLE;
    }

    conn->sync_done = false;
    conn->commit_done = false;
    return SQLITE_OK;
}
