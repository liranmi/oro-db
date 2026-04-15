/*
 * oro_sqlite_bestindex.cpp - xBestIndex implementation for oro virtual tables
 *
 * Tells SQLite how to access our data efficiently.  Recognizes:
 *   idxNum=0  Full table scan
 *   idxNum=1  EQ on rowid/PK (point lookup)
 *   idxNum=2  Range scan (lower AND upper bound on PK)
 *   idxNum=3  Lower-bound scan (GE/GT on PK)
 *   idxNum=4  Upper-bound scan (LE/LT on PK)
 */

#include "oro_sqlite_conn.h"
#include "sqlite3.h"

int OroBestIndex(sqlite3_vtab* pVtab, sqlite3_index_info* info)
{
    OroVtab* vtab = static_cast<OroVtab*>(pVtab);
    double est_rows = (double)vtab->est_rows;

    // Scan constraints looking for ones on the PK (rowid) column
    int eq_idx = -1;      // constraint index for EQ
    int ge_idx = -1;      // constraint index for GE/GT
    int le_idx = -1;      // constraint index for LE/LT

    for (int i = 0; i < info->nConstraint; i++) {
        if (!info->aConstraint[i].usable)
            continue;

        int col = info->aConstraint[i].iColumn;

        // iColumn == -1 means rowid, or it's our PK column
        bool is_pk = (col == -1 || col == vtab->pk_col);
        if (!is_pk)
            continue;

        switch (info->aConstraint[i].op) {
            case SQLITE_INDEX_CONSTRAINT_EQ:
                eq_idx = i;
                break;
            case SQLITE_INDEX_CONSTRAINT_GT:
            case SQLITE_INDEX_CONSTRAINT_GE:
                ge_idx = i;
                break;
            case SQLITE_INDEX_CONSTRAINT_LT:
            case SQLITE_INDEX_CONSTRAINT_LE:
                le_idx = i;
                break;
            default:
                break;
        }
    }

    // Choose the best plan
    int argv_slot = 1;  // 1-based

    if (eq_idx >= 0) {
        // Point lookup — best case
        info->idxNum = 1;
        info->estimatedCost = 1.0;
        info->estimatedRows = 1;
        info->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
        info->aConstraintUsage[eq_idx].argvIndex = argv_slot++;
        info->aConstraintUsage[eq_idx].omit = 1;

    } else if (ge_idx >= 0 && le_idx >= 0) {
        // Range scan with both bounds
        info->idxNum = 2;
        info->estimatedCost = est_rows > 10.0 ? est_rows / 10.0 : 1.0;
        info->estimatedRows = (sqlite3_int64)(est_rows / 10.0);
        if (info->estimatedRows < 1) info->estimatedRows = 1;
        info->aConstraintUsage[ge_idx].argvIndex = argv_slot++;  // argv[0] = lower bound
        info->aConstraintUsage[ge_idx].omit = 1;
        info->aConstraintUsage[le_idx].argvIndex = argv_slot++;  // argv[1] = upper bound
        info->aConstraintUsage[le_idx].omit = 1;

    } else if (ge_idx >= 0) {
        // Lower bound only
        info->idxNum = 3;
        info->estimatedCost = est_rows / 2.0;
        info->estimatedRows = (sqlite3_int64)(est_rows / 2.0);
        if (info->estimatedRows < 1) info->estimatedRows = 1;
        info->aConstraintUsage[ge_idx].argvIndex = argv_slot++;
        info->aConstraintUsage[ge_idx].omit = 1;

    } else if (le_idx >= 0) {
        // Upper bound only
        info->idxNum = 4;
        info->estimatedCost = est_rows / 2.0;
        info->estimatedRows = (sqlite3_int64)(est_rows / 2.0);
        if (info->estimatedRows < 1) info->estimatedRows = 1;
        info->aConstraintUsage[le_idx].argvIndex = argv_slot++;
        info->aConstraintUsage[le_idx].omit = 1;

    } else {
        // Full table scan
        info->idxNum = 0;
        info->estimatedCost = est_rows;
        info->estimatedRows = (sqlite3_int64)est_rows;
    }

    // If ORDER BY matches the primary index order, tell SQLite we handle it
    if (info->nOrderBy == 1) {
        int ob_col = info->aOrderBy[0].iColumn;
        bool is_pk = (ob_col == -1 || ob_col == vtab->pk_col);
        if (is_pk && !info->aOrderBy[0].desc) {
            info->orderByConsumed = 1;
        }
    }

    return SQLITE_OK;
}
