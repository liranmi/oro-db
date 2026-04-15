/*
 * oro_mot_adapter.h - C-linkage interface between SQLite and MOT engine
 *
 * This is the bridge that lets sqlite3.c (modified) dispatch CURTYPE_MOT
 * cursor operations to the MOT in-memory engine.
 *
 * The adapter stores SQLite serialized records as opaque BLOBs in MOT,
 * keyed by rowid. This avoids the complexity of column-by-column type
 * translation while still using MOT's MassTree + OCC for storage.
 *
 * Schema for each MOT table (internal MOT representation):
 *   col 0: data    (BLOB)  - the SQLite serialized record
 *   col 1: rowid   (LONG)  - the SQLite rowid (also the InternalKey for the index)
 */

#ifndef ORO_MOT_ADAPTER_H
#define ORO_MOT_ADAPTER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque cursor handle */
typedef struct OroMotCursor OroMotCursor;

/* ============================================================
 * Engine lifecycle
 * ============================================================ */

/* Initialize the MOT engine. Returns 0 on success. */
int oroMotInit(const char* config_path);

/* Shut down the MOT engine. */
void oroMotShutdown(void);

/* Returns 1 if the engine is initialized, 0 otherwise. */
int oroMotIsInit(void);

/* ============================================================
 * Table registry (called from sqlite3.c during DDL)
 * ============================================================ */

/* Create a MOT-backed table identified by (iDb, table_name).
 * Returns 0 on success. */
int oroMotTableCreate(int iDb, const char* table_name);

/* Drop a MOT table. Returns 0 on success. */
int oroMotTableDrop(int iDb, const char* table_name);

/* Returns 1 if a MOT table exists for (iDb, table_name). */
int oroMotTableExists(int iDb, const char* table_name);

/* ============================================================
 * Connection / transaction management
 * ============================================================ */

/* Ensure a MOT session exists for the calling thread (per-connection).
 * The ConnectionId is opaque to SQLite; we use the sqlite3* pointer. */
int oroMotConnAttach(void* pDb);
int oroMotConnDetach(void* pDb);

/* Transaction control. The active connection is the most recent attach. */
int oroMotBegin(void* pDb);
int oroMotCommit(void* pDb);
int oroMotRollback(void* pDb);

/* Auto-commit helper: if the connection has an active transaction that was
 * auto-started by cursor operations, commit it. No-op if no active txn.
 * Called by VDBE halt when SQLite's db->autoCommit is set. */
int oroMotAutoCommit(void* pDb);

/* Returns 1 if the connection has an active transaction. */
int oroMotHasActiveTxn(void* pDb);

/* ============================================================
 * Cursor operations (called from VDBE opcode handlers)
 * ============================================================ */

/* Open a cursor on the MOT table identified by (iDb, table_name).
 * pDb is the sqlite3* connection.
 * On success, *ppCursor receives the new cursor. */
int oroMotCursorOpen(void* pDb, int iDb, const char* table_name, int wrFlag,
                     OroMotCursor** ppCursor);

/* Close and free a cursor. */
void oroMotCursorClose(OroMotCursor* pCur);

/* Position the cursor at the first row. *pEof = 1 if no rows. */
int oroMotFirst(OroMotCursor* pCur, int* pEof);

/* Position the cursor at the last row. *pEof = 1 if no rows. */
int oroMotLast(OroMotCursor* pCur, int* pEof);

/* Advance to next row. *pEof = 1 when past end. */
int oroMotNext(OroMotCursor* pCur, int* pEof);

/* Move to previous row. *pEof = 1 when past start. */
int oroMotPrev(OroMotCursor* pCur, int* pEof);

/* Seek by rowid. *pRes:
 *   0  = found exact match
 *   <0 = positioned BEFORE the target (cursor key < target)
 *   >0 = positioned AFTER the target (cursor key > target) */
int oroMotSeekRowid(OroMotCursor* pCur, int64_t rowid, int* pRes);

/* Get the rowid of the current row. */
int oroMotRowid(OroMotCursor* pCur, int64_t* pRowid);

/* Get the size of the row data (SQLite record bytes). */
int oroMotPayloadSize(OroMotCursor* pCur, uint32_t* pSize);

/* Read row data into pBuf. */
int oroMotRowData(OroMotCursor* pCur, uint32_t offset, uint32_t amount, void* pBuf);

/* Get a pointer to the row data (zero-copy if possible). */
const void* oroMotPayloadFetch(OroMotCursor* pCur, uint32_t* pAmt);

/* Insert a row. The cursor's table receives a new row with the given rowid
 * and data. Returns 0 on success, non-zero on error. */
int oroMotInsert(OroMotCursor* pCur, int64_t rowid,
                 const void* pData, int nData);

/* Delete the row at the cursor's current position. */
int oroMotDelete(OroMotCursor* pCur);

/* Count rows in the cursor's table. */
int oroMotCount(OroMotCursor* pCur, int64_t* pCount);

/* Returns 1 if the cursor is positioned past EOF, 0 otherwise. */
int oroMotEof(OroMotCursor* pCur);

#ifdef __cplusplus
}
#endif

#endif /* ORO_MOT_ADAPTER_H */
