/*
 * oro_sqlite.h - SQLite virtual table interface for oro-db
 *
 * Exposes the oro-db in-memory transactional engine (MOT) as a SQLite
 * virtual table module.  This mirrors the openGauss FDW adapter layer:
 * SQLite handles SQL parsing & optimization, oro-db handles storage,
 * indexing (MassTree), and OCC transactions.
 *
 * Usage:
 *   // 1. Boot the MOT engine (once per process)
 *   oro_engine_init(NULL);  // or pass path to mot.conf
 *
 *   // 2. Open a SQLite connection and register the module
 *   sqlite3* db;
 *   sqlite3_open(":memory:", &db);
 *   oro_sqlite_register(db);
 *
 *   // 3. Create virtual tables via SQL
 *   sqlite3_exec(db, "CREATE VIRTUAL TABLE t USING oro("
 *                     "id INTEGER PRIMARY KEY, name TEXT, val REAL)", ...);
 *
 *   // 4. Use standard SQL: SELECT, INSERT, UPDATE, DELETE
 *
 *   // 5. Cleanup
 *   sqlite3_close(db);
 *   oro_engine_shutdown();
 */

#ifndef ORO_SQLITE_H
#define ORO_SQLITE_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initialize the MOT engine.  Must be called once before any
 * oro_sqlite_register() call.  Pass NULL to auto-detect mot.conf
 * next to the executable, or pass an explicit path.
 * Returns SQLITE_OK on success.
 */
int oro_engine_init(const char* config_path);

/*
 * Shut down the MOT engine.  Call after all sqlite3 connections
 * using oro tables have been closed.
 */
void oro_engine_shutdown(void);

/*
 * Register the "oro" virtual table module on a SQLite connection.
 * Returns SQLITE_OK on success.
 */
int oro_sqlite_register(sqlite3* db);

#ifdef __cplusplus
}
#endif

#endif /* ORO_SQLITE_H */
