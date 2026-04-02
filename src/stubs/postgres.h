/*
 * oro-db stub for postgres.h
 * Replaces openGauss/PostgreSQL postgres.h with minimal type definitions.
 */
#ifndef POSTGRES_H_STUB
#define POSTGRES_H_STUB

#include <cstdint>
#include <cstddef>
#include <cstdlib>

/* Prevent inclusion of sys/vtimes.h which doesn't exist on modern Linux */
#ifndef OPENEULER_MAJOR
#define OPENEULER_MAJOR 20
#endif

/* Basic PostgreSQL type definitions */
typedef unsigned int Oid;
typedef int64_t Datum;

#define PointerGetDatum(X) ((Datum)(X))
#define DatumGetPointer(X) ((void*)(X))

#define InvalidOid ((Oid)0)

/* Boolean type */
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

/* Error levels for elog stub */
#define DEBUG5 10
#define DEBUG4 11
#define DEBUG3 12
#define DEBUG2 13
#define DEBUG1 14
#define LOG 15
#define COMMERROR 16
#define INFO 17
#define NOTICE 18
#define WARNING 19
#define ERROR 20
#define FATAL 21
#define PANIC 22

/* Assert macros */
#ifdef USE_ASSERT_CHECKING
#include <cassert>
#define Assert(condition) assert(condition)
#else
#define Assert(condition) ((void)0)
#endif

#define AssertMacro(condition) ((void)0)

/* openGauss-specific strerror wrapper */
#include <cstring>
static inline char* gs_strerror(int errnum) { return strerror(errnum); }

/* on_proc_exit stub */
typedef void (*pg_on_exit_callback)(int code, unsigned long arg);
static inline void on_proc_exit(pg_on_exit_callback func, unsigned long arg) { (void)func; (void)arg; }

#endif /* POSTGRES_H_STUB */
