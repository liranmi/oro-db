/*
 * oro-db stub for utils/elog.h
 * Replaces openGauss/PostgreSQL error logging with fprintf.
 */
#ifndef ELOG_H_STUB
#define ELOG_H_STUB

#include <cstdio>
#include <cstdlib>
#include "postgres.h"

/* Simple elog implementation that prints to stderr */
#define elog(level, ...) \
    do { \
        if ((level) >= ERROR) { \
            fprintf(stderr, "ERROR: " __VA_ARGS__); \
            fprintf(stderr, "\n"); \
        } else if ((level) >= WARNING) { \
            fprintf(stderr, "WARNING: " __VA_ARGS__); \
            fprintf(stderr, "\n"); \
        } \
    } while (0)

#define ereport(level, rest) \
    do { \
        if ((level) >= ERROR) { \
            fprintf(stderr, "EREPORT level=%d\n", (level)); \
        } \
    } while (0)

#define errcode(code) (code)
#define errmsg(...) fprintf(stderr, __VA_ARGS__)

#endif /* ELOG_H_STUB */
