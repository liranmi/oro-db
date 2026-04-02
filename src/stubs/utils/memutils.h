/*
 * oro-db stub for utils/memutils.h
 * Replaces openGauss/PostgreSQL memory utilities.
 */
#ifndef MEMUTILS_H_STUB
#define MEMUTILS_H_STUB

#include <cstdlib>
#include <cstdint>

/* palloc/pfree stubs - not used in MOT core, but included for completeness */
static inline void* palloc(size_t size) { return malloc(size); }
static inline void* palloc0(size_t size) { return calloc(1, size); }
static inline void pfree(void* ptr) { free(ptr); }

#endif /* MEMUTILS_H_STUB */
