/*
 * oro-db stub definitions
 * Provides the thread_local and global variables that replace openGauss kernel contexts.
 */
#include "knl/knl_thread.h"
#include "knl/knl_session.h"
#include "knl/knl_instance.h"

/* Thread-local openGauss replacement */
thread_local knl_thrd_context t_thrd = {};

/* Session-local openGauss replacement - each thread gets its own session */
static thread_local knl_session_context s_local_session = {};
thread_local knl_session_context* u_sess = &s_local_session;

/* Global instance config */
knl_g_instance g_instance = {};

/* Date/time formatting stubs (normally in FDW adapter layer) */
#include <cstdio>
#include <cstdint>
#include <cstddef>
uint16_t MOTDateToStr(uintptr_t val, char* buf, size_t len)
{
    return (uint16_t)snprintf(buf, len, "%lu", (unsigned long)val);
}
uint16_t MOTTimestampToStr(uintptr_t val, char* buf, size_t len)
{
    return (uint16_t)snprintf(buf, len, "%lu", (unsigned long)val);
}
uint16_t MOTTimestampTzToStr(uintptr_t val, char* buf, size_t len)
{
    return (uint16_t)snprintf(buf, len, "%lu", (unsigned long)val);
}

/* MTLS Recovery Manager stub — we excluded the .cpp.
 * Provide a stub .cpp with just enough to satisfy the vtable. */

/* Masstree threadinfo — when using real masstree, mot_masstree_kvthread.cpp
 * defines mtSessionThreadInfo. Otherwise we provide a stub. */
#ifndef ORO_HAS_MASSTREE
class threadinfo;
__thread threadinfo* mtSessionThreadInfo = nullptr;
#endif

/* Masstree assertion functions (declared in config.h / compiler.hh) */
#ifdef ORO_HAS_MASSTREE
#include <cstdlib>
#include <cstdio>
void fail_always_assert(const char* file, int line, const char* assertion, const char* message) {
    fprintf(stderr, "assertion \"%s\" failed: file \"%s\", line %d\n", assertion, file, line);
    if (message) fprintf(stderr, "  %s\n", message);
    abort();
}
void fail_masstree_invariant(const char* file, int line, const char* assertion, const char* message) {
    fail_always_assert(file, line, assertion, message);
}
void fail_masstree_precondition(const char* file, int line, const char* assertion, const char* message) {
    fail_always_assert(file, line, assertion, message);
}
#endif
