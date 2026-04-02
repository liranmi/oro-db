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

/* Masstree threadinfo stub — mot_masstree_kvthread.cpp is excluded,
 * so we provide the minimal definitions here. */
class threadinfo;
__thread threadinfo* mtSessionThreadInfo = nullptr;
