/*
 * oro-db stub for knl/knl_thread.h
 * Replaces openGauss thread-local context with standalone thread_local storage.
 */
#ifndef KNL_THREAD_H_STUB
#define KNL_THREAD_H_STUB

#include <cstdint>
#include <cstdio>
#include <cstdarg>
#include <cstring>

namespace MOT {
class SessionContext;
class TxnManager;
}

/* Error frame constants */
#define MOT_MAX_ERROR_FRAMES 8
#define MOT_MAX_ERROR_MESSAGE 256

/* Error frame structure (matches what mot_error.cpp expects) */
struct mot_error_frame {
    const char* m_function;
    const char* m_file;
    int m_line;
    int m_errorCode;
    int m_severity;
    const char* m_entity;
    const char* m_context;
    char m_errorMessage[MOT_MAX_ERROR_MESSAGE];
};

/* Truncated vsnprintf helper */
static inline int vsnprintf_truncated_s(char* dest, size_t destMax, const char* format, va_list args)
{
    int ret = vsnprintf(dest, destMax, format, args);
    if (ret < 0) ret = 0;
    return ret;
}

/* Truncated snprintf helper */
static inline int snprintf_truncated_s(char* dest, size_t destMax, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    int ret = vsnprintf(dest, destMax, format, args);
    va_end(args);
    if (ret < 0) ret = 0;
    return ret;
}

/* Forward-declare StringBuffer for log_line_buf pointer */
namespace MOT { struct StringBuffer; }

/* Log line buffer size — must match mot_log.cpp */
#define MOT_LOG_BUF_SIZE_KB 1
#define MOT_MAX_LOG_LINE_LENGTH (MOT_LOG_BUF_SIZE_KB * 1024 - 1)

/* MOT-specific thread-local context (replaces t_thrd.mot_cxt) */
struct knl_t_mot_context {
    uint16_t currentThreadId = (uint16_t)-1;
    int currentNumaNodeId = -2;  /* MEM_INVALID_NODE */
    int bindPolicy = 0;
    unsigned int mbindFlags = 0;

    /* Error handling */
    int last_error_code = 0;
    int last_error_severity = 0;
    mot_error_frame error_stack[MOT_MAX_ERROR_FRAMES] = {};
    unsigned error_frame_count = 0;

    /* Logging — matches openGauss knl_thread.h:
     *   char log_line[MOT_MAX_LOG_LINE_LENGTH + 1];
     *   int log_line_pos;
     *   bool log_line_overflow;
     *   MOT::StringBuffer* log_line_buf;
     */
    char log_line[MOT_MAX_LOG_LINE_LENGTH + 1] = {};
    int log_line_pos = 0;
    bool log_line_overflow = false;
    MOT::StringBuffer* log_line_buf = nullptr;
};

/* Thread-local context structure (replaces t_thrd) */
struct knl_thrd_context {
    knl_t_mot_context mot_cxt;
};

/* The global thread-local variable */
extern thread_local knl_thrd_context t_thrd;

/* Initialization function stub */
static inline void knl_thread_mot_init()
{
    t_thrd.mot_cxt = {};
    t_thrd.mot_cxt.currentThreadId = (uint16_t)-1;
    t_thrd.mot_cxt.currentNumaNodeId = -2;  /* MEM_INVALID_NODE */
}

#endif /* KNL_THREAD_H_STUB */
