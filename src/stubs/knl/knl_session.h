/*
 * oro-db stub for knl/knl_session.h
 * Replaces openGauss session context with standalone thread_local storage.
 */
#ifndef KNL_SESSION_H_STUB
#define KNL_SESSION_H_STUB

#include <cstdint>

namespace MOT {
class SessionContext;
class TxnManager;
class GcContext;
}

/* MOT-specific session-local context (replaces u_sess->mot_cxt) */
struct knl_u_mot_context {
    bool callbacks_set = false;
    uint32_t session_id = (uint32_t)-1;
    uint32_t connection_id = (uint32_t)-1;
    MOT::SessionContext* session_context = nullptr;
    MOT::TxnManager* txn_manager = nullptr;
};

/* Session context structure (replaces u_sess) */
struct knl_session_context {
    knl_u_mot_context mot_cxt;
};

/* The global thread-local session variable */
extern thread_local knl_session_context* u_sess;

/* Initialization function stub */
static inline void knl_u_mot_init(knl_u_mot_context* ctx)
{
    ctx->callbacks_set = false;
    ctx->session_id = (uint32_t)-1;
    ctx->connection_id = (uint32_t)-1;
    ctx->session_context = nullptr;
    ctx->txn_manager = nullptr;
}

#endif /* KNL_SESSION_H_STUB */
