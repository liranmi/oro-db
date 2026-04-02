/*
 * oro-db masstree adapter
 * Bridges upstream kohler/masstree-beta threadinfo to MOT's expectations.
 * Provides mtSessionThreadInfo and the MOT-overloaded threadinfo::make.
 */
#ifdef ORO_HAS_MASSTREE

#include "kvthread.hh"
#include <new>

/* The per-thread threadinfo used by all MOT masstree operations */
__thread threadinfo* mtSessionThreadInfo = nullptr;

/* Global epoch counter for RCU (declared extern in kvthread.hh) */
relaxed_atomic<mrcu_epoch_type> globalepoch;

/* Active epoch (declared extern in kvthread.hh) */
relaxed_atomic<mrcu_epoch_type> active_epoch;

/* MOT calls threadinfo::make(obj_mem, purpose, index, rcu_max_free_count).
 * When rcu_max_free_count == -1, destroy the threadinfo.
 * Otherwise create a new one (obj_mem is ignored for creation). */
threadinfo* threadinfo::make(void* obj_mem, int purpose, int index, int rcu_max_free_count)
{
    if (rcu_max_free_count == -1) {
        /* Destructor path — nothing to do for upstream threadinfo
         * (it's allocated via the standard make and cleaned up there) */
        return nullptr;
    }

    /* Create path — delegate to upstream make */
    return threadinfo::make(purpose, index);
}

/* 3-param constructor — same as 2-param, store rcu_free_count as metadata */
threadinfo::threadinfo(int purpose, int index, int)
    : threadinfo(purpose, index)
{
}

#endif /* ORO_HAS_MASSTREE */
