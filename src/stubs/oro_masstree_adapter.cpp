/*
 * oro-db masstree adapter
 * Implements threadinfo::make and threadinfo constructor for the
 * openEuler-patched masstree used by MOT.
 */
#ifdef ORO_HAS_MASSTREE

#include "kvthread.hh"
#include <new>
#include <cstdlib>
#include <cstring>

/* Global thread list head */
threadinfo* threadinfo::allthreads = nullptr;

#if ENABLE_ASSERTIONS
int threadinfo::no_pool_value;
#endif

/* threadinfo constructor — initialize all fields */
threadinfo::threadinfo(int purpose, int index, int rcu_max_free_count)
{
    purpose_ = purpose;
    index_ = index;
    rcu_free_count = rcu_max_free_count;
    gc_epoch_ = 0;
    perform_gc_epoch_ = 0;
    logger_ = nullptr;
    next_ = nullptr;
    pthreadid_ = 0;
    ts_ = 0;
    total_limbo_inuse_elements = 0;
    limbo_head_ = nullptr;
    limbo_tail_ = nullptr;
    gc_session_ = nullptr;
    cur_working_index = nullptr;
    last_error = MT_MERR_OK;
    insertions_ = 0;
    memset(pool_, 0, sizeof(pool_));
}

/* threadinfo::make — allocate and register a new threadinfo, or destroy one.
 * When rcu_max_free_count == -1, destroy the threadinfo pointed to by obj_mem.
 * Otherwise create a new one. */
threadinfo* threadinfo::make(void* obj_mem, int purpose, int index, int rcu_max_free_count)
{
    if (rcu_max_free_count == -1) {
        /* Destroy path */
        if (obj_mem) {
            threadinfo* ti = static_cast<threadinfo*>(obj_mem);
            ti->~threadinfo();
            free(ti);
        }
        return nullptr;
    }

    /* Create path */
    void* mem = malloc(sizeof(threadinfo));
    if (!mem)
        return nullptr;
    threadinfo* ti = new (mem) threadinfo(purpose, index, rcu_max_free_count);

    /* Register in global thread list */
    ti->next_ = allthreads;
    allthreads = ti;

    return ti;
}

/* threadinfo GC session accessors */
void threadinfo::set_gc_session(void* gc_session) { gc_session_ = gc_session; }
void* threadinfo::get_gc_session() { return gc_session_; }

/* Per-thread threadinfo used by all MOT masstree operations */
__thread threadinfo* mtSessionThreadInfo = nullptr;

/* threadinfo::allocate — allocate memory for masstree internal nodes.
 * In openGauss this goes through MOT memory pools; standalone uses malloc. */
void* threadinfo::allocate(size_t sz, memtag tag, size_t* actual_size)
{
    void* p = malloc(sz);
    if (p && actual_size)
        *actual_size = sz;
    return p;
}

/* threadinfo::deallocate — free masstree internal node memory */
void threadinfo::deallocate(void* p, size_t sz, memtag tag)
{
    free(p);
}

/* threadinfo::ng_record_rcu — record an object for RCU-deferred freeing.
 * In standalone mode with a single-session GC, we free immediately. */
void threadinfo::ng_record_rcu(void* ptr, int size, memtag tag)
{
    /* For standalone: just free directly. In production openGauss this
     * goes through the MOT GC epoch-based reclamation. */
    if (ptr) {
        if ((tag & memtag_pool_mask) == 0)
            free(ptr);
        /* Pool-tagged objects are freed via pool_deallocate path */
    }
}

#endif /* ORO_HAS_MASSTREE */
