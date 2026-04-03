/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2016 President and Fellows of Harvard College
 * Copyright (c) 2012-2016 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef KVTHREAD_HH
#define KVTHREAD_HH 1
#include "mtcounters.hh"
#include "compiler.hh"
#include "circular_int.hh"
#include "timestamp.hh"
#include "memdebug.hh"
#include <assert.h>
#include <pthread.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <vector>

enum {
    MT_MERR_OK = 0,
    // Errors that will cause operation failure. bad flows are handled
    MT_MERR_MAKE_SPLIT_PRE_ALLOC = 1,
    MT_MERR_MAKE_SPLIT_LEAF_ALLOC = 2,
    MT_MERR_MAKE_NEW_LAYER_LEAF_ALLOC_1 = 3,
    MT_MERR_MAKE_NEW_LAYER_LEAF_ALLOC_2 = 4,
    MT_MERR_MAKE_NEW_LAYER_KSUFFIX_ALLOC_1 = 5,
    MT_MERR_MAKE_NEW_LAYER_KSUFFIX_ALLOC_2 = 6,
    MT_MERR_FIND_INSERT_ASSIGN_SUFFIX = 7,
    MT_MERR_SPLIT_INTO_ASSIGN_INITALIZE_1 = 8,
    MT_MERR_SPLIT_INTO_ASSIGN_INITALIZE_2 = 9,
    MT_MERR_GC_LAYER_REMOVAL_MAKE = 10,
    MT_MERR_MAKE_SPLIT_ASSIGN_SUFFIX = 11,
    MT_MERR_MAKE_SPLIT_PERM_EXCHANGE = 12,

    // Errors that are being handled internally (Operation should succeed even if last error contains them)
    MT_MERR_NON_DISRUPTIVE_ERRORS = 15,
    MT_MERR_MAKE_INTERNODE_USE_RESERVED = 16,
    MT_MERR_MAKE_INTERNODE_USE_RESERVED_2 = 17,

    // We should not reach the following errors as they should be covered with other errors in more upper layer
    MT_MERR_NOT_RETURNED_TO_USER_ERRORS = 20,
    MT_MERR_ASSIGN_KSUF = 21,
    MT_MERR_MAKE_LEAF = 22,
    MT_MERR_MAKE_ROOT_LEAF = 23,
    MT_MERR_MAKE_INTERNODE = 24,
    MT_MERR_LEAF_ASSIGN = 25,
    MT_MERR_ASSIGN_INITALIZE_1 = 26,
    MT_MERR_ASSIGN_INITALIZE_2 = 27,

    // We should not reach the following errors
    MT_MERR_UNREACHABLE_ERRORS = 30,
    MT_MERR_MAKE_SPLIT_INTERNODE_ALLOC_NOT_EXPECTED,
    MT_MERR_MAKE_SPLIT_INTERNODE_ALLOC_NOT_EXPECTED_2,
    MT_MERR_MAKE_SPLIT_INTERNODE_ALLOC_EMPTY_PRE_ALLOC_NOT_EXPECTED,

    MT_MERR_NOT_IN_USE_LAST_ENTRY = 40
};

#define MAX_ALLOC_ERROR_TYPES MT_MERR_NOT_IN_USE_LAST_ENTRY


class threadinfo;
class loginfo;

extern __thread threadinfo * mtSessionThreadInfo;

typedef uint64_t mrcu_epoch_type;
typedef int64_t mrcu_signed_epoch_type;

extern volatile mrcu_epoch_type globalepoch;  // global epoch, updated regularly
extern volatile mrcu_epoch_type active_epoch;

// Memtags max allocation size
#define MAX_MEMTAG_MASSTREE_LEAF_ALLOCATION_SIZE        iceil(sizeof(leaf<P>) + 128, 64)
#define MAX_MEMTAG_MASSTREE_INTERNODE_ALLOCATION_SIZE   sizeof(internode<P>)
#define MAX_MEMTAG_MASSTREE_LIMBO_GROUP_ALLOCATION_SIZE sizeof(mt_limbo_group)

// Upper bound for the ksuffixes structure max size.
#define MAX_MEMTAG_MASSTREE_KSUFFIXES_ALLOCATION_SIZE(width) iceil_log2(leaf<P>::external_ksuf_type::safe_size(width, MASSTREE_MAXKEYLEN * width));

inline uint64_t ng_getGlobalEpoch() {
  return globalepoch;
}

typedef struct mt_limbo_group {
    typedef mrcu_epoch_type epoch_type;
    typedef mrcu_signed_epoch_type signed_epoch_type;

    struct mt_limbo_element {
        void* ptr_;
        union {
            memtag tag;
            epoch_type epoch;
        } u_;
    };

    enum { capacity = (4076 - sizeof(epoch_type) - sizeof(mt_limbo_group*)) / sizeof(mt_limbo_element) };
    unsigned head_;
    unsigned tail_;
    epoch_type epoch_;
    mt_limbo_group* next_;
    mt_limbo_element e_[capacity];
    mt_limbo_group()
        : head_(0), tail_(0), next_() {
    }
    epoch_type first_epoch() const {
        assert(head_ != tail_);
        return e_[head_].u_.epoch;
    }
    void push_back(void* ptr, memtag tag, mrcu_epoch_type epoch) {
        assert(tail_ + 2 <= capacity);
        if (head_ == tail_ || epoch_ != epoch) {
            e_[tail_].ptr_ = nullptr;
            e_[tail_].u_.epoch = epoch;
            epoch_ = epoch;
            ++tail_;
        }
        e_[tail_].ptr_ = ptr;
        e_[tail_].u_.tag = tag;
        ++tail_;
    }
    inline unsigned clean_until(threadinfo& ti, mrcu_epoch_type epoch_bound, unsigned count);
} mt_limbo_group;

template <int N> struct has_threadcounter {
    static bool test(threadcounter ci) {
        return unsigned(ci) < unsigned(N);
    }
};
template <> struct has_threadcounter<0> {
    static bool test(threadcounter) {
        return false;
    }
};

struct mrcu_callback {
    virtual ~mrcu_callback() {
    }
    virtual size_t operator()(bool drop_index) = 0;
    virtual void operator()(threadinfo& ti) = 0;
};

class alignas(64) threadinfo {
  public:
    enum {
        TI_MAIN, TI_PROCESS, TI_LOG, TI_CHECKPOINT
    };

    typedef struct rcu_entry {
        void* p;
        size_t sz;
        memtag tag;
    } rcu_entry_t;

    static threadinfo* allthreads;

    threadinfo* next() const {
        return next_;
    }

    static threadinfo* make(void * obj_mem, int purpose, int index, int rcu_max_free_count = 0);
    // XXX destructor

    // thread information
    int purpose() const {
        return purpose_;
    }
    int index() const {
        return index_;
    }
    loginfo* logger() const {
        return logger_;
    }
    void set_logger(loginfo* logger) {
        assert(!logger_ && logger);
        logger_ = logger;
    }

    // timestamps
    kvtimestamp_t operation_timestamp() const {
        return timestamp();
    }
    kvtimestamp_t update_timestamp() const {
        return ts_;
    }
    kvtimestamp_t update_timestamp(kvtimestamp_t x) const {
        if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
            // x might be a marker timestamp; ensure result is not
            ts_ = (x | 1) + 1;
        return ts_;
    }
    template <typename N> void observe_phantoms(N* n) {
        if (circular_int<kvtimestamp_t>::less(ts_, n->phantom_epoch_[0]))
            ts_ = n->phantom_epoch_[0];
    }

    // event counters
    void mark(threadcounter ci) {
        if (has_threadcounter<int(ncounters)>::test(ci))
            ++counters_[ci];
    }
    void mark(threadcounter ci, int64_t delta) {
        if (has_threadcounter<int(ncounters)>::test(ci))
            counters_[ci] += delta;
    }
    void set_counter(threadcounter ci, uint64_t value) {
        if (has_threadcounter<int(ncounters)>::test(ci))
            counters_[ci] = value;
    }
    bool has_counter(threadcounter ci) const {
        return has_threadcounter<int(ncounters)>::test(ci);
    }
    uint64_t counter(threadcounter ci) const {
        return has_threadcounter<int(ncounters)>::test(ci) ? counters_[ci] : 0;
    }

    struct accounting_relax_fence_function {
        threadinfo* ti_;
        threadcounter ci_;
        accounting_relax_fence_function(threadinfo* ti, threadcounter ci)
            : ti_(ti), ci_(ci) {
        }
        void operator()() {
            relax_fence();
            ti_->mark(ci_);
        }
    };
    /** @brief Return a function object that calls mark(ci); relax_fence().
     *
     * This function object can be used to count the number of relax_fence()s
     * executed. */
    accounting_relax_fence_function accounting_relax_fence(threadcounter ci) {
        return accounting_relax_fence_function(this, ci);
    }

    struct stable_accounting_relax_fence_function {
        threadinfo* ti_;
        stable_accounting_relax_fence_function(threadinfo* ti)
            : ti_(ti) {
        }
        template <typename V>
        void operator()(V v) {
            relax_fence();
            ti_->mark(threadcounter(tc_stable + (v.isleaf() << 1) + v.splitting()));
        }
    };
    /** @brief Return a function object that calls mark(ci); relax_fence().
     *
     * This function object can be used to count the number of relax_fence()s
     * executed. */
    stable_accounting_relax_fence_function stable_fence() {
        return stable_accounting_relax_fence_function(this);
    }

    accounting_relax_fence_function lock_fence(threadcounter ci) {
        return accounting_relax_fence_function(this, ci);
    }

    // memory allocation
    void* allocate(size_t sz, memtag tag, size_t * actual_size = NULL);

    // memory deallocation
    void deallocate(void* p, size_t sz, memtag tag);

    void deallocate_rcu(void* p, size_t sz, memtag tag) {
        assert(p);
        dealloc_rcu.push_back({p, sz, tag});
    }

    void* pool_allocate(size_t sz, memtag tag) {
        void* p = NULL;
        int nl = (sz + memdebug_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
        if (use_pool()) {
            masstree_invariant(false); // internal memory pool is currently disabled
            assert(nl <= pool_max_nlines);
            if (unlikely(!pool_[nl - 1]))
                refill_pool(nl);
            p = pool_[nl - 1];
            if (p) {
                pool_[nl - 1] = *reinterpret_cast<void **> (p);
                p = memdebug::make(p, sz, memtag(tag + nl));
                mark(threadcounter(tc_alloc + (tag > memtag_value)),
                    nl * CACHE_LINE_SIZE);
            }
        } else {
            p = allocate(sz, tag);
            p = memdebug::make(p, sz, memtag(tag + nl));
            mark(threadcounter(tc_alloc + (tag > memtag_value)),
                 nl * CACHE_LINE_SIZE);
        }
        return p;
    }
    void pool_deallocate(void* p, size_t sz, memtag tag) {
        int nl = (sz + memdebug_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
        assert(p && nl <= pool_max_nlines);
        p = memdebug::check_free(p, sz, memtag(tag + nl));
        if (use_pool()) {
            masstree_invariant(false); // internal memory pool is currently disabled
            *reinterpret_cast<void **>(p) = pool_[nl - 1];
            pool_[nl - 1] = p;
        } else
            deallocate(p, sz, tag); // external memory pool deallocation
        mark(threadcounter(tc_alloc + (tag > memtag_value)),
             -nl * CACHE_LINE_SIZE);
    }
    void pool_deallocate_rcu(void* p, size_t sz, memtag tag) {
        if (unlikely(use_pool())) {
          int nl = (sz + memdebug_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
          assert(p && nl <= pool_max_nlines);
          memdebug::check_rcu(p, sz, memtag(tag + nl));
          mark(threadcounter(tc_alloc + (tag > memtag_value)),
               -nl * CACHE_LINE_SIZE);
          dealloc_rcu.push_back({p, sz, memtag(tag + nl)});
        } else {
          dealloc_rcu.push_back({p, sz, tag});
        }
    }

    void add_nodes_to_gc() {
        for (uint32_t i = 0 ; i < dealloc_rcu.size() ; i++) {
            masstree_invariant(dealloc_rcu[i].p);
            record_rcu(dealloc_rcu[i].p, dealloc_rcu[i].sz, dealloc_rcu[i].tag);
            dealloc_rcu[i].p = nullptr;
        }
        dealloc_rcu.clear();
    }

    // RCU
    void rcu_start() {
        if (gc_epoch_ != ng_getGlobalEpoch())
            gc_epoch_ = ng_getGlobalEpoch();
    }
    void rcu_end() {
        gc_epoch_ = 0;
    }
    void rcu_stop() {
        if (perform_gc_epoch_ != active_epoch)
            hard_rcu_quiesce();
        gc_epoch_ = 0;
    }
    void rcu_quiesce() {
        rcu_start();
        if (perform_gc_epoch_ != active_epoch)
            hard_rcu_quiesce();
    }
    typedef ::mrcu_callback mrcu_callback;
    void rcu_register(mrcu_callback* cb, size_t size) {
        record_rcu(cb, size, memtag_masstree_gc);
    }

    // thread management
    pthread_t& pthread() {
        return pthreadid_;
    }
    pthread_t pthread() const {
        return pthreadid_;
    }

    inline void set_last_error(int error) { masstree_invariant(error < MT_MERR_UNREACHABLE_ERRORS); last_error = error; }
    inline int get_last_error() { return last_error; }
    inline bool non_disruptive_error() { return last_error == 0 ||
                                     (last_error > MT_MERR_NON_DISRUPTIVE_ERRORS && last_error < MT_MERR_NOT_RETURNED_TO_USER_ERRORS); }

    void report_rcu(void* ptr) const;
    static void report_rcu_all(void* ptr);
    static inline mrcu_epoch_type min_active_epoch();

    void set_rcu_free_count(int rcu_count) { rcu_free_count = rcu_count; }
    int get_rcu_free_count() { return rcu_free_count; }

    void set_gc_session(void * gc_session);
    void * get_gc_session();

    inline uint32_t get_occupied_elements() { return total_limbo_inuse_elements; }

    void set_working_index (void * index) { cur_working_index = index; }
    void * get_working_index () { return cur_working_index; }

    // This function is now used to defer between Masstree internal memory pool (use_pool == true) vs external memory pool (use_pool == false)
    // Masstree internal memory pool is currently disabled
    static bool use_pool() {
#if ENABLE_ASSERTIONS
        return !no_pool_value;
#else
        return false;
#endif
    }

    bool is_empty_rcu_array() {
        return dealloc_rcu.size() == 0;
    }

  private:
    void * cur_working_index;
    int last_error = MT_MERR_OK;
    std::vector<struct rcu_entry> dealloc_rcu;
    union {
        struct {
            mrcu_epoch_type gc_epoch_;
            mrcu_epoch_type perform_gc_epoch_;
            loginfo *logger_;

            threadinfo *next_;
            int purpose_;
            int index_;         // the index of a udp, logging, tcp,
                                // checkpoint or recover thread

            pthread_t pthreadid_;
        };
        char padding1[CACHE_LINE_SIZE];
    };

    enum { pool_max_nlines = 20 };
    void* pool_[pool_max_nlines];
    int rcu_free_count;
    mt_limbo_group* limbo_head_;
    mt_limbo_group* limbo_tail_;
    void * gc_session_;
    uint32_t total_limbo_inuse_elements;

    mutable kvtimestamp_t ts_;

    //enum { ncounters = (int) tc_max };
    enum { ncounters = 0 };
    uint64_t counters_[ncounters];
    uint64_t insertions_ = 0;

    void refill_pool(int nl) { assert(0); }
    void refill_rcu() { assert(0); }

    void free_rcu(void *p, memtag tag) {
        if ((tag & memtag_pool_mask) == 0) {
            p = memdebug::check_free_after_rcu(p, tag);
            ::free(p);
        } else if (tag == memtag(-1))
            (*static_cast<mrcu_callback*>(p))(*this);
        else {
            p = memdebug::check_free_after_rcu(p, tag);
            int nl = tag & memtag_pool_mask;
            *reinterpret_cast<void**>(p) = pool_[nl - 1];
            pool_[nl - 1] = p;
        }
    }

    void ng_record_rcu(void* ptr, int size, memtag tag);

    void record_rcu(void* ptr, int size, memtag tag) {
      if (unlikely(use_pool())) {
        masstree_invariant(false); // internal memory pool is currently disabled
        if (limbo_tail_->tail_ + 2 > limbo_tail_->capacity)
          refill_rcu();
        uint64_t epoch = ng_getGlobalEpoch();
        limbo_tail_->push_back(ptr, tag, epoch);
        ++total_limbo_inuse_elements;
      } else {
        ng_record_rcu(ptr, size, tag);
      }
    }

#if ENABLE_ASSERTIONS
    static int no_pool_value;
#endif

    inline threadinfo(int purpose, int index, int rcu_max_free_count);
    threadinfo(const threadinfo&) = delete;
    ~threadinfo() {}
    threadinfo& operator=(const threadinfo&) = delete;

    void hard_rcu_quiesce();

    friend struct mt_limbo_group;
};

inline mrcu_epoch_type threadinfo::min_active_epoch() {
    mrcu_epoch_type ae = ng_getGlobalEpoch();
    for (threadinfo* ti = allthreads; ti; ti = ti->next()) {
        prefetch((const void*) ti->next());
        mrcu_epoch_type te = ti->gc_epoch_;
        if (te && mrcu_signed_epoch_type(te - ae) < 0)
            ae = te;
    }
    return ae;
}

#endif
