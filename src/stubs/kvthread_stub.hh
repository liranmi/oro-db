/*
 * oro-db stub for kvthread.hh (masstree library)
 * Minimal threadinfo class so MOT session/engine code compiles.
 */
#ifndef KVTHREAD_HH_STUB
#define KVTHREAD_HH_STUB

#include <cstdint>
#include <cstdlib>
#include <new>

class threadinfo {
public:
    enum { TI_MAIN = 0, TI_PROCESS, TI_LOG, TI_CHECKPOINT };

    static threadinfo* make(void* ti, int type, int threadId, int action)
    {
        if (action < 0) {
            delete static_cast<threadinfo*>(ti);
            return nullptr;
        }
        if (ti != nullptr) return static_cast<threadinfo*>(ti);
        return new (std::nothrow) threadinfo();
    }

    int index() const { return m_index; }
    void set_gc_session(void* s) { m_gc = s; }
    void* get_gc_session() { return m_gc; }
    void set_working_index(void* i) { m_idx = i; }
    void* get_working_index() { return m_idx; }

private:
    threadinfo() = default;
    int m_index = 0;
    void* m_gc = nullptr;
    void* m_idx = nullptr;
};

extern __thread threadinfo* mtSessionThreadInfo;

#endif /* KVTHREAD_HH_STUB */
