/*
 * oro_coro.h — Minimal C++20 coroutine harness for interleaved (software
 * pipelined) index probing.
 *
 * Pointer-chasing through an in-memory index stalls on DRAM: each node
 * dereference can be a last-level-cache miss (~100ns). A single lookup cannot
 * hide that latency, but *independent* lookups can: issue a prefetch for the
 * next node of lookup A, then switch to lookup B (whose line is already warm)
 * while A's line loads. C++20 coroutines express this cleanly — each lookup is
 * a coroutine that co_awaits a prefetch (a suspension point), and a round-robin
 * scheduler drives a batch of them so their misses overlap.
 *
 * See Psaropoulos et al., "Interleaving with Coroutines" (VLDB 2017).
 */
#ifndef ORO_CORO_H
#define ORO_CORO_H

#include <coroutine>
#include <cstddef>
#include <cstdlib>
#include <utility>

namespace oro {
namespace coro {

// Per-thread bump arena for coroutine frames. A naive interleaving coroutine
// heap-allocates a frame per lookup (the Task escapes into the scheduler, so
// the compiler cannot elide it) — and that malloc costs about as much as the
// cache miss we are trying to hide. All coroutines in one batch are created,
// run, and destroyed together, so a bump allocator that is reset per batch
// makes frame allocation almost free. Frames larger than the arena fall back
// to malloc (and are freed individually).
struct FrameArena {
    static constexpr std::size_t kSize = 8u << 20;  // 8 MiB
    char* buf;
    std::size_t off;

    FrameArena() : buf(static_cast<char*>(std::malloc(kSize))), off(0) {}
    ~FrameArena() { std::free(buf); }

    void reset() noexcept { off = 0; }
    bool owns(void* p) const noexcept { return buf && p >= buf && p < buf + kSize; }

    void* alloc(std::size_t n) noexcept
    {
        n = (n + 15) & ~std::size_t(15);
        if (buf && off + n <= kSize) {
            void* p = buf + off;
            off += n;
            return p;
        }
        return std::malloc(n);  // fallback for oversized batches
    }
    void dealloc(void* p) noexcept
    {
        if (!owns(p)) {
            std::free(p);
        }
    }
};

inline FrameArena& frame_arena() noexcept
{
    static thread_local FrameArena arena;
    return arena;
}

// Reclaim all coroutine frames from the previous batch. Call before creating a
// new batch of tasks (safe once the previous batch's tasks are destroyed).
inline void reset_frame_arena() noexcept
{
    frame_arena().reset();
}

// Awaiter: prefetch a cache line for read, then suspend so the scheduler can
// advance another interleaved lookup while the line is fetched from memory.
struct Prefetch {
    const void* addr;
    bool await_ready() const noexcept { return false; }  // always suspend
    void await_suspend(std::coroutine_handle<>) const noexcept
    {
        if (addr != nullptr) {
            // rw=0 (read), locality=3 (keep in all cache levels)
            __builtin_prefetch(addr, 0, 3);
        }
    }
    void await_resume() const noexcept {}
};

inline Prefetch prefetch(const void* a) noexcept
{
    return Prefetch{a};
}

// Lazy, void-returning coroutine task. The lookup result is delivered through
// out-parameters captured by the coroutine body. The task starts suspended
// (initial_suspend = suspend_always) and is driven by run_interleaved().
struct Task {
    struct promise_type {
        Task get_return_object() noexcept
        {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { __builtin_trap(); }

        // Frames come from the per-thread bump arena (reset per batch) instead
        // of the heap, so interleaving is not dominated by malloc.
        static void* operator new(std::size_t n) noexcept { return frame_arena().alloc(n); }
        static void operator delete(void* p, std::size_t) noexcept { frame_arena().dealloc(p); }
        static void operator delete(void* p) noexcept { frame_arena().dealloc(p); }
    };

    using handle_type = std::coroutine_handle<promise_type>;
    handle_type h{};

    Task() noexcept = default;
    explicit Task(handle_type hh) noexcept : h(hh) {}
    Task(Task&& o) noexcept : h(o.h) { o.h = {}; }
    Task& operator=(Task&& o) noexcept
    {
        if (this != &o) {
            if (h) {
                h.destroy();
            }
            h = o.h;
            o.h = {};
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    ~Task()
    {
        if (h) {
            h.destroy();
        }
    }

    bool done() const noexcept { return !h || h.done(); }
    std::coroutine_handle<> handle() const noexcept { return h; }
};

// Round-robin scheduler. Each pass advances every still-running task to its
// next suspension point (a prefetch), so the following pass touches warm cache.
// Runs until all tasks have completed. Resuming a done() handle is undefined,
// so it is guarded.
inline void run_interleaved(std::coroutine_handle<>* hs, std::size_t n)
{
    bool any = true;
    while (any) {
        any = false;
        for (std::size_t i = 0; i < n; ++i) {
            std::coroutine_handle<> h = hs[i];
            if (h && !h.done()) {
                h.resume();
                if (!h.done()) {
                    any = true;
                }
            }
        }
    }
}

}  // namespace coro
}  // namespace oro

#endif  // ORO_CORO_H
