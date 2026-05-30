/*
 * mot_masstree_get_coro.hpp — Coroutine-interleaved Masstree read descent.
 *
 * This is a faithful copy of unlocked_tcursor<P>::find_unlocked() (see
 * third_party/masstree/masstree_get.hh) with node_base<P>::reach_leaf() inlined
 * (see masstree_struct.hh) and prefetch+suspend points inserted before each
 * cold node dereference:
 *
 *   - before reading each internode child's version (the dominant per-level
 *     miss in a deep tree),
 *   - before reading the leaf body, and
 *   - before reading the value/sentinel object.
 *
 * The descent logic and the optimistic-concurrency (version validation / retry)
 * protocol are UNCHANGED — only suspension points are added. At each suspension
 * the round-robin scheduler (oro_coro.h) advances another interleaved lookup,
 * so the cache misses of independent probes overlap instead of stalling
 * serially. A single lookup's behaviour is identical to find_unlocked().
 */
#ifndef MOT_MASSTREE_GET_CORO_HPP
#define MOT_MASSTREE_GET_CORO_HPP

#include "masstree.hh"
#include "masstree_get.hh"
#include "oro_coro.h"

#include <cstdint>

namespace Masstree {

template <typename P>
::oro::coro::Task unlocked_tcursor<P>::find_unlocked_coro(
    threadinfo& ti, bool& found, value_type& out)
{
    int match;
    key_indexed_position kx;
    node_base<P>* root = const_cast<node_base<P>*>(root_);

 retry:
    {
        // --- node_base<P>::reach_leaf(), inlined with per-level prefetch ---
        const node_base<P>* n[2];
        typename node_base<P>::nodeversion_type v[2];
        unsigned sense = 0;
        n[sense] = root;

        // Find a non-stale local root.
        while (true) {
            v[sense] = n[sense]->stable_annotated(ti.stable_fence());
            if (v[sense].is_root())
                break;
            ti.mark(tc_root_retry);
            n[sense] = n[sense]->maybe_parent();
        }

        // Descend through internodes to a leaf.
        while (!v[sense].isleaf()) {
            const internode<P>* in = static_cast<const internode<P>*>(n[sense]);
            in->prefetch();
            int kp = internode<P>::bound_type::upper(ka_, *in);
            n[sense ^ 1] = in->child_[kp];
            if (!n[sense ^ 1])
                goto retry;

            // Prefetch the child node and yield: while this line is fetched,
            // the scheduler advances other interleaved lookups.
            co_await ::oro::coro::prefetch(n[sense ^ 1]);
            v[sense ^ 1] = n[sense ^ 1]->stable_annotated(ti.stable_fence());

            if (likely(!in->has_changed(v[sense]))) {
                sense ^= 1;
                continue;
            }

            typename node_base<P>::nodeversion_type oldv = v[sense];
            v[sense] = in->stable_annotated(ti.stable_fence());
            if (unlikely(oldv.has_split(v[sense]))
                && in->stable_last_key_compare(ka_, v[sense], ti) > 0) {
                ti.mark(tc_root_retry);
                goto retry;
            } else {
                ti.mark(tc_internode_retry);
            }
        }

        v_ = v[sense];
        n_ = const_cast<leaf<P>*>(static_cast<const leaf<P>*>(n[sense]));
    }

 forward:
    if (v_.deleted())
        goto retry;

    // Overlap the leaf-body miss.
    co_await ::oro::coro::prefetch(n_);
    n_->prefetch();
    perm_ = n_->permutation();
    kx = leaf<P>::bound_type::lower(ka_, *this);
    if (kx.p >= 0) {
        lv_ = n_->lv_[kx.p];
        // Overlap the value/sentinel miss (the cold object the caller reads).
        co_await ::oro::coro::prefetch(
            reinterpret_cast<const void*>(static_cast<std::uintptr_t>(lv_.value())));
        lv_.prefetch(n_->keylenx_[kx.p]);
        match = n_->ksuf_matches(kx.p, ka_);
    } else
        match = 0;
    if (n_->has_changed(v_)) {
        ti.mark(threadcounter(tc_stable_leaf_insert + n_->simple_has_split(v_)));
        n_ = n_->advance_to_key(ka_, v_, ti);
        goto forward;
    }

    if (match < 0) {
        ka_.shift_by(-match);
        root = lv_.layer();
        goto retry;
    }

    found = (match != 0);
    if (found) {
        out = lv_.value();
    }
    co_return;
}

}  // namespace Masstree

#endif  // MOT_MASSTREE_GET_CORO_HPP
