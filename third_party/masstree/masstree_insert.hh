/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
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
#ifndef MASSTREE_INSERT_HH
#define MASSTREE_INSERT_HH
#include "masstree_get.hh"
#include "masstree_split.hh"

namespace Masstree {

template <typename P>
bool tcursor<P>::find_insert(threadinfo& ti, bool & found)
{
    found = false;
    find_locked(ti);
    original_n_ = n_;
    original_v_ = n_->full_unlocked_version_value();

    // maybe we found it
    if (state_) {
        found = true;
        return true;
    }

    // otherwise mark as inserted but not present
    state_ = 2;

    // maybe we need a new layer
    if (kx_.p >= 0)
        /* Key with same prefix (of size ikey_size * layer height) was found but with different suffix. we need to create at least one new layer that will contain both keys
           n_.lv[kx_.p] will be replaced with pointer to a new lower layer and n_.keylenx_[kx.p] will be replaced with 128 (defined by layer_keylenx constant) */
        return make_new_layer(ti);

    // mark insertion if we are changing modification state
    if (unlikely(n_->modstate_ != leaf<P>::modstate_insert)) {
        masstree_invariant(n_->modstate_ == leaf<P>::modstate_remove);
        n_->mark_insert();
        n_->modstate_ = leaf<P>::modstate_insert;
    }

    // try inserting into this leaf
    if (n_->size() < n_->width) {
        kx_.p = permuter_type(n_->permutation_).back();
        /* don't inappropriately reuse position 0, which holds the ikey_bound. If this is the case, make_split will handle it.
           Before leaf's first split, ikey0_[0] (ikey_bound) might not contain the lower ikey value in the leaf as it doesn't have a parent yet. After the first split,
            The new leaf ikey0_[0] will contain the lower ikey value in the right leaf and will be used as a boundary in the parent internode.
           In case the key in slot 0 will be deleted, the ikey0_[0] ikey value will be kept (to avoid changing the parent's boundary) and entry 0 wont be used anymore.
           This rule has 2 exceptions:
             1. If leaf is the most left leaf in the btree which means ikey0_[0] is not used as a boundary. (!n_->prev_)
             2. If a new key, with ikey == ikey0_[0], is added. In this case, we can re-use slot 0 as we won't change the tree's structure. (n_->ikey_bound() == ka_.ikey()) */
        if (likely(kx_.p != 0) || !n_->prev_ || n_->ikey_bound() == ka_.ikey()) {
            // if n_->assign fails, we dont have enough space to place the suffix and we failed while allocating larger ksuffix.
            bool res = n_->assign(kx_.p, ka_, ti);
            if (!res)
                ti.set_last_error(MT_MERR_FIND_INSERT_ASSIGN_SUFFIX);
            return res;
        }
    }

    // otherwise we might need to split
    return make_split(ti);
}

template <typename P>
bool tcursor<P>::make_new_layer(threadinfo& ti) {
    key_type oka(n_->ksuf(kx_.p));
    ka_.shift();
    int kcmp = oka.compare(ka_);

    // Create a twig of nodes until the suffixes diverge
    // For each ikey_size bytes (currently 8) that matches in both key's suffixes, we will need to create a new layer
    leaf_type* twig_head = n_;
    leaf_type* twig_tail = n_;
    leaf_type* nl = nullptr;
    while (kcmp == 0) {
        nl = leaf_type::make_root(0, twig_tail, ti);
        if (!nl) {
            ti.set_last_error(MT_MERR_MAKE_NEW_LAYER_LEAF_ALLOC_1);
            goto make_new_layer_cleanup;
        }
        nl->assign_initialize_for_layer(0, oka);
        if (twig_head != n_)
            twig_tail->lv_[0] = nl;
        else
            twig_head = nl;
        nl->permutation_ = permuter_type::make_sorted(1);
        twig_tail = nl;
#ifndef MASSTREE_OBSOLETE_CODE
        new_nodes_.emplace_back(nl, nl->full_unlocked_version_value());
#endif
        oka.shift();
        ka_.shift();
        // Compare the ikey only. if ikey matches and one or more of the suffixes != 0, compare using suffix size
        kcmp = oka.compare(ka_);
    }

    // Estimate how much space will be required for keysuffixes
    size_t ksufsize;
    if (ka_.has_suffix() || oka.has_suffix())
        ksufsize = (std::max(0, ka_.suffix_length())
                    + std::max(0, oka.suffix_length())) * (n_->width / 2)
            + n_->iksuf_[0].overhead(n_->width);
    else
        ksufsize = 0;
    nl = leaf_type::make_root(ksufsize, twig_tail, ti);
    if (!nl) {
        ti.set_last_error(MT_MERR_MAKE_NEW_LAYER_LEAF_ALLOC_2);
        goto make_new_layer_cleanup;
    }
    // Even though the total ksuffix size was already provided to make_root, more memory might be allocated in assign_initialize calls
    //  as leaf internal suffix is bounded by 128 (+ 64 alignment).
    // We will hit this issue (for sure) if ka_.suffix_length() + oka.suffix_length() > 192, but might hit it also when ka_.suffix_length() + oka.suffix_length() > 128.
    if (!nl->assign_initialize(0, kcmp < 0 ? oka : ka_, ti)) {
        ti.set_last_error(MT_MERR_MAKE_NEW_LAYER_KSUFFIX_ALLOC_1);
        goto make_new_layer_cleanup;
    }

    if (!nl->assign_initialize(1, kcmp < 0 ? ka_ : oka, ti)) {
        ti.set_last_error(MT_MERR_MAKE_NEW_LAYER_KSUFFIX_ALLOC_2);
        goto make_new_layer_cleanup;
    }

    nl->lv_[kcmp > 0] = n_->lv_[kx_.p];
    nl->lock(*nl, ti.lock_fence(tc_leaf_lock));
    if (kcmp < 0)
        nl->permutation_ = permuter_type::make_sorted(1);
    else {
        permuter_type permnl = permuter_type::make_sorted(2);
        permnl.remove_to_back(0);
        nl->permutation_ = permnl.value();
    }
    // In a prior version, recursive tree levels and true values were
    // differentiated by a bit in the leafvalue. But this constrains the
    // values users could assign for true values. So now we use bits in
    // the key length, and changing a leafvalue from true value to
    // recursive tree requires two writes. How to make this work in the
    // face of concurrent lockless readers? Mark insertion so they
    // retry.
    n_->mark_insert();
    fence();
    if (twig_tail != n_)
        twig_tail->lv_[0] = nl;
    if (twig_head != n_)
        n_->lv_[kx_.p] = twig_head;
    else
        n_->lv_[kx_.p] = nl;
    n_->keylenx_[kx_.p] = n_->layer_keylenx;
    updated_v_ = n_->full_unlocked_version_value();
    n_->unlock();
    n_ = nl;
    kx_.i = kx_.p = kcmp < 0;
    return true;

make_new_layer_cleanup:
    // n_ was not updated yet. It contains the original key (without any change). it will be unlocked later on (in lp.finish)
    if (nl) {
        // nl is not connected yet to twig_tail. handle it seperatly
        nl->deallocate(ti);
        nl = nullptr;
    }

    // Leafs in leaf list (starts from twig_head) has no suffix. In addition, they are not connected to the masstree yet, so we dont need to hold any locks.
    if (twig_head != n_) {
        while (twig_head) {
            masstree_invariant(!twig_head->ksuf_);
            masstree_invariant(twig_head->size() == 1);
            masstree_invariant(twig_head->is_layer(0));
            masstree_invariant(twig_head->stable_annotated(ti.stable_fence()).is_root());
            leaf_type *next_layer_leaf = (leaf_type *)twig_head->lv_[0].layer();
            twig_head->lv_[0] = nullptr;
            // Remove it directly. no need to use rcu.
            ti.deallocate(twig_head, sizeof(*twig_head) /* Being ignored */, memtag_masstree_leaf);
            // Stop if we just finished to handle last leaf in list (twig_tail).
            // Validating that next_layer_leaf != null wont work as twig_tail->lv_[0] == twig_tail.
            twig_head = (twig_head == twig_tail) ? nullptr : next_layer_leaf;
        }
    }

    return false;
}

template <typename P>
void tcursor<P>::finish_insert()
{
    permuter_type perm(n_->permutation_);
    masstree_invariant(perm.back() == kx_.p);
    perm.insert_from_back(kx_.i);
    fence();
    n_->permutation_ = perm.value();
}

template <typename P>
inline void tcursor<P>::finish(int state, threadinfo& ti)
{
    if (state < 0 && state_ == 1) {
        if (finish_remove(ti))
            goto clean_ti;
    } else if (state > 0 && state_ == 2)
        finish_insert();
    // we finally know this!
    if (n_ == original_n_)
        updated_v_ = n_->full_unlocked_version_value();
#ifndef MASSTREE_OBSOLETE_CODE
    else
        new_nodes_.emplace_back(n_, n_->full_unlocked_version_value());
#endif
    n_->unlock();

clean_ti:
    ti.add_nodes_to_gc();
}

} // namespace Masstree
#endif
