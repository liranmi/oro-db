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
#ifndef MASSTREE_GET_HH
#define MASSTREE_GET_HH
#include "masstree_tcursor.hh"
#include "masstree_key.hh"
#include "masstree_remove.hh"

namespace Masstree {

template <typename P>
bool unlocked_tcursor<P>::find_unlocked(threadinfo& ti)
{
    int match;
    key_indexed_position kx;
    node_base<P>* root = const_cast<node_base<P>*>(root_);

 retry:
    n_ = root->reach_leaf(ka_, v_, ti);

 forward:
    if (v_.deleted())
        goto retry;

    n_->prefetch();
    perm_ = n_->permutation();
    kx = leaf<P>::bound_type::lower(ka_, *this);
    if (kx.p >= 0) {
        lv_ = n_->lv_[kx.p];
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
    } else
        return match;
}

template <typename P>
inline bool basic_table<P>::get(Str key, value_type &value,
                                threadinfo& ti) const
{
    unlocked_tcursor<P> lp(*this, key);
    bool found = lp.find_unlocked(ti);
    if (found)
        value = lp.value();
    return found;
}

template <typename P>
bool tcursor<P>::find_locked(threadinfo& ti)
{
    node_base<P>* root = const_cast<node_base<P>*>(root_);
    nodeversion_type v;
    permuter_type perm;

 retry:
    n_ = root->reach_leaf(ka_, v, ti);

 forward:
    if (v.deleted())
        goto retry;

    n_->prefetch();
    perm = n_->permutation();
    fence();
    kx_ = leaf<P>::bound_type::lower(ka_, *n_);
    if (kx_.p >= 0) {
        // Key slice (ikey) was found and it is stored in kx_.i
        leafvalue<P> lv = n_->lv_[kx_.p];
        lv.prefetch(n_->keylenx_[kx_.p]);
        state_ = n_->ksuf_matches(kx_.p, ka_);
         // lv.layer() should be the root of the lower layer but might not be the root anymore. This case handled later in "else if (unlikely(state_ < 0)) {"
        if (state_ < 0 && !n_->has_changed(v) && lv.layer()->is_root()) {
            // Going down to lower layer as the ikey in this layer matches. --> The full key prefix matches (not only this slice) while suffixes don't match
            // (-state_) == the size of the ikey (in our case, 8 bytes)
            ka_.shift_by(-state_);
            // Change the current cursor root to point to the root of the lower layer and continue the search for the key from there
            root = lv.layer();
            goto retry;
        }
    } else
        state_ = 0;

    // n_ now points to the leaf where the key exists or should be added
    n_->lock(v, ti.lock_fence(tc_leaf_lock));
    if (n_->has_changed(v) || n_->permutation() != perm) {
        ti.mark(threadcounter(tc_stable_leaf_insert + n_->simple_has_split(v)));
        n_->unlock();
        // If the node has split, look for the leaf that should hold the key (if exists) by traversing between the leaves in the same layer (using next pointer)
        n_ = n_->advance_to_key(ka_, v, ti);
        goto forward;
    } else if (unlikely(state_ < 0)) {
        // n_->lv_[kx_.p] is a node in lower layer and should be a root but it is not anymore. It means that it already has a parent in lower layer. So the value is replaced with the lower layer node's parent.
        // The right thing to do was to replace value with the root of the lower layer. instead, it is done in iterations (level by level)
        ka_.shift_by(-state_);
        n_->lv_[kx_.p] = root = n_->lv_[kx_.p].layer()->maybe_parent();
        n_->unlock();
        goto retry;
    } else if (unlikely(n_->deleted_layer())) {
        // Layer was deleted. restart scan from table's root.
        ka_.unshift_all();
        root = const_cast<node_base<P>*>(root_);
        n_->unlock();
        goto retry;
    }
    return state_;
}

} // namespace Masstree
#endif
