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
#ifndef KSEARCH_HH
#define KSEARCH_HH 1
#include "kpermuter.hh"

template <typename KA, typename T>
struct key_comparator {
    int operator()(const KA& ka, const T& n, int p) {
        return n.compare_key(ka, p);
    }
};

struct key_indexed_position {
    int i;
    int p;
    inline key_indexed_position() {
    }
    inline constexpr key_indexed_position(int i_, int p_)
        : i(i_), p(p_) {
    }
};


template <typename KA, typename T, typename F>
int key_upper_bound_by(const KA& ka, const T& n, F comparator)
{
    typename key_permuter<T>::type perm = key_permuter<T>::permutation(n);
    int l = 0, r = perm.size();
    while (l < r) {
        int m = (l + r) >> 1;
        int mp = perm[m];
        int cmp = comparator(ka, n, mp);
        if (cmp < 0)
            r = m;
        else if (cmp == 0)
            return m + 1;
        else
            l = m + 1;
    }
    return l;
}

template <typename KA, typename T>
inline int key_upper_bound(const KA& ka, const T& n)
{
    return key_upper_bound_by(ka, n, key_comparator<KA, T>());
}

// Binary search
template <typename KA, typename T, typename F>
key_indexed_position key_lower_bound_by(const KA& ka, const T& n, F comparator)
{
    typename key_permuter<T>::type perm = key_permuter<T>::permutation(n);
    int l = 0, r = perm.size();
    while (l < r) {
        int m = (l + r) >> 1;
        int mp = perm[m];
        int cmp = comparator(ka, n, mp);
        if (cmp < 0)
            r = m;
        else if (cmp == 0)
            return key_indexed_position(m, mp);
        else
            l = m + 1;
    }
    return key_indexed_position(l, -1);
}

template <typename KA, typename T>
inline key_indexed_position key_lower_bound(const KA& ka, const T& n)
{
    return key_lower_bound_by(ka, n, key_comparator<KA, T>());
}

/* For internode internal search:
   Return the index of the child (leaf) that might contain the provided ikey (ka)
   This is done be linear searching for the ikey that the provided ikey is the tightest upper boundary of (provided ikey is larger than the ikey in boundary array)
   e.g. internode - boundary ikeys array: ikey0_ = {40, 50, 100, 110} children in indexes 0, 1, 2, 3, 4
        For ka.ikey0_ == 100, 3 is returned because node in child_[3] might contain the ikey
        For ka.ikey0_ == 150, 4 is returned
        For ka.ikey0_ == 10, 0 is returned
        For ka.ikey0_ == 50, 2 is returned */

// Linear search
template <typename KA, typename T, typename F>
int key_find_upper_bound_by(const KA& ka, const T& n, F comparator)
{
    typename key_permuter<T>::type perm = key_permuter<T>::permutation(n);
    int l = 0, r = perm.size();
    while (l < r) {
        int lp = perm[l];
        int cmp = comparator(ka, n, lp);
        if (cmp < 0)
            break;
        else
            ++l;
    }
    return l;
}

/* In use in leafs for permutation search
   Find the ikey in node which equal or that the provided ikey is the tightest lower boundary of (provided ikey is smaller than the ikey in node). If match, also compare the keylen
   Return value: key_indexed_position which contains i and p variables
   i - Index inside the permutation. if key was found, index of the key
        If key was not found, index of the ikey which is tightest lower bounded by the provided key (ka)
        If no such exists, return perm.size() (invalid\not used location - currently outsize of the permutation)
   p - Position in child arrays ( == permutation[i]). if key was not found, p = -1
   e.g. Leaf - ikeys, perm { 0, 3, 1, 2 } ikey0_ = {40, 100, 110, 50}
        For ka.ikey0_ == 100, 110 is returned (ikey0_[2] == 110) --> i = 2, p = 3
        For ka.ikey0_ == 120, -1 is returned --> i = 4, p = -1
        For ka.ikey0_ == 85,  -1 is returned --> i = 1, p = -1 */

// Linear search
template <typename KA, typename T, typename F>
key_indexed_position key_find_lower_bound_by(const KA& ka, const T& n, F comparator)
{
    typename key_permuter<T>::type perm = key_permuter<T>::permutation(n);
    int l = 0, r = perm.size();
    while (l < r) {
        int lp = perm[l];
        int cmp = comparator(ka, n, lp);
        if (cmp < 0)
            break;
        else if (cmp == 0)
            return key_indexed_position(l, lp);
        else
            ++l;
    }
    return key_indexed_position(l, -1);
}


struct key_bound_binary {
    static constexpr bool is_binary = true;
    template <typename KA, typename T>
    static inline int upper(const KA& ka, const T& n) {
        return key_upper_bound_by(ka, n, key_comparator<KA, T>());
    }
    template <typename KA, typename T>
    static inline key_indexed_position lower(const KA& ka, const T& n) {
        return key_lower_bound_by(ka, n, key_comparator<KA, T>());
    }
    template <typename KA, typename T, typename F>
    static inline key_indexed_position lower_by(const KA& ka, const T& n, F comparator) {
        return key_lower_bound_by(ka, n, comparator);
    }
};

struct key_bound_linear {
    static constexpr bool is_binary = false;
    template <typename KA, typename T>
    static inline int upper(const KA& ka, const T& n) {
        return key_find_upper_bound_by(ka, n, key_comparator<KA, T>());
    }
    template <typename KA, typename T>
    static inline key_indexed_position lower(const KA& ka, const T& n) {
        return key_find_lower_bound_by(ka, n, key_comparator<KA, T>());
    }
    template <typename KA, typename T, typename F>
    static inline key_indexed_position lower_by(const KA& ka, const T& n, F comparator) {
        return key_find_lower_bound_by(ka, n, comparator);
    }
};


enum {
    bound_method_fast = 0,
    bound_method_binary,
    bound_method_linear
};
template <int max_size, int method = bound_method_fast> struct key_bound {};
template <int max_size> struct key_bound<max_size, bound_method_binary> {
    typedef key_bound_binary type;
};
template <int max_size> struct key_bound<max_size, bound_method_linear> {
    typedef key_bound_linear type;
};
template <int max_size> struct key_bound<max_size, bound_method_fast> {
    typedef typename key_bound<max_size, (max_size > 16 ? bound_method_binary : bound_method_linear)>::type type;
};

#endif
