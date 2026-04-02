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
#ifndef MASSTREE_HH
#define MASSTREE_HH
#include "compiler.hh"
#include "str.hh"
#include "ksearch.hh"
#include "mtcounters.hh"  // MOT: needed for memtag enum in basic_table::getMemtagMaxSize
#include <string>          // MOT: for basic_table::name_

namespace MOT { class Logger; }  // MOT: forward declaration for basic_table logger

namespace Masstree {
using lcdf::Str;
using lcdf::String;

class key_unparse_printable_string;
template <typename T> class value_print;

template <int LW = 15, int IW = LW> struct nodeparams {
    static constexpr int leaf_width = LW;
    static constexpr int internode_width = IW;
    static constexpr bool concurrent = true;
    static constexpr bool prefetch = true;
    static constexpr int bound_method = bound_method_binary;
    static constexpr int debug_level = 0;
    typedef uint64_t ikey_type;
    typedef uint32_t nodeversion_value_type;
    static constexpr bool need_phantom_epoch = true;
    typedef uint64_t phantom_epoch_type;
    static constexpr ssize_t print_max_indent_depth = 12;
    typedef key_unparse_printable_string key_unparse_type;
};

template <int LW, int IW> constexpr int nodeparams<LW, IW>::leaf_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::internode_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::debug_level;

template <typename P> class node_base;
template <typename P> class leaf;
template <typename P> class internode;
template <typename P> class leafvalue;
template <typename P> class key;
template <typename P> class basic_table;
template <typename P> class unlocked_tcursor;
template <typename P> class tcursor;

// Forward declarations for MOT iterator types
template <bool CONST_ITERATOR, bool FORWARD, typename P> class MasstreeIterator;

template <typename P>
class basic_table {
  public:
    typedef P parameters_type;
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef typename P::value_type value_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef unlocked_tcursor<P> unlocked_cursor_type;
    typedef tcursor<P> cursor_type;

    // MOT iterator typedefs
    typedef MasstreeIterator<false, true, P> ForwardIterator;
    typedef MasstreeIterator<false, false, P> ReverseIterator;

    // MOT destroy callback type
    typedef void (*destroy_value_cb_func)(void*);

    inline basic_table();

    void initialize(threadinfo& ti);
    void destroy(threadinfo& ti);

    inline node_type* root() const;
    inline node_type* fix_root();

    // MOT: accessor for root pointer (used by ReInitIndex)
    inline node_type** root_ref() { return &root_; }

    bool get(Str key, value_type& value, threadinfo& ti) const;

    template <typename F>
    int scan(Str firstkey, bool matchfirst, F& scanner, threadinfo& ti) const;
    template <typename F>
    int rscan(Str firstkey, bool matchfirst, F& scanner, threadinfo& ti) const;

    inline void print(FILE* f = 0) const;

    // --- MOT extensions ---
    // Implemented in mot_masstree.hpp, mot_masstree_insert.hpp, etc.
    bool init(uint16_t keyLength, const std::string& name, destroy_value_cb_func destroyValue_CB = nullptr);
    void find(const uint8_t* key, uint32_t key_len, void*& output, bool& found, const uint32_t& pid) const;
    void* insert(const uint8_t* key, uint32_t key_len, void* const& entry, bool& result, const uint32_t& pid);
    void* remove(uint8_t const* const& key, uint32_t length, bool& result, const uint32_t& pid);
    void iteratorScan(const char* keybuf, uint32_t keylen, const bool& matchKey, void* const& it,
                      const bool& forwardDirection, bool& result, const uint32_t& pid);
    int getMemtagMaxSize(enum memtag tag);

    // MOT members
    uint16_t keyLength_;
    std::string name_;
    destroy_value_cb_func destroyValue_CB_;

    // MOT: logger for IMPLEMENT_TEMPLATE_LOGGER
    static MOT::Logger _logger;

  // MOT: root_ made public for MasstreeIterator access
    node_type* root_;

  private:

    template <typename H, typename F>
    int scan(H helper, Str firstkey, bool matchfirst,
             F& scanner, threadinfo& ti) const;

    friend class unlocked_tcursor<P>;
    friend class tcursor<P>;
};

} // namespace Masstree
#endif
