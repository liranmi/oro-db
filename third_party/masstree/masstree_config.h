#ifndef MASSTREE_CONFIG_H
#define MASSTREE_CONFIG_H

#define HAVE_CXX_TEMPLATE_ALIAS 1
#define HAVE_INT64_T_IS_LONG 1
#define HAVE_SIZE_T_IS_UNSIGNED_LONG 1
#define HAVE_STD_HASH 1
#define HAVE_STD_IS_TRIVIALLY_COPYABLE 1
#define HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE 1
#define HAVE_SUPERPAGE 1
#define HAVE_TYPE_TRAITS 1
#define HAVE_UNALIGNED_ACCESS 0
#define HAVE___BUILTIN_CLZ 1
#define HAVE___BUILTIN_CLZL 1
#define HAVE___BUILTIN_CLZLL 1
#define HAVE___BUILTIN_CTZ 1
#define HAVE___BUILTIN_CTZL 1
#define HAVE___BUILTIN_CTZLL 1
#define HAVE___HAS_TRIVIAL_COPY 1
#define HAVE___HAS_TRIVIAL_DESTRUCTOR 1
#define HAVE___SYNC_BOOL_COMPARE_AND_SWAP 1
#define HAVE___SYNC_BOOL_COMPARE_AND_SWAP_8 1
#define HAVE___SYNC_FETCH_AND_ADD 1
#define HAVE___SYNC_FETCH_AND_ADD_8 1
#define HAVE___SYNC_FETCH_AND_OR 1
#define HAVE___SYNC_FETCH_AND_OR_8 1
#define HAVE___SYNC_VAL_COMPARE_AND_SWAP 1
#define HAVE___SYNC_VAL_COMPARE_AND_SWAP_8 1

/* Maximum key length */
#define MASSTREE_MAXKEYLEN 256U

#define SIZEOF_INT 4
#define SIZEOF_LONG 8
#define SIZEOF_LONG_LONG 8
#define SIZEOF_SHORT 2
#define WORDS_BIGENDIAN_SET 1

#define MASSTREE_OBSOLETE_CODE 1

#define masstree_invariant(x, ...) assert(x)
#define masstree_precondition(x, ...) assert(x)



#ifndef invariant
#define invariant masstree_invariant
#endif
#ifndef precondition
#define precondition masstree_precondition
#endif

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

#ifndef PRIu64
#if HAVE_SIZE_T_IS_UNSIGNED_LONG_LONG
#define PRIu64 "llu"
#else
#define PRIu64 "lu"
#endif
#endif


#endif  // MASSTREE_CONFIG_H
