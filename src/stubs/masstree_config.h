/*
 * oro-db bridge header: masstree_config.h
 * Includes masstree's generated config.h and defines MOT-specific constants.
 * This file is included by masstree_index.h before any masstree headers.
 */
#ifndef ORO_MASSTREE_CONFIG_H
#define ORO_MASSTREE_CONFIG_H

#include "config.h"   /* masstree's generated config.h */

/* MOT-specific masstree constants */

/* Maximum allocation sizes for masstree memory pools (per object type).
 * These values come from the openGauss masstree integration and depend on
 * the B-tree order (BTREE_ORDER = 15 for default nodeparams<15,15>). */
#define MAX_MEMTAG_MASSTREE_LEAF_ALLOCATION_SIZE      640
#define MAX_MEMTAG_MASSTREE_INTERNODE_ALLOCATION_SIZE  384

/* Key suffix allocation depends on the leaf width parameter */
#define MAX_MEMTAG_MASSTREE_KSUFFIXES_ALLOCATION_SIZE(lw) \
    ((lw) * (MASSTREE_MAXKEYLEN + 4) + 64)

/* Limbo group allocation */
#define MAX_MEMTAG_MASSTREE_LIMBO_GROUP_ALLOCATION_SIZE 4096

/* Error codes */
#define MT_MERR_OK 0
#define MT_MERR_GC_LAYER_REMOVAL_MAKE 1

/* Key alignment macro used by MOT */
#ifndef ALIGN8
#define ALIGN8(x) (((x) + 7) & ~7)
#endif

/* B-tree order for MasstreePrimaryIndex default_table_params */
#ifndef BTREE_ORDER
#define BTREE_ORDER 15
#endif

/* MAX_KEY_SIZE must match MASSTREE_MAXKEYLEN */
#ifndef MAX_KEY_SIZE
#define MAX_KEY_SIZE MASSTREE_MAXKEYLEN
#endif

/* mtSessionThreadInfo — forward-declare threadinfo so this extern works
 * even before kvthread.hh is fully included. */
class threadinfo;
extern __thread threadinfo* mtSessionThreadInfo;

#endif /* ORO_MASSTREE_CONFIG_H */
