#ifndef ORO_YCSB_TXN_H
#define ORO_YCSB_TXN_H

/*
 * YCSB Transaction Execution for oro-db.
 *
 * Implements the six YCSB workload profiles (A-F) as defined in:
 *   Cooper et al., "Benchmarking Cloud Serving Systems with YCSB", SoCC 2010.
 *
 * Each call to RunYcsbTxn executes a single multi-operation transaction whose
 * operation mix is determined by the requested YcsbProfile.
 */

#include "txn.h"
#include "index.h"
#include "index_iterator.h"
#include "sentinel.h"
#include "row.h"
#include "ycsb_workload.h"
#include "ycsb_config.h"
#include "ycsb_generator.h"
#include "bench_config.h"
#include "bench_util.h"
#include "bench_stats.h"

namespace oro::ycsb {

// Run a single YCSB transaction according to the workload profile.
// Returns RC_OK on success.
MOT::RC RunYcsbTxn(MOT::TxnManager* txn, const YcsbTables& tables,
                   oro::YcsbProfile profile, uint32_t ops_per_txn,
                   uint32_t field_count, uint32_t field_length,
                   uint32_t scan_length, uint64_t record_count,
                   /* key generators - pass by pointer so caller owns them */
                   UniformGenerator* uniform_gen,
                   ZipfianGenerator* zipfian_gen,
                   oro::Distribution dist,
                   oro::FastRandom& rng,
                   uint32_t thread_id);

}  // namespace oro::ycsb
#endif  // ORO_YCSB_TXN_H
