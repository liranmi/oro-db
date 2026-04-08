#ifndef ORO_TPCC_CONSISTENCY_H
#define ORO_TPCC_CONSISTENCY_H

/*
 * TPC-C Consistency Conditions (Clause 3.3)
 *
 * Each check can be run:
 *   1. Post-benchmark via RunAllConsistencyChecks() (--check flag)
 *   2. As a read-only transaction during the benchmark (--consistency-pct)
 *      to test MVCC snapshot correctness under concurrency.
 *
 * All checks use REPEATABLE_READ isolation for snapshot consistency.
 */

#include <cstdint>
#include <string>
#include "txn.h"
#include "tpcc_workload.h"

namespace oro::tpcc {

// Number of implemented consistency conditions
static constexpr uint32_t NUM_CONSISTENCY_CHECKS = 9;

// Result of a single consistency check
struct ConsistencyResult {
    const char* name   = nullptr;  // e.g., "Condition1_WarehouseYtd"
    bool        passed = false;
    std::string detail;            // failure details (empty if passed)
};

// Run a single consistency check as a transaction.
// check_id in [1..NUM_CONSISTENCY_CHECKS].
// Uses REPEATABLE_READ isolation.
// Returns RC_OK if the transaction committed; check result in `out`.
MOT::RC RunConsistencyCheck(MOT::TxnManager* txn, const TpccTables& t,
                            uint32_t num_warehouses, uint32_t check_id,
                            ConsistencyResult& out);

// Run ALL consistency checks (post-run validation).
// Returns total number of failures. Prints per-check results to stdout/stderr.
int RunAllConsistencyChecks(MOT::TxnManager* txn, const TpccTables& t,
                            uint32_t num_warehouses);

}  // namespace oro::tpcc
#endif  // ORO_TPCC_CONSISTENCY_H
