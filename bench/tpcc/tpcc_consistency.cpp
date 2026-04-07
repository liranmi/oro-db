/*
 * TPC-C Consistency Condition Checks
 *
 * Implements conditions 1–6 from TPC-C Standard Specification Rev 5.11, Clause 3.3.
 * Each check runs as a REPEATABLE_READ transaction for snapshot consistency.
 *
 * Range scans use ScanByKeyColumn which iterates the index and filters rows
 * by comparing the packed _KEY column value numerically against [start, end].
 */

#include "tpcc_consistency.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "bench_util.h"
#include "row.h"
#include "index.h"
#include "index_iterator.h"
#include "sentinel.h"
#include "key.h"

#include <cstdio>
#include <cstring>
#include <cmath>
#include <functional>
#include <string>

using namespace MOT;

namespace oro::tpcc {

// Point lookup helper (same pattern as tpcc_txn.cpp)
static Row* Lookup(TxnManager* txn, Table* table, Index* ix,
                   AccessType atype, uint64_t packed_key, RC& rc)
{
    Key* key = BuildSearchKey(txn, ix, packed_key);
    if (!key) { rc = RC_MEMORY_ALLOCATION_ERROR; return nullptr; }
    Row* row = txn->RowLookupByKey(table, atype, key, rc);
    txn->DestroyTxnKey(key);
    return row;
}

// ======================================================================
// Condition 1: W_YTD = SUM(D_YTD) for each warehouse
//
// SQL equivalent (per warehouse):
//   SELECT w.W_YTD, d_sum.total
//     FROM warehouse w,
//          (SELECT D_W_ID, SUM(D_YTD) AS total
//             FROM district
//            WHERE D_W_ID = :w_id
//            GROUP BY D_W_ID) d_sum
//    WHERE w.W_ID = :w_id
//      AND w.W_ID = d_sum.D_W_ID
//      AND w.W_YTD = d_sum.total;
// ======================================================================
static ConsistencyResult CheckCondition1(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 1: W_YTD = SUM(D_YTD)";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        RC rc = RC_OK;
        Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD, PackWhKey(w), rc);
        if (!w_row || rc != RC_OK) {
            res.passed = false;
            res.detail += "  W=" + std::to_string(w) + ": warehouse lookup failed\n";
            continue;
        }
        double w_ytd;
        w_row->GetValue(WH::W_YTD, w_ytd);

        double sum_d_ytd = 0.0;
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD,
                                PackDistKey(w, d), rc);
            if (!d_row || rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": district lookup failed\n";
                continue;
            }
            double d_ytd;
            d_row->GetValue(DIST::D_YTD, d_ytd);
            sum_d_ytd += d_ytd;
        }

        if (std::fabs(w_ytd - sum_d_ytd) > 0.01) {
            res.passed = false;
            char buf[256];
            snprintf(buf, sizeof(buf),
                     "  W=%lu: W_YTD=%.2f != SUM(D_YTD)=%.2f (diff=%.2f)\n",
                     (unsigned long)w, w_ytd, sum_d_ytd, w_ytd - sum_d_ytd);
            res.detail += buf;
        }
    }
    return res;
}

// ======================================================================
// Condition 2: D_NEXT_O_ID - 1 = MAX(O_ID) for each (w,d)
//
// SQL equivalent (per warehouse, district):
//   SELECT d.D_NEXT_O_ID, MAX(o.O_ID) AS max_oid
//     FROM district d
//     JOIN oorder o ON o.O_W_ID = d.D_W_ID AND o.O_D_ID = d.D_ID
//    WHERE d.D_W_ID = :w_id AND d.D_ID = :d_id
//    GROUP BY d.D_NEXT_O_ID
//   HAVING d.D_NEXT_O_ID - 1 = MAX(o.O_ID);
// ======================================================================
static ConsistencyResult CheckCondition2(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 2: D_NEXT_O_ID - 1 = MAX(O_ID)";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            RC rc = RC_OK;
            Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD,
                                PackDistKey(w, d), rc);
            if (!d_row || rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": district lookup failed\n";
                continue;
            }
            uint64_t d_next_o_id;
            d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);

            uint64_t max_o_id = 0;
            rc = ScanByKeyColumn(txn, t.ix_order, ORD::O_KEY,
                                 PackOrderKey(w, d, 1),
                                 PackOrderKey(w, d, 0xFFFFF),
                                 [&](Row* row) {
                                     uint64_t o_id;
                                     row->GetValue(ORD::O_ID, o_id);
                                     if (o_id > max_o_id) max_o_id = o_id;
                                     return true;
                                 });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": order scan failed\n";
                continue;
            }

            if (d_next_o_id - 1 != max_o_id) {
                res.passed = false;
                char buf[256];
                snprintf(buf, sizeof(buf),
                         "  W=%lu D=%lu: D_NEXT_O_ID-1=%lu != MAX(O_ID)=%lu\n",
                         (unsigned long)w, (unsigned long)d,
                         (unsigned long)(d_next_o_id - 1), (unsigned long)max_o_id);
                res.detail += buf;
            }
        }
    }
    return res;
}

// ======================================================================
// Condition 3: MAX(NO_O_ID) - MIN(NO_O_ID) + 1 = COUNT(*) in NEW_ORDER
//
// SQL equivalent (per warehouse, district):
//   SELECT MAX(NO_O_ID) - MIN(NO_O_ID) + 1 AS expected,
//          COUNT(*) AS actual
//     FROM new_order
//    WHERE NO_W_ID = :w_id AND NO_D_ID = :d_id
//   HAVING MAX(NO_O_ID) - MIN(NO_O_ID) + 1 = COUNT(*);
// ======================================================================
static ConsistencyResult CheckCondition3(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 3: New-order contiguity";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            uint64_t min_no = UINT64_MAX;
            uint64_t max_no = 0;
            uint64_t count = 0;

            RC rc = ScanByKeyColumn(txn, t.ix_new_order, NORD::NO_KEY,
                                    PackOrderKey(w, d, 1),
                                    PackOrderKey(w, d, 0xFFFFF),
                                    [&](Row* row) {
                                        uint64_t no_o_id;
                                        row->GetValue(NORD::NO_O_ID, no_o_id);
                                        if (no_o_id < min_no) min_no = no_o_id;
                                        if (no_o_id > max_no) max_no = no_o_id;
                                        count++;
                                        return true;
                                    });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": new_order scan failed\n";
                continue;
            }

            if (count == 0) continue;

            uint64_t expected = max_no - min_no + 1;
            if (expected != count) {
                res.passed = false;
                char buf[256];
                snprintf(buf, sizeof(buf),
                         "  W=%lu D=%lu: MAX-MIN+1=%lu != COUNT=%lu (min=%lu max=%lu)\n",
                         (unsigned long)w, (unsigned long)d,
                         (unsigned long)expected, (unsigned long)count,
                         (unsigned long)min_no, (unsigned long)max_no);
                res.detail += buf;
            }
        }
    }
    return res;
}

// ======================================================================
// Condition 4: SUM(O_OL_CNT) = COUNT(ORDER_LINE) for each (w,d)
//
// SQL equivalent (per warehouse, district):
//   SELECT o_agg.sum_cnt, ol_agg.cnt
//     FROM (SELECT SUM(O_OL_CNT) AS sum_cnt
//             FROM oorder
//            WHERE O_W_ID = :w_id AND O_D_ID = :d_id) o_agg,
//          (SELECT COUNT(*) AS cnt
//             FROM order_line
//            WHERE OL_W_ID = :w_id AND OL_D_ID = :d_id) ol_agg
//    WHERE o_agg.sum_cnt = ol_agg.cnt;
// ======================================================================
static ConsistencyResult CheckCondition4(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 4: SUM(O_OL_CNT) = COUNT(ORDER_LINE)";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            // Read D_NEXT_O_ID to bound the scan range properly
            RC rc = RC_OK;
            Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD,
                                PackDistKey(w, d), rc);
            if (!d_row || rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": district lookup failed\n";
                continue;
            }
            uint64_t d_next_o_id;
            d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);
            uint64_t max_o_id = d_next_o_id - 1;

            uint64_t sum_ol_cnt = 0;
            rc = ScanByKeyColumn(txn, t.ix_order, ORD::O_KEY,
                                 PackOrderKey(w, d, 1),
                                 PackOrderKey(w, d, max_o_id),
                                 [&](Row* row) {
                                     uint64_t ol_cnt;
                                     row->GetValue(ORD::O_OL_CNT, ol_cnt);
                                     sum_ol_cnt += ol_cnt;
                                     return true;
                                 });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": order scan failed\n";
                continue;
            }

            uint64_t ol_count = 0;
            rc = ScanByKeyColumn(txn, t.ix_order_line, ORDL::OL_KEY,
                                 PackOlKey(w, d, 1, 1),
                                 PackOlKey(w, d, max_o_id, MAX_OL_CNT),
                                 [&](Row*) {
                                     ol_count++;
                                     return true;
                                 });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": order_line scan failed\n";
                continue;
            }

            if (sum_ol_cnt != ol_count) {
                res.passed = false;
                char buf[256];
                snprintf(buf, sizeof(buf),
                         "  W=%lu D=%lu: SUM(O_OL_CNT)=%lu != COUNT(OL)=%lu\n",
                         (unsigned long)w, (unsigned long)d,
                         (unsigned long)sum_ol_cnt, (unsigned long)ol_count);
                res.detail += buf;
            }
        }
    }
    return res;
}

// ======================================================================
// Condition 5: Delivery flag consistency
//   For delivered orders (O_CARRIER_ID != 0): all OL_DELIVERY_D != 0
//   For open orders (O_CARRIER_ID == 0): all OL_DELIVERY_D == 0
//
// SQL equivalent (per warehouse, district — should return 0 rows):
//   -- Delivered orders with undelivered lines:
//   SELECT o.O_ID, ol.OL_NUMBER
//     FROM oorder o
//     JOIN order_line ol ON ol.OL_W_ID = o.O_W_ID
//                       AND ol.OL_D_ID = o.O_D_ID
//                       AND ol.OL_O_ID = o.O_ID
//    WHERE o.O_W_ID = :w_id AND o.O_D_ID = :d_id
//      AND o.O_CARRIER_ID != 0
//      AND ol.OL_DELIVERY_D = 0;
//
//   -- Open orders with delivery dates set:
//   SELECT o.O_ID, ol.OL_NUMBER
//     FROM oorder o
//     JOIN order_line ol ON ol.OL_W_ID = o.O_W_ID
//                       AND ol.OL_D_ID = o.O_D_ID
//                       AND ol.OL_O_ID = o.O_ID
//    WHERE o.O_W_ID = :w_id AND o.O_D_ID = :d_id
//      AND o.O_CARRIER_ID = 0
//      AND ol.OL_DELIVERY_D != 0;
// ======================================================================
static ConsistencyResult CheckCondition5(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 5: Delivery flag consistency";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            RC rc = ScanByKeyColumn(txn, t.ix_order, ORD::O_KEY,
                                    PackOrderKey(w, d, 1),
                                    PackOrderKey(w, d, 0xFFFFF),
                                    [&](Row* o_row) {
                uint64_t o_id, carrier, ol_cnt;
                o_row->GetValue(ORD::O_ID, o_id);
                o_row->GetValue(ORD::O_CARRIER_ID, carrier);
                o_row->GetValue(ORD::O_OL_CNT, ol_cnt);
                bool delivered = (carrier != 0);

                for (uint64_t ol = 1; ol <= ol_cnt; ol++) {
                    RC olrc = RC_OK;
                    Row* ol_row = Lookup(txn, t.order_line, t.ix_order_line,
                                         AccessType::RD,
                                         PackOlKey(w, d, o_id, ol), olrc);
                    if (!ol_row || olrc != RC_OK) continue;

                    int64_t del_d;
                    ol_row->GetValue(ORDL::OL_DELIVERY_D, del_d);

                    if (delivered && del_d == 0) {
                        res.passed = false;
                        char buf[256];
                        snprintf(buf, sizeof(buf),
                                 "  W=%lu D=%lu O=%lu OL=%lu: delivered but OL_DELIVERY_D=0\n",
                                 (unsigned long)w, (unsigned long)d,
                                 (unsigned long)o_id, (unsigned long)ol);
                        res.detail += buf;
                    } else if (!delivered && del_d != 0) {
                        res.passed = false;
                        char buf[256];
                        snprintf(buf, sizeof(buf),
                                 "  W=%lu D=%lu O=%lu OL=%lu: open but OL_DELIVERY_D=%ld\n",
                                 (unsigned long)w, (unsigned long)d,
                                 (unsigned long)o_id, (unsigned long)ol, (long)del_d);
                        res.detail += buf;
                    }
                }
                return true;
            });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": order scan failed\n";
            }
        }
    }
    return res;
}

// ======================================================================
// Condition 6: For each order, O_OL_CNT = actual count of its order-lines
//
// SQL equivalent (per warehouse, district — should return 0 rows):
//   SELECT o.O_ID, o.O_OL_CNT, COUNT(ol.OL_NUMBER) AS actual_cnt
//     FROM oorder o
//     LEFT JOIN order_line ol ON ol.OL_W_ID = o.O_W_ID
//                            AND ol.OL_D_ID = o.O_D_ID
//                            AND ol.OL_O_ID = o.O_ID
//    WHERE o.O_W_ID = :w_id AND o.O_D_ID = :d_id
//    GROUP BY o.O_ID, o.O_OL_CNT
//   HAVING o.O_OL_CNT != COUNT(ol.OL_NUMBER);
// ======================================================================
static ConsistencyResult CheckCondition6(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 6: O_OL_CNT = actual OL count per order";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            RC rc = ScanByKeyColumn(txn, t.ix_order, ORD::O_KEY,
                                    PackOrderKey(w, d, 1),
                                    PackOrderKey(w, d, 0xFFFFF),
                                    [&](Row* o_row) {
                uint64_t o_id, ol_cnt;
                o_row->GetValue(ORD::O_ID, o_id);
                o_row->GetValue(ORD::O_OL_CNT, ol_cnt);

                uint64_t actual_count = 0;
                for (uint64_t ol = 1; ol <= MAX_OL_CNT; ol++) {
                    RC olrc = RC_OK;
                    Row* ol_row = Lookup(txn, t.order_line, t.ix_order_line,
                                         AccessType::RD,
                                         PackOlKey(w, d, o_id, ol), olrc);
                    if (!ol_row) break;
                    actual_count++;
                }

                if (ol_cnt != actual_count) {
                    res.passed = false;
                    char buf[256];
                    snprintf(buf, sizeof(buf),
                             "  W=%lu D=%lu O=%lu: O_OL_CNT=%lu != actual=%lu\n",
                             (unsigned long)w, (unsigned long)d,
                             (unsigned long)o_id,
                             (unsigned long)ol_cnt, (unsigned long)actual_count);
                    res.detail += buf;
                }
                return true;
            });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": order scan failed\n";
            }
        }
    }
    return res;
}

// ======================================================================
// Dispatch table
// ======================================================================

using CheckFn = ConsistencyResult(*)(TxnManager*, const TpccTables&, uint32_t);

static const CheckFn g_checks[NUM_CONSISTENCY_CHECKS] = {
    CheckCondition1,
    CheckCondition2,
    CheckCondition3,
    CheckCondition4,
    CheckCondition5,
    CheckCondition6,
};

// ======================================================================
// Public API
// ======================================================================

RC RunConsistencyCheck(TxnManager* txn, const TpccTables& t,
                       uint32_t num_warehouses, uint32_t check_id,
                       ConsistencyResult& out)
{
    if (check_id < 1 || check_id > NUM_CONSISTENCY_CHECKS) {
        out.name = "Invalid";
        out.passed = false;
        out.detail = "check_id out of range";
        return RC_OK;
    }

    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::REPEATABLE_READ);

    out = g_checks[check_id - 1](txn, t, num_warehouses);

    RC rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

int RunAllConsistencyChecks(TxnManager* txn, const TpccTables& t,
                            uint32_t num_warehouses)
{
    int failures = 0;
    int passed = 0;

    for (uint32_t i = 1; i <= NUM_CONSISTENCY_CHECKS; i++) {
        ConsistencyResult result;
        RC rc = RunConsistencyCheck(txn, t, num_warehouses, i, result);

        if (rc != RC_OK) {
            fprintf(stderr, "    [FAIL] %s (transaction aborted)\n", result.name);
            failures++;
            continue;
        }

        if (result.passed) {
            printf("    [PASS] %s\n", result.name);
            passed++;
        } else {
            fprintf(stderr, "    [FAIL] %s\n", result.name);
            if (!result.detail.empty())
                fprintf(stderr, "%s", result.detail.c_str());
            failures++;
        }
    }

    if (failures == 0) {
        printf("    All %d consistency checks passed.\n", passed);
    } else {
        fprintf(stderr, "    %d/%d checks passed, %d FAILED.\n",
                passed, passed + failures, failures);
    }

    return failures;
}

}  // namespace oro::tpcc
