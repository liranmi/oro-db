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
#include <unordered_map>

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
// Condition 7: For each new-order row, the corresponding order must
// have O_CARRIER_ID = 0 (undelivered).  Conversely, every order with
// O_CARRIER_ID = 0 must have a corresponding new-order row.
//
// SQL equivalent (should return 0 rows):
//   -- New-order rows whose order has a carrier (should be empty):
//   SELECT no.NO_O_ID
//     FROM new_order no
//     JOIN oorder o ON o.O_W_ID = no.NO_W_ID
//                  AND o.O_D_ID = no.NO_D_ID
//                  AND o.O_ID   = no.NO_O_ID
//    WHERE no.NO_W_ID = :w_id AND no.NO_D_ID = :d_id
//      AND o.O_CARRIER_ID != 0;
//
//   -- Open orders without a new-order row (should be empty):
//   SELECT o.O_ID
//     FROM oorder o
//    WHERE o.O_W_ID = :w_id AND o.O_D_ID = :d_id
//      AND o.O_CARRIER_ID = 0
//      AND NOT EXISTS (SELECT 1 FROM new_order no
//                       WHERE no.NO_W_ID = o.O_W_ID
//                         AND no.NO_D_ID = o.O_D_ID
//                         AND no.NO_O_ID = o.O_ID);
// ======================================================================
static ConsistencyResult CheckCondition7(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 7: New-order ↔ O_CARRIER_ID consistency";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            // Part A: every new-order row must have O_CARRIER_ID == 0
            RC rc = ScanByKeyColumn(txn, t.ix_new_order, NORD::NO_KEY,
                                    PackOrderKey(w, d, 1),
                                    PackOrderKey(w, d, 0xFFFFF),
                                    [&](Row* no_row) {
                uint64_t no_o_id;
                no_row->GetValue(NORD::NO_O_ID, no_o_id);

                RC orc = RC_OK;
                Row* o_row = Lookup(txn, t.order_tbl, t.ix_order,
                                    AccessType::RD,
                                    PackOrderKey(w, d, no_o_id), orc);
                if (!o_row || orc != RC_OK) {
                    res.passed = false;
                    char buf[256];
                    snprintf(buf, sizeof(buf),
                             "  W=%lu D=%lu NO_O_ID=%lu: order not found\n",
                             (unsigned long)w, (unsigned long)d,
                             (unsigned long)no_o_id);
                    res.detail += buf;
                    return true;
                }

                uint64_t carrier;
                o_row->GetValue(ORD::O_CARRIER_ID, carrier);
                if (carrier != 0) {
                    res.passed = false;
                    char buf[256];
                    snprintf(buf, sizeof(buf),
                             "  W=%lu D=%lu NO_O_ID=%lu: in new_order but O_CARRIER_ID=%lu\n",
                             (unsigned long)w, (unsigned long)d,
                             (unsigned long)no_o_id, (unsigned long)carrier);
                    res.detail += buf;
                }
                return true;
            });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": new_order scan failed\n";
            }

            // Part B: every order with O_CARRIER_ID == 0 must have a new-order row
            rc = ScanByKeyColumn(txn, t.ix_order, ORD::O_KEY,
                                 PackOrderKey(w, d, 1),
                                 PackOrderKey(w, d, 0xFFFFF),
                                 [&](Row* o_row) {
                uint64_t carrier;
                o_row->GetValue(ORD::O_CARRIER_ID, carrier);
                if (carrier != 0) return true;  // delivered — skip

                uint64_t o_id;
                o_row->GetValue(ORD::O_ID, o_id);

                RC norc = RC_OK;
                Row* no_row = Lookup(txn, t.new_order, t.ix_new_order,
                                     AccessType::RD,
                                     PackOrderKey(w, d, o_id), norc);
                if (!no_row) {
                    res.passed = false;
                    char buf[256];
                    snprintf(buf, sizeof(buf),
                             "  W=%lu D=%lu O_ID=%lu: O_CARRIER_ID=0 but no new_order row\n",
                             (unsigned long)w, (unsigned long)d,
                             (unsigned long)o_id);
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
// Condition 8: For each customer, C_DELIVERY_CNT must equal the number
// of orders delivered by the Delivery transaction during the benchmark.
// Population pre-delivers orders 1..2100 but initializes C_DELIVERY_CNT=0,
// so we only count delivered orders with O_ID >= 2101.
//
// SQL equivalent (should return 0 rows):
//   SELECT c.C_ID, c.C_DELIVERY_CNT, COUNT(o.O_ID) AS delivered
//     FROM customer c
//     LEFT JOIN oorder o ON o.O_W_ID = c.C_W_ID
//                       AND o.O_D_ID = c.C_D_ID
//                       AND o.O_C_ID = c.C_ID
//                       AND o.O_CARRIER_ID != 0
//                       AND o.O_ID >= 2101
//    WHERE c.C_W_ID = :w_id AND c.C_D_ID = :d_id
//    GROUP BY c.C_ID, c.C_DELIVERY_CNT
//   HAVING c.C_DELIVERY_CNT != COUNT(o.O_ID);
// ======================================================================
static ConsistencyResult CheckCondition8(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 8: C_DELIVERY_CNT = delivered order count";
    res.passed = true;

    for (uint64_t w = 1; w <= num_wh; w++) {
        for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
            // Scan orders >= 2101 for (w,d), count delivered orders per customer
            static constexpr uint64_t INIT_DELIVERED_END = 2101;
            std::unordered_map<uint64_t, uint64_t> delivered_cnt;
            RC rc = ScanByKeyColumn(txn, t.ix_order, ORD::O_KEY,
                                    PackOrderKey(w, d, INIT_DELIVERED_END),
                                    PackOrderKey(w, d, 0xFFFFF),
                                    [&](Row* o_row) {
                uint64_t carrier;
                o_row->GetValue(ORD::O_CARRIER_ID, carrier);
                if (carrier != 0) {
                    uint64_t o_c_id;
                    o_row->GetValue(ORD::O_C_ID, o_c_id);
                    delivered_cnt[o_c_id]++;
                }
                return true;
            });
            if (rc != RC_OK) {
                res.passed = false;
                res.detail += "  W=" + std::to_string(w) + " D=" + std::to_string(d)
                              + ": order scan failed\n";
                continue;
            }

            // Check each customer's C_DELIVERY_CNT
            for (uint64_t c = 1; c <= CUST_PER_DIST; c++) {
                RC crc = RC_OK;
                Row* c_row = Lookup(txn, t.customer, t.ix_customer,
                                    AccessType::RD,
                                    PackCustKey(w, d, c), crc);
                if (!c_row || crc != RC_OK) continue;

                uint64_t c_del_cnt;
                c_row->GetValue(CUST::C_DELIVERY_CNT, c_del_cnt);
                uint64_t expected = delivered_cnt.count(c) ? delivered_cnt[c] : 0;

                if (c_del_cnt != expected) {
                    res.passed = false;
                    char buf[256];
                    snprintf(buf, sizeof(buf),
                             "  W=%lu D=%lu C=%lu: C_DELIVERY_CNT=%lu but delivered orders=%lu\n",
                             (unsigned long)w, (unsigned long)d,
                             (unsigned long)c, (unsigned long)c_del_cnt,
                             (unsigned long)expected);
                    res.detail += buf;
                }
            }
        }
    }
    return res;
}

// ======================================================================
// Condition 9: For each order, O_OL_CNT must be between MIN_OL_CNT (5)
// and MAX_OL_CNT (15) inclusive, per TPC-C Clause 2.4.1.3.
//
// SQL equivalent (should return 0 rows):
//   SELECT o.O_ID, o.O_OL_CNT
//     FROM oorder o
//    WHERE o.O_W_ID = :w_id AND o.O_D_ID = :d_id
//      AND (o.O_OL_CNT < 5 OR o.O_OL_CNT > 15);
// ======================================================================
static ConsistencyResult CheckCondition9(TxnManager* txn, const TpccTables& t, uint32_t num_wh)
{
    ConsistencyResult res;
    res.name = "Condition 9: O_OL_CNT in [5..15]";
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

                if (ol_cnt < MIN_OL_CNT || ol_cnt > MAX_OL_CNT) {
                    res.passed = false;
                    char buf[256];
                    snprintf(buf, sizeof(buf),
                             "  W=%lu D=%lu O=%lu: O_OL_CNT=%lu (expected %u..%u)\n",
                             (unsigned long)w, (unsigned long)d,
                             (unsigned long)o_id, (unsigned long)ol_cnt,
                             MIN_OL_CNT, MAX_OL_CNT);
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
    CheckCondition7,
    CheckCondition8,
    CheckCondition9,
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
