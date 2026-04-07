#ifndef ORO_TPCC_QUERY_H
#define ORO_TPCC_QUERY_H

/*
 * TPC-C query parameter generation.
 * Generates random transaction parameters per TPC-C Clause 2.4–2.8.
 */

#include "tpcc_txn.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "bench_config.h"
#include "bench_util.h"

namespace oro::tpcc {

// Transaction type selection
enum class TxnType { NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL, CONSISTENCY };

inline TxnType PickTxnType(const BenchConfig& cfg, FastRandom& rng)
{
    double r = rng.NextDouble();

    // Consistency checks are taken off the top of the mix
    if (cfg.tpcc_consistency_pct > 0.0 && r < cfg.tpcc_consistency_pct)
        return TxnType::CONSISTENCY;

    // Scale remaining random into [0, 1) for the normal TPC-C mix
    double remaining = (r - cfg.tpcc_consistency_pct) / (1.0 - cfg.tpcc_consistency_pct);
    double cum = cfg.tpcc_new_order_pct;
    if (remaining < cum) return TxnType::NEW_ORDER;
    cum += cfg.tpcc_payment_pct;
    if (remaining < cum) return TxnType::PAYMENT;
    cum += cfg.tpcc_order_status_pct;
    if (remaining < cum) return TxnType::ORDER_STATUS;
    cum += cfg.tpcc_delivery_pct;
    if (remaining < cum) return TxnType::DELIVERY;
    return TxnType::STOCK_LEVEL;
}

// ======================================================================
// NewOrder parameters — Clause 2.4.1
// ======================================================================
inline NewOrderParams GenNewOrder(uint32_t num_warehouses, uint64_t home_w_id, FastRandom& rng)
{
    NewOrderParams p{};
    p.w_id = home_w_id;
    p.d_id = rng.NextUniform(1, DIST_PER_WARE);
    p.c_id = NURand(rng, NURAND_C_ID, 1, CUST_PER_DIST);
    p.ol_cnt = (uint32_t)rng.NextUniform(MIN_OL_CNT, MAX_OL_CNT);
    p.all_local = true;
    p.rollback = false;

    for (uint32_t i = 0; i < p.ol_cnt; i++) {
        // Clause 2.4.1.5: 1% of NewOrders use an unused item ID to trigger rollback
        if (i == p.ol_cnt - 1 && rng.NextUniform(1, 100) == 1) {
            p.items[i].ol_i_id = ITEM_COUNT + 1;  // invalid item
            p.rollback = true;
        } else {
            p.items[i].ol_i_id = NURand(rng, NURAND_OL_I_ID, 1, ITEM_COUNT);
        }

        // Clause 2.4.1.5: 1% of order-lines are remote (if > 1 warehouse)
        if (num_warehouses > 1 && rng.NextUniform(1, 100) == 1) {
            // Pick a different warehouse
            uint64_t remote_w;
            do {
                remote_w = rng.NextUniform(1, num_warehouses);
            } while (remote_w == home_w_id);
            p.items[i].ol_supply_w_id = remote_w;
            p.all_local = false;
        } else {
            p.items[i].ol_supply_w_id = home_w_id;
        }
        p.items[i].ol_quantity = rng.NextUniform(1, 10);
    }
    return p;
}

// ======================================================================
// Payment parameters — Clause 2.5.1
// ======================================================================
inline PaymentParams GenPayment(uint32_t num_warehouses, uint64_t home_w_id, FastRandom& rng)
{
    PaymentParams p{};
    p.w_id = home_w_id;
    p.d_id = rng.NextUniform(1, DIST_PER_WARE);
    p.h_amount = (double)rng.NextUniform(100, 500000) / 100.0;  // $1.00 to $5,000.00

    // Clause 2.5.1.2: 85% home warehouse, 15% remote
    if (num_warehouses > 1 && rng.NextUniform(1, 100) > 85) {
        uint64_t remote_w;
        do {
            remote_w = rng.NextUniform(1, num_warehouses);
        } while (remote_w == home_w_id);
        p.c_w_id = remote_w;
        p.c_d_id = rng.NextUniform(1, DIST_PER_WARE);
    } else {
        p.c_w_id = home_w_id;
        p.c_d_id = p.d_id;
    }

    // Clause 2.5.1.2: 60% by last name, 40% by ID
    p.by_last_name = (rng.NextUniform(1, 100) <= 60);
    if (p.by_last_name) {
        GenLastName((uint32_t)NURand(rng, NURAND_C_LAST, 0, 999), p.c_last, sizeof(p.c_last));
        p.c_id = 0;
    } else {
        p.c_id = NURand(rng, NURAND_C_ID, 1, CUST_PER_DIST);
        p.c_last[0] = '\0';
    }
    return p;
}

// ======================================================================
// OrderStatus parameters — Clause 2.6.1
// ======================================================================
inline OrderStatusParams GenOrderStatus(uint64_t home_w_id, FastRandom& rng)
{
    OrderStatusParams p{};
    p.w_id = home_w_id;
    p.d_id = rng.NextUniform(1, DIST_PER_WARE);

    // Clause 2.6.1.2: 60% by last name, 40% by ID
    p.by_last_name = (rng.NextUniform(1, 100) <= 60);
    if (p.by_last_name) {
        GenLastName((uint32_t)NURand(rng, NURAND_C_LAST, 0, 999), p.c_last, sizeof(p.c_last));
        p.c_id = 0;
    } else {
        p.c_id = NURand(rng, NURAND_C_ID, 1, CUST_PER_DIST);
        p.c_last[0] = '\0';
    }
    return p;
}

// ======================================================================
// Delivery parameters — Clause 2.7.1
// ======================================================================
inline DeliveryParams GenDelivery(uint64_t home_w_id, FastRandom& rng)
{
    DeliveryParams p{};
    p.w_id = home_w_id;
    p.o_carrier_id = rng.NextUniform(1, 10);
    return p;
}

// ======================================================================
// StockLevel parameters — Clause 2.8.1
// ======================================================================
inline StockLevelParams GenStockLevel(uint64_t home_w_id, FastRandom& rng)
{
    StockLevelParams p{};
    p.w_id = home_w_id;
    p.d_id = rng.NextUniform(1, DIST_PER_WARE);
    p.threshold = rng.NextUniform(10, 20);  // Clause 2.8.1.2
    return p;
}

}  // namespace oro::tpcc
#endif  // ORO_TPCC_QUERY_H
