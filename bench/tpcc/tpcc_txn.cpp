/*
 * TPC-C Transaction Implementations for oro-db.
 *
 * All 5 transactions per TPC-C Standard Specification Rev 5.11, Clauses 2.4–2.8.
 */

#include "tpcc_txn.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "bench_util.h"
#include "row.h"
#include "index.h"
#include "index_iterator.h"
#include "sentinel.h"
#include "key.h"

#include <cstring>
#include <ctime>
#include <algorithm>

using namespace MOT;

namespace oro::tpcc {

// Lookup helper: build key, point lookup, destroy key
static Row* Lookup(TxnManager* txn, Table* table, Index* ix,
                   AccessType atype, uint64_t packed_key, RC& rc)
{
    Key* key = BuildSearchKey(txn, ix, packed_key);
    if (!key) { rc = RC_MEMORY_ALLOCATION_ERROR; return nullptr; }
    Row* row = txn->RowLookupByKey(table, atype, key, rc);
    txn->DestroyTxnKey(key);
    return row;
}

// Customer by-last-name lookup via sequential scan of C_ID 1..CUST_PER_DIST.
// Returns the C_ID at the midpoint (sorted by C_FIRST) per Clause 2.5.2.2.
// Customer by-last-name lookup using the secondary index ix_customer_last.
// The secondary key is PackCustLastKey(w_id, d_id, last_name_num).
// Non-unique: multiple customers share the same last name.
// We iterate all matching rows, sort by C_FIRST, pick midpoint per Clause 2.5.2.2.
static uint64_t FindCustomerByLastName(TxnManager* txn, const TpccTables& t,
                                        uint64_t w_id, uint64_t d_id,
                                        const char* c_last, RC& rc)
{
    uint32_t last_num = LastNameToNum(c_last);
    uint64_t sec_key_val = PackCustLastKey(w_id, d_id, last_num);

    // Build search key for secondary index
    Key* skey = BuildSearchKey(txn, t.ix_customer_last, sec_key_val);
    if (!skey) return 0;

    // Use Index::Search to get a forward iterator at the matching key
    uint32_t pid = txn->GetThdId();
    bool found = false;
    IndexIterator* it = t.ix_customer_last->Search(skey, true, true, pid, found);
    txn->DestroyTxnKey(skey);

    struct Hit { uint64_t c_id; char c_first[17]; };
    Hit hits[32];
    uint32_t n = 0;

    // Iterate matching sentinel chain — all rows with the same secondary key
    while (it != nullptr && it->IsValid() && n < 32) {
        Sentinel* sentinel = it->GetPrimarySentinel();
        if (!sentinel) break;
        Row* row = sentinel->GetData();
        if (!row) { it->Next(); continue; }

        // Verify it's the same key (iterator may advance past our key)
        uint64_t row_last_key; row->GetValue(CUST::C_LAST_KEY, row_last_key);
        if (row_last_key != sec_key_val) break;

        uint64_t c_id; row->GetValue(CUST::C_ID, c_id);
        hits[n].c_id = c_id;
        memcpy(hits[n].c_first, row->GetValue(CUST::C_FIRST), 16);
        hits[n].c_first[16] = '\0';
        n++;
        it->Next();
    }
    if (it) { it->Invalidate(); delete it; }

    if (n == 0) return 0;

    std::sort(hits, hits + n, [](const Hit& a, const Hit& b) {
        return strcmp(a.c_first, b.c_first) < 0;
    });
    return hits[(n - 1) / 2].c_id;
}

// ======================================================================
// 1. NewOrder — Clause 2.4
// ======================================================================
RC RunNewOrder(TxnManager* txn, const TpccTables& t, const NewOrderParams& p, FastRandom& rng)
{
    RC rc = RC_OK;
    (void)rng;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD, PackWhKey(p.w_id), rc);
    if (!w_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double w_tax; w_row->GetValue(WH::W_TAX, w_tax);

    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD_FOR_UPDATE,
                        PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double d_tax; d_row->GetValue(DIST::D_TAX, d_tax);
    uint64_t d_next_o_id; d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);
    rc = txn->UpdateRow(d_row, DIST::D_NEXT_O_ID, d_next_o_id + 1);
    if (rc != RC_OK) { txn->Rollback(); return rc; }
    uint64_t o_id = d_next_o_id;

    Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD,
                        PackCustKey(p.w_id, p.d_id, p.c_id), rc);
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

    // INSERT ORDER
    {
        Row* orow = t.order_tbl->CreateNewRow();
        if (!orow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        orow->SetValue<uint64_t>(ORD::O_C_ID, p.c_id);
        orow->SetValue<uint64_t>(ORD::O_D_ID, p.d_id);
        orow->SetValue<uint64_t>(ORD::O_W_ID, p.w_id);
        orow->SetValue<int64_t>(ORD::O_ENTRY_D, (int64_t)time(nullptr));
        orow->SetValue<uint64_t>(ORD::O_CARRIER_ID, 0);
        orow->SetValue<uint64_t>(ORD::O_OL_CNT, p.ol_cnt);
        orow->SetValue<uint64_t>(ORD::O_ALL_LOCAL, p.all_local ? 1 : 0);
        orow->SetValue<uint64_t>(ORD::O_ID, o_id);
        orow->SetInternalKey(ORD::O_KEY, PackOrderKey(p.w_id, p.d_id, o_id));
        rc = t.order_tbl->InsertRow(orow, txn);
        if (rc != RC_OK) { t.order_tbl->DestroyRow(orow); txn->Rollback(); return rc; }
    }

    // INSERT NEW_ORDER
    {
        Row* norow = t.new_order->CreateNewRow();
        if (!norow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        norow->SetValue<uint64_t>(NORD::NO_O_ID, o_id);
        norow->SetValue<uint64_t>(NORD::NO_D_ID, p.d_id);
        norow->SetValue<uint64_t>(NORD::NO_W_ID, p.w_id);
        norow->SetInternalKey(NORD::NO_KEY, PackOrderKey(p.w_id, p.d_id, o_id));
        rc = t.new_order->InsertRow(norow, txn);
        if (rc != RC_OK) { t.new_order->DestroyRow(norow); txn->Rollback(); return rc; }
    }

    for (uint32_t ol = 0; ol < p.ol_cnt; ol++) {
        uint64_t ol_i_id = p.items[ol].ol_i_id;
        uint64_t ol_supply_w_id = p.items[ol].ol_supply_w_id;
        uint64_t ol_quantity = p.items[ol].ol_quantity;

        Row* i_row = Lookup(txn, t.item, t.ix_item, AccessType::RD, PackItemKey(ol_i_id), rc);
        if (!i_row) { txn->Rollback(); return RC_OK; }
        double i_price; i_row->GetValue(ITEM::I_PRICE, i_price);

        Row* s_row = Lookup(txn, t.stock, t.ix_stock, AccessType::RD_FOR_UPDATE,
                            PackStockKey(ol_supply_w_id, ol_i_id), rc);
        if (!s_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

        uint64_t s_quantity; s_row->GetValue(STK::S_QUANTITY, s_quantity);
        s_quantity = (s_quantity >= ol_quantity + 10) ? s_quantity - ol_quantity : s_quantity - ol_quantity + 91;
        rc = txn->UpdateRow(s_row, STK::S_QUANTITY, s_quantity);
        if (rc != RC_OK) { txn->Rollback(); return rc; }
        Row* s_draft = txn->GetLastAccessedDraft();

        uint64_t s_ytd; s_row->GetValue(STK::S_YTD, s_ytd);
        s_draft->SetValue<uint64_t>(STK::S_YTD, s_ytd + ol_quantity);
        uint64_t s_order_cnt; s_row->GetValue(STK::S_ORDER_CNT, s_order_cnt);
        s_draft->SetValue<uint64_t>(STK::S_ORDER_CNT, s_order_cnt + 1);
        if (ol_supply_w_id != p.w_id) {
            uint64_t s_remote_cnt; s_row->GetValue(STK::S_REMOTE_CNT, s_remote_cnt);
            s_draft->SetValue<uint64_t>(STK::S_REMOTE_CNT, s_remote_cnt + 1);
        }

        char ol_dist_info[25] = {0};
        memcpy(ol_dist_info, s_row->GetValue(STK::S_DIST_01 + (int)(p.d_id - 1)), 24);

        Row* olrow = t.order_line->CreateNewRow();
        if (!olrow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        olrow->SetValue<uint64_t>(ORDL::OL_I_ID, ol_i_id);
        olrow->SetValue<uint64_t>(ORDL::OL_D_ID, p.d_id);
        olrow->SetValue<uint64_t>(ORDL::OL_W_ID, p.w_id);
        olrow->SetValue<uint64_t>(ORDL::OL_SUPPLY_W_ID, ol_supply_w_id);
        olrow->SetValue<int64_t>(ORDL::OL_DELIVERY_D, 0);
        olrow->SetValue<uint64_t>(ORDL::OL_QUANTITY, ol_quantity);
        olrow->SetValue<double>(ORDL::OL_AMOUNT, (double)ol_quantity * i_price);
        olrow->SetValueVariable(ORDL::OL_DIST_INFO, ol_dist_info, strlen(ol_dist_info) + 1);
        olrow->SetValue<uint64_t>(ORDL::OL_O_ID, o_id);
        olrow->SetValue<uint64_t>(ORDL::OL_NUMBER, (uint64_t)(ol + 1));
        olrow->SetInternalKey(ORDL::OL_KEY, PackOlKey(p.w_id, p.d_id, o_id, (uint64_t)(ol + 1)));
        rc = t.order_line->InsertRow(olrow, txn);
        if (rc != RC_OK) { t.order_line->DestroyRow(olrow); txn->Rollback(); return rc; }
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 2. Payment — Clause 2.5
// ======================================================================
RC RunPayment(TxnManager* txn, const TpccTables& t, const PaymentParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD_FOR_UPDATE, PackWhKey(p.w_id), rc);
    if (!w_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double w_ytd; w_row->GetValue(WH::W_YTD, w_ytd);
    rc = txn->UpdateRow(w_row, WH::W_YTD, w_ytd + p.h_amount);
    if (rc != RC_OK) { txn->Rollback(); return rc; }

    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD_FOR_UPDATE, PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double d_ytd; d_row->GetValue(DIST::D_YTD, d_ytd);
    rc = txn->UpdateRow(d_row, DIST::D_YTD, d_ytd + p.h_amount);
    if (rc != RC_OK) { txn->Rollback(); return rc; }

    // Customer lookup — by ID or by last name (Clause 2.5.2.2)
    Row* c_row = nullptr;
    if (p.by_last_name) {
        uint64_t c_id = FindCustomerByLastName(txn, t, p.c_w_id, p.c_d_id, p.c_last, rc);
        if (c_id == 0 || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
        c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                       PackCustKey(p.c_w_id, p.c_d_id, c_id), rc);
    } else {
        c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                       PackCustKey(p.c_w_id, p.c_d_id, p.c_id), rc);
    }
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

    double c_balance; c_row->GetValue(CUST::C_BALANCE, c_balance);
    c_row->SetValue<double>(CUST::C_BALANCE, c_balance - p.h_amount);
    double c_ytd_payment; c_row->GetValue(CUST::C_YTD_PAYMENT, c_ytd_payment);
    c_row->SetValue<double>(CUST::C_YTD_PAYMENT, c_ytd_payment + p.h_amount);
    uint64_t c_payment_cnt; c_row->GetValue(CUST::C_PAYMENT_CNT, c_payment_cnt);
    c_row->SetValue<uint64_t>(CUST::C_PAYMENT_CNT, c_payment_cnt + 1);

    // INSERT HISTORY
    {
        Row* hrow = t.history->CreateNewRow();
        if (!hrow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        hrow->SetValue<uint64_t>(HIST::H_C_D_ID, p.c_d_id);
        hrow->SetValue<uint64_t>(HIST::H_C_W_ID, p.c_w_id);
        hrow->SetValue<uint64_t>(HIST::H_D_ID, p.d_id);
        hrow->SetValue<uint64_t>(HIST::H_W_ID, p.w_id);
        hrow->SetValue<int64_t>(HIST::H_DATE, (int64_t)time(nullptr));
        hrow->SetValue<double>(HIST::H_AMOUNT, p.h_amount);
        char h_data[25] = {0};
        char w_name[11] = {0}; memcpy(w_name, w_row->GetValue(WH::W_NAME), 10);
        char d_name[11] = {0}; memcpy(d_name, d_row->GetValue(DIST::D_NAME), 10);
        snprintf(h_data, sizeof(h_data), "%.10s    %.10s", w_name, d_name);
        hrow->SetValueVariable(HIST::H_DATA, h_data, strlen(h_data) + 1);
        rc = t.history->InsertRow(hrow, txn);
        if (rc != RC_OK) { t.history->DestroyRow(hrow); txn->Rollback(); return rc; }
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 3. OrderStatus — Clause 2.6 (read-only)
// ======================================================================
RC RunOrderStatus(TxnManager* txn, const TpccTables& t, const OrderStatusParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    // Customer lookup
    uint64_t cust_id = p.c_id;
    if (p.by_last_name) {
        cust_id = FindCustomerByLastName(txn, t, p.w_id, p.d_id, p.c_last, rc);
        if (cust_id == 0 || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    }
    Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD,
                        PackCustKey(p.w_id, p.d_id, cust_id), rc);
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

    // Find latest order for this customer: scan backwards from D_NEXT_O_ID
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD,
                        PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    uint64_t d_next_o_id; d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);

    uint64_t found_o_id = 0;
    for (uint64_t oid = d_next_o_id - 1; oid >= 1; oid--) {
        Row* orow = Lookup(txn, t.order_tbl, t.ix_order, AccessType::RD,
                           PackOrderKey(p.w_id, p.d_id, oid), rc);
        if (!orow) break;
        uint64_t o_c_id; orow->GetValue(ORD::O_C_ID, o_c_id);
        if (o_c_id == cust_id) { found_o_id = oid; break; }
    }

    if (found_o_id > 0) {
        for (uint64_t ol = 1; ol <= MAX_OL_CNT; ol++) {
            Row* olrow = Lookup(txn, t.order_line, t.ix_order_line, AccessType::RD,
                                PackOlKey(p.w_id, p.d_id, found_o_id, ol), rc);
            if (!olrow) break;
        }
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 4. Delivery — Clause 2.7
// ======================================================================
RC RunDelivery(TxnManager* txn, const TpccTables& t, const DeliveryParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
        Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD,
                            PackDistKey(p.w_id, d_id), rc);
        if (!d_row || rc != RC_OK) continue;
        uint64_t d_next_o_id; d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);

        // Find oldest new-order via point lookups (start from reasonable lower bound)
        uint64_t no_o_id = 0;
        uint64_t scan_lo = (d_next_o_id > ORD_PER_DIST) ? (d_next_o_id - ORD_PER_DIST) : 1;
        for (uint64_t oid = scan_lo; oid < d_next_o_id; oid++) {
            Row* norow = Lookup(txn, t.new_order, t.ix_new_order, AccessType::RD,
                                PackOrderKey(p.w_id, d_id, oid), rc);
            if (norow) { no_o_id = oid; break; }
        }
        if (no_o_id == 0) continue;

        // Delete the new-order row
        Row* no_del = Lookup(txn, t.new_order, t.ix_new_order, AccessType::RD_FOR_UPDATE,
                             PackOrderKey(p.w_id, d_id, no_o_id), rc);
        if (!no_del || rc != RC_OK) continue;
        rc = txn->DeleteLastRow();
        if (rc != RC_OK) continue;

        // Update order: set carrier_id
        Row* orow = Lookup(txn, t.order_tbl, t.ix_order, AccessType::RD_FOR_UPDATE,
                           PackOrderKey(p.w_id, d_id, no_o_id), rc);
        if (!orow || rc != RC_OK) continue;
        uint64_t o_c_id; orow->GetValue(ORD::O_C_ID, o_c_id);
        rc = txn->UpdateRow(orow, ORD::O_CARRIER_ID, p.o_carrier_id);
        if (rc != RC_OK) continue;

        // Sum order-line amounts, set delivery date
        double ol_total = 0.0;
        uint64_t now = (uint64_t)time(nullptr);
        uint64_t o_ol_cnt; orow->GetValue(ORD::O_OL_CNT, o_ol_cnt);
        for (uint64_t ol = 1; ol <= o_ol_cnt; ol++) {
            Row* olrow = Lookup(txn, t.order_line, t.ix_order_line, AccessType::RD_FOR_UPDATE,
                                PackOlKey(p.w_id, d_id, no_o_id, ol), rc);
            if (!olrow || rc != RC_OK) break;
            double ol_amount; olrow->GetValue(ORDL::OL_AMOUNT, ol_amount);
            ol_total += ol_amount;
            rc = txn->UpdateRow(olrow, ORDL::OL_DELIVERY_D, now);
            if (rc != RC_OK) break;
        }

        // Update customer balance and delivery count
        Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                            PackCustKey(p.w_id, d_id, o_c_id), rc);
        if (!c_row || rc != RC_OK) continue;
        double c_balance; c_row->GetValue(CUST::C_BALANCE, c_balance);
        rc = txn->UpdateRow(c_row, CUST::C_BALANCE, c_balance + ol_total);
        if (rc != RC_OK) continue;
        Row* c_draft = txn->GetLastAccessedDraft();
        uint64_t c_delivery_cnt; c_row->GetValue(CUST::C_DELIVERY_CNT, c_delivery_cnt);
        c_draft->SetValue<uint64_t>(CUST::C_DELIVERY_CNT, c_delivery_cnt + 1);
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 5. StockLevel — Clause 2.8 (read-only)
// ======================================================================
RC RunStockLevel(TxnManager* txn, const TpccTables& t, const StockLevelParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD, PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    uint64_t d_next_o_id; d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);

    uint64_t o_id_low = (d_next_o_id > 20) ? (d_next_o_id - 20) : 1;
    uint64_t item_ids[20 * MAX_OL_CNT];
    uint32_t item_count = 0;
    for (uint64_t oid = o_id_low; oid < d_next_o_id; oid++) {
        for (uint64_t ol = 1; ol <= MAX_OL_CNT; ol++) {
            Row* olrow = Lookup(txn, t.order_line, t.ix_order_line, AccessType::RD,
                                PackOlKey(p.w_id, p.d_id, oid, ol), rc);
            if (!olrow) break;
            uint64_t ol_i_id; olrow->GetValue(ORDL::OL_I_ID, ol_i_id);
            item_ids[item_count++] = ol_i_id;
        }
    }
    std::sort(item_ids, item_ids + item_count);
    uint32_t unique_count = (uint32_t)(std::unique(item_ids, item_ids + item_count) - item_ids);

    uint64_t low_stock = 0;
    for (uint32_t i = 0; i < unique_count; i++) {
        Row* s_row = Lookup(txn, t.stock, t.ix_stock, AccessType::RD, PackStockKey(p.w_id, item_ids[i]), rc);
        if (!s_row) continue;
        uint64_t sq; s_row->GetValue(STK::S_QUANTITY, sq);
        if (sq < p.threshold) low_stock++;
    }
    (void)low_stock;

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

}  // namespace oro::tpcc
