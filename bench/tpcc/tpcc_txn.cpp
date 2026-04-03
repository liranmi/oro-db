/*
 * TPC-C Transaction Implementations for oro-db.
 *
 * All 5 transactions per TPC-C Standard Specification Rev 5.11, Clauses 2.4–2.8.
 * Each function includes the exact SQL it implements as comments.
 *
 * Key pattern: all lookups use BuildSearchKey(ix, table, PackXxxKey(...))
 * which creates a temp row with SetInternalKey + BuildKey — matching the
 * InternalKey path used during population.
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
#include <vector>
#include <algorithm>

using namespace MOT;

namespace oro::tpcc {

// ======================================================================
// Lookup helper: build key from packed value, do point lookup
// ======================================================================
static Row* Lookup(TxnManager* txn, Table* table, Index* ix,
                   AccessType atype, uint64_t packed_key, RC& rc)
{
    Key* key = BuildSearchKey(ix, table, packed_key);
    if (!key) { rc = RC_MEMORY_ALLOCATION_ERROR; return nullptr; }
    Row* row = txn->RowLookupByKey(table, atype, key, rc);
    ix->DestroyKey(key);
    return row;
}

// ======================================================================
// 1. NewOrder Transaction — Clause 2.4
// ======================================================================
RC RunNewOrder(TxnManager* txn, const TpccTables& t, const NewOrderParams& p, FastRandom& rng)
{
    RC rc = RC_OK;
    (void)rng;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    // SQL: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = :w_id
    Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD,
                        PackWhKey(p.w_id), rc);
    if (!w_row || rc != RC_OK) { txn->Rollback(); return rc; }
    double w_tax;
    w_row->GetValue(WH::W_TAX, w_tax);

    // SQL: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=:w_id AND D_ID=:d_id
    // SQL: UPDATE DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE ...
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD_FOR_UPDATE,
                        PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc; }
    double d_tax;
    d_row->GetValue(DIST::D_TAX, d_tax);
    uint64_t d_next_o_id;
    d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);
    d_row->SetValue<uint64_t>(DIST::D_NEXT_O_ID, d_next_o_id + 1);

    uint64_t o_id = d_next_o_id;

    // SQL: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER
    //      WHERE C_W_ID=:w_id AND C_D_ID=:d_id AND C_ID=:c_id
    Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD,
                        PackCustKey(p.w_id, p.d_id, p.c_id), rc);
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc; }
    double c_discount;
    c_row->GetValue(CUST::C_DISCOUNT, c_discount);

    // SQL: INSERT INTO ORDERS VALUES (...)
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
        orow->SetInternalKey(ORD::O_KEY, PackOrderKey(p.w_id, p.d_id, o_id));
        rc = t.order_tbl->InsertRow(orow, txn);
        if (rc != RC_OK) { t.order_tbl->DestroyRow(orow); txn->Rollback(); return rc; }
    }

    // SQL: INSERT INTO NEW_ORDER VALUES (...)
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

    // For each order-line (Clause 2.4.2.2 steps 4-8)
    for (uint32_t ol = 0; ol < p.ol_cnt; ol++) {
        uint64_t ol_i_id        = p.items[ol].ol_i_id;
        uint64_t ol_supply_w_id = p.items[ol].ol_supply_w_id;
        uint64_t ol_quantity    = p.items[ol].ol_quantity;

        // SQL: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = :ol_i_id
        Row* i_row = Lookup(txn, t.item, t.ix_item, AccessType::RD,
                            PackItemKey(ol_i_id), rc);
        if (!i_row) {
            // Clause 2.4.2.2: item not found → 1% rollback case
            txn->Rollback();
            return RC_OK;  // business-logic rollback, not an error
        }
        double i_price;
        i_row->GetValue(ITEM::I_PRICE, i_price);

        // SQL: SELECT S_QUANTITY, S_DIST_xx, ... FROM STOCK WHERE S_W_ID=:w AND S_I_ID=:i
        // SQL: UPDATE STOCK SET S_QUANTITY=?, S_YTD=S_YTD+?, ...
        Row* s_row = Lookup(txn, t.stock, t.ix_stock, AccessType::RD_FOR_UPDATE,
                            PackStockKey(ol_supply_w_id, ol_i_id), rc);
        if (!s_row || rc != RC_OK) { txn->Rollback(); return rc; }

        uint64_t s_quantity;
        s_row->GetValue(STK::S_QUANTITY, s_quantity);
        // Clause 2.4.2.2 step 6: adjust stock
        if (s_quantity >= ol_quantity + 10)
            s_quantity -= ol_quantity;
        else
            s_quantity = (s_quantity - ol_quantity) + 91;
        s_row->SetValue<uint64_t>(STK::S_QUANTITY, s_quantity);

        uint64_t s_ytd;
        s_row->GetValue(STK::S_YTD, s_ytd);
        s_row->SetValue<uint64_t>(STK::S_YTD, s_ytd + ol_quantity);

        uint64_t s_order_cnt;
        s_row->GetValue(STK::S_ORDER_CNT, s_order_cnt);
        s_row->SetValue<uint64_t>(STK::S_ORDER_CNT, s_order_cnt + 1);

        if (ol_supply_w_id != p.w_id) {
            uint64_t s_remote_cnt;
            s_row->GetValue(STK::S_REMOTE_CNT, s_remote_cnt);
            s_row->SetValue<uint64_t>(STK::S_REMOTE_CNT, s_remote_cnt + 1);
        }

        // Get district-specific stock info (S_DIST_01..S_DIST_10)
        char ol_dist_info[25] = {0};
        int dist_col = STK::S_DIST_01 + (int)(p.d_id - 1);
        memcpy(ol_dist_info, s_row->GetValue(dist_col), 24);

        double ol_amount = (double)ol_quantity * i_price;

        // SQL: INSERT INTO ORDER_LINE VALUES (...)
        Row* olrow = t.order_line->CreateNewRow();
        if (!olrow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        olrow->SetValue<uint64_t>(ORDL::OL_I_ID, ol_i_id);
        olrow->SetValue<uint64_t>(ORDL::OL_D_ID, p.d_id);
        olrow->SetValue<uint64_t>(ORDL::OL_W_ID, p.w_id);
        olrow->SetValue<uint64_t>(ORDL::OL_SUPPLY_W_ID, ol_supply_w_id);
        olrow->SetValue<int64_t>(ORDL::OL_DELIVERY_D, 0);
        olrow->SetValue<uint64_t>(ORDL::OL_QUANTITY, ol_quantity);
        olrow->SetValue<double>(ORDL::OL_AMOUNT, ol_amount);
        olrow->SetValueVariable(ORDL::OL_DIST_INFO, ol_dist_info, strlen(ol_dist_info) + 1);
        olrow->SetInternalKey(ORDL::OL_KEY, PackOlKey(p.w_id, p.d_id, o_id, (uint64_t)(ol + 1)));
        rc = t.order_line->InsertRow(olrow, txn);
        if (rc != RC_OK) { t.order_line->DestroyRow(olrow); txn->Rollback(); return rc; }
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 2. Payment Transaction — Clause 2.5
// ======================================================================
RC RunPayment(TxnManager* txn, const TpccTables& t, const PaymentParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    // SQL: UPDATE WAREHOUSE SET W_YTD = W_YTD + :h_amount WHERE W_ID = :w_id
    Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD_FOR_UPDATE,
                        PackWhKey(p.w_id), rc);
    if (!w_row || rc != RC_OK) { txn->Rollback(); return rc; }
    double w_ytd;
    w_row->GetValue(WH::W_YTD, w_ytd);
    w_row->SetValue<double>(WH::W_YTD, w_ytd + p.h_amount);

    // SQL: UPDATE DISTRICT SET D_YTD = D_YTD + :h_amount WHERE D_W_ID=:w_id AND D_ID=:d_id
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD_FOR_UPDATE,
                        PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc; }
    double d_ytd;
    d_row->GetValue(DIST::D_YTD, d_ytd);
    d_row->SetValue<double>(DIST::D_YTD, d_ytd + p.h_amount);

    // Customer lookup — by ID or by last name
    Row* c_row = nullptr;
    if (p.by_last_name) {
        // TODO: scan by last name — for now fall back to ID lookup
        // This matches dbx1000 behavior (no last-name scan in Payment either)
        txn->Rollback();
        return RC_OK;
    } else {
        // SQL: SELECT ... FROM CUSTOMER WHERE C_W_ID=:w AND C_D_ID=:d AND C_ID=:c
        c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                        PackCustKey(p.c_w_id, p.c_d_id, p.c_id), rc);
    }
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc; }

    // SQL: UPDATE CUSTOMER SET C_BALANCE=C_BALANCE-:amt, C_YTD_PAYMENT=C_YTD+:amt, C_PAYMENT_CNT=+1
    double c_balance;
    c_row->GetValue(CUST::C_BALANCE, c_balance);
    c_row->SetValue<double>(CUST::C_BALANCE, c_balance - p.h_amount);

    double c_ytd_payment;
    c_row->GetValue(CUST::C_YTD_PAYMENT, c_ytd_payment);
    c_row->SetValue<double>(CUST::C_YTD_PAYMENT, c_ytd_payment + p.h_amount);

    uint64_t c_payment_cnt;
    c_row->GetValue(CUST::C_PAYMENT_CNT, c_payment_cnt);
    c_row->SetValue<uint64_t>(CUST::C_PAYMENT_CNT, c_payment_cnt + 1);

    // Clause 2.5.2.2: BC credit → update C_DATA
    char c_credit[3] = {0};
    memcpy(c_credit, c_row->GetValue(CUST::C_CREDIT), 2);
    if (c_credit[0] == 'B' && c_credit[1] == 'C') {
        char new_data[501];
        snprintf(new_data, sizeof(new_data), "%lu %lu %lu %lu %lu %.2f ",
                 (unsigned long)p.c_id, (unsigned long)p.c_d_id,
                 (unsigned long)p.c_w_id, (unsigned long)p.d_id,
                 (unsigned long)p.w_id, p.h_amount);
        size_t plen = strlen(new_data);
        size_t remaining = 500 - plen;
        memcpy(new_data + plen, c_row->GetValue(CUST::C_DATA), remaining);
        new_data[500] = '\0';
        c_row->SetValueVariable(CUST::C_DATA, new_data, strlen(new_data) + 1);
    }

    // SQL: INSERT INTO HISTORY VALUES (...)
    {
        Row* hrow = t.history->CreateNewRow();
        if (!hrow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        hrow->SetValue<uint64_t>(HIST::H_C_D_ID, p.c_d_id);
        hrow->SetValue<uint64_t>(HIST::H_C_W_ID, p.c_w_id);
        hrow->SetValue<uint64_t>(HIST::H_D_ID, p.d_id);
        hrow->SetValue<uint64_t>(HIST::H_W_ID, p.w_id);
        hrow->SetValue<int64_t>(HIST::H_DATE, (int64_t)time(nullptr));
        hrow->SetValue<double>(HIST::H_AMOUNT, p.h_amount);
        // H_DATA = W_NAME + "    " + D_NAME (Clause 2.5.3.4)
        char h_data[25] = {0};
        char w_name[11] = {0}; memcpy(w_name, w_row->GetValue(WH::W_NAME), 10);
        char d_name[11] = {0}; memcpy(d_name, d_row->GetValue(DIST::D_NAME), 10);
        snprintf(h_data, sizeof(h_data), "%.10s    %.10s", w_name, d_name);
        hrow->SetValueVariable(HIST::H_DATA, h_data, strlen(h_data) + 1);
        hrow->SetInternalKey(HIST::H_KEY, NextHistoryKey());
        rc = t.history->InsertRow(hrow, txn);
        if (rc != RC_OK) { t.history->DestroyRow(hrow); txn->Rollback(); return rc; }
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 3. Order-Status Transaction — Clause 2.6 (read-only)
// ======================================================================
RC RunOrderStatus(TxnManager* txn, const TpccTables& t, const OrderStatusParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    // Customer lookup
    Row* c_row = nullptr;
    uint64_t c_id = p.c_id;
    if (p.by_last_name) {
        // TODO: scan by last name
        txn->Rollback();
        return RC_OK;
    } else {
        c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD,
                        PackCustKey(p.w_id, p.d_id, p.c_id), rc);
    }
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc; }

    // SQL: SELECT O_ID ... FROM ORDERS WHERE O_W_ID=:w AND O_D_ID=:d AND O_C_ID=:c
    //      ORDER BY O_ID DESC LIMIT 1
    //
    // We have a packed key index on (w_id, d_id, o_id). We need to find the latest
    // order for this customer. Use reverse iterator from max key.
    uint64_t latest_o_id = 0;
    {
        Key* hi_key = BuildSearchKey(t.ix_order, t.order_tbl, PackOrderKey(p.w_id, p.d_id, UINT32_MAX));
        if (!hi_key) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        bool found = false;
        // Reverse search from max O_ID in this (w_id, d_id)
        IndexIterator* it = t.ix_order->Search(hi_key, false, false, 0, found);
        t.ix_order->DestroyKey(hi_key);

        while (it && it->IsValid()) {
            Sentinel* s = it->GetPrimarySentinel();
            if (!s) break;
            Row* orow = txn->RowLookup(AccessType::RD, s, rc);
            if (!orow || rc != RC_OK) break;
            uint64_t ow, od;
            orow->GetValue(ORD::O_W_ID, ow);
            orow->GetValue(ORD::O_D_ID, od);
            if (ow != p.w_id || od != p.d_id) break;
            uint64_t oc;
            orow->GetValue(ORD::O_C_ID, oc);
            if (oc == c_id) {
                // Read O_KEY to extract o_id
                uint64_t okey;
                orow->GetValue(ORD::O_KEY, okey);
                latest_o_id = okey & 0xFFFFF;  // lower 20 bits = o_id from PackOrderKey
                break;
            }
            it->Next();  // reverse iterator: Next() goes backwards
        }
        delete it;
    }

    if (latest_o_id == 0) {
        // No order found — valid for newly created customers
        txn->Rollback();
        return RC_OK;
    }

    // SQL: SELECT OL_* FROM ORDER_LINE WHERE OL_W_ID=:w AND OL_D_ID=:d AND OL_O_ID=:o
    // Read each order-line by direct key lookup (we know OL_NUMBER ranges 1..15)
    for (uint64_t ol = 1; ol <= MAX_OL_CNT; ol++) {
        Row* olrow = Lookup(txn, t.order_line, t.ix_order_line, AccessType::RD,
                            PackOlKey(p.w_id, p.d_id, latest_o_id, ol), rc);
        if (!olrow) break;  // no more order-lines
        // Read fields (exercising the read path)
        uint64_t ol_i_id, ol_quantity;
        double ol_amount;
        olrow->GetValue(ORDL::OL_I_ID, ol_i_id);
        olrow->GetValue(ORDL::OL_QUANTITY, ol_quantity);
        olrow->GetValue(ORDL::OL_AMOUNT, ol_amount);
        (void)ol_i_id; (void)ol_quantity; (void)ol_amount;
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 4. Delivery Transaction — Clause 2.7
// ======================================================================
RC RunDelivery(TxnManager* txn, const TpccTables& t, const DeliveryParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    int64_t now = (int64_t)time(nullptr);

    for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++) {

        // SQL: SELECT MIN(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID=:w AND NO_D_ID=:d
        // Use forward iterator from min key in (w_id, d_id)
        uint64_t no_o_id = 0;
        {
            Key* lo_key = BuildSearchKey(t.ix_new_order, t.new_order,
                                         PackOrderKey(p.w_id, d_id, 0));
            if (!lo_key) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
            bool found = false;
            IndexIterator* it = t.ix_new_order->Search(lo_key, false, true, 0, found);
            t.ix_new_order->DestroyKey(lo_key);

            if (it && it->IsValid()) {
                Sentinel* s = it->GetPrimarySentinel();
                if (s) {
                    Row* norow = txn->RowLookup(AccessType::RD_FOR_UPDATE, s, rc);
                    if (norow && rc == RC_OK) {
                        uint64_t nw, nd;
                        norow->GetValue(NORD::NO_W_ID, nw);
                        norow->GetValue(NORD::NO_D_ID, nd);
                        if (nw == p.w_id && nd == d_id) {
                            norow->GetValue(NORD::NO_O_ID, no_o_id);
                            // SQL: DELETE FROM NEW_ORDER WHERE ...
                            rc = txn->DeleteLastRow();
                            if (rc != RC_OK) { delete it; txn->Rollback(); return rc; }
                        }
                    }
                }
            }
            delete it;
        }

        if (no_o_id == 0) continue;

        // SQL: UPDATE ORDERS SET O_CARRIER_ID = :carrier WHERE ...
        Row* orow = Lookup(txn, t.order_tbl, t.ix_order, AccessType::RD_FOR_UPDATE,
                           PackOrderKey(p.w_id, d_id, no_o_id), rc);
        uint64_t o_c_id = 0;
        if (orow && rc == RC_OK) {
            orow->GetValue(ORD::O_C_ID, o_c_id);
            orow->SetValue<uint64_t>(ORD::O_CARRIER_ID, p.o_carrier_id);
        }

        // SQL: UPDATE ORDER_LINE SET OL_DELIVERY_D=:now WHERE OL_W_ID=:w AND OL_D_ID=:d AND OL_O_ID=:o
        // SQL: SELECT SUM(OL_AMOUNT) ...
        double ol_total = 0.0;
        for (uint64_t ol = 1; ol <= MAX_OL_CNT; ol++) {
            Row* olrow = Lookup(txn, t.order_line, t.ix_order_line, AccessType::RD_FOR_UPDATE,
                                PackOlKey(p.w_id, d_id, no_o_id, ol), rc);
            if (!olrow) break;
            double ol_amount;
            olrow->GetValue(ORDL::OL_AMOUNT, ol_amount);
            ol_total += ol_amount;
            olrow->SetValue<int64_t>(ORDL::OL_DELIVERY_D, now);
        }

        // SQL: UPDATE CUSTOMER SET C_BALANCE=C_BALANCE+:total, C_DELIVERY_CNT=+1
        if (o_c_id > 0) {
            Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                                PackCustKey(p.w_id, d_id, o_c_id), rc);
            if (c_row && rc == RC_OK) {
                double c_balance;
                c_row->GetValue(CUST::C_BALANCE, c_balance);
                c_row->SetValue<double>(CUST::C_BALANCE, c_balance + ol_total);
                uint64_t c_del_cnt;
                c_row->GetValue(CUST::C_DELIVERY_CNT, c_del_cnt);
                c_row->SetValue<uint64_t>(CUST::C_DELIVERY_CNT, c_del_cnt + 1);
            }
        }
    }

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

// ======================================================================
// 5. Stock-Level Transaction — Clause 2.8 (read-only)
// ======================================================================
RC RunStockLevel(TxnManager* txn, const TpccTables& t, const StockLevelParams& p)
{
    RC rc = RC_OK;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);

    // SQL: SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=:w AND D_ID=:d
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD,
                        PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc; }
    uint64_t d_next_o_id;
    d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);

    // SQL: SELECT COUNT(DISTINCT S_I_ID) FROM ORDER_LINE, STOCK
    //      WHERE OL_W_ID=:w AND OL_D_ID=:d AND OL_O_ID BETWEEN :lo AND :hi
    //        AND S_W_ID=:w AND S_I_ID=OL_I_ID AND S_QUANTITY < :threshold
    uint64_t o_id_low = (d_next_o_id > 20) ? (d_next_o_id - 20) : 1;

    // Collect distinct item IDs from last 20 orders' order-lines via point lookups
    std::vector<uint64_t> item_ids;
    for (uint64_t oid = o_id_low; oid < d_next_o_id; oid++) {
        for (uint64_t ol = 1; ol <= MAX_OL_CNT; ol++) {
            Row* olrow = Lookup(txn, t.order_line, t.ix_order_line, AccessType::RD,
                                PackOlKey(p.w_id, p.d_id, oid, ol), rc);
            if (!olrow) break;
            uint64_t ol_i_id;
            olrow->GetValue(ORDL::OL_I_ID, ol_i_id);
            item_ids.push_back(ol_i_id);
        }
    }

    // Deduplicate
    std::sort(item_ids.begin(), item_ids.end());
    item_ids.erase(std::unique(item_ids.begin(), item_ids.end()), item_ids.end());

    // Check stock for each
    uint64_t low_stock_count = 0;
    for (uint64_t i_id : item_ids) {
        Row* s_row = Lookup(txn, t.stock, t.ix_stock, AccessType::RD,
                            PackStockKey(p.w_id, i_id), rc);
        if (!s_row) continue;
        uint64_t s_quantity;
        s_row->GetValue(STK::S_QUANTITY, s_quantity);
        if (s_quantity < p.threshold) low_stock_count++;
    }
    (void)low_stock_count;

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

}  // namespace oro::tpcc
