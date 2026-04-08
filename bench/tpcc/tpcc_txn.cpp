/*
 * TPC-C Transaction Implementations for oro-db.
 *
 * All 5 transactions per TPC-C Standard Specification Rev 5.11, Clauses 2.4–2.8.
 * Each function includes the exact SQL it implements as comments.
 *
 * Key pattern: all lookups use BuildSearchKey(txn, ix, PackXxxKey(...))
 * which allocates from the transaction's key pool (GetTxnKey/DestroyTxnKey).
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
#include <set>

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

// ======================================================================
// 1. NewOrder — Clause 2.4
// ======================================================================
RC RunNewOrder(TxnManager* txn, const TpccTables& t, const NewOrderParams& p, FastRandom& rng)
{
    RC rc = RC_OK;
    (void)rng;
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);

    // SQL: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = :w_id
    Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD, PackWhKey(p.w_id), rc);
    if (!w_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double w_tax; w_row->GetValue(WH::W_TAX, w_tax);

    // SQL: SELECT/UPDATE D_NEXT_O_ID FROM DISTRICT
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD_FOR_UPDATE,
                        PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double d_tax; d_row->GetValue(DIST::D_TAX, d_tax);
    uint64_t d_next_o_id; d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);
    rc = txn->UpdateRow(d_row, DIST::D_NEXT_O_ID, d_next_o_id + 1);
    if (rc != RC_OK) { txn->Rollback(); return rc; }
    uint64_t o_id = d_next_o_id;

    // SQL: SELECT C_DISCOUNT FROM CUSTOMER
    Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD,
                        PackCustKey(p.w_id, p.d_id, p.c_id), rc);
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

    // SQL: INSERT INTO ORDERS
    {
        Row* orow = t.order_tbl->CreateNewRow();
        if (!orow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        orow->SetValue<uint64_t>(ORD::O_ID, o_id);
        orow->SetValue<uint64_t>(ORD::O_C_ID, p.c_id);
        orow->SetValue<uint64_t>(ORD::O_D_ID, p.d_id);
        orow->SetValue<uint64_t>(ORD::O_W_ID, p.w_id);
        orow->SetValue<int64_t>(ORD::O_ENTRY_D, (int64_t)time(nullptr));
        orow->SetValue<uint64_t>(ORD::O_CARRIER_ID, 0);
        orow->SetValue<uint64_t>(ORD::O_OL_CNT, p.ol_cnt);
        orow->SetValue<uint64_t>(ORD::O_ALL_LOCAL, p.all_local ? 1 : 0);
        orow->SetInternalKey(ORD::O_KEY, PackOrderKey(p.w_id, p.d_id, o_id));
        rc = t.order_tbl->InsertRow(orow, txn);
        if (rc != RC_OK) { txn->Rollback(); return rc; }
    }

    // SQL: INSERT INTO NEW_ORDER
    {
        Row* norow = t.new_order->CreateNewRow();
        if (!norow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        norow->SetValue<uint64_t>(NORD::NO_O_ID, o_id);
        norow->SetValue<uint64_t>(NORD::NO_D_ID, p.d_id);
        norow->SetValue<uint64_t>(NORD::NO_W_ID, p.w_id);
        norow->SetInternalKey(NORD::NO_KEY, PackOrderKey(p.w_id, p.d_id, o_id));
        rc = t.new_order->InsertRow(norow, txn);
        if (rc != RC_OK) { txn->Rollback(); return rc; }
    }

    // For each order-line
    for (uint32_t ol = 0; ol < p.ol_cnt; ol++) {
        uint64_t ol_i_id = p.items[ol].ol_i_id;
        uint64_t ol_supply_w_id = p.items[ol].ol_supply_w_id;
        uint64_t ol_quantity = p.items[ol].ol_quantity;

        // SQL: SELECT FROM ITEM
        Row* i_row = Lookup(txn, t.item, t.ix_item, AccessType::RD, PackItemKey(ol_i_id), rc);
        if (!i_row) { txn->Rollback(); return RC_OK; }  // 1% invalid item rollback
        double i_price; i_row->GetValue(ITEM::I_PRICE, i_price);

        // SQL: SELECT/UPDATE STOCK
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

        // SQL: INSERT INTO ORDER_LINE
        Row* olrow = t.order_line->CreateNewRow();
        if (!olrow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        olrow->SetValue<uint64_t>(ORDL::OL_O_ID, o_id);
        olrow->SetValue<uint64_t>(ORDL::OL_NUMBER, (uint64_t)(ol + 1));
        olrow->SetValue<uint64_t>(ORDL::OL_I_ID, ol_i_id);
        olrow->SetValue<uint64_t>(ORDL::OL_D_ID, p.d_id);
        olrow->SetValue<uint64_t>(ORDL::OL_W_ID, p.w_id);
        olrow->SetValue<uint64_t>(ORDL::OL_SUPPLY_W_ID, ol_supply_w_id);
        olrow->SetValue<int64_t>(ORDL::OL_DELIVERY_D, 0);
        olrow->SetValue<uint64_t>(ORDL::OL_QUANTITY, ol_quantity);
        olrow->SetValue<double>(ORDL::OL_AMOUNT, (double)ol_quantity * i_price);
        SetStringValue(olrow, ORDL::OL_DIST_INFO, ol_dist_info);
        olrow->SetInternalKey(ORDL::OL_KEY, PackOlKey(p.w_id, p.d_id, o_id, (uint64_t)(ol + 1)));
        rc = t.order_line->InsertRow(olrow, txn);
        if (rc != RC_OK) { txn->Rollback(); return rc; }
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
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);

    // SQL: UPDATE warehouse SET w_ytd = w_ytd + :h_amount
    //      WHERE w_id = :w_id
    Row* w_row = Lookup(txn, t.warehouse, t.ix_warehouse, AccessType::RD_FOR_UPDATE, PackWhKey(p.w_id), rc);
    if (!w_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double w_ytd; w_row->GetValue(WH::W_YTD, w_ytd);
    rc = txn->UpdateRow(w_row, WH::W_YTD, w_ytd + p.h_amount);
    if (rc != RC_OK) { txn->Rollback(); return rc; }

    // SQL: UPDATE district SET d_ytd = d_ytd + :h_amount
    //      WHERE d_w_id = :w_id AND d_id = :d_id
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD_FOR_UPDATE, PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    double d_ytd; d_row->GetValue(DIST::D_YTD, d_ytd);
    rc = txn->UpdateRow(d_row, DIST::D_YTD, d_ytd + p.h_amount);
    if (rc != RC_OK) { txn->Rollback(); return rc; }

    // Customer lookup — by-last-name (60%) or by-ID (40%)
    Row* c_row = nullptr;
    uint64_t c_id = p.c_id;

    if (p.by_last_name) {
        // SQL: SELECT count(c_id) INTO :namecnt
        //        FROM customer
        //       WHERE c_last = :c_last AND c_d_id = :c_d_id AND c_w_id = :c_w_id;
        //
        // SQL: DECLARE c_byname CURSOR FOR
        //      SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city,
        //             c_state, c_zip, c_phone, c_credit, c_credit_lim,
        //             c_discount, c_balance, c_since
        //        FROM customer
        //       WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last
        //       ORDER BY c_first;
        //      OPEN c_byname;
        //      FETCH c_byname to midpoint (namecnt/2 rounded up);
        //      CLOSE c_byname;
        //
        Index* ix = t.ix_customer_last;
        if (!ix) { txn->Rollback(); return RC_ABORT; }

        Key* prefix = BuildCustLastSearchKey(ix, p.c_w_id, p.c_d_id, p.c_last);
        if (!prefix) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }

        // Count visible rows matching (c_w_id, c_d_id, c_last), collect their c_ids
        uint32_t namecnt = 0;
        bool found = false;
        IndexIterator* it = ix->Search(prefix, true, true, 0, found);
        std::vector<uint64_t> c_id_vector;
        while (it != nullptr && it->IsValid()) {
            const Key* cur = reinterpret_cast<const Key*>(it->GetKey());
            if (!cur || memcmp(cur->GetKeyBuf(), prefix->GetKeyBuf(), CUST_LAST_PREFIX_LEN) != 0)
                break;
            Sentinel* s = it->GetPrimarySentinel();
            Row* r = txn->RowLookup(AccessType::RD, s, rc);
            if (rc != RC_OK) break;
            if (r) {
                uint64_t it_c_id;
                r->GetValue(CUST::C_ID, it_c_id);
                c_id_vector.push_back(it_c_id);
                namecnt++;
            }
            it->Next();
        }
        if (it) it->Destroy();
        ix->DestroyKey(prefix);

        if (rc != RC_OK || namecnt == 0) {
            txn->Rollback();
            return rc != RC_OK ? rc : RC_ABORT;
        }

        // Midpoint: spec says "if namecnt is odd, namecnt++; fetch namecnt/2 rows"
        // i.e. position = ceil(namecnt/2), 1-indexed → 0-indexed = ceil(namecnt/2) - 1
        uint32_t mid = (namecnt + 1) / 2 - 1;
        c_id = c_id_vector[mid];

        // Re-lookup the midpoint customer by primary key with RD_FOR_UPDATE
        c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                       PackCustKey(p.c_w_id, p.c_d_id, c_id), rc);
        if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

    } else {
        // SQL: SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,
        //             c_state, c_zip, c_phone, c_credit, c_credit_lim,
        //             c_discount, c_balance, c_since
        //        FROM customer
        //       WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
        c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                       PackCustKey(p.c_w_id, p.c_d_id, p.c_id), rc);
        if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    }

    // SQL: UPDATE customer SET c_balance = :c_balance,
    //             c_ytd_payment = c_ytd_payment + :h_amount,
    //             c_payment_cnt = c_payment_cnt + 1
    //      WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
    double c_balance; c_row->GetValue(CUST::C_BALANCE, c_balance);
    rc = txn->UpdateRow(c_row, CUST::C_BALANCE, c_balance - p.h_amount);
    if (rc != RC_OK) { txn->Rollback(); return rc; }
    Row* c_draft = txn->GetLastAccessedDraft();
    double c_ytd_payment; c_row->GetValue(CUST::C_YTD_PAYMENT, c_ytd_payment);
    c_draft->SetValue<double>(CUST::C_YTD_PAYMENT, c_ytd_payment + p.h_amount);
    uint64_t c_payment_cnt; c_row->GetValue(CUST::C_PAYMENT_CNT, c_payment_cnt);
    c_draft->SetValue<uint64_t>(CUST::C_PAYMENT_CNT, c_payment_cnt + 1);

    // SQL (BC path):
    //   SELECT c_data INTO :c_data FROM customer
    //    WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
    //   UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
    //    WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
    // SQL (GC path):
    //   UPDATE customer SET c_balance = :c_balance
    //    WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
    const char* c_credit = reinterpret_cast<const char*>(c_row->GetValue(CUST::C_CREDIT));
    if (c_credit[0] == 'B' && c_credit[1] == 'C') {
        const char* old_data = reinterpret_cast<const char*>(c_row->GetValue(CUST::C_DATA));
        char new_data[501];
        memset(new_data, 0, sizeof(new_data));
        int prefix_len = snprintf(new_data, sizeof(new_data),
            "%lu %lu %lu %lu %lu %.2f ",
            (unsigned long)c_id,
            (unsigned long)p.c_d_id,
            (unsigned long)p.c_w_id,
            (unsigned long)p.d_id,
            (unsigned long)p.w_id,
            p.h_amount);
        if (prefix_len < 0) prefix_len = 0;
        if (prefix_len > 500) prefix_len = 500;
        uint32_t old_len = (uint32_t)strnlen(old_data, 500);
        uint32_t copy_len = ((uint32_t)(500 - prefix_len) < old_len)
                            ? (uint32_t)(500 - prefix_len) : old_len;
        if (copy_len > 0)
            memcpy(new_data + prefix_len, old_data, copy_len);
        new_data[prefix_len + copy_len] = '\0';
        SetStringValue(c_draft, CUST::C_DATA, new_data);
    }

    // SQL: INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,
    //                         h_w_id, h_date, h_amount, h_data)
    //      VALUES (:c_d_id, :c_w_id, :c_id, :d_id,
    //              :w_id, :datetime, :h_amount, :h_data)
    {
        Row* hrow = t.history->CreateNewRow();
        if (!hrow) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        hrow->SetValue<uint64_t>(HIST::H_C_ID, c_id);
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
        SetStringValue(hrow, HIST::H_DATA, h_data);
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
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);

    // Case 1 (by_last_name = true):
    // SQL: SELECT count(c_id) INTO :namecnt
    //        FROM customer
    //       WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_last = :c_last
    //
    // SQL: DECLARE c_byname CURSOR FOR
    //        SELECT c_balance, c_first, c_middle, c_last
    //          FROM customer
    //         WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_last = :c_last
    //         ORDER BY c_first
    //      OPEN c_byname
    //      -- position to middle row (namecnt/2 rounded up)
    //      FETCH c_byname INTO :c_balance, :c_first, :c_middle, :c_last
    //      CLOSE c_byname
    //
    // Case 2 (by_last_name = false):
    // SQL: SELECT c_balance, c_first, c_middle, c_last
    //        FROM customer
    //       WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id

    if (p.by_last_name) { txn->Rollback(); return RC_ABORT; }

    Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD,
                        PackCustKey(p.w_id, p.d_id, p.c_id), rc);
    if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

    // SQL: SELECT o_id, o_entry_d, o_carrier_id
    //        FROM orders
    //       WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :c_id
    //       ORDER BY o_id DESC
    //      -- fetch first (latest) row

    // SQL: SELECT ol_i_id, ol_supply_w_id, ol_quantity,
    //             ol_amount, ol_delivery_d
    //        FROM order_line
    //       WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id = :o_id

    // TODO: find latest order for this customer, then read its order-lines
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
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);

    int64_t delivery_d = (int64_t)time(nullptr);

    // For each district (1..10):
    for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
        // SQL: SELECT no_o_id FROM new_order
        //       WHERE no_d_id = :d_id AND no_w_id = :w_id
        //       ORDER BY no_o_id ASC  -- fetch first (oldest) row
        // Open cursor at start of (w_id, d_id) prefix — first row is oldest
        auto [no_lo, no_hi] = PrefixKeyRange(
            PackOrderKey(p.w_id, d_id, 0),
            PackOrderKey(p.w_id, d_id, 0),
            ORDER_SUFFIX_BITS);
        Key* search_key = BuildSearchKey(txn, t.ix_new_order, no_lo);
        if (!search_key) { txn->Rollback(); return RC_MEMORY_ALLOCATION_ERROR; }
        uint8_t hi_buf[8];
        memcpy(hi_buf, &no_hi, 8);

        bool found = false;
        IndexIterator* it = t.ix_new_order->Search(search_key, true, true, txn->GetThdId(), found);
        Row* no_row = nullptr;
        uint64_t no_o_id = 0;
        while (it != nullptr && it->IsValid()) {
            const Key* cur = reinterpret_cast<const Key*>(it->GetKey());
            if (!cur || memcmp(cur->GetKeyBuf(), hi_buf, 8) > 0) break;
            Sentinel* s = it->GetPrimarySentinel();
            no_row = txn->RowLookup(AccessType::RD_FOR_UPDATE, s, rc);
            if (rc != RC_OK) break;
            if (no_row) {
                no_row->GetValue(NORD::NO_O_ID, no_o_id);
                break;  // first visible = oldest
            }
            it->Next();
        }
        if (it) it->Destroy();

        if (!no_row) continue;  // no new-order for this district — skip
        if (rc != RC_OK) { txn->Rollback(); return rc; }

        // SQL: DELETE FROM new_order WHERE no_o_id = :no_o_id ...
        rc = txn->DeleteLastRow();
        if (rc != RC_OK) { txn->Rollback(); return rc; }

        // SQL: SELECT o_c_id FROM orders WHERE o_id = :no_o_id ...
        // SQL: UPDATE orders SET o_carrier_id = :o_carrier_id ...
        Row* o_row = Lookup(txn, t.order_tbl, t.ix_order, AccessType::RD_FOR_UPDATE,
                            PackOrderKey(p.w_id, d_id, no_o_id), rc);
        if (!o_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

        uint64_t o_c_id;
        o_row->GetValue(ORD::O_C_ID, o_c_id);
        rc = txn->UpdateRow(o_row, ORD::O_CARRIER_ID, (uint64_t)p.o_carrier_id);
        if (rc != RC_OK) { txn->Rollback(); return rc; }

        // SQL: UPDATE order_line SET ol_delivery_d = :datetime ...
        //      SUM(ol_amount) → :ol_total
        auto [ol_lo, ol_hi] = PrefixKeyRange(
            PackOlKey(p.w_id, d_id, no_o_id, 0),
            PackOlKey(p.w_id, d_id, no_o_id, 0),
            OL_SUFFIX_BITS);
        double ol_total = 0.0;
        RangeScan ol_scan(txn, t.ix_order_line, ol_lo, ol_hi, AccessType::RD_FOR_UPDATE);
        for (Row* ol_row : ol_scan) {
            double ol_amount;
            ol_row->GetValue(ORDL::OL_AMOUNT, ol_amount);
            ol_total += ol_amount;

            rc = txn->UpdateRow(ol_row, ORDL::OL_DELIVERY_D, (uint64_t)delivery_d);
            if (rc != RC_OK) break;
        }
        if (ol_scan.rc() != RC_OK) rc = ol_scan.rc();
        if (rc != RC_OK) { txn->Rollback(); return rc; }

        // SQL: UPDATE customer SET c_balance = c_balance + :ol_total,
        //                          c_delivery_cnt = c_delivery_cnt + 1 ...
        Row* c_row = Lookup(txn, t.customer, t.ix_customer, AccessType::RD_FOR_UPDATE,
                            PackCustKey(p.w_id, d_id, o_c_id), rc);
        if (!c_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }

        double c_balance;
        c_row->GetValue(CUST::C_BALANCE, c_balance);
        rc = txn->UpdateRow(c_row, CUST::C_BALANCE, c_balance + ol_total);
        if (rc != RC_OK) { txn->Rollback(); return rc; }

        Row* c_draft = txn->GetLastAccessedDraft();
        uint64_t c_delivery_cnt;
        c_row->GetValue(CUST::C_DELIVERY_CNT, c_delivery_cnt);
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
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);

    // SQL: SELECT d_next_o_id INTO :o_id
    //        FROM district
    //       WHERE d_w_id = :w_id AND d_id = :d_id
    Row* d_row = Lookup(txn, t.district, t.ix_district, AccessType::RD, PackDistKey(p.w_id, p.d_id), rc);
    if (!d_row || rc != RC_OK) { txn->Rollback(); return rc ? rc : RC_ABORT; }
    uint64_t d_next_o_id; d_row->GetValue(DIST::D_NEXT_O_ID, d_next_o_id);

    // SQL: SELECT COUNT(DISTINCT(s_i_id))
    //        FROM order_line, stock
    //       WHERE ol_w_id = :w_id AND ol_d_id = :d_id
    //         AND ol_o_id < :o_id AND ol_o_id >= :o_id - 20
    //         AND s_w_id = :w_id AND s_i_id = ol_i_id
    //         AND s_quantity < :threshold
    uint64_t o_id_low = (d_next_o_id > 20) ? (d_next_o_id - 20) : 1;
    auto [lo, hi] = PrefixKeyRange(
        PackOlKey(p.w_id, p.d_id, o_id_low, 0),
        PackOlKey(p.w_id, p.d_id, d_next_o_id - 1, 0),
    OL_SUFFIX_BITS);
    std::set<uint64_t> low_stock_items;
    RangeScan scan(txn, t.ix_order_line, lo, hi, AccessType::RD);
    for (Row* row : scan) {
        uint64_t ol_i_id;
        row->GetValue(OL_I_ID, ol_i_id);

        Row* s_row = Lookup(txn, t.stock, t.ix_stock, AccessType::RD, PackStockKey(p.w_id, ol_i_id), rc);
        if (rc != RC_OK) break;
        if (s_row) {
            uint64_t s_quantity;
            s_row->GetValue(STK::S_QUANTITY, s_quantity);
            if (s_quantity < p.threshold) {
                low_stock_items.insert(ol_i_id);
            }
        }
    }
    if (scan.rc() != RC_OK) rc = scan.rc();

    rc = txn->Commit();
    txn->EndTransaction();
    return rc;
}

}  // namespace oro::tpcc
