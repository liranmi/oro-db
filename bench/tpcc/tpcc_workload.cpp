/*
 * TPC-C Schema Creation and Data Population for oro-db.
 *
 * Schema follows TPC-C Standard Specification Rev 5.11, Clause 1.3.
 * Population follows Clause 4.3.3.
 *
 * Key convention: each table has a packed _KEY column as the LAST column.
 * This enables the InternalKey path in MOT's BuildKey (raw CpKey, no PackKey).
 * Composite keys are packed into a single uint64 via PackXxxKey() helpers.
 */

#include "tpcc_workload.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "bench_util.h"
#include "catalog_column_types.h"
#include "mot_configuration.h"
#include "row.h"
#include "session_manager.h"
#include "table_manager.h"

#include <cstdio>
#include <cstring>
#include <ctime>
#include <thread>
#include <vector>
#include <algorithm>
#include <numeric>

using namespace MOT;

// SetStringValue is defined in tpcc_helper.h (friend of Row)

namespace oro::tpcc {

// ======================================================================
// Helper: create a primary index on the LAST column of the table.
// Follows the test_index.cpp pattern: keyLen = raw column size (8 bytes).
// ======================================================================
static Index* MakePrimaryIndex(TxnManager* txn, Table* table, const char* name)
{
    int lastCol = table->GetFieldCount() - 1;
    uint32_t keyLen = table->GetFieldSize(lastCol);  // 8 = sizeof(uint64_t)

    RC rc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY, IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR, true, keyLen, name, rc, nullptr);
    if (!ix || rc != RC_OK) {
        fprintf(stderr, "ERROR: Failed to create index '%s': %s\n", name, RcToString(rc));
        return nullptr;
    }
    if (!ix->SetNumTableFields(table->GetFieldCount())) {
        fprintf(stderr, "ERROR: SetNumTableFields failed for '%s'\n", name);
        delete ix;
        return nullptr;
    }
    ix->SetNumIndexFields(1);
    ix->SetLenghtKeyFields(0, lastCol, keyLen);
    ix->SetTable(table);

    rc = txn->CreateIndex(table, ix, true);
    if (rc != RC_OK) {
        fprintf(stderr, "ERROR: CreateIndex '%s' failed: %s\n", name, RcToString(rc));
        delete ix;
        return nullptr;
    }
    return ix;
}

// ======================================================================
// Schema creation — Full schema (Clause 1.3)
// Column order: data columns first, packed _KEY column LAST.
// ======================================================================
bool CreateSchema(TxnManager* txn, TpccTables& t, bool small_schema)
{
    using F = MOT_CATALOG_FIELD_TYPES;
    RC rc;
    (void)small_schema;

    // ---- WAREHOUSE (Clause 1.3.1) ----
    t.warehouse = new Table();
    if (!t.warehouse->Init("warehouse", "public.warehouse", WH::W_NUM_COLS)) return false;
    t.warehouse->AddColumn("W_ID",       8,   F::MOT_TYPE_LONG,    false);
    t.warehouse->AddColumn("W_NAME",     11,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_STREET_1", 21,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_STREET_2", 21,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_CITY",     21,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_STATE",    3,   F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_ZIP",      10,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_TAX",      8,   F::MOT_TYPE_DOUBLE,  false);
    t.warehouse->AddColumn("W_YTD",      8,   F::MOT_TYPE_DOUBLE,  false);
    t.warehouse->AddColumn("W_KEY",      8,   F::MOT_TYPE_LONG,    true);
    if (!t.warehouse->InitRowPool()) return false;
    if (!t.warehouse->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.warehouse);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable warehouse: %s\n", RcToString(rc)); return false; }
    t.ix_warehouse = MakePrimaryIndex(txn, t.warehouse, "ix_warehouse");
    if (!t.ix_warehouse) return false;

    // ---- DISTRICT (Clause 1.3.2) ----
    t.district = new Table();
    if (!t.district->Init("district", "public.district", DIST::D_NUM_COLS)) return false;
    t.district->AddColumn("D_ID",        8,  F::MOT_TYPE_LONG,    false);
    t.district->AddColumn("D_W_ID",      8,  F::MOT_TYPE_LONG,    false);
    t.district->AddColumn("D_NAME",      11, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_STREET_1",  21, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_STREET_2",  21, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_CITY",      21, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_STATE",     3,  F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_ZIP",       10, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_TAX",       8,  F::MOT_TYPE_DOUBLE,  false);
    t.district->AddColumn("D_YTD",       8,  F::MOT_TYPE_DOUBLE,  false);
    t.district->AddColumn("D_NEXT_O_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.district->AddColumn("D_KEY",       8,  F::MOT_TYPE_LONG,    true);
    if (!t.district->InitRowPool()) return false;
    if (!t.district->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.district);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable district: %s\n", RcToString(rc)); return false; }
    t.ix_district = MakePrimaryIndex(txn, t.district, "ix_district");
    if (!t.ix_district) return false;

    // ---- CUSTOMER (Clause 1.3.3) ----
    t.customer = new Table();
    if (!t.customer->Init("customer", "public.customer", CUST::C_NUM_COLS)) return false;
    t.customer->AddColumn("C_ID",          8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_D_ID",       8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_W_ID",       8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_FIRST",       17,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_MIDDLE",      3,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_LAST",        17,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_STREET_1",    21,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_STREET_2",    21,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_CITY",        21,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_STATE",       3,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_ZIP",         10,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_PHONE",       17,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_SINCE",       8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_CREDIT",      3,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_CREDIT_LIM",  8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_DISCOUNT",    8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_BALANCE",     8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_YTD_PAYMENT", 8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_PAYMENT_CNT", 8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_DELIVERY_CNT",8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_DATA",        501, F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_KEY",         8,   F::MOT_TYPE_LONG,    true);
    if (!t.customer->InitRowPool()) return false;
    if (!t.customer->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.customer);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable customer: %s\n", RcToString(rc)); return false; }
    t.ix_customer = MakePrimaryIndex(txn, t.customer, "ix_customer");
    if (!t.ix_customer) return false;
    // Secondary index: (C_W_ID, C_D_ID, C_LAST, C_FIRST) — non-unique, manually managed.
    // Not registered on the table (BuildKey's InternalKey path can't build multi-field keys).
    {
        RC irc = RC_OK;
        t.ix_customer_last = IndexFactory::CreateIndexEx(
            IndexOrder::INDEX_ORDER_SECONDARY, IndexingMethod::INDEXING_METHOD_TREE,
            DEFAULT_TREE_FLAVOR, false /*non-unique*/, CUST_LAST_USER_KEY_LEN,
            "ix_customer_last", irc, nullptr);
        if (!t.ix_customer_last || irc != RC_OK) {
            fprintf(stderr, "ERROR: Failed to create ix_customer_last: %s\n", RcToString(irc));
            return false;
        }
        t.ix_customer_last->SetTable(t.customer);
    }

    // ---- HISTORY (Clause 1.3.4) ----
    // No natural PK per spec — use fake primary with surrogate m_rowId
    t.history = new Table();
    if (!t.history->Init("history", "public.history", HIST::H_NUM_COLS)) return false;
    t.history->AddColumn("H_C_ID",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_C_D_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_C_W_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_D_ID",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_W_ID",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_DATE",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_AMOUNT", 8,  F::MOT_TYPE_DOUBLE,  false);
    t.history->AddColumn("H_DATA",   25, F::MOT_TYPE_VARCHAR, false);
    if (!t.history->InitRowPool()) return false;
    if (!t.history->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.history);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable history: %s\n", RcToString(rc)); return false; }
    {
        // Fake primary index — key is the auto-generated surrogate m_rowId
        uint32_t keyLen = sizeof(uint64_t);  // 8 bytes for surrogate key
        RC irc = RC_OK;
        Index* ix = IndexFactory::CreateIndexEx(
            IndexOrder::INDEX_ORDER_PRIMARY, IndexingMethod::INDEXING_METHOD_TREE,
            DEFAULT_TREE_FLAVOR, true, keyLen, "ix_history", irc, nullptr);
        if (!ix || irc != RC_OK) { fprintf(stderr, "ERROR: ix_history: %s\n", RcToString(irc)); return false; }
        if (!ix->SetNumTableFields(t.history->GetFieldCount())) { delete ix; return false; }
        ix->SetNumIndexFields(0);  // no user key fields — surrogate
        ix->SetTable(t.history);
        ix->SetFakePrimary(true);
        irc = txn->CreateIndex(t.history, ix, true);
        if (irc != RC_OK) { delete ix; return false; }
        t.ix_history = ix;
    }

    // ---- NEW-ORDER (Clause 1.3.5) ----
    t.new_order = new Table();
    if (!t.new_order->Init("new_order", "public.new_order", NORD::NO_NUM_COLS)) return false;
    t.new_order->AddColumn("NO_O_ID", 8, F::MOT_TYPE_LONG, false);
    t.new_order->AddColumn("NO_D_ID", 8, F::MOT_TYPE_LONG, false);
    t.new_order->AddColumn("NO_W_ID", 8, F::MOT_TYPE_LONG, false);
    t.new_order->AddColumn("NO_KEY",  8, F::MOT_TYPE_LONG, true);
    if (!t.new_order->InitRowPool()) return false;
    if (!t.new_order->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.new_order);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable new_order: %s\n", RcToString(rc)); return false; }
    t.ix_new_order = MakePrimaryIndex(txn, t.new_order, "ix_new_order");
    if (!t.ix_new_order) return false;

    // ---- ORDER (Clause 1.3.6) ----
    t.order_tbl = new Table();
    if (!t.order_tbl->Init("oorder", "public.oorder", ORD::O_NUM_COLS)) return false;
    t.order_tbl->AddColumn("O_ID",         8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_D_ID",       8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_W_ID",       8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_C_ID",       8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_ENTRY_D",    8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_CARRIER_ID", 8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_OL_CNT",     8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_ALL_LOCAL",  8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_KEY",        8, F::MOT_TYPE_LONG, true);
    if (!t.order_tbl->InitRowPool()) return false;
    if (!t.order_tbl->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.order_tbl);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable oorder: %s\n", RcToString(rc)); return false; }
    t.ix_order = MakePrimaryIndex(txn, t.order_tbl, "ix_order");
    if (!t.ix_order) return false;
    // Secondary index: (O_W_ID, O_D_ID, O_C_ID, O_ID) — non-unique, manually managed.
    // Used by OrderStatus to find latest order for a customer.
    {
        RC irc = RC_OK;
        t.ix_order_customer = IndexFactory::CreateIndexEx(
            IndexOrder::INDEX_ORDER_SECONDARY, IndexingMethod::INDEXING_METHOD_TREE,
            DEFAULT_TREE_FLAVOR, false /*non-unique*/, ORD_CUST_USER_KEY_LEN,
            "ix_order_customer", irc, nullptr);
        if (!t.ix_order_customer || irc != RC_OK) {
            fprintf(stderr, "ERROR: Failed to create ix_order_customer: %s\n", RcToString(irc));
            return false;
        }
        t.ix_order_customer->SetTable(t.order_tbl);
    }

    // ---- ORDER-LINE (Clause 1.3.7) ----
    t.order_line = new Table();
    if (!t.order_line->Init("order_line", "public.order_line", ORDL::OL_NUM_COLS)) return false;
    t.order_line->AddColumn("OL_O_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_D_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_W_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_NUMBER",     8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_I_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_SUPPLY_W_ID",8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_DELIVERY_D", 8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_QUANTITY",   8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_AMOUNT",     8,  F::MOT_TYPE_DOUBLE,  false);
    t.order_line->AddColumn("OL_DIST_INFO",  25, F::MOT_TYPE_VARCHAR, false);
    t.order_line->AddColumn("OL_KEY",        8,  F::MOT_TYPE_LONG,    true);
    if (!t.order_line->InitRowPool()) return false;
    if (!t.order_line->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.order_line);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable order_line: %s\n", RcToString(rc)); return false; }
    t.ix_order_line = MakePrimaryIndex(txn, t.order_line, "ix_order_line");
    if (!t.ix_order_line) return false;

    // ---- ITEM (Clause 1.3.8) ----
    t.item = new Table();
    if (!t.item->Init("item", "public.item", ITEM::I_NUM_COLS)) return false;
    t.item->AddColumn("I_ID",    8,  F::MOT_TYPE_LONG,    false);
    t.item->AddColumn("I_IM_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.item->AddColumn("I_NAME",  25, F::MOT_TYPE_VARCHAR, false);
    t.item->AddColumn("I_PRICE", 8,  F::MOT_TYPE_DOUBLE,  false);
    t.item->AddColumn("I_DATA",  51, F::MOT_TYPE_VARCHAR, false);
    t.item->AddColumn("I_KEY",   8,  F::MOT_TYPE_LONG,    true);
    if (!t.item->InitRowPool()) return false;
    if (!t.item->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.item);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable item: %s\n", RcToString(rc)); return false; }
    t.ix_item = MakePrimaryIndex(txn, t.item, "ix_item");
    if (!t.ix_item) return false;

    // ---- STOCK (Clause 1.3.9) ----
    t.stock = new Table();
    if (!t.stock->Init("stock", "public.stock", STK::S_NUM_COLS)) return false;
    t.stock->AddColumn("S_I_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_W_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_QUANTITY",   8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_DIST_01",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_02",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_03",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_04",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_05",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_06",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_07",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_08",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_09",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_10",    25, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_YTD",        8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_ORDER_CNT",  8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_REMOTE_CNT", 8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_DATA",       51, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_KEY",        8,  F::MOT_TYPE_LONG,    true);
    if (!t.stock->InitRowPool()) return false;
    if (!t.stock->InitTombStonePool()) return false;
    rc = txn->CreateTable(t.stock);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable stock: %s\n", RcToString(rc)); return false; }
    t.ix_stock = MakePrimaryIndex(txn, t.stock, "ix_stock");
    if (!t.ix_stock) return false;

    return true;
}

// ======================================================================
// Data Population — Clause 4.3.3
//
// Each population function uses a single bulk transaction:
//   StartTransaction → loop(CreateNewRow, SetInternalKey, table->InsertRow) → Commit → EndTransaction
// ======================================================================

static void PopulateItems(TxnManager* txn, TpccTables& t, FastRandom& rng)
{
    printf("    Populating ITEM (%u rows)...\n", ITEM_COUNT);
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    for (uint64_t i = 1; i <= ITEM_COUNT; i++) {
        Row* row = t.item->CreateNewRow();
        if (!row) continue;
        row->SetValue<uint64_t>(ITEM::I_ID, i);
        row->SetValue<uint64_t>(ITEM::I_IM_ID, rng.NextUniform(1, 10000));
        char name[25]; rng.RandomString(name, 14, 24);
        SetStringValue(row, ITEM::I_NAME, name);
        row->SetValue<double>(ITEM::I_PRICE, (double)rng.NextUniform(100, 10000) / 100.0);
        char data[51]; rng.RandomString(data, 26, 50);
        if (rng.NextUniform(1, 10) == 1) {
            uint32_t pos = (uint32_t)rng.NextUniform(0, strlen(data) > 8 ? strlen(data) - 8 : 0);
            memcpy(data + pos, "ORIGINAL", 8);
        }
        SetStringValue(row, ITEM::I_DATA, data);
        row->SetInternalKey(ITEM::I_KEY, PackItemKey(i));
        RC rc = t.item->InsertRow(row, txn);
        if (rc != RC_OK) {
            t.item->DestroyRow(row);
            fprintf(stderr, "WARN: Failed to insert ITEM %lu: %s\n", (unsigned long)i, RcToString(rc));
        }
    }
    RC rc = txn->Commit();
    if (rc != RC_OK) fprintf(stderr, "WARN: ITEM commit failed: %s\n", RcToString(rc));
    txn->EndTransaction();
}

static void PopulateWarehouse(TxnManager* txn, TpccTables& t, uint64_t w_id, FastRandom& rng)
{
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    Row* row = t.warehouse->CreateNewRow();
    row->SetValue<uint64_t>(WH::W_ID, w_id);
    char buf[21];
    rng.RandomString(buf, 6, 10);  SetStringValue(row, WH::W_NAME, buf);
    rng.RandomString(buf, 10, 20); SetStringValue(row, WH::W_STREET_1, buf);
    rng.RandomString(buf, 10, 20); SetStringValue(row, WH::W_STREET_2, buf);
    rng.RandomString(buf, 10, 20); SetStringValue(row, WH::W_CITY, buf);
    rng.RandomString(buf, 2, 2);   SetStringValue(row, WH::W_STATE, buf);
    rng.RandomNumericString(buf, 9);SetStringValue(row, WH::W_ZIP, buf);
    row->SetValue<double>(WH::W_TAX, (double)rng.NextUniform(0, 2000) / 10000.0);
    row->SetValue<double>(WH::W_YTD, 300000.00);
    row->SetInternalKey(WH::W_KEY, PackWhKey(w_id));
    t.warehouse->InsertRow(row, txn);
    txn->Commit();
    txn->EndTransaction();

    /* // Uncomment to verify warehouse readback:
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    Key* vkey = BuildSearchKey(t.ix_warehouse, t.warehouse, PackWhKey(w_id));
    RC vrc = RC_OK;
    Row* found = txn->RowLookupByKey(t.warehouse, AccessType::RD, vkey, vrc);
    if (found) {
        double ytd; found->GetValue(WH::W_YTD, ytd);
        fprintf(stderr, "VERIFY WH(%lu): OK ytd=%.2f\n", (unsigned long)w_id, ytd);
    } else {
        fprintf(stderr, "VERIFY WH(%lu): FAIL\n", (unsigned long)w_id);
    }
    t.ix_warehouse->DestroyKey(vkey);
    txn->Rollback();
    */
}

static void PopulateDistricts(TxnManager* txn, TpccTables& t, uint64_t w_id, FastRandom& rng)
{
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
        Row* row = t.district->CreateNewRow();
        row->SetValue<uint64_t>(DIST::D_ID, d);
        row->SetValue<uint64_t>(DIST::D_W_ID, w_id);
        char buf[21];
        rng.RandomString(buf, 6, 10); SetStringValue(row, DIST::D_NAME, buf);
        rng.RandomString(buf, 10, 20); SetStringValue(row, DIST::D_STREET_1, buf);
        rng.RandomString(buf, 10, 20); SetStringValue(row, DIST::D_STREET_2, buf);
        rng.RandomString(buf, 10, 20); SetStringValue(row, DIST::D_CITY, buf);
        rng.RandomString(buf, 2, 2);   SetStringValue(row, DIST::D_STATE, buf);
        rng.RandomNumericString(buf, 9);SetStringValue(row, DIST::D_ZIP, buf);
        row->SetValue<double>(DIST::D_TAX, (double)rng.NextUniform(0, 2000) / 10000.0);
        row->SetValue<double>(DIST::D_YTD, 30000.00);
        row->SetValue<uint64_t>(DIST::D_NEXT_O_ID, ORD_PER_DIST + 1);
        row->SetInternalKey(DIST::D_KEY, PackDistKey(w_id, d));
        t.district->InsertRow(row, txn);
    }
    txn->Commit();
    txn->EndTransaction();
}

static void PopulateCustomers(TxnManager* txn, TpccTables& t,
                              uint64_t w_id, uint64_t d_id, FastRandom& rng)
{
    int64_t now = (int64_t)time(nullptr);

    // Customer rows
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    for (uint64_t c = 1; c <= CUST_PER_DIST; c++) {
        Row* row = t.customer->CreateNewRow();
        row->SetValue<uint64_t>(CUST::C_ID, c);
        row->SetValue<uint64_t>(CUST::C_D_ID, d_id);
        row->SetValue<uint64_t>(CUST::C_W_ID, w_id);
        char buf[21];
        rng.RandomString(buf, 8, 16); SetStringValue(row, CUST::C_FIRST, buf);
        SetStringValue(row, CUST::C_MIDDLE, "OE");
        char last[17];
        if (c <= 1000)
            GenLastName((uint32_t)(c - 1), last, sizeof(last));
        else
            GenLastName((uint32_t)NURand(rng, NURAND_C_LAST, 0, 999), last, sizeof(last));
        SetStringValue(row, CUST::C_LAST, last);
        rng.RandomString(buf, 10, 20); SetStringValue(row, CUST::C_STREET_1, buf);
        rng.RandomString(buf, 10, 20); SetStringValue(row, CUST::C_STREET_2, buf);
        rng.RandomString(buf, 10, 20); SetStringValue(row, CUST::C_CITY, buf);
        rng.RandomString(buf, 2, 2);   SetStringValue(row, CUST::C_STATE, buf);
        rng.RandomNumericString(buf, 9);SetStringValue(row, CUST::C_ZIP, buf);
        rng.RandomNumericString(buf, 16);SetStringValue(row, CUST::C_PHONE, buf);
        row->SetValue<int64_t>(CUST::C_SINCE, now);
        SetStringValue(row, CUST::C_CREDIT, (rng.NextUniform(1, 10) == 1) ? "BC" : "GC");
        row->SetValue<double>(CUST::C_CREDIT_LIM, 50000.00);
        row->SetValue<double>(CUST::C_DISCOUNT, (double)rng.NextUniform(0, 5000) / 10000.0);
        row->SetValue<double>(CUST::C_BALANCE, -10.00);
        row->SetValue<double>(CUST::C_YTD_PAYMENT, 10.00);
        row->SetValue<uint64_t>(CUST::C_PAYMENT_CNT, 1);
        row->SetValue<uint64_t>(CUST::C_DELIVERY_CNT, 0);
        char data[501]; rng.RandomString(data, 300, 500);
        SetStringValue(row, CUST::C_DATA, data);
        row->SetInternalKey(CUST::C_KEY, PackCustKey(w_id, d_id, c));
        t.customer->InsertRow(row, txn);
    }
    txn->Commit();
    txn->EndTransaction();

    // History rows (separate transaction)
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    for (uint64_t c = 1; c <= CUST_PER_DIST; c++) {
        Row* hrow = t.history->CreateNewRow();
        hrow->SetValue<uint64_t>(HIST::H_C_ID, c);
        hrow->SetValue<uint64_t>(HIST::H_C_D_ID, d_id);
        hrow->SetValue<uint64_t>(HIST::H_C_W_ID, w_id);
        hrow->SetValue<uint64_t>(HIST::H_D_ID, d_id);
        hrow->SetValue<uint64_t>(HIST::H_W_ID, w_id);
        hrow->SetValue<int64_t>(HIST::H_DATE, now);
        hrow->SetValue<double>(HIST::H_AMOUNT, 10.00);
        char buf[25]; rng.RandomString(buf, 12, 24);
        SetStringValue(hrow, HIST::H_DATA, buf);
        // No SetInternalKey — fake primary auto-generates surrogate via SetRowId
        t.history->InsertRow(hrow, txn);
    }
    txn->Commit();
    txn->EndTransaction();
}

static void PopulateOrders(TxnManager* txn, TpccTables& t,
                           uint64_t w_id, uint64_t d_id, FastRandom& rng)
{
    std::vector<uint64_t> perm(CUST_PER_DIST);
    std::iota(perm.begin(), perm.end(), 1);
    for (uint32_t i = CUST_PER_DIST - 1; i > 0; i--) {
        uint32_t j = (uint32_t)rng.NextUniform(0, i);
        std::swap(perm[i], perm[j]);
    }
    int64_t now = (int64_t)time(nullptr);

    // Orders + NewOrders + OrderLines in one transaction
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    for (uint64_t o = 1; o <= ORD_PER_DIST; o++) {
        uint64_t c_id = perm[o - 1];
        uint64_t ol_cnt = rng.NextUniform(MIN_OL_CNT, MAX_OL_CNT);

        // ORDER row
        Row* orow = t.order_tbl->CreateNewRow();
        orow->SetValue<uint64_t>(ORD::O_ID, o);
        orow->SetValue<uint64_t>(ORD::O_C_ID, c_id);
        orow->SetValue<uint64_t>(ORD::O_D_ID, d_id);
        orow->SetValue<uint64_t>(ORD::O_W_ID, w_id);
        orow->SetValue<int64_t>(ORD::O_ENTRY_D, now);
        orow->SetValue<uint64_t>(ORD::O_CARRIER_ID, (o < 2101) ? rng.NextUniform(1, 10) : 0);
        orow->SetValue<uint64_t>(ORD::O_OL_CNT, ol_cnt);
        orow->SetValue<uint64_t>(ORD::O_ALL_LOCAL, 1);
        orow->SetInternalKey(ORD::O_KEY, PackOrderKey(w_id, d_id, o));
        t.order_tbl->InsertRow(orow, txn);

        // NEW-ORDER for last 900 orders
        if (o >= 2101) {
            Row* norow = t.new_order->CreateNewRow();
            norow->SetValue<uint64_t>(NORD::NO_O_ID, o);
            norow->SetValue<uint64_t>(NORD::NO_D_ID, d_id);
            norow->SetValue<uint64_t>(NORD::NO_W_ID, w_id);
            norow->SetInternalKey(NORD::NO_KEY, PackOrderKey(w_id, d_id, o));
            t.new_order->InsertRow(norow, txn);
        }

        // ORDER-LINE rows
        for (uint64_t ol = 1; ol <= ol_cnt; ol++) {
            Row* olrow = t.order_line->CreateNewRow();
            olrow->SetValue<uint64_t>(ORDL::OL_O_ID, o);
            olrow->SetValue<uint64_t>(ORDL::OL_NUMBER, ol);
            olrow->SetValue<uint64_t>(ORDL::OL_I_ID, rng.NextUniform(1, ITEM_COUNT));
            olrow->SetValue<uint64_t>(ORDL::OL_D_ID, d_id);
            olrow->SetValue<uint64_t>(ORDL::OL_W_ID, w_id);
            olrow->SetValue<uint64_t>(ORDL::OL_SUPPLY_W_ID, w_id);
            olrow->SetValue<int64_t>(ORDL::OL_DELIVERY_D, (o < 2101) ? now : 0);
            olrow->SetValue<uint64_t>(ORDL::OL_QUANTITY, 5);
            olrow->SetValue<double>(ORDL::OL_AMOUNT,
                (o < 2101) ? 0.0 : (double)rng.NextUniform(1, 999999) / 100.0);
            char dist_info[25]; rng.RandomString(dist_info, 24, 24);
            SetStringValue(olrow, ORDL::OL_DIST_INFO, dist_info);
            olrow->SetInternalKey(ORDL::OL_KEY, PackOlKey(w_id, d_id, o, ol));
            t.order_line->InsertRow(olrow, txn);
        }
    }
    txn->Commit();
    txn->EndTransaction();
}

static void PopulateStock(TxnManager* txn, TpccTables& t, uint64_t w_id, FastRandom& rng)
{
    txn->StartTransaction(txn->GetTransactionId(), ISOLATION_LEVEL::READ_COMMITED);
    for (uint64_t i = 1; i <= STOCK_PER_WARE; i++) {
        Row* row = t.stock->CreateNewRow();
        if (!row) continue;
        row->SetValue<uint64_t>(STK::S_I_ID, i);
        row->SetValue<uint64_t>(STK::S_W_ID, w_id);
        row->SetValue<uint64_t>(STK::S_QUANTITY, rng.NextUniform(10, 100));
        char dist[25];
        for (int d = 0; d < 10; d++) {
            rng.RandomString(dist, 24, 24);
            SetStringValue(row, STK::S_DIST_01 + d, dist);
        }
        row->SetValue<uint64_t>(STK::S_YTD, 0);
        row->SetValue<uint64_t>(STK::S_ORDER_CNT, 0);
        row->SetValue<uint64_t>(STK::S_REMOTE_CNT, 0);
        char data[51]; rng.RandomString(data, 26, 50);
        if (rng.NextUniform(1, 10) == 1) {
            uint32_t pos = (uint32_t)rng.NextUniform(0, strlen(data) > 8 ? strlen(data) - 8 : 0);
            memcpy(data + pos, "ORIGINAL", 8);
        }
        SetStringValue(row, STK::S_DATA, data);
        row->SetInternalKey(STK::S_KEY, PackStockKey(w_id, i));
        t.stock->InsertRow(row, txn);
    }
    txn->Commit();
    txn->EndTransaction();
}

// ======================================================================
// Per-warehouse thread
// ======================================================================

struct PopulateArgs {
    MOTEngine*  engine;
    TpccTables* tables;
    uint64_t    w_id;
};

static void PopulateWarehouseThread(PopulateArgs args)
{
    SessionContext* session = args.engine->GetSessionManager()->CreateSessionContext();
    if (!session) {
        fprintf(stderr, "ERROR: Failed to create session for W%lu\n", (unsigned long)args.w_id);
        return;
    }
    TxnManager* txn = session->GetTxnManager();
    FastRandom rng(args.w_id * 31337 + 42);

    printf("  [W%lu] Populating...\n", (unsigned long)args.w_id);
    PopulateWarehouse(txn, *args.tables, args.w_id, rng);
    PopulateDistricts(txn, *args.tables, args.w_id, rng);
    PopulateStock(txn, *args.tables, args.w_id, rng);
    for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
        PopulateCustomers(txn, *args.tables, args.w_id, d, rng);
        PopulateOrders(txn, *args.tables, args.w_id, d, rng);
    }
    printf("  [W%lu] Done.\n", (unsigned long)args.w_id);
    args.engine->GetSessionManager()->DestroySessionContext(session);
    // NOTE: Do NOT call OnCurrentThreadEnding() here.  That call clears the
    // thread's memory-pool caches (ClearTablesThreadMemoryCache), orphaning
    // sub-pools that still hold live populated rows.  When worker threads
    // later reuse the same thread-ID, MVCC operations on those rows trigger
    // heap corruption.  The pthread-key destructor will free the thread-ID
    // in the bitset automatically when this OS thread exits.
}

bool PopulateData(MOTEngine* engine, TpccTables& tables, uint32_t num_warehouses, bool small_schema)
{
    (void)small_schema;
    printf("[Population] Loading TPC-C data for %u warehouse(s)...\n", num_warehouses);

    // ITEM table first (shared, single-threaded)
    {
        SessionContext* session = engine->GetSessionManager()->CreateSessionContext();
        if (!session) { fprintf(stderr, "ERROR: session for ITEM\n"); return false; }
        TxnManager* txn = session->GetTxnManager();
        FastRandom rng(12345);
        PopulateItems(txn, tables, rng);
        engine->GetSessionManager()->DestroySessionContext(session);
    }

    // One thread per warehouse for the rest
    std::vector<std::thread> threads;
    threads.reserve(num_warehouses);
    for (uint32_t w = 1; w <= num_warehouses; w++) {
        threads.emplace_back(PopulateWarehouseThread, PopulateArgs{engine, &tables, w});
    }
    for (auto& th : threads) th.join();

    // Populate the manually-managed customer secondary index (by-last-name).
    // Must run after all warehouse threads finish so all customer rows exist.
    if (tables.ix_customer_last && tables.ix_customer) {
        printf("  Building ix_customer_last secondary index...\n");
        PopulateCustLastIndex(tables.ix_customer_last, tables.ix_customer,
                              tables.customer);
    }

    // Populate the manually-managed order secondary index (by-customer).
    // Must run after all warehouse threads finish so all order rows exist.
    if (tables.ix_order_customer && tables.ix_order) {
        printf("  Building ix_order_customer secondary index...\n");
        PopulateOrderCustIndex(tables.ix_order_customer, tables.ix_order,
                               tables.order_tbl);
    }

    printf("[Population] Done.\n\n");
    return true;
}

}  // namespace oro::tpcc
