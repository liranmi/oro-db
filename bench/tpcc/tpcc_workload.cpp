/*
 * TPC-C Schema Creation and Data Population for oro-db.
 *
 * Schema follows TPC-C Standard Specification Rev 5.11, Clause 1.3.
 * Population follows Clause 4.3.3.
 */

#include "tpcc_workload.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "bench_util.h"
#include "catalog_column_types.h"
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

// Helper: set a string column value (Row::SetValue(int, char*) is protected)
static inline void SetStringValue(Row* row, int colId, const char* str)
{
    row->SetValueVariable(colId, str, strlen(str) + 1);
}

namespace oro::tpcc {

// ======================================================================
// Helper: create an index, set key column metadata, register with txn
// ======================================================================
static Index* MakeIndex(TxnManager* txn, Table* table, const char* name,
                        IndexOrder order, bool unique, uint32_t keyLen,
                        const std::vector<std::pair<int, uint16_t>>& keyCols)
{
    RC rc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        order, IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR, unique, keyLen, name, rc, nullptr);
    if (ix == nullptr || rc != RC_OK) {
        fprintf(stderr, "ERROR: Failed to create index '%s': %s\n", name, RcToString(rc));
        return nullptr;
    }

    if (!ix->SetNumTableFields(table->GetFieldCount())) {
        fprintf(stderr, "ERROR: SetNumTableFields failed for index '%s'\n", name);
        delete ix;
        return nullptr;
    }
    ix->SetNumIndexFields((uint32_t)keyCols.size());
    for (uint32_t i = 0; i < keyCols.size(); i++) {
        ix->SetLenghtKeyFields(i, keyCols[i].first, keyCols[i].second);
    }
    ix->SetTable(table);

    rc = txn->CreateIndex(table, ix, order == IndexOrder::INDEX_ORDER_PRIMARY);
    if (rc != RC_OK) {
        fprintf(stderr, "ERROR: CreateIndex '%s' failed: %s\n", name, RcToString(rc));
        delete ix;
        return nullptr;
    }
    return ix;
}

// ======================================================================
// Schema creation — Full schema (Clause 1.3)
// ======================================================================
bool CreateSchema(TxnManager* txn, TpccTables& t, bool small_schema)
{
    using F = MOT_CATALOG_FIELD_TYPES;
    RC rc;
    (void)small_schema;  // TODO: small schema variant

    // ---- WAREHOUSE (Clause 1.3.1) ----
    t.warehouse = new Table();
    if (!t.warehouse->Init("warehouse", "public.warehouse", WH::W_NUM_COLS)) return false;
    t.warehouse->AddColumn("W_ID",       8,   F::MOT_TYPE_LONG,    true);
    t.warehouse->AddColumn("W_NAME",     10,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_STREET_1", 20,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_STREET_2", 20,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_CITY",     20,  F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_STATE",    2,   F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_ZIP",      9,   F::MOT_TYPE_VARCHAR, false);
    t.warehouse->AddColumn("W_TAX",      8,   F::MOT_TYPE_DOUBLE,  false);
    t.warehouse->AddColumn("W_YTD",      8,   F::MOT_TYPE_DOUBLE,  false);
    if (!t.warehouse->InitRowPool()) return false;
    rc = txn->CreateTable(t.warehouse);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable warehouse: %s\n", RcToString(rc)); return false; }
    t.ix_warehouse = MakeIndex(txn, t.warehouse, "ix_warehouse",
        IndexOrder::INDEX_ORDER_PRIMARY, true, WH_KEY_LEN,
        {{WH::W_ID, K}});
    if (!t.ix_warehouse) return false;

    // ---- DISTRICT (Clause 1.3.2) ----
    t.district = new Table();
    if (!t.district->Init("district", "public.district", DIST::D_NUM_COLS)) return false;
    t.district->AddColumn("D_ID",        8,  F::MOT_TYPE_LONG,    true);
    t.district->AddColumn("D_W_ID",      8,  F::MOT_TYPE_LONG,    true);
    t.district->AddColumn("D_NAME",      10, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_STREET_1",  20, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_STREET_2",  20, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_CITY",      20, F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_STATE",     2,  F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_ZIP",       9,  F::MOT_TYPE_VARCHAR, false);
    t.district->AddColumn("D_TAX",       8,  F::MOT_TYPE_DOUBLE,  false);
    t.district->AddColumn("D_YTD",       8,  F::MOT_TYPE_DOUBLE,  false);
    t.district->AddColumn("D_NEXT_O_ID", 8,  F::MOT_TYPE_LONG,    false);
    if (!t.district->InitRowPool()) return false;
    rc = txn->CreateTable(t.district);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable district: %s\n", RcToString(rc)); return false; }
    t.ix_district = MakeIndex(txn, t.district, "ix_district",
        IndexOrder::INDEX_ORDER_PRIMARY, true, DIST_KEY_LEN,
        {{DIST::D_W_ID, K}, {DIST::D_ID, K}});
    if (!t.ix_district) return false;

    // ---- CUSTOMER (Clause 1.3.3) ----
    t.customer = new Table();
    if (!t.customer->Init("customer", "public.customer", CUST::C_NUM_COLS)) return false;
    t.customer->AddColumn("C_ID",          8,   F::MOT_TYPE_LONG,    true);
    t.customer->AddColumn("C_D_ID",        8,   F::MOT_TYPE_LONG,    true);
    t.customer->AddColumn("C_W_ID",        8,   F::MOT_TYPE_LONG,    true);
    t.customer->AddColumn("C_FIRST",       16,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_MIDDLE",      2,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_LAST",        16,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_STREET_1",    20,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_STREET_2",    20,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_CITY",        20,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_STATE",       2,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_ZIP",         9,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_PHONE",       16,  F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_SINCE",       8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_CREDIT",      2,   F::MOT_TYPE_VARCHAR, false);
    t.customer->AddColumn("C_CREDIT_LIM",  8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_DISCOUNT",    8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_BALANCE",     8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_YTD_PAYMENT", 8,   F::MOT_TYPE_DOUBLE,  false);
    t.customer->AddColumn("C_PAYMENT_CNT", 8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_DELIVERY_CNT",8,   F::MOT_TYPE_LONG,    false);
    t.customer->AddColumn("C_DATA",        500, F::MOT_TYPE_VARCHAR, false);
    if (!t.customer->InitRowPool()) return false;
    rc = txn->CreateTable(t.customer);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable customer: %s\n", RcToString(rc)); return false; }
    t.ix_customer = MakeIndex(txn, t.customer, "ix_customer",
        IndexOrder::INDEX_ORDER_PRIMARY, true, CUST_KEY_LEN,
        {{CUST::C_W_ID, K}, {CUST::C_D_ID, K}, {CUST::C_ID, K}});
    if (!t.ix_customer) return false;
    // Secondary: customer by last name (non-unique for mid-point selection per Clause 2.5.2.2)
    t.ix_customer_last = MakeIndex(txn, t.customer, "ix_customer_last",
        IndexOrder::INDEX_ORDER_SECONDARY, false, CUST_LAST_KEY_LEN,
        {{CUST::C_W_ID, K}, {CUST::C_D_ID, K}, {CUST::C_LAST, 16}});
    if (!t.ix_customer_last) return false;

    // ---- HISTORY (Clause 1.3.4) ----
    t.history = new Table();
    if (!t.history->Init("history", "public.history", HIST::H_NUM_COLS)) return false;
    t.history->AddColumn("H_C_ID",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_C_D_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_C_W_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_D_ID",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_W_ID",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_DATE",   8,  F::MOT_TYPE_LONG,    false);
    t.history->AddColumn("H_AMOUNT", 8,  F::MOT_TYPE_DOUBLE,  false);
    t.history->AddColumn("H_DATA",   24, F::MOT_TYPE_VARCHAR, false);
    if (!t.history->InitRowPool()) return false;
    rc = txn->CreateTable(t.history);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable history: %s\n", RcToString(rc)); return false; }
    // History has no natural PK per spec. We use a surrogate via H_C_ID slot.
    t.ix_history = MakeIndex(txn, t.history, "ix_history",
        IndexOrder::INDEX_ORDER_PRIMARY, true, HIST_KEY_LEN,
        {{HIST::H_C_ID, K}});
    if (!t.ix_history) return false;

    // ---- NEW-ORDER (Clause 1.3.5) ----
    t.new_order = new Table();
    if (!t.new_order->Init("new_order", "public.new_order", NORD::NO_NUM_COLS)) return false;
    t.new_order->AddColumn("NO_O_ID", 8, F::MOT_TYPE_LONG, true);
    t.new_order->AddColumn("NO_D_ID", 8, F::MOT_TYPE_LONG, true);
    t.new_order->AddColumn("NO_W_ID", 8, F::MOT_TYPE_LONG, true);
    if (!t.new_order->InitRowPool()) return false;
    rc = txn->CreateTable(t.new_order);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable new_order: %s\n", RcToString(rc)); return false; }
    t.ix_new_order = MakeIndex(txn, t.new_order, "ix_new_order",
        IndexOrder::INDEX_ORDER_PRIMARY, true, ORDER_KEY_LEN,
        {{NORD::NO_W_ID, K}, {NORD::NO_D_ID, K}, {NORD::NO_O_ID, K}});
    if (!t.ix_new_order) return false;

    // ---- ORDER (Clause 1.3.6) ----
    t.order_tbl = new Table();
    if (!t.order_tbl->Init("oorder", "public.oorder", ORD::O_NUM_COLS)) return false;
    t.order_tbl->AddColumn("O_ID",         8, F::MOT_TYPE_LONG, true);
    t.order_tbl->AddColumn("O_C_ID",       8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_D_ID",       8, F::MOT_TYPE_LONG, true);
    t.order_tbl->AddColumn("O_W_ID",       8, F::MOT_TYPE_LONG, true);
    t.order_tbl->AddColumn("O_ENTRY_D",    8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_CARRIER_ID", 8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_OL_CNT",     8, F::MOT_TYPE_LONG, false);
    t.order_tbl->AddColumn("O_ALL_LOCAL",  8, F::MOT_TYPE_LONG, false);
    if (!t.order_tbl->InitRowPool()) return false;
    rc = txn->CreateTable(t.order_tbl);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable oorder: %s\n", RcToString(rc)); return false; }
    t.ix_order = MakeIndex(txn, t.order_tbl, "ix_order",
        IndexOrder::INDEX_ORDER_PRIMARY, true, ORDER_KEY_LEN,
        {{ORD::O_W_ID, K}, {ORD::O_D_ID, K}, {ORD::O_ID, K}});
    if (!t.ix_order) return false;

    // ---- ORDER-LINE (Clause 1.3.7) ----
    t.order_line = new Table();
    if (!t.order_line->Init("order_line", "public.order_line", ORDL::OL_NUM_COLS)) return false;
    t.order_line->AddColumn("OL_O_ID",       8,  F::MOT_TYPE_LONG,    true);
    t.order_line->AddColumn("OL_D_ID",       8,  F::MOT_TYPE_LONG,    true);
    t.order_line->AddColumn("OL_W_ID",       8,  F::MOT_TYPE_LONG,    true);
    t.order_line->AddColumn("OL_NUMBER",     8,  F::MOT_TYPE_LONG,    true);
    t.order_line->AddColumn("OL_I_ID",       8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_SUPPLY_W_ID",8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_DELIVERY_D", 8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_QUANTITY",   8,  F::MOT_TYPE_LONG,    false);
    t.order_line->AddColumn("OL_AMOUNT",     8,  F::MOT_TYPE_DOUBLE,  false);
    t.order_line->AddColumn("OL_DIST_INFO",  24, F::MOT_TYPE_VARCHAR, false);
    if (!t.order_line->InitRowPool()) return false;
    rc = txn->CreateTable(t.order_line);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable order_line: %s\n", RcToString(rc)); return false; }
    t.ix_order_line = MakeIndex(txn, t.order_line, "ix_order_line",
        IndexOrder::INDEX_ORDER_PRIMARY, true, OL_KEY_LEN,
        {{ORDL::OL_W_ID, K}, {ORDL::OL_D_ID, K}, {ORDL::OL_O_ID, K}, {ORDL::OL_NUMBER, K}});
    if (!t.ix_order_line) return false;

    // ---- ITEM (Clause 1.3.8) ----
    t.item = new Table();
    if (!t.item->Init("item", "public.item", ITEM::I_NUM_COLS)) return false;
    t.item->AddColumn("I_ID",    8,  F::MOT_TYPE_LONG,    true);
    t.item->AddColumn("I_IM_ID", 8,  F::MOT_TYPE_LONG,    false);
    t.item->AddColumn("I_NAME",  24, F::MOT_TYPE_VARCHAR, false);
    t.item->AddColumn("I_PRICE", 8,  F::MOT_TYPE_DOUBLE,  false);
    t.item->AddColumn("I_DATA",  50, F::MOT_TYPE_VARCHAR, false);
    if (!t.item->InitRowPool()) return false;
    rc = txn->CreateTable(t.item);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable item: %s\n", RcToString(rc)); return false; }
    t.ix_item = MakeIndex(txn, t.item, "ix_item",
        IndexOrder::INDEX_ORDER_PRIMARY, true, ITEM_KEY_LEN,
        {{ITEM::I_ID, K}});
    if (!t.ix_item) return false;

    // ---- STOCK (Clause 1.3.9) ----
    t.stock = new Table();
    if (!t.stock->Init("stock", "public.stock", STK::S_NUM_COLS)) return false;
    t.stock->AddColumn("S_I_ID",       8,  F::MOT_TYPE_LONG,    true);
    t.stock->AddColumn("S_W_ID",       8,  F::MOT_TYPE_LONG,    true);
    t.stock->AddColumn("S_QUANTITY",   8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_DIST_01",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_02",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_03",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_04",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_05",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_06",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_07",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_08",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_09",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_DIST_10",    24, F::MOT_TYPE_VARCHAR, false);
    t.stock->AddColumn("S_YTD",        8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_ORDER_CNT",  8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_REMOTE_CNT", 8,  F::MOT_TYPE_LONG,    false);
    t.stock->AddColumn("S_DATA",       50, F::MOT_TYPE_VARCHAR, false);
    if (!t.stock->InitRowPool()) return false;
    rc = txn->CreateTable(t.stock);
    if (rc != RC_OK) { fprintf(stderr, "CreateTable stock: %s\n", RcToString(rc)); return false; }
    t.ix_stock = MakeIndex(txn, t.stock, "ix_stock",
        IndexOrder::INDEX_ORDER_PRIMARY, true, STOCK_KEY_LEN,
        {{STK::S_W_ID, K}, {STK::S_I_ID, K}});
    if (!t.ix_stock) return false;

    return true;
}

// ======================================================================
// Data Population — Clause 4.3.3
// ======================================================================

static bool InsertAndCommit(TxnManager* txn, Table* table, Row* row)
{
    RC rc = txn->InsertRow(row);
    if (rc != RC_OK) {
        table->DestroyRow(row);
        txn->Rollback();
        return false;
    }
    rc = txn->Commit();
    return rc == RC_OK;
}

static void PopulateItems(TxnManager* txn, TpccTables& t, FastRandom& rng)
{
    printf("    Populating ITEM (%u rows)...\n", ITEM_COUNT);
    for (uint64_t i = 1; i <= ITEM_COUNT; i++) {
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
        Row* row = t.item->CreateNewRow();
        if (!row) { txn->Rollback(); continue; }

        row->SetValue<uint64_t>(ITEM::I_ID, i);
        row->SetValue<uint64_t>(ITEM::I_IM_ID, rng.NextUniform(1, 10000));

        char name[25]; rng.RandomString(name, 14, 24);
        SetStringValue(row, ITEM::I_NAME, name);
        row->SetValue<double>(ITEM::I_PRICE, (double)rng.NextUniform(100, 10000) / 100.0);

        char data[51]; rng.RandomString(data, 26, 50);
        // 10% of items have "ORIGINAL" in I_DATA (Clause 4.3.3.1)
        if (rng.NextUniform(1, 10) == 1) {
            uint32_t pos = (uint32_t)rng.NextUniform(0, strlen(data) > 8 ? strlen(data) - 8 : 0);
            memcpy(data + pos, "ORIGINAL", 8);
        }
        SetStringValue(row, ITEM::I_DATA, data);

        if (!InsertAndCommit(txn, t.item, row)) {
            fprintf(stderr, "WARN: Failed to insert ITEM %lu\n", (unsigned long)i);
        }
    }
}

static void PopulateWarehouse(TxnManager* txn, TpccTables& t, uint64_t w_id, FastRandom& rng)
{
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    Row* row = t.warehouse->CreateNewRow();
    row->SetValue<uint64_t>(WH::W_ID, w_id);
    char buf[21];
    rng.RandomString(buf, 6, 10); SetStringValue(row, WH::W_NAME, buf);
    rng.RandomString(buf, 10, 20); SetStringValue(row, WH::W_STREET_1, buf);
    rng.RandomString(buf, 10, 20); SetStringValue(row, WH::W_STREET_2, buf);
    rng.RandomString(buf, 10, 20); SetStringValue(row, WH::W_CITY, buf);
    rng.RandomString(buf, 2, 2);   SetStringValue(row, WH::W_STATE, buf);
    rng.RandomNumericString(buf, 9);SetStringValue(row, WH::W_ZIP, buf);
    row->SetValue<double>(WH::W_TAX, (double)rng.NextUniform(0, 2000) / 10000.0);
    row->SetValue<double>(WH::W_YTD, 300000.00);
    InsertAndCommit(txn, t.warehouse, row);
}

static void PopulateDistricts(TxnManager* txn, TpccTables& t, uint64_t w_id, FastRandom& rng)
{
    for (uint64_t d = 1; d <= DIST_PER_WARE; d++) {
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
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
        InsertAndCommit(txn, t.district, row);
    }
}

static void PopulateCustomers(TxnManager* txn, TpccTables& t,
                              uint64_t w_id, uint64_t d_id, FastRandom& rng)
{
    int64_t now = (int64_t)time(nullptr);
    for (uint64_t c = 1; c <= CUST_PER_DIST; c++) {
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
        Row* row = t.customer->CreateNewRow();
        row->SetValue<uint64_t>(CUST::C_ID, c);
        row->SetValue<uint64_t>(CUST::C_D_ID, d_id);
        row->SetValue<uint64_t>(CUST::C_W_ID, w_id);

        char buf[21];
        rng.RandomString(buf, 8, 16); SetStringValue(row, CUST::C_FIRST, buf);
        SetStringValue(row, CUST::C_MIDDLE, "OE");

        // Clause 4.3.2.3: C_LAST for C_ID [1..1000] is deterministic, rest is NURand
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
        InsertAndCommit(txn, t.customer, row);

        // History row (Clause 4.3.3.1)
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
        Row* hrow = t.history->CreateNewRow();
        uint64_t h_surr = (w_id << 40) | (d_id << 20) | c;
        hrow->SetValue<uint64_t>(HIST::H_C_ID, h_surr);
        hrow->SetValue<uint64_t>(HIST::H_C_D_ID, d_id);
        hrow->SetValue<uint64_t>(HIST::H_C_W_ID, w_id);
        hrow->SetValue<uint64_t>(HIST::H_D_ID, d_id);
        hrow->SetValue<uint64_t>(HIST::H_W_ID, w_id);
        hrow->SetValue<int64_t>(HIST::H_DATE, now);
        hrow->SetValue<double>(HIST::H_AMOUNT, 10.00);
        rng.RandomString(buf, 12, 24); SetStringValue(hrow, HIST::H_DATA, buf);
        InsertAndCommit(txn, t.history, hrow);
    }
}

static void PopulateOrders(TxnManager* txn, TpccTables& t,
                           uint64_t w_id, uint64_t d_id, FastRandom& rng)
{
    // Random permutation of customer IDs (Clause 4.3.3.1)
    std::vector<uint64_t> perm(CUST_PER_DIST);
    std::iota(perm.begin(), perm.end(), 1);
    for (uint32_t i = CUST_PER_DIST - 1; i > 0; i--) {
        uint32_t j = (uint32_t)rng.NextUniform(0, i);
        std::swap(perm[i], perm[j]);
    }

    int64_t now = (int64_t)time(nullptr);
    for (uint64_t o = 1; o <= ORD_PER_DIST; o++) {
        uint64_t c_id = perm[o - 1];
        uint64_t ol_cnt = rng.NextUniform(MIN_OL_CNT, MAX_OL_CNT);

        // ORDER row
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
        Row* orow = t.order_tbl->CreateNewRow();
        orow->SetValue<uint64_t>(ORD::O_ID, o);
        orow->SetValue<uint64_t>(ORD::O_C_ID, c_id);
        orow->SetValue<uint64_t>(ORD::O_D_ID, d_id);
        orow->SetValue<uint64_t>(ORD::O_W_ID, w_id);
        orow->SetValue<int64_t>(ORD::O_ENTRY_D, now);
        orow->SetValue<uint64_t>(ORD::O_CARRIER_ID, (o < 2101) ? rng.NextUniform(1, 10) : 0);
        orow->SetValue<uint64_t>(ORD::O_OL_CNT, ol_cnt);
        orow->SetValue<uint64_t>(ORD::O_ALL_LOCAL, 1);
        InsertAndCommit(txn, t.order_tbl, orow);

        // NEW-ORDER for last 900 orders
        if (o >= 2101) {
            txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
            Row* norow = t.new_order->CreateNewRow();
            norow->SetValue<uint64_t>(NORD::NO_O_ID, o);
            norow->SetValue<uint64_t>(NORD::NO_D_ID, d_id);
            norow->SetValue<uint64_t>(NORD::NO_W_ID, w_id);
            InsertAndCommit(txn, t.new_order, norow);
        }

        // ORDER-LINE rows
        for (uint64_t ol = 1; ol <= ol_cnt; ol++) {
            txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
            Row* olrow = t.order_line->CreateNewRow();
            olrow->SetValue<uint64_t>(ORDL::OL_O_ID, o);
            olrow->SetValue<uint64_t>(ORDL::OL_D_ID, d_id);
            olrow->SetValue<uint64_t>(ORDL::OL_W_ID, w_id);
            olrow->SetValue<uint64_t>(ORDL::OL_NUMBER, ol);
            olrow->SetValue<uint64_t>(ORDL::OL_I_ID, rng.NextUniform(1, ITEM_COUNT));
            olrow->SetValue<uint64_t>(ORDL::OL_SUPPLY_W_ID, w_id);
            olrow->SetValue<int64_t>(ORDL::OL_DELIVERY_D, (o < 2101) ? now : 0);
            olrow->SetValue<uint64_t>(ORDL::OL_QUANTITY, 5);
            olrow->SetValue<double>(ORDL::OL_AMOUNT,
                (o < 2101) ? 0.0 : (double)rng.NextUniform(1, 999999) / 100.0);
            char dist_info[25]; rng.RandomString(dist_info, 24, 24);
            SetStringValue(olrow, ORDL::OL_DIST_INFO, dist_info);
            InsertAndCommit(txn, t.order_line, olrow);
        }
    }
}

static void PopulateStock(TxnManager* txn, TpccTables& t, uint64_t w_id, FastRandom& rng)
{
    for (uint64_t i = 1; i <= STOCK_PER_WARE; i++) {
        txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
        Row* row = t.stock->CreateNewRow();
        if (!row) { txn->Rollback(); continue; }
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
        InsertAndCommit(txn, t.stock, row);
    }
}

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

    printf("[Population] Done.\n\n");
    return true;
}

}  // namespace oro::tpcc
