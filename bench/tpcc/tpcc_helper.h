#ifndef ORO_TPCC_HELPER_H
#define ORO_TPCC_HELPER_H

/*
 * TPC-C helper functions: NURand, last name generation, key encoding.
 * Per TPC-C Standard Specification Revision 5.11, Clause 2.1.6 and Clause 4.3.2.3
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include "bench_util.h"
#include "row.h"

namespace oro::tpcc {

// ======================================================================
// NURand (Non-Uniform Random) — Clause 2.1.6
// NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
// ======================================================================
inline uint64_t NURand(FastRandom& rng, uint64_t A, uint64_t x, uint64_t y)
{
    // C values are chosen once and held constant (simplified: use a fixed C per run)
    static constexpr uint64_t C_LOAD = 0;  // for initial population
    return (((rng.NextUniform(0, A) | rng.NextUniform(x, y)) + C_LOAD) % (y - x + 1)) + x;
}

// ======================================================================
// Customer last name generation — Clause 4.3.2.3
// Maps 0..999 to syllable combinations
// ======================================================================
static const char* const LAST_NAME_SYLLABLES[] = {
    "BAR", "OUGHT", "ABLE", "PRI", "PRES",
    "ESE", "ANTI", "CALLY", "ATION", "EING"
};

inline void GenLastName(uint32_t num, char* buf, uint32_t bufSize)
{
    // num must be in [0, 999]
    if (num > 999) num = num % 1000;
    uint32_t s1 = num / 100;
    uint32_t s2 = (num / 10) % 10;
    uint32_t s3 = num % 10;
    snprintf(buf, bufSize, "%s%s%s",
             LAST_NAME_SYLLABLES[s1],
             LAST_NAME_SYLLABLES[s2],
             LAST_NAME_SYLLABLES[s3]);
}

// ======================================================================
// Packed composite key encoding
//
// In standalone mode, MOT uses the InternalKey path in BuildKey:
//   row->SetInternalKey(lastCol, value) → BuildKey does CpKey(raw 8 bytes)
//
// All composite keys are packed into a single uint64. The packing must
// preserve ordering so that range scans work correctly in MassTree.
// Since all TPC-C IDs are small positive integers, simple shift+or works.
//
// Key length is always sizeof(uint64_t) = 8 bytes.
// ======================================================================

static constexpr uint32_t PACKED_KEY_LEN = sizeof(uint64_t);  // 8

// Warehouse: W_ID (max ~64K warehouses, plenty of room)
inline uint64_t PackWhKey(uint64_t w_id) { return w_id; }

// District: (W_ID, D_ID) — D_ID max 10
inline uint64_t PackDistKey(uint64_t w_id, uint64_t d_id) {
    return (w_id << 32) | d_id;
}

// Customer: (W_ID, D_ID, C_ID) — C_ID max 3000
inline uint64_t PackCustKey(uint64_t w_id, uint64_t d_id, uint64_t c_id) {
    return (w_id << 40) | (d_id << 20) | c_id;
}

// NewOrder / Order: (W_ID, D_ID, O_ID)
inline uint64_t PackOrderKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
    return (w_id << 40) | (d_id << 20) | o_id;
}

// OrderLine: (W_ID, D_ID, O_ID, OL_NUMBER) — OL_NUMBER max 15
inline uint64_t PackOlKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t ol_num) {
    return (w_id << 48) | (d_id << 32) | (o_id << 16) | ol_num;
}

// Stock: (W_ID, I_ID) — I_ID max 100000
inline uint64_t PackStockKey(uint64_t w_id, uint64_t i_id) {
    return (w_id << 32) | i_id;
}

// Item: I_ID
inline uint64_t PackItemKey(uint64_t i_id) { return i_id; }

// History: surrogate (auto-increment)
static std::atomic<uint64_t> g_history_surr_key{1ULL << 48};
inline uint64_t NextHistoryKey() {
    return g_history_surr_key.fetch_add(1, std::memory_order_relaxed);
}

// ======================================================================
// BuildSearchKey helper — creates a key with the raw packed uint64 value.
//
// In the InternalKey path, BuildKey does CpKey with the raw column bytes.
// So the key is just the native-endian uint64 value, 8 bytes.
// We fill it directly without creating a temp row.
// Caller must destroy the returned key via ix->DestroyKey().
// ======================================================================
inline MOT::Key* BuildSearchKey(MOT::TxnManager* txn, MOT::Index* ix, uint64_t packed_key)
{
    MOT::Key* key = txn->GetTxnKey(ix);
    if (!key) return nullptr;
    key->FillValue(reinterpret_cast<const uint8_t*>(&packed_key), sizeof(uint64_t), 0);
    return key;
}

// ======================================================================
// Per-table row debug printers
//
// Each function prints all data columns of a row to stderr.
// String columns are read via the pointer GetValue and printed with
// a max-length to avoid garbage past the null terminator.
// ======================================================================

inline void PrintWarehouseRow(MOT::Row* r)
{
    uint64_t id; r->GetValue(WH::W_ID, id);
    double tax, ytd;
    r->GetValue(WH::W_TAX, tax);
    r->GetValue(WH::W_YTD, ytd);
    fprintf(stderr, "WAREHOUSE  W_ID=%lu NAME=%.10s STREET1=%.20s STREET2=%.20s "
            "CITY=%.20s STATE=%.2s ZIP=%.9s TAX=%.4f YTD=%.2f\n",
            (unsigned long)id,
            (const char*)r->GetValue(WH::W_NAME),
            (const char*)r->GetValue(WH::W_STREET_1),
            (const char*)r->GetValue(WH::W_STREET_2),
            (const char*)r->GetValue(WH::W_CITY),
            (const char*)r->GetValue(WH::W_STATE),
            (const char*)r->GetValue(WH::W_ZIP),
            tax, ytd);
}

inline void PrintDistrictRow(MOT::Row* r)
{
    uint64_t id, wid, next_oid;
    double tax, ytd;
    r->GetValue(DIST::D_ID, id);
    r->GetValue(DIST::D_W_ID, wid);
    r->GetValue(DIST::D_TAX, tax);
    r->GetValue(DIST::D_YTD, ytd);
    r->GetValue(DIST::D_NEXT_O_ID, next_oid);
    fprintf(stderr, "DISTRICT   D_ID=%lu D_W_ID=%lu NAME=%.10s STREET1=%.20s STREET2=%.20s "
            "CITY=%.20s STATE=%.2s ZIP=%.9s TAX=%.4f YTD=%.2f NEXT_O_ID=%lu\n",
            (unsigned long)id, (unsigned long)wid,
            (const char*)r->GetValue(DIST::D_NAME),
            (const char*)r->GetValue(DIST::D_STREET_1),
            (const char*)r->GetValue(DIST::D_STREET_2),
            (const char*)r->GetValue(DIST::D_CITY),
            (const char*)r->GetValue(DIST::D_STATE),
            (const char*)r->GetValue(DIST::D_ZIP),
            tax, ytd, (unsigned long)next_oid);
}

inline void PrintCustomerRow(MOT::Row* r)
{
    uint64_t cid, did, wid, since, pay_cnt, del_cnt;
    double cred_lim, disc, bal, ytd_pay;
    r->GetValue(CUST::C_ID, cid);
    r->GetValue(CUST::C_D_ID, did);
    r->GetValue(CUST::C_W_ID, wid);
    r->GetValue(CUST::C_SINCE, since);
    r->GetValue(CUST::C_CREDIT_LIM, cred_lim);
    r->GetValue(CUST::C_DISCOUNT, disc);
    r->GetValue(CUST::C_BALANCE, bal);
    r->GetValue(CUST::C_YTD_PAYMENT, ytd_pay);
    r->GetValue(CUST::C_PAYMENT_CNT, pay_cnt);
    r->GetValue(CUST::C_DELIVERY_CNT, del_cnt);
    fprintf(stderr, "CUSTOMER   C_ID=%lu C_D_ID=%lu C_W_ID=%lu FIRST=%.16s MIDDLE=%.2s LAST=%.16s "
            "CREDIT=%.2s CRED_LIM=%.2f DISC=%.4f BAL=%.2f YTD_PAY=%.2f PAY_CNT=%lu DEL_CNT=%lu\n",
            (unsigned long)cid, (unsigned long)did, (unsigned long)wid,
            (const char*)r->GetValue(CUST::C_FIRST),
            (const char*)r->GetValue(CUST::C_MIDDLE),
            (const char*)r->GetValue(CUST::C_LAST),
            (const char*)r->GetValue(CUST::C_CREDIT),
            cred_lim, disc, bal, ytd_pay,
            (unsigned long)pay_cnt, (unsigned long)del_cnt);
}

inline void PrintHistoryRow(MOT::Row* r)
{
    uint64_t cid, cdid, cwid, did, wid;
    int64_t date;
    double amount;
    r->GetValue(HIST::H_C_ID, cid);
    r->GetValue(HIST::H_C_D_ID, cdid);
    r->GetValue(HIST::H_C_W_ID, cwid);
    r->GetValue(HIST::H_D_ID, did);
    r->GetValue(HIST::H_W_ID, wid);
    r->GetValue(HIST::H_DATE, date);
    r->GetValue(HIST::H_AMOUNT, amount);
    fprintf(stderr, "HISTORY    H_C_ID=%lu H_C_D_ID=%lu H_C_W_ID=%lu H_D_ID=%lu H_W_ID=%lu "
            "DATE=%ld AMOUNT=%.2f DATA=%.24s\n",
            (unsigned long)cid, (unsigned long)cdid, (unsigned long)cwid,
            (unsigned long)did, (unsigned long)wid,
            (long)date, amount,
            (const char*)r->GetValue(HIST::H_DATA));
}

inline void PrintNewOrderRow(MOT::Row* r)
{
    uint64_t oid, did, wid;
    r->GetValue(NORD::NO_O_ID, oid);
    r->GetValue(NORD::NO_D_ID, did);
    r->GetValue(NORD::NO_W_ID, wid);
    fprintf(stderr, "NEW_ORDER  NO_O_ID=%lu NO_D_ID=%lu NO_W_ID=%lu\n",
            (unsigned long)oid, (unsigned long)did, (unsigned long)wid);
}

inline void PrintOrderRow(MOT::Row* r)
{
    uint64_t oid, did, wid, cid, carrier, ol_cnt, all_local;
    int64_t entry_d;
    r->GetValue(ORD::O_ID, oid);
    r->GetValue(ORD::O_D_ID, did);
    r->GetValue(ORD::O_W_ID, wid);
    r->GetValue(ORD::O_C_ID, cid);
    r->GetValue(ORD::O_ENTRY_D, entry_d);
    r->GetValue(ORD::O_CARRIER_ID, carrier);
    r->GetValue(ORD::O_OL_CNT, ol_cnt);
    r->GetValue(ORD::O_ALL_LOCAL, all_local);
    fprintf(stderr, "ORDER      O_ID=%lu O_D_ID=%lu O_W_ID=%lu O_C_ID=%lu "
            "ENTRY_D=%ld CARRIER=%lu OL_CNT=%lu ALL_LOCAL=%lu\n",
            (unsigned long)oid, (unsigned long)did, (unsigned long)wid,
            (unsigned long)cid, (long)entry_d, (unsigned long)carrier,
            (unsigned long)ol_cnt, (unsigned long)all_local);
}

inline void PrintOrderLineRow(MOT::Row* r)
{
    uint64_t oid, did, wid, num, iid, supply_wid, qty;
    int64_t del_d;
    double amount;
    r->GetValue(ORDL::OL_O_ID, oid);
    r->GetValue(ORDL::OL_D_ID, did);
    r->GetValue(ORDL::OL_W_ID, wid);
    r->GetValue(ORDL::OL_NUMBER, num);
    r->GetValue(ORDL::OL_I_ID, iid);
    r->GetValue(ORDL::OL_SUPPLY_W_ID, supply_wid);
    r->GetValue(ORDL::OL_DELIVERY_D, del_d);
    r->GetValue(ORDL::OL_QUANTITY, qty);
    r->GetValue(ORDL::OL_AMOUNT, amount);
    fprintf(stderr, "ORDER_LINE OL_O_ID=%lu OL_D_ID=%lu OL_W_ID=%lu OL_NUMBER=%lu "
            "OL_I_ID=%lu SUPPLY_W_ID=%lu DEL_D=%ld QTY=%lu AMOUNT=%.2f DIST=%.24s\n",
            (unsigned long)oid, (unsigned long)did, (unsigned long)wid,
            (unsigned long)num, (unsigned long)iid, (unsigned long)supply_wid,
            (long)del_d, (unsigned long)qty, amount,
            (const char*)r->GetValue(ORDL::OL_DIST_INFO));
}

inline void PrintItemRow(MOT::Row* r)
{
    uint64_t id, im_id;
    double price;
    r->GetValue(ITEM::I_ID, id);
    r->GetValue(ITEM::I_IM_ID, im_id);
    r->GetValue(ITEM::I_PRICE, price);
    fprintf(stderr, "ITEM       I_ID=%lu I_IM_ID=%lu NAME=%.24s PRICE=%.2f DATA=%.50s\n",
            (unsigned long)id, (unsigned long)im_id,
            (const char*)r->GetValue(ITEM::I_NAME),
            price,
            (const char*)r->GetValue(ITEM::I_DATA));
}

inline void PrintStockRow(MOT::Row* r)
{
    uint64_t iid, wid, qty, ytd, ord_cnt, rem_cnt;
    r->GetValue(STK::S_I_ID, iid);
    r->GetValue(STK::S_W_ID, wid);
    r->GetValue(STK::S_QUANTITY, qty);
    r->GetValue(STK::S_YTD, ytd);
    r->GetValue(STK::S_ORDER_CNT, ord_cnt);
    r->GetValue(STK::S_REMOTE_CNT, rem_cnt);
    fprintf(stderr, "STOCK      S_I_ID=%lu S_W_ID=%lu QTY=%lu YTD=%lu ORD_CNT=%lu REM_CNT=%lu DATA=%.50s\n",
            (unsigned long)iid, (unsigned long)wid,
            (unsigned long)qty, (unsigned long)ytd,
            (unsigned long)ord_cnt, (unsigned long)rem_cnt,
            (const char*)r->GetValue(STK::S_DATA));
}

}  // namespace oro::tpcc
#endif  // ORO_TPCC_HELPER_H
