#ifndef ORO_TPCC_HELPER_H
#define ORO_TPCC_HELPER_H

/*
 * TPC-C helper functions: NURand, last name generation, key encoding.
 * Per TPC-C Standard Specification Revision 5.11, Clause 2.1.6 and Clause 4.3.2.3
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <utility>
#include "bench_util.h"
#include "row.h"
#include "index.h"
#include "index_iterator.h"
#include "sentinel.h"
#include "txn.h"
#include "mm_gc_manager.h"

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
// All composite keys are packed into a single uint64. The packing puts
// the most significant field (W_ID) in the high bits. Pack functions
// return big-endian so that MassTree's byte-string comparison preserves
// the correct field ordering for range scans and prefix scans.
//
// Key length is always sizeof(uint64_t) = 8 bytes.
// ======================================================================

static constexpr uint32_t PACKED_KEY_LEN = sizeof(uint64_t);  // 8

// Convert a uint64 to big-endian representation stored in a uint64.
// MassTree compares keys as byte strings (via htonq), so BE encoding
// makes numerical order match iteration order.
inline uint64_t ToBE(uint64_t val)
{
    uint8_t buf[8];
    EncodeU64BE(buf, val);
    uint64_t result;
    memcpy(&result, buf, 8);
    return result;
}

// Warehouse: W_ID (max ~64K warehouses, plenty of room)
inline uint64_t PackWhKey(uint64_t w_id) { return ToBE(w_id); }

// District: (W_ID, D_ID) — D_ID max 10
static constexpr uint32_t DIST_SUFFIX_BITS = 32;   // D_ID occupies low 32 bits
inline uint64_t PackDistKey(uint64_t w_id, uint64_t d_id) {
    return ToBE((w_id << 32) | d_id);
}

// Customer: (W_ID, D_ID, C_ID) — C_ID max 3000
static constexpr uint32_t CUST_SUFFIX_BITS = 20;   // C_ID occupies low 20 bits
inline uint64_t PackCustKey(uint64_t w_id, uint64_t d_id, uint64_t c_id) {
    return ToBE((w_id << 40) | (d_id << 20) | c_id);
}

// NewOrder / Order: (W_ID, D_ID, O_ID)
static constexpr uint32_t ORDER_SUFFIX_BITS = 20;  // O_ID occupies low 20 bits
inline uint64_t PackOrderKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
    return ToBE((w_id << 40) | (d_id << 20) | o_id);
}

// OrderLine: (W_ID, D_ID, O_ID, OL_NUMBER) — OL_NUMBER max 15
static constexpr uint32_t OL_SUFFIX_BITS = 16;     // OL_NUMBER occupies low 16 bits
inline uint64_t PackOlKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t ol_num) {
    return ToBE((w_id << 48) | (d_id << 32) | (o_id << 16) | ol_num);
}

// Stock: (W_ID, I_ID) — I_ID max 100000
static constexpr uint32_t STOCK_SUFFIX_BITS = 32;  // I_ID occupies low 32 bits
inline uint64_t PackStockKey(uint64_t w_id, uint64_t i_id) {
    return ToBE((w_id << 32) | i_id);
}

// Item: I_ID
inline uint64_t PackItemKey(uint64_t i_id) { return ToBE(i_id); }

// Prefix range helper — builds start/end packed keys for range scans.
// Pack the prefix fields with 0 in the suffix, pass the number of low bits
// occupied by the suffix fields.  Returns {start, end} with suffix padded
// to min (0) and max (all-1s).
// The mask is BE-encoded to match the Pack* output.
//
// Examples:
//   OrderLine (w,d,o prefix):
//     auto [lo, hi] = PrefixKeyRange(PackOlKey(w,d,o_lo,0), PackOlKey(w,d,o_hi,0), OL_SUFFIX_BITS);
//   NewOrder/Order (w,d prefix):
//     auto [lo, hi] = PrefixKeyRange(PackOrderKey(w,d,0), PackOrderKey(w,d,0), ORDER_SUFFIX_BITS);
inline std::pair<uint64_t, uint64_t> PrefixKeyRange(
    uint64_t lo_packed, uint64_t hi_packed, uint32_t suffix_bits)
{
    uint64_t mask = ToBE((1ULL << suffix_bits) - 1);
    return { lo_packed & ~mask, hi_packed | mask };
}

// History: surrogate (auto-increment)
static std::atomic<uint64_t> g_history_surr_key{1ULL << 48};
inline uint64_t NextHistoryKey() {
    return ToBE(g_history_surr_key.fetch_add(1, std::memory_order_relaxed));
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
// Index cursor helpers
//
// Follow the MOT IndexIterator cursor pattern (same as openGauss FDW):
//   Open:    ix->Begin() / ix->Search() / ix->LowerBound()
//   Iterate: it->IsValid() + it->Next()
//   Close:   it->Destroy()
// ======================================================================

// SELECT COUNT(*) — count all rows in an index (no transaction needed)
inline uint64_t CountRows(MOT::Index* ix, uint32_t pid = 0)
{
    uint64_t count = 0;
    MOT::IndexIterator* it = ix->Begin(pid);
    while (it != nullptr && it->IsValid()) {
        count++;
        it->Next();
    }
    if (it) it->Destroy();
    return count;
}

// Scan by key column value — full index scan with filtering on the packed
// _KEY column.  Keys are stored as big-endian uint64 values, so we use
// memcmp for range comparison (memcmp preserves BE ordering).
inline MOT::RC ScanByKeyColumn(MOT::TxnManager* txn, MOT::Index* ix,
                               int key_col, uint64_t start_key, uint64_t end_key,
                               const std::function<bool(MOT::Row*)>& cb, uint32_t pid = 0)
{
    MOT::RC rc = MOT::RC_OK;
    MOT::IndexIterator* it = ix->Begin(pid);

    while (it != nullptr && it->IsValid()) {
        MOT::Sentinel* sentinel = it->GetPrimarySentinel();
        MOT::Row* row = txn->RowLookup(MOT::AccessType::RD, sentinel, rc);
        if (rc != MOT::RC_OK) break;
        if (row) {
            uint64_t key_val;
            row->GetValue(key_col, key_val);
            if (memcmp(&key_val, &start_key, 8) >= 0 && memcmp(&key_val, &end_key, 8) <= 0) {
                if (!cb(row)) break;
            }
        }
        it->Next();
    }

    if (it) it->Destroy();
    return rc;
}

// Range scan [start_key, end_key] inclusive — cursor walks from start to end.
// Callback returns true to continue, false to stop early.
inline MOT::RC ScanRange(MOT::TxnManager* txn, MOT::Table* table, MOT::Index* ix,
                         MOT::AccessType atype, uint64_t start_key, uint64_t end_key,
                         const std::function<bool(MOT::Row*)>& cb, uint32_t pid = 0)
{
    MOT::RC rc = MOT::RC_OK;

    MOT::Key* lo = ix->CreateNewSearchKey();
    MOT::Key* hi = ix->CreateNewSearchKey();
    if (!lo || !hi) {
        if (lo) ix->DestroyKey(lo);
        if (hi) ix->DestroyKey(hi);
        return MOT::RC_MEMORY_ALLOCATION_ERROR;
    }
    lo->FillValue(reinterpret_cast<const uint8_t*>(&start_key), 8, 0);
    hi->FillValue(reinterpret_cast<const uint8_t*>(&end_key), 8, 0);

    bool found = false;
    MOT::IndexIterator* it = ix->Search(lo, true, true, pid, found);

    while (it != nullptr && it->IsValid()) {
        // Compare current key against end bound
        MOT::Key* cur = (MOT::Key*)it->GetKey();
        if (cur && memcmp(cur->GetKeyBuf(), hi->GetKeyBuf(), hi->GetKeyLength()) > 0)
            break;

        MOT::Sentinel* sentinel = it->GetPrimarySentinel();
        MOT::Row* row = txn->RowLookup(atype, sentinel, rc);
        if (rc != MOT::RC_OK) break;
        if (row && !cb(row)) break;

        it->Next();
    }

    if (it) it->Destroy();
    ix->DestroyKey(lo);
    ix->DestroyKey(hi);
    return rc;
}

// Prefix scan — iterate while the first prefix_len bytes of the key match.
// Useful for secondary non-unique index style lookups.
// Callback returns true to continue, false to stop early.
inline MOT::RC ScanPrefix(MOT::TxnManager* txn, MOT::Table* table, MOT::Index* ix,
                          MOT::AccessType atype, uint64_t prefix_key, uint16_t prefix_len,
                          const std::function<bool(MOT::Row*)>& cb, uint32_t pid = 0)
{
    MOT::RC rc = MOT::RC_OK;

    MOT::Key* key = ix->CreateNewSearchKey();
    if (!key) return MOT::RC_MEMORY_ALLOCATION_ERROR;
    key->FillValue(reinterpret_cast<const uint8_t*>(&prefix_key), 8, 0);

    bool found = false;
    MOT::IndexIterator* it = ix->Search(key, true, true, pid, found);

    while (it != nullptr && it->IsValid()) {
        MOT::Key* cur = (MOT::Key*)it->GetKey();
        if (!cur || memcmp(cur->GetKeyBuf(), key->GetKeyBuf(), prefix_len) != 0)
            break;

        MOT::Sentinel* sentinel = it->GetPrimarySentinel();
        MOT::Row* row = txn->RowLookup(atype, sentinel, rc);
        if (rc != MOT::RC_OK) break;
        if (row && !cb(row)) break;

        it->Next();
    }

    if (it) it->Destroy();
    ix->DestroyKey(key);
    return rc;
}

// ======================================================================
// Secondary index helpers for customer by-last-name lookup
//
// Key layout for ix_customer_last (non-unique):
//   [C_W_ID:8 BE][C_D_ID:8 BE][C_LAST:17 raw][C_FIRST:17 raw][rowId:8]
//   User portion = 50 bytes, suffix = 8 bytes, total = 58 bytes.
// ======================================================================

static constexpr uint16_t CUST_LAST_USER_KEY_LEN = 8 + 8 + 17 + 17;  // 50
static constexpr uint16_t CUST_LAST_PREFIX_LEN   = 8 + 8 + 17;       // 33

// Build a search key for ix_customer_last.
// c_first may be nullptr (for prefix-only searches).
// Caller must destroy via ix->DestroyKey().
inline MOT::Key* BuildCustLastSearchKey(MOT::Index* ix,
                                        uint64_t w_id, uint64_t d_id,
                                        const char* c_last, const char* c_first = nullptr)
{
    MOT::Key* key = ix->CreateNewSearchKey();
    if (!key) return nullptr;

    // Zero entire buffer (including suffix) so prefix scans start at first match
    key->FillPattern(0x00, key->GetKeyLength(), 0);

    uint8_t buf8[8];
    EncodeU64BE(buf8, w_id);
    key->FillValue(buf8, 8, 0);
    EncodeU64BE(buf8, d_id);
    key->FillValue(buf8, 8, 8);
    if (c_last) {
        uint16_t len = (uint16_t)strnlen(c_last, 17);
        key->FillValue(reinterpret_cast<const uint8_t*>(c_last), len, 16);
    }
    if (c_first) {
        uint16_t len = (uint16_t)strnlen(c_first, 17);
        key->FillValue(reinterpret_cast<const uint8_t*>(c_first), len, 33);
    }
    return key;
}

// Populate the manually-managed secondary index ix_customer_last by iterating
// the primary index.  Must be called after all customers are loaded.
inline void PopulateCustLastIndex(MOT::Index* ix_sec, MOT::Index* ix_pri,
                                  MOT::Table* table, uint32_t pid = 0)
{
    uint64_t count = 0;
    MOT::IndexIterator* it = ix_pri->Begin(pid);
    while (it != nullptr && it->IsValid()) {
        MOT::Sentinel* ps = it->GetPrimarySentinel();
        MOT::Row* row = ps ? ps->GetData() : nullptr;
        if (!row) { it->Next(); continue; }

        // Extract fields
        uint64_t w_id, d_id;
        row->GetValue(CUST::C_W_ID, w_id);
        row->GetValue(CUST::C_D_ID, d_id);
        const char* c_last  = reinterpret_cast<const char*>(row->GetValue(CUST::C_LAST));
        const char* c_first = reinterpret_cast<const char*>(row->GetValue(CUST::C_FIRST));

        // Build secondary key (50 user bytes + 8 rowId suffix = 58 total)
        MOT::Key* skey = ix_sec->CreateNewSearchKey();
        if (!skey) { fprintf(stderr, "ERROR: failed to alloc secondary key\n"); break; }
        skey->FillPattern(0x00, skey->GetKeyLength(), 0);

        uint8_t buf8[8];
        EncodeU64BE(buf8, w_id);
        skey->FillValue(buf8, 8, 0);
        EncodeU64BE(buf8, d_id);
        skey->FillValue(buf8, 8, 8);
        uint16_t lastLen = (uint16_t)strnlen(c_last, 17);
        skey->FillValue(reinterpret_cast<const uint8_t*>(c_last), lastLen, 16);
        uint16_t firstLen = (uint16_t)strnlen(c_first, 17);
        skey->FillValue(reinterpret_cast<const uint8_t*>(c_first), firstLen, 33);

        // Non-unique suffix: rowId in native endian at offset 50
        uint64_t rowId = row->GetRowId();
        skey->FillValue(reinterpret_cast<const uint8_t*>(&rowId),
                        NON_UNIQUE_INDEX_SUFFIX_LEN,
                        CUST_LAST_USER_KEY_LEN);

        MOT::Sentinel* res = ix_sec->IndexInsert(skey, row, pid);
        if (!res) {
            fprintf(stderr, "WARN: secondary index insert failed for customer row %lu\n",
                    (unsigned long)rowId);
        }
        ix_sec->DestroyKey(skey);
        count++;
        it->Next();
    }
    if (it) it->Destroy();
    fprintf(stderr, "  ix_customer_last: populated %lu entries\n", (unsigned long)count);
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

// ======================================================================
// Memory debug helpers (ORO_MEMORY_DEBUG only)
// ======================================================================
#ifdef ORO_MEMORY_DEBUG
inline void PrintTableMemoryStats(MOT::Table* t, const char* name)
{
    fprintf(stderr, "MEMSTATS %-14s  rows: alloc=%lu free=%lu net=%ld  drafts: alloc=%lu free=%lu net=%ld\n",
            name,
            (unsigned long)t->m_dbgRowAllocCount.load(),
            (unsigned long)t->m_dbgRowFreeCount.load(),
            (long)(t->m_dbgRowAllocCount.load() - t->m_dbgRowFreeCount.load()),
            (unsigned long)t->m_dbgDraftAllocCount.load(),
            (unsigned long)t->m_dbgDraftFreeCount.load(),
            (long)(t->m_dbgDraftAllocCount.load() - t->m_dbgDraftFreeCount.load()));
}

inline void PrintGcStats(MOT::TxnManager* txn)
{
    MOT::GcManager* gc = txn->GetGcSession();
    if (!gc) { fprintf(stderr, "GC: no session\n"); return; }
    fprintf(stderr, "GC: limbo_inuse=%lu reclaimed=%lu_bytes retired=%lu_bytes "
            "group_allocs=%u free_allocs=%u\n",
            (unsigned long)gc->GetTotalLimboInuseElements(),
            (unsigned long)gc->GetTotalLimboReclaimedSizeInBytes(),
            (unsigned long)gc->GetTotalLimboRetiredSizeInBytes(),
            (unsigned)gc->GetLimboGroupAllocations(),
            (unsigned)gc->GetFreeAllocations());
}
#endif

}  // namespace oro::tpcc
#endif  // ORO_TPCC_HELPER_H
