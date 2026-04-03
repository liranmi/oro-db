#ifndef ORO_TPCC_HELPER_H
#define ORO_TPCC_HELPER_H

/*
 * TPC-C helper functions: NURand, last name generation, key encoding.
 * Per TPC-C Standard Specification Revision 5.11, Clause 2.1.6 and Clause 4.3.2.3
 */

#include <cstdint>
#include <cstring>
#include "bench_util.h"

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
// BuildSearchKey helper — matches the test_index.cpp pattern.
// Creates a temp row, calls SetInternalKey, then BuildKey.
// Caller must destroy the returned key via ix->DestroyKey().
// ======================================================================
inline MOT::Key* BuildSearchKey(MOT::Index* ix, MOT::Table* table, uint64_t packed_key)
{
    MOT::Key* key = ix->CreateNewKey();
    if (!key) return nullptr;
    MOT::Row* tmp = table->CreateNewRow();
    if (!tmp) { ix->DestroyKey(key); return nullptr; }
    int lastCol = table->GetFieldCount() - 1;
    tmp->SetInternalKey(lastCol, packed_key);
    ix->BuildKey(table, tmp, key);
    table->DestroyRow(tmp);
    return key;
}

}  // namespace oro::tpcc
#endif  // ORO_TPCC_HELPER_H
