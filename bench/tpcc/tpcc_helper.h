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
// Composite key encoding helpers
// All keys use big-endian uint64_t encoding for proper ordering in masstree.
// ======================================================================

// Key field size for LONG columns (matches ColumnLONG::SetKeySize = m_size + 1 = 9)
static constexpr uint16_t K = MOT_LONG_KEY_SIZE;  // 9

// ALIGN8: MasstreePrimaryIndex::Search passes ALIGN8(keyLen) to iteratorScan,
// but insert uses raw keyLen. Key lengths MUST be multiples of 8 for consistency.
#define ORO_ALIGN8(x) (((x) + 7u) & ~7u)

// Warehouse key: (W_ID) — ALIGN8(9) = 16
static constexpr uint16_t WH_KEY_LEN = ORO_ALIGN8(1 * K);
inline void EncodeWhKey(MOT::Key* key, uint64_t w_id)
{
    KeyFillU64(key, 0, w_id);
}

// District key: (D_W_ID, D_ID) — 18 bytes
static constexpr uint16_t DIST_KEY_LEN = ORO_ALIGN8(2 * K);
inline void EncodeDistKey(MOT::Key* key, uint64_t w_id, uint64_t d_id)
{
    KeyFillU64(key, 0, w_id);
    KeyFillU64(key, K, d_id);
}

// Customer primary key: (C_W_ID, C_D_ID, C_ID) — 27 bytes
static constexpr uint16_t CUST_KEY_LEN = ORO_ALIGN8(3 * K);
inline void EncodeCustKey(MOT::Key* key, uint64_t w_id, uint64_t d_id, uint64_t c_id)
{
    KeyFillU64(key, 0, w_id);
    KeyFillU64(key, K, d_id);
    KeyFillU64(key, 2 * K, c_id);
}

// Customer secondary index by last name: (C_W_ID, C_D_ID, C_LAST[16]) — 34 bytes
static constexpr uint16_t CUST_LAST_KEY_LEN = ORO_ALIGN8(2 * K + 16);
inline void EncodeCustLastKey(MOT::Key* key, uint64_t w_id, uint64_t d_id, const char* c_last)
{
    KeyFillU64(key, 0, w_id);
    KeyFillU64(key, K, d_id);
    uint8_t name_buf[16] = {0};
    size_t len = strlen(c_last);
    if (len > 16) len = 16;
    memcpy(name_buf, c_last, len);
    key->FillValue(name_buf, 16, 2 * K);
}

// NewOrder / Order key: (W_ID, D_ID, O_ID) — 27 bytes
static constexpr uint16_t ORDER_KEY_LEN = ORO_ALIGN8(3 * K);
inline void EncodeOrderKey(MOT::Key* key, uint64_t w_id, uint64_t d_id, uint64_t o_id)
{
    KeyFillU64(key, 0, w_id);
    KeyFillU64(key, K, d_id);
    KeyFillU64(key, 2 * K, o_id);
}

// OrderLine key: (W_ID, D_ID, O_ID, OL_NUMBER) — 36 bytes
static constexpr uint16_t OL_KEY_LEN = ORO_ALIGN8(4 * K);
inline void EncodeOlKey(MOT::Key* key, uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t ol_num)
{
    KeyFillU64(key, 0, w_id);
    KeyFillU64(key, K, d_id);
    KeyFillU64(key, 2 * K, o_id);
    KeyFillU64(key, 3 * K, ol_num);
}

// Stock key: (S_W_ID, S_I_ID) — 18 bytes
static constexpr uint16_t STOCK_KEY_LEN = ORO_ALIGN8(2 * K);
inline void EncodeStockKey(MOT::Key* key, uint64_t w_id, uint64_t i_id)
{
    KeyFillU64(key, 0, w_id);
    KeyFillU64(key, K, i_id);
}

// Item key: (I_ID) — 9 bytes
static constexpr uint16_t ITEM_KEY_LEN = ORO_ALIGN8(1 * K);
inline void EncodeItemKey(MOT::Key* key, uint64_t i_id)
{
    KeyFillU64(key, 0, i_id);
}

// History key: surrogate — 9 bytes
static constexpr uint16_t HIST_KEY_LEN = ORO_ALIGN8(1 * K);

}  // namespace oro::tpcc
#endif  // ORO_TPCC_HELPER_H
