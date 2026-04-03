#ifndef ORO_YCSB_CONFIG_H
#define ORO_YCSB_CONFIG_H

#include <cstdint>

namespace oro::ycsb {

// YCSB table schema: single table with a key + N value fields
// Per Cooper et al., "Benchmarking Cloud Serving Systems with YCSB" (SoCC 2010)

static constexpr uint32_t DEFAULT_FIELD_COUNT  = 10;
static constexpr uint32_t DEFAULT_FIELD_LENGTH = 100;  // bytes per field
static constexpr uint32_t MAX_FIELD_COUNT      = 20;

// Column layout: col 0 = YCSB_KEY (uint64), cols 1..N = FIELD_0..FIELD_(N-1)
enum Col : int {
    YCSB_KEY = 0,
    FIELD_0  = 1  // FIELD_i = FIELD_0 + i
};

// Key: single uint64, 9 bytes per ColumnLONG::PackKey (1 sign + 8 BE)
// ALIGN8(9) = 16 to match Search's ALIGN8(keyLen) vs insert's raw keyLen
static constexpr uint16_t YCSB_KEY_LEN = 16;

}  // namespace oro::ycsb
#endif  // ORO_YCSB_CONFIG_H
