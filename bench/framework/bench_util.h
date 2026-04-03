#ifndef ORO_BENCH_UTIL_H
#define ORO_BENCH_UTIL_H

#include <cstdint>
#include <cstring>
#include <random>

namespace oro {

// Big-endian encoding for index key ordering (MOT masstree compares keys as byte strings)
inline void EncodeU64BE(uint8_t* dst, uint64_t val)
{
    dst[0] = (uint8_t)(val >> 56);
    dst[1] = (uint8_t)(val >> 48);
    dst[2] = (uint8_t)(val >> 40);
    dst[3] = (uint8_t)(val >> 32);
    dst[4] = (uint8_t)(val >> 24);
    dst[5] = (uint8_t)(val >> 16);
    dst[6] = (uint8_t)(val >> 8);
    dst[7] = (uint8_t)(val);
}

inline uint64_t DecodeU64BE(const uint8_t* src)
{
    return ((uint64_t)src[0] << 56) | ((uint64_t)src[1] << 48) |
           ((uint64_t)src[2] << 40) | ((uint64_t)src[3] << 32) |
           ((uint64_t)src[4] << 24) | ((uint64_t)src[5] << 16) |
           ((uint64_t)src[6] << 8)  | ((uint64_t)src[7]);
}

// Fill a Key object with a single uint64 at a given offset (big-endian)
inline void KeyFillU64(MOT::Key* key, uint16_t offset, uint64_t val)
{
    uint8_t buf[8];
    EncodeU64BE(buf, val);
    key->FillValue(buf, 8, offset);
}

// Thread-local fast RNG
class FastRandom {
public:
    explicit FastRandom(uint64_t seed) : rng_(seed) {}

    uint64_t Next()
    {
        return rng_();
    }

    // [low, high] inclusive
    uint64_t NextUniform(uint64_t low, uint64_t high)
    {
        std::uniform_int_distribution<uint64_t> dist(low, high);
        return dist(rng_);
    }

    double NextDouble()
    {
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(rng_);
    }

    // Generate random alphanumeric string of length [minLen, maxLen]
    void RandomString(char* buf, uint32_t minLen, uint32_t maxLen)
    {
        static const char charset[] =
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "0123456789";
        uint32_t len = (minLen == maxLen) ? minLen : (uint32_t)NextUniform(minLen, maxLen);
        for (uint32_t i = 0; i < len; i++) {
            buf[i] = charset[NextUniform(0, sizeof(charset) - 2)];
        }
        buf[len] = '\0';
    }

    // Generate random numeric string (digits only)
    void RandomNumericString(char* buf, uint32_t len)
    {
        for (uint32_t i = 0; i < len; i++) {
            buf[i] = '0' + (char)NextUniform(0, 9);
        }
        buf[len] = '\0';
    }

private:
    std::mt19937_64 rng_;
};

}  // namespace oro
#endif  // ORO_BENCH_UTIL_H
