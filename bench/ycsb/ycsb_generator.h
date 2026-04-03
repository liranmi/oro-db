#ifndef ORO_YCSB_GENERATOR_H
#define ORO_YCSB_GENERATOR_H

/*
 * YCSB key distribution generators.
 * - Uniform: keys uniformly distributed over [0, record_count)
 * - Zipfian: skewed distribution per Gray et al. with configurable theta
 */

#include <cmath>
#include <cstdint>
#include "bench_util.h"

namespace oro::ycsb {

class UniformGenerator {
public:
    explicit UniformGenerator(uint64_t record_count)
        : record_count_(record_count) {}

    uint64_t Next(FastRandom& rng)
    {
        return rng.NextUniform(0, record_count_ - 1);
    }

private:
    uint64_t record_count_;
};

// Scrambled Zipfian generator (avoids head-of-distribution hotspot on key 0)
// Based on YCSB's ZipfianGenerator with FNV hash scrambling.
class ZipfianGenerator {
public:
    ZipfianGenerator(uint64_t record_count, double theta = 0.99)
        : n_(record_count), theta_(theta)
    {
        zetan_ = Zeta(n_, theta_);
        zeta2_ = Zeta(2, theta_);
        alpha_ = 1.0 / (1.0 - theta_);
        eta_ = (1.0 - std::pow(2.0 / (double)n_, 1.0 - theta_)) / (1.0 - zeta2_ / zetan_);
    }

    uint64_t Next(FastRandom& rng)
    {
        double u = rng.NextDouble();
        double uz = u * zetan_;

        uint64_t val;
        if (uz < 1.0)
            val = 0;
        else if (uz < 1.0 + std::pow(0.5, theta_))
            val = 1;
        else
            val = (uint64_t)((double)n_ * std::pow(eta_ * u - eta_ + 1.0, alpha_));

        // Scramble with FNV to spread hotspot across key space
        return FnvHash(val) % n_;
    }

private:
    uint64_t n_;
    double theta_, zetan_, zeta2_, alpha_, eta_;

    static double Zeta(uint64_t n, double theta)
    {
        double sum = 0.0;
        for (uint64_t i = 0; i < n; i++)
            sum += 1.0 / std::pow((double)(i + 1), theta);
        // For large n, approximate after first 10000 terms
        if (n > 10000) {
            // Use integral approximation for tail
            sum = 0.0;
            for (uint64_t i = 0; i < 10000; i++)
                sum += 1.0 / std::pow((double)(i + 1), theta);
            // Euler-Maclaurin approx for remaining terms
            double tail = (std::pow((double)n, 1.0 - theta) - std::pow(10000.0, 1.0 - theta)) /
                          (1.0 - theta);
            sum += tail;
        }
        return sum;
    }

    static uint64_t FnvHash(uint64_t val)
    {
        // FNV-1a 64-bit
        uint64_t hash = 14695981039346656037ULL;
        for (int i = 0; i < 8; i++) {
            hash ^= (val & 0xFF);
            hash *= 1099511628211ULL;
            val >>= 8;
        }
        return hash;
    }
};

}  // namespace oro::ycsb
#endif  // ORO_YCSB_GENERATOR_H
