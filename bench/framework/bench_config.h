#ifndef ORO_BENCH_CONFIG_H
#define ORO_BENCH_CONFIG_H

#include <cstdint>
#include <string>

namespace oro {

enum class WorkloadType { TPCC, YCSB };

// YCSB workload profiles per Cooper et al. (2010)
enum class YcsbProfile {
    A,  // 50% read, 50% update  (update heavy)
    B,  // 95% read, 5% update   (read mostly)
    C,  // 100% read              (read only)
    D,  // 95% read, 5% insert   (read latest)
    E,  // 95% scan, 5% insert   (short ranges)
    F   // 50% read, 50% read-modify-write
};

enum class Distribution { UNIFORM, ZIPFIAN };

// TPC-C execution mode
enum class TpccMode {
    MIXED,       // Full TPC-C mix (5 transaction types, Clause 5.2.3)
    TWO_PHASE    // NewOrder + Payment only (DBx1000-style)
};

struct BenchConfig {
    // Common
    WorkloadType workload = WorkloadType::TPCC;
    uint32_t     threads  = 1;
    uint32_t     duration_sec = 0;     // 0 = use max_txns (default); >0 = time-based, set explicitly
    uint64_t     max_txns = 100000;    // per-thread transaction count (default 100K per thread)
    bool         json_output  = false;
    bool         check = false;       // post-run data consistency validation
    std::string  json_file;
    std::string  config_path;  // mot.conf path override

    // TPC-C
    uint32_t tpcc_warehouses   = 1;
    bool     tpcc_small_schema = false;
    TpccMode tpcc_mode         = TpccMode::MIXED;
    double   tpcc_new_order_pct   = 0.45;
    double   tpcc_payment_pct     = 0.43;
    double   tpcc_order_status_pct = 0.04;
    double   tpcc_delivery_pct     = 0.04;
    double   tpcc_stock_level_pct  = 0.04;
    double   tpcc_consistency_pct  = 0.0;   // fraction of txns that run consistency checks (0 = disabled)

    // YCSB
    YcsbProfile  ycsb_profile      = YcsbProfile::A;
    Distribution ycsb_distribution = Distribution::ZIPFIAN;
    uint64_t     ycsb_record_count = 1000000;
    uint32_t     ycsb_field_count  = 10;
    uint32_t     ycsb_field_length = 100;
    uint32_t     ycsb_ops_per_txn  = 16;
    double       ycsb_zipfian_theta = 0.99;
    uint32_t     ycsb_scan_length   = 100;
};

}  // namespace oro
#endif  // ORO_BENCH_CONFIG_H
