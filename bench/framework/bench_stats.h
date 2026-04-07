#ifndef ORO_BENCH_STATS_H
#define ORO_BENCH_STATS_H

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <chrono>

namespace oro {

struct TxnCounters {
    std::atomic<uint64_t> commits{0};
    std::atomic<uint64_t> aborts{0};
};

// Per-thread stats, padded to avoid false sharing
struct alignas(64) ThreadStats {
    uint64_t commits = 0;
    uint64_t aborts  = 0;
    // TPC-C per-transaction-type counters
    uint64_t tpcc_new_order_ok   = 0;
    uint64_t tpcc_payment_ok     = 0;
    uint64_t tpcc_order_status_ok = 0;
    uint64_t tpcc_delivery_ok    = 0;
    uint64_t tpcc_stock_level_ok = 0;
    uint64_t tpcc_new_order_fail   = 0;
    uint64_t tpcc_payment_fail     = 0;
    uint64_t tpcc_order_status_fail = 0;
    uint64_t tpcc_delivery_fail    = 0;
    uint64_t tpcc_stock_level_fail = 0;
    uint64_t tpcc_consistency_ok   = 0;
    uint64_t tpcc_consistency_fail = 0;
};

struct AggregateStats {
    double   elapsed_sec      = 0;
    uint64_t total_commits    = 0;
    uint64_t total_aborts     = 0;
    double   throughput_tps   = 0;  // commits / sec
    double   abort_rate       = 0;  // aborts / (commits + aborts)

    // TPC-C breakdown
    uint64_t new_order_ok = 0, new_order_fail = 0;
    uint64_t payment_ok = 0, payment_fail = 0;
    uint64_t order_status_ok = 0, order_status_fail = 0;
    uint64_t delivery_ok = 0, delivery_fail = 0;
    uint64_t stock_level_ok = 0, stock_level_fail = 0;
    uint64_t consistency_ok = 0, consistency_fail = 0;
};

inline AggregateStats Aggregate(const std::vector<ThreadStats>& per_thread, double elapsed_sec)
{
    AggregateStats agg;
    agg.elapsed_sec = elapsed_sec;
    for (auto& ts : per_thread) {
        agg.total_commits += ts.commits;
        agg.total_aborts  += ts.aborts;
        agg.new_order_ok += ts.tpcc_new_order_ok;
        agg.payment_ok += ts.tpcc_payment_ok;
        agg.order_status_ok += ts.tpcc_order_status_ok;
        agg.delivery_ok += ts.tpcc_delivery_ok;
        agg.stock_level_ok += ts.tpcc_stock_level_ok;
        agg.new_order_fail += ts.tpcc_new_order_fail;
        agg.payment_fail += ts.tpcc_payment_fail;
        agg.order_status_fail += ts.tpcc_order_status_fail;
        agg.delivery_fail += ts.tpcc_delivery_fail;
        agg.stock_level_fail += ts.tpcc_stock_level_fail;
        agg.consistency_ok += ts.tpcc_consistency_ok;
        agg.consistency_fail += ts.tpcc_consistency_fail;
    }
    uint64_t total = agg.total_commits + agg.total_aborts;
    agg.throughput_tps = (elapsed_sec > 0) ? (double)agg.total_commits / elapsed_sec : 0;
    agg.abort_rate = (total > 0) ? (double)agg.total_aborts / (double)total : 0;
    return agg;
}

inline void FormatLargeNumber(char* buf, size_t bufsize, double val, const char* suffix)
{
    if (val >= 1e6)
        snprintf(buf, bufsize, "%.2fM %s", val / 1e6, suffix);
    else if (val >= 1e3)
        snprintf(buf, bufsize, "%.2fK %s", val / 1e3, suffix);
    else
        snprintf(buf, bufsize, "%.0f %s", val, suffix);
}

inline void PrintStats(const AggregateStats& s, bool is_tpcc, bool is_mixed = false)
{
    printf("\n=== Benchmark Results ===\n");
    printf("  Duration:    %.2f sec\n", s.elapsed_sec);
    printf("  Commits:     %lu\n", (unsigned long)s.total_commits);
    printf("  Aborts:      %lu\n", (unsigned long)s.total_aborts);

    char throughput_buf[64];
    if (is_tpcc && is_mixed) {
        double tpmc = (s.elapsed_sec > 0) ? (double)s.new_order_ok / s.elapsed_sec * 60.0 : 0;
        FormatLargeNumber(throughput_buf, sizeof(throughput_buf), tpmc, "tpmC");
    } else {
        double tpm = (s.elapsed_sec > 0) ? (double)s.total_commits / s.elapsed_sec * 60.0 : 0;
        FormatLargeNumber(throughput_buf, sizeof(throughput_buf), tpm, "TPM");
    }
    printf("  Throughput:  %s\n", throughput_buf);

    printf("  Abort rate:  %.2f%%\n", s.abort_rate * 100.0);

    if (is_tpcc) {
        printf("\n  TPC-C Transaction Mix:\n");
        printf("  %-16s %8s %8s\n", "Transaction", "OK", "Fail");
        printf("  %-16s %8lu %8lu\n", "NewOrder",     (unsigned long)s.new_order_ok,     (unsigned long)s.new_order_fail);
        printf("  %-16s %8lu %8lu\n", "Payment",      (unsigned long)s.payment_ok,       (unsigned long)s.payment_fail);
        printf("  %-16s %8lu %8lu\n", "OrderStatus",  (unsigned long)s.order_status_ok,  (unsigned long)s.order_status_fail);
        printf("  %-16s %8lu %8lu\n", "Delivery",     (unsigned long)s.delivery_ok,      (unsigned long)s.delivery_fail);
        printf("  %-16s %8lu %8lu\n", "StockLevel",   (unsigned long)s.stock_level_ok,   (unsigned long)s.stock_level_fail);
        if (s.consistency_ok + s.consistency_fail > 0)
            printf("  %-16s %8lu %8lu\n", "Consistency",  (unsigned long)s.consistency_ok,   (unsigned long)s.consistency_fail);
    }
    printf("=========================\n");
}

inline std::string StatsToJson(const AggregateStats& s, bool is_tpcc, bool is_mixed = false)
{
    double tpm = (s.elapsed_sec > 0) ? (double)s.total_commits / s.elapsed_sec * 60.0 : 0;
    std::string j = "{\n";
    j += "  \"elapsed_sec\": "   + std::to_string(s.elapsed_sec)    + ",\n";
    j += "  \"commits\": "       + std::to_string(s.total_commits)  + ",\n";
    j += "  \"aborts\": "        + std::to_string(s.total_aborts)   + ",\n";
    j += "  \"tpm\": "           + std::to_string(tpm)              + ",\n";
    if (is_tpcc && is_mixed) {
        double tpmc = (s.elapsed_sec > 0) ? (double)s.new_order_ok / s.elapsed_sec * 60.0 : 0;
        j += "  \"tpmc\": "     + std::to_string(tpmc)             + ",\n";
    }
    j += "  \"abort_rate\": "    + std::to_string(s.abort_rate);
    if (is_tpcc) {
        j += ",\n  \"tpcc\": {\n";
        j += "    \"new_order\":     {\"ok\": " + std::to_string(s.new_order_ok)     + ", \"fail\": " + std::to_string(s.new_order_fail) + "},\n";
        j += "    \"payment\":       {\"ok\": " + std::to_string(s.payment_ok)       + ", \"fail\": " + std::to_string(s.payment_fail) + "},\n";
        j += "    \"order_status\":  {\"ok\": " + std::to_string(s.order_status_ok)  + ", \"fail\": " + std::to_string(s.order_status_fail) + "},\n";
        j += "    \"delivery\":      {\"ok\": " + std::to_string(s.delivery_ok)      + ", \"fail\": " + std::to_string(s.delivery_fail) + "},\n";
        j += "    \"stock_level\":   {\"ok\": " + std::to_string(s.stock_level_ok)   + ", \"fail\": " + std::to_string(s.stock_level_fail) + "}";
        if (s.consistency_ok + s.consistency_fail > 0) {
            j += ",\n    \"consistency\":   {\"ok\": " + std::to_string(s.consistency_ok)   + ", \"fail\": " + std::to_string(s.consistency_fail) + "}";
        }
        j += "\n";
        j += "  }";
    }
    j += "\n}\n";
    return j;
}

}  // namespace oro
#endif  // ORO_BENCH_STATS_H
