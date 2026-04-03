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
    }
    uint64_t total = agg.total_commits + agg.total_aborts;
    agg.throughput_tps = (elapsed_sec > 0) ? (double)agg.total_commits / elapsed_sec : 0;
    agg.abort_rate = (total > 0) ? (double)agg.total_aborts / (double)total : 0;
    return agg;
}

inline void PrintStats(const AggregateStats& s, bool is_tpcc)
{
    printf("\n=== Benchmark Results ===\n");
    printf("  Duration:    %.2f sec\n", s.elapsed_sec);
    printf("  Commits:     %lu\n", (unsigned long)s.total_commits);
    printf("  Aborts:      %lu\n", (unsigned long)s.total_aborts);
    printf("  Throughput:  %.0f txn/sec\n", s.throughput_tps);
    printf("  Abort rate:  %.2f%%\n", s.abort_rate * 100.0);

    if (is_tpcc) {
        printf("\n  TPC-C Transaction Mix:\n");
        printf("  %-16s %8s %8s\n", "Transaction", "OK", "Fail");
        printf("  %-16s %8lu %8lu\n", "NewOrder",     (unsigned long)s.new_order_ok,     (unsigned long)s.new_order_fail);
        printf("  %-16s %8lu %8lu\n", "Payment",      (unsigned long)s.payment_ok,       (unsigned long)s.payment_fail);
        printf("  %-16s %8lu %8lu\n", "OrderStatus",  (unsigned long)s.order_status_ok,  (unsigned long)s.order_status_fail);
        printf("  %-16s %8lu %8lu\n", "Delivery",     (unsigned long)s.delivery_ok,      (unsigned long)s.delivery_fail);
        printf("  %-16s %8lu %8lu\n", "StockLevel",   (unsigned long)s.stock_level_ok,   (unsigned long)s.stock_level_fail);
    }
    printf("=========================\n");
}

inline std::string StatsToJson(const AggregateStats& s, bool is_tpcc)
{
    std::string j = "{\n";
    j += "  \"elapsed_sec\": "   + std::to_string(s.elapsed_sec)    + ",\n";
    j += "  \"commits\": "       + std::to_string(s.total_commits)  + ",\n";
    j += "  \"aborts\": "        + std::to_string(s.total_aborts)   + ",\n";
    j += "  \"throughput_tps\": " + std::to_string(s.throughput_tps) + ",\n";
    j += "  \"abort_rate\": "    + std::to_string(s.abort_rate);
    if (is_tpcc) {
        j += ",\n  \"tpcc\": {\n";
        j += "    \"new_order\":     {\"ok\": " + std::to_string(s.new_order_ok)     + ", \"fail\": " + std::to_string(s.new_order_fail) + "},\n";
        j += "    \"payment\":       {\"ok\": " + std::to_string(s.payment_ok)       + ", \"fail\": " + std::to_string(s.payment_fail) + "},\n";
        j += "    \"order_status\":  {\"ok\": " + std::to_string(s.order_status_ok)  + ", \"fail\": " + std::to_string(s.order_status_fail) + "},\n";
        j += "    \"delivery\":      {\"ok\": " + std::to_string(s.delivery_ok)      + ", \"fail\": " + std::to_string(s.delivery_fail) + "},\n";
        j += "    \"stock_level\":   {\"ok\": " + std::to_string(s.stock_level_ok)   + ", \"fail\": " + std::to_string(s.stock_level_fail) + "}\n";
        j += "  }";
    }
    j += "\n}\n";
    return j;
}

}  // namespace oro
#endif  // ORO_BENCH_STATS_H
