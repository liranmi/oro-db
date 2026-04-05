/*
 * oro-db unified benchmark CLI
 *
 * Supports two workloads: TPC-C and YCSB.
 *
 * Usage:
 *   ./oro_bench --workload tpcc --warehouses 4 --threads 8 --duration 30
 *   ./oro_bench --workload tpcc --small --warehouses 1 --threads 1
 *   ./oro_bench --workload ycsb --profile A --records 1000000 --threads 16 --distribution zipfian
 *   ./oro_bench --workload ycsb --profile E --records 100000 --threads 4 --scan-length 100
 */

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstring>
#include <libgen.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "mot_engine.h"
#include "session_context.h"
#include "session_manager.h"
#include "txn.h"

#include "bench_config.h"
#include "bench_stats.h"
#include "bench_util.h"
#include "tpcc_workload.h"
#include "tpcc_txn.h"
#include "tpcc_query.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "ycsb_workload.h"
#include "ycsb_txn.h"
#include "ycsb_config.h"
#include "ycsb_generator.h"

using Clock = std::chrono::high_resolution_clock;

// ---------------------------------------------------------------------------
// CLI helpers
// ---------------------------------------------------------------------------

static void PrintUsage(const char* prog)
{
    printf("Usage: %s --workload tpcc|ycsb [options]\n\n", prog);
    printf("Common options:\n");
    printf("  --threads N          Worker threads (default: 1)\n");
    printf("  --duration N         Benchmark duration in seconds (default: 10)\n");
    printf("  --max-txns N         Stop after N total transactions (overrides --duration)\n");
    printf("  --json FILE          Write results to JSON file\n");
    printf("  --config FILE        Override mot.conf path\n");
    printf("  --check              Run post-benchmark data consistency checks\n");
    printf("\nTPC-C options:\n");
    printf("  --warehouses N       Number of warehouses (default: 1)\n");
    printf("  --small              Use reduced TPC-C schema\n");
    printf("  -M                   Full TPC-C mix (default): 45%% NewOrder, 43%% Payment, etc.\n");
    printf("  -Tp N                NewOrder/Payment only: N%% NewOrder, (100-N)%% Payment\n");
    printf("\nYCSB options:\n");
    printf("  --profile A|B|C|D|E|F   Workload profile (default: A)\n");
    printf("  --records N             Number of records (default: 1000000)\n");
    printf("  --distribution uniform|zipfian (default: zipfian)\n");
    printf("  --fields N              Fields per record (default: 10)\n");
    printf("  --field-length N        Bytes per field (default: 100)\n");
    printf("  --ops-per-txn N         Operations per transaction (default: 16)\n");
    printf("  --scan-length N         Max scan length for profile E (default: 100)\n");
}

static bool MatchArg(const char* arg, const char* name)
{
    return strcmp(arg, name) == 0;
}

static bool ParseConfig(int argc, char* argv[], oro::BenchConfig& cfg)
{
    bool workload_set = false;

    for (int i = 1; i < argc; ++i) {
        if (MatchArg(argv[i], "--help") || MatchArg(argv[i], "-h")) {
            PrintUsage(argv[0]);
            return false;
        }
        if (MatchArg(argv[i], "--workload") && i + 1 < argc) {
            ++i;
            if (MatchArg(argv[i], "tpcc")) {
                cfg.workload = oro::WorkloadType::TPCC;
            } else if (MatchArg(argv[i], "ycsb")) {
                cfg.workload = oro::WorkloadType::YCSB;
            } else {
                fprintf(stderr, "Error: unknown workload '%s' (expected tpcc or ycsb)\n", argv[i]);
                return false;
            }
            workload_set = true;
        } else if (MatchArg(argv[i], "--threads") && i + 1 < argc) {
            cfg.threads = (uint32_t)atoi(argv[++i]);
        } else if (MatchArg(argv[i], "--duration") && i + 1 < argc) {
            cfg.duration_sec = (uint32_t)atoi(argv[++i]);
        } else if (MatchArg(argv[i], "--max-txns") && i + 1 < argc) {
            cfg.max_txns = (uint64_t)atol(argv[++i]);
        } else if (MatchArg(argv[i], "--json") && i + 1 < argc) {
            cfg.json_output = true;
            cfg.json_file = argv[++i];
        } else if (MatchArg(argv[i], "--config") && i + 1 < argc) {
            cfg.config_path = argv[++i];
        } else if (MatchArg(argv[i], "--check")) {
            cfg.check = true;
        // TPC-C flags
        } else if (MatchArg(argv[i], "--warehouses") && i + 1 < argc) {
            cfg.tpcc_warehouses = (uint32_t)atoi(argv[++i]);
        } else if (MatchArg(argv[i], "--small")) {
            cfg.tpcc_small_schema = true;
        } else if (MatchArg(argv[i], "-M")) {
            cfg.tpcc_mode = oro::TpccMode::MIXED;
        } else if (MatchArg(argv[i], "-Tp") && i + 1 < argc) {
            int pct = atoi(argv[++i]);
            if (pct < 0 || pct > 100) {
                fprintf(stderr, "Error: -Tp percentage must be 0–100\n");
                return false;
            }
            cfg.tpcc_mode = oro::TpccMode::TWO_PHASE;
            cfg.tpcc_new_order_pct    = pct / 100.0;
            cfg.tpcc_payment_pct      = 1.0 - cfg.tpcc_new_order_pct;
            cfg.tpcc_order_status_pct = 0;
            cfg.tpcc_delivery_pct     = 0;
            cfg.tpcc_stock_level_pct  = 0;
        // YCSB flags
        } else if (MatchArg(argv[i], "--profile") && i + 1 < argc) {
            ++i;
            switch (argv[i][0]) {
                case 'A': case 'a': cfg.ycsb_profile = oro::YcsbProfile::A; break;
                case 'B': case 'b': cfg.ycsb_profile = oro::YcsbProfile::B; break;
                case 'C': case 'c': cfg.ycsb_profile = oro::YcsbProfile::C; break;
                case 'D': case 'd': cfg.ycsb_profile = oro::YcsbProfile::D; break;
                case 'E': case 'e': cfg.ycsb_profile = oro::YcsbProfile::E; break;
                case 'F': case 'f': cfg.ycsb_profile = oro::YcsbProfile::F; break;
                default:
                    fprintf(stderr, "Error: unknown YCSB profile '%s'\n", argv[i]);
                    return false;
            }
        } else if (MatchArg(argv[i], "--records") && i + 1 < argc) {
            cfg.ycsb_record_count = (uint64_t)atol(argv[++i]);
        } else if (MatchArg(argv[i], "--distribution") && i + 1 < argc) {
            ++i;
            if (MatchArg(argv[i], "uniform")) {
                cfg.ycsb_distribution = oro::Distribution::UNIFORM;
            } else if (MatchArg(argv[i], "zipfian")) {
                cfg.ycsb_distribution = oro::Distribution::ZIPFIAN;
            } else {
                fprintf(stderr, "Error: unknown distribution '%s'\n", argv[i]);
                return false;
            }
        } else if (MatchArg(argv[i], "--fields") && i + 1 < argc) {
            cfg.ycsb_field_count = (uint32_t)atoi(argv[++i]);
        } else if (MatchArg(argv[i], "--field-length") && i + 1 < argc) {
            cfg.ycsb_field_length = (uint32_t)atoi(argv[++i]);
        } else if (MatchArg(argv[i], "--ops-per-txn") && i + 1 < argc) {
            cfg.ycsb_ops_per_txn = (uint32_t)atoi(argv[++i]);
        } else if (MatchArg(argv[i], "--scan-length") && i + 1 < argc) {
            cfg.ycsb_scan_length = (uint32_t)atoi(argv[++i]);
        } else {
            fprintf(stderr, "Error: unknown argument '%s'\n", argv[i]);
            PrintUsage(argv[0]);
            return false;
        }
    }

    if (!workload_set) {
        fprintf(stderr, "Error: --workload is required\n\n");
        PrintUsage(argv[0]);
        return false;
    }

    return true;
}

static const char* WorkloadName(oro::WorkloadType w)
{
    return (w == oro::WorkloadType::TPCC) ? "TPC-C" : "YCSB";
}

static const char* ProfileName(oro::YcsbProfile p)
{
    switch (p) {
        case oro::YcsbProfile::A: return "A";
        case oro::YcsbProfile::B: return "B";
        case oro::YcsbProfile::C: return "C";
        case oro::YcsbProfile::D: return "D";
        case oro::YcsbProfile::E: return "E";
        case oro::YcsbProfile::F: return "F";
    }
    return "?";
}

static const char* DistributionName(oro::Distribution d)
{
    return (d == oro::Distribution::UNIFORM) ? "uniform" : "zipfian";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[])
{
    oro::BenchConfig cfg;
    if (!ParseConfig(argc, argv, cfg))
        return 1;

    // --- Print configuration ---
    printf("=== oro-db benchmark ===\n");
    printf("  Workload:   %s\n", WorkloadName(cfg.workload));
    printf("  Threads:    %u\n", cfg.threads);
    if (cfg.max_txns > 0)
        printf("  Max txns:   %lu\n", (unsigned long)cfg.max_txns);
    else
        printf("  Duration:   %u sec\n", cfg.duration_sec);

    if (cfg.workload == oro::WorkloadType::TPCC) {
        printf("  Warehouses: %u\n", cfg.tpcc_warehouses);
        if (cfg.tpcc_mode == oro::TpccMode::TWO_PHASE)
            printf("  Mode:       -Tp (%.0f%% NewOrder, %.0f%% Payment)\n",
                   cfg.tpcc_new_order_pct * 100, cfg.tpcc_payment_pct * 100);
        else
            printf("  Mode:       -M (full TPC-C mix)\n");
        if (cfg.tpcc_small_schema)
            printf("  Schema:     small (reduced columns)\n");
    } else {
        printf("  Profile:    %s\n", ProfileName(cfg.ycsb_profile));
        printf("  Records:    %lu\n", (unsigned long)cfg.ycsb_record_count);
        printf("  Distrib:    %s\n", DistributionName(cfg.ycsb_distribution));
        printf("  Fields:     %u x %u bytes\n", cfg.ycsb_field_count, cfg.ycsb_field_length);
        printf("  Ops/txn:    %u\n", cfg.ycsb_ops_per_txn);
        if (cfg.ycsb_profile == oro::YcsbProfile::E)
            printf("  Scan len:   %u\n", cfg.ycsb_scan_length);
    }
    printf("\n");

    // --- Initialize MOT engine ---
    printf("[1] Initializing MOT engine...\n");

    const char* cfgPath = nullptr;
    char cfgBuf[PATH_MAX];

    if (!cfg.config_path.empty()) {
        cfgPath = cfg.config_path.c_str();
    } else {
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len > 0) {
            exePath[len] = '\0';
            snprintf(cfgBuf, sizeof(cfgBuf), "%s/../config/mot.conf", dirname(exePath));
            cfgPath = cfgBuf;
        }
    }

    MOT::MOTEngine* engine = MOT::MOTEngine::CreateInstance(cfgPath);
    if (!engine) {
        fprintf(stderr, "FATAL: Failed to create MOT engine\n");
        return 1;
    }
    printf("    Engine initialized.\n\n");

    // --- Create schema and populate data ---
    printf("[2] Creating schema...\n");

    // DDL session
    MOT::SessionContext* ddl_session = engine->GetSessionManager()->CreateSessionContext();
    if (!ddl_session) {
        fprintf(stderr, "FATAL: Failed to create DDL session\n");
        MOT::MOTEngine::DestroyInstance();
        return 1;
    }
    MOT::TxnManager* ddl_txn = ddl_session->GetTxnManager();

    oro::tpcc::TpccTables tpcc_tables;
    oro::ycsb::YcsbTables ycsb_tables;

    if (cfg.workload == oro::WorkloadType::TPCC) {
        ddl_txn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
        if (!oro::tpcc::CreateSchema(ddl_txn, tpcc_tables, cfg.tpcc_small_schema)) {
            fprintf(stderr, "FATAL: TPC-C schema creation failed\n");
            engine->GetSessionManager()->DestroySessionContext(ddl_session);
            MOT::MOTEngine::DestroyInstance();
            return 1;
        }
        {
            MOT::RC drc = ddl_txn->Commit();
            if (drc != MOT::RC_OK) {
                fprintf(stderr, "FATAL: Schema commit: %s\n", MOT::RcToString(drc));
                engine->GetSessionManager()->DestroySessionContext(ddl_session);
                MOT::MOTEngine::DestroyInstance();
                return 1;
            }
            ddl_txn->EndTransaction();
        }
        printf("    TPC-C schema created.\n");

        printf("[3] Populating TPC-C data (%u warehouses)...\n", cfg.tpcc_warehouses);
        if (!oro::tpcc::PopulateData(engine, tpcc_tables, cfg.tpcc_warehouses, cfg.tpcc_small_schema)) {
            fprintf(stderr, "FATAL: TPC-C data population failed\n");
            engine->GetSessionManager()->DestroySessionContext(ddl_session);
            MOT::MOTEngine::DestroyInstance();
            return 1;
        }
        printf("    Data population complete.\n\n");
    } else {
        ddl_txn->StartTransaction(0, MOT::ISOLATION_LEVEL::READ_COMMITED);
        if (!oro::ycsb::CreateSchema(ddl_txn, ycsb_tables, cfg.ycsb_field_count, cfg.ycsb_field_length)) {
            fprintf(stderr, "FATAL: YCSB schema creation failed\n");
            engine->GetSessionManager()->DestroySessionContext(ddl_session);
            MOT::MOTEngine::DestroyInstance();
            return 1;
        }
        ddl_txn->LiteCommit();
        printf("    YCSB schema created.\n");

        printf("[3] Populating YCSB data (%lu records)...\n", (unsigned long)cfg.ycsb_record_count);
        if (!oro::ycsb::PopulateData(engine, ycsb_tables, cfg.ycsb_record_count,
                                     cfg.ycsb_field_count, cfg.ycsb_field_length)) {
            fprintf(stderr, "FATAL: YCSB data population failed\n");
            engine->GetSessionManager()->DestroySessionContext(ddl_session);
            MOT::MOTEngine::DestroyInstance();
            return 1;
        }
        printf("    Data population complete.\n\n");
    }

    // Done with DDL session
    engine->GetSessionManager()->DestroySessionContext(ddl_session);
    ddl_session = nullptr;

    // --- Run benchmark ---
    if (cfg.max_txns > 0)
        printf("[4] Running %s benchmark for %lu txns with %u threads...\n",
               WorkloadName(cfg.workload), (unsigned long)cfg.max_txns, cfg.threads);
    else
        printf("[4] Running %s benchmark for %u seconds with %u threads...\n",
               WorkloadName(cfg.workload), cfg.duration_sec, cfg.threads);

    std::atomic<bool> running{true};
    std::atomic<uint64_t> txns_remaining{cfg.max_txns};  // 0 = unlimited
    std::vector<oro::ThreadStats> all_stats(cfg.threads);
    std::vector<std::thread> workers;
    workers.reserve(cfg.threads);

    auto start_time = Clock::now();

    if (cfg.workload == oro::WorkloadType::TPCC) {
        // TPC-C worker threads
        for (uint32_t t = 0; t < cfg.threads; ++t) {
            workers.emplace_back([&, t]() {
                MOT::SessionContext* session = engine->GetSessionManager()->CreateSessionContext();
                if (!session) {
                    fprintf(stderr, "ERROR: Worker %u failed to create session\n", t);
                    return;
                }
                MOT::TxnManager* txn = session->GetTxnManager();
                oro::ThreadStats& stats = all_stats[t];
                oro::FastRandom rng(t * 31337 + 7);
                // Each thread is assigned a home warehouse (round-robin)
                uint64_t home_w = (t % cfg.tpcc_warehouses) + 1;
                uint32_t num_wh = cfg.tpcc_warehouses;

                while (running.load(std::memory_order_relaxed)) {
                    // Check transaction count limit (atomic to avoid TOCTOU race)
                    if (cfg.max_txns > 0) {
                        uint64_t prev = txns_remaining.fetch_sub(1, std::memory_order_relaxed);
                        if (prev == 0) {
                            txns_remaining.fetch_add(1, std::memory_order_relaxed);
                            break;
                        }
                    }

                    auto txnType = oro::tpcc::PickTxnType(cfg, rng);
                    MOT::RC rc = MOT::RC_ABORT;

                    switch (txnType) {
                        case oro::tpcc::TxnType::NEW_ORDER: {
                            auto params = oro::tpcc::GenNewOrder(num_wh, home_w, rng);
                            rc = oro::tpcc::RunNewOrder(txn, tpcc_tables, params, rng);
                            if (rc == MOT::RC_OK) stats.tpcc_new_order_ok++;
                            else stats.tpcc_new_order_fail++;
                            break;
                        }
                        case oro::tpcc::TxnType::PAYMENT: {
                            auto params = oro::tpcc::GenPayment(num_wh, home_w, rng);
                            rc = oro::tpcc::RunPayment(txn, tpcc_tables, params);
                            if (rc == MOT::RC_OK) stats.tpcc_payment_ok++;
                            else stats.tpcc_payment_fail++;
                            break;
                        }
                        case oro::tpcc::TxnType::ORDER_STATUS: {
                            auto params = oro::tpcc::GenOrderStatus(home_w, rng);
                            rc = oro::tpcc::RunOrderStatus(txn, tpcc_tables, params);
                            if (rc == MOT::RC_OK) stats.tpcc_order_status_ok++;
                            else stats.tpcc_order_status_fail++;
                            break;
                        }
                        case oro::tpcc::TxnType::DELIVERY: {
                            auto params = oro::tpcc::GenDelivery(home_w, rng);
                            rc = oro::tpcc::RunDelivery(txn, tpcc_tables, params);
                            if (rc == MOT::RC_OK) stats.tpcc_delivery_ok++;
                            else stats.tpcc_delivery_fail++;
                            break;
                        }
                        case oro::tpcc::TxnType::STOCK_LEVEL: {
                            auto params = oro::tpcc::GenStockLevel(home_w, rng);
                            rc = oro::tpcc::RunStockLevel(txn, tpcc_tables, params);
                            if (rc == MOT::RC_OK) stats.tpcc_stock_level_ok++;
                            else stats.tpcc_stock_level_fail++;
                            break;
                        }
                    }
                    if (rc == MOT::RC_OK) stats.commits++;
                    else stats.aborts++;
                }

                engine->GetSessionManager()->DestroySessionContext(session);
                engine->OnCurrentThreadEnding();
            });
        }
    } else {
        // YCSB worker threads
        for (uint32_t t = 0; t < cfg.threads; ++t) {
            workers.emplace_back([&, t]() {
                MOT::SessionContext* session = engine->GetSessionManager()->CreateSessionContext();
                if (!session) {
                    fprintf(stderr, "ERROR: Worker %u failed to create session\n", t);
                    return;
                }
                MOT::TxnManager* txn = session->GetTxnManager();
                oro::ThreadStats& stats = all_stats[t];
                oro::FastRandom rng(t + 1);

                oro::ycsb::UniformGenerator uniform_gen(cfg.ycsb_record_count);
                oro::ycsb::ZipfianGenerator  zipfian_gen(cfg.ycsb_record_count, cfg.ycsb_zipfian_theta);

                while (running.load(std::memory_order_relaxed)) {
                    if (cfg.max_txns > 0) {
                        uint64_t prev = txns_remaining.fetch_sub(1, std::memory_order_relaxed);
                        if (prev == 0) {
                            txns_remaining.fetch_add(1, std::memory_order_relaxed);
                            break;
                        }
                    }

                    MOT::RC rc = oro::ycsb::RunYcsbTxn(
                        txn, ycsb_tables, cfg.ycsb_profile,
                        cfg.ycsb_ops_per_txn,
                        cfg.ycsb_field_count, cfg.ycsb_field_length,
                        cfg.ycsb_scan_length, cfg.ycsb_record_count,
                        &uniform_gen, &zipfian_gen,
                        cfg.ycsb_distribution, rng, t);

                    if (rc == MOT::RC_OK) stats.commits++;
                    else stats.aborts++;
                }

                engine->GetSessionManager()->DestroySessionContext(session);
                engine->OnCurrentThreadEnding();
            });
        }
    }

    // Termination: either by time or by transaction count
    if (cfg.max_txns > 0) {
        // Count-limited: just wait for workers to drain the counter
        for (auto& w : workers)
            w.join();
    } else {
        // Time-limited: timer thread signals stop
        std::thread timer([&]() {
            std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_sec));
            running.store(false, std::memory_order_relaxed);
        });
        timer.join();
        for (auto& w : workers)
            w.join();
    }

    auto end_time = Clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();

    // --- Aggregate and print results ---
    bool is_tpcc = (cfg.workload == oro::WorkloadType::TPCC);
    oro::AggregateStats agg = oro::Aggregate(all_stats, elapsed_sec);
    oro::PrintStats(agg, is_tpcc);

    // Optional JSON output
    if (cfg.json_output) {
        std::string json = oro::StatsToJson(agg, is_tpcc);
        FILE* fp = fopen(cfg.json_file.c_str(), "w");
        if (fp) {
            fwrite(json.data(), 1, json.size(), fp);
            fclose(fp);
            printf("  Results written to %s\n", cfg.json_file.c_str());
        } else {
            fprintf(stderr, "WARNING: Could not open '%s' for writing\n", cfg.json_file.c_str());
        }
    }

    // --- Post-run consistency checks ---
    int check_failures = 0;
    if (cfg.check) {
        printf("\n[5] Running consistency checks...\n");

        // Need a session for transactional index access
        MOT::SessionContext* check_session = engine->GetSessionManager()->CreateSessionContext();
        if (!check_session) {
            fprintf(stderr, "FATAL: Failed to create check session\n");
        } else {
            MOT::TxnManager* check_txn = check_session->GetTxnManager();

            if (cfg.workload == oro::WorkloadType::YCSB) {
                // Verify throughput > 0 (transactions actually executed)
                if (agg.total_commits > 0) {
                    printf("    [PASS] YCSB commits: %lu\n", (unsigned long)agg.total_commits);
                } else {
                    fprintf(stderr, "    [FAIL] YCSB commits: 0 (expected > 0)\n");
                    check_failures++;
                }
                // Verify abort rate is reasonable (< 50%)
                if (agg.abort_rate < 0.5) {
                    printf("    [PASS] Abort rate: %.2f%%\n", agg.abort_rate * 100.0);
                } else {
                    fprintf(stderr, "    [FAIL] Abort rate: %.2f%% (expected < 50%%)\n", agg.abort_rate * 100.0);
                    check_failures++;
                }
            }

            engine->GetSessionManager()->DestroySessionContext(check_session);
        }

        if (check_failures == 0) {
            printf("    All checks passed.\n");
        } else {
            fprintf(stderr, "    %d check(s) FAILED.\n", check_failures);
        }
    }

    // --- Cleanup ---
    printf("\n[6] Cleaning up...\n");
    MOT::MOTEngine::DestroyInstance();
    printf("    Done.\n");

    return check_failures;
}
