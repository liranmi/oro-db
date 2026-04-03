#ifndef ORO_TPCC_WORKLOAD_H
#define ORO_TPCC_WORKLOAD_H

#include <cstdint>
#include "table.h"
#include "index.h"
#include "index_factory.h"
#include "mot_engine.h"
#include "session_context.h"
#include "txn.h"
#include "bench_config.h"

namespace oro::tpcc {

/*
 * TPC-C schema and data population.
 * Tables and indexes are created following the TPC-C spec (Clause 1.3, 1.4).
 */
struct TpccTables {
    MOT::Table* warehouse  = nullptr;
    MOT::Table* district   = nullptr;
    MOT::Table* customer   = nullptr;
    MOT::Table* history    = nullptr;
    MOT::Table* new_order  = nullptr;
    MOT::Table* order_tbl  = nullptr;
    MOT::Table* order_line = nullptr;
    MOT::Table* item       = nullptr;
    MOT::Table* stock      = nullptr;

    // Primary indexes (one per table)
    MOT::Index* ix_warehouse  = nullptr;  // (W_ID)
    MOT::Index* ix_district   = nullptr;  // (D_W_ID, D_ID)
    MOT::Index* ix_customer   = nullptr;  // (C_W_ID, C_D_ID, C_ID)
    MOT::Index* ix_history    = nullptr;  // surrogate key
    MOT::Index* ix_new_order  = nullptr;  // (NO_W_ID, NO_D_ID, NO_O_ID)
    MOT::Index* ix_order      = nullptr;  // (O_W_ID, O_D_ID, O_ID)
    MOT::Index* ix_order_line = nullptr;  // (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
    MOT::Index* ix_item       = nullptr;  // (I_ID)
    MOT::Index* ix_stock      = nullptr;  // (S_W_ID, S_I_ID)

    // Secondary indexes
    MOT::Index* ix_customer_last = nullptr;  // (C_W_ID, C_D_ID, C_LAST) — non-unique
};

// Create all TPC-C tables and indexes. Must be called within a transaction.
bool CreateSchema(MOT::TxnManager* txn, TpccTables& tables, bool small_schema);

// Populate all tables with initial data. Multi-threaded: one thread per warehouse,
// plus the main thread populates the ITEM table first.
bool PopulateData(MOT::MOTEngine* engine, TpccTables& tables, uint32_t num_warehouses, bool small_schema);

}  // namespace oro::tpcc
#endif  // ORO_TPCC_WORKLOAD_H
