#ifndef ORO_YCSB_WORKLOAD_H
#define ORO_YCSB_WORKLOAD_H

#include <cstdint>
#include "table.h"
#include "index.h"
#include "index_factory.h"
#include "mot_engine.h"
#include "session_context.h"
#include "session_manager.h"
#include "txn.h"
#include "row.h"
#include "catalog_column_types.h"
#include "bench_config.h"
#include "bench_util.h"
#include "ycsb_config.h"

namespace oro::ycsb {

struct YcsbTables {
    MOT::Table* usertable = nullptr;
    MOT::Index* ix_primary = nullptr;
};

// Create the YCSB "usertable" schema with a primary index on YCSB_KEY.
// Must be called within an active transaction context.
bool CreateSchema(MOT::TxnManager* txn, YcsbTables& tables,
                  uint32_t field_count, uint32_t field_length);

// Populate the usertable with record_count rows of random data.
// Creates its own session internally. Each row is inserted as a micro-transaction.
bool PopulateData(MOT::MOTEngine* engine, YcsbTables& tables,
                  uint64_t record_count, uint32_t field_count,
                  uint32_t field_length);

}  // namespace oro::ycsb
#endif  // ORO_YCSB_WORKLOAD_H
