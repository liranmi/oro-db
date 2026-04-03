/*
 * TPC-C Transaction Implementations for oro-db — STUB
 * TODO: Rewrite to use BuildSearchKey + packed keys after population is verified.
 */

#include "tpcc_txn.h"
#include "tpcc_config.h"
#include "tpcc_helper.h"
#include "bench_util.h"
#include "row.h"
#include "index.h"
#include "index_iterator.h"
#include "sentinel.h"
#include "key.h"

#include <cstring>
#include <ctime>
#include <atomic>
#include <vector>

using namespace MOT;

namespace oro::tpcc {

RC RunNewOrder(TxnManager* txn, const TpccTables& t, const NewOrderParams& p, FastRandom& rng)
{
    (void)t; (void)p; (void)rng;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    txn->Rollback();
    return RC_ABORT;
}

RC RunPayment(TxnManager* txn, const TpccTables& t, const PaymentParams& p)
{
    (void)t; (void)p;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    txn->Rollback();
    return RC_ABORT;
}

RC RunOrderStatus(TxnManager* txn, const TpccTables& t, const OrderStatusParams& p)
{
    (void)t; (void)p;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    txn->Rollback();
    return RC_ABORT;
}

RC RunDelivery(TxnManager* txn, const TpccTables& t, const DeliveryParams& p)
{
    (void)t; (void)p;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    txn->Rollback();
    return RC_ABORT;
}

RC RunStockLevel(TxnManager* txn, const TpccTables& t, const StockLevelParams& p)
{
    (void)t; (void)p;
    txn->StartTransaction(0, ISOLATION_LEVEL::READ_COMMITED);
    txn->Rollback();
    return RC_ABORT;
}

}  // namespace oro::tpcc
