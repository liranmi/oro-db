#ifndef ORO_TPCC_CONFIG_H
#define ORO_TPCC_CONFIG_H

/*
 * TPC-C Benchmark Constants
 * Per TPC-C Standard Specification Revision 5.11, Clauses 1, 4, and 5.
 *
 * Column layout convention: data columns first, packed _KEY column LAST.
 * The _KEY column holds a uint64 packed composite key used by the
 * InternalKey / BuildKey path in MOT indexes.
 */

#include <cstdint>

namespace oro::tpcc {

// TPC-C fixed cardinalities (Clause 1.2)
static constexpr uint32_t DIST_PER_WARE  = 10;     // 10 districts per warehouse
static constexpr uint32_t CUST_PER_DIST  = 3000;   // 3000 customers per district
static constexpr uint32_t ORD_PER_DIST   = 3000;   // 3000 orders per district (initial)
static constexpr uint32_t ITEM_COUNT     = 100000;  // 100,000 items
static constexpr uint32_t STOCK_PER_WARE = 100000;  // 100,000 stock items per warehouse
static constexpr uint32_t MAX_OL_CNT     = 15;      // max order-lines per order
static constexpr uint32_t MIN_OL_CNT     = 5;       // min order-lines per order

// NURand constants (Clause 2.1.6)
static constexpr uint32_t NURAND_C_LAST = 255;
static constexpr uint32_t NURAND_C_ID   = 1023;
static constexpr uint32_t NURAND_OL_I_ID = 8191;

// Transaction mix defaults (Clause 5.2.3)
// Minimum required: NewOrder >= 45%, Payment >= 43%, OrderStatus >= 4%, Delivery >= 4%, StockLevel >= 4%

// ======================================================================
// Column enumerations — data columns first, _KEY column LAST
// ======================================================================

// WAREHOUSE table columns
enum WH : int {
    W_NAME = 0,        // varchar(10)
    W_STREET_1,        // varchar(20)
    W_STREET_2,        // varchar(20)
    W_CITY,            // varchar(20)
    W_STATE,           // char(2)
    W_ZIP,             // char(9)
    W_TAX,             // decimal(4,4) -> double
    W_YTD,             // decimal(12,2) -> double
    W_ID,              // PK: warehouse id
    W_KEY,             // packed key: PackWhKey(W_ID)
    W_NUM_COLS
};

// DISTRICT table columns
enum DIST : int {
    D_NAME = 0,        // varchar(10)
    D_STREET_1,        // varchar(20)
    D_STREET_2,        // varchar(20)
    D_CITY,            // varchar(20)
    D_STATE,           // char(2)
    D_ZIP,             // char(9)
    D_TAX,             // decimal(4,4) -> double
    D_YTD,             // decimal(12,2) -> double
    D_NEXT_O_ID,       // int
    D_W_ID,            // PK: warehouse id
    D_ID,              // PK: district id
    D_KEY,             // packed key: PackDistKey(W_ID, D_ID)
    D_NUM_COLS
};

// CUSTOMER table columns
enum CUST : int {
    C_FIRST = 0,       // varchar(16)
    C_MIDDLE,          // char(2)
    C_LAST,            // varchar(16)
    C_STREET_1,        // varchar(20)
    C_STREET_2,        // varchar(20)
    C_CITY,            // varchar(20)
    C_STATE,           // char(2)
    C_ZIP,             // char(9)
    C_PHONE,           // char(16)
    C_SINCE,           // datetime -> int64
    C_CREDIT,          // char(2) "GC" or "BC"
    C_CREDIT_LIM,      // decimal(12,2) -> double
    C_DISCOUNT,        // decimal(4,4) -> double
    C_BALANCE,         // decimal(12,2) -> double
    C_YTD_PAYMENT,     // decimal(12,2) -> double
    C_PAYMENT_CNT,     // int
    C_DELIVERY_CNT,    // int
    C_DATA,            // varchar(500)
    C_W_ID,            // PK: warehouse id
    C_D_ID,            // PK: district id
    C_ID,              // PK: customer id
    C_KEY,             // packed key: PackCustKey(W_ID, D_ID, C_ID)
    C_NUM_COLS
};

// HISTORY table columns — no natural PK, uses fake primary (surrogate m_rowId)
enum HIST : int {
    H_C_D_ID = 0,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,            // datetime -> int64
    H_AMOUNT,          // decimal(6,2) -> double
    H_DATA,            // varchar(24)
    H_NUM_COLS         // no _KEY column — fake primary uses m_rowId
};

// NEW-ORDER table columns
enum NORD : int {
    NO_O_ID = 0,
    NO_D_ID,
    NO_W_ID,
    NO_KEY,            // packed key: PackOrderKey(W_ID, D_ID, O_ID)
    NO_NUM_COLS
};

// ORDER table columns
enum ORD : int {
    O_C_ID = 0,
    O_D_ID,
    O_W_ID,
    O_ENTRY_D,         // datetime -> int64
    O_CARRIER_ID,      // int, null for open orders
    O_OL_CNT,          // int
    O_ALL_LOCAL,       // int
    O_ID,              // PK: order id
    O_KEY,             // packed key: PackOrderKey(W_ID, D_ID, O_ID)
    O_NUM_COLS
};

// ORDER-LINE table columns
enum ORDL : int {
    OL_I_ID = 0,
    OL_D_ID,
    OL_W_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D,     // datetime -> int64, null for open orders
    OL_QUANTITY,        // int
    OL_AMOUNT,         // decimal(6,2) -> double
    OL_DIST_INFO,      // char(24)
    OL_O_ID,           // PK: order id
    OL_NUMBER,         // PK: order-line number
    OL_KEY,            // packed key: PackOlKey(W_ID, D_ID, O_ID, OL_NUMBER)
    OL_NUM_COLS
};

// ITEM table columns
enum ITEM : int {
    I_IM_ID = 0,       // int
    I_NAME,            // varchar(24)
    I_PRICE,           // decimal(5,2) -> double
    I_DATA,            // varchar(50)
    I_ID,              // PK: item id
    I_KEY,             // packed key: PackItemKey(I_ID)
    I_NUM_COLS
};

// STOCK table columns
enum STK : int {
    S_QUANTITY = 0,    // int
    S_DIST_01,         // char(24)
    S_DIST_02,
    S_DIST_03,
    S_DIST_04,
    S_DIST_05,
    S_DIST_06,
    S_DIST_07,
    S_DIST_08,
    S_DIST_09,
    S_DIST_10,
    S_YTD,             // int
    S_ORDER_CNT,       // int
    S_REMOTE_CNT,      // int
    S_DATA,            // varchar(50)
    S_W_ID,            // PK: warehouse id
    S_I_ID,            // PK: item id
    S_KEY,             // packed key: PackStockKey(W_ID, I_ID)
    S_NUM_COLS
};

// ======================================================================
// Reduced schema column enumerations (TPCC_SMALL mode)
// Drops varchar address/name fields to reduce row size for quick testing
// ======================================================================
namespace small {

enum WH : int {
    W_TAX = 0, W_YTD, W_ID, W_KEY, W_NUM_COLS
};

enum DIST : int {
    D_TAX = 0, D_YTD, D_NEXT_O_ID, D_W_ID, D_ID, D_KEY, D_NUM_COLS
};

enum CUST : int {
    C_MIDDLE = 0, C_LAST, C_STATE, C_CREDIT, C_DISCOUNT, C_BALANCE,
    C_YTD_PAYMENT, C_PAYMENT_CNT, C_W_ID, C_D_ID, C_ID, C_KEY, C_NUM_COLS
};

enum HIST : int {
    H_C_D_ID = 0, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_KEY, H_NUM_COLS
};

enum NORD : int {
    NO_O_ID = 0, NO_D_ID, NO_W_ID, NO_KEY, NO_NUM_COLS
};

enum ORD : int {
    O_C_ID = 0, O_D_ID, O_W_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ID, O_KEY, O_NUM_COLS
};

enum ORDL : int {
    OL_I_ID = 0, OL_D_ID, OL_W_ID, OL_O_ID, OL_NUMBER, OL_KEY, OL_NUM_COLS
};

enum ITEM : int {
    I_IM_ID = 0, I_NAME, I_PRICE, I_DATA, I_ID, I_KEY, I_NUM_COLS
};

enum STK : int {
    S_QUANTITY = 0, S_REMOTE_CNT, S_W_ID, S_I_ID, S_KEY, S_NUM_COLS
};

}  // namespace small
}  // namespace oro::tpcc
#endif  // ORO_TPCC_CONFIG_H
