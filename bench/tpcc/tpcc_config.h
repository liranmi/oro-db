#ifndef ORO_TPCC_CONFIG_H
#define ORO_TPCC_CONFIG_H

/*
 * TPC-C Benchmark Constants
 * Per TPC-C Standard Specification Revision 5.11, Clauses 1, 4, and 5.
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
// Column enumerations — Full schema (TPC-C Clause 1.3)
// ======================================================================

// WAREHOUSE table columns
enum WH : int {
    W_ID = 0,
    W_NAME,        // varchar(10)
    W_STREET_1,    // varchar(20)
    W_STREET_2,    // varchar(20)
    W_CITY,        // varchar(20)
    W_STATE,       // char(2)
    W_ZIP,         // char(9)
    W_TAX,         // decimal(4,4) → double
    W_YTD,         // decimal(12,2) → double
    W_NUM_COLS
};

// DISTRICT table columns
enum DIST : int {
    D_ID = 0,
    D_W_ID,
    D_NAME,        // varchar(10)
    D_STREET_1,    // varchar(20)
    D_STREET_2,    // varchar(20)
    D_CITY,        // varchar(20)
    D_STATE,       // char(2)
    D_ZIP,         // char(9)
    D_TAX,         // decimal(4,4) → double
    D_YTD,         // decimal(12,2) → double
    D_NEXT_O_ID,   // int
    D_NUM_COLS
};

// CUSTOMER table columns
enum CUST : int {
    C_ID = 0,
    C_D_ID,
    C_W_ID,
    C_FIRST,       // varchar(16)
    C_MIDDLE,      // char(2)
    C_LAST,        // varchar(16)
    C_STREET_1,    // varchar(20)
    C_STREET_2,    // varchar(20)
    C_CITY,        // varchar(20)
    C_STATE,       // char(2)
    C_ZIP,         // char(9)
    C_PHONE,       // char(16)
    C_SINCE,       // datetime → int64
    C_CREDIT,      // char(2) "GC" or "BC"
    C_CREDIT_LIM,  // decimal(12,2) → double
    C_DISCOUNT,    // decimal(4,4) → double
    C_BALANCE,     // decimal(12,2) → double
    C_YTD_PAYMENT, // decimal(12,2) → double
    C_PAYMENT_CNT, // int
    C_DELIVERY_CNT,// int
    C_DATA,        // varchar(500)
    C_NUM_COLS
};

// HISTORY table columns
enum HIST : int {
    H_C_ID = 0,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,        // datetime → int64
    H_AMOUNT,      // decimal(6,2) → double
    H_DATA,        // varchar(24)
    H_NUM_COLS
};

// NEW-ORDER table columns
enum NORD : int {
    NO_O_ID = 0,
    NO_D_ID,
    NO_W_ID,
    NO_NUM_COLS
};

// ORDER table columns
enum ORD : int {
    O_ID = 0,
    O_C_ID,
    O_D_ID,
    O_W_ID,
    O_ENTRY_D,     // datetime → int64
    O_CARRIER_ID,  // int, null for open orders
    O_OL_CNT,      // int
    O_ALL_LOCAL,   // int
    O_NUM_COLS
};

// ORDER-LINE table columns
enum ORDL : int {
    OL_O_ID = 0,
    OL_D_ID,
    OL_W_ID,
    OL_NUMBER,
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D, // datetime → int64, null for open orders
    OL_QUANTITY,    // int
    OL_AMOUNT,     // decimal(6,2) → double
    OL_DIST_INFO,  // char(24)
    OL_NUM_COLS
};

// ITEM table columns
enum ITEM : int {
    I_ID = 0,
    I_IM_ID,       // int
    I_NAME,        // varchar(24)
    I_PRICE,       // decimal(5,2) → double
    I_DATA,        // varchar(50)
    I_NUM_COLS
};

// STOCK table columns
enum STK : int {
    S_I_ID = 0,
    S_W_ID,
    S_QUANTITY,    // int
    S_DIST_01,     // char(24)
    S_DIST_02,
    S_DIST_03,
    S_DIST_04,
    S_DIST_05,
    S_DIST_06,
    S_DIST_07,
    S_DIST_08,
    S_DIST_09,
    S_DIST_10,
    S_YTD,         // int
    S_ORDER_CNT,   // int
    S_REMOTE_CNT,  // int
    S_DATA,        // varchar(50)
    S_NUM_COLS
};

// ======================================================================
// Reduced schema column enumerations (TPCC_SMALL mode)
// Drops varchar address/name fields to reduce row size for quick testing
// ======================================================================
namespace small {

enum WH : int {
    W_ID = 0, W_TAX, W_YTD, W_NUM_COLS
};

enum DIST : int {
    D_ID = 0, D_W_ID, D_TAX, D_YTD, D_NEXT_O_ID, D_NUM_COLS
};

enum CUST : int {
    C_ID = 0, C_D_ID, C_W_ID, C_MIDDLE, C_LAST,
    C_STATE, C_CREDIT, C_DISCOUNT, C_BALANCE,
    C_YTD_PAYMENT, C_PAYMENT_CNT, C_NUM_COLS
};

enum HIST : int {
    H_C_ID = 0, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_NUM_COLS
};

enum NORD : int {
    NO_O_ID = 0, NO_D_ID, NO_W_ID, NO_NUM_COLS
};

enum ORD : int {
    O_ID = 0, O_C_ID, O_D_ID, O_W_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_NUM_COLS
};

enum ORDL : int {
    OL_O_ID = 0, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_NUM_COLS
};

enum ITEM : int {
    I_ID = 0, I_IM_ID, I_NAME, I_PRICE, I_DATA, I_NUM_COLS
};

enum STK : int {
    S_I_ID = 0, S_W_ID, S_QUANTITY, S_REMOTE_CNT, S_NUM_COLS
};

}  // namespace small
}  // namespace oro::tpcc
#endif  // ORO_TPCC_CONFIG_H
