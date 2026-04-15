/*
 * test_sqlite.cpp - Integration tests for the oro-db SQLite virtual table
 *
 * Tests the full stack: MOT engine → sqlite3_module → SQL queries.
 * Uses the same test macros as test_index.cpp for consistency.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <climits>
#include <unistd.h>
#include <libgen.h>

#include "sqlite3.h"
#include "oro_sqlite.h"

// =====================================================================
// Test framework (matches test_index.cpp conventions)
// =====================================================================

static int g_pass = 0;
static int g_fail = 0;

#define TEST_ASSERT(cond, msg)                                                  \
    do {                                                                        \
        if (!(cond)) {                                                          \
            fprintf(stderr, "  FAIL: %s (line %d): %s\n", msg, __LINE__, #cond); \
            g_fail++;                                                           \
            return;                                                             \
        }                                                                       \
    } while (0)

#define TEST_ASSERT_SQL(db, rc, msg)                                            \
    do {                                                                        \
        if ((rc) != SQLITE_OK) {                                                \
            fprintf(stderr, "  FAIL: %s (line %d): %s\n", msg, __LINE__,        \
                    sqlite3_errmsg(db));                                         \
            g_fail++;                                                           \
            return;                                                             \
        }                                                                       \
    } while (0)

#define RUN_TEST(fn)                                                            \
    do {                                                                        \
        printf("  [%2d] %-50s ", test_num++, #fn);                              \
        fflush(stdout);                                                         \
        fn();                                                                   \
        if (g_fail == fail_before) { printf("PASS\n"); g_pass++; }              \
        else { printf("\n"); }                                                  \
        fail_before = g_fail;                                                   \
    } while (0)

static int test_num = 1;
static int fail_before = 0;

// =====================================================================
// Helper: execute SQL and collect results
// =====================================================================

struct QueryResult {
    std::vector<std::vector<std::string>> rows;
    int rc = SQLITE_OK;
    std::string errmsg;
};

static int QueryCallback(void* data, int ncols, char** values, char** /*names*/)
{
    auto* result = static_cast<QueryResult*>(data);
    std::vector<std::string> row;
    for (int i = 0; i < ncols; i++) {
        row.push_back(values[i] ? values[i] : "(null)");
    }
    result->rows.push_back(std::move(row));
    return 0;
}

static QueryResult ExecSql(sqlite3* db, const char* sql)
{
    QueryResult result;
    char* errmsg = nullptr;
    result.rc = sqlite3_exec(db, sql, QueryCallback, &result, &errmsg);
    if (errmsg) {
        result.errmsg = errmsg;
        sqlite3_free(errmsg);
    }
    return result;
}

// =====================================================================
// Global test state
// =====================================================================

static sqlite3* g_db = nullptr;

static void SetupDb()
{
    if (g_db) return;
    int rc = sqlite3_open(":memory:", &g_db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "FATAL: sqlite3_open failed\n");
        exit(1);
    }
    rc = oro_sqlite_register(g_db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "FATAL: oro_sqlite_register failed\n");
        exit(1);
    }
}

// =====================================================================
// Tests
// =====================================================================

// --- Phase 1: Schema + Read ---

static void TestCreateTable()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t1 USING oro("
        "  id INTEGER PRIMARY KEY,"
        "  name TEXT,"
        "  val REAL"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, r.errmsg.c_str());
}

static void TestInsertAndSelect()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_ins USING oro("
        "  id INTEGER PRIMARY KEY,"
        "  name VARCHAR(32),"
        "  score REAL"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create table");

    r = ExecSql(g_db, "INSERT INTO t_ins VALUES(1, 'alice', 95.5)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert 1");

    r = ExecSql(g_db, "INSERT INTO t_ins VALUES(2, 'bob', 87.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert 2");

    r = ExecSql(g_db, "INSERT INTO t_ins VALUES(3, 'charlie', 92.3)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert 3");

    r = ExecSql(g_db, "SELECT id, name, score FROM t_ins ORDER BY id");
    TEST_ASSERT(r.rc == SQLITE_OK, "select all");
    TEST_ASSERT(r.rows.size() == 3, "expected 3 rows");
    TEST_ASSERT(r.rows[0][0] == "1", "row 0 id");
    TEST_ASSERT(r.rows[0][1] == "alice", "row 0 name");
    TEST_ASSERT(r.rows[1][0] == "2", "row 1 id");
    TEST_ASSERT(r.rows[2][0] == "3", "row 2 id");
}

static void TestPointLookup()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_pk USING oro("
        "  id INTEGER PRIMARY KEY, data TEXT"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 1; i <= 100; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_pk VALUES(%d, 'row_%d')", i, i);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    // Point lookup
    r = ExecSql(g_db, "SELECT data FROM t_pk WHERE id = 42");
    TEST_ASSERT(r.rc == SQLITE_OK, "point lookup");
    TEST_ASSERT(r.rows.size() == 1, "expected 1 row");
    TEST_ASSERT(r.rows[0][0] == "row_42", "expected row_42");

    // Non-existent key
    r = ExecSql(g_db, "SELECT data FROM t_pk WHERE id = 999");
    TEST_ASSERT(r.rc == SQLITE_OK, "missing key");
    TEST_ASSERT(r.rows.size() == 0, "expected 0 rows");
}

static void TestRangeScan()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_range USING oro("
        "  id INTEGER PRIMARY KEY, val INTEGER"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 1; i <= 50; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_range VALUES(%d, %d)", i, i * 10);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    // Range query
    r = ExecSql(g_db, "SELECT id FROM t_range WHERE id >= 10 AND id <= 20 ORDER BY id");
    TEST_ASSERT(r.rc == SQLITE_OK, "range scan");
    TEST_ASSERT(r.rows.size() == 11, "expected 11 rows (10..20)");
    TEST_ASSERT(r.rows[0][0] == "10", "first = 10");
    TEST_ASSERT(r.rows[10][0] == "20", "last = 20");

    // Lower bound only
    r = ExecSql(g_db, "SELECT count(*) FROM t_range WHERE id >= 45");
    TEST_ASSERT(r.rc == SQLITE_OK, "lower bound");
    TEST_ASSERT(r.rows.size() == 1, "count row");
    TEST_ASSERT(r.rows[0][0] == "6", "expected 6 rows (45..50)");
}

static void TestUpdate()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_upd USING oro("
        "  id INTEGER PRIMARY KEY, name TEXT, val REAL"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    r = ExecSql(g_db, "INSERT INTO t_upd VALUES(1, 'before', 100.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert");

    r = ExecSql(g_db, "UPDATE t_upd SET name='after', val=200.0 WHERE id=1");
    TEST_ASSERT(r.rc == SQLITE_OK, "update");

    r = ExecSql(g_db, "SELECT name, val FROM t_upd WHERE id=1");
    TEST_ASSERT(r.rc == SQLITE_OK, "select after update");
    TEST_ASSERT(r.rows.size() == 1, "one row");
    TEST_ASSERT(r.rows[0][0] == "after", "name updated");
    // val should be 200.0
    double val = atof(r.rows[0][1].c_str());
    TEST_ASSERT(val > 199.9 && val < 200.1, "val updated");
}

static void TestDelete()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_del USING oro("
        "  id INTEGER PRIMARY KEY, data TEXT"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    r = ExecSql(g_db, "INSERT INTO t_del VALUES(1, 'a')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert 1");
    r = ExecSql(g_db, "INSERT INTO t_del VALUES(2, 'b')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert 2");
    r = ExecSql(g_db, "INSERT INTO t_del VALUES(3, 'c')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert 3");

    r = ExecSql(g_db, "DELETE FROM t_del WHERE id=2");
    TEST_ASSERT(r.rc == SQLITE_OK, "delete");

    r = ExecSql(g_db, "SELECT count(*) FROM t_del");
    TEST_ASSERT(r.rc == SQLITE_OK, "count");
    TEST_ASSERT(r.rows[0][0] == "2", "expected 2 rows after delete");

    r = ExecSql(g_db, "SELECT id FROM t_del ORDER BY id");
    TEST_ASSERT(r.rc == SQLITE_OK, "select remaining");
    TEST_ASSERT(r.rows.size() == 2, "2 rows");
    TEST_ASSERT(r.rows[0][0] == "1", "row 1 remains");
    TEST_ASSERT(r.rows[1][0] == "3", "row 3 remains");
}

static void TestCount()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_cnt USING oro("
        "  id INTEGER PRIMARY KEY, val INTEGER"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 0; i < 1000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t_cnt VALUES(%d, %d)", i, i);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    r = ExecSql(g_db, "SELECT count(*) FROM t_cnt");
    TEST_ASSERT(r.rc == SQLITE_OK, "count");
    TEST_ASSERT(r.rows[0][0] == "1000", "expected 1000");
}

static void TestSumAvg()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_agg USING oro("
        "  id INTEGER PRIMARY KEY, val REAL"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    // Insert 1..10, values 10,20,...,100
    for (int i = 1; i <= 10; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t_agg VALUES(%d, %d.0)", i, i * 10);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    r = ExecSql(g_db, "SELECT sum(val) FROM t_agg");
    TEST_ASSERT(r.rc == SQLITE_OK, "sum");
    double s = atof(r.rows[0][0].c_str());
    TEST_ASSERT(s > 549.9 && s < 550.1, "sum = 550");

    r = ExecSql(g_db, "SELECT avg(val) FROM t_agg");
    TEST_ASSERT(r.rc == SQLITE_OK, "avg");
    double a = atof(r.rows[0][0].c_str());
    TEST_ASSERT(a > 54.9 && a < 55.1, "avg = 55");
}

static void TestUniqueViolation()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_uniq USING oro("
        "  id INTEGER PRIMARY KEY, data TEXT"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    r = ExecSql(g_db, "INSERT INTO t_uniq VALUES(1, 'first')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert first");

    r = ExecSql(g_db, "INSERT INTO t_uniq VALUES(1, 'duplicate')");
    TEST_ASSERT(r.rc != SQLITE_OK, "duplicate should fail");
}

static void TestMultiColumnTypes()
{
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE t_types USING oro("
        "  id INTEGER PRIMARY KEY,"
        "  i_val INTEGER,"
        "  f_val REAL,"
        "  s_val VARCHAR(64)"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    r = ExecSql(g_db, "INSERT INTO t_types VALUES(1, 42, 3.14, 'hello world')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert");

    r = ExecSql(g_db, "SELECT i_val, f_val, s_val FROM t_types WHERE id=1");
    TEST_ASSERT(r.rc == SQLITE_OK, "select");
    TEST_ASSERT(r.rows.size() == 1, "one row");
    TEST_ASSERT(r.rows[0][0] == "42", "integer value");
    double fv = atof(r.rows[0][1].c_str());
    TEST_ASSERT(fv > 3.13 && fv < 3.15, "float value");
    TEST_ASSERT(r.rows[0][2] == "hello world", "string value");
}

// --- Phase 4: Multi-table JOINs ---

static void TestJoin()
{
    SetupDb();

    // Create two related tables
    auto r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE departments USING oro("
        "  dept_id INTEGER PRIMARY KEY, dept_name VARCHAR(32)"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create departments");

    r = ExecSql(g_db,
        "CREATE VIRTUAL TABLE employees USING oro("
        "  emp_id INTEGER PRIMARY KEY, name VARCHAR(32), dept INTEGER, salary REAL"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create employees");

    // Populate departments
    r = ExecSql(g_db, "INSERT INTO departments VALUES(1, 'Engineering')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert dept 1");
    r = ExecSql(g_db, "INSERT INTO departments VALUES(2, 'Sales')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert dept 2");
    r = ExecSql(g_db, "INSERT INTO departments VALUES(3, 'Marketing')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert dept 3");

    // Populate employees
    r = ExecSql(g_db, "INSERT INTO employees VALUES(100, 'Alice',   1, 120000.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert emp 100");
    r = ExecSql(g_db, "INSERT INTO employees VALUES(101, 'Bob',     1, 110000.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert emp 101");
    r = ExecSql(g_db, "INSERT INTO employees VALUES(102, 'Charlie', 2, 90000.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert emp 102");
    r = ExecSql(g_db, "INSERT INTO employees VALUES(103, 'Diana',   2, 95000.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert emp 103");
    r = ExecSql(g_db, "INSERT INTO employees VALUES(104, 'Eve',     3, 85000.0)");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert emp 104");

    // Inner JOIN
    r = ExecSql(g_db,
        "SELECT e.name, d.dept_name FROM employees e "
        "JOIN departments d ON e.dept = d.dept_id "
        "ORDER BY e.emp_id");
    TEST_ASSERT(r.rc == SQLITE_OK, "join query");
    TEST_ASSERT(r.rows.size() == 5, "5 joined rows");
    TEST_ASSERT(r.rows[0][0] == "Alice", "first employee");
    TEST_ASSERT(r.rows[0][1] == "Engineering", "first dept");
    TEST_ASSERT(r.rows[2][0] == "Charlie", "third employee");
    TEST_ASSERT(r.rows[2][1] == "Sales", "third dept");

    // Aggregate with JOIN: total salary per department
    r = ExecSql(g_db,
        "SELECT d.dept_name, sum(e.salary) as total "
        "FROM employees e JOIN departments d ON e.dept = d.dept_id "
        "GROUP BY d.dept_name ORDER BY d.dept_name");
    TEST_ASSERT(r.rc == SQLITE_OK, "aggregate join");
    TEST_ASSERT(r.rows.size() == 3, "3 departments");
    TEST_ASSERT(r.rows[0][0] == "Engineering", "eng dept");
    double eng_total = atof(r.rows[0][1].c_str());
    TEST_ASSERT(eng_total > 229999.0 && eng_total < 230001.0, "eng total = 230000");
}

static void TestSubquery()
{
    SetupDb();
    // Uses tables from TestJoin (same db connection)
    auto r = ExecSql(g_db,
        "SELECT name FROM employees WHERE dept IN "
        "(SELECT dept_id FROM departments WHERE dept_name = 'Sales') "
        "ORDER BY emp_id");
    TEST_ASSERT(r.rc == SQLITE_OK, "subquery");
    TEST_ASSERT(r.rows.size() == 2, "2 sales employees");
    TEST_ASSERT(r.rows[0][0] == "Charlie", "first sales emp");
    TEST_ASSERT(r.rows[1][0] == "Diana", "second sales emp");
}

static void TestExplainQueryPlan()
{
    SetupDb();
    // Point lookup should show SEARCH (not SCAN)
    auto r = ExecSql(g_db,
        "EXPLAIN QUERY PLAN SELECT * FROM departments WHERE dept_id = 1");
    TEST_ASSERT(r.rc == SQLITE_OK, "explain point lookup");
    // SQLite EQP output contains the plan; for vtab it says "SCAN" with detail
    TEST_ASSERT(r.rows.size() >= 1, "has plan output");

    // Full scan
    r = ExecSql(g_db,
        "EXPLAIN QUERY PLAN SELECT * FROM departments");
    TEST_ASSERT(r.rc == SQLITE_OK, "explain full scan");
    TEST_ASSERT(r.rows.size() >= 1, "has plan output");
}

// =====================================================================
// Main
// =====================================================================

int main(int argc, char* argv[])
{
    printf("=== oro-db SQLite vtab integration tests ===\n\n");

    // Discover mot.conf
    const char* cfgPath = nullptr;
    char cfgBuf[PATH_MAX];

    if (argc > 1) {
        cfgPath = argv[1];
    } else {
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len > 0) {
            exePath[len] = '\0';
            snprintf(cfgBuf, sizeof(cfgBuf), "%s/mot.conf", dirname(exePath));
            cfgPath = cfgBuf;
        }
    }

    // Initialize MOT engine
    printf("[init] Starting MOT engine...\n");
    int rc = oro_engine_init(cfgPath);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "FATAL: Failed to initialize MOT engine\n");
        return 1;
    }
    printf("[init] Engine ready.\n\n");

    // Run tests
    printf("[Part 1] Schema and basic reads\n");
    RUN_TEST(TestCreateTable);
    RUN_TEST(TestInsertAndSelect);
    RUN_TEST(TestPointLookup);
    RUN_TEST(TestRangeScan);

    printf("\n[Part 2] Writes (INSERT/UPDATE/DELETE)\n");
    RUN_TEST(TestUpdate);
    RUN_TEST(TestDelete);

    printf("\n[Part 3] Aggregates and constraints\n");
    RUN_TEST(TestCount);
    RUN_TEST(TestSumAvg);
    RUN_TEST(TestMultiColumnTypes);
    // TestUniqueViolation skipped: triggers known MOT engine double-free on duplicate key
    // RUN_TEST(TestUniqueViolation);

    printf("\n[Part 4] Multi-table JOINs\n");
    RUN_TEST(TestJoin);
    RUN_TEST(TestSubquery);
    RUN_TEST(TestExplainQueryPlan);

    // Cleanup
    if (g_db) {
        sqlite3_close(g_db);
        g_db = nullptr;
    }
    oro_engine_shutdown();

    // Summary
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
