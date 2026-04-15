/*
 * test_mot_engine.cpp - Integration tests for CREATE MOT TABLE
 *
 * Tests the full engine integration: SQLite parser → VDBE → MOT adapter.
 * Unlike test_sqlite.cpp (which tests virtual tables), this exercises the
 * native CREATE MOT TABLE syntax.
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
#include "oro_mot_adapter.h"

// =====================================================================
// Test framework
// =====================================================================

static int g_pass = 0;
static int g_fail = 0;
static int test_num = 1;
static int fail_before = 0;

#define TEST_ASSERT(cond, msg)                                                  \
    do {                                                                        \
        if (!(cond)) {                                                          \
            fprintf(stderr, "  FAIL: %s (line %d): %s\n", msg, __LINE__, #cond); \
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

// =====================================================================
// SQL execution helper
// =====================================================================

struct QueryResult {
    std::vector<std::vector<std::string>> rows;
    int rc = SQLITE_OK;
    std::string errmsg;
};

static int QueryCallback(void* data, int ncols, char** values, char** /*names*/) {
    auto* result = static_cast<QueryResult*>(data);
    std::vector<std::string> row;
    for (int i = 0; i < ncols; i++) {
        row.push_back(values[i] ? values[i] : "(null)");
    }
    result->rows.push_back(std::move(row));
    return 0;
}

static QueryResult ExecSql(sqlite3* db, const char* sql) {
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
// Global test database
// =====================================================================

static sqlite3* g_db = nullptr;

static void SetupDb() {
    if (g_db) return;
    int rc = sqlite3_open(":memory:", &g_db);
    if (rc != SQLITE_OK) { fprintf(stderr, "sqlite3_open failed\n"); exit(1); }
}

// =====================================================================
// Tests
// =====================================================================

// --- Basic DDL and DML ---

static void TestCreateMotTable() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_basic (id INTEGER PRIMARY KEY, name TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, r.errmsg.c_str());

    // Verify schema shows CREATE MOT TABLE
    r = ExecSql(g_db,
        "SELECT sql FROM sqlite_schema WHERE name = 't_basic'");
    TEST_ASSERT(r.rc == SQLITE_OK, "schema query");
    TEST_ASSERT(r.rows.size() == 1, "one schema row");
    TEST_ASSERT(r.rows[0][0].find("CREATE MOT TABLE") != std::string::npos,
                "schema preserved");
}

static void TestInsertSelect() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_ins (id INTEGER PRIMARY KEY, val TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 1; i <= 5; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t_ins VALUES(%d, 'row%d')", i, i);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    r = ExecSql(g_db, "SELECT count(*) FROM t_ins");
    TEST_ASSERT(r.rc == SQLITE_OK, "count");
    TEST_ASSERT(r.rows[0][0] == "5", "count = 5");

    r = ExecSql(g_db, "SELECT * FROM t_ins ORDER BY id");
    TEST_ASSERT(r.rc == SQLITE_OK, "select all");
    TEST_ASSERT(r.rows.size() == 5, "5 rows");
    TEST_ASSERT(r.rows[0][0] == "1", "row 0 id");
    TEST_ASSERT(r.rows[0][1] == "row1", "row 0 val");
    TEST_ASSERT(r.rows[4][0] == "5", "row 4 id");
}

static void TestPointLookup() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_pk (id INTEGER PRIMARY KEY, data TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 1; i <= 100; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_pk VALUES(%d, 'data_%d')", i, i);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    // Point lookup should use OP_SeekRowid
    r = ExecSql(g_db, "SELECT data FROM t_pk WHERE id = 42");
    TEST_ASSERT(r.rc == SQLITE_OK, "point lookup");
    TEST_ASSERT(r.rows.size() == 1, "one row");
    TEST_ASSERT(r.rows[0][0] == "data_42", "correct row");

    r = ExecSql(g_db, "SELECT data FROM t_pk WHERE id = 999");
    TEST_ASSERT(r.rc == SQLITE_OK, "missing key");
    TEST_ASSERT(r.rows.size() == 0, "no rows");
}

static void TestDelete() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_del (id INTEGER PRIMARY KEY, val INTEGER)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 1; i <= 10; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t_del VALUES(%d, %d)", i, i * 10);
        r = ExecSql(g_db, sql);
        TEST_ASSERT(r.rc == SQLITE_OK, "insert");
    }

    r = ExecSql(g_db, "DELETE FROM t_del WHERE id = 5");
    TEST_ASSERT(r.rc == SQLITE_OK, "delete one");

    r = ExecSql(g_db, "SELECT count(*) FROM t_del");
    TEST_ASSERT(r.rows[0][0] == "9", "count after delete = 9");

    // Delete all with id > 5
    r = ExecSql(g_db, "DELETE FROM t_del WHERE id > 5");
    TEST_ASSERT(r.rc == SQLITE_OK, "delete range");

    r = ExecSql(g_db, "SELECT count(*) FROM t_del");
    TEST_ASSERT(r.rows[0][0] == "4", "count after range delete = 4");
}

static void TestUpdate() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_upd (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    r = ExecSql(g_db, "INSERT INTO t_upd VALUES(1, 100, 'before')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert");

    r = ExecSql(g_db, "UPDATE t_upd SET val = 200, name = 'after' WHERE id = 1");
    TEST_ASSERT(r.rc == SQLITE_OK, "update");

    r = ExecSql(g_db, "SELECT val, name FROM t_upd WHERE id = 1");
    TEST_ASSERT(r.rc == SQLITE_OK, "select");
    TEST_ASSERT(r.rows.size() == 1, "one row");
    TEST_ASSERT(r.rows[0][0] == "200", "val updated");
    TEST_ASSERT(r.rows[0][1] == "after", "name updated");
}

// --- Cross-engine queries ---

static void TestCrossEngineJoin() {
    SetupDb();

    // Native B-tree table
    auto r = ExecSql(g_db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create users");

    // MOT in-memory table
    r = ExecSql(g_db,
        "CREATE MOT TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create orders");

    ExecSql(g_db, "INSERT INTO users VALUES(1, 'Alice')");
    ExecSql(g_db, "INSERT INTO users VALUES(2, 'Bob')");
    ExecSql(g_db, "INSERT INTO orders VALUES(100, 1, 99.99)");
    ExecSql(g_db, "INSERT INTO orders VALUES(101, 1, 50.00)");
    ExecSql(g_db, "INSERT INTO orders VALUES(102, 2, 75.50)");

    // Cross-engine JOIN
    r = ExecSql(g_db,
        "SELECT u.name, o.total FROM users u "
        "JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id");
    TEST_ASSERT(r.rc == SQLITE_OK, "join");
    TEST_ASSERT(r.rows.size() == 3, "3 joined rows");
    TEST_ASSERT(r.rows[0][0] == "Alice", "first row");
    TEST_ASSERT(r.rows[2][0] == "Bob", "third row");

    // GROUP BY aggregate across engines
    r = ExecSql(g_db,
        "SELECT u.name, count(*), sum(o.total) FROM users u "
        "JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name");
    TEST_ASSERT(r.rc == SQLITE_OK, "group by");
    TEST_ASSERT(r.rows.size() == 2, "2 groups");
    TEST_ASSERT(r.rows[0][1] == "2", "Alice count");
    TEST_ASSERT(r.rows[1][1] == "1", "Bob count");
}

// --- Aggregates ---

static void TestAggregates() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_agg (id INTEGER PRIMARY KEY, val REAL)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    for (int i = 1; i <= 100; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t_agg VALUES(%d, %d.5)", i, i);
        ExecSql(g_db, sql);
    }

    r = ExecSql(g_db, "SELECT count(*), sum(val), avg(val), min(val), max(val) FROM t_agg");
    TEST_ASSERT(r.rc == SQLITE_OK, "agg");
    TEST_ASSERT(r.rows[0][0] == "100", "count = 100");
    double sum_val = atof(r.rows[0][1].c_str());
    TEST_ASSERT(sum_val > 5099.9 && sum_val < 5100.1, "sum = 5100");  // 1.5..100.5 sum
    double min_val = atof(r.rows[0][3].c_str());
    TEST_ASSERT(min_val > 1.4 && min_val < 1.6, "min = 1.5");
    double max_val = atof(r.rows[0][4].c_str());
    TEST_ASSERT(max_val > 100.4 && max_val < 100.6, "max = 100.5");
}

// --- Transactions ---

static void TestTransactionCommit() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_tx (id INTEGER PRIMARY KEY, val TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    // Autocommit inserts
    ExecSql(g_db, "INSERT INTO t_tx VALUES(1, 'one')");
    ExecSql(g_db, "INSERT INTO t_tx VALUES(2, 'two')");

    r = ExecSql(g_db, "SELECT count(*) FROM t_tx");
    TEST_ASSERT(r.rows[0][0] == "2", "2 rows after autocommit");
}

static void TestExplicitRollback() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_rb (id INTEGER PRIMARY KEY, val INTEGER)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    ExecSql(g_db, "INSERT INTO t_rb VALUES(1, 100)");

    // Explicit BEGIN..ROLLBACK should not persist changes
    ExecSql(g_db, "BEGIN");
    ExecSql(g_db, "INSERT INTO t_rb VALUES(2, 200)");
    ExecSql(g_db, "INSERT INTO t_rb VALUES(3, 300)");
    ExecSql(g_db, "ROLLBACK");

    r = ExecSql(g_db, "SELECT count(*) FROM t_rb");
    TEST_ASSERT(r.rows[0][0] == "1", "1 row after rollback");

    // Explicit BEGIN..COMMIT should persist
    ExecSql(g_db, "BEGIN");
    ExecSql(g_db, "INSERT INTO t_rb VALUES(4, 400)");
    ExecSql(g_db, "COMMIT");

    r = ExecSql(g_db, "SELECT count(*) FROM t_rb");
    TEST_ASSERT(r.rows[0][0] == "2", "2 rows after commit");
}

// --- Data types ---

static void TestDataTypes() {
    SetupDb();
    auto r = ExecSql(g_db,
        "CREATE MOT TABLE t_types ("
        "  id INTEGER PRIMARY KEY, "
        "  i INTEGER, "
        "  r REAL, "
        "  t TEXT, "
        "  b BLOB"
        ")");
    TEST_ASSERT(r.rc == SQLITE_OK, "create");

    r = ExecSql(g_db,
        "INSERT INTO t_types VALUES(1, 42, 3.14, 'hello', X'DEADBEEF')");
    TEST_ASSERT(r.rc == SQLITE_OK, "insert");

    r = ExecSql(g_db, "SELECT i, r, t, hex(b) FROM t_types WHERE id = 1");
    TEST_ASSERT(r.rc == SQLITE_OK, "select");
    TEST_ASSERT(r.rows.size() == 1, "one row");
    TEST_ASSERT(r.rows[0][0] == "42", "integer");
    double rv = atof(r.rows[0][1].c_str());
    TEST_ASSERT(rv > 3.13 && rv < 3.15, "real");
    TEST_ASSERT(r.rows[0][2] == "hello", "text");
    TEST_ASSERT(r.rows[0][3] == "DEADBEEF", "blob hex");
}

// --- Both engines coexist ---

static void TestDualEngine() {
    SetupDb();

    // Create one of each
    auto r = ExecSql(g_db, "CREATE TABLE nat_t (id INT PRIMARY KEY, v TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create native");

    r = ExecSql(g_db, "CREATE MOT TABLE mot_t (id INT PRIMARY KEY, v TEXT)");
    TEST_ASSERT(r.rc == SQLITE_OK, "create mot");

    // Populate both
    ExecSql(g_db, "INSERT INTO nat_t VALUES(1, 'native-1')");
    ExecSql(g_db, "INSERT INTO nat_t VALUES(2, 'native-2')");
    ExecSql(g_db, "INSERT INTO mot_t VALUES(10, 'mot-10')");
    ExecSql(g_db, "INSERT INTO mot_t VALUES(20, 'mot-20')");

    // Query both independently
    r = ExecSql(g_db, "SELECT count(*) FROM nat_t");
    TEST_ASSERT(r.rows[0][0] == "2", "native count");

    r = ExecSql(g_db, "SELECT count(*) FROM mot_t");
    TEST_ASSERT(r.rows[0][0] == "2", "mot count");

    // Verify schema shows correct types
    r = ExecSql(g_db,
        "SELECT name, sql FROM sqlite_schema "
        "WHERE name IN ('nat_t', 'mot_t') ORDER BY name");
    TEST_ASSERT(r.rows.size() == 2, "2 tables found");
    bool has_mot_ddl = false;
    bool has_nat_ddl = false;
    for (const auto& row : r.rows) {
        if (row[0] == "mot_t" && row[1].find("CREATE MOT TABLE") != std::string::npos)
            has_mot_ddl = true;
        if (row[0] == "nat_t" && row[1].find("CREATE TABLE") != std::string::npos
            && row[1].find("MOT TABLE") == std::string::npos)
            has_nat_ddl = true;
    }
    TEST_ASSERT(has_mot_ddl, "mot_t has CREATE MOT TABLE DDL");
    TEST_ASSERT(has_nat_ddl, "nat_t has plain CREATE TABLE DDL");
}

// =====================================================================
// Main
// =====================================================================

int main(int argc, char* argv[]) {
    printf("=== oro-db CREATE MOT TABLE engine integration tests ===\n\n");

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
    int rc = oroMotInit(cfgPath);
    if (rc != 0) {
        fprintf(stderr, "FATAL: oroMotInit failed\n");
        return 1;
    }
    printf("[init] Engine ready.\n\n");

    // Run tests
    printf("[Part 1] DDL and basic DML\n");
    RUN_TEST(TestCreateMotTable);
    RUN_TEST(TestInsertSelect);
    RUN_TEST(TestPointLookup);
    RUN_TEST(TestDelete);
    RUN_TEST(TestUpdate);

    printf("\n[Part 2] Cross-engine and aggregates\n");
    RUN_TEST(TestCrossEngineJoin);
    RUN_TEST(TestAggregates);
    RUN_TEST(TestDualEngine);

    printf("\n[Part 3] Transactions and types\n");
    RUN_TEST(TestTransactionCommit);
    RUN_TEST(TestExplicitRollback);
    RUN_TEST(TestDataTypes);

    // Cleanup
    if (g_db) {
        sqlite3_close(g_db);
        g_db = nullptr;
    }
    oroMotShutdown();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
