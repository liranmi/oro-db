/*
 * oro_server.cpp - PostgreSQL wire protocol server for oro-db
 *
 * Listens on a TCP port and translates PostgreSQL v3 simple-query protocol
 * into sqlite3_exec calls against the MOT+SQLite engine. Any PostgreSQL
 * client (psql, pgAdmin, DBeaver, DataGrip) can connect.
 *
 * Supports:
 *   - Simple query protocol (Q message → RowDescription + DataRow* + CommandComplete)
 *   - Multiple statements per query string (semicolon-separated)
 *   - NULL values, all SQLite types (sent as text)
 *   - Basic error reporting via ErrorResponse
 *   - Concurrent connections (one thread per client)
 *
 * Usage:
 *   ./oro_server                          # listen on :5432, in-memory DB
 *   ./oro_server --port 5433              # custom port
 *   ./oro_server --db /path/to/file.db    # persistent SQLite file
 *   ./oro_server --mot-config mot.conf    # explicit MOT config
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <climits>
#include <unistd.h>
#include <libgen.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "sqlite3.h"
#include "oro_mot_adapter.h"

// =====================================================================
// PG wire protocol helpers
// =====================================================================

// Write buffer that accumulates a message before sending
struct PgMsg {
    std::vector<uint8_t> buf;

    void clear() { buf.clear(); }

    void putByte(uint8_t b) { buf.push_back(b); }

    void putInt16(int16_t v) {
        uint16_t n = htons((uint16_t)v);
        buf.insert(buf.end(), (uint8_t*)&n, (uint8_t*)&n + 2);
    }

    void putInt32(int32_t v) {
        uint32_t n = htonl((uint32_t)v);
        buf.insert(buf.end(), (uint8_t*)&n, (uint8_t*)&n + 4);
    }

    void putString(const char* s) {
        size_t len = strlen(s) + 1;  // include null terminator
        buf.insert(buf.end(), (const uint8_t*)s, (const uint8_t*)s + len);
    }

    void putBytes(const void* data, size_t len) {
        buf.insert(buf.end(), (const uint8_t*)data, (const uint8_t*)data + len);
    }

    // Finalize: write type byte + length prefix, then send
    bool send(int fd, char type) {
        uint32_t bodyLen = (uint32_t)buf.size() + 4;  // length includes itself
        uint32_t netLen = htonl(bodyLen);
        uint8_t hdr[5];
        hdr[0] = (uint8_t)type;
        memcpy(hdr + 1, &netLen, 4);

        if (write(fd, hdr, 5) != 5) return false;
        if (!buf.empty()) {
            size_t total = 0;
            while (total < buf.size()) {
                ssize_t n = write(fd, buf.data() + total, buf.size() - total);
                if (n <= 0) return false;
                total += n;
            }
        }
        return true;
    }
};

// Read exactly n bytes from fd
static bool readFull(int fd, void* buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, (uint8_t*)buf + total, n - total);
        if (r <= 0) return false;
        total += r;
    }
    return true;
}

static int32_t readInt32(int fd) {
    uint32_t v;
    if (!readFull(fd, &v, 4)) return -1;
    return (int32_t)ntohl(v);
}

// =====================================================================
// PG protocol messages
// =====================================================================

static bool sendAuthOk(int fd) {
    PgMsg m;
    m.putInt32(0);  // auth ok
    return m.send(fd, 'R');
}

static bool sendParameterStatus(int fd, const char* name, const char* value) {
    PgMsg m;
    m.putString(name);
    m.putString(value);
    return m.send(fd, 'S');
}

static bool sendBackendKeyData(int fd, int32_t pid, int32_t secret) {
    PgMsg m;
    m.putInt32(pid);
    m.putInt32(secret);
    return m.send(fd, 'K');
}

static bool sendReadyForQuery(int fd, char status) {
    PgMsg m;
    m.putByte((uint8_t)status);
    return m.send(fd, 'Z');
}

static bool sendErrorResponse(int fd, const char* severity,
                              const char* code, const char* message) {
    PgMsg m;
    m.putByte('S'); m.putString(severity);
    m.putByte('V'); m.putString(severity);
    m.putByte('C'); m.putString(code);
    m.putByte('M'); m.putString(message);
    m.putByte(0);  // terminator
    return m.send(fd, 'E');
}

static bool sendRowDescription(int fd, int ncols, const char** names) {
    PgMsg m;
    m.putInt16((int16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        m.putString(names[i]);
        m.putInt32(0);       // table OID
        m.putInt16(0);       // column attribute number
        m.putInt32(25);      // type OID: text
        m.putInt16(-1);      // type size: variable
        m.putInt32(-1);      // type modifier
        m.putInt16(0);       // format: text
    }
    return m.send(fd, 'T');
}

static bool sendDataRow(int fd, int ncols, const char** values) {
    PgMsg m;
    m.putInt16((int16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        if (values[i] == nullptr) {
            m.putInt32(-1);  // NULL
        } else {
            int32_t len = (int32_t)strlen(values[i]);
            m.putInt32(len);
            m.putBytes(values[i], len);
        }
    }
    return m.send(fd, 'D');
}

static bool sendCommandComplete(int fd, const char* tag) {
    PgMsg m;
    m.putString(tag);
    return m.send(fd, 'C');
}

static bool sendEmptyQuery(int fd) {
    PgMsg m;
    return m.send(fd, 'I');
}

// =====================================================================
// Query execution
// =====================================================================

struct QueryState {
    int fd;
    int ncols;
    int nrows;
    bool header_sent;
    bool error;
};

static int queryCallback(void* data, int ncols, char** values, char** names) {
    auto* qs = (QueryState*)data;
    if (qs->error) return 1;

    // Send RowDescription on first row
    if (!qs->header_sent) {
        qs->ncols = ncols;
        if (!sendRowDescription(qs->fd, ncols, (const char**)names)) {
            qs->error = true;
            return 1;
        }
        qs->header_sent = true;
    }

    // Send DataRow
    if (!sendDataRow(qs->fd, ncols, (const char**)values)) {
        qs->error = true;
        return 1;
    }
    qs->nrows++;
    return 0;
}

static void handleQuery(int fd, sqlite3* db, const char* sql) {
    // Skip empty queries
    const char* p = sql;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p == '\0') {
        sendEmptyQuery(fd);
        sendReadyForQuery(fd, 'I');
        return;
    }

    QueryState qs = {fd, 0, 0, false, false};
    char* errmsg = nullptr;
    int rc = sqlite3_exec(db, sql, queryCallback, &qs, &errmsg);

    if (rc != SQLITE_OK) {
        const char* msg = errmsg ? errmsg : "unknown error";
        sendErrorResponse(fd, "ERROR", "42000", msg);
        if (errmsg) sqlite3_free(errmsg);
    } else {
        // If no rows were returned (DDL/DML), send appropriate tag
        if (!qs.header_sent) {
            // Determine command type for the tag
            const char* tag = "OK";
            if (strncasecmp(p, "INSERT", 6) == 0) {
                tag = "INSERT 0 1";
            } else if (strncasecmp(p, "UPDATE", 6) == 0) {
                char buf[64];
                snprintf(buf, sizeof(buf), "UPDATE %d", sqlite3_changes(db));
                sendCommandComplete(fd, buf);
                sendReadyForQuery(fd, 'I');
                return;
            } else if (strncasecmp(p, "DELETE", 6) == 0) {
                char buf[64];
                snprintf(buf, sizeof(buf), "DELETE %d", sqlite3_changes(db));
                sendCommandComplete(fd, buf);
                sendReadyForQuery(fd, 'I');
                return;
            } else if (strncasecmp(p, "CREATE", 6) == 0) {
                tag = "CREATE TABLE";
            } else if (strncasecmp(p, "DROP", 4) == 0) {
                tag = "DROP TABLE";
            } else if (strncasecmp(p, "BEGIN", 5) == 0) {
                tag = "BEGIN";
            } else if (strncasecmp(p, "COMMIT", 6) == 0) {
                tag = "COMMIT";
            } else if (strncasecmp(p, "ROLLBACK", 8) == 0) {
                tag = "ROLLBACK";
            }
            sendCommandComplete(fd, tag);
        } else {
            char buf[64];
            snprintf(buf, sizeof(buf), "SELECT %d", qs.nrows);
            sendCommandComplete(fd, buf);
        }
    }

    sendReadyForQuery(fd, 'I');
}

// =====================================================================
// Client connection handler
// =====================================================================

static void handleClient(int fd, const char* dbPath) {
    // --- Startup phase ---
    // Read startup message (no type byte): [4:length][4:version][params...]
    int32_t msgLen = readInt32(fd);
    if (msgLen < 8 || msgLen > 10000) { close(fd); return; }

    std::vector<uint8_t> buf(msgLen - 4);
    if (!readFull(fd, buf.data(), buf.size())) { close(fd); return; }

    // Check protocol version
    uint32_t version;
    memcpy(&version, buf.data(), 4);
    version = ntohl(version);

    // Handle SSL request (80877103 = 1234.5679)
    if (version == 80877103) {
        // Reject SSL: send 'N'
        char n = 'N';
        write(fd, &n, 1);
        // Client will retry with normal startup
        msgLen = readInt32(fd);
        if (msgLen < 8 || msgLen > 10000) { close(fd); return; }
        buf.resize(msgLen - 4);
        if (!readFull(fd, buf.data(), buf.size())) { close(fd); return; }
        memcpy(&version, buf.data(), 4);
        version = ntohl(version);
    }

    if ((version >> 16) != 3) {
        sendErrorResponse(fd, "FATAL", "08004", "unsupported protocol version");
        close(fd);
        return;
    }

    // Parse startup parameters (just log them)
    // params start at offset 4, null-terminated key-value pairs
    std::string user = "unknown";
    size_t pos = 4;
    while (pos < buf.size() && buf[pos] != 0) {
        const char* key = (const char*)&buf[pos];
        pos += strlen(key) + 1;
        if (pos >= buf.size()) break;
        const char* val = (const char*)&buf[pos];
        pos += strlen(val) + 1;
        if (strcmp(key, "user") == 0) user = val;
    }

    fprintf(stderr, "[oro-server] client connected: user=%s\n", user.c_str());

    // Send auth OK + startup parameters
    sendAuthOk(fd);
    sendParameterStatus(fd, "server_version", "15.0 (oro-db MOT)");
    sendParameterStatus(fd, "server_encoding", "UTF8");
    sendParameterStatus(fd, "client_encoding", "UTF8");
    sendParameterStatus(fd, "DateStyle", "ISO, MDY");
    sendParameterStatus(fd, "integer_datetimes", "on");
    sendBackendKeyData(fd, getpid(), 0);
    sendReadyForQuery(fd, 'I');

    // --- Open a per-connection SQLite database ---
    sqlite3* db = nullptr;
    int rc = sqlite3_open(dbPath, &db);
    if (rc != SQLITE_OK) {
        sendErrorResponse(fd, "FATAL", "08001", "failed to open database");
        close(fd);
        return;
    }

    // Enable FK enforcement
    sqlite3_exec(db, "PRAGMA foreign_keys = ON", nullptr, nullptr, nullptr);

    // --- Query loop ---
    while (true) {
        // Read message: [1:type][4:length][payload]
        uint8_t msgType;
        if (!readFull(fd, &msgType, 1)) break;

        int32_t bodyLen = readInt32(fd);
        if (bodyLen < 4) break;

        int32_t payloadLen = bodyLen - 4;
        std::vector<char> payload(payloadLen + 1);
        if (payloadLen > 0) {
            if (!readFull(fd, payload.data(), payloadLen)) break;
        }
        payload[payloadLen] = '\0';

        switch (msgType) {
            case 'Q':
                // Simple query
                handleQuery(fd, db, payload.data());
                break;

            case 'X':
                // Terminate
                goto done;

            case 'p':
                // Password message (we don't require auth)
                sendAuthOk(fd);
                sendReadyForQuery(fd, 'I');
                break;

            default:
                // Unknown message — send error but keep connection
                sendErrorResponse(fd, "ERROR", "0A000",
                    "unsupported message type");
                sendReadyForQuery(fd, 'I');
                break;
        }
    }

done:
    fprintf(stderr, "[oro-server] client disconnected: user=%s\n", user.c_str());
    sqlite3_close(db);
    close(fd);
}

// =====================================================================
// Main
// =====================================================================

static volatile sig_atomic_t g_running = 1;

static void sigHandler(int) { g_running = 0; }

int main(int argc, char* argv[]) {
    int port = 5432;
    const char* dbPath = ":memory:";
    const char* motConfig = nullptr;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
            dbPath = argv[++i];
        } else if (strcmp(argv[i], "--mot-config") == 0 && i + 1 < argc) {
            motConfig = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                "Usage: %s [options]\n"
                "  --port N          Listen port (default: 5432)\n"
                "  --db PATH         SQLite database file (default: :memory:)\n"
                "  --mot-config PATH MOT engine config file\n"
                "\nConnect with: psql -h localhost -p N\n", argv[0]);
            return 0;
        }
    }

    // Auto-detect mot.conf
    char cfgBuf[PATH_MAX];
    if (!motConfig) {
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len > 0) {
            exePath[len] = '\0';
            snprintf(cfgBuf, sizeof(cfgBuf), "%s/mot.conf", dirname(exePath));
            motConfig = cfgBuf;
        }
    }

    // Initialize MOT engine
    if (oroMotInit(motConfig) != 0) {
        fprintf(stderr, "FATAL: failed to initialize MOT engine\n");
        return 1;
    }

    // Create listening socket
    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(listenFd);
        return 1;
    }

    if (listen(listenFd, 8) < 0) {
        perror("listen");
        close(listenFd);
        return 1;
    }

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    signal(SIGPIPE, SIG_IGN);

    fprintf(stderr, "oro-db server listening on port %d\n", port);
    fprintf(stderr, "Connect with: psql -h localhost -p %d\n\n", port);

    while (g_running) {
        struct sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = accept(listenFd, (struct sockaddr*)&clientAddr, &clientLen);
        if (clientFd < 0) {
            if (g_running) perror("accept");
            continue;
        }

        // Disable Nagle for snappy responses
        int flag = 1;
        setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // One thread per connection
        std::thread([clientFd, dbPath]() {
            handleClient(clientFd, dbPath);
        }).detach();
    }

    fprintf(stderr, "\nShutting down...\n");
    close(listenFd);
    oroMotShutdown();
    return 0;
}
