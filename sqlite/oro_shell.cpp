/*
 * oro_shell.cpp - Interactive SQL shell backed by oro-db
 *
 * This is a thin wrapper around SQLite's shell.c that:
 *   1. Boots the MOT engine
 *   2. Registers the "oro" virtual table module via sqlite3_auto_extension
 *   3. Delegates to the standard SQLite shell main()
 *
 * Usage:
 *   ./oro_shell                          # interactive mode
 *   ./oro_shell --mot-config /path/to/mot.conf   # explicit config
 *   echo "SELECT ..." | ./oro_shell      # pipe mode
 */

#include <cstdio>
#include <cstring>
#include <climits>
#include <unistd.h>
#include <libgen.h>

#include "sqlite3.h"
#include "oro_sqlite.h"
#include "oro_mot_adapter.h"

// Auto-extension callback: called for every new sqlite3 connection.
// Registers the "oro" vtab module automatically.
static int oro_auto_extension(sqlite3* db, const char** /*pzErrMsg*/,
                              const sqlite3_api_routines* /*pApi*/)
{
    return oro_sqlite_register(db);
}

// The SQLite shell defines its own main(). We rename it via preprocessor
// so we can wrap it.
extern "C" int sqlite3_shell_main(int argc, char** argv);

int main(int argc, char* argv[])
{
    // Check for --mot-config argument (must come before SQLite args)
    const char* mot_config = nullptr;
    int shell_argc = argc;
    char** shell_argv = argv;

    // Simple arg scan: extract --mot-config before passing rest to shell
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mot-config") == 0 && i + 1 < argc) {
            mot_config = argv[i + 1];
            // Remove these two args from the argv passed to shell
            // (shift remaining args down)
            for (int j = i; j + 2 < argc; j++) {
                argv[j] = argv[j + 2];
            }
            argc -= 2;
            shell_argc = argc;
            break;
        }
    }

    // Auto-detect mot.conf next to executable if not specified
    char cfgBuf[PATH_MAX];
    if (!mot_config) {
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len > 0) {
            exePath[len] = '\0';
            snprintf(cfgBuf, sizeof(cfgBuf), "%s/mot.conf", dirname(exePath));
            mot_config = cfgBuf;
        }
    }

    // Initialize MOT engine (vtab path — creates MOTEngine singleton)
    int rc = oro_engine_init(mot_config);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "oro_shell: failed to initialize MOT engine (vtab path)\n");
        if (mot_config)
            fprintf(stderr, "  config: %s\n", mot_config);
        return 1;
    }

    // Initialize MOT adapter (engine path — for CREATE MOT TABLE).
    // MOTEngine::CreateInstance is idempotent, so this just flips the adapter's
    // "initialized" flag and sets its g_engine pointer.
    if (oroMotInit(mot_config) != 0) {
        fprintf(stderr, "oro_shell: failed to initialize MOT adapter\n");
        oro_engine_shutdown();
        return 1;
    }

    // Register auto-extension so every new connection gets the "oro" vtab module
    rc = sqlite3_auto_extension((void(*)(void))oro_auto_extension);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "oro_shell: failed to register auto-extension\n");
        oro_engine_shutdown();
        return 1;
    }

    // Print a banner
    fprintf(stderr, "oro-db shell (SQLite + in-memory MOT engine)\n");
    fprintf(stderr, "Native: CREATE MOT TABLE t (col1 TYPE, ...);\n");
    fprintf(stderr, "Legacy: CREATE VIRTUAL TABLE t USING oro(col1 TYPE, ...);\n\n");

    // Delegate to the SQLite shell
    rc = sqlite3_shell_main(shell_argc, shell_argv);

    // Cleanup
    oroMotShutdown();
    oro_engine_shutdown();
    return rc;
}
