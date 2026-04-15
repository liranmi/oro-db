/*
 * oro_shell.cpp - Interactive SQL shell backed by oro-db
 *
 * Thin wrapper around SQLite's shell.c that:
 *   1. Boots the MOT engine
 *   2. Registers CREATE MOT TABLE support (built into the patched SQLite)
 *   3. Delegates to the standard SQLite shell main()
 *
 * Usage:
 *   ./oro_shell                          # interactive mode, in-memory DB
 *   ./oro_shell /path/to/file.db         # persistent SQLite file (MOT tables still in-memory)
 *   ./oro_shell --mot-config /path/to/mot.conf  # explicit MOT config
 *   echo "SELECT ..." | ./oro_shell      # pipe mode
 */

#include <cstdio>
#include <cstring>
#include <climits>
#include <unistd.h>
#include <libgen.h>

#include "sqlite3.h"
#include "oro_mot_adapter.h"

// The SQLite shell defines its own main(). We rename it via preprocessor
// (-Dmain=sqlite3_shell_main) so we can wrap it.
extern "C" int sqlite3_shell_main(int argc, char** argv);

int main(int argc, char* argv[])
{
    // Extract --mot-config before passing rest to SQLite shell
    const char* mot_config = nullptr;
    int shell_argc = argc;
    char** shell_argv = argv;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mot-config") == 0 && i + 1 < argc) {
            mot_config = argv[i + 1];
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

    // Initialize MOT engine
    if (oroMotInit(mot_config) != 0) {
        fprintf(stderr, "oro_shell: failed to initialize MOT engine\n");
        if (mot_config)
            fprintf(stderr, "  config: %s\n", mot_config);
        return 1;
    }

    // Banner
    fprintf(stderr, "oro-db SQL shell (SQLite + in-memory MOT engine)\n");
    fprintf(stderr, "Create MOT tables with: CREATE MOT TABLE t (col TYPE, ...);\n\n");

    // Delegate to the SQLite shell
    int rc = sqlite3_shell_main(shell_argc, shell_argv);

    oroMotShutdown();
    return rc;
}
