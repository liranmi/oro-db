/*
 * oro-db stub for libintl.h
 * Provides minimal gettext stubs. Must also provide bindtextdomain/textdomain
 * since the system C++ headers (messages_members.h) expect them.
 */
#ifndef LIBINTL_H_STUB
#define LIBINTL_H_STUB

#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

static inline char* gettext(const char* msgid) { return (char*)msgid; }
static inline char* dgettext(const char* domain, const char* msgid) { return (char*)msgid; }
static inline char* dcgettext(const char* domain, const char* msgid, int category) { return (char*)msgid; }
static inline char* ngettext(const char* msgid1, const char* msgid2, unsigned long n)
{
    return (char*)(n == 1 ? msgid1 : msgid2);
}
static inline char* bindtextdomain(const char* domain, const char* dir) { return (char*)dir; }
static inline char* textdomain(const char* domain) { return (char*)domain; }
static inline char* bind_textdomain_codeset(const char* domain, const char* codeset) { return (char*)codeset; }

#ifdef __cplusplus
}
#endif

#define _(String) (String)
#define N_(String) (String)

#endif /* LIBINTL_H_STUB */
