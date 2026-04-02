/*
 * oro-db stub for securec.h
 * Replaces Huawei's securec library with standard C equivalents.
 */
#ifndef SECUREC_H_STUB
#define SECUREC_H_STUB

#include <cstring>
#include <cstdio>
#include <cstdarg>
#include <cerrno>

typedef int errno_t;
#define EOK 0

static inline errno_t memcpy_s(void* dest, size_t destMax, const void* src, size_t count)
{
    if (dest == nullptr || src == nullptr || count > destMax) {
        return EINVAL;
    }
    memcpy(dest, src, count);
    return EOK;
}

static inline errno_t memmove_s(void* dest, size_t destMax, const void* src, size_t count)
{
    if (dest == nullptr || src == nullptr || count > destMax) {
        return EINVAL;
    }
    memmove(dest, src, count);
    return EOK;
}

static inline errno_t memset_s(void* dest, size_t destMax, int c, size_t count)
{
    if (dest == nullptr || count > destMax) {
        return EINVAL;
    }
    memset(dest, c, count);
    return EOK;
}

static inline errno_t strcpy_s(char* dest, size_t destMax, const char* src)
{
    if (dest == nullptr || src == nullptr) {
        return EINVAL;
    }
    size_t len = strlen(src);
    if (len >= destMax) {
        return ERANGE;
    }
    memcpy(dest, src, len + 1);
    return EOK;
}

static inline errno_t strncpy_s(char* dest, size_t destMax, const char* src, size_t count)
{
    if (dest == nullptr || src == nullptr) {
        return EINVAL;
    }
    size_t len = strlen(src);
    if (len > count) {
        len = count;
    }
    if (len >= destMax) {
        return ERANGE;
    }
    memcpy(dest, src, len);
    dest[len] = '\0';
    return EOK;
}

static inline errno_t strcat_s(char* dest, size_t destMax, const char* src)
{
    if (dest == nullptr || src == nullptr) {
        return EINVAL;
    }
    size_t dlen = strlen(dest);
    size_t slen = strlen(src);
    if (dlen + slen >= destMax) {
        return ERANGE;
    }
    memcpy(dest + dlen, src, slen + 1);
    return EOK;
}

static inline int sprintf_s(char* dest, size_t destMax, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    int ret = vsnprintf(dest, destMax, format, args);
    va_end(args);
    return ret;
}

static inline int snprintf_s(char* dest, size_t destMax, size_t /*count*/, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    int ret = vsnprintf(dest, destMax, format, args);
    va_end(args);
    return ret;
}

static inline int vsnprintf_s(char* dest, size_t destMax, size_t /*count*/, const char* format, va_list args)
{
    return vsnprintf(dest, destMax, format, args);
}

static inline int fscanf_s(FILE* stream, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    int ret = vfscanf(stream, format, args);
    va_end(args);
    return ret;
}

static inline int sscanf_s(const char* buffer, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    int ret = vsscanf(buffer, format, args);
    va_end(args);
    return ret;
}

/* securec_check macros - used extensively in MOT core */
#define securec_check(errno, charList, ...) ((void)(errno))
#define securec_check_ss(errno, charList, ...) ((void)(errno))
#define securec_check_intval(errno, ...) ((void)(errno))
#define securec_check_errno(errno, ...) ((void)(errno))
#define check_securec_ret(errno) ((void)(errno))

#endif /* SECUREC_H_STUB */
