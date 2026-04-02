/*
 * oro-db stub for knl/knl_instance.h
 * Replaces openGauss global instance with minimal standalone config.
 */
#ifndef KNL_INSTANCE_H_STUB
#define KNL_INSTANCE_H_STUB

#include <cstdint>
#include <atomic>

/* Minimal global instance structure */
struct knl_instance_attr_common {
    bool enable_thread_pool = false;
    const char* MOTConfigFileName = nullptr;
};

struct knl_instance_attr_memory {
    int64_t max_process_memory = 0;
};

struct knl_instance_attr {
    knl_instance_attr_common attr_common;
    knl_instance_attr_memory attr_memory;
};

struct knl_g_instance {
    knl_instance_attr attr;
};

extern knl_g_instance g_instance;

#endif /* KNL_INSTANCE_H_STUB */
