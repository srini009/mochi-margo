/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>     /* defines printf for tests */
#include <time.h>      /* defines time_t for timings in the test */
#include <stdint.h>    /* defines uint32_t etc */
#include <sys/param.h> /* attempt to define endianness */
#ifdef linux
    #include <endian.h> /* attempt to define endianness */
    #include <sys/resource.h>
#endif

#ifndef __MARGO_DIAG
    #define __MARGO_DIAG

    #ifdef __cplusplus
extern "C" {
    #endif

/* used to identify a globally unique breadcrumb */
struct margo_global_breadcrumb_key {
    uint64_t rpc_breadcrumb; /* a.k.a RPC callpath */
    uint64_t addr_hash;      /* hash of server addr */
    uint16_t provider_id; /* provider_id within a server. NOT a globally unique
                             identifier */
};

enum margo_breadcrumb_type
{
    origin,
    target
};

typedef enum margo_breadcrumb_type margo_breadcrumb_type;

struct margo_breadcrumb_stats {
    /* stats for breadcrumb call times */
    double min;
    double max;
    double cumulative;

    /* stats for RPC handler pool sizes */
    /* Total pool size = Total number of runnable items + items waiting on a
     * lock */
    unsigned long abt_pool_total_size_lwm; /* low watermark */
    unsigned long abt_pool_total_size_hwm; /* high watermark */
    unsigned long abt_pool_total_size_cumulative;

    unsigned long abt_pool_size_lwm; /* low watermark */
    unsigned long abt_pool_size_hwm; /* high watermark */
    unsigned long abt_pool_size_cumulative;

    /* count of occurrences of breadcrumb */
    unsigned long count;

    /* SYMBIOSYS experimental fields */
    /* additional stats */
    double handler_time;
    double completion_callback_time;
    double internal_rdma_transfer_time;
    double input_serial_time;
    double input_deserial_time;
    double output_serial_time;
    size_t internal_rdma_transfer_size;
    double bulk_transfer_time;
    double bulk_create_elapsed;
    double bulk_free_elapsed;
};

typedef struct margo_breadcrumb_stats margo_breadcrumb_stats;

/* structure to store breadcrumb snapshot, for consumption outside of margo.
   reflects the margo-internal structure used to hold diagnostic data */
struct margo_breadcrumb {
    margo_breadcrumb_stats stats;
    /* 0 is this is a origin-side breadcrumb, 1 if this is a target-side
     * breadcrumb */
    margo_breadcrumb_type type;

    struct margo_global_breadcrumb_key key;

    struct margo_breadcrumb* next;
};

/* snapshot contains linked list of breadcrumb data */
struct margo_breadcrumb_snapshot {
    struct margo_breadcrumb* ptr;
};

/* SYMBIOSYS tracing and sampling logic */
/* Request tracing definitions */
enum margo_trace_ev_type
{
  cs, sr, ss, cr
};

typedef enum margo_trace_ev_type margo_trace_ev_type;

struct margo_trace_metadata
{
   size_t abt_pool_size;
   size_t abt_pool_total_size;
   uint64_t mid;
   #ifdef linux
   struct rusage usage;
   #endif
};

struct margo_trace_record
{
  uint64_t trace_id;
  double ts;
  uint64_t rpc;
  size_t ofi_events_read;
  margo_trace_ev_type ev;
  uint64_t order;
  struct margo_trace_metadata metadata;
  double bulk_transfer_bw;
  double bulk_transfer_start;
  double bulk_transfer_end;
  char name[30];
};

struct margo_system_stat
{
  double system_cpu_util;
  double system_memory_util;
  double loadavg_1m;
  double loadavg_5m;
  double loadavg_15m;
  double ts;
};

struct margo_request_metadata
{
  uint64_t rpc_breadcrumb;
  uint64_t trace_id;
  uint64_t order;
  uint64_t current_rpc;
};

typedef struct margo_request_metadata margo_request_metadata;
typedef struct margo_trace_record margo_trace_record;
typedef struct margo_system_stat margo_system_stat;

    #ifdef __cplusplus
}
    #endif

#endif /* __MARGO_DIAG */
