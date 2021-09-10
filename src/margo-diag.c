/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <margo.h>
#include "margo-logging.h"
#include "margo-diag-internal.h"
#include "margo-instance.h"

#define SPARKLINE_ARRAY_LEN 100

static FILE* margo_output_file_open(margo_instance_id mid,
                                    const char*       file,
                                    int               uniquify,
                                    const char*       extension,
                                    char**            resolved_file_name);
static void  margo_diag_dump_fp(margo_instance_id mid, FILE* outfile);
static void  margo_diag_dump_abt_fp(margo_instance_id mid, FILE* outfile);
static void  margo_profile_dump_fp(margo_instance_id mid, FILE* outfile);

/* SYMBIOSYS begin */
#ifdef MERCURY_PROFILING
/* Initialize the Mercury Profiling Interface */
void __margo_initialize_mercury_profiling_interface(hg_class_t *hg_class) {

       char name[128];
       char desc[128];
       int name_len, desc_len, continuous;
       hg_prof_class_t pvar_class;
       hg_prof_datatype_t pvar_datatype;
       hg_prof_bind_t pvar_bind;
       HG_Prof_init(hg_class);
       //HG_Prof_pvar_get_info(hg_class, 0, name, &name_len, &pvar_class, &pvar_datatype, desc, &desc_len, &pvar_bind, &continuous);
       int num_pvars;
       num_pvars = HG_Prof_pvar_get_num(hg_class);
       fprintf(stderr, "[MARGO] Initializing profiling interface. Num PVARs exported: %d\n", num_pvars);
       HG_Prof_pvar_session_create(hg_class, &pvar_session);
       pvar_handle = (hg_prof_pvar_handle_t*)malloc(num_pvars*sizeof(hg_prof_pvar_handle_t));
       pvar_count = (int*)malloc(num_pvars*sizeof(int));
       for(int i = 0 ; i < num_pvars; i++)
         HG_Prof_pvar_handle_alloc(pvar_session, i, NULL, &(pvar_handle[i]), &(pvar_count[i]));
}

/* Finalize the Mercury Profiling Interface */
void __margo_finalize_mercury_profiling_interface(hg_class_t *hg_class) {
       int ret;

       int num_pvars = HG_Prof_pvar_get_num(hg_class);
       for(int i = 0; i < num_pvars; i++) {
         ret = HG_Prof_pvar_handle_free(pvar_session, 0, &(pvar_handle[i]));
         assert(ret == HG_SUCCESS);
       }

       ret = HG_Prof_pvar_session_destroy(hg_class, &pvar_session);
       assert(ret == HG_SUCCESS);
       ret = HG_Prof_finalize(hg_class);
       assert(ret == HG_SUCCESS);
       free(pvar_count);
       free(pvar_handle);
       fprintf(stderr, "[MARGO] Successfully shutdown profiling interface \n");
}

/* Query the Mercury PVAR interface */
void __margo_read_pvar_data(margo_instance_id mid, hg_handle_t handle, int index, void *buf) {
   double * temp = (double *)malloc(sizeof(double));

   HG_Prof_pvar_read(pvar_session, pvar_handle[index], handle, (void*)temp);
   *(double*)buf += *(double*)temp;
   free(temp);
}
#endif

void __margo_internal_breadcrumb_handler_set(uint64_t rpc_breadcrumb)
{
    uint64_t* val;

    ABT_key_get(g_margo_rpc_breadcrumb_key, (void**)(&val));

    if (val == NULL) {
        /* key not set yet on this ULT; we need to allocate a new one */
        /* best effort; just return and don't set it if we can't allocate memory
         */
        val = malloc(sizeof(*val));
        if (!val) return;
    }
    *val = rpc_breadcrumb;

    ABT_key_set(g_margo_rpc_breadcrumb_key, val);

    return;
}

void __margo_internal_start_server_time(margo_instance_id mid, hg_handle_t handle, double ts)
{
    hg_return_t ret;
    margo_request_metadata * metadata;
    const struct hg_info* info;
    char * name;
    struct margo_request_struct* req;

    ret = HG_Get_input_buf(handle, (void**)&metadata, NULL);
    assert(ret == HG_SUCCESS);
    (*metadata).rpc_breadcrumb = le64toh((*metadata).rpc_breadcrumb);
  
    /* add the incoming breadcrumb info to a ULT-local key if profiling is enabled */
    if(mid->profile_enabled) {

        ABT_key_get(g_margo_target_timing_key, (void**)(&req));

        if(req == NULL)
        {
            req = calloc(1, sizeof(*req));
        }
    
        req->rpc_breadcrumb = (*metadata).rpc_breadcrumb;

        req->timer = NULL;
        req->handle = handle;
        req->current_rpc = (*metadata).current_rpc;
        req->trace_id = (*metadata).trace_id;
        req->start_time = ABT_get_wtime();
        req->handler_time = (req->start_time - ts);
        req->is_server = 0;
        info = HG_Get_info(handle);
        req->provider_id = 0;
        req->provider_id += ((info->id) & (((1<<(__MARGO_PROVIDER_ID_SIZE*8))-1)));
        req->server_addr_hash = mid->self_addr_hash;
 
        /* Note: we use this opportunity to retrieve the incoming RPC
         * breadcrumb and put it in a thread-local argobots key.  It is
         * shifted down 16 bits so that if this handler in turn issues more
         * RPCs, there will be a stack showing the ancestry of RPC calls that
         * led to that point.
         */
        ABT_key_set(g_margo_target_timing_key, req);
        __margo_internal_generate_trace_event(mid, (*metadata).trace_id, sr, (*metadata).current_rpc, (*metadata).order + 1, 0, 0, 0);
        __margo_internal_request_order_set((*metadata).order + 1);
        __margo_internal_breadcrumb_handler_set((*metadata).rpc_breadcrumb << 16);
        __margo_internal_trace_id_set((*metadata).trace_id);
    }
}
uint64_t __margo_internal_generate_trace_id(margo_instance_id mid)
{
    char * name;
    uint64_t trace_id;
    uint64_t hash;

    GET_SELF_ADDR_STR(mid, name);
    HASH_JEN(name, strlen(name), hash); /*record own address in the breadcrumb */
    trace_id = hash;
    trace_id = ((trace_id) << 32);
    trace_id |= mid->trace_id_counter;
    mid->trace_id_counter++;
    return trace_id;
}

void __margo_internal_generate_trace_event(margo_instance_id mid, uint64_t trace_id, margo_trace_ev_type ev, uint64_t rpc, uint64_t order, double bw, double bw_start, double bw_end)
{

   mid->trace_records[mid->trace_record_index].trace_id = trace_id;
   mid->trace_records[mid->trace_record_index].ts = ABT_get_wtime();
   mid->trace_records[mid->trace_record_index].rpc = rpc;
   mid->trace_records[mid->trace_record_index].ev = ev;
   #ifdef MERCURY_PROFILING
   __margo_read_pvar_data(mid, NULL, 3, (void*)&mid->trace_records[mid->trace_record_index].ofi_events_read);
   #endif
   mid->trace_records[mid->trace_record_index].bulk_transfer_bw = bw;
   mid->trace_records[mid->trace_record_index].bulk_transfer_start = bw_start;
   mid->trace_records[mid->trace_record_index].bulk_transfer_end = bw_end;
   ABT_pool_get_total_size(mid->rpc_pool, &(mid->trace_records[mid->trace_record_index].metadata.abt_pool_total_size));
   ABT_pool_get_size(mid->rpc_pool, &(mid->trace_records[mid->trace_record_index].metadata.abt_pool_size));
   mid->trace_records[mid->trace_record_index].metadata.mid = mid->self_addr_hash;
   mid->trace_records[mid->trace_record_index].order = order;
   mid->trace_record_index++;

   #ifdef linux
   getrusage(RUSAGE_SELF, &mid->trace_records[mid->trace_record_index].metadata.usage);
   #endif
}
void __margo_internal_trace_id_set(uint64_t trace_id)
{
    uint64_t *val;

    ABT_key_get(g_margo_trace_id_key, (void**)(&val));

    if(val == NULL)
    {
        /* key not set yet on this ULT; we need to allocate a new one */
        /* best effort; just return and don't set it if we can't allocate memory */
        val = malloc(sizeof(*val));
        if(!val)
            return;
    }
    *val = trace_id;

    ABT_key_set(g_margo_trace_id_key, val);

    return;
}

void __margo_internal_request_order_set(uint64_t order)
{
    uint64_t *val;

    ABT_key_get(g_margo_request_order_key, (void**)(&val));

    if(val == NULL)
    {
        /* key not set yet on this ULT; we need to allocate a new one */
        /* best effort; just return and don't set it if we can't allocate memory */
        val = malloc(sizeof(*val));
        if(!val)
            return;
    }
    *val = order;

    ABT_key_set(g_margo_request_order_key, val);

    return;
}

static double calculate_percent_cpu_util()
{
    char str[100], dummy[10];
    uint64_t user, nice, system, idle, iowait_1, iowait_2, iowait_3, irq, softirq, steal;
    FILE* fp = fopen("/proc/stat","r");
    fgets(str,100,fp);
    fclose(fp);
    sscanf(str, "%s %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu", dummy, &user, &nice, &system, &idle, &iowait_1, &iowait_2, &iowait_3, &irq, &softirq, &steal);
    return (1.0-((double)idle/(double)(user+nice+system+idle+iowait_1+iowait_2+iowait_3+irq+softirq+steal)))*100.0;
}

static double calculate_percent_memory_util()
{
    char str1[100], str2[100], dummy1[20], dummy2[20], dummy3[20], dummy4[20];
    char * buf1 = str1;
    char * buf2 = str2;
    uint64_t memtotal, memfree;
    FILE* fp = fopen("/proc/meminfo","r");
    size_t s1 = 100, s2 = 100;
    getline(&buf1, &s1, fp);
    getline(&buf2, &s2, fp);
    sscanf(str1, "%s %lu %s", dummy1, &memtotal, dummy2);
    sscanf(str2, "%s %lu %s", dummy3, &memfree, dummy4);
    fclose(fp);
    return (1.0-((double)memfree/(double)memtotal))*100.0;
}

void __margo_system_stats_data_collection_fn(void* foo)
{
    int ret;
    struct margo_instance *mid = (struct margo_instance *)foo;
    double time_passed, end = 0;
    struct diag_data *stat, *tmp;
    double load_averages[3];
    char str[100];

    /* double check that profile collection should run, else, close this ULT */
    if(!mid->profile_enabled) {
      ABT_thread_join(mid->system_data_collection_tid);
      ABT_thread_free(&mid->system_data_collection_tid);
    }

    int sleep_time_msec = 1000; //TODO: Get this from a configuration, like the sparkline time

    while(!mid->hg_progress_shutdown_flag)
    {
      
        margo_thread_sleep(mid, sleep_time_msec);
        getloadavg(load_averages, 3);
        mid->system_stats[mid->system_stats_index].loadavg_1m = load_averages[0];
        mid->system_stats[mid->system_stats_index].loadavg_5m = load_averages[1];
        mid->system_stats[mid->system_stats_index].loadavg_15m = load_averages[2];
        mid->system_stats[mid->system_stats_index].system_cpu_util = calculate_percent_cpu_util();
        mid->system_stats[mid->system_stats_index].system_memory_util = calculate_percent_memory_util();

        mid->system_stats[mid->system_stats_index].ts = ABT_get_wtime();
        mid->system_stats_index++;
   }

   return;
}
/* SYMBIOSYS END */

void __margo_sparkline_data_collection_fn(void* foo)
{
    struct margo_instance* mid = (struct margo_instance*)foo;
    struct diag_data *     stat, *tmp;

    /* double check that profile collection should run, else, close this ULT */
    if (!mid->profile_enabled) {
        ABT_thread_join(mid->sparkline_data_collection_tid);
        ABT_thread_free(&mid->sparkline_data_collection_tid);
    }

    int sleep_time_msec = json_object_get_int64(json_object_object_get(
        mid->json_cfg, "profile_sparkline_timeslice_msec"));

    while (!mid->hg_progress_shutdown_flag) {
        margo_thread_sleep(mid, sleep_time_msec);
        HASH_ITER(hh, mid->diag_rpc, stat, tmp)
        {

            if (mid->sparkline_index > 0
                && mid->sparkline_index < SPARKLINE_ARRAY_LEN) {
                stat->sparkline_time[mid->sparkline_index]
                    = stat->stats.cumulative
                    - stat->sparkline_time[mid->sparkline_index - 1];
                stat->sparkline_count[mid->sparkline_index]
                    = stat->stats.count
                    - stat->sparkline_count[mid->sparkline_index - 1];
            } else if (mid->sparkline_index == 0) {
                stat->sparkline_time[mid->sparkline_index]
                    = stat->stats.cumulative;
                stat->sparkline_count[mid->sparkline_index] = stat->stats.count;
            } else {
                // Drop!
            }
        }
        mid->sparkline_index++;
        mid->previous_sparkline_data_collection_time = ABT_get_wtime();
    }

    return;
}

void __margo_print_diag_data(margo_instance_id mid,
                             FILE*             file,
                             const char*       name,
                             const char*       description,
                             struct diag_data* data)
{
    (void)mid;
    (void)description; // TODO was this supposed to be used?

    double avg;

    if (data->stats.count != 0)
        avg = data->stats.cumulative / data->stats.count;
    else
        avg = 0;

    fprintf(file, "%s,%.9f,%.9f,%.9f,%.9f,%lu\n", name, avg,
            data->stats.cumulative, data->stats.min, data->stats.max,
            data->stats.count);

    return;
}

void __margo_print_profile_data(margo_instance_id mid,
                                FILE*             file,
                                const char*       name,
                                const char*       description,
                                struct diag_data* data)
{
    (void)description; // TODO was this supposed to be used?
    double avg;
    int    i;

    if (data->stats.count != 0)
        avg = data->stats.cumulative / data->stats.count;
    else
        avg = 0;

    fprintf(stderr, "Do I even get here?\n");

    /* SYMBIOSYS BEGIN */
    /* first line is breadcrumb data */
    fprintf(file, "%s,%.9f,%lu,%lu,%d,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%lu,%.9f,%.9f,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%.9f,%.9f,%.9f\n", name, avg, data->key.rpc_breadcrumb, data->key.addr_hash, data->type, data->stats.cumulative, data->stats.handler_time, data->stats.completion_callback_time, data->stats.input_serial_time, data->stats.input_deserial_time, data->stats.output_serial_time, data->stats.internal_rdma_transfer_time, data->stats.internal_rdma_transfer_size, data->stats.min, data->stats.max, data->stats.count, data->stats.abt_pool_size_hwm, data->stats.abt_pool_size_lwm, data->stats.abt_pool_size_cumulative, data->stats.abt_pool_total_size_hwm, data->stats.abt_pool_total_size_lwm, data->stats.abt_pool_total_size_cumulative, data->stats.bulk_transfer_time, data->stats.bulk_create_elapsed, data->stats.bulk_free_elapsed);

    /* second line is sparkline data for the given breadcrumb*/
    fprintf(file, "%s,%d;", name, data->type);
    for(i = 0; (i < mid->sparkline_index && i < SPARKLINE_ARRAY_LEN); i++)
      fprintf(file, "%.9f,%.9f, %d;", data->sparkline_time[i], data->sparkline_count[i], i);
    fprintf(file,"\n");

    /* SYMBIOSYS END */

    /* first line is breadcrumb data */
    /*fprintf(file,
            "%s,%.9f,%lu,%lu,%d,%.9f,%.9f,%.9f,%lu,%lu,%lu,%lu,%lu,%lu,%lu\n",
            name, avg, data->key.rpc_breadcrumb, data->key.addr_hash,
            data->type, data->stats.cumulative, data->stats.min,
            data->stats.max, data->stats.count, data->stats.abt_pool_size_hwm,
            data->stats.abt_pool_size_lwm, data->stats.abt_pool_size_cumulative,
            data->stats.abt_pool_total_size_hwm,
            data->stats.abt_pool_total_size_lwm,
            data->stats.abt_pool_total_size_cumulative);

    fprintf(file, "%s,%d;", name, data->type);
    for (i = 0; (i < mid->sparkline_index && i < SPARKLINE_ARRAY_LEN); i++)
        fprintf(file, "%.9f,%.9f, %d;", data->sparkline_time[i],
                data->sparkline_count[i], i);
    fprintf(file, "\n");*/

    return;
}

/* records statistics for a breadcrumb, to be used after completion of an
 * RPC, both on the origin as well as on the target */
void __margo_breadcrumb_measure(margo_instance_id     mid,
				margo_request req,
                                margo_breadcrumb_type type)
{
    /* SYMBIOSYS begin */
    uint64_t rpc_breadcrumb = req->rpc_breadcrumb;
    double start = req->start_time;
    uint16_t provider_id = req->provider_id;
    uint64_t hash = req->server_addr_hash;
    hg_handle_t h = req->handle;
    struct diag_data *stat;
    double end, elapsed;
    uint16_t t = (type == origin) ? 2: 1;
    uint64_t hash_;
    /* SYMBIOSYS end */

    __uint128_t x = 0;

    /* IMPT NOTE: presently not adding provider_id to the breadcrumb,
       thus, the breadcrumb represents cumulative information for all providers
       offering or making a certain RPC call on this Margo instance */

    /* Bake in information about whether or not this was an origin or
     * target-side breadcrumb */
    hash_ = hash;
    hash_ = (hash_ >> 16) << 16;
    hash_ |= t;

    /* add in the server address */
    x = hash_;
    x = x << 64;
    x |= rpc_breadcrumb;

    if (!mid->profile_enabled) return;

    end     = ABT_get_wtime();
    elapsed = end - start;

    ABT_mutex_lock(mid->diag_rpc_mutex);

    HASH_FIND(hh, mid->diag_rpc, &x, sizeof(uint64_t) * 2, stat);

    if (!stat) {
        /* we aren't tracking this breadcrumb yet; add it */
        stat = calloc(1, sizeof(*stat));
        if (!stat) {
            /* best effort; we return gracefully without recording stats if this
             * happens.
             */
            ABT_mutex_unlock(mid->diag_rpc_mutex);
            return;
        }

        stat->rpc_breadcrumb     = rpc_breadcrumb;
        stat->type               = type;
        stat->key.rpc_breadcrumb = rpc_breadcrumb;
        stat->key.addr_hash      = hash;
        stat->key.provider_id    = provider_id;
        stat->x                  = x;

        /* initialize pool stats for breadcrumb */
        stat->stats.abt_pool_size_lwm        = 0x11111111; // Some high value
        stat->stats.abt_pool_size_cumulative = 0;
        stat->stats.abt_pool_size_hwm        = -1;

        stat->stats.abt_pool_total_size_lwm = 0x11111111; // Some high value
        stat->stats.abt_pool_total_size_cumulative = 0;
        stat->stats.abt_pool_total_size_hwm        = -1;

        /* initialize sparkline data */
        memset(stat->sparkline_time, 0.0, SPARKLINE_ARRAY_LEN * sizeof(double));
        memset(stat->sparkline_count, 0.0,
               SPARKLINE_ARRAY_LEN * sizeof(double));

        HASH_ADD(hh, mid->diag_rpc, x, sizeof(x), stat);
    }

    /* Argobots pool info */
    size_t                 s, s1;
    struct margo_rpc_data* margo_data;
    if (type) {
        const struct hg_info* info;
        info       = HG_Get_info(h);
        margo_data = (struct margo_rpc_data*)HG_Registered_data(mid->hg_class,
                                                                info->id);
        if (margo_data && margo_data->pool != ABT_POOL_NULL) {
            ABT_pool_get_total_size(margo_data->pool, &s);
            ABT_pool_get_size(margo_data->pool, &s1);
        } else {
            ABT_pool_get_total_size(mid->rpc_pool, &s);
            ABT_pool_get_size(mid->rpc_pool, &s1);
        }

        stat->stats.abt_pool_size_hwm
            = stat->stats.abt_pool_size_hwm > (double)s1
                ? stat->stats.abt_pool_size_hwm
                : s1;
        stat->stats.abt_pool_size_lwm
            = stat->stats.abt_pool_size_lwm < (double)s1
                ? stat->stats.abt_pool_size_lwm
                : s1;
        stat->stats.abt_pool_size_cumulative += s1;

        stat->stats.abt_pool_total_size_hwm
            = stat->stats.abt_pool_total_size_hwm > (double)s
                ? stat->stats.abt_pool_total_size_hwm
                : s;
        stat->stats.abt_pool_total_size_lwm
            = stat->stats.abt_pool_total_size_lwm < (double)s
                ? stat->stats.abt_pool_total_size_lwm
                : s;
        stat->stats.abt_pool_total_size_cumulative += s;
    }
    /* Argobots pool info */

    stat->stats.count++;
    /* SYMBIOSYS begin */
    if(type) {
      stat->stats.cumulative += req->ult_time;
      stat->stats.handler_time += req->handler_time;
      stat->stats.completion_callback_time += elapsed;
      elapsed += req->ult_time + req->handler_time;
      stat->stats.bulk_transfer_time += req->bulk_transfer_end - req->bulk_transfer_start;
      stat->stats.bulk_create_elapsed += req->bulk_create_elapsed;
      stat->stats.bulk_free_elapsed += req->bulk_free_elapsed;
      #ifdef MERCURY_PROFILING
      /* Read the exported PVAR data from the Mercury Profiling Interface */
      __margo_read_pvar_data(mid, req->handle, 6, (void*)&stat->stats.internal_rdma_transfer_time);
      __margo_read_pvar_data(mid, req->handle, 7, (void*)&stat->stats.internal_rdma_transfer_size);
      __margo_read_pvar_data(mid, req->handle, 9, (void*)&stat->stats.input_deserial_time);
      __margo_read_pvar_data(mid, req->handle, 11, (void*)&stat->stats.output_serial_time);
      #endif
    } else {
      stat->stats.cumulative += elapsed;
      stat->stats.handler_time = 0;
      #ifdef MERCURY_PROFILING
      /* Read the exported PVAR data from the Mercury Profiling Interface */
      __margo_read_pvar_data(mid, req->handle, 5, (void*)&stat->stats.completion_callback_time);
      __margo_read_pvar_data(mid, req->handle, 8, (void*)&stat->stats.input_deserial_time);
      #endif
    }
    /* SYMBIOSYS end */
    if (elapsed > stat->stats.max) stat->stats.max = elapsed;
    if (stat->stats.min == 0 || elapsed < stat->stats.min)
        stat->stats.min = elapsed;

    ABT_mutex_unlock(mid->diag_rpc_mutex);

    return;
}

/* sets the value of a breadcrumb, to be called just before issuing an RPC */
uint64_t __margo_breadcrumb_set(hg_id_t rpc_id)
{
    uint64_t* val;
    uint64_t  tmp;

    ABT_key_get(g_margo_rpc_breadcrumb_key, (void**)(&val));
    if (val == NULL) {
        /* key not set yet on this ULT; we need to allocate a new one
         * with all zeroes for initial value of breadcrumb and idx
         */
        /* NOTE: treating this as best effort; just return 0 if it fails */
        val = calloc(1, sizeof(*val));
        if (!val) return (0);
    }

    /* NOTE: an rpc_id (after mux'ing) has provider in low order bits and
     * base rpc_id in high order bits.  After demuxing, a base_id has zeroed
     * out low bits.  So regardless of whether the rpc_id is a base_id or a
     * mux'd id, either way we need to shift right to get either the
     * provider id (or the space reserved for it) out of the way, then mask
     * off 16 bits for use as a breadcrumb.
     */
    tmp = rpc_id >> (__MARGO_PROVIDER_ID_SIZE * 8);
    tmp &= 0xffff;

    /* clear low 16 bits of breadcrumb */
    *val = (*val >> 16) << 16;

    /* combine them, so that we have low order 16 of rpc id and high order
     * bits of previous breadcrumb */
    *val |= tmp;

    ABT_key_set(g_margo_rpc_breadcrumb_key, val);

    return *val;
}

void margo_breadcrumb_snapshot_destroy(margo_instance_id                 mid,
                                       struct margo_breadcrumb_snapshot* snap)
{
    struct margo_breadcrumb* tmp_bc      = snap->ptr;
    struct margo_breadcrumb* tmp_bc_next = NULL;
    while (tmp_bc) {
        tmp_bc_next = tmp_bc->next;
        free(tmp_bc);
        tmp_bc = tmp_bc_next;
    }
}

/* copy out the entire list of breadcrumbs on this margo instance */
void margo_breadcrumb_snapshot(margo_instance_id                 mid,
                               struct margo_breadcrumb_snapshot* snap)
{
    struct diag_data *       dd, *tmp;
    struct margo_breadcrumb* tmp_bc;

    memset(snap, 0, sizeof(*snap));

    if (!mid->profile_enabled) return;

    snap->ptr = calloc(1, sizeof(struct margo_breadcrumb));
    tmp_bc    = snap->ptr;

    HASH_ITER(hh, mid->diag_rpc, dd, tmp)
    {
        tmp_bc->stats.min        = dd->stats.min;
        tmp_bc->stats.max        = dd->stats.max;
        tmp_bc->type             = dd->type;
        tmp_bc->key              = dd->key;
        tmp_bc->stats.count      = dd->stats.count;
        tmp_bc->stats.cumulative = dd->stats.cumulative;

        tmp_bc->stats.abt_pool_total_size_hwm
            = dd->stats.abt_pool_total_size_hwm;
        tmp_bc->stats.abt_pool_total_size_lwm
            = dd->stats.abt_pool_total_size_lwm;
        tmp_bc->stats.abt_pool_total_size_cumulative
            = dd->stats.abt_pool_total_size_cumulative;
        tmp_bc->stats.abt_pool_size_hwm = dd->stats.abt_pool_size_hwm;
        tmp_bc->stats.abt_pool_size_lwm = dd->stats.abt_pool_size_lwm;
        tmp_bc->stats.abt_pool_size_cumulative
            = dd->stats.abt_pool_size_cumulative;

        tmp_bc->next = calloc(1, sizeof(struct margo_breadcrumb));
        tmp_bc       = tmp_bc->next;
        tmp_bc->next = NULL;
    }
}

/* open a file pointer for diagnostic/profile/state dumps */
static FILE* margo_output_file_open(margo_instance_id mid,
                                    const char*       file,
                                    int               uniquify,
                                    const char*       extension,
                                    char**            resolved_file_name)
{
    FILE* outfile;
    char* revised_file_name  = NULL;
    char* absolute_file_name = NULL;

    /* return early if the caller just wants stdout */
    if (strcmp("-", file) == 0) {
        if (resolved_file_name) *resolved_file_name = strdup("<STDOUT>");
        return (stdout);
    }

    revised_file_name = malloc(strlen(file) + 256);
    if (!revised_file_name) {
        MARGO_ERROR(mid, "malloc() failure: %d\n", errno);
        return (NULL);
    }

    /* construct revised file name with correct extension and (if desired)
     * substitutes unique information
     */
    if (uniquify) {
        char hostname[128] = {0};
        int  pid;

        gethostname(hostname, 128);
        pid = getpid();

        sprintf(revised_file_name, "%s-%s-%d.%s", file, hostname, pid,
                extension);
    } else {
        sprintf(revised_file_name, "%s.%s", file, extension);
    }

    /* if directory is not specified then use output directory from margo
     * configuration
     */
    if (revised_file_name[0] == '/') {
        absolute_file_name = revised_file_name;
    } else {
        absolute_file_name
            = malloc(strlen(json_object_get_string(
                         json_object_object_get(mid->json_cfg, "output_dir")))
                     + strlen(revised_file_name) + 2);
        if (!absolute_file_name) {
            MARGO_ERROR(mid, "malloc() failure: %d\n", errno);
            free(revised_file_name);
            return (NULL);
        }
        sprintf(absolute_file_name, "%s/%s",
                json_object_get_string(
                    json_object_object_get(mid->json_cfg, "output_dir")),
                revised_file_name);
    }

    /* actually open file */
    outfile = fopen(absolute_file_name, "a");
    if (!outfile)
        MARGO_ERROR(mid, "fopen(%s) failure: %d\n", absolute_file_name, errno);

    if (resolved_file_name) {
        if (absolute_file_name != revised_file_name) {
            *resolved_file_name = absolute_file_name;
            free(revised_file_name);
        } else {
            *resolved_file_name = revised_file_name;
        }
    } else {
        if (absolute_file_name != revised_file_name) free(absolute_file_name);
        free(revised_file_name);
    }

    return (outfile);
}

static void margo_diag_dump_abt_fp(margo_instance_id mid, FILE* outfile)
{
    time_t   ltime;
    char*    name;
    uint64_t hash;

    if (!mid->diag_enabled) return;

    time(&ltime);

    fprintf(outfile, "# Margo diagnostics (Argobots profile)\n");
    GET_SELF_ADDR_STR(mid, name);
    HASH_JEN(name, strlen(name),
             hash); /*record own address in the breadcrumb */
    fprintf(outfile, "# Addr Hash and Address Name: %lu,%s\n", hash, name);
    free(name);
    fprintf(outfile, "# %s\n", ctime(&ltime));

    if (g_margo_abt_prof_started) {
        /* have to stop profiling briefly to print results */
        ABTX_prof_stop(g_margo_abt_prof_context);
        ABTX_prof_print(g_margo_abt_prof_context, outfile,
                        ABTX_PRINT_MODE_SUMMARY | ABTX_PRINT_MODE_FANCY);
        /* TODO: consider supporting PROF_MODE_DETAILED also? */
        ABTX_prof_start(g_margo_abt_prof_context, ABTX_PROF_MODE_BASIC);
    }

    return;
}

static void margo_diag_dump_fp(margo_instance_id mid, FILE* outfile)
{
    time_t   ltime;
    char*    name;
    uint64_t hash;

    if (!mid->diag_enabled) return;

    time(&ltime);

    fprintf(outfile, "# Margo diagnostics\n");
    GET_SELF_ADDR_STR(mid, name);
    HASH_JEN(name, strlen(name),
             hash); /*record own address in the breadcrumb */
    fprintf(outfile, "# Addr Hash and Address Name: %lu,%s\n", hash, name);
    free(name);
    fprintf(outfile, "# %s\n", ctime(&ltime));
    fprintf(outfile,
            "# Function Name, Average Time Per Call, Cumulative Time, "
            "Highwatermark, Lowwatermark, Call Count\n");

    __margo_print_diag_data(mid, outfile, "trigger_elapsed",
                            "Time consumed by HG_Trigger()",
                            &mid->diag_trigger_elapsed);
    __margo_print_diag_data(
        mid, outfile, "progress_elapsed_zero_timeout",
        "Time consumed by HG_Progress() when called with timeout==0",
        &mid->diag_progress_elapsed_zero_timeout);
    __margo_print_diag_data(
        mid, outfile, "progress_elapsed_nonzero_timeout",
        "Time consumed by HG_Progress() when called with timeout!=0",
        &mid->diag_progress_elapsed_nonzero_timeout);
    __margo_print_diag_data(mid, outfile, "bulk_create_elapsed",
                            "Time consumed by HG_Bulk_create()",
                            &mid->diag_bulk_create_elapsed);

    return;
}

void margo_diag_dump(margo_instance_id mid, const char* file, int uniquify)
{
    FILE* outfile;

    if (!mid->diag_enabled) return;

    /* rpc diagnostics */
    outfile = margo_output_file_open(mid, file, uniquify, "diag", NULL);
    if (!outfile) return;

    margo_diag_dump_fp(mid, outfile);

    /* abt profiling */
    outfile = margo_output_file_open(mid, file, uniquify, "diag.abt", NULL);
    if (!outfile) return;

    margo_diag_dump_abt_fp(mid, outfile);

    if (outfile != stdout) fclose(outfile);

    return;
}

static void margo_profile_dump_fp(margo_instance_id mid, FILE* outfile)
{
    time_t                       ltime;
    struct diag_data *           dd, *tmp;
    char                         rpc_breadcrumb_str[256] = {0};
    struct margo_registered_rpc* tmp_rpc;
    char*                        name;
    uint64_t                     hash;

    if (!mid->profile_enabled) return;

    time(&ltime);

    fprintf(outfile, "%u\n", mid->num_registered_rpcs);
    GET_SELF_ADDR_STR(mid, name);
    HASH_JEN(name, strlen(name),
             hash); /*record own address in the breadcrumb */

    fprintf(outfile, "%lu,%s\n", hash, name);
    free(name);

    tmp_rpc = mid->registered_rpcs;
    while (tmp_rpc) {
        fprintf(outfile, "0x%.4lx,%s\n", tmp_rpc->rpc_breadcrumb_fragment,
                tmp_rpc->func_name);
        tmp_rpc = tmp_rpc->next;
    }

    fprintf(stderr, "Is this invoked?\n");
    HASH_ITER(hh, mid->diag_rpc, dd, tmp)
    {
	fprintf(stderr , "How many times is this invoked?\n");
        int      i;
        uint64_t tmp_breadcrumb;
        for (i = 0; i < 4; i++) {
            tmp_breadcrumb = dd->rpc_breadcrumb;
            tmp_breadcrumb >>= (i * 16);
            tmp_breadcrumb &= 0xffff;

            if (!tmp_breadcrumb) continue;

            if (i == 3)
                sprintf(&rpc_breadcrumb_str[i * 7], "0x%.4lx", tmp_breadcrumb);
            else
                sprintf(&rpc_breadcrumb_str[i * 7], "0x%.4lx ", tmp_breadcrumb);
        }
        __margo_print_profile_data(mid, outfile, rpc_breadcrumb_str,
                                   "RPC statistics", dd);
    }

    return;
}

void margo_profile_dump(margo_instance_id mid, const char* file, int uniquify)
{
    FILE* outfile;

    if (!mid->profile_enabled) return;

    outfile = margo_output_file_open(mid, file, uniquify, "csv", NULL);
    if (!outfile) return;

    margo_profile_dump_fp(mid, outfile);

    if (outfile != stdout) fclose(outfile);

    return;
}

/* SYMBIOSYS BEGIN */
void margo_system_stats_dump(margo_instance_id mid, const char* file, int uniquify)
{
    FILE *outfile;
    char revised_file_name[256] = {0};
    struct margo_registered_rpc *tmp_rpc;
    char hostname[128] = {0};
    int pid;
    int i;

    assert(mid->profile_enabled);

    if(uniquify)
    {
        gethostname(hostname, 128);
        pid = getpid();

        sprintf(revised_file_name, "%s-%s-%d.stats", file, hostname, pid);
    }

    else
    {
        sprintf(revised_file_name, "%s.stats", file);
    }

    if(strcmp("-", file) == 0)
    {
        outfile = stdout;
    }
    else
    {
        outfile = fopen(revised_file_name, "a");
        if(!outfile)
        {
            perror("fopen");
            return;
        }
    }

    fprintf(outfile, "%s\n", hostname);
    fprintf(outfile, "%d\n", pid);
    fprintf(outfile, "%d\n", mid->system_stats_index);

    for(i = 0; i < mid->system_stats_index; i++)
      fprintf(outfile, "%.9f, %.9f, %.9f, %.9f, %.9f, %.9f\n", mid->system_stats[i].ts, mid->system_stats[i].system_cpu_util, mid->system_stats[i].system_memory_util, mid->system_stats[i].loadavg_1m, mid->system_stats[i].loadavg_5m, mid->system_stats[i].loadavg_15m);


    if(outfile != stdout)
        fclose(outfile);

    return;
}

void margo_trace_dump(margo_instance_id mid, const char* file, int uniquify)
{
    FILE *outfile;
    char revised_file_name[256] = {0};
    struct margo_registered_rpc *tmp_rpc;
    int i;
    char hostname[128] = {0};
    int pid;

    assert(mid->profile_enabled);

    if(uniquify)
    {
        gethostname(hostname, 128);
        pid = getpid();

        sprintf(revised_file_name, "%s-%s-%d.trace", file, hostname, pid);
    }

    else
    {
        sprintf(revised_file_name, "%s.trace", file);
    }

    if(strcmp("-", file) == 0)
    {
        outfile = stdout;
    }
    else
    {
        outfile = fopen(revised_file_name, "a");
        if(!outfile)
        {
            perror("fopen");
            return;
        }
    }

    fprintf(outfile, "%s\n", hostname);
    fprintf(outfile, "%d\n", pid);
    fprintf(outfile, "%u\n", mid->num_registered_rpcs);
    tmp_rpc = mid->registered_rpcs;

    while(tmp_rpc)
    {
        fprintf(outfile, "%lu, %s\n", tmp_rpc->rpc_breadcrumb_fragment, tmp_rpc->func_name);
        tmp_rpc = tmp_rpc->next;
    }

    for(i = 0; i < mid->trace_record_index; i++) {
      fprintf(outfile, "%lu, %.9f, %lu, %d, %d, %d, %lu, %d, %d, %lu, %lu, %.9f, %.9f, %.9f\n", mid->trace_records[i].trace_id, mid->trace_records[i].ts, mid->trace_records[i].rpc, mid->trace_records[i].ev, mid->trace_records[i].metadata.abt_pool_size, mid->trace_records[i].metadata.abt_pool_total_size, mid->trace_records[i].metadata.mid, mid->trace_records[i].order, mid->trace_id_counter, mid->trace_records[i].metadata.usage.ru_maxrss, mid->trace_records[i].ofi_events_read, mid->trace_records[i].bulk_transfer_bw,  mid->trace_records[i].bulk_transfer_start,  mid->trace_records[i].bulk_transfer_end);

      /* Below is the chrome-compatible format */
      /*if(mid->trace_records[i].ev == 0 || mid->trace_records[i].ev == 3) {
         fprintf(outfile, "{\"name\":\"%lu\", \"cat\": \"PERF\", \"ph\":\"B\", \"pid\": %lu, \"ts\": %f, \"trace_id\": %lu},\n", mid->trace_records[i].rpc, mid->trace_records[i].metadata.mid, (mid->trace_records[i].ts - mid->trace_collection_start_time), mid->trace_records[i].trace_id);
      } else {
         fprintf(outfile, "{\"name\":\"%lu\", \"cat\": \"PERF\", \"ph\":\"E\", \"pid\": %lu, \"ts\": %f, \"trace_id\": %lu},\n", mid->trace_records[i].rpc, mid->trace_records[i].metadata.mid, (mid->trace_records[i].ts - mid->trace_collection_start_time), mid->trace_records[i].trace_id);
      }*/
    }

    fprintf(stderr, "Margo Instance has %d trace record_entries\n", mid->trace_record_index); 

    if(outfile != stdout)
        fclose(outfile);

    return;
}
/* SYMBIOSYS END */

void margo_state_dump(margo_instance_id mid,
                      const char*       file,
                      int               uniquify,
                      char**            resolved_file_name)
{
    FILE*    outfile;
    time_t   ltime;
    char*    name;
    int      i = 0;
    char*    encoded_json;
    ABT_bool qconfig;
    unsigned pending_operations;

    outfile = margo_output_file_open(mid, file, uniquify, "state",
                                     resolved_file_name);
    if (!outfile) return;

    time(&ltime);

    fprintf(outfile, "# Margo state dump\n");
    GET_SELF_ADDR_STR(mid, name);
    fprintf(outfile, "# Mercury address: %s\n", name);
    free(name);
    fprintf(outfile, "# %s\n", ctime(&ltime));

    fprintf(outfile,
            "\n# Margo configuration (JSON)\n"
            "# ==========================\n");
    encoded_json = margo_get_config(mid);
    fprintf(outfile, "%s\n", encoded_json);
    if (encoded_json) free(encoded_json);

    fprintf(outfile,
            "\n# Margo instance state\n"
            "# ==========================\n");
    ABT_mutex_lock(mid->pending_operations_mtx);
    pending_operations = mid->pending_operations;
    ABT_mutex_unlock(mid->pending_operations_mtx);
    fprintf(outfile, "mid->pending_operations: %d\n", pending_operations);
    fprintf(outfile, "mid->diag_enabled: %d\n", mid->diag_enabled);
    fprintf(outfile, "mid->profile_enabled: %d\n", mid->profile_enabled);

    fprintf(
        outfile,
        "\n# Margo diagnostics\n"
        "\n# NOTE: this is only available if mid->diag_enabled == 1 above.  "
        "You can\n"
        "#       turn this on by calling margo_diag_start() "
        "programatically, by setting\n"
        "#       the MARGO_ENABLE_DIAGNOSTICS=1 environment variable, or "
        "by setting\n"
        "#       the \"enable_diagnostics\" JSON configuration parameter.\n"
        "# ==========================\n");
    margo_diag_dump_fp(mid, outfile);

    fprintf(
        outfile,
        "\n# Margo RPC profiling\n"
        "\n# NOTE: this is only available if mid->profile_enabled == 1 above.  "
        "You can\n"
        "#       turn this on by calling margo_profile_start() "
        "programatically, by\n"
        "#       setting the MARGO_ENABLE_PROFILING=1 environment variable, or "
        "by setting\n"
        "#       the \"enable_profiling\" JSON configuration parameter.\n"
        "# ==========================\n");
    margo_profile_dump_fp(mid, outfile);

    fprintf(outfile,
            "\n# Argobots configuration (ABT_info_print_config())\n"
            "# ================================================\n");
    ABT_info_print_config(outfile);

    fprintf(outfile,
            "\n# Argobots execution streams (ABT_info_print_all_xstreams())\n"
            "# ================================================\n");
    ABT_info_print_all_xstreams(outfile);

    fprintf(outfile,
            "\n# Margo Argobots profiling summary\n"
            "\n# NOTE: this is only available if mid->diag_enabled == 1 above "
            "*and* Argobots\n"
            "# has been compiled with tool interface support.  You can turn on "
            "Margo\n"
            "# diagnostics at runtime by calling margo_diag_start() "
            "programatically, by\n"
            "# setting the MARGO_ENABLE_DIAGNOSTICS=1 environment variable, or "
            "by setting\n"
            "# the \"enable_diagnostics\" JSON configuration parameter. You "
            "can enable the\n"
            "# Argobots tool interface by compiling Argobots with the "
            "--enable-tool or the\n"
            "# +tool spack variant.\n"
            "# ==========================\n");
    margo_diag_dump_abt_fp(mid, outfile);

    fprintf(outfile,
            "\n# Argobots stack dump (ABT_info_print_thread_stacks_in_pool())\n"
            "#   *IMPORTANT NOTE*\n"
            "# This stack dump does *not* display information about currently "
            "executing\n"
            "# user-level threads.  The user-level threads shown here are "
            "awaiting\n"
            "# execution due to synchronization primitives or resource "
            "constraints.\n");

    ABT_info_query_config(ABT_INFO_QUERY_KIND_ENABLED_STACK_UNWIND, &qconfig);
    fprintf(outfile, "# Argobots stack unwinding: %s\n",
            (qconfig == ABT_TRUE) ? "ENABLED" : "DISABLED");
    if (qconfig != ABT_TRUE) {
        fprintf(outfile,
                "# *IMPORTANT NOTE*\n"
                "# You can make the following stack dump more human readable "
                "by compiling\n"
                "# Argobots with --enable-stack-unwind or the +stackunwind "
                "spack variant.\n");
    }
    fprintf(outfile, "# ================================================\n");

    /* for each pool that margo is aware of */
    for (i = 0; i < mid->num_abt_pools; i++) {
        /* Display stack trace of ULTs within that pool.  This will not
         * include any ULTs that are presently executing (including the
         * caller).
         */
        ABT_info_print_thread_stacks_in_pool(outfile, mid->abt_pools[i].pool);
    }

    if (outfile != stdout) fclose(outfile);

    return;
}

void __margo_sparkline_thread_stop(margo_instance_id mid)
{
    if (!mid->profile_enabled) return;

    MARGO_TRACE(mid,
                "Waiting for sparkline data collection thread to complete");
    ABT_thread_join(mid->sparkline_data_collection_tid);
    ABT_thread_free(&mid->sparkline_data_collection_tid);

    return;
}

void __margo_sparkline_thread_start(margo_instance_id mid)
{
    int ret;

    if (!mid->profile_enabled) return;

    MARGO_TRACE(mid, "Profiling is enabled, starting profiling thread");
    mid->previous_sparkline_data_collection_time = ABT_get_wtime();

    ret = ABT_thread_create(
        mid->progress_pool, __margo_sparkline_data_collection_fn, mid,
        ABT_THREAD_ATTR_NULL, &mid->sparkline_data_collection_tid);
    if (ret != ABT_SUCCESS) {
        MARGO_WARNING(
            0,
            "Failed to start sparkline data collection thread, "
            "continuing to profile without sparkline data collection");
    }

    return;
}

void __margo_system_stats_thread_stop(margo_instance_id mid)
{
    if (!mid->profile_enabled) return;

    MARGO_TRACE(mid,
                "Waiting for sparkline data collection thread to complete");
    ABT_thread_join(mid->system_data_collection_tid);
    ABT_thread_free(&mid->system_data_collection_tid);

    return;
}

void __margo_system_stats_thread_start(margo_instance_id mid)
{
    int ret;

    if (!mid->profile_enabled) return;

    MARGO_TRACE(mid, "Profiling is enabled, starting profiling thread");

    ret = ABT_thread_create(
        mid->progress_pool, __margo_system_stats_data_collection_fn, mid,
        ABT_THREAD_ATTR_NULL, &mid->system_data_collection_tid);
    if (ret != ABT_SUCCESS) {
        MARGO_WARNING(
            0,
            "Failed to start system stats data collection thread, "
            "continuing to profile without system stats data collection");
    }

    return;
}
