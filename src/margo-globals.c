/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "margo-globals.h"
#include "margo-logging.h"

int               g_margo_num_instances      = 0;
ABT_mutex         g_margo_num_instances_mtx  = ABT_MUTEX_NULL;
bool              g_margo_abt_init           = 0;
bool              g_margo_abt_prof_init      = 0;
bool              g_margo_abt_prof_started   = 0;
ABT_key           g_margo_rpc_breadcrumb_key = ABT_KEY_NULL;
ABT_key           g_margo_target_timing_key  = ABT_KEY_NULL;
margo_log_level   g_margo_log_level          = MARGO_LOG_ERROR;
ABTX_prof_context g_margo_abt_prof_context;

/* SYMBIOSYS start */
ABT_key g_margo_trace_id_key = ABT_KEY_NULL;
ABT_key g_margo_request_order_key = ABT_KEY_NULL;

#ifdef MERCURY_PROFILING
hg_prof_pvar_session_t pvar_session;
hg_prof_pvar_handle_t *pvar_handle;
int *pvar_count;
#endif

/* SYMBIOSYS end */
