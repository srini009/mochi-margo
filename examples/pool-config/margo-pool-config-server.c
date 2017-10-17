/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <mercury.h>
#include <abt.h>
#include <margo.h>

#if 0
#define SNOOZE 1
#define BUSY_MARGO 1
#endif

#ifdef SNOOZE
#include <abt-snoozer.h>
#endif

#include "my-rpc.h"

ABT_pool    shared_pool;

int main(int argc, char **argv) 
{
    hg_return_t hret;
    margo_instance_id mid;
    hg_addr_t addr_self;
    char addr_self_string[128];
    hg_size_t addr_self_string_sz = 128;
    int use_dedicated_progress_pool = 0;
    int shared_pool_size = 0;
    ABT_xstream *xstreams;
    ABT_sched   *scheds;
    ABT_sched   progress_sched;
    ABT_pool    progress_pool;
    ABT_xstream progress_xstream;
    int i;
    hg_class_t *hg_class = NULL;
    hg_context_t *hg_context = NULL;
    int ret;

    if(argc != 4)
    {
        fprintf(stderr, "Usage: ./server <listen_addr> <0|1> <#>\n");
        fprintf(stderr, "     <0|1> : disable/enable separate ES and pool for hg progress.\n");
        fprintf(stderr, "     <#> : number of ES's in shared service pool.\n");
        fprintf(stderr, "Example: ./server na+sm:// 1 1\n");
        fprintf(stderr, "         (1 ES for progress, 1 ES for RPCs\n");
        fprintf(stderr, "Example: ./server na+sm:// 0 4\n");
        fprintf(stderr, "         (4 ES's in a shared pool for both progress and RPCs\n");
        return(-1);
    }

    ret = ABT_init(argc, argv);
    assert(ret == 0);

#ifdef SNOOZE
    /* set primary ES to idle without polling in the scheduler */
    ret = ABT_snoozer_xstream_self_set();
#endif

    ret = sscanf(argv[2], "%d", &use_dedicated_progress_pool);
    assert(ret == 1);
    assert(use_dedicated_progress_pool == 0 || use_dedicated_progress_pool == 1);

    ret = sscanf(argv[3], "%d", &shared_pool_size);
    assert(ret == 1);
    assert(shared_pool_size > 0);

    xstreams = malloc(sizeof(*xstreams)*(shared_pool_size));
    assert(xstreams);
    scheds = malloc(sizeof(*scheds)*(shared_pool_size));
    assert(scheds);

    /* Create a shared pool */
    /********************************************/
#ifdef SNOOZE
    ret = ABT_snoozer_xstream_create(shared_pool_size, &shared_pool,
        xstreams);
    assert(ret == 0);
#else
    ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC,
                          ABT_TRUE, &shared_pool);
    for (i = 0; i < shared_pool_size; i++) {
        ABT_sched_create_basic(ABT_SCHED_DEFAULT, 1, &shared_pool,
                               ABT_SCHED_CONFIG_NULL, &scheds[i]);
        ABT_xstream_create(scheds[i], &xstreams[i]);
    }
#endif

    /* create a progress pool (if requested */
    /*********************************************/
    if(use_dedicated_progress_pool)
    {
#ifdef SNOOZE
        ret = ABT_snoozer_xstream_create(1, &progress_pool,
            &progress_xstream);
        assert(ret == 0);
#else
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC,
                              ABT_TRUE, &progress_pool);
        ABT_sched_create_basic(ABT_SCHED_DEFAULT, 1, &progress_pool,
                               ABT_SCHED_CONFIG_NULL, &progress_sched);
        ABT_xstream_create(progress_sched, &progress_xstream);
#endif
    }
    else
    {
        progress_pool = shared_pool;
    }

    hg_class = HG_Init(argv[1], 1);
    assert(hg_class);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context);

    mid = margo_init_pool(progress_pool, shared_pool, hg_context);
    assert(mid);

#ifdef BUSY_MARGO
    unsigned int timeout=0;
    margo_set_param(mid, MARGO_PARAM_PROGRESS_TIMEOUT_UB, &timeout);
#endif

    /* figure out what address this server is listening on */
    hret = margo_addr_self(mid, &addr_self);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_self()\n");
        margo_finalize(mid);
        return(-1);
    }
    hret = margo_addr_to_string(mid, addr_self_string, &addr_self_string_sz, addr_self);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_to_string()\n");
        margo_addr_free(mid, addr_self);
        margo_finalize(mid);
        return(-1);
    }
    margo_addr_free(mid, addr_self);

    printf("# accepting RPCs on address \"%s\"\n", addr_self_string);

    /* register RPC */
    MARGO_REGISTER(mid, "my_rpc", my_rpc_in_t, my_rpc_out_t, my_rpc_ult);
    MARGO_REGISTER(mid, "my_shutdown_rpc", void, void, my_rpc_shutdown_ult);

    /* NOTE: there isn't anything else for the server to do at this point
     * except wait for itself to be shut down.  The
     * margo_wait_for_finalize() call here yields to let Margo drive
     * progress until that happens.
     */
    
    margo_wait_for_finalize(mid);

    HG_Context_destroy(hg_context);

    HG_Finalize(hg_class);

    ABT_finalize();

    return(0);
}
