/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <abt.h>
#include <abt-snoozer.h>
#include <margo.h>

#include "my-rpc.h"

struct lookup_args
{
    margo_instance_id mid;
    char *addr_str;
    hg_addr_t *addr_p;
};

static void lookup_ult(void *_arg);

static hg_id_t my_rpc_shutdown_id;

int main(int argc, char **argv) 
{
    struct lookup_args args[2];
    ABT_thread threads[2];
    hg_addr_t svr_addrs[2] = {HG_ADDR_NULL};
    int i;
    int ret;
    ABT_xstream xstream;
    ABT_pool pool;
    margo_instance_id mid;
    hg_context_t *hg_context;
    hg_class_t *hg_class;
    char proto[12] = {0};
  
    if(argc != 3)
    {
        fprintf(stderr, "Usage: ./client <server_addr1> <server_addr2>\n");
        return(-1);
    }
       
    /* boilerplate HG initialization steps */
    /***************************************/

    /* initialize Mercury using the transport portion of the destination
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; i<11 && argv[1][i] != '\0' && argv[1][i] != ':'; i++)
        proto[i] = argv[1][i];
    hg_class = HG_Init(proto, HG_FALSE);
    if(!hg_class)
    {
        fprintf(stderr, "Error: HG_Init()\n");
        return(-1);
    }
    hg_context = HG_Context_create(hg_class);
    if(!hg_context)
    {
        fprintf(stderr, "Error: HG_Context_create()\n");
        HG_Finalize(hg_class);
        return(-1);
    }

    /* set up argobots */
    /***************************************/
    ret = ABT_init(argc, argv);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_init()\n");
        return(-1);
    }

    /* set primary ES to idle without polling */
    ret = ABT_snoozer_xstream_self_set();
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_snoozer_xstream_self_set()\n");
        return(-1);
    }

    /* retrieve current pool to use for ULT creation */
    ret = ABT_xstream_self(&xstream);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_xstream_self()\n");
        return(-1);
    }
    ret = ABT_xstream_get_main_pools(xstream, 1, &pool);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_xstream_get_main_pools()\n");
        return(-1);
    }

    /* actually start margo */
    /***************************************/
    mid = margo_init(0, 0, hg_context);

    /* register RPC */
    my_rpc_shutdown_id = MERCURY_REGISTER(hg_class, "my_shutdown_rpc", void, void, 
        NULL);

    /* issue concurrrent server lookups */
    for(i=0; i<2; i++)
    {
        args[i].mid = mid;
        args[i].addr_str = argv[i+1];
        args[i].addr_p = &svr_addrs[i];

        ret = ABT_thread_create(pool, lookup_ult, &args[i],
            ABT_THREAD_ATTR_NULL, &threads[i]);
        if(ret != 0)
        {
            fprintf(stderr, "Error: ABT_thread_create()\n");
            return(-1);
        }
    }

    /* yield to one of the threads */
    ABT_thread_yield_to(threads[0]);

    for(i=0; i<2; i++)
    {
        ret = ABT_thread_join(threads[i]);
        if(ret != 0)
        {
            fprintf(stderr, "Error: ABT_thread_join()\n");
            return(-1);
        }
        ret = ABT_thread_free(&threads[i]);
        if(ret != 0)
        {
            fprintf(stderr, "Error: ABT_thread_join()\n");
            return(-1);
        }
    }

    for(i=0; i<2; i++)
        HG_Addr_free(hg_class, svr_addrs[i]);

    /* shut down everything */
    margo_finalize(mid);
    
    ABT_finalize();

    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);

    return(0);
}

static void lookup_ult(void *arg)
{
    struct lookup_args *l = arg;
    hg_return_t hret;
    char tmp_addr_str[128] = {0};
    hg_size_t tmp_addr_str_sz = 128;

    hret = margo_addr_lookup(l->mid, l->addr_str, l->addr_p);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Failed to lookup server %s\n", l->addr_str);
    }
    else
    {
        HG_Addr_to_string(margo_get_class(l->mid), tmp_addr_str,
            &tmp_addr_str_sz, *(l->addr_p));
        printf("lookup of %s returned %s\n", l->addr_str, tmp_addr_str);
    }
}
