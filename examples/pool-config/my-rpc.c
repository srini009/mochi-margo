/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include "my-rpc.h"

extern ABT_pool shared_pool;

static void worker(void *_arg)
{
    /* TODO: do something here */
    return;
}

/* The rpc handler is defined as a single ULT in Argobots.  It uses
 * underlying asynchronous operations for the HG transfer, open, write, and
 * close.
 */

static void my_rpc_ult(hg_handle_t handle)
{
    hg_return_t hret;
    my_rpc_out_t out;
    my_rpc_in_t in;
    hg_size_t size;
    void *buffer;
    hg_bulk_t bulk_handle;
    const struct hg_info *hgi;
    ABT_thread *threads;
    int i;
    int ret;

    margo_instance_id mid;

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    if(in.num_threads)
    {
        /* spawn n threads based on client request */
        threads = calloc(in.num_threads, sizeof(*threads));
        assert(threads);
        
        for(i=0; i<in.num_threads; i++)
        {
            ret = ABT_thread_create(shared_pool, worker, NULL, ABT_THREAD_ATTR_NULL,
                &threads[i]);
            assert(ret == 0);
        }
       
        /* wait for them to finish */
        for(i=0; i<in.num_threads; i++)
        {
            ret = ABT_thread_join(threads[i]);
            assert(ret == 0);
        }
        free(threads);
    }

    /* set up target buffer for bulk transfer */
    size = 512;
    buffer = calloc(1, 512);
    assert(buffer);

    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    assert(mid != MARGO_INSTANCE_NULL);

    /* register local target buffer for bulk access */
    hret = margo_bulk_create(mid, 1, &buffer,
        &size, HG_BULK_READ_ONLY, &bulk_handle);
    assert(hret == HG_SUCCESS);

    /* do bulk transfer from client to server */
    hret = margo_bulk_transfer(mid, HG_BULK_PUSH,
        hgi->addr, in.bulk_handle, 0,
        bulk_handle, 0, size);
    assert(hret == HG_SUCCESS);

    margo_free_input(handle, &in);

    out.ret = 0;

    hret = margo_respond(handle, &out);
    assert(hret == HG_SUCCESS);

    margo_bulk_free(bulk_handle);
    margo_destroy(handle);
    free(buffer);

    return;
}
DEFINE_MARGO_RPC_HANDLER(my_rpc_ult)

static void my_rpc_shutdown_ult(hg_handle_t handle)
{
    hg_return_t hret;
    margo_instance_id mid;

    printf("Got RPC request to shutdown\n");

    /* get handle info and margo instance */
    mid = margo_hg_handle_get_instance(handle);
    assert(mid != MARGO_INSTANCE_NULL);

    hret = margo_respond(handle, NULL);
    assert(hret == HG_SUCCESS);

    margo_destroy(handle);

    /* NOTE: we assume that the server daemon is using
     * margo_wait_for_finalize() to suspend until this RPC executes, so there
     * is no need to send any extra signal to notify it.
     */
    margo_finalize(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(my_rpc_shutdown_ult)
