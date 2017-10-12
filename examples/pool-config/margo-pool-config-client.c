/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include "my-rpc.h"

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

struct run_my_rpc_args
{
    int num_threads;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    int completed;
    double end_time;
    double start_time;
    double benchmark_seconds;
    uint64_t usec_per_worker_thread;
};

static void run_my_rpc(void *_arg);

static hg_id_t my_rpc_id;
static hg_id_t my_rpc_shutdown_id;

int main(int argc, char **argv) 
{
    struct run_my_rpc_args *args;
    ABT_thread *threads;
    int i;
    int ret;
    hg_return_t hret;
    ABT_xstream xstream;
    ABT_pool pool;
    margo_instance_id mid;
    hg_addr_t svr_addr = HG_ADDR_NULL;
    hg_handle_t handle;
    char proto[12] = {0};
    int concurrency = 0;
    double benchmark_seconds;
    int num_threads;
    double end_time;
    double start_time;
    long unsigned usec_per_worker_thread;
    int total_completed;
    int min_completed;
    int max_completed;
  
    if(argc != 6)
    {
        fprintf(stderr, "Usage: ./client <server_addr> <concurrency> <benchmark_seconds> <worker_threads_per_svr_rpc> <usec_per_worker_thread>\n");
        return(-1);
    }

    ret = sscanf(argv[2], "%d", &concurrency);
    assert(ret == 1);
    assert(concurrency > 0);
 
    ret = sscanf(argv[3], "%lf", &benchmark_seconds);
    assert(ret == 1);
    assert(benchmark_seconds > 0.0);
      
    ret = sscanf(argv[4], "%d", &num_threads);
    assert(ret == 1);
    assert(num_threads > -1);
    
    ret = sscanf(argv[5], "%lu", &usec_per_worker_thread);
    assert(ret == 1);
 
    args = calloc(concurrency, sizeof(*args));
    assert(args);
    threads = calloc(concurrency, sizeof(*threads));
    assert(threads);

    /* initialize Mercury using the transport portion of the destination
     * address (i.e., the part before the first : character if present)
     */
    for(i=0; i<11 && argv[1][i] != '\0' && argv[1][i] != ':'; i++)
        proto[i] = argv[1][i];

    /* actually start margo -- margo_init() encapsulates the Mercury &
     * Argobots initialization, so this step must precede their use. */
    /* Use main process to drive progress (it will relinquish control to
     * Mercury during blocking communication calls). No RPC threads are
     * used because this is a pure client that will not be servicing
     * rpc requests.
     */
    /***************************************/
    mid = margo_init(proto, MARGO_CLIENT_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
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

    /* register RPC */
	my_rpc_id = MARGO_REGISTER(mid, "my_rpc", my_rpc_in_t, my_rpc_out_t, NULL);
	my_rpc_shutdown_id = MARGO_REGISTER(mid, "my_shutdown_rpc", void, void, NULL);

    /* find addr for server */
    hret = margo_addr_lookup(mid, argv[1], &svr_addr);
    assert(hret == HG_SUCCESS);

    for(i=0; i<concurrency; i++)
    {
        args[i].num_threads = num_threads;
        args[i].mid = mid;
        args[i].svr_addr = svr_addr;
        args[i].benchmark_seconds = benchmark_seconds;
        args[i].usec_per_worker_thread = usec_per_worker_thread;

        /* Each ult gets a pointer to an element of the array to use
         * as input for the run_my_rpc() function.
         */
        ret = ABT_thread_create(pool, run_my_rpc, &args[i],
            ABT_THREAD_ATTR_NULL, &threads[i]);
        if(ret != 0)
        {
            fprintf(stderr, "Error: ABT_thread_create()\n");
            return(-1);
        }

    }

    /* yield to one of the threads */
    ABT_thread_yield_to(threads[0]);

    for(i=0; i<concurrency; i++)
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

    /* calculate throughput */
    end_time = args[0].end_time;
    start_time = args[0].start_time;
    total_completed = args[0].completed;
    min_completed = args[0].completed;
    max_completed = args[0].completed;
    //printf("# 0 completed %d\n", args[0].completed);
    for(i=1; i<concurrency; i++)
    {
        /* which thread was first to start or last to finish? */
        if(end_time < args[i].end_time)
            end_time = args[i].end_time;
        if(start_time > args[i].start_time)
            start_time = args[i].start_time;

        total_completed += args[i].completed;
        if(min_completed > args[i].completed)
            min_completed = args[i].completed;
        if(max_completed < args[i].completed)
            max_completed = args[i].completed;

        //printf("# %d completed %d\n", i, args[i].completed);
    }
    
    printf("%f ops/s\n", ((double)total_completed)/(end_time-start_time));
    printf("%f ratio of most busy to least busy ult\n", (double)max_completed / (double)min_completed);

    /* send one rpc to server to shut it down */

    /* create handle */
    hret = margo_create(mid, svr_addr, my_rpc_shutdown_id, &handle);
    assert(hret == HG_SUCCESS);

    hret = margo_forward(handle, NULL);
    assert(hret == HG_SUCCESS);

    margo_destroy(handle);
    margo_addr_free(mid, svr_addr);

    /* shut down everything */
    margo_finalize(mid);

    return(0);
}

static void run_my_rpc(void *_arg)
{
    struct run_my_rpc_args *arg = _arg;
    hg_handle_t handle;
    my_rpc_in_t in;
    my_rpc_out_t out;
    hg_return_t hret;
    hg_size_t size;
    void* buffer;

    arg->start_time = wtime();

    /* allocate buffer for bulk transfer */
    size = 512;
    buffer = calloc(1, 512);
    assert(buffer);

    /* register buffer for rdma/bulk access by server */
    hret = margo_bulk_create(arg->mid, 1, &buffer, &size, 
        HG_BULK_WRITE_ONLY, &in.bulk_handle);
    assert(hret == HG_SUCCESS);

    /* create handle */
    hret = margo_create(arg->mid, arg->svr_addr, my_rpc_id, &handle);
    assert(hret == HG_SUCCESS);

    in.num_threads = arg->num_threads;
    in.usec_per_thread = arg->usec_per_worker_thread;

    /* continuously send rpcs for the duration of this benchmark */
    while((wtime() - arg->start_time) < arg->benchmark_seconds)
    {

        /* Send rpc.  */ 
        hret = margo_forward(handle, &in);
        assert(hret == HG_SUCCESS);

        /* decode response */
        hret = margo_get_output(handle, &out);
        assert(hret == HG_SUCCESS);

        margo_free_output(handle, &out);
        arg->completed++;
    }

    /* clean up resources consumed by this rpc */
    margo_bulk_free(in.bulk_handle);
    margo_destroy(handle);
    free(buffer);

    arg->end_time = wtime();
    return;
}

