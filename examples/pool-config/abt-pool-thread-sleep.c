/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#define SNOOZE 1

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include <abt.h>
#ifdef SNOOZE
#include <abt-snoozer.h>
#endif

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

static void worker(void *_arg)
{
    uint64_t *usec_per_thread = _arg;
    struct timespec req, rem;
    int ret;
#if 0
    ABT_xstream my_xstream;
#endif

    if(*usec_per_thread > 0)
    {
        req.tv_sec = (*usec_per_thread) / 1000000L;
        req.tv_nsec = ((*usec_per_thread) % 1000000L) * 1000L;
        rem.tv_sec = 0;
        rem.tv_nsec = 0;
#if 0
        ABT_xstream_self(&my_xstream);
        printf("nanosleep %lu sec and %lu nsec on xstream %p\n", req.tv_sec, req.tv_nsec, my_xstream);
#endif
        ret = nanosleep(&req, &rem);
        assert(ret == 0);
    }
    
    return;
}



int main(int argc, char **argv) 
{
    ABT_xstream *xstreams;
    ABT_sched   *scheds;
    ABT_pool    shared_pool;
    int i,j;
    int ret;
    int pool_size;
    int thread_concurrency;
    long unsigned usec_sleep_per_thread;
    int iterations;
    ABT_thread *threads;
    double start_ts, end_ts;
    int max_real_concurrency;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: abt-pool-thread-sleep <pool_size> <thread_concurrency> <usec_sleep_per_thread> <iterations>\n");
        fprintf(stderr, "Example: abt-pool-thread-sleep 1 4 25 100\n");
        fprintf(stderr, "... will run 100 iterations, spawning 4 threads per iteration, with each thread sleeping 25 usec, in a pool with 1 ES.\n");
        return(-1);
    }

    ret = ABT_init(argc, argv);
    assert(ret == 0);

#ifdef SNOOZE
    /* set primary ES to idle without polling in the scheduler */
    ret = ABT_snoozer_xstream_self_set();
#endif

    ret = sscanf(argv[1], "%d", &pool_size);
    assert(ret == 1);
    assert(pool_size > 0);

    ret = sscanf(argv[2], "%d", &thread_concurrency);
    assert(ret == 1);
    assert(thread_concurrency > 0);

    ret = sscanf(argv[3], "%lu", &usec_sleep_per_thread);
    assert(ret == 1);
    assert(usec_sleep_per_thread >= 0);

    ret = sscanf(argv[4], "%d", &iterations);
    assert(ret == 1);
    assert(iterations >= 0);

    xstreams = calloc(pool_size, sizeof(*xstreams));
    assert(xstreams);
    scheds = calloc(pool_size, sizeof(*scheds));
    assert(scheds);
    threads = calloc(thread_concurrency, sizeof(*threads));
    assert(threads);

#ifdef SNOOZE
    ret = ABT_snoozer_xstream_create(pool_size, &shared_pool,
        xstreams);
    assert(ret == 0);
#else
    /* Create a shared pool */
    ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC,
                          ABT_TRUE, &shared_pool);
    /* Create schedulers for shared pool */
    for (i = 0; i < pool_size; i++) {
        ABT_sched_create_basic(ABT_SCHED_DEFAULT, 1, &shared_pool,
                               ABT_SCHED_CONFIG_NULL, &scheds[i]);
    }
    /* create ESs */
    for (i = 0; i < pool_size; i++) {
        ABT_xstream_create(scheds[i], &xstreams[i]);
    }
#endif

    start_ts = wtime();

    /* run iterations */
    for(i=0; i<iterations; i++)
    {
        /* create a new set of threads and wait for them to complete in each
         * iteration
         */
        for(j=0; j<thread_concurrency; j++)
        {
            ret = ABT_thread_create(shared_pool, worker, 
                &usec_sleep_per_thread, ABT_THREAD_ATTR_NULL, &threads[j]);
            assert(ret == 0);
        }

        for(j=0; j<thread_concurrency; j++)
        {
            ret = ABT_thread_join(threads[j]);
            assert(ret == 0);
            ret = ABT_thread_free(&threads[j]);
            assert(ret == 0);
        }
    }
    end_ts = wtime();
    
    max_real_concurrency = thread_concurrency;
    if(max_real_concurrency > pool_size)
        max_real_concurrency = pool_size;

    printf("%f seconds elapsed (lowest possible would have been %f seconds)\n", (end_ts-start_ts), ((double)(usec_sleep_per_thread*iterations*thread_concurrency))/((double)max_real_concurrency*(double)1000000));

    printf("params: %d iterations, %d ULTs per iteration, each ULT sleeping %lu usec, on a pool with %d ESs\n", iterations, thread_concurrency, usec_sleep_per_thread, pool_size);


    ABT_finalize();

    return(0);
}
