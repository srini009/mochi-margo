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
#include <string.h>

#include <abt.h>
#ifdef SNOOZE
#include <abt-snoozer.h>
#endif

static int worker_concurrency;
static int iterations;
static ABT_pool    shared_pool;
static long unsigned usec_sleep_per_thread;

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

static void worker(void *_arg)
{
    int *completed_sleeps = _arg;
    struct timespec req, rem;
    int ret;
#if 0
    ABT_xstream my_xstream;
#endif

    if(usec_sleep_per_thread > 0)
    {
        req.tv_sec = (usec_sleep_per_thread) / 1000000L;
        req.tv_nsec = ((usec_sleep_per_thread) % 1000000L) * 1000L;
        rem.tv_sec = 0;
        rem.tv_nsec = 0;
#if 0
        ABT_xstream_self(&my_xstream);
        printf("nanosleep %lu sec and %lu nsec on xstream %p\n", req.tv_sec, req.tv_nsec, my_xstream);
#endif
        ret = nanosleep(&req, &rem);
        assert(ret == 0);
        (*completed_sleeps)++;
    }
    
    return;
}

struct thread_state
{
    ABT_thread thread;
    int completed_sleeps;
};

static void spawner(void *_arg)
{
    int *completed_sleeps  = _arg;
    int i,j;
    struct thread_state *state_array;
    int ret;
    
    state_array = calloc(worker_concurrency, sizeof(*state_array));
    assert(state_array);

    for(i=0; i<iterations; i++)
    {
        memset(state_array, 0, (worker_concurrency*sizeof(*state_array)));
        /* create a new set of threads and wait for them to complete in each
         * iteration
         */
        for(j=0; j<worker_concurrency; j++)
        {
            ret = ABT_thread_create(shared_pool, worker, 
                &state_array[j].completed_sleeps, ABT_THREAD_ATTR_NULL, &state_array[j].thread);
            assert(ret == 0);
        }

        for(j=0; j<worker_concurrency; j++)
        {
            ret = ABT_thread_join(state_array[j].thread);
            assert(ret == 0);
            ret = ABT_thread_free(&state_array[j].thread);
            assert(ret == 0);

            assert(state_array[j].completed_sleeps == 1);
            (*completed_sleeps)++;
        }
    }
}

int main(int argc, char **argv) 
{
    ABT_xstream *xstreams;
    ABT_sched   *scheds;
    int i;
    int ret;
    int pool_size;
    int spawner_concurrency;
    double start_ts, end_ts;
    int max_real_concurrency;
    struct thread_state *state_array;
    int completed_sleeps = 0;

    if(argc != 6)
    {
        fprintf(stderr, "Usage: abt-pool-thread-sleep <pool_size> <spawner_worker_concurrency> <worker_worker_concurrency> <usec_sleep_per_thread> <iterations>\n");
        fprintf(stderr, "Example: abt-pool-thread-sleep 1 8 4 25 100\n");
        fprintf(stderr, "... will run 8 concurrent threads, each looping for 100 iterations, spawning 4 worker threads per iteration, with each worker thread sleeping 25 usec, in a pool with 1 ES.\n");
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

    ret = sscanf(argv[2], "%d", &spawner_concurrency);
    assert(ret == 1);
    assert(spawner_concurrency > 0);

    ret = sscanf(argv[3], "%d", &worker_concurrency);
    assert(ret == 1);
    assert(worker_concurrency > 0);

    ret = sscanf(argv[4], "%lu", &usec_sleep_per_thread);
    assert(ret == 1);
    assert(usec_sleep_per_thread >= 0);

    ret = sscanf(argv[5], "%d", &iterations);
    assert(ret == 1);
    assert(iterations >= 0);

    xstreams = calloc(pool_size, sizeof(*xstreams));
    assert(xstreams);
    scheds = calloc(pool_size, sizeof(*scheds));
    assert(scheds);
    state_array = calloc(spawner_concurrency, sizeof(*state_array));
    assert(state_array);


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
    for(i=0; i<spawner_concurrency; i++)
    {
        /* create a set of "spawner" threads that will run the iterations
         * that launch workers
         */
        ret = ABT_thread_create(shared_pool, spawner, 
            &state_array[i].completed_sleeps, ABT_THREAD_ATTR_NULL, &state_array[i].thread);
        assert(ret == 0);
    }

    /* wait for spawners to finish */
    for(i=0; i<spawner_concurrency; i++)
    {
        ret = ABT_thread_join(state_array[i].thread);
        assert(ret == 0);
        ret = ABT_thread_free(&state_array[i].thread);
        assert(ret == 0);

        assert(state_array[i].completed_sleeps == (iterations * worker_concurrency));
        completed_sleeps += state_array[i].completed_sleeps;
    }

    end_ts = wtime();
    
    max_real_concurrency = worker_concurrency * spawner_concurrency;
    if(max_real_concurrency > pool_size)
        max_real_concurrency = pool_size;

    printf("%f seconds elapsed (lowest possible would have been %f seconds)\n", (end_ts-start_ts), ((double)(usec_sleep_per_thread*iterations*spawner_concurrency*worker_concurrency))/((double)max_real_concurrency*(double)1000000));

    printf("params: %d total spawner threads, %d iterations per spawner thread, %d worker threads per iteration, each worker thread sleeping %lu usec, on a pool with %d ESs\n", spawner_concurrency, iterations, worker_concurrency, usec_sleep_per_thread, pool_size);

    assert((iterations*spawner_concurrency*worker_concurrency) == completed_sleeps);
    ABT_finalize();

    return(0);
}
