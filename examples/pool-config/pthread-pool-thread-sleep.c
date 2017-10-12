/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include <pthread.h>

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

static void* worker(void *_arg)
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
    
    return(NULL);
}
   

int main(int argc, char **argv) 
{
    int i,j;
    int ret;
    int thread_concurrency;
    long unsigned usec_sleep_per_thread;
    pthread_t *threads;
    double start_ts, end_ts;
    int iterations;

    if(argc != 4)
    {
        fprintf(stderr, "Usage: pthread-pool-thread-sleep <thread_concurrency> <usec_sleep_per_thread> <iterations>\n");
        fprintf(stderr, "Example: pthread-pool-thread-sleep 4 25 100\n");
        fprintf(stderr, "... will run 100 iterations, spawning 4 threads per iteration, with each thread sleeping 25 usec.\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%d", &thread_concurrency);
    assert(ret == 1);
    assert(thread_concurrency > 0);

    ret = sscanf(argv[2], "%lu", &usec_sleep_per_thread);
    assert(ret == 1);
    assert(usec_sleep_per_thread >= 0);

    ret = sscanf(argv[3], "%d", &iterations);
    assert(ret == 1);
    assert(iterations >= 0);

    threads = calloc(thread_concurrency, sizeof(*threads));
    assert(threads);

    start_ts = wtime();

    /* run iterations */
    for(i=0; i<iterations; i++)
    {
        for(j=0; j<thread_concurrency; j++)
        {
            ret = pthread_create(&threads[j], NULL, worker, &usec_sleep_per_thread);
            assert(ret == 0);
        }
     
        for(j=0; j<thread_concurrency; j++)
        {
            ret = pthread_join(threads[j], NULL);
            assert(ret == 0);
        }   
    }
    end_ts = wtime();

    printf("%f seconds elapsed (lowest possible would have been %f seconds)\n", (end_ts-start_ts), ((double)(usec_sleep_per_thread*iterations*thread_concurrency))/((double)thread_concurrency*(double)1000000));

    printf("params: %d iterations, %d pthreads per iteration, each pthread sleeping %lu usec\n", iterations, thread_concurrency, usec_sleep_per_thread);


    return(0);
}
