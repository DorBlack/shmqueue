/*
 * lfps_benchmark.c
 * Performance benchmark for different modes
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include "lockfree_pubsub.h"

#define BENCHMARK_DURATION 5
#define WARMUP_DURATION 1

typedef struct {
    lfps_ring_t* ring;
    int thread_id;
    int is_publisher;
    uint64_t operations;
    uint64_t bytes;
    int msg_size;
    volatile int* running;
} benchmark_thread_t;

static double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

void* publisher_worker(void* arg) {
    benchmark_thread_t* ctx = (benchmark_thread_t*)arg;
    char* buffer = malloc(ctx->msg_size);
    
    // Fill buffer with data
    memset(buffer, 'X', ctx->msg_size);
    snprintf(buffer, ctx->msg_size, "Thread-%d", ctx->thread_id);
    
    // Pin to CPU
    lfps_set_cpu_affinity(ctx->thread_id * 2);
    
    while (*ctx->running) {
        if (lfps_publish_notify(ctx->ring, buffer, ctx->msg_size, 0) == 0) {
            ctx->operations++;
            ctx->bytes += ctx->msg_size;
        }
    }
    
    free(buffer);
    return NULL;
}

void* subscriber_worker(void* arg) {
    benchmark_thread_t* ctx = (benchmark_thread_t*)arg;
    char* buffer = malloc(ctx->msg_size + 1024); // Extra space
    lfps_reader_t* reader = lfps_open_reader(0x12345678);
    
    if (!reader) {
        fprintf(stderr, "Failed to open reader\n");
        return NULL;
    }
    
    // Pin to CPU (different from publishers)
    lfps_set_cpu_affinity(ctx->thread_id * 2 + 1);
    
    while (*ctx->running) {
        int len = lfps_read(reader, buffer, ctx->msg_size + 1024, NULL, NULL);
        if (len > 0) {
            ctx->operations++;
            ctx->bytes += len;
            
            // Prefetch next for better performance
            lfps_prefetch_next(reader);
        }
    }
    
    lfps_reader_destroy(reader);
    free(buffer);
    return NULL;
}

void run_benchmark(const char* mode_name, int num_publishers, int num_subscribers, int msg_size) {
    printf("\n=== Benchmark: %s ===\n", mode_name);
    printf("Publishers: %d, Subscribers: %d, Message size: %d bytes\n", 
           num_publishers, num_subscribers, msg_size);
    
    // Create ring buffer
    uint32_t element_count = 10000;
    uint32_t element_size = msg_size + 128; // Extra space for header
    
    lfps_ring_t* ring = lfps_create(0x12345678, element_size, element_count);
    if (!ring) {
        fprintf(stderr, "Failed to create ring buffer\n");
        return;
    }
    
    // Thread contexts
    int total_threads = num_publishers + num_subscribers;
    pthread_t* threads = malloc(total_threads * sizeof(pthread_t));
    benchmark_thread_t* contexts = calloc(total_threads, sizeof(benchmark_thread_t));
    volatile int running = 1;
    
    // Initialize contexts
    for (int i = 0; i < total_threads; i++) {
        contexts[i].thread_id = i;
        contexts[i].msg_size = msg_size;
        contexts[i].running = &running;
        
        if (i < num_publishers) {
            contexts[i].ring = (i == 0) ? ring : lfps_open_publisher(0x12345678);
            contexts[i].is_publisher = 1;
        } else {
            contexts[i].is_publisher = 0;
        }
    }
    
    printf("Warming up...\n");
    
    // Start threads
    for (int i = 0; i < total_threads; i++) {
        if (contexts[i].is_publisher) {
            pthread_create(&threads[i], NULL, publisher_worker, &contexts[i]);
        } else {
            pthread_create(&threads[i], NULL, subscriber_worker, &contexts[i]);
        }
    }
    
    // Warmup
    sleep(WARMUP_DURATION);
    
    // Reset counters
    for (int i = 0; i < total_threads; i++) {
        contexts[i].operations = 0;
        contexts[i].bytes = 0;
    }
    
    printf("Benchmarking...\n");
    double start_time = get_time();
    
    // Run benchmark
    sleep(BENCHMARK_DURATION);
    
    // Stop threads
    running = 0;
    double end_time = get_time();
    double duration = end_time - start_time;
    
    // Wait for threads
    for (int i = 0; i < total_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Calculate results
    uint64_t total_pub_ops = 0, total_pub_bytes = 0;
    uint64_t total_sub_ops = 0, total_sub_bytes = 0;
    
    for (int i = 0; i < total_threads; i++) {
        if (contexts[i].is_publisher) {
            total_pub_ops += contexts[i].operations;
            total_pub_bytes += contexts[i].bytes;
        } else {
            total_sub_ops += contexts[i].operations;
            total_sub_bytes += contexts[i].bytes;
        }
    }
    
    // Print results
    printf("\nResults (duration: %.2f seconds):\n", duration);
    printf("Publishers:\n");
    printf("  Total messages: %llu\n", (unsigned long long)total_pub_ops);
    printf("  Rate: %.2f msg/s (%.2f msg/s per thread)\n", 
           total_pub_ops / duration, 
           total_pub_ops / duration / num_publishers);
    printf("  Throughput: %.2f MB/s\n", 
           total_pub_bytes / duration / (1024 * 1024));
    
    printf("Subscribers:\n");
    printf("  Total messages: %llu\n", (unsigned long long)total_sub_ops);
    printf("  Rate: %.2f msg/s (%.2f msg/s per thread)\n", 
           total_sub_ops / duration,
           total_sub_ops / duration / num_subscribers);
    printf("  Throughput: %.2f MB/s\n", 
           total_sub_bytes / duration / (1024 * 1024));
    
    // Get stats
    lfps_stats_t stats;
    lfps_get_stats(ring, &stats);
    printf("Ring buffer:\n");
    printf("  Buffer usage: %u%%\n", stats.buffer_usage_percent);
    printf("  Message loss rate: %.2f%%\n", 
           total_pub_ops > 0 ? 
           (total_pub_ops - total_sub_ops) * 100.0 / total_pub_ops : 0);
    
    // Cleanup
    for (int i = 1; i < num_publishers; i++) {
        if (contexts[i].ring) {
            lfps_destroy(contexts[i].ring);
        }
    }
    lfps_destroy(ring);
    free(threads);
    free(contexts);
}

int main(int argc, char* argv[]) {
    printf("Lock-free Publish-Subscribe Benchmark\n");
    printf("=====================================\n");
    
#ifdef LFPS_MODE_SPSC
    printf("Mode: SPSC (Single Producer Single Consumer)\n");
    const char* mode = "SPSC";
#elif defined(LFPS_MODE_MPSC)
    printf("Mode: MPSC (Multiple Producer Single Consumer)\n");
    const char* mode = "MPSC";
#elif defined(LFPS_MODE_SPMC)
    printf("Mode: SPMC (Single Producer Multiple Consumer)\n");
    const char* mode = "SPMC";
#else
    printf("Mode: MPMC (Multiple Producer Multiple Consumer)\n");
    const char* mode = "MPMC";
#endif
    
    // Different test scenarios
    struct {
        int publishers;
        int subscribers;
        int msg_size;
        const char* desc;
    } scenarios[] = {
        {1, 1, 64, "Small messages (64B)"},
        {1, 1, 256, "Medium messages (256B)"},
        {1, 1, 1024, "Large messages (1KB)"},
        {2, 2, 256, "2x2 medium messages"},
        {4, 4, 256, "4x4 medium messages"},
        {1, 4, 256, "1 pub -> 4 sub"},
        {4, 1, 256, "4 pub -> 1 sub"},
    };
    
    int num_scenarios = sizeof(scenarios) / sizeof(scenarios[0]);
    
    // Override with command line args if provided
    if (argc == 4) {
        int pub = atoi(argv[1]);
        int sub = atoi(argv[2]);
        int size = atoi(argv[3]);
        
        char desc[100];
        snprintf(desc, sizeof(desc), "%s %dx%d %dB", mode, pub, sub, size);
        run_benchmark(desc, pub, sub, size);
    } else {
        // Run all scenarios
        for (int i = 0; i < num_scenarios; i++) {
            char desc[100];
            snprintf(desc, sizeof(desc), "%s %s", mode, scenarios[i].desc);
            run_benchmark(desc, scenarios[i].publishers, 
                         scenarios[i].subscribers, scenarios[i].msg_size);
            
            if (i < num_scenarios - 1) {
                printf("\nPress Enter to continue...\n");
                getchar();
            }
        }
    }
    
    return 0;
}