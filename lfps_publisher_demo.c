/*
 * lfps_publisher_demo.c
 * Lock-free publisher demonstration
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include "lockfree_pubsub.h"

static volatile int g_running = 1;
static uint64_t g_msg_count = 0;

void signal_handler(int sig) {
    g_running = 0;
    printf("\nStopping publishers...\n");
}

typedef struct {
    int thread_id;
    lfps_ring_t* ring;
    int msg_size;
    int interval_us;
    uint64_t published;
} publisher_thread_data_t;

void* publisher_thread(void* arg) {
    publisher_thread_data_t* data = (publisher_thread_data_t*)arg;
    char* buffer = malloc(data->msg_size);
    
    // Pin to CPU for better performance
    lfps_set_cpu_affinity(data->thread_id % sysconf(_SC_NPROCESSORS_ONLN));
    
    printf("Publisher thread %d started\n", data->thread_id);
    
    while (g_running) {
        // Create message
        snprintf(buffer, data->msg_size, 
                 "Thread-%d Msg-%llu Time-%ld", 
                 data->thread_id, 
                 (unsigned long long)data->published,
                 time(NULL));
        
        // Publish
        if (lfps_publish(data->ring, buffer, data->msg_size) == 0) {
            data->published++;
            __sync_fetch_and_add(&g_msg_count, 1);
        }
        
        // Sleep if interval specified
        if (data->interval_us > 0) {
            usleep(data->interval_us);
        }
    }
    
    free(buffer);
    printf("Publisher thread %d stopped: %llu messages\n", 
           data->thread_id, (unsigned long long)data->published);
    return NULL;
}

int main(int argc, char* argv[]) {
    uint64_t shm_key = 0x12345678;
    uint32_t element_size = 4096;
    uint32_t element_count = 1000;
    int num_threads = 2;
    int msg_size = 256;
    int interval_us = 1000;  // 1ms
    int duration_sec = 10;
    
    // Parse arguments
    if (argc > 1) shm_key = strtoull(argv[1], NULL, 0);
    if (argc > 2) num_threads = atoi(argv[2]);
    if (argc > 3) msg_size = atoi(argv[3]);
    if (argc > 4) interval_us = atoi(argv[4]);
    if (argc > 5) duration_sec = atoi(argv[5]);
    
    printf("Lock-free Publisher Configuration:\n");
    printf("  SHM Key: 0x%llx\n", (unsigned long long)shm_key);
    printf("  Element Size: %u\n", element_size);
    printf("  Element Count: %u\n", element_count);
    printf("  Publisher Threads: %d\n", num_threads);
    printf("  Message Size: %d\n", msg_size);
    printf("  Interval: %d us\n", interval_us);
    printf("  Duration: %d sec\n", duration_sec);
    
#ifdef LFPS_MODE_SPSC
    printf("  Mode: SPSC (Single Producer Single Consumer)\n");
#elif defined(LFPS_MODE_MPSC)
    printf("  Mode: MPSC (Multiple Producer Single Consumer)\n");
#elif defined(LFPS_MODE_SPMC)
    printf("  Mode: SPMC (Single Producer Multiple Consumer)\n");
#else
    printf("  Mode: MPMC (Multiple Producer Multiple Consumer)\n");
#endif
    printf("\n");
    
    // Create ring buffer
    lfps_ring_t* ring = lfps_create(shm_key, element_size, element_count);
    if (!ring) {
        fprintf(stderr, "Failed to create ring buffer: %s\n", lfps_errorstr());
        return 1;
    }
    
    // Setup signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Create publisher threads
    pthread_t* threads = malloc(num_threads * sizeof(pthread_t));
    publisher_thread_data_t* thread_data = malloc(num_threads * sizeof(publisher_thread_data_t));
    
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].ring = (i == 0) ? ring : lfps_open_publisher(shm_key);
        thread_data[i].msg_size = msg_size;
        thread_data[i].interval_us = interval_us;
        thread_data[i].published = 0;
        
        pthread_create(&threads[i], NULL, publisher_thread, &thread_data[i]);
    }
    
    // Run for specified duration
    time_t start_time = time(NULL);
    time_t last_stat_time = start_time;
    uint64_t last_msg_count = 0;
    
    while (g_running && (time(NULL) - start_time < duration_sec)) {
        sleep(1);
        
        // Print stats every second
        time_t now = time(NULL);
        if (now > last_stat_time) {
            uint64_t current_count = g_msg_count;
            uint64_t rate = current_count - last_msg_count;
            
            lfps_stats_t stats;
            lfps_get_stats(ring, &stats);
            
            printf("Rate: %llu msg/s, Total: %llu, Buffer: %u%%, Notif sent: %llu\r",
                   (unsigned long long)rate,
                   (unsigned long long)stats.total_published,
                   stats.buffer_usage_percent,
                   (unsigned long long)stats.notification_sent);
            fflush(stdout);
            
            last_stat_time = now;
            last_msg_count = current_count;
        }
    }
    
    g_running = 0;
    
    // Wait for threads
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        if (i > 0 && thread_data[i].ring) {
            lfps_destroy(thread_data[i].ring);
        }
    }
    
    // Final stats
    lfps_stats_t stats;
    lfps_get_stats(ring, &stats);
    
    printf("\n\nFinal Statistics:\n");
    printf("  Total Published: %llu\n", (unsigned long long)stats.total_published);
    printf("  Average Rate: %llu msg/s\n", 
           (unsigned long long)(stats.total_published / duration_sec));
    printf("  Notifications Sent: %llu\n", (unsigned long long)stats.notification_sent);
    printf("  Notifications Missed: %llu\n", (unsigned long long)stats.notification_missed);
    
    // Cleanup
    free(threads);
    free(thread_data);
    lfps_destroy(ring);
    
    return 0;
}