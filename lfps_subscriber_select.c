/*
 * lfps_subscriber_select.c
 * Lock-free subscriber with select() support
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <sys/select.h>
#include <pthread.h>
#include "lockfree_pubsub.h"

static volatile int g_running = 1;

void signal_handler(int sig) {
    g_running = 0;
    printf("\nStopping subscriber...\n");
}

typedef struct {
    int thread_id;
    lfps_reader_t* reader;
    int use_select;
    int batch_size;
    uint64_t consumed;
    uint64_t lost;
} subscriber_thread_data_t;

void* subscriber_thread(void* arg) {
    subscriber_thread_data_t* data = (subscriber_thread_data_t*)arg;
    char buffer[4096];
    struct timeval tv;
    uint64_t seq;
    int notify_fd = -1;
    
    // Pin to CPU
    lfps_set_cpu_affinity(data->thread_id % sysconf(_SC_NPROCESSORS_ONLN));
    
    printf("Subscriber thread %d started (select=%d)\n", 
           data->thread_id, data->use_select);
    
    // Setup notification if using select
    if (data->use_select) {
        // Enable notification with threshold
        if (lfps_reader_enable_notify(data->reader, data->batch_size) < 0) {
            fprintf(stderr, "Thread %d: Failed to enable notification\n", 
                    data->thread_id);
            return NULL;
        }
        
        notify_fd = lfps_reader_get_fd(data->reader);
        if (notify_fd < 0) {
            fprintf(stderr, "Thread %d: Failed to get notification fd\n", 
                    data->thread_id);
            return NULL;
        }
        
        printf("Thread %d: Notification enabled, fd=%d, threshold=%d\n",
               data->thread_id, notify_fd, data->batch_size);
    }
    
    while (g_running) {
        if (data->use_select) {
            // Use select to wait for data
            fd_set rfds;
            struct timeval timeout;
            
            FD_ZERO(&rfds);
            FD_SET(notify_fd, &rfds);
            
            // 100ms timeout
            timeout.tv_sec = 0;
            timeout.tv_usec = 100000;
            
            int ret = select(notify_fd + 1, &rfds, NULL, NULL, &timeout);
            if (ret > 0 && FD_ISSET(notify_fd, &rfds)) {
                // Notification received, consume event
                lfps_reader_consume_event(data->reader, 1);
                
                // Read batch of messages
                int batch_count = 0;
                while (batch_count < data->batch_size * 2) { // Read up to 2x threshold
                    int len = lfps_read(data->reader, buffer, sizeof(buffer), &tv, &seq);
                    if (len > 0) {
                        data->consumed++;
                        batch_count++;
                        
                        // Prefetch next message for performance
                        lfps_prefetch_next(data->reader);
                    } else if (len == -2) {
                        data->lost++;
                    } else {
                        break; // No more data
                    }
                }
                
                if (batch_count > 0) {
                    printf("Thread %d: Read %d messages after notification\n",
                           data->thread_id, batch_count);
                }
            }
        } else {
            // Busy polling mode
            int len = lfps_read(data->reader, buffer, sizeof(buffer), &tv, &seq);
            if (len > 0) {
                data->consumed++;
            } else if (len == -2) {
                data->lost++;
            } else {
                // No data, small sleep to avoid burning CPU
                usleep(100);
            }
        }
    }
    
    printf("Subscriber thread %d stopped: consumed=%llu, lost=%llu\n",
           data->thread_id, 
           (unsigned long long)data->consumed,
           (unsigned long long)data->lost);
    
    return NULL;
}

int main(int argc, char* argv[]) {
    uint64_t shm_key = 0x12345678;
    int num_threads = 2;
    int use_select = 1;
    int batch_size = 10;
    int duration_sec = 10;
    
    // Parse arguments
    if (argc > 1) shm_key = strtoull(argv[1], NULL, 0);
    if (argc > 2) num_threads = atoi(argv[2]);
    if (argc > 3) use_select = atoi(argv[3]);
    if (argc > 4) batch_size = atoi(argv[4]);
    if (argc > 5) duration_sec = atoi(argv[5]);
    
    printf("Lock-free Subscriber Configuration:\n");
    printf("  SHM Key: 0x%llx\n", (unsigned long long)shm_key);
    printf("  Subscriber Threads: %d\n", num_threads);
    printf("  Use Select: %s\n", use_select ? "Yes" : "No");
    printf("  Batch Size: %d\n", batch_size);
    printf("  Duration: %d sec\n", duration_sec);
    
#ifdef LFPS_MODE_SPSC
    printf("  Mode: SPSC\n");
#elif defined(LFPS_MODE_MPSC)
    printf("  Mode: MPSC\n");
#elif defined(LFPS_MODE_SPMC)
    printf("  Mode: SPMC\n");
#else
    printf("  Mode: MPMC\n");
#endif
    printf("\n");
    
    // Setup signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Create subscriber threads
    pthread_t* threads = malloc(num_threads * sizeof(pthread_t));
    subscriber_thread_data_t* thread_data = malloc(num_threads * sizeof(subscriber_thread_data_t));
    
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].reader = lfps_open_reader(shm_key);
        thread_data[i].use_select = use_select;
        thread_data[i].batch_size = batch_size;
        thread_data[i].consumed = 0;
        thread_data[i].lost = 0;
        
        if (!thread_data[i].reader) {
            fprintf(stderr, "Failed to open reader %d: %s\n", i, lfps_errorstr());
            continue;
        }
        
        pthread_create(&threads[i], NULL, subscriber_thread, &thread_data[i]);
    }
    
    // Run for specified duration
    time_t start_time = time(NULL);
    time_t last_stat_time = start_time;
    
    while (g_running && (time(NULL) - start_time < duration_sec)) {
        sleep(1);
        
        // Print aggregated stats
        time_t now = time(NULL);
        if (now > last_stat_time) {
            uint64_t total_consumed = 0;
            uint64_t total_lost = 0;
            
            for (int i = 0; i < num_threads; i++) {
                total_consumed += thread_data[i].consumed;
                total_lost += thread_data[i].lost;
            }
            
            printf("Total consumed: %llu, lost: %llu, rate: %llu msg/s\r",
                   (unsigned long long)total_consumed,
                   (unsigned long long)total_lost,
                   (unsigned long long)(total_consumed / (now - start_time)));
            fflush(stdout);
            
            last_stat_time = now;
        }
    }
    
    g_running = 0;
    
    // Wait for threads
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Final stats
    printf("\n\nFinal Statistics:\n");
    uint64_t total_consumed = 0;
    uint64_t total_lost = 0;
    
    for (int i = 0; i < num_threads; i++) {
        printf("  Thread %d: consumed=%llu, lost=%llu\n",
               i,
               (unsigned long long)thread_data[i].consumed,
               (unsigned long long)thread_data[i].lost);
        total_consumed += thread_data[i].consumed;
        total_lost += thread_data[i].lost;
    }
    
    printf("  Total: consumed=%llu, lost=%llu\n",
           (unsigned long long)total_consumed,
           (unsigned long long)total_lost);
    printf("  Average rate: %llu msg/s\n",
           (unsigned long long)(total_consumed / duration_sec));
    
    // Cleanup
    for (int i = 0; i < num_threads; i++) {
        if (thread_data[i].reader) {
            lfps_reader_destroy(thread_data[i].reader);
        }
    }
    
    free(threads);
    free(thread_data);
    
    return 0;
}