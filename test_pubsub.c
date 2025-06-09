/*
 * test_pubsub.c
 * Comprehensive test for the publish-subscribe ring buffer
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <time.h>
#include "pubsub_ringbuffer.h"

#define SHM_KEY 0x99887766
#define ELEMENT_SIZE 1024
#define ELEMENT_COUNT 50
#define NUM_PUBLISHERS 3
#define NUM_SUBSCRIBERS 5
#define MESSAGES_PER_PUBLISHER 100

typedef struct {
    int id;
    pubsub_rb_t* rb;
    int message_count;
} publisher_data_t;

typedef struct {
    int id;
    pubsub_reader_t* reader;
    int start_from_oldest;
} subscriber_data_t;

// Publisher thread
void* publisher_thread(void* arg) {
    publisher_data_t* data = (publisher_data_t*)arg;
    char message[256];
    
    printf("Publisher %d started\n", data->id);
    
    for (int i = 0; i < data->message_count; i++) {
        snprintf(message, sizeof(message), 
                 "Publisher-%d Message-%d Time-%ld", 
                 data->id, i, time(NULL));
        
        if (psrb_publish(data->rb, message, strlen(message) + 1) != 0) {
            fprintf(stderr, "Publisher %d: Failed to publish\n", data->id);
        }
        
        // Random delay between 1-10ms
        usleep((rand() % 10 + 1) * 1000);
    }
    
    printf("Publisher %d finished\n", data->id);
    return NULL;
}

// Subscriber thread
void* subscriber_thread(void* arg) {
    subscriber_data_t* data = (subscriber_data_t*)arg;
    char buffer[ELEMENT_SIZE];
    struct timeval tv;
    uint64_t seq;
    int read_count = 0;
    int loss_count = 0;
    
    printf("Subscriber %d started\n", data->id);
    
    if (data->start_from_oldest) {
        psrb_reset_to_oldest(data->reader);
    }
    
    // Read for a fixed duration
    time_t start_time = time(NULL);
    while (time(NULL) - start_time < 10) { // Run for 10 seconds
        int len = psrb_read(data->reader, buffer, sizeof(buffer), &tv, &seq);
        
        if (len > 0) {
            read_count++;
            if (read_count % 50 == 0) {
                printf("Subscriber %d: Read %d messages\n", data->id, read_count);
            }
        } else if (len == -2) {
            loss_count++;
            psrb_reset_to_oldest(data->reader);
        }
        
        // Small delay
        usleep(5000); // 5ms
    }
    
    printf("Subscriber %d finished: Read=%d, Loss detected=%d\n", 
           data->id, read_count, loss_count);
    return NULL;
}

int main() {
    printf("=== Publish-Subscribe Ring Buffer Test ===\n\n");
    
    // Create ring buffer
    printf("Creating ring buffer...\n");
    pubsub_rb_t* rb = psrb_create(SHM_KEY, ELEMENT_SIZE, ELEMENT_COUNT);
    if (!rb) {
        fprintf(stderr, "Failed to create ring buffer: %s\n", psrb_errorstr());
        return 1;
    }
    
    // Create publishers
    pthread_t pub_threads[NUM_PUBLISHERS];
    publisher_data_t pub_data[NUM_PUBLISHERS];
    
    printf("\nStarting %d publishers...\n", NUM_PUBLISHERS);
    for (int i = 0; i < NUM_PUBLISHERS; i++) {
        pub_data[i].id = i;
        pub_data[i].rb = (i == 0) ? rb : psrb_open_publisher(SHM_KEY);
        pub_data[i].message_count = MESSAGES_PER_PUBLISHER;
        
        if (!pub_data[i].rb) {
            fprintf(stderr, "Failed to open publisher %d\n", i);
            continue;
        }
        
        pthread_create(&pub_threads[i], NULL, publisher_thread, &pub_data[i]);
    }
    
    // Create subscribers
    pthread_t sub_threads[NUM_SUBSCRIBERS];
    subscriber_data_t sub_data[NUM_SUBSCRIBERS];
    
    printf("\nStarting %d subscribers...\n", NUM_SUBSCRIBERS);
    for (int i = 0; i < NUM_SUBSCRIBERS; i++) {
        sub_data[i].id = i;
        sub_data[i].reader = psrb_open_subscriber(SHM_KEY);
        sub_data[i].start_from_oldest = (i % 2); // Half start from oldest
        
        if (!sub_data[i].reader) {
            fprintf(stderr, "Failed to open subscriber %d\n", i);
            continue;
        }
        
        pthread_create(&sub_threads[i], NULL, subscriber_thread, &sub_data[i]);
    }
    
    // Wait for publishers to finish
    printf("\nWaiting for publishers to finish...\n");
    for (int i = 0; i < NUM_PUBLISHERS; i++) {
        pthread_join(pub_threads[i], NULL);
        if (i > 0 && pub_data[i].rb) {
            psrb_destroy(pub_data[i].rb);
        }
    }
    
    // Wait for subscribers to finish
    printf("\nWaiting for subscribers to finish...\n");
    for (int i = 0; i < NUM_SUBSCRIBERS; i++) {
        pthread_join(sub_threads[i], NULL);
        if (sub_data[i].reader) {
            psrb_reader_destroy(sub_data[i].reader);
        }
    }
    
    // Print final statistics
    psrb_stats_t stats;
    psrb_get_stats(rb, &stats);
    
    printf("\n=== Final Statistics ===\n");
    printf("Total Published: %llu\n", (unsigned long long)stats.total_published);
    printf("Total Overwrites: %llu\n", (unsigned long long)stats.total_overwrites);
    printf("Buffer Usage: %u%%\n", stats.buffer_usage_percent);
    printf("Expected Messages: %d\n", NUM_PUBLISHERS * MESSAGES_PER_PUBLISHER);
    
    // Cleanup
    psrb_destroy_and_remove(rb);
    
    printf("\nTest completed!\n");
    return 0;
}