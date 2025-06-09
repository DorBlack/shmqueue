/*
 * test_pubsub.c
 * Test program for publish-subscribe ringbuffer
 */

#include "pubsub_ringbuffer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <pthread.h>
#include <time.h>

#define TEST_SHM_KEY 0x12345678
#define BUFFER_SIZE (1024 * 1024)  // 1MB
#define MAX_SUBSCRIBERS 10

// Test message structure
struct test_message {
    int seq;
    pid_t sender;
    char data[256];
};

// Publisher thread function
void* publisher_thread(void* arg) {
    int thread_id = *(int*)arg;
    struct psr_ringbuffer* rb = psr_open_publisher(TEST_SHM_KEY);
    if (!rb) {
        printf("Publisher %d: Failed to open ringbuffer: %s\n", thread_id, psr_errorstr());
        return NULL;
    }
    
    printf("Publisher %d: Started\n", thread_id);
    
    for (int i = 0; i < 100; i++) {
        struct test_message msg;
        msg.seq = i;
        msg.sender = getpid() * 1000 + thread_id;
        snprintf(msg.data, sizeof(msg.data), "Message %d from publisher %d", i, thread_id);
        
        // Test zero-copy publish
        if (i % 2 == 0) {
            struct psr_msg_header* header;
            struct test_message* buffer = (struct test_message*)psr_reserve(rb, sizeof(msg), &header);
            if (buffer) {
                *buffer = msg;
                psr_commit(rb, header);
                printf("Publisher %d: Published message %d (zero-copy)\n", thread_id, i);
            } else {
                printf("Publisher %d: Failed to reserve space for message %d\n", thread_id, i);
            }
        } else {
            // Test regular publish
            if (psr_publish(rb, &msg, sizeof(msg)) == 0) {
                printf("Publisher %d: Published message %d\n", thread_id, i);
            } else {
                printf("Publisher %d: Failed to publish message %d\n", thread_id, i);
            }
        }
        
        usleep(10000);  // 10ms
    }
    
    psr_destroy(rb);
    printf("Publisher %d: Finished\n", thread_id);
    return NULL;
}

// Subscriber process function
void subscriber_process(int sub_id) {
    struct psr_subscriber* sub = psr_open_subscriber(TEST_SHM_KEY);
    if (!sub) {
        printf("Subscriber %d: Failed to open subscriber: %s\n", sub_id, psr_errorstr());
        exit(1);
    }
    
    int event_fd = psr_subscriber_get_eventfd(sub);
    if (event_fd < 0) {
        printf("Subscriber %d: Failed to get event fd\n", sub_id);
        psr_subscriber_close(sub);
        exit(1);
    }
    
    printf("Subscriber %d: Started with event_fd=%d\n", sub_id, event_fd);
    
    int msg_count = 0;
    time_t start_time = time(NULL);
    
    while (time(NULL) - start_time < 10) {  // Run for 10 seconds
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(event_fd, &readfds);
        
        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        
        int ret = select(event_fd + 1, &readfds, NULL, NULL, &timeout);
        if (ret > 0 && FD_ISSET(event_fd, &readfds)) {
            psr_subscriber_consume_event(sub);
            
            // Read all available messages
            while (1) {
                size_t size;
                struct timeval timestamp;
                
                // Test zero-copy read
                const struct test_message* msg = (const struct test_message*)
                    psr_subscriber_read_next(sub, &size, &timestamp);
                
                if (!msg) break;
                
                printf("Subscriber %d: Received message seq=%d from sender=%d: %s\n",
                       sub_id, msg->seq, msg->sender, msg->data);
                msg_count++;
            }
        }
    }
    
    printf("Subscriber %d: Received %d messages total\n", sub_id, msg_count);
    psr_subscriber_close(sub);
}

// Test different memory ordering modes
void test_memory_ordering() {
    printf("\n=== Testing Memory Ordering Modes ===\n");
    
#if PSR_MEMORY_ORDER_MODE == 0
    printf("Using RELAXED memory ordering (single reader/writer mode)\n");
#elif PSR_MEMORY_ORDER_MODE == 1
    printf("Using ACQUIRE-RELEASE memory ordering (multiple readers/writers mode)\n");
#else
    printf("Using SEQUENTIAL CONSISTENCY memory ordering (strongest mode)\n");
#endif
}

int main(int argc, char* argv[]) {
    int num_publishers = 2;
    int num_subscribers = 3;
    
    if (argc > 1) num_publishers = atoi(argv[1]);
    if (argc > 2) num_subscribers = atoi(argv[2]);
    
    printf("Starting pubsub test with %d publishers and %d subscribers\n", 
           num_publishers, num_subscribers);
    
    test_memory_ordering();
    
    // Create the ringbuffer
    struct psr_ringbuffer* rb = psr_create(TEST_SHM_KEY, BUFFER_SIZE, MAX_SUBSCRIBERS);
    if (!rb) {
        printf("Failed to create ringbuffer: %s\n", psr_errorstr());
        return 1;
    }
    
    printf("Created ringbuffer with shm_id=%d\n", psr_get_shmid(rb));
    
    // Fork subscriber processes
    pid_t sub_pids[num_subscribers];
    for (int i = 0; i < num_subscribers; i++) {
        sub_pids[i] = fork();
        if (sub_pids[i] == 0) {
            // Child process
            subscriber_process(i);
            exit(0);
        }
    }
    
    // Give subscribers time to start
    sleep(1);
    
    // Create publisher threads
    pthread_t pub_threads[num_publishers];
    int thread_ids[num_publishers];
    
    for (int i = 0; i < num_publishers; i++) {
        thread_ids[i] = i;
        pthread_create(&pub_threads[i], NULL, publisher_thread, &thread_ids[i]);
    }
    
    // Wait for publishers to finish
    for (int i = 0; i < num_publishers; i++) {
        pthread_join(pub_threads[i], NULL);
    }
    
    // Wait for subscribers to finish
    for (int i = 0; i < num_subscribers; i++) {
        waitpid(sub_pids[i], NULL, 0);
    }
    
    // Print statistics
    struct psr_stats stats;
    if (psr_get_stats(rb, &stats) == 0) {
        printf("\n=== Final Statistics ===\n");
        printf("Total published: %lu messages\n", stats.total_published);
        printf("Total bytes: %lu\n", stats.total_bytes);
        printf("Active subscribers: %u\n", stats.active_subscribers);
        printf("Buffer usage: %u%%\n", stats.buffer_usage_percent);
    }
    
    // Cleanup
    psr_destroy_and_remove(rb);
    
    printf("\nTest completed successfully!\n");
    return 0;
}