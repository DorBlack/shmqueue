/*
 * perf_test.c
 * Performance test for publish-subscribe ringbuffer
 */

#include "pubsub_ringbuffer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>

#define TEST_SHM_KEY 0x87654321
#define BUFFER_SIZE (16 * 1024 * 1024)  // 16MB
#define MAX_SUBSCRIBERS 10

// Test configuration
struct test_config {
    int num_publishers;
    int num_subscribers;
    int messages_per_publisher;
    int message_size;
    int use_zero_copy;
    int test_duration_sec;
};

// Test statistics
struct test_stats {
    uint64_t messages_sent;
    uint64_t messages_received;
    uint64_t bytes_sent;
    uint64_t bytes_received;
    double publish_time_ms;
    double subscribe_time_ms;
};

volatile int g_running = 1;

void signal_handler(int sig) {
    g_running = 0;
}

// Get current time in milliseconds
double get_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

// Publisher thread
void* publisher_thread(void* arg) {
    struct test_config* config = (struct test_config*)arg;
    struct psr_ringbuffer* rb = psr_open_publisher(TEST_SHM_KEY);
    if (!rb) {
        printf("Publisher: Failed to open ringbuffer\n");
        return NULL;
    }
    
    char* buffer = malloc(config->message_size);
    memset(buffer, 'A', config->message_size);
    
    uint64_t messages_sent = 0;
    double start_time = get_time_ms();
    
    while (g_running && messages_sent < config->messages_per_publisher) {
        if (config->use_zero_copy) {
            struct psr_msg_header* header;
            void* msg_buffer = psr_reserve(rb, config->message_size, &header);
            if (msg_buffer) {
                memcpy(msg_buffer, buffer, config->message_size);
                psr_commit(rb, header);
                messages_sent++;
            }
        } else {
            if (psr_publish(rb, buffer, config->message_size) == 0) {
                messages_sent++;
            }
        }
    }
    
    double end_time = get_time_ms();
    double duration = end_time - start_time;
    
    struct test_stats* stats = malloc(sizeof(struct test_stats));
    stats->messages_sent = messages_sent;
    stats->bytes_sent = messages_sent * config->message_size;
    stats->publish_time_ms = duration;
    
    free(buffer);
    psr_destroy(rb);
    
    return stats;
}

// Subscriber thread
void* subscriber_thread(void* arg) {
    struct test_config* config = (struct test_config*)arg;
    struct psr_subscriber* sub = psr_open_subscriber(TEST_SHM_KEY);
    if (!sub) {
        printf("Subscriber: Failed to open subscriber\n");
        return NULL;
    }
    
    uint64_t messages_received = 0;
    uint64_t bytes_received = 0;
    double start_time = get_time_ms();
    
    // Skip to latest to start fresh
    psr_subscriber_skip_to_latest(sub);
    
    while (g_running) {
        size_t size;
        const void* data = psr_subscriber_read_next(sub, &size, NULL);
        if (data) {
            messages_received++;
            bytes_received += size;
        } else {
            usleep(100);  // Small sleep when no data
        }
    }
    
    double end_time = get_time_ms();
    double duration = end_time - start_time;
    
    struct test_stats* stats = malloc(sizeof(struct test_stats));
    stats->messages_received = messages_received;
    stats->bytes_received = bytes_received;
    stats->subscribe_time_ms = duration;
    
    psr_subscriber_close(sub);
    
    return stats;
}

// Run performance test
void run_perf_test(struct test_config* config) {
    printf("\n=== Performance Test Configuration ===\n");
    printf("Publishers: %d\n", config->num_publishers);
    printf("Subscribers: %d\n", config->num_subscribers);
    printf("Messages per publisher: %d\n", config->messages_per_publisher);
    printf("Message size: %d bytes\n", config->message_size);
    printf("Zero-copy: %s\n", config->use_zero_copy ? "Yes" : "No");
    printf("Test duration: %d seconds\n", config->test_duration_sec);
    
#if PSR_MEMORY_ORDER_MODE == 0
    printf("Memory ordering: RELAXED\n");
#elif PSR_MEMORY_ORDER_MODE == 1
    printf("Memory ordering: ACQUIRE-RELEASE\n");
#else
    printf("Memory ordering: SEQUENTIAL CONSISTENCY\n");
#endif
    
    // Create ringbuffer
    struct psr_ringbuffer* rb = psr_create(TEST_SHM_KEY, BUFFER_SIZE, MAX_SUBSCRIBERS);
    if (!rb) {
        printf("Failed to create ringbuffer\n");
        return;
    }
    
    // Start subscribers
    pthread_t sub_threads[config->num_subscribers];
    for (int i = 0; i < config->num_subscribers; i++) {
        pthread_create(&sub_threads[i], NULL, subscriber_thread, config);
    }
    
    usleep(100000);  // Let subscribers start
    
    // Start publishers
    pthread_t pub_threads[config->num_publishers];
    double test_start = get_time_ms();
    
    for (int i = 0; i < config->num_publishers; i++) {
        pthread_create(&pub_threads[i], NULL, publisher_thread, config);
    }
    
    // Run for specified duration
    sleep(config->test_duration_sec);
    g_running = 0;
    
    // Collect publisher statistics
    struct test_stats total_pub_stats = {0};
    for (int i = 0; i < config->num_publishers; i++) {
        struct test_stats* stats;
        pthread_join(pub_threads[i], (void**)&stats);
        if (stats) {
            total_pub_stats.messages_sent += stats->messages_sent;
            total_pub_stats.bytes_sent += stats->bytes_sent;
            free(stats);
        }
    }
    
    // Collect subscriber statistics
    struct test_stats total_sub_stats = {0};
    for (int i = 0; i < config->num_subscribers; i++) {
        struct test_stats* stats;
        pthread_join(sub_threads[i], (void**)&stats);
        if (stats) {
            total_sub_stats.messages_received += stats->messages_received;
            total_sub_stats.bytes_received += stats->bytes_received;
            free(stats);
        }
    }
    
    double test_duration = (get_time_ms() - test_start) / 1000.0;
    
    // Print results
    printf("\n=== Performance Test Results ===\n");
    printf("Test duration: %.2f seconds\n", test_duration);
    printf("\nPublisher Statistics:\n");
    printf("  Total messages sent: %lu\n", total_pub_stats.messages_sent);
    printf("  Total bytes sent: %lu (%.2f MB)\n", 
           total_pub_stats.bytes_sent, total_pub_stats.bytes_sent / (1024.0 * 1024.0));
    printf("  Throughput: %.2f messages/sec\n", total_pub_stats.messages_sent / test_duration);
    printf("  Bandwidth: %.2f MB/sec\n", 
           (total_pub_stats.bytes_sent / (1024.0 * 1024.0)) / test_duration);
    
    printf("\nSubscriber Statistics (average per subscriber):\n");
    uint64_t avg_messages = total_sub_stats.messages_received / config->num_subscribers;
    uint64_t avg_bytes = total_sub_stats.bytes_received / config->num_subscribers;
    printf("  Average messages received: %lu\n", avg_messages);
    printf("  Average bytes received: %lu (%.2f MB)\n", 
           avg_bytes, avg_bytes / (1024.0 * 1024.0));
    printf("  Average throughput: %.2f messages/sec\n", avg_messages / test_duration);
    printf("  Average bandwidth: %.2f MB/sec\n", 
           (avg_bytes / (1024.0 * 1024.0)) / test_duration);
    
    // Clean up
    psr_destroy_and_remove(rb);
}

int main(int argc, char* argv[]) {
    struct test_config config = {
        .num_publishers = 4,
        .num_subscribers = 4,
        .messages_per_publisher = 1000000,
        .message_size = 1024,
        .use_zero_copy = 1,
        .test_duration_sec = 5
    };
    
    // Parse command line arguments
    if (argc > 1) config.num_publishers = atoi(argv[1]);
    if (argc > 2) config.num_subscribers = atoi(argv[2]);
    if (argc > 3) config.message_size = atoi(argv[3]);
    if (argc > 4) config.use_zero_copy = atoi(argv[4]);
    if (argc > 5) config.test_duration_sec = atoi(argv[5]);
    
    signal(SIGINT, signal_handler);
    
    printf("Publish-Subscribe Ringbuffer Performance Test\n");
    
    // Run test with zero-copy
    g_running = 1;
    config.use_zero_copy = 1;
    run_perf_test(&config);
    
    sleep(1);
    
    // Run test without zero-copy
    g_running = 1;
    config.use_zero_copy = 0;
    run_perf_test(&config);
    
    return 0;
}