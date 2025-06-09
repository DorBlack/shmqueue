/*
 * pubsub_publisher.c
 * Example publisher for the publish-subscribe ring buffer
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include "pubsub_ringbuffer.h"

static volatile int g_running = 1;

void signal_handler(int sig) {
    g_running = 0;
    printf("\nStopping publisher...\n");
}

int main(int argc, char* argv[]) {
    uint64_t shm_key = 0x12345678;
    uint32_t element_size = 4096;
    uint32_t element_count = 100;
    int publish_interval_ms = 100;
    int message_size = 256;
    
    // Parse arguments
    if (argc > 1) shm_key = strtoull(argv[1], NULL, 0);
    if (argc > 2) element_count = atoi(argv[2]);
    if (argc > 3) publish_interval_ms = atoi(argv[3]);
    if (argc > 4) message_size = atoi(argv[4]);
    
    printf("Publisher Configuration:\n");
    printf("  SHM Key: 0x%llx\n", (unsigned long long)shm_key);
    printf("  Element Size: %u bytes\n", element_size);
    printf("  Element Count: %u\n", element_count);
    printf("  Publish Interval: %d ms\n", publish_interval_ms);
    printf("  Message Size: %d bytes\n", message_size);
    printf("\n");
    
    // Create or open ring buffer
    pubsub_rb_t* rb = psrb_create(shm_key, element_size, element_count);
    if (!rb) {
        // Try to open existing
        rb = psrb_open_publisher(shm_key);
        if (!rb) {
            fprintf(stderr, "Failed to create/open ring buffer: %s\n", psrb_errorstr());
            return 1;
        }
        printf("Opened existing ring buffer\n");
    } else {
        printf("Created new ring buffer\n");
        if (shm_key == 0) {
            int shmid = psrb_get_shmid(rb);
            printf("Anonymous SHM ID: %d\n", shmid);
        }
    }
    
    // Setup signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Prepare message buffer
    char* message = malloc(message_size);
    if (!message) {
        fprintf(stderr, "Out of memory\n");
        psrb_destroy(rb);
        return 1;
    }
    
    uint64_t sequence = 0;
    psrb_stats_t stats;
    
    printf("\nPublishing messages... (Press Ctrl+C to stop)\n\n");
    
    while (g_running) {
        // Create message
        sequence++;
        snprintf(message, message_size, 
                 "Message #%llu from PID %d at %ld", 
                 (unsigned long long)sequence, getpid(), time(NULL));
        
        // Fill rest with pattern
        int header_len = strlen(message);
        for (int i = header_len; i < message_size - 1; i++) {
            message[i] = 'A' + (i % 26);
        }
        message[message_size - 1] = '\0';
        
        // Publish
        if (psrb_publish(rb, message, message_size) != 0) {
            fprintf(stderr, "Failed to publish: %s\n", psrb_errorstr());
            break;
        }
        
        // Print stats every 10 messages
        if (sequence % 10 == 0) {
            psrb_get_stats(rb, &stats);
            printf("Published: %llu, Overwrites: %llu, Buffer usage: %u%%\r",
                   (unsigned long long)stats.total_published,
                   (unsigned long long)stats.total_overwrites,
                   stats.buffer_usage_percent);
            fflush(stdout);
        }
        
        // Sleep
        usleep(publish_interval_ms * 1000);
    }
    
    // Final stats
    psrb_get_stats(rb, &stats);
    printf("\n\nFinal Statistics:\n");
    printf("  Total Published: %llu\n", (unsigned long long)stats.total_published);
    printf("  Total Overwrites: %llu\n", (unsigned long long)stats.total_overwrites);
    printf("  Buffer Usage: %u%%\n", stats.buffer_usage_percent);
    
    // Cleanup
    free(message);
    psrb_destroy(rb);
    
    return 0;
}