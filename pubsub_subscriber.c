/*
 * pubsub_subscriber.c
 * Example subscriber for the publish-subscribe ring buffer
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
    printf("\nStopping subscriber...\n");
}

void print_message_info(const char* data, int len, struct timeval* tv, uint64_t seq) {
    printf("[SEQ:%llu] [TIME:%ld.%06ld] [LEN:%d] ", 
           (unsigned long long)seq,
           tv->tv_sec, tv->tv_usec, len);
    
    // Print first 60 chars of message
    printf("%.60s", data);
    if (len > 60) printf("...");
    printf("\n");
}

int main(int argc, char* argv[]) {
    uint64_t shm_key = 0x12345678;
    int read_interval_ms = 50;
    int start_from_oldest = 0;
    int verbose = 0;
    
    // Parse arguments
    if (argc > 1) shm_key = strtoull(argv[1], NULL, 0);
    if (argc > 2) read_interval_ms = atoi(argv[2]);
    if (argc > 3) start_from_oldest = atoi(argv[3]);
    if (argc > 4) verbose = atoi(argv[4]);
    
    printf("Subscriber Configuration:\n");
    printf("  SHM Key: 0x%llx\n", (unsigned long long)shm_key);
    printf("  Read Interval: %d ms\n", read_interval_ms);
    printf("  Start From: %s\n", start_from_oldest ? "Oldest" : "Latest");
    printf("  Verbose: %s\n", verbose ? "Yes" : "No");
    printf("\n");
    
    // Open ring buffer as subscriber
    pubsub_reader_t* reader = psrb_open_subscriber(shm_key);
    if (!reader) {
        fprintf(stderr, "Failed to open ring buffer: %s\n", psrb_errorstr());
        return 1;
    }
    
    if (start_from_oldest) {
        psrb_reset_to_oldest(reader);
        printf("Starting from oldest available message\n");
    } else {
        printf("Starting from latest message\n");
    }
    
    // Setup signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Prepare read buffer
    char* buffer = malloc(64 * 1024);  // 64KB buffer
    if (!buffer) {
        fprintf(stderr, "Out of memory\n");
        psrb_reader_destroy(reader);
        return 1;
    }
    
    uint64_t total_read = 0;
    uint64_t total_lost = 0;
    uint64_t last_seq = 0;
    int first_read = 1;
    
    printf("\nReading messages... (Press Ctrl+C to stop)\n\n");
    
    while (g_running) {
        struct timeval tv;
        uint64_t seq;
        
        int len = psrb_read(reader, buffer, 64 * 1024, &tv, &seq);
        
        if (len > 0) {
            total_read++;
            
            // Check for sequence gaps
            if (!first_read && seq != last_seq + 1) {
                uint64_t gap = seq - last_seq - 1;
                total_lost += gap;
                printf("*** WARNING: Lost %llu messages (seq %llu -> %llu) ***\n",
                       (unsigned long long)gap,
                       (unsigned long long)last_seq,
                       (unsigned long long)seq);
            }
            
            if (verbose) {
                print_message_info(buffer, len, &tv, seq);
            }
            
            last_seq = seq;
            first_read = 0;
            
            // Print stats every 100 messages
            if (total_read % 100 == 0 && !verbose) {
                uint32_t lag = psrb_get_lag(reader);
                printf("Read: %llu, Lost: %llu, Lag: %u messages\r",
                       (unsigned long long)total_read,
                       (unsigned long long)total_lost,
                       lag);
                fflush(stdout);
            }
        } else if (len == -2) {
            // Data loss detected
            printf("*** Data loss detected! Resetting to oldest available ***\n");
            psrb_reset_to_oldest(reader);
            first_read = 1;
        } else if (len < 0) {
            fprintf(stderr, "Read error: %s\n", psrb_errorstr());
            break;
        }
        
        // No data available, sleep
        if (len == 0) {
            usleep(read_interval_ms * 1000);
        }
    }
    
    // Final stats
    printf("\n\nFinal Statistics:\n");
    printf("  Total Messages Read: %llu\n", (unsigned long long)total_read);
    printf("  Total Messages Lost: %llu\n", (unsigned long long)total_lost);
    printf("  Messages Available: %u\n", psrb_available(reader));
    printf("  Current Lag: %u messages\n", psrb_get_lag(reader));
    
    // Cleanup
    free(buffer);
    psrb_reader_destroy(reader);
    
    return 0;
}