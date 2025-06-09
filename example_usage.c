/*
 * example_usage.c
 * Simple example showing how to use the publish-subscribe ringbuffer
 * 
 * This example simulates a sensor data collection system where:
 * - Multiple sensors (publishers) send data
 * - Multiple monitors (subscribers) receive and process data
 */

#include "pubsub_ringbuffer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define SHM_KEY 0xABCDEF00
#define BUFFER_SIZE (1024 * 1024)  // 1MB
#define MAX_MONITORS 5

// Sensor data structure
struct sensor_data {
    int sensor_id;
    time_t timestamp;
    float temperature;
    float humidity;
    float pressure;
    char location[32];
};

// Sensor publisher function
void sensor_publisher(int sensor_id, const char* location) {
    struct psr_ringbuffer* rb = psr_open_publisher(SHM_KEY);
    if (!rb) {
        printf("Sensor %d: Failed to connect to data bus\n", sensor_id);
        return;
    }
    
    printf("Sensor %d at %s: Started publishing\n", sensor_id, location);
    
    // Simulate sensor readings
    for (int i = 0; i < 10; i++) {
        struct sensor_data data = {
            .sensor_id = sensor_id,
            .timestamp = time(NULL),
            .temperature = 20.0 + (rand() % 100) / 10.0,
            .humidity = 40.0 + (rand() % 400) / 10.0,
            .pressure = 1000.0 + (rand() % 100) / 10.0
        };
        strncpy(data.location, location, sizeof(data.location) - 1);
        
        // Use zero-copy publish for efficiency
        struct psr_msg_header* header;
        struct sensor_data* buffer = (struct sensor_data*)psr_reserve(rb, sizeof(data), &header);
        if (buffer) {
            *buffer = data;
            psr_commit(rb, header);
            printf("Sensor %d: Published data - Temp:%.1f°C, Humidity:%.1f%%, Pressure:%.1fhPa\n",
                   sensor_id, data.temperature, data.humidity, data.pressure);
        }
        
        sleep(1);  // Simulate sensor read interval
    }
    
    printf("Sensor %d: Finished\n", sensor_id);
    psr_destroy(rb);
}

// Monitor subscriber function
void monitor_subscriber(const char* monitor_name) {
    struct psr_subscriber* sub = psr_open_subscriber(SHM_KEY);
    if (!sub) {
        printf("Monitor %s: Failed to connect to data bus\n", monitor_name);
        return;
    }
    
    int event_fd = psr_subscriber_get_eventfd(sub);
    if (event_fd < 0) {
        printf("Monitor %s: Failed to get event notification\n", monitor_name);
        psr_subscriber_close(sub);
        return;
    }
    
    printf("Monitor %s: Started monitoring sensor data\n", monitor_name);
    
    // Statistics
    float temp_sum = 0, humidity_sum = 0, pressure_sum = 0;
    int count = 0;
    
    // Monitor for 15 seconds
    time_t start_time = time(NULL);
    while (time(NULL) - start_time < 15) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(event_fd, &readfds);
        
        struct timeval timeout = { .tv_sec = 1, .tv_usec = 0 };
        
        int ret = select(event_fd + 1, &readfds, NULL, NULL, &timeout);
        if (ret > 0 && FD_ISSET(event_fd, &readfds)) {
            psr_subscriber_consume_event(sub);
            
            // Read all available sensor data
            while (1) {
                size_t size;
                struct timeval msg_time;
                const struct sensor_data* data = (const struct sensor_data*)
                    psr_subscriber_read_next(sub, &size, &msg_time);
                
                if (!data) break;
                
                printf("Monitor %s: Received from Sensor %d at %s - Temp:%.1f°C, Humidity:%.1f%%, Pressure:%.1fhPa\n",
                       monitor_name, data->sensor_id, data->location,
                       data->temperature, data->humidity, data->pressure);
                
                // Update statistics
                temp_sum += data->temperature;
                humidity_sum += data->humidity;
                pressure_sum += data->pressure;
                count++;
                
                // Alert on anomalies
                if (data->temperature > 30.0) {
                    printf("Monitor %s: ALERT! High temperature detected at %s: %.1f°C\n",
                           monitor_name, data->location, data->temperature);
                }
            }
        }
    }
    
    // Print summary
    if (count > 0) {
        printf("\nMonitor %s: Summary after monitoring %d readings:\n", monitor_name, count);
        printf("  Average Temperature: %.1f°C\n", temp_sum / count);
        printf("  Average Humidity: %.1f%%\n", humidity_sum / count);
        printf("  Average Pressure: %.1fhPa\n", pressure_sum / count);
    }
    
    psr_subscriber_close(sub);
}

int main() {
    printf("=== Sensor Data Collection System Example ===\n\n");
    
    // Create the data bus (ringbuffer)
    struct psr_ringbuffer* rb = psr_create(SHM_KEY, BUFFER_SIZE, MAX_MONITORS);
    if (!rb) {
        printf("Failed to create data bus: %s\n", psr_errorstr());
        return 1;
    }
    
    printf("Data bus created successfully\n\n");
    
    // Fork sensor processes
    pid_t pid;
    
    // Sensor 1
    pid = fork();
    if (pid == 0) {
        sensor_publisher(1, "Room A");
        exit(0);
    }
    
    // Sensor 2
    pid = fork();
    if (pid == 0) {
        sensor_publisher(2, "Room B");
        exit(0);
    }
    
    // Sensor 3
    pid = fork();
    if (pid == 0) {
        sensor_publisher(3, "Outside");
        exit(0);
    }
    
    // Fork monitor processes
    
    // Monitor 1
    pid = fork();
    if (pid == 0) {
        monitor_subscriber("Central");
        exit(0);
    }
    
    // Monitor 2
    pid = fork();
    if (pid == 0) {
        sleep(2);  // Start monitoring a bit later
        monitor_subscriber("Backup");
        exit(0);
    }
    
    // Parent process waits for all children
    int status;
    while (wait(&status) > 0);
    
    // Get final statistics
    struct psr_stats stats;
    if (psr_get_stats(rb, &stats) == 0) {
        printf("\n=== Final System Statistics ===\n");
        printf("Total messages published: %lu\n", stats.total_published);
        printf("Total data transferred: %lu bytes\n", stats.total_bytes);
        printf("Active monitors: %u\n", stats.active_subscribers);
    }
    
    // Clean up
    psr_destroy_and_remove(rb);
    
    printf("\nExample completed successfully!\n");
    return 0;
}