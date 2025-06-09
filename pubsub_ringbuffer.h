/*
 * pubsub_ringbuffer.h
 * A publish-subscribe ring buffer based on shared memory
 * 
 * Features:
 * 1) Publisher writes without caring about subscribers
 * 2) Multiple subscribers can read at their own pace
 * 3) Ring buffer overwrites old data when full
 * 4) Each subscriber maintains its own read position
 * 5) Subscribers can detect data loss (overwritten data)
 */

#ifndef __PUBSUB_RINGBUFFER_H__
#define __PUBSUB_RINGBUFFER_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/time.h>

#ifndef BOOL
#define BOOL int
#endif

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

typedef struct pubsub_rb pubsub_rb_t;
typedef struct pubsub_reader pubsub_reader_t;

// Create a new publish-subscribe ring buffer
// Parameters:
//   shm_key: shared memory key (0 for anonymous)
//   element_size: size of each element
//   element_count: number of elements in ring buffer
// Returns: handle to ring buffer or NULL on error
pubsub_rb_t* psrb_create(uint64_t shm_key, uint32_t element_size, uint32_t element_count);

// Open an existing ring buffer as publisher (for additional publishers)
pubsub_rb_t* psrb_open_publisher(uint64_t shm_key);

// Open an existing ring buffer as subscriber
pubsub_reader_t* psrb_open_subscriber(uint64_t shm_key);

// Open by shared memory ID (for anonymous shared memory)
pubsub_rb_t* psrb_open_publisher_by_shmid(int shm_id);
pubsub_reader_t* psrb_open_subscriber_by_shmid(int shm_id);

// Get shared memory ID (for anonymous shared memory)
int psrb_get_shmid(pubsub_rb_t* rb);
int psrb_reader_get_shmid(pubsub_reader_t* reader);

// Publish data to ring buffer
// This will overwrite old data if buffer is full
// Returns: 0 on success, -1 on error
int psrb_publish(pubsub_rb_t* rb, const void* data, uint32_t data_len);

// Read next available data
// Returns:
//   > 0: data length read
//   0: no new data available
//   -1: error
//   -2: data loss detected (some data was overwritten)
int psrb_read(pubsub_reader_t* reader, void* buffer, uint32_t buffer_size, 
              struct timeval* timestamp, uint64_t* sequence_num);

// Peek at next available data without consuming
int psrb_peek(pubsub_reader_t* reader, void* buffer, uint32_t buffer_size,
              struct timeval* timestamp, uint64_t* sequence_num);

// Get number of available messages for a reader
uint32_t psrb_available(pubsub_reader_t* reader);

// Get reader lag (how many messages behind the latest)
uint32_t psrb_get_lag(pubsub_reader_t* reader);

// Reset reader to latest position (skip all old messages)
void psrb_reset_to_latest(pubsub_reader_t* reader);

// Reset reader to oldest available position
void psrb_reset_to_oldest(pubsub_reader_t* reader);

// Get statistics
typedef struct {
    uint64_t total_published;
    uint64_t total_overwrites;
    uint32_t current_subscribers;
    uint32_t buffer_usage_percent;
} psrb_stats_t;

void psrb_get_stats(pubsub_rb_t* rb, psrb_stats_t* stats);

// Cleanup
void psrb_destroy(pubsub_rb_t* rb);
void psrb_destroy_and_remove(pubsub_rb_t* rb);
void psrb_reader_destroy(pubsub_reader_t* reader);

// Error handling
const char* psrb_errorstr(void);

#ifdef __cplusplus
}
#endif

#endif /* __PUBSUB_RINGBUFFER_H__ */