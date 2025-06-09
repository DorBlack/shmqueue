/*
 * pubsub_ringbuffer.h
 * Lock-free publish-subscribe ringbuffer implementation
 * 
 * Features:
 * 1) Support multiple publishers and multiple subscribers
 * 2) Lock-free implementation with configurable memory ordering
 * 3) FIFO notification support for select/poll
 * 4) Zero-copy interface to minimize memory copies
 * 5) Each subscriber can read all published data independently
 */

#ifndef __PUBSUB_RINGBUFFER_H__
#define __PUBSUB_RINGBUFFER_H__

#include <stdint.h>
#include <sys/time.h>

#ifdef __cplusplus
extern "C" {
#endif

// Configurable memory ordering modes
#ifndef PSR_MEMORY_ORDER_MODE
#define PSR_MEMORY_ORDER_MODE 0  // 0: relaxed, 1: acquire-release, 2: sequential
#endif

// Memory ordering macros based on mode
#if PSR_MEMORY_ORDER_MODE == 0
    // Relaxed ordering for maximum performance (single reader/writer)
    #define PSR_LOAD_ACQUIRE(ptr) __atomic_load_n(ptr, __ATOMIC_RELAXED)
    #define PSR_STORE_RELEASE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELAXED)
    #define PSR_CAS(ptr, expected, desired) __atomic_compare_exchange_n(ptr, expected, desired, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED)
    #define PSR_FENCE_ACQUIRE() ((void)0)
    #define PSR_FENCE_RELEASE() ((void)0)
#elif PSR_MEMORY_ORDER_MODE == 1
    // Acquire-release ordering (multiple readers/writers)
    #define PSR_LOAD_ACQUIRE(ptr) __atomic_load_n(ptr, __ATOMIC_ACQUIRE)
    #define PSR_STORE_RELEASE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELEASE)
    #define PSR_CAS(ptr, expected, desired) __atomic_compare_exchange_n(ptr, expected, desired, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)
    #define PSR_FENCE_ACQUIRE() __atomic_thread_fence(__ATOMIC_ACQUIRE)
    #define PSR_FENCE_RELEASE() __atomic_thread_fence(__ATOMIC_RELEASE)
#else
    // Sequential consistency (strongest ordering)
    #define PSR_LOAD_ACQUIRE(ptr) __atomic_load_n(ptr, __ATOMIC_SEQ_CST)
    #define PSR_STORE_RELEASE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_SEQ_CST)
    #define PSR_CAS(ptr, expected, desired) __atomic_compare_exchange_n(ptr, expected, desired, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
    #define PSR_FENCE_ACQUIRE() __atomic_thread_fence(__ATOMIC_SEQ_CST)
    #define PSR_FENCE_RELEASE() __atomic_thread_fence(__ATOMIC_SEQ_CST)
#endif

// Forward declarations
struct psr_ringbuffer;
struct psr_subscriber;

// Message header structure
struct psr_msg_header {
    uint64_t sequence;      // Global sequence number
    uint32_t size;          // Message size
    uint32_t status;        // 0: empty, 1: writing, 2: ready
    struct timeval timestamp;
};

// Create a publish-subscribe ringbuffer
// Parameters:
//   shm_key: Shared memory key (use 0 for anonymous)
//   buffer_size: Total buffer size in bytes
//   max_subscribers: Maximum number of concurrent subscribers
// Returns: Ringbuffer handle or NULL on failure
struct psr_ringbuffer* psr_create(uint64_t shm_key, size_t buffer_size, int max_subscribers);

// Open an existing ringbuffer as publisher
struct psr_ringbuffer* psr_open_publisher(uint64_t shm_key);

// Open an existing ringbuffer as subscriber
// Returns: Subscriber handle or NULL on failure
struct psr_subscriber* psr_open_subscriber(uint64_t shm_key);

// Open by shared memory ID (for anonymous shared memory)
struct psr_ringbuffer* psr_open_publisher_by_shmid(int shm_id);
struct psr_subscriber* psr_open_subscriber_by_shmid(int shm_id);

// Get shared memory ID
int psr_get_shmid(struct psr_ringbuffer* rb);

// Publisher APIs

// Reserve space for writing (zero-copy)
// Returns: Pointer to reserved space or NULL if not enough space
void* psr_reserve(struct psr_ringbuffer* rb, size_t size, struct psr_msg_header** header);

// Commit the reserved message
int psr_commit(struct psr_ringbuffer* rb, struct psr_msg_header* header);

// Publish data (with copy)
int psr_publish(struct psr_ringbuffer* rb, const void* data, size_t size);

// Subscriber APIs

// Get event file descriptor for select/poll
int psr_subscriber_get_eventfd(struct psr_subscriber* sub);

// Consume event after select/poll notification
int psr_subscriber_consume_event(struct psr_subscriber* sub);

// Read next message (zero-copy)
// Returns: Pointer to message data or NULL if no new messages
const void* psr_subscriber_read_next(struct psr_subscriber* sub, size_t* size, struct timeval* timestamp);

// Read next message with copy
// Returns: Number of bytes read, 0 if no data, -1 on error
int psr_subscriber_read(struct psr_subscriber* sub, void* buffer, size_t buffer_size, struct timeval* timestamp);

// Get number of available messages for a subscriber
int psr_subscriber_available(struct psr_subscriber* sub);

// Skip messages to catch up to latest
int psr_subscriber_skip_to_latest(struct psr_subscriber* sub);

// Cleanup
void psr_destroy(struct psr_ringbuffer* rb);
void psr_destroy_and_remove(struct psr_ringbuffer* rb);
void psr_subscriber_close(struct psr_subscriber* sub);

// Error handling
const char* psr_errorstr(void);

// Statistics
struct psr_stats {
    uint64_t total_published;
    uint64_t total_bytes;
    uint32_t active_subscribers;
    uint32_t buffer_usage_percent;
};

int psr_get_stats(struct psr_ringbuffer* rb, struct psr_stats* stats);

#ifdef __cplusplus
}
#endif

#endif /* __PUBSUB_RINGBUFFER_H__ */