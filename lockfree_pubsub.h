/*
 * lockfree_pubsub.h
 * Lock-free publish-subscribe ring buffer with configurable memory ordering
 * 
 * Features:
 * 1) Lock-free implementation for multiple readers and writers
 * 2) Configurable memory ordering for different scenarios
 * 3) FIFO notification support with select/poll
 * 4) Per-reader notification control
 */

#ifndef __LOCKFREE_PUBSUB_H__
#define __LOCKFREE_PUBSUB_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/time.h>

// Configuration macros for different usage patterns
// Define one of these before including this header:
// - LFPS_MODE_SPSC: Single Producer Single Consumer (most relaxed)
// - LFPS_MODE_MPSC: Multiple Producer Single Consumer
// - LFPS_MODE_SPMC: Single Producer Multiple Consumer
// - LFPS_MODE_MPMC: Multiple Producer Multiple Consumer (most strict)

#ifndef LFPS_MODE_MPMC
#ifndef LFPS_MODE_MPSC
#ifndef LFPS_MODE_SPMC
#ifndef LFPS_MODE_SPSC
#define LFPS_MODE_MPMC  // Default to most conservative mode
#endif
#endif
#endif
#endif

// Memory ordering based on mode
#ifdef LFPS_MODE_SPSC
  #define LFPS_PRODUCER_FENCE() __asm__ __volatile__("" ::: "memory")
  #define LFPS_CONSUMER_FENCE() __asm__ __volatile__("" ::: "memory")
  #define LFPS_FULL_FENCE()     __asm__ __volatile__("" ::: "memory")
#elif defined(LFPS_MODE_MPSC)
  #define LFPS_PRODUCER_FENCE() __asm__ __volatile__("mfence" ::: "memory")
  #define LFPS_CONSUMER_FENCE() __asm__ __volatile__("" ::: "memory")
  #define LFPS_FULL_FENCE()     __asm__ __volatile__("mfence" ::: "memory")
#elif defined(LFPS_MODE_SPMC)
  #define LFPS_PRODUCER_FENCE() __asm__ __volatile__("sfence" ::: "memory")
  #define LFPS_CONSUMER_FENCE() __asm__ __volatile__("lfence" ::: "memory")
  #define LFPS_FULL_FENCE()     __asm__ __volatile__("mfence" ::: "memory")
#else // LFPS_MODE_MPMC
  #define LFPS_PRODUCER_FENCE() __asm__ __volatile__("mfence" ::: "memory")
  #define LFPS_CONSUMER_FENCE() __asm__ __volatile__("lfence" ::: "memory")
  #define LFPS_FULL_FENCE()     __asm__ __volatile__("mfence" ::: "memory")
#endif

// Atomic operations
#if defined(__x86_64__) || defined(__x86_32__)
  #define LFPS_CAS64(ptr, old, new) __sync_bool_compare_and_swap(ptr, old, new)
  #define LFPS_CAS32(ptr, old, new) __sync_bool_compare_and_swap(ptr, old, new)
  #define LFPS_FETCH_ADD64(ptr, val) __sync_fetch_and_add(ptr, val)
  #define LFPS_FETCH_ADD32(ptr, val) __sync_fetch_and_add(ptr, val)
  #define LFPS_ATOMIC_LOAD64(ptr) __sync_fetch_and_add(ptr, 0)
  #define LFPS_ATOMIC_LOAD32(ptr) __sync_fetch_and_add(ptr, 0)
#else
  #define LFPS_CAS64(ptr, old, new) __atomic_compare_exchange_n(ptr, &old, new, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
  #define LFPS_CAS32(ptr, old, new) __atomic_compare_exchange_n(ptr, &old, new, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
  #define LFPS_FETCH_ADD64(ptr, val) __atomic_fetch_add(ptr, val, __ATOMIC_SEQ_CST)
  #define LFPS_FETCH_ADD32(ptr, val) __atomic_fetch_add(ptr, val, __ATOMIC_SEQ_CST)
  #define LFPS_ATOMIC_LOAD64(ptr) __atomic_load_n(ptr, __ATOMIC_SEQ_CST)
  #define LFPS_ATOMIC_LOAD32(ptr) __atomic_load_n(ptr, __ATOMIC_SEQ_CST)
#endif

// Types
typedef struct lfps_ring lfps_ring_t;
typedef struct lfps_reader lfps_reader_t;

// Notification callback type
typedef void (*lfps_notify_cb)(lfps_reader_t* reader, void* user_data);

// Publisher API
lfps_ring_t* lfps_create(uint64_t shm_key, uint32_t element_size, uint32_t element_count);
lfps_ring_t* lfps_open_publisher(uint64_t shm_key);
lfps_ring_t* lfps_open_publisher_by_shmid(int shm_id);

// Publishing with optional notification control
int lfps_publish(lfps_ring_t* ring, const void* data, uint32_t data_len);
int lfps_publish_notify(lfps_ring_t* ring, const void* data, uint32_t data_len, int notify_readers);

// Subscriber API
lfps_reader_t* lfps_open_reader(uint64_t shm_key);
lfps_reader_t* lfps_open_reader_by_shmid(int shm_id);

// Reading with notification support
int lfps_read(lfps_reader_t* reader, void* buffer, uint32_t buffer_size, 
              struct timeval* timestamp, uint64_t* sequence);
int lfps_read_batch(lfps_reader_t* reader, void** buffers, uint32_t* sizes,
                    uint32_t max_messages, struct timeval* timestamps, 
                    uint64_t* sequences);

// Notification control
int lfps_reader_get_fd(lfps_reader_t* reader);  // Get fd for select/poll/epoll
int lfps_reader_enable_notify(lfps_reader_t* reader, int threshold);  // Enable notification
int lfps_reader_disable_notify(lfps_reader_t* reader);  // Disable notification
int lfps_reader_consume_event(lfps_reader_t* reader, int count);  // Consume notification events

// Callback-based notification (optional)
int lfps_reader_set_callback(lfps_reader_t* reader, lfps_notify_cb callback, void* user_data);

// Reader control
uint32_t lfps_reader_available(lfps_reader_t* reader);
uint32_t lfps_reader_lag(lfps_reader_t* reader);
void lfps_reader_reset_latest(lfps_reader_t* reader);
void lfps_reader_reset_oldest(lfps_reader_t* reader);

// Statistics
typedef struct {
    uint64_t total_published;
    uint64_t total_consumed;
    uint64_t total_lost;
    uint32_t current_readers;
    uint32_t buffer_usage_percent;
    uint32_t notification_sent;
    uint32_t notification_missed;
} lfps_stats_t;

void lfps_get_stats(lfps_ring_t* ring, lfps_stats_t* stats);
void lfps_reader_get_stats(lfps_reader_t* reader, uint64_t* consumed, uint64_t* lost);

// Utilities
int lfps_get_shmid(lfps_ring_t* ring);
int lfps_reader_get_shmid(lfps_reader_t* reader);
const char* lfps_errorstr(void);

// Cleanup
void lfps_destroy(lfps_ring_t* ring);
void lfps_destroy_and_remove(lfps_ring_t* ring);
void lfps_reader_destroy(lfps_reader_t* reader);

// Performance tuning
void lfps_set_cpu_affinity(int cpu_id);  // Pin to specific CPU
void lfps_prefetch_next(lfps_reader_t* reader);  // Prefetch next message

#ifdef __cplusplus
}
#endif

#endif /* __LOCKFREE_PUBSUB_H__ */