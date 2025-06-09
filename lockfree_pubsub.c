/*
 * lockfree_pubsub.c
 * Lock-free publish-subscribe ring buffer implementation
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <sched.h>

#include "lockfree_pubsub.h"

#define LFPS_MAGIC 0x4C465053  // "LFPS"
#define LFPS_VERSION 1
#define LFPS_MAX_DATA_SIZE (32*1024*1024)
#define LFPS_MAX_READERS 64
#define LFPS_CACHE_LINE_SIZE 64

// Align to cache line to avoid false sharing
#define LFPS_CACHE_ALIGN __attribute__((aligned(LFPS_CACHE_LINE_SIZE)))

// Message states
#define MSG_STATE_EMPTY     0
#define MSG_STATE_WRITING   1
#define MSG_STATE_READY     2
#define MSG_STATE_READING   3

static char g_errmsg[256];

// Message header in ring buffer
typedef struct {
    volatile uint32_t state;        // Message state
    uint32_t data_len;              // Data length
    uint64_t sequence;              // Sequence number
    struct timeval timestamp;       // Publication timestamp
    uint8_t data[0];               // Actual data follows
} msg_header_t;

// Reader registration info
typedef struct {
    volatile pid_t pid;             // Reader process ID
    volatile uint64_t read_seq;     // Last read sequence
    volatile uint32_t notify_threshold;  // Notification threshold
    volatile uint8_t notify_enabled;     // Notification enabled flag
    volatile uint8_t notify_pending;     // Notification pending flag
    char fifo_path[256];            // FIFO path for this reader
} reader_info_t;

// Shared memory header
typedef struct {
    uint32_t magic;                 // Magic number
    uint32_t version;               // Version
    uint32_t element_size;          // Size per element
    uint32_t element_count;         // Number of elements
    
    // Cache line aligned for performance
    volatile uint64_t write_seq LFPS_CACHE_ALIGN;   // Next write sequence
    volatile uint64_t alloc_seq LFPS_CACHE_ALIGN;   // Allocation sequence
    
    // Statistics
    volatile uint64_t total_published LFPS_CACHE_ALIGN;
    volatile uint64_t notification_sent;
    volatile uint64_t notification_missed;
    
    // Reader management
    volatile uint32_t reader_count LFPS_CACHE_ALIGN;
    reader_info_t readers[LFPS_MAX_READERS];
    
    // Ring buffer data
    uint8_t data[0] LFPS_CACHE_ALIGN;
} shm_header_t;

// Publisher handle
struct lfps_ring {
    shm_header_t* shm;
    uint64_t shm_key;
    int shm_id;
    int is_creator;
    
    // Cached notification FDs
    int notify_fds[LFPS_MAX_READERS];
    uint32_t notify_fd_count;
};

// Reader handle
struct lfps_reader {
    shm_header_t* shm;
    uint64_t shm_key;
    int shm_id;
    
    // Reader specific
    uint32_t reader_idx;            // Index in reader array
    uint64_t read_seq;              // Next sequence to read
    uint64_t total_consumed;        // Total messages consumed
    uint64_t total_lost;            // Total messages lost
    
    // Notification
    int notify_fd;                  // FIFO fd for reading
    lfps_notify_cb callback;        // Optional callback
    void* callback_data;            // Callback user data
};

const char* lfps_errorstr(void) {
    return g_errmsg;
}

// Calculate element address
static inline uint8_t* get_element_ptr(shm_header_t* shm, uint64_t seq) {
    uint32_t idx = seq % shm->element_count;
    return shm->data + (idx * shm->element_size);
}

// Attach shared memory
static shm_header_t* attach_shm(uint64_t shm_key, size_t size, int create, int* shm_id) {
    int id = 0;
    void* addr = NULL;
    
    if (!create) {
        id = shmget(shm_key, 0, 0);
        if (id < 0) {
            snprintf(g_errmsg, sizeof(g_errmsg), "shmget failed: %s", strerror(errno));
            return NULL;
        }
    } else {
        id = shmget(shm_key, size, IPC_CREAT | 0666);
        if (id < 0) {
            snprintf(g_errmsg, sizeof(g_errmsg), "shmget create failed: %s", strerror(errno));
            return NULL;
        }
    }
    
    addr = shmat(id, NULL, 0);
    if (addr == (void*)-1) {
        snprintf(g_errmsg, sizeof(g_errmsg), "shmat failed: %s", strerror(errno));
        return NULL;
    }
    
    *shm_id = id;
    return (shm_header_t*)addr;
}

// Create FIFO for reader
static int create_reader_fifo(uint64_t shm_key, uint32_t reader_idx, int for_reading) {
    char fifo_path[256];
    snprintf(fifo_path, sizeof(fifo_path), "/tmp/lfps_%llx_%u", 
             (unsigned long long)shm_key, reader_idx);
    
    // Create FIFO if not exists
    if (mkfifo(fifo_path, 0666) < 0 && errno != EEXIST) {
        snprintf(g_errmsg, sizeof(g_errmsg), "mkfifo failed: %s", strerror(errno));
        return -1;
    }
    
    // Open FIFO
    int flags = O_NONBLOCK | (for_reading ? O_RDWR : O_WRONLY);
    int fd = open(fifo_path, flags, 0666);
    if (fd < 0) {
        snprintf(g_errmsg, sizeof(g_errmsg), "open fifo failed: %s", strerror(errno));
        return -1;
    }
    
    return fd;
}

// Send notification to specific reader
static void notify_reader(lfps_ring_t* ring, uint32_t reader_idx) {
    if (reader_idx >= LFPS_MAX_READERS) return;
    
    reader_info_t* reader = &ring->shm->readers[reader_idx];
    if (!reader->notify_enabled || !reader->pid) return;
    
    // Check if already has pending notification
    if (reader->notify_pending) {
        LFPS_FETCH_ADD64(&ring->shm->notification_missed, 1);
        return;
    }
    
    // Try to get/create FD
    if (reader_idx >= ring->notify_fd_count || ring->notify_fds[reader_idx] <= 0) {
        int fd = create_reader_fifo(ring->shm_key, reader_idx, 0);
        if (fd > 0) {
            if (reader_idx >= ring->notify_fd_count) {
                ring->notify_fd_count = reader_idx + 1;
            }
            ring->notify_fds[reader_idx] = fd;
        }
    }
    
    // Send notification
    if (ring->notify_fds[reader_idx] > 0) {
        char c = 1;
        if (write(ring->notify_fds[reader_idx], &c, 1) == 1) {
            reader->notify_pending = 1;
            LFPS_FETCH_ADD64(&ring->shm->notification_sent, 1);
        }
    }
}

// Create new ring buffer
lfps_ring_t* lfps_create(uint64_t shm_key, uint32_t element_size, uint32_t element_count) {
    if (element_size < sizeof(msg_header_t) + 1 || element_count < 2) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return NULL;
    }
    
    // Calculate required size
    size_t shm_size = sizeof(shm_header_t) + (element_size * element_count);
    shm_size = (shm_size + 4095) & ~4095;  // Page align
    
    lfps_ring_t* ring = calloc(1, sizeof(lfps_ring_t));
    if (!ring) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    ring->shm_key = shm_key;
    ring->shm_id = 0;
    ring->shm = attach_shm(shm_key, shm_size, 1, &ring->shm_id);
    if (!ring->shm) {
        free(ring);
        return NULL;
    }
    
    // Initialize shared memory
    memset(ring->shm, 0, shm_size);
    ring->shm->magic = LFPS_MAGIC;
    ring->shm->version = LFPS_VERSION;
    ring->shm->element_size = element_size;
    ring->shm->element_count = element_count;
    ring->shm->write_seq = 1;
    ring->shm->alloc_seq = 1;
    
    ring->is_creator = 1;
    
    // Initialize notification FDs
    memset(ring->notify_fds, 0, sizeof(ring->notify_fds));
    ring->notify_fd_count = 0;
    
    return ring;
}

// Open as publisher
lfps_ring_t* lfps_open_publisher(uint64_t shm_key) {
    lfps_ring_t* ring = calloc(1, sizeof(lfps_ring_t));
    if (!ring) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    ring->shm_key = shm_key;
    ring->shm_id = 0;
    ring->shm = attach_shm(shm_key, 0, 0, &ring->shm_id);
    if (!ring->shm) {
        free(ring);
        return NULL;
    }
    
    if (ring->shm->magic != LFPS_MAGIC) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory");
        shmdt(ring->shm);
        free(ring);
        return NULL;
    }
    
    ring->is_creator = 0;
    memset(ring->notify_fds, 0, sizeof(ring->notify_fds));
    ring->notify_fd_count = 0;
    
    return ring;
}

// Lock-free publish implementation
int lfps_publish_notify(lfps_ring_t* ring, const void* data, uint32_t data_len, int notify_readers) {
    if (!ring || !ring->shm || !data || data_len == 0) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return -1;
    }
    
    if (data_len > LFPS_MAX_DATA_SIZE) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Data too large");
        return -1;
    }
    
    uint32_t total_size = sizeof(msg_header_t) + data_len;
    if (total_size > ring->shm->element_size) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Data exceeds element size");
        return -1;
    }
    
    // Lock-free allocation using CAS
    uint64_t seq;
    while (1) {
        seq = LFPS_ATOMIC_LOAD64(&ring->shm->alloc_seq);
        if (LFPS_CAS64(&ring->shm->alloc_seq, seq, seq + 1)) {
            break;
        }
    }
    
    // Get message location
    uint8_t* elem = get_element_ptr(ring->shm, seq);
    msg_header_t* msg = (msg_header_t*)elem;
    
    // Mark as writing
    msg->state = MSG_STATE_WRITING;
    LFPS_PRODUCER_FENCE();
    
    // Fill message
    msg->sequence = seq;
    msg->data_len = data_len;
    gettimeofday(&msg->timestamp, NULL);
    memcpy(msg->data, data, data_len);
    
    // Mark as ready
    LFPS_PRODUCER_FENCE();
    msg->state = MSG_STATE_READY;
    
    // Update write sequence
    while (1) {
        uint64_t expected = seq - 1;
        if (LFPS_CAS64(&ring->shm->write_seq, expected, seq)) {
            break;
        }
        // Another writer is still writing, spin wait
        LFPS_FULL_FENCE();
    }
    
    // Update statistics
    LFPS_FETCH_ADD64(&ring->shm->total_published, 1);
    
    // Send notifications if requested
    if (notify_readers) {
        uint32_t reader_count = LFPS_ATOMIC_LOAD32(&ring->shm->reader_count);
        for (uint32_t i = 0; i < reader_count && i < LFPS_MAX_READERS; i++) {
            reader_info_t* reader = &ring->shm->readers[i];
            if (reader->pid && reader->notify_enabled) {
                // Check if threshold is met
                uint64_t available = seq - reader->read_seq;
                if (available >= reader->notify_threshold) {
                    notify_reader(ring, i);
                }
            }
        }
    }
    
    return 0;
}

// Default publish with notification
int lfps_publish(lfps_ring_t* ring, const void* data, uint32_t data_len) {
    return lfps_publish_notify(ring, data, data_len, 1);
}

// Open as reader
lfps_reader_t* lfps_open_reader(uint64_t shm_key) {
    lfps_reader_t* reader = calloc(1, sizeof(lfps_reader_t));
    if (!reader) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    reader->shm_key = shm_key;
    reader->shm_id = 0;
    reader->shm = attach_shm(shm_key, 0, 0, &reader->shm_id);
    if (!reader->shm) {
        free(reader);
        return NULL;
    }
    
    if (reader->shm->magic != LFPS_MAGIC) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory");
        shmdt(reader->shm);
        free(reader);
        return NULL;
    }
    
    // Register reader
    pid_t pid = getpid();
    uint32_t reader_idx = LFPS_MAX_READERS;
    
    // Find empty slot or existing registration
    for (uint32_t i = 0; i < LFPS_MAX_READERS; i++) {
        pid_t old_pid = reader->shm->readers[i].pid;
        if (old_pid == 0) {
            if (LFPS_CAS32((uint32_t*)&reader->shm->readers[i].pid, 0, pid)) {
                reader_idx = i;
                break;
            }
        } else if (old_pid == pid) {
            reader_idx = i;
            break;
        }
    }
    
    if (reader_idx >= LFPS_MAX_READERS) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Too many readers");
        shmdt(reader->shm);
        free(reader);
        return NULL;
    }
    
    reader->reader_idx = reader_idx;
    reader->read_seq = LFPS_ATOMIC_LOAD64(&reader->shm->write_seq);
    reader->notify_fd = -1;
    
    // Update reader count
    uint32_t count = reader_idx + 1;
    uint32_t old_count;
    do {
        old_count = LFPS_ATOMIC_LOAD32(&reader->shm->reader_count);
        if (old_count >= count) break;
    } while (!LFPS_CAS32(&reader->shm->reader_count, old_count, count));
    
    // Update reader info
    reader->shm->readers[reader_idx].read_seq = reader->read_seq;
    
    return reader;
}

// Read message
int lfps_read(lfps_reader_t* reader, void* buffer, uint32_t buffer_size,
              struct timeval* timestamp, uint64_t* sequence) {
    if (!reader || !reader->shm || !buffer || buffer_size == 0) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return -1;
    }
    
    LFPS_CONSUMER_FENCE();
    
    // Check for new data
    uint64_t write_seq = LFPS_ATOMIC_LOAD64(&reader->shm->write_seq);
    if (reader->read_seq > write_seq) {
        return 0;  // No new data
    }
    
    // Check for data loss
    if (write_seq - reader->read_seq > reader->shm->element_count) {
        uint64_t lost = write_seq - reader->read_seq - reader->shm->element_count;
        reader->total_lost += lost;
        reader->read_seq = write_seq - reader->shm->element_count + 1;
        return -2;  // Data loss
    }
    
    // Get message
    uint8_t* elem = get_element_ptr(reader->shm, reader->read_seq);
    msg_header_t* msg = (msg_header_t*)elem;
    
    // Wait for message to be ready
    uint32_t state;
    int spin_count = 0;
    while ((state = LFPS_ATOMIC_LOAD32(&msg->state)) == MSG_STATE_WRITING) {
        if (++spin_count > 1000000) {
            // Writer seems stuck, skip this message
            reader->read_seq++;
            reader->total_lost++;
            return -2;
        }
        LFPS_CONSUMER_FENCE();
    }
    
    if (state != MSG_STATE_READY || msg->sequence != reader->read_seq) {
        // Corrupted or overwritten
        reader->read_seq = write_seq;
        return -2;
    }
    
    // Check buffer size
    if (msg->data_len > buffer_size) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Buffer too small");
        return -1;
    }
    
    // Copy data
    memcpy(buffer, msg->data, msg->data_len);
    if (timestamp) *timestamp = msg->timestamp;
    if (sequence) *sequence = msg->sequence;
    
    // Update read position
    reader->read_seq++;
    reader->total_consumed++;
    
    // Update reader info in shm
    reader->shm->readers[reader->reader_idx].read_seq = reader->read_seq;
    
    return msg->data_len;
}

// Get reader notification FD
int lfps_reader_get_fd(lfps_reader_t* reader) {
    if (!reader || reader->reader_idx >= LFPS_MAX_READERS) {
        return -1;
    }
    
    if (reader->notify_fd <= 0) {
        reader->notify_fd = create_reader_fifo(reader->shm_key, reader->reader_idx, 1);
    }
    
    return reader->notify_fd;
}

// Enable notification
int lfps_reader_enable_notify(lfps_reader_t* reader, int threshold) {
    if (!reader || reader->reader_idx >= LFPS_MAX_READERS) {
        return -1;
    }
    
    reader->shm->readers[reader->reader_idx].notify_threshold = threshold;
    reader->shm->readers[reader->reader_idx].notify_enabled = 1;
    
    // Ensure we have a valid FD
    if (reader->notify_fd <= 0) {
        reader->notify_fd = create_reader_fifo(reader->shm_key, reader->reader_idx, 1);
    }
    
    return reader->notify_fd > 0 ? 0 : -1;
}

// Disable notification
int lfps_reader_disable_notify(lfps_reader_t* reader) {
    if (!reader || reader->reader_idx >= LFPS_MAX_READERS) {
        return -1;
    }
    
    reader->shm->readers[reader->reader_idx].notify_enabled = 0;
    return 0;
}

// Consume notification events
int lfps_reader_consume_event(lfps_reader_t* reader, int count) {
    if (!reader || reader->notify_fd <= 0) {
        return -1;
    }
    
    if (count <= 0) count = 64;
    
    char buf[count];
    int n = read(reader->notify_fd, buf, count);
    
    // Clear pending flag
    reader->shm->readers[reader->reader_idx].notify_pending = 0;
    
    return n > 0 ? 0 : -1;
}

// Get available messages
uint32_t lfps_reader_available(lfps_reader_t* reader) {
    if (!reader || !reader->shm) return 0;
    
    uint64_t write_seq = LFPS_ATOMIC_LOAD64(&reader->shm->write_seq);
    if (reader->read_seq >= write_seq) return 0;
    
    uint64_t available = write_seq - reader->read_seq;
    if (available > reader->shm->element_count) {
        available = reader->shm->element_count;
    }
    
    return (uint32_t)available;
}

// Get statistics
void lfps_get_stats(lfps_ring_t* ring, lfps_stats_t* stats) {
    if (!ring || !ring->shm || !stats) return;
    
    memset(stats, 0, sizeof(*stats));
    stats->total_published = ring->shm->total_published;
    stats->current_readers = ring->shm->reader_count;
    stats->notification_sent = ring->shm->notification_sent;
    stats->notification_missed = ring->shm->notification_missed;
    
    // Calculate buffer usage
    uint64_t write_seq = ring->shm->write_seq;
    uint64_t min_read_seq = write_seq;
    
    for (uint32_t i = 0; i < ring->shm->reader_count && i < LFPS_MAX_READERS; i++) {
        if (ring->shm->readers[i].pid) {
            uint64_t read_seq = ring->shm->readers[i].read_seq;
            if (read_seq < min_read_seq) {
                min_read_seq = read_seq;
            }
        }
    }
    
    uint64_t used = write_seq - min_read_seq;
    if (used > ring->shm->element_count) {
        stats->buffer_usage_percent = 100;
    } else {
        stats->buffer_usage_percent = (used * 100) / ring->shm->element_count;
    }
}

// Cleanup
void lfps_destroy(lfps_ring_t* ring) {
    if (ring) {
        // Close notification FDs
        for (uint32_t i = 0; i < ring->notify_fd_count; i++) {
            if (ring->notify_fds[i] > 0) {
                close(ring->notify_fds[i]);
            }
        }
        
        if (ring->shm) shmdt(ring->shm);
        free(ring);
    }
}

void lfps_reader_destroy(lfps_reader_t* reader) {
    if (reader) {
        // Clear reader registration
        if (reader->shm && reader->reader_idx < LFPS_MAX_READERS) {
            reader->shm->readers[reader->reader_idx].pid = 0;
        }
        
        if (reader->notify_fd > 0) {
            close(reader->notify_fd);
        }
        
        if (reader->shm) shmdt(reader->shm);
        free(reader);
    }
}

// Additional utility functions
void lfps_set_cpu_affinity(int cpu_id) {
#ifndef _WIN32
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    sched_setaffinity(0, sizeof(cpuset), &cpuset);
#endif
}

void lfps_prefetch_next(lfps_reader_t* reader) {
    if (reader && reader->shm && reader->read_seq < reader->shm->write_seq) {
        uint8_t* next = get_element_ptr(reader->shm, reader->read_seq);
        __builtin_prefetch(next, 0, 3);
    }
}