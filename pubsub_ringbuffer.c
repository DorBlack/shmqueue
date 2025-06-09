/*
 * pubsub_ringbuffer.c
 * Implementation of a publish-subscribe ring buffer
 */

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
#include <pthread.h>

#include "pubsub_ringbuffer.h"

#ifdef _WIN32
#include "shm_win.h"
#define shmget(key, size, flag) shmget_win(key, size, flag)
#define shmat(id) shmat_win(id)
#define shmdt(buf) shmdt_win(buf)
#define IPC_CREAT 1
#endif

// Memory barriers
#if defined(__x86_64__) || defined(__x86_32__)
#define CAS32(ptr, val_old, val_new) ({ char ret; __asm__ __volatile__("lock; cmpxchgl %2,%0; setz %1": "+m"(*ptr), "=q"(ret): "r"(val_new),"a"(val_old): "memory"); ret;})
#define CAS64(ptr, val_old, val_new) ({ char ret; __asm__ __volatile__("lock; cmpxchgq %2,%0; setz %1": "+m"(*ptr), "=q"(ret): "r"(val_new),"a"(val_old): "memory"); ret;})
#define wmb() __asm__ __volatile__("sfence":::"memory")
#define rmb() __asm__ __volatile__("lfence":::"memory")
#else
#define CAS32(ptr, val_old, val_new) __sync_bool_compare_and_swap(ptr, val_old, val_new)
#define CAS64(ptr, val_old, val_new) __sync_bool_compare_and_swap(ptr, val_old, val_new)
#define wmb() __sync_synchronize()
#define rmb() __sync_synchronize()
#endif

#define MAGIC_NUMBER 0x50535242  // "PSRB"
#define VERSION 1
#define MAX_DATA_SIZE (32*1024*1024)  // 32MB max per message

static char g_errmsg[256];

// Message header in ring buffer
typedef struct {
    uint64_t sequence;          // Sequence number
    uint32_t data_len;          // Data length
    uint32_t magic;             // Magic number for validation
    struct timeval timestamp;   // Publication timestamp
    // Data follows immediately after this header
} msg_header_t;

// Shared memory structure
typedef struct {
    uint32_t magic;             // Magic number for validation
    uint32_t version;           // Version for compatibility
    uint32_t element_size;      // Size of each element
    uint32_t element_count;     // Total number of elements
    
    volatile uint64_t write_seq;     // Current write sequence number
    volatile uint32_t write_pos;     // Current write position (index)
    
    volatile uint64_t total_published;    // Total messages published
    volatile uint64_t total_overwrites;   // Total messages overwritten
    
    pthread_mutex_t write_mutex;     // Mutex for write operations
    
    uint8_t reserved[4096];     // Reserved for future use
    
    // Ring buffer data starts here
    uint8_t data[0];
} shm_header_t;

// Publisher handle
struct pubsub_rb {
    shm_header_t* shm;
    uint64_t shm_key;
    int shm_id;
    BOOL is_creator;
};

// Subscriber handle
struct pubsub_reader {
    shm_header_t* shm;
    uint64_t shm_key;
    int shm_id;
    uint64_t read_seq;      // Next sequence to read
    uint32_t read_pos;      // Next position to read
};

const char* psrb_errorstr(void) {
    return g_errmsg;
}

// Calculate buffer position for a given index
static inline uint8_t* get_element_ptr(shm_header_t* shm, uint32_t index) {
    return shm->data + (index * shm->element_size);
}

// Attach to shared memory
static shm_header_t* attach_shm(uint64_t shm_key, size_t size, BOOL create, int* shm_id) {
    int id = 0;
    void* addr = NULL;
    
#ifdef _WIN32
    if (*shm_id != 0 && !create) {
        shm_key = *shm_id;
        id = shmget_pri_win(shm_key, size, 0);
    } else
#endif
    {
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
    }
    
    addr = shmat(id, NULL, 0);
    if (addr == (void*)-1) {
        snprintf(g_errmsg, sizeof(g_errmsg), "shmat failed: %s", strerror(errno));
        return NULL;
    }
    
    *shm_id = id;
    return (shm_header_t*)addr;
}

// Create a new ring buffer
pubsub_rb_t* psrb_create(uint64_t shm_key, uint32_t element_size, uint32_t element_count) {
    if (element_size < sizeof(msg_header_t) + 1 || element_count < 2) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return NULL;
    }
    
    // Calculate required size
    size_t shm_size = sizeof(shm_header_t) + (element_size * element_count);
    shm_size = (shm_size + 4095) & ~4095;  // Align to page size
    
    pubsub_rb_t* rb = calloc(1, sizeof(pubsub_rb_t));
    if (!rb) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    rb->shm_key = shm_key;
    rb->shm_id = 0;
    rb->shm = attach_shm(shm_key, shm_size, TRUE, &rb->shm_id);
    if (!rb->shm) {
        free(rb);
        return NULL;
    }
    
    // Initialize shared memory
    memset(rb->shm, 0, shm_size);
    rb->shm->magic = MAGIC_NUMBER;
    rb->shm->version = VERSION;
    rb->shm->element_size = element_size;
    rb->shm->element_count = element_count;
    rb->shm->write_seq = 1;  // Start from 1 to detect uninitialized reads
    rb->shm->write_pos = 0;
    
    // Initialize mutex
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&rb->shm->write_mutex, &attr);
    pthread_mutexattr_destroy(&attr);
    
    rb->is_creator = TRUE;
    return rb;
}

// Open as publisher
pubsub_rb_t* psrb_open_publisher(uint64_t shm_key) {
    pubsub_rb_t* rb = calloc(1, sizeof(pubsub_rb_t));
    if (!rb) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    rb->shm_key = shm_key;
    rb->shm_id = 0;
    rb->shm = attach_shm(shm_key, 0, FALSE, &rb->shm_id);
    if (!rb->shm) {
        free(rb);
        return NULL;
    }
    
    // Validate shared memory
    if (rb->shm->magic != MAGIC_NUMBER || rb->shm->version != VERSION) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory format");
        shmdt(rb->shm);
        free(rb);
        return NULL;
    }
    
    rb->is_creator = FALSE;
    return rb;
}

// Open as subscriber
pubsub_reader_t* psrb_open_subscriber(uint64_t shm_key) {
    pubsub_reader_t* reader = calloc(1, sizeof(pubsub_reader_t));
    if (!reader) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    reader->shm_key = shm_key;
    reader->shm_id = 0;
    reader->shm = attach_shm(shm_key, 0, FALSE, &reader->shm_id);
    if (!reader->shm) {
        free(reader);
        return NULL;
    }
    
    // Validate shared memory
    if (reader->shm->magic != MAGIC_NUMBER || reader->shm->version != VERSION) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory format");
        shmdt(reader->shm);
        free(reader);
        return NULL;
    }
    
    // Start reading from current position
    rmb();
    reader->read_seq = reader->shm->write_seq;
    reader->read_pos = reader->shm->write_pos;
    
    return reader;
}

// Open by shmid
pubsub_rb_t* psrb_open_publisher_by_shmid(int shm_id) {
    pubsub_rb_t* rb = calloc(1, sizeof(pubsub_rb_t));
    if (!rb) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    rb->shm_key = 0;
    rb->shm_id = shm_id;
    rb->shm = attach_shm(0, 0, FALSE, &rb->shm_id);
    if (!rb->shm) {
        free(rb);
        return NULL;
    }
    
    if (rb->shm->magic != MAGIC_NUMBER || rb->shm->version != VERSION) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory format");
        shmdt(rb->shm);
        free(rb);
        return NULL;
    }
    
    rb->is_creator = FALSE;
    return rb;
}

pubsub_reader_t* psrb_open_subscriber_by_shmid(int shm_id) {
    pubsub_reader_t* reader = calloc(1, sizeof(pubsub_reader_t));
    if (!reader) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    reader->shm_key = 0;
    reader->shm_id = shm_id;
    reader->shm = attach_shm(0, 0, FALSE, &reader->shm_id);
    if (!reader->shm) {
        free(reader);
        return NULL;
    }
    
    if (reader->shm->magic != MAGIC_NUMBER || reader->shm->version != VERSION) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory format");
        shmdt(reader->shm);
        free(reader);
        return NULL;
    }
    
    rmb();
    reader->read_seq = reader->shm->write_seq;
    reader->read_pos = reader->shm->write_pos;
    
    return reader;
}

// Get shmid
int psrb_get_shmid(pubsub_rb_t* rb) {
    return rb ? rb->shm_id : -1;
}

int psrb_reader_get_shmid(pubsub_reader_t* reader) {
    return reader ? reader->shm_id : -1;
}

// Publish data
int psrb_publish(pubsub_rb_t* rb, const void* data, uint32_t data_len) {
    if (!rb || !rb->shm || !data || data_len == 0) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return -1;
    }
    
    if (data_len > MAX_DATA_SIZE) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Data too large: %u > %u", data_len, MAX_DATA_SIZE);
        return -1;
    }
    
    uint32_t total_size = sizeof(msg_header_t) + data_len;
    if (total_size > rb->shm->element_size) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Data too large for element: %u > %u", 
                 total_size, rb->shm->element_size);
        return -1;
    }
    
    // Lock for writing
    pthread_mutex_lock(&rb->shm->write_mutex);
    
    // Get current position
    uint32_t pos = rb->shm->write_pos;
    uint64_t seq = rb->shm->write_seq;
    
    // Get element pointer
    uint8_t* elem = get_element_ptr(rb->shm, pos);
    msg_header_t* hdr = (msg_header_t*)elem;
    
    // Check if we're overwriting
    if (hdr->magic == MAGIC_NUMBER && hdr->sequence > 0) {
        __sync_fetch_and_add(&rb->shm->total_overwrites, 1);
    }
    
    // Write header
    hdr->sequence = seq;
    hdr->data_len = data_len;
    hdr->magic = MAGIC_NUMBER;
    gettimeofday(&hdr->timestamp, NULL);
    
    // Write data
    memcpy(elem + sizeof(msg_header_t), data, data_len);
    
    // Memory barrier to ensure data is written before updating position
    wmb();
    
    // Update position and sequence
    rb->shm->write_pos = (pos + 1) % rb->shm->element_count;
    rb->shm->write_seq = seq + 1;
    __sync_fetch_and_add(&rb->shm->total_published, 1);
    
    pthread_mutex_unlock(&rb->shm->write_mutex);
    
    return 0;
}

// Read data
int psrb_read(pubsub_reader_t* reader, void* buffer, uint32_t buffer_size,
              struct timeval* timestamp, uint64_t* sequence_num) {
    if (!reader || !reader->shm || !buffer || buffer_size == 0) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return -1;
    }
    
    rmb();
    
    // Check if we're behind
    uint64_t current_seq = reader->shm->write_seq;
    if (reader->read_seq < current_seq - reader->shm->element_count) {
        // Data loss detected
        reader->read_seq = current_seq - reader->shm->element_count + 1;
        reader->read_pos = (reader->shm->write_pos + 1) % reader->shm->element_count;
        return -2;
    }
    
    // Check if there's new data
    if (reader->read_seq >= current_seq) {
        return 0;  // No new data
    }
    
    // Read from current position
    uint8_t* elem = get_element_ptr(reader->shm, reader->read_pos);
    msg_header_t* hdr = (msg_header_t*)elem;
    
    // Validate header
    if (hdr->magic != MAGIC_NUMBER || hdr->sequence != reader->read_seq) {
        // Data corruption or overwrite race
        psrb_reset_to_latest(reader);
        return -2;
    }
    
    // Check buffer size
    if (hdr->data_len > buffer_size) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Buffer too small: %u < %u", 
                 buffer_size, hdr->data_len);
        return -1;
    }
    
    // Copy data
    memcpy(buffer, elem + sizeof(msg_header_t), hdr->data_len);
    if (timestamp) *timestamp = hdr->timestamp;
    if (sequence_num) *sequence_num = hdr->sequence;
    
    // Update read position
    reader->read_seq++;
    reader->read_pos = (reader->read_pos + 1) % reader->shm->element_count;
    
    return hdr->data_len;
}

// Peek at next data without consuming
int psrb_peek(pubsub_reader_t* reader, void* buffer, uint32_t buffer_size,
              struct timeval* timestamp, uint64_t* sequence_num) {
    if (!reader || !reader->shm) {
        return -1;
    }
    
    // Save current position
    uint64_t saved_seq = reader->read_seq;
    uint32_t saved_pos = reader->read_pos;
    
    // Read data
    int result = psrb_read(reader, buffer, buffer_size, timestamp, sequence_num);
    
    // Restore position
    reader->read_seq = saved_seq;
    reader->read_pos = saved_pos;
    
    return result;
}

// Get number of available messages
uint32_t psrb_available(pubsub_reader_t* reader) {
    if (!reader || !reader->shm) return 0;
    
    rmb();
    uint64_t current_seq = reader->shm->write_seq;
    
    if (reader->read_seq >= current_seq) return 0;
    
    uint64_t available = current_seq - reader->read_seq;
    if (available > reader->shm->element_count) {
        available = reader->shm->element_count;
    }
    
    return (uint32_t)available;
}

// Get reader lag
uint32_t psrb_get_lag(pubsub_reader_t* reader) {
    return psrb_available(reader);
}

// Reset to latest
void psrb_reset_to_latest(pubsub_reader_t* reader) {
    if (!reader || !reader->shm) return;
    
    rmb();
    reader->read_seq = reader->shm->write_seq;
    reader->read_pos = reader->shm->write_pos;
}

// Reset to oldest available
void psrb_reset_to_oldest(pubsub_reader_t* reader) {
    if (!reader || !reader->shm) return;
    
    rmb();
    uint64_t current_seq = reader->shm->write_seq;
    uint32_t count = reader->shm->element_count;
    
    if (current_seq > count) {
        reader->read_seq = current_seq - count + 1;
        reader->read_pos = (reader->shm->write_pos + 1) % count;
    } else {
        reader->read_seq = 1;
        reader->read_pos = 0;
    }
}

// Get statistics
void psrb_get_stats(pubsub_rb_t* rb, psrb_stats_t* stats) {
    if (!rb || !rb->shm || !stats) return;
    
    rmb();
    stats->total_published = rb->shm->total_published;
    stats->total_overwrites = rb->shm->total_overwrites;
    stats->current_subscribers = 0;  // Would need tracking mechanism
    
    // Calculate usage
    uint64_t published = rb->shm->total_published;
    uint64_t overwrites = rb->shm->total_overwrites;
    uint32_t count = rb->shm->element_count;
    
    if (published - overwrites < count) {
        stats->buffer_usage_percent = ((published - overwrites) * 100) / count;
    } else {
        stats->buffer_usage_percent = 100;
    }
}

// Cleanup
void psrb_destroy(pubsub_rb_t* rb) {
    if (rb) {
        if (rb->shm) shmdt(rb->shm);
        free(rb);
    }
}

void psrb_destroy_and_remove(pubsub_rb_t* rb) {
    if (rb) {
        if (rb->shm) {
            shmdt(rb->shm);
            if (rb->is_creator && rb->shm_id > 0) {
#ifndef _WIN32
                shmctl(rb->shm_id, IPC_RMID, NULL);
#endif
            }
        }
        free(rb);
    }
}

void psrb_reader_destroy(pubsub_reader_t* reader) {
    if (reader) {
        if (reader->shm) shmdt(reader->shm);
        free(reader);
    }
}