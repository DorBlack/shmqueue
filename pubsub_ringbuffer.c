/*
 * pubsub_ringbuffer.c
 * Lock-free publish-subscribe ringbuffer implementation
 */

#include "pubsub_ringbuffer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <signal.h>

#define PSR_MAX_SUBSCRIBERS 64
#define PSR_NOTIFICATION_BATCH 10
#define PSR_CACHE_LINE_SIZE 64

// Align to cache line to avoid false sharing
#define CACHE_ALIGNED __attribute__((aligned(PSR_CACHE_LINE_SIZE)))

// Status values for messages
#define MSG_STATUS_EMPTY 0
#define MSG_STATUS_WRITING 1
#define MSG_STATUS_READY 2

// Shared memory header structure
struct psr_shm_header {
    // Metadata
    uint32_t magic;                   // Magic number for validation
    uint32_t version;                 // Version for compatibility
    size_t buffer_size;               // Total buffer size
    size_t data_offset;               // Offset to data area
    
    // Publisher state (cache-line aligned)
    uint64_t write_seq CACHE_ALIGNED; // Next sequence number to write
    uint64_t write_pos;               // Current write position in buffer
    
    // Subscriber management
    uint32_t max_subscribers CACHE_ALIGNED;
    uint32_t active_subscribers;
    
    // Statistics
    uint64_t total_published CACHE_ALIGNED;
    uint64_t total_bytes;
    
    // Notification FIFOs info
    uint64_t shm_key;
    char reserved[256];               // Reserved for future use
};

// Subscriber slot in shared memory
struct psr_subscriber_slot {
    pid_t pid;                        // Process ID (0 = slot free)
    uint64_t read_seq CACHE_ALIGNED;  // Last read sequence number
    uint32_t active;                  // Active flag
    uint32_t notify_enabled;          // Notification enabled flag
};

// Main ringbuffer structure
struct psr_ringbuffer {
    struct psr_shm_header* header;
    struct psr_subscriber_slot* sub_slots;
    char* data_buffer;
    int shm_id;
    uint64_t shm_key;
    int is_creator;
    char errmsg[256];
};

// Subscriber handle structure
struct psr_subscriber {
    struct psr_ringbuffer* rb;
    int slot_index;
    uint64_t last_read_seq;
    int event_fd;
    char fifo_path[256];
};

// Global error message
static char g_errmsg[256];

const char* psr_errorstr(void) {
    return g_errmsg;
}

// Calculate aligned size
static size_t align_size(size_t size, size_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

// Create or get shared memory
static void* attach_shm(uint64_t key, size_t size, int create, int* shm_id) {
    int shmid;
    void* addr;
    
    if (create) {
        shmid = shmget(key, size, IPC_CREAT | 0666);
        if (shmid < 0) {
            snprintf(g_errmsg, sizeof(g_errmsg), "shmget failed: %s", strerror(errno));
            return NULL;
        }
    } else {
        shmid = shmget(key, 0, 0);
        if (shmid < 0) {
            snprintf(g_errmsg, sizeof(g_errmsg), "shmget failed: %s", strerror(errno));
            return NULL;
        }
    }
    
    addr = shmat(shmid, NULL, 0);
    if (addr == (void*)-1) {
        snprintf(g_errmsg, sizeof(g_errmsg), "shmat failed: %s", strerror(errno));
        return NULL;
    }
    
    if (shm_id) *shm_id = shmid;
    return addr;
}

// Create FIFO for notification
static int create_notification_fifo(uint64_t shm_key, int subscriber_idx, int for_reading) {
    char fifo_path[256];
    snprintf(fifo_path, sizeof(fifo_path), "/tmp/psr_fifo_%llu_%d", 
             (unsigned long long)shm_key, subscriber_idx);
    
    if (mkfifo(fifo_path, 0666) < 0 && errno != EEXIST) {
        return -1;
    }
    
    int flags = for_reading ? (O_RDWR | O_NONBLOCK) : (O_WRONLY | O_NONBLOCK);
    return open(fifo_path, flags);
}

// Create ringbuffer
struct psr_ringbuffer* psr_create(uint64_t shm_key, size_t buffer_size, int max_subscribers) {
    if (buffer_size < 1024 || max_subscribers < 1 || max_subscribers > PSR_MAX_SUBSCRIBERS) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid parameters");
        return NULL;
    }
    
    // Calculate total shared memory size
    size_t header_size = align_size(sizeof(struct psr_shm_header), PSR_CACHE_LINE_SIZE);
    size_t slots_size = align_size(sizeof(struct psr_subscriber_slot) * max_subscribers, PSR_CACHE_LINE_SIZE);
    size_t total_size = header_size + slots_size + buffer_size;
    
    // Allocate ringbuffer structure
    struct psr_ringbuffer* rb = calloc(1, sizeof(struct psr_ringbuffer));
    if (!rb) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    // Create shared memory
    rb->header = (struct psr_shm_header*)attach_shm(shm_key, total_size, 1, &rb->shm_id);
    if (!rb->header) {
        free(rb);
        return NULL;
    }
    
    // Initialize header
    rb->header->magic = 0x50535242;  // "PSRB"
    rb->header->version = 1;
    rb->header->buffer_size = buffer_size;
    rb->header->data_offset = header_size + slots_size;
    rb->header->write_seq = 1;  // Start from 1 (0 means no data)
    rb->header->write_pos = 0;
    rb->header->max_subscribers = max_subscribers;
    rb->header->active_subscribers = 0;
    rb->header->total_published = 0;
    rb->header->total_bytes = 0;
    rb->header->shm_key = shm_key;
    
    // Set up pointers
    rb->sub_slots = (struct psr_subscriber_slot*)((char*)rb->header + header_size);
    rb->data_buffer = (char*)rb->header + rb->header->data_offset;
    rb->shm_key = shm_key;
    rb->is_creator = 1;
    
    // Initialize subscriber slots
    memset(rb->sub_slots, 0, sizeof(struct psr_subscriber_slot) * max_subscribers);
    
    signal(SIGPIPE, SIG_IGN);
    return rb;
}

// Open as publisher
struct psr_ringbuffer* psr_open_publisher(uint64_t shm_key) {
    struct psr_ringbuffer* rb = calloc(1, sizeof(struct psr_ringbuffer));
    if (!rb) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    rb->header = (struct psr_shm_header*)attach_shm(shm_key, 0, 0, &rb->shm_id);
    if (!rb->header) {
        free(rb);
        return NULL;
    }
    
    // Validate magic number
    if (rb->header->magic != 0x50535242) {
        snprintf(g_errmsg, sizeof(g_errmsg), "Invalid shared memory format");
        shmdt(rb->header);
        free(rb);
        return NULL;
    }
    
    // Set up pointers
    size_t header_size = align_size(sizeof(struct psr_shm_header), PSR_CACHE_LINE_SIZE);
    rb->sub_slots = (struct psr_subscriber_slot*)((char*)rb->header + header_size);
    rb->data_buffer = (char*)rb->header + rb->header->data_offset;
    rb->shm_key = shm_key;
    rb->is_creator = 0;
    
    signal(SIGPIPE, SIG_IGN);
    return rb;
}

// Reserve space for zero-copy write
void* psr_reserve(struct psr_ringbuffer* rb, size_t size, struct psr_msg_header** header) {
    if (!rb || !header || size == 0 || size > rb->header->buffer_size / 2) {
        return NULL;
    }
    
    size_t total_size = sizeof(struct psr_msg_header) + size;
    total_size = align_size(total_size, 8);  // Align to 8 bytes
    
    while (1) {
        uint64_t write_pos = PSR_LOAD_ACQUIRE(&rb->header->write_pos);
        uint64_t next_pos = (write_pos + total_size) % rb->header->buffer_size;
        
        // Check if we have enough contiguous space
        if (next_pos < write_pos && next_pos < total_size) {
            // Need to wrap around, skip to beginning
            next_pos = total_size;
            if (!PSR_CAS(&rb->header->write_pos, &write_pos, 0)) {
                continue;  // Someone else wrapped, retry
            }
            write_pos = 0;
        }
        
        // Try to reserve space
        if (PSR_CAS(&rb->header->write_pos, &write_pos, next_pos)) {
            // Successfully reserved space
            struct psr_msg_header* msg = (struct psr_msg_header*)(rb->data_buffer + write_pos);
            
            // Initialize header
            uint64_t seq = __atomic_fetch_add(&rb->header->write_seq, 1, __ATOMIC_ACQ_REL);
            msg->sequence = seq;
            msg->size = size;
            msg->status = MSG_STATUS_WRITING;
            gettimeofday(&msg->timestamp, NULL);
            
            *header = msg;
            return (char*)msg + sizeof(struct psr_msg_header);
        }
    }
}

// Commit reserved message
int psr_commit(struct psr_ringbuffer* rb, struct psr_msg_header* header) {
    if (!rb || !header) return -1;
    
    // Mark message as ready
    PSR_STORE_RELEASE(&header->status, MSG_STATUS_READY);
    
    // Update statistics
    __atomic_fetch_add(&rb->header->total_published, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(&rb->header->total_bytes, header->size, __ATOMIC_RELAXED);
    
    // Notify subscribers if needed
    // Simple notification: write to FIFOs of active subscribers
    for (int i = 0; i < rb->header->max_subscribers; i++) {
        if (rb->sub_slots[i].pid != 0 && rb->sub_slots[i].notify_enabled) {
            int fd = create_notification_fifo(rb->shm_key, i, 0);
            if (fd >= 0) {
                char c = 1;
                write(fd, &c, 1);  // Ignore errors
                close(fd);
            }
        }
    }
    
    return 0;
}

// Publish with copy
int psr_publish(struct psr_ringbuffer* rb, const void* data, size_t size) {
    struct psr_msg_header* header;
    void* buffer = psr_reserve(rb, size, &header);
    if (!buffer) return -1;
    
    memcpy(buffer, data, size);
    return psr_commit(rb, header);
}

// Open as subscriber
struct psr_subscriber* psr_open_subscriber(uint64_t shm_key) {
    struct psr_ringbuffer* rb = psr_open_publisher(shm_key);  // Reuse publisher open
    if (!rb) return NULL;
    
    struct psr_subscriber* sub = calloc(1, sizeof(struct psr_subscriber));
    if (!sub) {
        psr_destroy(rb);
        snprintf(g_errmsg, sizeof(g_errmsg), "Out of memory");
        return NULL;
    }
    
    sub->rb = rb;
    sub->slot_index = -1;
    sub->event_fd = -1;
    
    // Find or allocate subscriber slot
    pid_t pid = getpid();
    for (int i = 0; i < rb->header->max_subscribers; i++) {
        if (rb->sub_slots[i].pid == pid) {
            sub->slot_index = i;
            break;
        }
    }
    
    if (sub->slot_index < 0) {
        // Allocate new slot
        for (int i = 0; i < rb->header->max_subscribers; i++) {
            pid_t expected = 0;
            if (PSR_CAS(&rb->sub_slots[i].pid, &expected, pid)) {
                sub->slot_index = i;
                rb->sub_slots[i].active = 1;
                rb->sub_slots[i].read_seq = 0;
                __atomic_fetch_add(&rb->header->active_subscribers, 1, __ATOMIC_RELAXED);
                break;
            }
        }
    }
    
    if (sub->slot_index < 0) {
        psr_destroy(rb);
        free(sub);
        snprintf(g_errmsg, sizeof(g_errmsg), "No available subscriber slots");
        return NULL;
    }
    
    sub->last_read_seq = rb->sub_slots[sub->slot_index].read_seq;
    return sub;
}

// Get event file descriptor
int psr_subscriber_get_eventfd(struct psr_subscriber* sub) {
    if (!sub || sub->event_fd >= 0) return sub ? sub->event_fd : -1;
    
    sub->event_fd = create_notification_fifo(sub->rb->shm_key, sub->slot_index, 1);
    if (sub->event_fd >= 0) {
        sub->rb->sub_slots[sub->slot_index].notify_enabled = 1;
    }
    
    return sub->event_fd;
}

// Consume event
int psr_subscriber_consume_event(struct psr_subscriber* sub) {
    if (!sub || sub->event_fd < 0) return -1;
    
    char buffer[64];
    while (read(sub->event_fd, buffer, sizeof(buffer)) > 0) {
        // Drain the FIFO
    }
    
    return 0;
}

// Read next message (zero-copy)
const void* psr_subscriber_read_next(struct psr_subscriber* sub, size_t* size, struct timeval* timestamp) {
    if (!sub || !size) return NULL;
    
    uint64_t target_seq = sub->last_read_seq + 1;
    uint64_t current_write_seq = PSR_LOAD_ACQUIRE(&sub->rb->header->write_seq);
    
    if (target_seq >= current_write_seq) {
        return NULL;  // No new messages
    }
    
    // Search for the message with target sequence
    // This is a simplified linear search - in production, you'd want an index
    size_t buffer_size = sub->rb->header->buffer_size;
    char* buffer = sub->rb->data_buffer;
    
    for (size_t pos = 0; pos < buffer_size; ) {
        struct psr_msg_header* header = (struct psr_msg_header*)(buffer + pos);
        
        // Check if this position has a valid message
        if (PSR_LOAD_ACQUIRE(&header->status) == MSG_STATUS_READY) {
            if (header->sequence == target_seq) {
                // Found our message
                *size = header->size;
                if (timestamp) *timestamp = header->timestamp;
                
                sub->last_read_seq = target_seq;
                sub->rb->sub_slots[sub->slot_index].read_seq = target_seq;
                
                return (char*)header + sizeof(struct psr_msg_header);
            }
        }
        
        // Move to next potential message location
        size_t msg_size = sizeof(struct psr_msg_header) + header->size;
        msg_size = align_size(msg_size, 8);
        pos += msg_size;
        
        if (pos + sizeof(struct psr_msg_header) > buffer_size) {
            pos = 0;  // Wrap around
        }
    }
    
    return NULL;  // Message not found (might have been overwritten)
}

// Skip to latest
int psr_subscriber_skip_to_latest(struct psr_subscriber* sub) {
    if (!sub) return -1;
    
    uint64_t latest_seq = PSR_LOAD_ACQUIRE(&sub->rb->header->write_seq) - 1;
    sub->last_read_seq = latest_seq;
    sub->rb->sub_slots[sub->slot_index].read_seq = latest_seq;
    
    return 0;
}

// Cleanup
void psr_destroy(struct psr_ringbuffer* rb) {
    if (!rb) return;
    
    if (rb->header) shmdt(rb->header);
    free(rb);
}

void psr_destroy_and_remove(struct psr_ringbuffer* rb) {
    if (!rb) return;
    
    int shm_id = rb->shm_id;
    psr_destroy(rb);
    
    if (shm_id >= 0) {
        shmctl(shm_id, IPC_RMID, NULL);
    }
}

void psr_subscriber_close(struct psr_subscriber* sub) {
    if (!sub) return;
    
    // Clear subscriber slot
    if (sub->slot_index >= 0) {
        sub->rb->sub_slots[sub->slot_index].pid = 0;
        sub->rb->sub_slots[sub->slot_index].active = 0;
        sub->rb->sub_slots[sub->slot_index].notify_enabled = 0;
        __atomic_fetch_sub(&sub->rb->header->active_subscribers, 1, __ATOMIC_RELAXED);
    }
    
    if (sub->event_fd >= 0) close(sub->event_fd);
    
    psr_destroy(sub->rb);
    free(sub);
}

// Get statistics
int psr_get_stats(struct psr_ringbuffer* rb, struct psr_stats* stats) {
    if (!rb || !stats) return -1;
    
    stats->total_published = rb->header->total_published;
    stats->total_bytes = rb->header->total_bytes;
    stats->active_subscribers = rb->header->active_subscribers;
    
    // Calculate buffer usage (simplified)
    uint64_t used = rb->header->write_pos;
    stats->buffer_usage_percent = (used * 100) / rb->header->buffer_size;
    
    return 0;
}

// Get shared memory ID
int psr_get_shmid(struct psr_ringbuffer* rb) {
    return rb ? rb->shm_id : -1;
}