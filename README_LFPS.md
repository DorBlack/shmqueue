# Lock-free Publish-Subscribe System (LFPS)

A high-performance, lock-free publish-subscribe messaging system with configurable memory ordering for different usage patterns.

## Features

- **Lock-free Implementation**: Uses atomic operations and CAS for thread-safe operations
- **Configurable Memory Ordering**: Optimized for different producer/consumer patterns
- **FIFO Notification**: Support for `select()`/`poll()` based event notification
- **Zero-Copy Design**: Efficient data transfer through shared memory
- **Cache-Line Optimization**: Avoids false sharing for better performance
- **Per-Reader Notification**: Individual notification thresholds and control

## Memory Ordering Modes

The system supports four different modes optimized for specific usage patterns:

| Mode | Description | Memory Barriers | Use Case |
|------|-------------|-----------------|----------|
| **SPSC** | Single Producer Single Consumer | Minimal (compiler only) | Fastest, point-to-point communication |
| **MPSC** | Multiple Producer Single Consumer | Full fence on producer | Multiple sources to single processor |
| **SPMC** | Single Producer Multiple Consumer | Store/Load fences | Broadcast from single source |
| **MPMC** | Multiple Producer Multiple Consumer | Full fences | Most flexible, general purpose |

## Building

### Build for specific mode:
```bash
make -f Makefile.lfps MODE=SPSC    # Single Producer Single Consumer
make -f Makefile.lfps MODE=MPSC    # Multiple Producer Single Consumer
make -f Makefile.lfps MODE=SPMC    # Single Producer Multiple Consumer
make -f Makefile.lfps MODE=MPMC    # Multiple Producer Multiple Consumer (default)
```

### Build all modes:
```bash
make -f Makefile.lfps all-modes
```

This creates binaries with mode suffixes: `lfps_publisher_demo_spsc`, `lfps_publisher_demo_mpsc`, etc.

## API Usage

### Publisher Side

```c
#include "lockfree_pubsub.h"

// Create ring buffer
lfps_ring_t* ring = lfps_create(0x12345678, 4096, 1000);

// Publish message
char msg[] = "Hello, World!";
lfps_publish(ring, msg, sizeof(msg));

// Publish without notification (for batch publishing)
lfps_publish_notify(ring, msg, sizeof(msg), 0);

// Cleanup
lfps_destroy(ring);
```

### Subscriber Side with Select

```c
// Open reader
lfps_reader_t* reader = lfps_open_reader(0x12345678);

// Enable notification with threshold
lfps_reader_enable_notify(reader, 10);  // Notify when 10+ messages available
int fd = lfps_reader_get_fd(reader);

// Select loop
fd_set rfds;
struct timeval tv;
char buffer[4096];

while (running) {
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    
    if (select(fd + 1, &rfds, NULL, NULL, &tv) > 0) {
        // Consume notification
        lfps_reader_consume_event(reader, 1);
        
        // Read messages
        int len;
        while ((len = lfps_read(reader, buffer, sizeof(buffer), NULL, NULL)) > 0) {
            // Process message
        }
    }
}

lfps_reader_destroy(reader);
```

## Running Examples

### 1. Basic Publisher/Subscriber

Start publisher:
```bash
./lfps_publisher_demo [shm_key] [threads] [msg_size] [interval_us] [duration]
# Example: ./lfps_publisher_demo 0x12345678 4 256 0 10
```

Start subscriber with select:
```bash
./lfps_subscriber_select [shm_key] [threads] [use_select] [batch_size] [duration]
# Example: ./lfps_subscriber_select 0x12345678 2 1 10 10
```

### 2. Performance Benchmark

Run benchmark for current mode:
```bash
./lfps_benchmark
```

Run specific scenario:
```bash
./lfps_benchmark [publishers] [subscribers] [message_size]
# Example: ./lfps_benchmark 4 4 256
```

## Performance Characteristics

### Memory Ordering Impact

| Mode | Relative Performance | Scalability |
|------|---------------------|-------------|
| SPSC | 100% (baseline) | N/A |
| MPSC | ~85-90% | Good for many writers |
| SPMC | ~80-85% | Good for many readers |
| MPMC | ~70-80% | Best overall scalability |

### Notification Overhead

- **Polling**: Highest throughput, highest CPU usage
- **Select with batching**: Lower CPU, slightly lower throughput
- **Threshold tuning**: Higher threshold = better throughput, higher latency

## Design Details

### Lock-free Algorithm

1. **Publishing**:
   - Atomic increment to allocate sequence number
   - Write data with WRITING state
   - Memory fence
   - Update to READY state
   - CAS to update write sequence

2. **Reading**:
   - Check for new data
   - Verify sequence continuity
   - Read data if READY
   - Update read position

### Data Structure

```
[SHM Header]
  - Magic/Version
  - Element size/count
  - Write sequence (cache-aligned)
  - Allocation sequence (cache-aligned)
  - Statistics
  - Reader registrations

[Ring Buffer]
  - Message 0: [State][Length][Sequence][Timestamp][Data...]
  - Message 1: [State][Length][Sequence][Timestamp][Data...]
  - ...
  - Message N: [State][Length][Sequence][Timestamp][Data...]
```

### FIFO Notification

- Each reader has dedicated FIFO at `/tmp/lfps_<key>_<reader_id>`
- Writers check notification threshold before sending
- Readers use select/poll/epoll for efficient waiting
- Supports both edge-triggered and level-triggered modes

## Tuning Guidelines

### Buffer Size
- Choose based on: message rate × maximum acceptable latency
- Larger buffer = more tolerance for slow readers
- Consider memory constraints

### Notification Threshold
- Low threshold (1-10): Low latency, more syscalls
- Medium threshold (10-100): Balanced
- High threshold (100+): High throughput, batched processing

### CPU Affinity
- Use `lfps_set_cpu_affinity()` to pin threads
- Separate producers and consumers to different cores
- Consider NUMA topology for large systems

## Comparison with Original Queue

| Feature | Original shm_queue | Lock-free LFPS |
|---------|-------------------|----------------|
| Locking | Mutex-based | Lock-free (CAS) |
| Scalability | Limited | High |
| Memory Ordering | Fixed | Configurable |
| Notification | Basic | Advanced with thresholds |
| Performance | Good | Excellent |
| Complexity | Simple | Advanced |

## Limitations

- Maximum 64 concurrent readers
- No persistence
- Local machine only (shared memory)
- Readers must handle data loss
- x86/x86_64 optimized (other architectures use generic atomics)

## Error Handling

Check `lfps_errorstr()` for detailed error messages:
```c
if (operation_failed) {
    fprintf(stderr, "Error: %s\n", lfps_errorstr());
}
```

## Advanced Usage

### Prefetching
```c
// Prefetch next message for better cache performance
lfps_prefetch_next(reader);
```

### Batch Reading
```c
// Read multiple messages at once (not implemented in basic version)
void* buffers[10];
uint32_t sizes[10];
int count = lfps_read_batch(reader, buffers, sizes, 10, NULL, NULL);
```

### Statistics
```c
lfps_stats_t stats;
lfps_get_stats(ring, &stats);
printf("Published: %llu, Notifications: %llu\n", 
       stats.total_published, stats.notification_sent);
```