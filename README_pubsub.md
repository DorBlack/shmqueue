# Publish-Subscribe Ring Buffer

A high-performance publish-subscribe messaging system based on shared memory ring buffer.

## Features

- **True Publish-Subscribe Pattern**: Publishers write without caring about subscribers
- **Ring Buffer Design**: Automatically overwrites old data when buffer is full
- **Multiple Publishers/Subscribers**: Support for multiple concurrent publishers and subscribers
- **Independent Read Positions**: Each subscriber maintains its own read position
- **Data Loss Detection**: Subscribers can detect when data has been overwritten
- **Zero-Copy Design**: Efficient data transfer through shared memory
- **Cross-Process Communication**: Works across different processes
- **Sequence Numbering**: Built-in sequence numbers for message ordering

## Architecture

```
    [Publisher 1] ----\
                      \
    [Publisher 2] -----+----> [Ring Buffer in SHM] ----+----> [Subscriber 1]
                      /                                 |
    [Publisher 3] ----/                                 +----> [Subscriber 2]
                                                        |
                                                        +----> [Subscriber N]
```

## API Overview

### Publisher Functions

```c
// Create a new ring buffer
pubsub_rb_t* psrb_create(uint64_t shm_key, uint32_t element_size, uint32_t element_count);

// Open existing ring buffer as publisher
pubsub_rb_t* psrb_open_publisher(uint64_t shm_key);

// Publish data (overwrites old data if buffer is full)
int psrb_publish(pubsub_rb_t* rb, const void* data, uint32_t data_len);

// Get statistics
void psrb_get_stats(pubsub_rb_t* rb, psrb_stats_t* stats);

// Cleanup
void psrb_destroy(pubsub_rb_t* rb);
void psrb_destroy_and_remove(pubsub_rb_t* rb);
```

### Subscriber Functions

```c
// Open ring buffer as subscriber
pubsub_reader_t* psrb_open_subscriber(uint64_t shm_key);

// Read next message
int psrb_read(pubsub_reader_t* reader, void* buffer, uint32_t buffer_size, 
              struct timeval* timestamp, uint64_t* sequence_num);

// Check available messages
uint32_t psrb_available(pubsub_reader_t* reader);

// Get lag (messages behind latest)
uint32_t psrb_get_lag(pubsub_reader_t* reader);

// Reset read position
void psrb_reset_to_latest(pubsub_reader_t* reader);
void psrb_reset_to_oldest(pubsub_reader_t* reader);

// Cleanup
void psrb_reader_destroy(pubsub_reader_t* reader);
```

## Building

```bash
make clean
make all
```

This will build:
- `pubsub_publisher`: Example publisher program
- `pubsub_subscriber`: Example subscriber program
- `test_pubsub`: Comprehensive test program

## Usage Examples

### Basic Publisher

```c
// Create ring buffer with 100 elements, 4KB each
pubsub_rb_t* rb = psrb_create(0x12345678, 4096, 100);

// Publish messages
char message[] = "Hello, World!";
if (psrb_publish(rb, message, strlen(message) + 1) == 0) {
    printf("Message published\n");
}

// Cleanup
psrb_destroy_and_remove(rb);
```

### Basic Subscriber

```c
// Open existing ring buffer
pubsub_reader_t* reader = psrb_open_subscriber(0x12345678);

// Read messages
char buffer[4096];
struct timeval tv;
uint64_t seq;

int len = psrb_read(reader, buffer, sizeof(buffer), &tv, &seq);
if (len > 0) {
    printf("Read message: %s (seq=%llu)\n", buffer, seq);
} else if (len == -2) {
    printf("Data loss detected!\n");
}

// Cleanup
psrb_reader_destroy(reader);
```

### Running Example Programs

1. **Start a publisher:**
```bash
./pubsub_publisher [shm_key] [element_count] [interval_ms] [message_size]
# Example: ./pubsub_publisher 0x12345678 100 50 256
```

2. **Start one or more subscribers:**
```bash
./pubsub_subscriber [shm_key] [interval_ms] [start_from_oldest] [verbose]
# Example: ./pubsub_subscriber 0x12345678 10 1 1
```

3. **Run comprehensive test:**
```bash
./test_pubsub
```

## Key Differences from Original Queue

| Feature | Original Queue | Publish-Subscribe Ring Buffer |
|---------|---------------|------------------------------|
| Pattern | Producer-Consumer | Publish-Subscribe |
| Buffer Full Behavior | Blocks/Returns Error | Overwrites Old Data |
| Read Position | Single Shared | Independent per Subscriber |
| Data Lifetime | Until Consumed | Until Overwritten |
| Writer Awareness | Checks Reader Status | No Reader Awareness |

## Performance Considerations

1. **Buffer Size**: Choose element count based on:
   - Expected message rate
   - Acceptable data loss tolerance
   - Available shared memory

2. **Element Size**: Should be larger than your largest message

3. **Read Frequency**: Subscribers should read frequently enough to avoid data loss

4. **Memory Barriers**: Uses hardware memory barriers for lock-free operations

## Error Handling

```c
// Check for errors
const char* error = psrb_errorstr();

// Read return values:
// > 0: Success, returns data length
//   0: No new data
//  -1: Error (check psrb_errorstr())
//  -2: Data loss detected
```

## Advanced Features

- **Anonymous Shared Memory**: Use shm_key=0 for anonymous SHM
- **Sequence Numbers**: Detect gaps in message stream
- **Timestamps**: Each message includes publication timestamp
- **Statistics**: Track publishes, overwrites, and buffer usage
- **Peek Function**: Read without consuming

## Limitations

- Maximum message size: 32MB (configurable)
- Subscribers must handle data loss gracefully
- No built-in persistence
- No network support (local machine only)