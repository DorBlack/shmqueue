# 发布订阅环形缓冲区实现

## 概述

这是一个基于共享内存的无锁发布订阅环形缓冲区实现，满足以下需求：

1. **发布订阅模式**：支持多个发布者和多个订阅者，每个订阅者都能独立读取所有发布的消息
2. **无锁实现**：使用原子操作实现无锁并发，支持通过宏配置不同的内存序
3. **FIFO通知机制**：支持通过文件描述符进行select/poll监听
4. **零拷贝接口**：提供零拷贝API减少内存拷贝次数

## 主要特性

### 1. 多发布者多订阅者支持
- 多个进程/线程可以同时发布消息
- 每个订阅者维护独立的读取位置
- 订阅者可以按自己的速度读取消息

### 2. 灵活的内存序控制

通过编译时宏 `PSR_MEMORY_ORDER_MODE` 控制内存序：

```c
// 0: Relaxed ordering - 最高性能，适用于单读单写场景
#define PSR_MEMORY_ORDER_MODE 0

// 1: Acquire-Release ordering - 平衡性能，适用于多读多写场景（默认）
#define PSR_MEMORY_ORDER_MODE 1  

// 2: Sequential consistency - 最强保证，适用于需要严格顺序的场景
#define PSR_MEMORY_ORDER_MODE 2
```

### 3. 零拷贝接口

发布者零拷贝：
```c
// 预留空间
struct psr_msg_header* header;
void* buffer = psr_reserve(rb, size, &header);

// 直接写入数据
memcpy(buffer, data, size);

// 提交消息
psr_commit(rb, header);
```

订阅者零拷贝：
```c
// 直接获取消息指针，无需拷贝
size_t size;
const void* data = psr_subscriber_read_next(sub, &size, &timestamp);
```

### 4. 事件通知机制

```c
// 获取事件文件描述符
int fd = psr_subscriber_get_eventfd(sub);

// 使用select监听
fd_set readfds;
FD_SET(fd, &readfds);
select(fd + 1, &readfds, NULL, NULL, &timeout);

// 消费事件
psr_subscriber_consume_event(sub);
```

## 编译和使用

### 编译

```bash
# 编译所有版本
make -f Makefile_pubsub all

# 编译特定内存序版本
make -f Makefile_pubsub MEMORY_ORDER=0  # Relaxed
make -f Makefile_pubsub MEMORY_ORDER=1  # Acquire-Release (默认)
make -f Makefile_pubsub MEMORY_ORDER=2  # Sequential Consistency

# 编译为库
make -f Makefile_pubsub libpubsub.so   # 动态库
make -f Makefile_pubsub libpubsub.a    # 静态库
```

### 运行测试

```bash
# 基本测试（2个发布者，3个订阅者）
make -f Makefile_pubsub test

# 性能对比测试
make -f Makefile_pubsub benchmark

# 清理
make -f Makefile_pubsub clean
make -f Makefile_pubsub clean_shm  # 清理共享内存
```

## API 使用示例

### 发布者

```c
// 创建或打开环形缓冲区
struct psr_ringbuffer* rb = psr_create(0x12345678, 1024*1024, 10);
// 或
struct psr_ringbuffer* rb = psr_open_publisher(0x12345678);

// 发布消息（拷贝方式）
psr_publish(rb, data, size);

// 发布消息（零拷贝方式）
struct psr_msg_header* header;
void* buffer = psr_reserve(rb, size, &header);
if (buffer) {
    // 直接写入buffer
    memcpy(buffer, data, size);
    psr_commit(rb, header);
}

// 清理
psr_destroy(rb);
```

### 订阅者

```c
// 打开订阅者
struct psr_subscriber* sub = psr_open_subscriber(0x12345678);

// 获取事件通知fd
int event_fd = psr_subscriber_get_eventfd(sub);

// 事件循环
while (running) {
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(event_fd, &readfds);
    
    if (select(event_fd + 1, &readfds, NULL, NULL, &timeout) > 0) {
        psr_subscriber_consume_event(sub);
        
        // 读取所有可用消息
        while (1) {
            size_t size;
            struct timeval timestamp;
            const void* data = psr_subscriber_read_next(sub, &size, &timestamp);
            if (!data) break;
            
            // 处理消息
            process_message(data, size);
        }
    }
}

// 清理
psr_subscriber_close(sub);
```

## 性能优化建议

1. **选择合适的内存序**：
   - 单发布者单订阅者：使用 `PSR_MEMORY_ORDER_MODE=0`
   - 多发布者或多订阅者：使用 `PSR_MEMORY_ORDER_MODE=1`（默认）
   - 需要严格顺序保证：使用 `PSR_MEMORY_ORDER_MODE=2`

2. **减少内存拷贝**：
   - 尽量使用零拷贝接口（`psr_reserve`/`psr_commit`）
   - 订阅者使用 `psr_subscriber_read_next` 直接访问数据

3. **批量处理**：
   - 订阅者收到通知后，一次性读取所有可用消息
   - 发布者可以批量发布消息后再触发通知

4. **缓冲区大小**：
   - 根据消息大小和频率调整缓冲区大小
   - 避免缓冲区过小导致消息被覆盖

## 实现细节

### 数据结构布局

```
共享内存布局：
+------------------------+
| Header (缓存行对齐)    |
+------------------------+
| 订阅者槽位数组         |
+------------------------+
| 环形缓冲区数据         |
| +------------------+   |
| | Message Header   |   |
| | Message Data     |   |
| +------------------+   |
| | Message Header   |   |
| | Message Data     |   |
| +------------------+   |
| ...                    |
+------------------------+
```

### 无锁算法

1. **多写支持**：使用CAS操作原子地预留写入空间
2. **读写分离**：每个订阅者维护独立的读取序列号
3. **消息完整性**：使用状态标记确保读取完整消息

### 通知机制

- 使用FIFO文件实现跨进程通知
- 每个订阅者有独立的FIFO文件
- 支持select/poll/epoll等IO多路复用

## 限制和注意事项

1. 最大订阅者数量：64（可通过修改 `PSR_MAX_SUBSCRIBERS` 调整）
2. 消息可能被覆盖：当环形缓冲区满时，旧消息会被覆盖
3. 需要定期清理FIFO文件：位于 `/tmp/psr_fifo_*`
4. 共享内存需要手动清理：使用 `ipcrm` 或调用 `psr_destroy_and_remove`

## 与原 shmqueue 的对比

| 特性 | 原 shmqueue | 新 pubsub_ringbuffer |
|------|------------|---------------------|
| 模式 | 队列（消费即删除） | 发布订阅（多次读取） |
| 写并发 | 单写 | 多写 |
| 内存序 | 固定 | 可配置 |
| 零拷贝 | 部分支持 | 完全支持 |
| 订阅者管理 | N/A | 独立读取位置 |