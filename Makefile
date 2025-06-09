# Makefile for Publish-Subscribe Ring Buffer

CC = gcc
CFLAGS = -Wall -O2 -g
LDFLAGS = -lpthread -lrt

# Targets
TARGETS = pubsub_publisher pubsub_subscriber test_pubsub

# Source files
PUBSUB_OBJS = pubsub_ringbuffer.o

all: $(TARGETS)

# Publisher
pubsub_publisher: pubsub_publisher.o $(PUBSUB_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Subscriber
pubsub_subscriber: pubsub_subscriber.o $(PUBSUB_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Test program
test_pubsub: test_pubsub.o $(PUBSUB_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Object files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean
clean:
	rm -f *.o $(TARGETS)

# Install (optional)
install: $(TARGETS)
	@echo "Installing to /usr/local/bin..."
	@cp $(TARGETS) /usr/local/bin/ 2>/dev/null || echo "Install failed. Try 'sudo make install'"

.PHONY: all clean install
