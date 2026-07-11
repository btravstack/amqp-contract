# Performance Tuning Guide

This guide covers performance optimization strategies for amqp-contract applications.

## Prefetch Configuration

Prefetch controls how many messages a consumer receives before acknowledging them. Proper configuration is critical for throughput.

### Understanding Prefetch

```typescript
const handlers = {
  processOrder: [
    handler,
    { prefetch: 10 }, // Consumer-specific prefetch
  ],
};
```

**Guidelines:**

| Message Processing Time | Recommended Prefetch |
| ----------------------- | -------------------- |
| < 100ms (fast)          | 50-100               |
| 100ms - 1s (medium)     | 10-50                |
| > 1s (slow)             | 1-10                 |
| Mixed/Variable          | Start with 10        |

### When to Increase Prefetch

- Message processing is CPU-bound and fast
- Network latency to RabbitMQ is high
- You need higher throughput

### When to Decrease Prefetch

- Processing is I/O-bound or slow
- Messages vary significantly in processing time
- You want fairer distribution across consumers
- Memory usage is a concern

## Message Compression

Compression reduces network bandwidth but adds CPU overhead.

### Enabling Compression

```typescript
// Publish with compression
await client.publish("orderCreated", largePayload, {
  compression: "gzip", // Good compression ratio
});

// Or use deflate for faster compression
await client.publish("orderCreated", largePayload, {
  compression: "deflate",
});
```

### When to Use Compression

| Scenario                        | Recommendation                        |
| ------------------------------- | ------------------------------------- |
| Small messages (< 1KB)          | Skip compression (overhead > benefit) |
| Medium messages (1KB - 100KB)   | Consider deflate                      |
| Large messages (> 100KB)        | Use gzip                              |
| High throughput, small messages | Skip compression                      |
| Network-constrained environment | Always compress                       |

### Compression Comparison

| Algorithm | Compression Ratio | Speed  | CPU Usage |
| --------- | ----------------- | ------ | --------- |
| gzip      | Better (60-70%)   | Slower | Higher    |
| deflate   | Good (50-60%)     | Faster | Lower     |

## Connection Management

### Connection Sharing

amqp-contract automatically shares connections across clients and workers with the same URL and options:

```typescript
// These share the same underlying connection
const client = await TypedAmqpClient.create({ contract, urls }).getOrElse((e) => {
  throw e;
});
const worker = await TypedAmqpWorker.create({ contract, handlers, urls }).getOrElse((e) => {
  throw e;
});
```

### Connection Pool Sizing

For high-throughput scenarios, you may want separate connections:

```typescript
// Separate connection for publishing
const client = await TypedAmqpClient.create({
  contract,
  urls,
  connectionOptions: {
    connectionOptions: {
      clientProperties: { connection_name: "publisher" },
    },
  },
}).getOrElse((e) => {
  throw e;
});

// Separate connection for consuming
const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  urls,
  connectionOptions: {
    connectionOptions: {
      clientProperties: { connection_name: "consumer" },
    },
  },
}).getOrElse((e) => {
  throw e;
});
```

### Heartbeat Configuration

Heartbeats detect dead connections but add overhead:

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls,
  connectionOptions: {
    heartbeatIntervalInSeconds: 60, // Default: 0 (disabled)
  },
}).getOrElse((e) => {
  throw e;
});
```

**Recommendations:**

- Production: Enable heartbeats (30-60 seconds)
- High-latency networks: Use longer intervals
- Local development: Can be disabled

## Batched Publishing

For high-throughput publishing, batch messages:

```typescript
// Instead of individual publishes
for (const order of orders) {
  await client.publish("orderCreated", order); // Slow
}

// Use Promise.all for concurrent publishing
await Promise.all(orders.map((order) => client.publish("orderCreated", order)));
```

### Publisher Confirms

Publisher confirms ensure messages reach RabbitMQ:

```typescript
// amqp-contract uses confirms by default
// Each publish returns a AsyncResult that resolves when confirmed
const result = await client.publish("orderCreated", payload);
```

For maximum throughput without confirms (not recommended for critical messages):

```typescript
// Fire-and-forget publishing (use with caution)
client.publish("orderCreated", payload); // Don't await
```

## Queue Configuration

### Quorum vs Classic Queues

| Feature        | Quorum Queue   | Classic Queue                    |
| -------------- | -------------- | -------------------------------- |
| Durability     | Always durable | Configurable                     |
| Replication    | Raft-based     | Mirroring (deprecated)           |
| Performance    | Slightly lower | Higher                           |
| Memory         | Higher         | Lower                            |
| Delivery limit | Native support | Requires custom headers tracking |

**Recommendation:** Use quorum queues for production workloads.

### Queue Arguments

Configure queue behavior for performance:

```typescript
const orderQueue = defineQueue("orders", {
  type: "quorum",
  deadLetter: { exchange: dlx },
  arguments: {
    "x-max-length": 100000, // Limit queue size
    "x-overflow": "reject-publish", // Backpressure
    "x-message-ttl": 86400000, // 24 hour TTL
  },
});
```

## Memory Considerations

### Large Message Handling

For large messages, consider:

1. **Message size limits:**

   ```typescript
   arguments: {
     'x-max-length-bytes': 104857600,  // 100MB queue limit
   }
   ```

2. **Streaming for very large payloads:**
   - Store payload in object storage (S3, etc.)
   - Send reference in message

### Consumer Memory

Each prefetched message uses memory. Calculate:

```
Memory per consumer = prefetch × average_message_size × safety_factor
```

Example: 100 prefetch × 10KB messages × 2 = ~2MB per consumer

## Monitoring Performance

### Key Metrics

Monitor these metrics for performance tuning:

1. **Message throughput:**
   - Messages published/consumed per second
   - Compare against baseline

2. **Latency:**
   - Time from publish to consume
   - Handler execution time

3. **Queue depth:**
   - Messages waiting in queue
   - Should stay stable under load

4. **Consumer utilization:**
   - Prefetch buffer usage
   - Acknowledge rate

### Using OpenTelemetry

amqp-contract provides built-in metrics:

```typescript
// Metrics automatically recorded:
// - amqp.publish.count
// - amqp.publish.duration
// - amqp.consume.count
// - amqp.consume.duration
```

See [OpenTelemetry Guide](./opentelemetry-observability.md) for setup.

## Benchmarking

### Simple Benchmark Pattern

```typescript
import { performance } from "perf_hooks";

async function benchmark(iterations: number) {
  const start = performance.now();

  const promises = Array.from({ length: iterations }, (_, i) =>
    client.publish("orderCreated", { orderId: `order-${i}`, amount: 100 }),
  );

  await Promise.all(promises);

  const duration = performance.now() - start;
  console.log(`Published ${iterations} messages in ${duration}ms`);
  console.log(`Throughput: ${((iterations / duration) * 1000).toFixed(2)} msg/s`);
}

await benchmark(10000);
```

## Common Performance Issues

### Issue: Low Throughput

**Symptoms:** Messages accumulate in queue, consumers underutilized

**Solutions:**

- Increase prefetch
- Add more consumers
- Optimize handler code
- Check for I/O bottlenecks

### Issue: High Latency

**Symptoms:** Long time between publish and consume

**Solutions:**

- Reduce prefetch if handlers are slow
- Check network latency
- Enable compression for large messages
- Optimize handler code

### Issue: Memory Growth

**Symptoms:** Consumer memory increases over time

**Solutions:**

- Reduce prefetch
- Add max-length to queues
- Ensure messages are acknowledged
- Check for memory leaks in handlers

### Issue: Connection Drops

**Symptoms:** Frequent reconnections, message loss

**Solutions:**

- Enable heartbeats
- Check network stability
- Increase connection timeout
- Review RabbitMQ server logs

## Production Checklist

- [ ] Prefetch configured based on message processing time
- [ ] Heartbeats enabled (30-60 seconds)
- [ ] Quorum queues used for durability
- [ ] Dead letter exchanges configured
- [ ] Queue size limits set
- [ ] Monitoring and alerting in place
- [ ] Compression enabled for large messages
- [ ] Connection pooling configured if needed
