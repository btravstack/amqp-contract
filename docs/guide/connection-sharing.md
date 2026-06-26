# Connection Sharing Guide

## Overview

When an application uses both the AMQP client (for publishing) and worker (for consuming), amqp-contract automatically shares a single connection between them following RabbitMQ best practices. This guide explains how automatic connection sharing works.

## Why Share Connections?

According to [RabbitMQ best practices](https://www.rabbitmq.com/connections.html):

- **Connections are expensive**: TCP connection, TLS handshake, authentication, and heartbeat overhead
- **Channels are lightweight**: Multiplexed over a single connection
- **Best practice**: Share one connection, use multiple channels

### Benefits

1. **Resource Efficiency**: One TCP connection instead of two
2. **Reduced Overhead**: Single authentication and heartbeat loop
3. **Better Scalability**: Lower connection count in large deployments
4. **Cost Savings**: ~50ms startup time improvement, ~5-10MB memory savings per service

## Usage

### Automatic Connection Sharing (Recommended)

Connection sharing is **completely automatic** when you use the same URLs:

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { contract } from "./contract";

// 1. Create client - automatically creates connection
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"], // ← Just provide URLs
    connectionOptions: {
      heartbeatIntervalInSeconds: 30,
    },
  })
).unwrap();

// 2. Create worker - automatically reuses the same connection!
const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"], // ← Same URLs = automatic sharing
    handlers: {
      processOrder: ({ payload }) => {
        console.log("Processing order:", payload.orderId);

        // Can publish from within consumer — `publish` already returns a
        // AsyncResult, so we chain its combinators directly.
        return client
          .publish("orderProcessed", {
            orderId: payload.orderId,
            status: "completed",
          })
          .map(() => {
            console.log("Order processed event published");
          })
          .mapErr((error) => {
            console.error("Failed to publish:", error);
            return new RetryableError("Failed to publish", error);
          });
      },
    },
  })
).unwrap();

// Both client and worker automatically share a single connection! ✅
// Result: 1 connection, 2 channels
// No manual connection management needed!
```

### Lifecycle Management

With automatic connection sharing, lifecycle management is simple:

```typescript
// Create client and worker
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
  })
).get();

const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"],
    handlers: {
      /* ... */
    },
  })
).get();

// Close components when done
// 1. Close worker first (stops consuming)
await worker.close();

// 2. Close client (stops publishing)
await client.close();

// The shared connection is managed automatically by the singleton
```

**Important**: Each client and worker closes its own channel. When all clients/workers using a shared connection are closed, the underlying connection is automatically closed and cleaned up by the internal singleton's reference counting.

### How Connection Sharing Works

When you create multiple clients or workers with the same URLs and connection options, amqp-contract automatically reuses the same underlying connection:

```typescript
// ✅ Automatically shares a single connection
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"], // ← URLs match
  })
).unwrap();

const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"], // ← URLs match = shared connection
    handlers: {
      /* ... */
    },
  })
).unwrap();

// Result: 1 connection, 2 channels ✅
// - Less resource usage
// - Less network overhead
// - Faster startup
// - Zero manual connection management
```

The singleton `ConnectionManagerSingleton` internally caches connections based on URLs and connection options. When you create a new client or worker with matching parameters, it automatically returns the existing connection instead of creating a new one.

## Advanced Patterns

### Multiple Clients Sharing One Connection

You can create multiple clients and workers - they automatically share connections when URLs match:

```typescript
// All automatically share the same connection
const orderClient = (
  await TypedAmqpClient.create({
    contract: orderContract,
    urls: ["amqp://localhost"], // ← Same URLs
  })
).unwrap();

const notificationClient = (
  await TypedAmqpClient.create({
    contract: notificationContract,
    urls: ["amqp://localhost"], // ← Same URLs
  })
).unwrap();

const orderWorker = (
  await TypedAmqpWorker.create({
    contract: orderContract,
    urls: ["amqp://localhost"], // ← Same URLs
    handlers: {
      /* ... */
    },
  })
).unwrap();

const notificationWorker = (
  await TypedAmqpWorker.create({
    contract: notificationContract,
    urls: ["amqp://localhost"], // ← Same URLs
    handlers: {
      /* ... */
    },
  })
).unwrap();

// All automatically share one connection with 4 separate channels
```

### Multiple Separate Connections

If you need separate connections (e.g., for different RabbitMQ clusters), just use different URLs:

```typescript
// These will have separate connections
const mainClient = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://main-cluster"], // ← Different URLs
  })
).unwrap();

const analyticsClient = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://analytics-cluster"], // ← Different URLs
  })
).unwrap();

// Result: 2 separate connections (one per cluster)
```

## Best Practices and Limitations

### Connection Configuration Best Practices

When using automatic connection sharing, follow these best practices to avoid configuration conflicts:

#### 1. **Use Consistent Connection Options**

For maximum sharing benefits, use the same `connectionOptions` across all clients and workers, or omit them to use defaults:

```typescript
// ✅ Best: Use consistent options (or omit for defaults)
const connectionOptions = { heartbeatIntervalInSeconds: 30 };

const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
    connectionOptions, // ← Same options
  })
).unwrap();

const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"],
    connectionOptions, // ← Same options = connection shared
    handlers: {
      /* ... */
    },
  })
).unwrap();

// ✅ Also good: Omit options to use defaults
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"], // ← No options
  })
).unwrap();

const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"], // ← No options = connection shared
    handlers: {
      /* ... */
    },
  })
).unwrap();
```

#### 2. **Extract Shared Configuration**

For applications with multiple clients/workers, define shared configuration once:

```typescript
// ✅ Recommended: Centralize connection configuration
const AMQP_CONFIG = {
  urls: ["amqp://localhost"],
  connectionOptions: {
    heartbeatIntervalInSeconds: 30,
    reconnectTimeInSeconds: 5,
  },
} as const;

// All components use the same configuration
const client = (
  await TypedAmqpClient.create({
    contract: orderContract,
    ...AMQP_CONFIG,
  })
).unwrap();

const worker = (
  await TypedAmqpWorker.create({
    contract: orderContract,
    ...AMQP_CONFIG,
    handlers: {
      /* ... */
    },
  })
).unwrap();
```

#### 3. **Understand Configuration Conflicts**

Different `connectionOptions` create separate connections:

```typescript
// ⚠️ Warning: Different options = separate connections (may be intentional)
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
    connectionOptions: { heartbeatIntervalInSeconds: 30 }, // ← Options A
  })
).unwrap();

const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"],
    connectionOptions: { heartbeatIntervalInSeconds: 60 }, // ← Options B (different)
    handlers: {
      /* ... */
    },
  })
).unwrap();

// Result: 2 separate connections (different configurations)
// This may be intentional if you need different heartbeat settings
```

### Limitations

1. **Connection Options Must Match for Sharing**
   - Connections are cached by both URLs and connection options
   - Different options = separate connections
   - Use consistent options or omit them for automatic sharing

2. **No Cross-Process Sharing**
   - Connection sharing only works within a single Node.js process
   - Each process creates its own connections
   - For multi-process deployments, each process will have its own connection pool

3. **Testing Requires Cache Reset**
   - The singleton caches connections across tests
   - In test cleanup, call `await AmqpClient._resetConnectionCacheForTesting()`
   - See "Cleanup in tests" section below

4. **Connection Lifecycle Tied to Usage**
   - Connections remain open as long as any client/worker is using them
   - Connections close automatically when all references are released
   - No manual connection lifecycle management available

## When to Use Connection Sharing

### ✅ Use Connection Sharing When:

- Your application both publishes and consumes messages
- You have multiple microservices in the same process
- You want to optimize resource usage
- You're following RabbitMQ best practices

### ❌ Don't Use Connection Sharing When:

- You only publish OR only consume (not both)
- Clients/workers are in different processes
- You need complete isolation between components
- The added complexity isn't worth the benefits for your use case

## Performance Impact

### Startup Time

- **Before**: ~100-200ms per connection
- **After**: ~50ms (shared connection)
- **Savings**: ~50-150ms per service

### Memory Usage

- **Before**: ~5-10 MB per connection
- **After**: ~5-10 MB total (shared)
- **Savings**: ~5-10 MB per hybrid service

### Scalability

**Scenario**: 100 microservices, 50% are hybrid (both publish and consume)

- **Before**: 150 connections (100 single-purpose + 50×2 hybrid)
- **After**: 100 connections (100 single-purpose + 50 hybrid)
- **Improvement**: 33% reduction in connection count

## Backward Compatibility

Connection sharing is **completely backward compatible** and happens automatically:

```typescript
// Existing code automatically benefits from connection sharing
const client = (
  await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"], // ← Connection automatically created
  })
).unwrap();

const worker = (
  await TypedAmqpWorker.create({
    contract,
    urls: ["amqp://localhost"], // ← Same URLs = connection automatically shared
    handlers: {
      /* ... */
    },
  })
).unwrap();

// No code changes needed - connection sharing just works!
// Result: 1 connection, 2 channels (automatically managed)
```

## Troubleshooting

### Connection sharing not working

Connection sharing is automatic when URLs and connection options match. If you see multiple connections:

1. **Check URLs match exactly**:

   ```typescript
   // ❌ Different URLs = different connections
   const client = (
     await TypedAmqpClient.create({
       contract,
       urls: ["amqp://localhost:5672"],
     })
   ).unwrap();
   const worker = (
     await TypedAmqpWorker.create({
       contract,
       urls: ["amqp://localhost"], // Different URL!
       handlers: {
         /* ... */
       },
     })
   ).unwrap();

   // ✅ Same URLs = shared connection
   const urls = ["amqp://localhost"];
   const client = (
     await TypedAmqpClient.create({
       contract,
       urls,
     })
   ).unwrap();
   const worker = (
     await TypedAmqpWorker.create({
       contract,
       urls, // Same URL reference
       handlers: {
         /* ... */
       },
     })
   ).unwrap();
   ```

2. **Check connection options match**:

   ```typescript
   // ❌ Different options = different connections
   const client = (
     await TypedAmqpClient.create({
       contract,
       urls: ["amqp://localhost"],
       connectionOptions: { heartbeatIntervalInSeconds: 30 },
     })
   ).unwrap();
   const worker = (
     await TypedAmqpWorker.create({
       contract,
       urls: ["amqp://localhost"],
       connectionOptions: { heartbeatIntervalInSeconds: 60 }, // Different!
       handlers: {
         /* ... */
       },
     })
   ).unwrap();

   // ✅ Same options = shared connection
   const connectionOptions = { heartbeatIntervalInSeconds: 30 };
   const client = (
     await TypedAmqpClient.create({
       contract,
       urls: ["amqp://localhost"],
       connectionOptions,
     })
   ).unwrap();
   const worker = (
     await TypedAmqpWorker.create({
       contract,
       urls: ["amqp://localhost"],
       connectionOptions, // Same options reference
       handlers: {
         /* ... */
       },
     })
   ).unwrap();
   ```

### Cleanup in tests

For test isolation, the internal connection cache can be reset:

```typescript
import { AmqpClient } from "@amqp-contract/core";

afterEach(async () => {
  await AmqpClient._resetConnectionCacheForTesting();
});
```

## Related Documentation

- [Client Usage Guide](/guide/client-usage)
- [Worker Usage Guide](/guide/worker-usage)
- [RabbitMQ Connection Best Practices](https://www.rabbitmq.com/connections.html)
