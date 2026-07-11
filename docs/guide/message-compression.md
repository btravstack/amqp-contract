# Message Compression

Learn how to use message compression to reduce bandwidth and improve performance for large AMQP payloads.

## Overview

The `@amqp-contract` library supports optional message compression using industry-standard algorithms (gzip and deflate). Compression is applied at **publish time** as a runtime decision, giving you flexibility to compress messages based on size, content type, or other runtime conditions.

### Key Features

- **Runtime Decision**: Choose whether to compress each message when publishing
- **Automatic Decompression**: Workers automatically decompress messages based on `contentEncoding` header
- **Type-Safe**: Compression options are fully type-checked
- **Zero Consumer Config**: No configuration needed on the consumer side
- **Multiple Algorithms**: Support for gzip and deflate compression

## When to Use Compression

Compression is beneficial for:

✅ **Large messages** (>1KB) - Reduces network bandwidth and transmission time  
✅ **Text-heavy payloads** - JSON, XML, and text compress very well  
✅ **High-volume messaging** - Reduces network costs and improves throughput  
✅ **Limited bandwidth** - Useful in constrained network environments

Compression may not be beneficial for:

❌ **Small messages** (<500 bytes) - Compression overhead may exceed savings  
❌ **Already compressed data** - Images, videos, or pre-compressed files  
❌ **CPU-constrained systems** - Compression requires CPU resources

## Basic Usage

### Publishing with Compression

Compression is specified in the publish options:

```typescript
import { TypedAmqpClient } from "@amqp-contract/client";
import { contract } from "./contract";

const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
}).getOrThrow();

// Publish with gzip compression
await client
  .publish(
    "orderCreated",
    {
      orderId: "ORD-123",
      items: [...], // Large array of items
    },
    {
      compression: "gzip",
    },
  )

// Publish with deflate compression
await client
  .publish(
    "orderCreated",
    {
      orderId: "ORD-124",
      items: [...],
    },
    {
      compression: "deflate",
    },
  )

// Publish without compression
await client
  .publish("orderCreated", {
    orderId: "ORD-125",
    items: [],
  })
```

### Consuming Compressed Messages

**No configuration needed!** Workers automatically detect and decompress messages:

```typescript
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { contract } from "./contract";

const worker = await TypedAmqpWorker.create({
  contract,
  handlers: {
    processOrder: ({ payload }) => {
      // Message is automatically decompressed
      console.log("Processing order:", payload.orderId);
      console.log("Items:", payload.items); // Already decompressed
      return Ok(undefined).toAsync();
    },
  },
  urls: ["amqp://localhost"],
}).getOrThrow();
```

The worker automatically:

1. Reads the `contentEncoding` header
2. Decompresses the payload if needed
3. Validates and passes the decompressed message to your handler

## Conditional Compression

Compress messages based on runtime conditions:

```typescript
class OrderPublisher {
  constructor(private client: TypedAmqpClient<typeof contract>) {}

  async publishOrder(order: Order) {
    // Calculate message size
    const messageSize = JSON.stringify(order).length;

    // Compress if message is larger than 1KB
    const shouldCompress = messageSize > 1024;

    await this.client.publish("orderCreated", order, {
      compression: shouldCompress ? "gzip" : undefined,
    });
  }
}
```

## Compression Algorithms

### gzip

- **Best for**: General-purpose compression
- **Compression ratio**: High (typically 70-80% reduction for text)
- **Speed**: Moderate
- **Compatibility**: Widely supported

```typescript
client.publish("event", data, { compression: "gzip" });
```

### deflate

- **Best for**: Faster compression with slightly lower ratio
- **Compression ratio**: Good (typically 65-75% reduction for text)
- **Speed**: Fast
- **Compatibility**: Widely supported

```typescript
client.publish("event", data, { compression: "deflate" });
```

## How It Works

### Publishing Flow

1. **Message validation**: Schema validation happens first
2. **Serialization**: Message is converted to JSON
3. **Compression**: If specified, payload is compressed using the chosen algorithm
4. **Header setting**: `contentEncoding` header is set to the algorithm name
5. **Publishing**: Compressed payload is sent to RabbitMQ

### Consumption Flow

1. **Message received**: Worker receives the message
2. **Header check**: `contentEncoding` header is read
3. **Decompression**: If present, payload is decompressed
4. **Deserialization**: JSON is parsed
5. **Validation**: Schema validation runs
6. **Handler invocation**: Your handler receives the validated message

## Error Handling

### Compression Errors

Compression errors are returned in the Result type:

```typescript
await client.publish("event", data, {
  compression: "gzip",
});
```

### Decompression Errors

Unsupported encodings throw errors during consumption:

```typescript
// Worker automatically handles known encodings (gzip, deflate)
// Unsupported encodings will throw an error and reject the message
```

## Best Practices

### 1. Measure Before Compressing

Test compression with your actual data:

```typescript
const testData = {
  /* your typical message */
};
const json = JSON.stringify(testData);
console.log("Original size:", json.length, "bytes");

// Test compression
const { gzip } = require("node:zlib");
const { promisify } = require("node:util");
const gzipAsync = promisify(gzip);

const compressed = await gzipAsync(Buffer.from(json));
console.log("Compressed size:", compressed.length, "bytes");
console.log("Reduction:", ((1 - compressed.length / json.length) * 100).toFixed(1) + "%");
```

### 2. Set a Size Threshold

Only compress messages above a certain size:

```typescript
const SIZE_THRESHOLD = 1024; // 1KB

function shouldCompress(message: unknown): boolean {
  return JSON.stringify(message).length > SIZE_THRESHOLD;
}

await client.publish("event", data, {
  compression: shouldCompress(data) ? "gzip" : undefined,
});
```

### 3. Monitor Performance

Track compression ratios and performance:

```typescript
const startTime = Date.now();
const originalSize = JSON.stringify(data).length;

await client.publish("event", data, {
  compression: "gzip",
});

const duration = Date.now() - startTime;
console.log("Published in", duration, "ms");
console.log("Original size:", originalSize, "bytes");
```

### 4. Consider Content Type

Compress text-heavy content, skip binary data:

```typescript
function getCompressionForContent(data: unknown): "gzip" | undefined {
  // Check if data is text-heavy (JSON, strings, arrays)
  if (Array.isArray(data) || typeof data === "object") {
    return "gzip";
  }
  // Skip compression for binary or already compressed data
  return undefined;
}
```

### 5. Document Compression Usage

Document which messages use compression in your contract comments:

```typescript
/**
 * Order created event
 *
 * @remarks
 * Consider using gzip compression for large orders with many items
 */
const orderMessage = defineMessage(orderSchema, {
  summary: "Order created event",
});
```

## Troubleshooting

### Message Size Still Large

- Verify compression is actually being applied (check `contentEncoding` header in RabbitMQ UI)
- Try a different algorithm (gzip vs deflate)
- Consider if your data is already compressed (images, etc.)

### Performance Issues

- Compression adds CPU overhead; monitor your application's CPU usage
- Consider reducing compression for time-sensitive messages
- Use deflate instead of gzip for faster (but slightly less effective) compression

### Compatibility Issues

- Ensure all consumers support decompression (built into `@amqp-contract/worker`)
- Check that RabbitMQ version supports content-encoding headers (3.x+)
- Verify network proxies don't interfere with compressed payloads

## Next Steps

- Learn about [Client Usage](/guide/client-usage)
- Explore [Worker Usage](/guide/worker-usage)
- See [Testing](/guide/testing) strategies
