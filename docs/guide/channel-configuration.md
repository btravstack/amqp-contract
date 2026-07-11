# Channel Configuration Guide

## Overview

The `@amqp-contract/core` package provides `AmqpClient`, which manages AMQP connections and channels. By default, channels are configured with sensible defaults, but you can customize channel behavior to meet specific requirements like custom setup logic, message serialization, or publisher confirms.

## Why Customize Channels?

Channel configuration allows you to:

- **Customize message serialization**: Override the default JSON serialization
- **Customize publisher confirms**: Override default acknowledgments when messages are successfully routed
- **Add custom setup logic**: Configure prefetch, quality of service (QoS), or create additional AMQP resources
- **Debug channel behavior**: Set custom channel names for easier troubleshooting

## Basic Usage

### Default Configuration

By default, `AmqpClient` creates channels with JSON serialization and publisher confirms enabled:

```typescript
import { AmqpClient } from "@amqp-contract/core";
import { defineContract } from "@amqp-contract/contract";

const contract = defineContract({});

// Default: JSON serialization & publisher confirms enabled
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
});
```

### Custom Configuration

Use the `channelOptions` parameter to customize channel behavior:

```typescript
import { AmqpClient } from "@amqp-contract/core";
import type { Channel } from "amqplib";

const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    // Override JSON serialization (default: true)
    json: false,

    // Override publisher confirms (default: true)
    confirm: false,

    // Set a custom channel name for debugging
    name: "my-custom-channel",

    // Add custom setup logic after contract topology is established
    setup: async (channel: Channel) => {
      // Configure prefetch for better load distribution
      await channel.prefetch(10);

      // Add additional AMQP resources not in the contract
      await channel.assertQueue("custom-queue");
    },
  },
});
```

## Configuration Options

### JSON Serialization

Control whether messages are automatically serialized to JSON:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    json: false, // Disable automatic JSON serialization
  },
});
```

**When to disable JSON:**

- You need to send binary data or use a custom serialization format
- You're integrating with systems that don't use JSON
- You want full control over message encoding

**Note:** When using `@amqp-contract/client` or `@amqp-contract/worker`, JSON serialization is typically required for schema validation to work properly.

### Publisher Confirms

Control whether publisher confirms are enabled to receive acknowledgments when messages are routed:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    confirm: false, // Disable automatic publisher confirms
  },
});
```

**Publisher confirms guarantee:**

- Messages have been accepted by the broker
- Messages have been routed to at least one queue (if mandatory)
- Persistent messages have been written to disk (if durable)

**When to disable publisher confirms:**

- You don't need strong delivery guarantees
- You want minimum latency

### Channel Names

Set custom channel names for easier debugging:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    name: "order-publisher-channel", // Appears in RabbitMQ management UI
  },
});
```

**Benefits:**

- Identify channels in RabbitMQ management console
- Easier troubleshooting in production
- Clear correlation between code and runtime behavior

### Custom Setup Function

The `setup` function allows you to run custom initialization logic after the contract topology is established:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      // Configure Quality of Service (QoS)
      await channel.prefetch(10); // Process max 10 messages concurrently

      // Create additional resources not in contract
      await channel.assertQueue("dead-letter-queue");

      // Configure channel-level settings
      await channel.assertExchange("retry-exchange", "topic");
    },
  },
});
```

**Important:** The custom setup function runs **after** all contract-defined resources (exchanges, queues, bindings) are established. This ensures your contract is always properly set up before custom logic executes.

## Common Patterns

### Prefetch Configuration

Configure prefetch to control message flow and load distribution:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      // Limit to 10 unacknowledged messages per channel
      await channel.prefetch(10);
    },
  },
});
```

**Prefetch best practices:**

- **Low prefetch (1-10)**: Fair distribution across multiple workers
- **Medium prefetch (10-50)**: Balance between throughput and fairness
- **High prefetch (50+)**: Maximum throughput for single worker

### Dead Letter Queues

Set up dead letter queues for failed message handling:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      // Create dead letter exchange
      await channel.assertExchange("dlx", "topic");

      // Create dead letter queue
      await channel.assertQueue("dead-letters");

      // Bind dead letter queue to exchange
      await channel.bindQueue("dead-letters", "dlx", "#");
    },
  },
});
```

### Multiple Environment Configurations

Use environment-specific configurations:

```typescript
const channelOptions =
  process.env.NODE_ENV === "production"
    ? {
        name: `${process.env.SERVICE_NAME}-channel`,
        setup: async (channel: Channel) => {
          await channel.prefetch(50); // Higher throughput
        },
      }
    : {
        name: "dev-channel",
        setup: async (channel: Channel) => {
          await channel.prefetch(1); // Fair distribution for testing
        },
      };

const client = new AmqpClient(contract, {
  urls: [process.env.AMQP_URL ?? "amqp://localhost"],
  channelOptions,
});
```

## Setup Function Signatures

The setup function supports both Promise-based and callback-based signatures:

### Promise-Based (Recommended)

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      await channel.prefetch(10);
      // All async operations
    },
  },
});
```

### Callback-Based (Legacy)

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: (channel: Channel, callback: (error?: Error) => void) => {
      channel
        .prefetch(10)
        .then(() => callback())
        .catch((err) => callback(err));
    },
  },
});
```

**Recommendation:** Use Promise-based setup for cleaner, more maintainable code.

## Integration with Client and Worker

When using `@amqp-contract/client` or `@amqp-contract/worker`, channel options are not directly exposed. These packages use `AmqpClient` internally with appropriate defaults.

For advanced channel configuration needs:

1. **Use core package directly** for full control
2. **Submit a feature request** if you need channel options in client/worker
3. **Consider if the use case fits** the higher-level abstractions

## Best Practices

### 1. Keep Setup Logic Simple

```typescript
// ✅ Good: Simple, focused setup
channelOptions: {
  setup: async (channel: Channel) => {
    await channel.prefetch(10);
  },
}

// ❌ Avoid: Complex logic in setup
channelOptions: {
  setup: async (channel: Channel) => {
    // Avoid complex business logic here
    const config = await fetchConfigFromDatabase();
    await setupComplexTopology(channel, config);
  },
}
```

### 2. Document Custom Configuration

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      // Prefetch of 10 ensures fair distribution across workers
      // while maintaining reasonable throughput
      await channel.prefetch(10);
    },
  },
});
```

### 3. Test Custom Configuration

```typescript
import { describe, it, expect } from "vitest";

describe("Channel Configuration", () => {
  it("should apply custom prefetch setting", async () => {
    const client = new AmqpClient(contract, {
      urls: ["amqp://localhost"],
      channelOptions: {
        setup: async (channel: Channel) => {
          await channel.prefetch(10);
        },
      },
    });

    const connectResult = await client.waitForConnect();
    expect(connectResult.isOk()).toBe(true);

    await client.close().getOrElse((e) => {
      throw e;
    });
  });
});
```

### 4. Handle Setup Errors

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      try {
        await channel.prefetch(10);
        await channel.assertQueue("custom-queue");
      } catch (error) {
        console.error("Channel setup failed:", error);
        throw error; // Re-throw to trigger reconnection
      }
    },
  },
});
```

## Limitations

### 1. Setup Runs on Every Reconnection

The setup function executes every time the channel reconnects. Ensure your setup logic is idempotent:

```typescript
channelOptions: {
  setup: async (channel: Channel) => {
    // ✅ Idempotent: Can be called multiple times
    await channel.prefetch(10);
    await channel.assertQueue('queue'); // idempotent

    // ❌ Avoid: Non-idempotent operations
    // await incrementCounter(); // Called on every reconnect!
  },
}
```

### 2. Cannot Override Contract Topology

Custom setup runs **after** contract topology. You cannot override or prevent contract resources from being created:

```typescript
const contract = defineContract({
  // ... publishers and consumers
});

const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    setup: async (channel: Channel) => {
      // ❌ This won't prevent contract-defined resources from being created
      // The exchanges, queues, and bindings are already created by the contract
    },
  },
});
```

### 3. Limited to Channel-Level Configuration

Channel options only affect the individual channel. Connection-level configuration uses `connectionOptions`:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  connectionOptions: {
    heartbeatIntervalInSeconds: 30, // Connection-level
  },
  channelOptions: {
    confirm: false, // Channel-level
  },
});
```

## Troubleshooting

### Setup Function Not Called

**Symptom:** Custom setup logic doesn't execute.

**Solutions:**

1. Ensure channel is connected: check `(await client.waitForConnect()).isOk()` — `await` alone returns a `Result` and does not throw on failure
2. Check for errors in setup function (they may cause silent failures)
3. Verify setup function signature is correct

### Resources Not Created

**Symptom:** Queues or exchanges created in setup don't appear.

**Solutions:**

1. Ensure setup function is async and properly awaits operations
2. Check RabbitMQ permissions for creating resources
3. Verify resource names don't conflict with contract definitions

### Prefetch Not Working

**Symptom:** Messages not distributed as expected.

**Solutions:**

1. Prefetch only affects consumers, not publishers
2. Use `@amqp-contract/worker` for consuming with prefetch support
3. Ensure prefetch is set before consuming starts

## Related Documentation

- [Connection Sharing Guide](/guide/connection-sharing) - Managing shared connections
- [Client Usage Guide](/guide/client-usage) - Using the typed client
- [Worker Usage Guide](/guide/worker-usage) - Building message consumers
- [RabbitMQ Channels Documentation](https://www.rabbitmq.com/channels.html)
