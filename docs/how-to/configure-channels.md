---
title: Configure channels - amqp-contract
description: Customize channel behaviour on the low-level AmqpClient — serialization, publisher confirms, channel names and custom setup logic.
---

# Configure channels

Channel options live on the low-level `AmqpClient` from `@amqp-contract/core`. `TypedAmqpClient` and `TypedAmqpWorker` cover almost everything, so reach for this only when you need channel behaviour they do not expose.

## Know the defaults

`AmqpClient` creates channels with JSON serialization and publisher confirms both enabled:

```typescript
import { AmqpClient } from "@amqp-contract/core";

const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
});
```

Publisher confirms are on because without them `publish` cannot tell you whether the broker accepted the message — it would report success as soon as the bytes were written to the socket.

## Customize a channel

```typescript
import type { Channel } from "amqplib";

const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  channelOptions: {
    json: true,
    confirm: true,
    name: "orders-publisher",
    setup: async (channel: Channel) => {
      await channel.prefetch(10);
    },
  },
});
```

`name` appears in RabbitMQ's management UI and in logs — worth setting when a process opens several channels, because otherwise they are indistinguishable when you go looking for the one that is misbehaving.

## Add resources the contract does not describe

The `setup` function runs after the contract's topology is established, so everything in the contract already exists when it fires:

```typescript
channelOptions: {
  setup: async (channel: Channel) => {
    await channel.prefetch(10);
    await channel.assertQueue("legacy-integration-queue");
    await channel.assertExchange("legacy-exchange", "topic");
  },
},
```

This is the escape hatch for integrating with topology you do not own — a legacy queue another team declares, for instance. Anything you can express in the contract belongs in the contract, where it is typed and documented.

## Keep setup idempotent

`setup` runs on **every** reconnection, not once at startup. Anything with side effects beyond declaring topology will run repeatedly:

```typescript
setup: async (channel: Channel) => {
  await channel.prefetch(10); // fine — idempotent
  await channel.assertQueue("queue"); // fine — idempotent
  await incrementDeploymentCounter(); // wrong — fires on every reconnect
},
```

Declaring exchanges and queues is idempotent by design, so topology work is safe. Bookkeeping is not.

## Use the callback form

A callback-style `setup` is supported for compatibility:

```typescript
setup: (channel: Channel, callback: (error?: Error) => void) => {
  channel
    .prefetch(10)
    .then(() => callback())
    .catch((err) => callback(err));
},
```

Prefer the promise form.

## Understand what you cannot do

**You cannot suppress contract topology.** `setup` runs after it, so contract exchanges, queues and bindings are already declared. There is no hook to skip them — that is what makes the contract authoritative.

**Channel options are per channel.** Connection-level settings are separate:

```typescript
const client = new AmqpClient(contract, {
  urls: ["amqp://localhost"],
  connectionOptions: { heartbeatIntervalInSeconds: 30 }, // connection
  channelOptions: { confirm: false }, // channel
});
```

Note that `connectionOptions` participates in the connection cache key — see [share connections](/how-to/share-connections).

## Diagnose setup that seems not to run

**Nothing happens.** Confirm the channel is actually being created — channels are created lazily, so a client that never publishes never runs `setup`.

**Resources are missing after a reconnect.** Something in `setup` threw. A rejected `setup` aborts the rest of it, so a failure on the first line silently loses everything after. Log inside it.

**`prefetch` appears to have no effect.** Setting prefetch here applies to the channel, but the worker sets its own per-consumer prefetch, which wins. Use `defaultConsumerOptions` or the per-handler form instead — see [consume messages](/how-to/consume-messages#control-concurrency).

## Where next

- [Consume messages](/how-to/consume-messages#control-concurrency) — the supported way to set prefetch.
- [Share connections](/how-to/share-connections) — connection-level options.
- [Tune performance](/how-to/tune-performance) — publisher confirms and throughput.
