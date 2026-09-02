---
title: Compress messages - amqp-contract
description: Compress large payloads with gzip or deflate at publish time, compress conditionally by size, and understand what happens on the consuming side.
---

# Compress messages

Compression is a per-publish decision, so you can compress the payloads that benefit and leave the rest alone.

## Compress a payload

```typescript
await client.publish("orderCreated", order, { compression: "gzip" });
```

`compression` accepts `"gzip"` or `"deflate"`. The client compresses the serialized payload and sets `contentEncoding` accordingly.

## Consume a compressed message

Nothing to do. The worker reads `contentEncoding`, decompresses, then validates and dispatches as usual:

```typescript
processOrder: (_, { payload }) => {
  console.log(payload.items); // already decompressed
  return OkAsync(undefined);
},
```

Because decompression happens before validation, a message that cannot be decompressed never reaches your handler — it is dead-lettered, like any other unparseable payload, without retrying.

## Compress only when it is worth it

Compression has a fixed cost and a payload-dependent benefit, so a size threshold is usually better than compressing everything:

```typescript
const COMPRESSION_THRESHOLD_BYTES = 1024;

const publishOrder = (order: Order) => {
  const size = JSON.stringify(order).length;
  return client.publish("orderCreated", order, {
    compression: size > COMPRESSION_THRESHOLD_BYTES ? "gzip" : undefined,
  });
};
```

Below roughly a kilobyte, gzip's header overhead often makes the message _larger_, and you have paid CPU for the privilege.

## Compress every message from a client

```typescript
const client = await TypedAmqpClient.create({
  contract,
  urls: ["amqp://localhost"],
  defaultPublishOptions: { compression: "gzip" },
}).get();
```

Per-call options override it, so `{ compression: undefined }` opts a single publish out.

## Choose an algorithm

**gzip** is the general-purpose choice, widely understood by tooling, and what you should reach for by default.

**deflate** produces slightly smaller output with slightly less overhead — it is gzip without the header and checksum. Prefer it only when you have measured a difference that matters.

Neither helps on data that is already compressed. Images, video, and anything base64-encoded from a compressed source will not shrink, so compressing them is pure cost.

## Decide whether to compress at all

Worth it for large, repetitive JSON — arrays of similar objects, long text, verbose nested structures — where ratios of 5–10x are common, and where bandwidth or broker memory is the constraint.

Not worth it for small messages, already-compressed data, or on CPU-constrained workers where the compression cost competes with the actual work.

The honest way to decide is to measure a real payload:

```typescript
import { gzipSync } from "node:zlib";

const raw = Buffer.from(JSON.stringify(sampleOrder));
console.log(raw.length, gzipSync(raw).length);
```

## Where next

- [Tune performance](/how-to/tune-performance) — where compression fits among the other levers.
- [Publish messages](/how-to/publish-messages#set-amqp-properties) — the rest of `PublishOptions`.
