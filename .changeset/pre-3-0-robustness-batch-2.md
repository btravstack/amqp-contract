---
"@amqp-contract/contract": patch
"@amqp-contract/core": patch
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

Second robustness pass from the pre-3.0 audit — correctness fixes, resource-safety guards, and internal idiom alignment. All additive or bug-fix; no further breaking changes beyond those already listed in the other pre-3.0 changesets.

**Correctness fixes**

- **A channel `error` event no longer crashes the process.** amqp-connection-manager's `ChannelWrapper` emits plain `'error'` events for conditions it recovers from by reconnecting (topology setup failure on connect/reconnect, publish-worker faults, consumer re-establishment). With no listener attached, Node escalated the emit to `ERR_UNHANDLED_ERROR` and the process died. `AmqpClient` now always attaches a listener that degrades the event to `logger.error`; the typed client/worker thread their logger down, and user `on('error', …)` listeners still fire. (`AmqpClientOptions` gains an optional `logger`.)
- **A single un-composed middleware now merges its context over the `createContext` seed** instead of replacing it, so `middleware: mw` and `middleware: [mw]` behave identically (previously the bare form silently dropped every seed field).
- **`client.publish(...)` / `client.call(...)` with a name the contract does not declare** now resolve to a `Defect` (a `TechnicalError` naming the culprit and the declared names) instead of throwing a raw `TypeError`, honoring the client's "nothing in the public API throws" contract.
- **Reconnect-safe settles.** Delivery tags are per-channel, but a buffered retry publish or RPC reply can confirm on a _new_ channel; the follow-up ack/nack then targeted a foreign tag (channel-closing 406, or settling an unrelated delivery whose own DLQ nack was lost). `AmqpClient` now tracks a channel epoch (`currentChannelEpoch`) and skips a settle stamped with a stale epoch — the broker's redelivery preserves at-least-once.
- **The RPC timeout stays armed through async reply validation**, so a slow or never-settling response validator can no longer leave the caller hanging past `timeoutMs`.

**Resource-safety guards**

- **`publishTimeoutMs`** (client and worker `create` options): bounds how long a publish may sit buffered during a broker outage before its promise settles as a `Defect`, instead of buffering unboundedly forever.
- **`maxDecompressedBytes`** (worker `create` option, default 64 MiB): caps inbound decompression so a decompression bomb follows the poison-message DLQ path instead of exhausting memory.
- **Connection-pool keys distinguish function-valued options** (`findServers`, amqplib `credentials`), which `JSON.stringify` dropped — two clients differing only in a callback no longer collapse onto one shared connection.
- **`TypedAmqpWorker.create` fails fast on a handler key that names no contract entry** (a stale key from a spread, or a missed rename), before any connection is acquired, instead of silently leaving that message class unprocessed.
- **A poison message nacked on a queue with no DLX now logs a "will be lost" warning**, matching the retry path's existing diagnostic.

**Internal idiom alignment (no observable behavior change)**

- Adopt `@unthrown/standard-schema`'s `fromSchemaAsync` at the six hand-rolled Standard Schema validation boundaries in the client and worker.
- Hoist the `technicalDefect` defect-mint seam into `@amqp-contract/core` (deleting the three copies), and give `safeJsonParse` the full `(cause, defect)` qualify signature so callers no longer model-then-defect.
