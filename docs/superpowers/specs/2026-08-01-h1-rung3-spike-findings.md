# H1 rung 3 — `basic.return` correlation spike

**Date:** 2026-08-01
**Status:** research complete — decision recorded
**Question:** can a returned (unroutable) message be reliably matched back to the
specific `publish()` call that produced it, through `amqp-connection-manager`?

**Answer: yes.** The mechanism is reachable, and correlation by a per-publish
header is empirically exact under concurrency, forced reconnects, and a
TCP-level split between the return frame and the confirm frame.

**Recommendation: implement rung 3.** Scope, cost, and the parts it provably
does _not_ cover are in [Recommendation](#recommendation).

Everything below was measured against a real RabbitMQ 4.2.1 broker
(`rabbitmq:4.2.1-management-alpine`) with `amqp-connection-manager@5.0.0` and
`amqplib@2.0.1` — the versions this repo pins. Nothing here is inferred from
documentation.

---

## 1. Is `basic.return` reachable?

**Yes — on the raw amqplib channel handed to the `setup` callback. Not on the
`ChannelWrapper`.**

`ChannelWrapper` (v5.0.0) emits exactly three events. Its source contains six
`emit(` call sites and they are all one of:

| event     | source line (`dist/esm/ChannelWrapper.js`) |
| --------- | ------------------------------------------ |
| `error`   | 267, 285, 492, 537                         |
| `connect` | 282                                        |
| `close`   | 344                                        |

There is no occurrence of the string `return` as an event name, and none of
`mandatory`, anywhere in the package. A listener attached with
`wrapper.on("return", …)` never fires — confirmed empirically alongside a raw
listener that _did_ fire for the same message.

The raw channel **is** reachable. `_onConnect` calls each registered setup
function with the live channel (`pb.call(setupFn, this, channel)`, line 262),
and that object is an `amqplib` `ConfirmChannel`, which is an `EventEmitter`:

```js
conn.createChannel({
  confirm: true,
  setup: async (ch) => {
    // ch.constructor.name === "ConfirmChannel"
    ch.on("return", (msg) => {
      /* fires */
    });
  },
});
```

Two properties of this attachment point matter and both were verified:

- **It is re-run on every reconnect.** Each reconnect creates a _new_ channel
  object and re-runs every setup function against it, so the listener is
  re-attached exactly once per channel incarnation — no stacking, no leak.
  (Observed: `setup incarnation 1` → forced close → `setup incarnation 2`, with
  returns arriving on both.)
- **It is per-channel, so returns do not leak between clients.** This codebase
  pools _connections_ (`ConnectionManagerSingleton`) but gives every
  `AmqpClient` its own `ChannelWrapper`. Two wrappers on one pooled connection,
  each publishing one unroutable message: channel A saw only A's return, channel
  B only B's.

### This is already reachable from userland today

`AmqpClient.addSetup((channel) => …)` is public and hands out the same raw
channel, and `AmqpClient.publish` forwards `options` straight through. Verified
against the built `packages/core/dist`:

```js
client.addSetup((ch) =>
  ch.on("return", (m) => {
    /* … */
  }),
);
await client.publish({ exchange, routingKey }, payload, {
  mandatory: true,
  headers: { "x-ac-pid": id },
});
// -> { isOk: true, unroutable: { code: 312, text: "NO_ROUTE", ex, rk } }
```

Rung 3 is therefore not a capability the library is missing — it is plumbing
and a modeled error the library would own on the user's behalf.

---

## 2. What the returned message carries

A returned message is the original message, echoed back with a delivery header
describing why. Full dump of a real return:

```json
{
  "fields": {
    "replyCode": 312,
    "replyText": "NO_ROUTE",
    "exchange": "spike.direct",
    "routingKey": "no.such.key"
  },
  "properties": {
    "contentType": "application/json",
    "headers": { "x-amqp-contract-publish-id": "pid-1" },
    "correlationId": "corr-id-1",
    "messageId": "msg-id-1",
    "appId": "spike"
  },
  "content": "{\"hello\":\"world\"}"
}
```

Every AMQP basic property survives, **including the full user headers table**
and the exact body bytes. `fields.exchange` / `fields.routingKey` are the
original publish target. `replyCode` is `312 NO_ROUTE`.

For `sendToQueue` to a nonexistent queue, the return arrives with
`fields.exchange === ""` and `fields.routingKey === "<queue name>"`.

---

## 3. Ordering: the return always precedes the confirm

RabbitMQ sends `basic.return` before `basic.ack` for the same message, amqplib
dispatches frames in socket order, and `amqp-connection-manager` resolves the
publish promise from the confirm callback — so the `'return'` event is observed
strictly before the promise settles.

Measured, never violated:

| run                  | publishes | unroutable | return-after-its-own-confirm |
| -------------------- | --------- | ---------- | ---------------------------- |
| interleaved N=200    | 200       | 100        | **0**                        |
| interleaved N=2000   | 2000      | 1000       | **0**                        |
| burst N=50000 + kill | 50000     | 50000      | **0**                        |
| paced 7620 + kill    | 7620      | 7620       | **0**                        |

The single most important derived number, from the 50 000-publish burst with a
forced reconnect in the middle:

```
"resolvedOk": 47353,
"SILENT_LOSS_okButNoReturn": 0
```

**Every publish that resolved `Ok` and was in fact unroutable had already had
its return observed by the time the promise settled.** Zero exceptions.

The return is _not_ the immediately preceding event, though — under load, up to
96 unrelated events interleaved between a return and its own confirm. So "the
last return I saw belongs to this publish" is wrong; explicit correlation is
required.

---

## 4. Correlation mechanism

### Recommended: a per-publish header

Stamp `x-amqp-contract-publish-id` on every publish, register a pending entry
under that id, let the `'return'` handler flip a flag on it, and read the flag
when the publish promise settles.

```
publish(target, payload, opts):
  id = nextId()                       // channel-scoped monotonic counter
  entry = { unroutable: undefined }
  pending.set(id, entry)
  try:
    await wrapper.publish(ex, rk, body, { ...opts, mandatory: true,
                                          headers: { ...opts.headers, [H]: id } })
    // the return, if any, has already been observed (§3)
    return entry.unroutable ? Err(new UnroutableMessageError(...)) : Ok()
  finally:
    pending.delete(id)                // no orphan is possible: see §5
```

Evidence, 2000 concurrent publishes with 1000 unroutable interleaved among 1000
routable ones:

```json
{
  "publishesConfirmed": 2000,
  "returnsSeen": 1000,
  "expectedUnroutable": 1000,
  "missingReturns": 0,
  "spuriousReturns": 0,
  "returnCarriedUndefinedPid": false,
  "orderViolations_returnAfterItsOwnConfirm": 0,
  "allReturnsUnique": true
}
```

### Rejected: header-free FIFO correlation

Returns _do_ arrive in publish order — verified exactly, 1000 publishes across
7 routing keys, `GLOBAL_FIFO_matches_publish_order: true`. That makes a
zero-wire-overhead design look possible: keep a FIFO of pending publishes per
`(exchange, routingKey)` and pop its head on each return.

It is unsound, and unsound _specifically in the scenario rung 3 exists for_.
FIFO correlation assumes routability for a key is constant across the in-flight
window; a binding deleted mid-flight breaks that assumption, and there is no
way to tell which of the pending publishes for that key were the routable ones.

Measured: 6000 paced publishes to one key, binding deleted at #2000.

```json
{
  "actuallyUnroutable_byHeader": 4179,
  "firstUnroutableIndex_truth": 1821,
  "firstBlamedIndex_fifo": 1600,
  "MISATTRIBUTED_BY_FIFO": 4155,
  "fifoIsSound": false
}
```

**99.4 % of returns were attributed to the wrong publish.** Both a false
positive (a delivered message reported unroutable) and a false negative (a lost
message reported `Ok`) on nearly every one. The header is not optional.

### Rejected: reusing `messageId` / `correlationId`

Both survive the return intact, but both are user-owned AMQP properties with
established meanings — `correlationId` is already load-bearing for RPC
(`client.ts:722`). A namespaced header cannot collide.

---

## 5. Reconnect behavior

Forced via the management API, `DELETE /api/connections/{name}` with basic auth
(`guest:guest`), the port available to integration tests as
`__TESTCONTAINERS_RABBITMQ_PORT_15672__`. Note the management DB takes ~1 s to
register a new connection — a test that kills too early lists zero connections
and silently no-ops.

### Can a publish be left permanently unresolved? No.

Across every run — 7620 paced publishes spanning a forced reconnect, and 50 000
burst publishes with ~40 000 still queued when the connection died — the count
of publishes that never settled was **0**. Max observed settle latency was
2063 ms (the reconnect gap plus the requeued backlog).

The 50 000-burst reconnect split as:

```json
{
  "settled": 50000,
  "PERMANENTLY_UNSETTLED": 0,
  "resolvedOk": 47353,
  "rejected": 2647,
  "rejectReasons": ["channel closed"],
  "returnsPerChannelIncarnation": { "1": 7400, "2": 39953 },
  "PIDS_WITH_MULTIPLE_RETURNS": 0,
  "SILENT_LOSS_okButNoReturn": 0
}
```

- Messages **already sent but unconfirmed** when the connection dropped
  (2647 of them) had their confirm callbacks invoked with an error and
  `_messageRejected` rejected them outright with `"channel closed"` — because
  `this._channel` was still set at that instant, the requeue branch
  (`ChannelWrapper.js:368`) was not taken. The caller sees a `Defect`.
- Messages **still queued and never sent** (39 953) were published for the first
  time on the new channel and returned there.
- **No message produced two returns.** The republish-with-the-same-headers
  hazard did not materialise in any run.

### The worst window: return delivered, confirm never

Tested directly with a TCP proxy that parses the broker→client frame stream,
forwards bytes up to and including the returned message's body frame, and then
destroys the socket — so the client observes the return and can never receive
the ack.

```
[11ms] >>> forwarded the basic.return, now destroying the TCP connection
[11ms] RETURN pid=SPLIT-1 incarnation=1
[12ms] DISCONNECT: Unexpected close
[12ms] publish -> REJECTED: channel closed after 2ms
```

```json
{ "publishOutcome": "REJECTED: channel closed", "DUPLICATE_RETURN_AFTER_RECONNECT": false }
```

The publish rejects in 2 ms. It does not hang, and the message is not
republished. The pending entry is removed on the rejection path, so **an orphan
correlation entry is not reachable** — every registered entry is deleted when
its promise settles, and every promise settles.

An implementation may optionally report `UnroutableMessageError` rather than the
generic channel-closed `Defect` here, since the return _was_ observed. That is
strictly more information; it is not required for correctness.

### The one residual correlation hazard

If a message is requeued rather than rejected (the `!this._channel` branch), it
is republished with the _same_ headers and therefore the same publish id. The
design above tolerates that — the second return simply re-sets the same boolean
on a still-registered entry. The only misreport it can produce is: attempt 1
unroutable, connection drops, binding restored during the gap, attempt 2
routable → reported unroutable. That requires the topology to be repaired inside
the reconnect window, and it fails in the safe direction (a spurious error on a
message that got through, which at-least-once delivery already permits callers
to handle).

---

## 6. Per-publish cost

**Wire.** An AMQP field-table entry costs `1 + len(name) + 1 + 4 + len(value)`
bytes.

| scheme                                             | bytes/message |
| -------------------------------------------------- | ------------- |
| `x-amqp-contract-publish-id` + UUID                | 68            |
| `x-ac-pid` + base-36 counter (up to 60 M messages) | **19**        |

Plus a 4-byte field-table length prefix on messages that had no headers at all.
The id only needs to be unique within a channel's in-flight window, so a
channel-scoped monotonic counter is sufficient and 19 bytes is the number to
budget. On a 60-byte JSON body that is ~30 % overhead; on a realistic 500-byte
event, ~4 %.

**Latency.** Sequential publish-and-await, 3000 iterations after 300 warmup:

| variant          | p50 (ms) | p95    | p99    | mean   |
| ---------------- | -------- | ------ | ------ | ------ |
| plain            | 0.1390   | 0.1582 | 0.1801 | 0.1393 |
| mandatory + corr | 0.1374   | 0.1540 | 0.1797 | 0.1398 |

**+0.4 % mean, inside the noise floor.** For any workload that awaits its
publishes, the cost is not measurable.

**Throughput,** 20 000 publishes fired without backpressure:

| variant          | msg/s  | vs plain |
| ---------------- | ------ | -------- |
| plain            | 97 653 | —        |
| mandatory only   | 88 560 | −9.3 %   |
| mandatory + corr | 85 979 | −12.0 %  |

Most of that is `mandatory` itself (the broker doing routing bookkeeping and
emitting returns for 100 % of these publishes), not the correlation. The
correlation adds 2.7 points on top.

**Bookkeeping.** One `Map` entry per in-flight publish, deleted on settle;
`pendingMapLeftover: 0` in every benchmark. Bounded by concurrency, not by
throughput.

**Visibility.** The header is delivered to consumers — it appears in
`properties.headers` on the receiving side and is not stripped. It does not break
headers-exchange routing (verified with `x-match: all`), but it is permanently
part of every message this library ever publishes, including messages consumed
by systems that are not using this library.

---

## 7. What rung 3 provably does _not_ cover

These are measured negative results, not caveats-by-analogy.

1. **RPC replies cannot be covered.** RabbitMQ ignores `mandatory` on
   `amq.rabbitmq.reply-to`. With the requester's connection fully closed, a
   `mandatory` reply publish to its direct-reply-to pseudo-queue produced
   **zero returns** — the reply is silently dropped. `worker.ts:964` is
   therefore out of reach; a dead RPC requester still surfaces only as the
   caller's `RpcTimeoutError`.
2. **An alternate exchange suppresses the return, correctly.** An exchange
   declared with `alternate-exchange` routes its otherwise-unroutable messages
   to the AE; the message lands in the AE's queue and **no return is issued**.
   That is right — the message _was_ routed — but it means "no return" means
   "routed somewhere", not "routed to the queue you intended".
3. **`Ok` still does not mean "processed".** A return only fires when the
   exchange matches _zero_ queues. A message dropped by `x-max-length`
   overflow, expired by a queue TTL, or sitting in a queue nobody consumes is
   routed, confirmed, and returns nothing.
4. **The reconnect window is best-effort.** 2647 of 50 000 publishes in the
   burst test rejected with `"channel closed"` — their routability is unknown,
   as it is today.

There is also a framing correction worth recording: **"topology never set up in
this environment" is already impossible.** `setupAmqpTopology` runs inside the
channel `setup` callback (`amqp-client.ts:235`) and asserts every exchange,
queue, and binding in the contract on _every_ connect and reconnect. The
library creates its own topology. What rung 3 actually guards is narrower than
the framing suggests:

| scenario                                                                                   | covered by                       | rung 3 adds                 |
| ------------------------------------------------------------------------------------------ | -------------------------------- | --------------------------- |
| mistyped routing key inside one contract                                                   | rung 1 (types) + rung 2 (define) | nothing                     |
| topology missing in a fresh environment                                                    | auto-setup on connect            | nothing                     |
| contract-declared binding deleted by an operator                                           | self-heals at the next reconnect | detection during the window |
| **`externalConsumers: true` publisher whose binding never existed, or was decommissioned** | **nothing**                      | **the only check there is** |

---

## Recommendation

**Implement rung 3.**

The single strongest piece of evidence: across 50 000 publishes spanning a
broker-forced reconnect, `SILENT_LOSS_okButNoReturn` was **0** and
`PIDS_WITH_MULTIPLE_RETURNS` was **0** — header-correlated detection produced
neither a false negative nor a false positive, including when a TCP-level cut
delivered the return and destroyed the connection before the confirm could
arrive (that case rejects in 2 ms rather than hanging or double-reporting).

The decisive _product_ argument is the last row of the table above.
`externalConsumers: true` is the escape hatch rungs 1 and 2 hand to anyone whose
consumer lives in another service, and it is exactly the population most likely
to have drifted: another team's deploy, another repo's contract, a queue
decommissioned without telling the publisher. For those publishers there is
currently **no check at any level** — define-time, connect-time, or runtime —
and the failure mode is total, permanent, silent message loss. Rung 3 is the
only thing that can catch it, and it catches it on the first message.

### Proposed API shape

Client-level, opt-out-able, because the cost is not free for high-throughput
fire-and-forget publishers and because it widens a public error union:

```ts
TypedAmqpClient.create({
  contract,
  urls,
  // "error"  — publish with mandatory; an unroutable message becomes Err
  // "ignore" — today's behaviour; no mandatory flag, no header, no cost
  onUnroutable: "error", // default in the next major; "ignore" behind a flag before that
});
```

### Signature changes required

| location                              | from                                        | to                                                 |
| ------------------------------------- | ------------------------------------------- | -------------------------------------------------- |
| `AmqpClient.publish`                  | `AsyncResult<void, never>`                  | `AsyncResult<void, UnroutableMessageError>`        |
| `AmqpClient.sendToQueue`              | `AsyncResult<void, never>`                  | `AsyncResult<void, UnroutableMessageError>`        |
| `PublishError` (`interceptors.ts:12`) | `MessageValidationError`                    | `MessageValidationError \| UnroutableMessageError` |
| `TypedAmqpClient.publish`             | `AsyncResult<void, MessageValidationError>` | `AsyncResult<void, PublishError>`                  |

This is a **breaking change**: every user `errCases` matcher that handles only
`P.tag("@amqp-contract/MessageValidationError")` without a `P._` catch-all stops
compiling. It needs a major bump, a changeset, and an upgrade-guide entry.

Additional implementation notes for whoever picks this up:

- Register the `'return'` listener inside `AmqpClient`'s `defaultSetup`
  (`amqp-client.ts:235`), before the user-setup wrapper, so it survives every
  reconnect and cannot be displaced by a user-supplied `channelOptions.setup`.
- The publish id must be **channel-scoped** and reset per incarnation; reuse the
  existing `channelEpoch` counter (`amqp-client.ts:206`) as its prefix rather
  than inventing a second one.
- Merge the header into `options.headers`; never replace the table.
- `retry.ts:310` republishes to the _original_ exchange or, for classic-queue
  immediate requeue, to the default exchange. Turning `mandatory` on there
  converts a silently-lost retry into a modeled error, which is desirable, but
  it changes the retry pipeline's error surface — see load-bearing invariants
  1, 3 and 14 in `CLAUDE.md`. Cover it explicitly.
- Do **not** enable `mandatory` on the RPC reply path (`worker.ts:964`); per §7
  it is inert there and only costs bytes.
- Guarding test to add to the invariant list: an unroutable publish on a
  contract with `externalConsumers: true` returns `Err(UnroutableMessageError)`
  while a routable one on the same channel returns `Ok`, both published
  concurrently.

### If this is deferred instead

The residual risk to document is precise: **for any publisher declared with
`externalConsumers: true`, a missing or deleted broker-side binding causes
total, permanent, silent message loss, and `publish()` reports success.** The
mitigation a user should apply is to assert the binding themselves at startup —
`channel.checkQueue(name)` plus a management-API binding check — or to attach
their own return listener via the already-public
`AmqpClient.addSetup((ch) => ch.on("return", …))`, which §1 shows works today.

---

## Reproduction

All experiments were throwaway scripts run against a standalone
`rabbitmq:4.2.1-management-alpine` container on ports 45672/45673, and were
deleted after the run. The ten of them covered: reachability and message dump;
correlation under concurrency at N=200/2000; a paced reconnect with management-API
force-close; a 50 000-message in-flight burst kill; alternate-exchange, headers-exchange,
and `sendToQueue` edge cases; throughput and sequential-latency benchmarks; channel
isolation across a pooled connection; a frame-parsing TCP proxy that splits the
return from the confirm; FIFO-correlation soundness under a mid-flight unbind;
and an end-to-end check through the built `AmqpClient`. Raw output is recorded in
`.superpowers/sdd/2026-08-01-h1-unroutable-publish/task-6-report.md`.
