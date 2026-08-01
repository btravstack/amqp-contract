# H1 rung 3 — `basic.return` correlation spike

**Date:** 2026-08-01
**Status:** research complete — decision recorded
**Question:** can a returned (unroutable) message be reliably matched back to the
specific `publish()` call that produced it, through `amqp-connection-manager`?

**Answer: yes.** The mechanism is reachable, and correlation by a per-publish
header held under every configuration tested — including the one that exercises
both error oracles at once (mixed routable/unroutable traffic across a forced
reconnect, verdict sampled at promise-settle time).

**Recommendation: implement rung 3.** The decision rests on the _product_
argument in [Recommendation](#recommendation) — publishers declared
`externalConsumers: true` have zero coverage at any rung today and fail totally
and silently. The measurements below establish that the mechanism is sound
enough to build on; they are not themselves the reason to build it.

Everything here was measured against a real RabbitMQ 4.2.1 broker
(`rabbitmq:4.2.1-management-alpine`) with `amqp-connection-manager@5.0.0` and
`amqplib@2.0.1` — the versions this repo pins. Claims a given run did not
instrument are marked **not measured**, never assumed to be zero.

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

## 3. Ordering: the return precedes the confirm

RabbitMQ sends `basic.return` before `basic.ack` for the same message, amqplib
dispatches frames in socket order, and `amqp-connection-manager` resolves the
publish promise from the confirm callback — so the `'return'` event is observed
before the promise settles.

What each run actually instrumented:

| run                           | publishes  | mixed workload?         | reconnect? | return-after-its-own-confirm | return arriving _after_ its own settle |
| ----------------------------- | ---------- | ----------------------- | ---------- | ---------------------------- | -------------------------------------- |
| §4 interleaved N=200          | 200        | yes (50 % routable)     | no         | **0**                        | not measured                           |
| §4 interleaved N=2000         | 2000       | yes (50 % routable)     | no         | **0**                        | not measured                           |
| §5 paced 7620 + kill          | 7620       | no (100 % unroutable)   | yes        | not measured                 | not measured                           |
| §5 burst 50 000 + kill        | 50 000     | no (100 % unroutable)   | yes        | not measured                 | not measured                           |
| **§6 mixed 12 000 + kill ×4** | **48 000** | **yes (50 % routable)** | **yes**    | **0**                        | **0**                                  |

The two middle rows only ever produced an _end-of-run tally_ of "did a return
ever arrive for this publish id". That tally cannot distinguish "the return
arrived before the confirm" from "the return arrived 5 ms after it" — and the
proposed design reads the flag at settle time, so a late return is a silent
false negative in production while still scoring as "seen" at end of run. §6 is
the run that closes that gap.

Under load the return is _not_ the immediately preceding event: up to 96
unrelated events interleaved between a return and its own confirm at N=2000. So
"the last return I saw belongs to this publish" is wrong; explicit correlation
is required.

---

## 4. Correlation mechanism

### Recommended: a per-publish header

Stamp `x-amqp-contract-publish-id` on every publish, register a pending entry
under that id, let the `'return'` handler flip a flag on it, and read the flag
when the publish promise settles.

```
publish(target, payload, opts):
  id = nextId()                       // channel-scoped monotonic counter
  entry = { unroutable: false }
  pending.set(id, entry)
  try:
    await wrapper.publish(ex, rk, body, { ...opts, mandatory: true,
                                          headers: { ...opts.headers, [H]: id } })
    // read the flag HERE, at settle time — §6 measures that no return arrives later
    return entry.unroutable ? Err(new UnroutableMessageError(...)) : Ok()
  finally:
    pending.delete(id)                // no orphan is possible: see §5
```

Evidence, 2000 concurrent publishes with 1000 unroutable interleaved among 1000
routable ones (no reconnect — that combination is §6):

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

**99.4 % of returns were attributed to the wrong publish** — both false
positives and false negatives. The header is not optional.

### Rejected: reusing `messageId` / `correlationId`

Both survive the return intact, but both are user-owned AMQP properties with
established meanings — `correlationId` is already load-bearing for RPC
(`client.ts:722`). A namespaced header cannot collide.

---

## 5. Reconnect behavior

Forced via the management API, `DELETE /api/connections/{name}` with basic auth
(`guest:guest`), the port available to integration tests as
`__TESTCONTAINERS_RABBITMQ_PORT_15672__`. **The management DB takes 0.5–5 s to
register a new connection**; a `DELETE` issued before that silently no-ops
against an empty listing. Poll `/api/connections` until non-empty first.

### Can a publish be left permanently unresolved? No.

Across every run — 7620 paced publishes spanning a forced reconnect, 50 000
burst publishes with ~40 000 still queued when the connection died, and the four
§6 runs — the count of publishes that never settled was **0**.

Max settle latency, attributed precisely:

| run                    | max settle latency |
| ---------------------- | ------------------ |
| §5 paced 7620 + kill   | 1021 ms            |
| §5 burst 50 000 + kill | 2063 ms            |

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
  "SILENT_LOSS_okButNoReturn": 0,
  "maxLatencyMs": 2063
}
```

Note the scope limit on that run: **every publish in it was unroutable**, so
`PIDS_WITH_MULTIPLE_RETURNS: 0` and `SILENT_LOSS: 0` there are informative about
false _negatives_ only. There was no routable publish available to be falsely
blamed, so the run carries no evidence about false positives. §6 supplies that.

- Messages **already sent but unconfirmed** when the connection dropped
  (2647 of them) had their confirm callbacks invoked with an error and
  `_messageRejected` rejected them outright with `"channel closed"` — because
  `this._channel` was still set at that instant, the requeue branch
  (`ChannelWrapper.js:368`) was not taken. The caller sees a `Defect`.
- Messages **still queued and never sent** (39 953) were published for the first
  time on the new channel and returned there.

### The worst window: return delivered, confirm never

Tested with a TCP proxy that parses the broker→client frame stream, forwards
bytes up to and including the returned message's body frame, then destroys the
socket — so the client observes the return and can never receive the ack.

```
[11ms] >>> forwarded the basic.return, now destroying the TCP connection
[11ms] RETURN pid=SPLIT-1 incarnation=1
[12ms] publish -> REJECTED: channel closed after 2ms
DUPLICATE_RETURN_AFTER_RECONNECT: false
```

The publish rejects in 2 ms. It does not hang, and the message is not
republished. The pending entry is removed on the rejection path, so **an orphan
correlation entry is not reachable** — every registered entry is deleted when
its promise settles, and every promise settles.

An implementation may optionally report `UnroutableMessageError` rather than the
generic channel-closed `Defect` here, since the return _was_ observed. That is
strictly more information; it is not required for correctness.

### The duplicate-return hazard is UNTESTED, not disproven

If a message takes the requeue branch of `_messageRejected` rather than the
reject branch, it is republished with the _same_ headers and therefore the same
publish id, and can produce a second return. Every run reported
`PIDS_WITH_MULTIPLE_RETURNS: 0` — but that proves the hazard never _fired_, not
that the design survives it.

`_messageRejected` was instrumented directly to count which branch each
rejection takes. Two disturbance mechanisms were tried:

| disturbance                                    | reject branch | requeue branch |
| ---------------------------------------------- | ------------- | -------------- |
| connection-level force close (management API)  | 0             | 0              |
| channel-level 404 (exchange deleted mid-burst) | 300           | 0              |
| 50 000-message in-flight burst + force close   | 2647          | 0              |

**The requeue branch was never reached in any attempt.** On amqplib 2.0.1,
pending confirm callbacks are invoked with an error before
`amqp-connection-manager`'s channel-close handler clears `this._channel`, so the
`!this._channel` guard is false and the reject branch always wins. That may be
timing-dependent rather than structural.

An implementation must therefore be idempotent **by construction, not by
evidence**. The design above is: a second return simply re-sets the same boolean
on a still-registered entry. Its one misreport remains analytic — attempt 1
unroutable, connection drops, binding restored during the gap, attempt 2
routable → reported unroutable. That fails in the safe direction (a spurious
error on a message that got through, which at-least-once already permits), but
it has not been observed and should be covered by a unit test that fakes the
republish rather than by an integration test that cannot trigger it.

---

## 6. Mixed workload across a forced reconnect, verdict read at settle time

This is the configuration the other runs were missing: both error oracles active
at once, and the flag sampled at the exact instant the design would read it.

- **Ground truth** = routing key. The binding is re-asserted by `setup` on every
  reconnect, so routability is constant by construction for the whole run.
- **Verdict** = `entry.unroutable` read synchronously inside the promise's
  settle handler.
- **False negative** = truly unroutable, verdict said routable (silent loss).
- **False positive** = truly routable, verdict said unroutable.
- **Late return** = the flag flipped _after_ the settle-time snapshot — the
  silent-false-negative mode an end-of-run tally cannot see.
- Publishes rejected with `"channel closed"` have unknown routability and are
  excluded from both oracles.

Four runs, 12 000 publishes each (50 % routable), connection force-closed at a
different point in each:

| kill at batch | resolved Ok | truly unroutable | truly routable | rejected | FN    | FP    | late returns | order violations | dup returns |
| ------------- | ----------- | ---------------- | -------------- | -------- | ----- | ----- | ------------ | ---------------- | ----------- |
| 2             | 11 093      | 5586             | 5507           | 907      | 0     | 0     | 0            | 0                | 0           |
| 3             | 11 073      | 5537             | 5536           | 927      | 0     | 0     | 0            | 0                | 0           |
| 6             | 11 349      | 5750             | 5599           | 651      | 0     | 0     | 0            | 0                | 0           |
| 10            | 11 816      | 6000             | 5816           | 184      | 0     | 0     | 0            | 0                | 0           |
| **total**     | **45 331**  | **22 873**       | **22 458**     | **2669** | **0** | **0** | **0**        | **0**            | **0**       |

Representative raw output:

```json
{
  "publishes": 12000,
  "disconnects": 1,
  "channelIncarnations": 2,
  "settleOutcome": "ALL-SETTLED",
  "permanentlyUnsettled": 0,
  "resolvedOk": 11073,
  "rejected_routabilityUnknown": 927,
  "oracle": {
    "okAndTrulyUnroutable": 5537,
    "okAndTrulyRoutable": 5536,
    "FALSE_NEGATIVES_lostButReportedOk": 0,
    "FALSE_POSITIVES_deliveredButReportedUnroutable": 0
  },
  "LATE_RETURNS_arrivedAfterItsOwnSettle": 0,
  "returnAfterItsOwnConfirm_seqViolations": 0,
  "pidsWithMultipleReturns": 0
}
```

Each run allowed a 5-second grace period after the last settle before tallying,
so a late return had every opportunity to appear. None did.

**What this does and does not establish.** It establishes that across 45 331
successfully-confirmed publishes spanning four forced reconnects, with roughly
equal routable and unroutable traffic, the settle-time verdict was correct every
time. It does not establish a probabilistic bound — 0/45 331 on loopback with a
sub-millisecond RTT is consistent with a rare ordering violation on a
high-latency link — and it says nothing about brokers other than RabbitMQ 4.2.1
or clients other than amqplib 2.0.1.

---

## 7. Per-publish cost

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

**Latency**, sequential publish-and-await (each publish awaited before the next
is issued), 3000 iterations after 300 warmup, small two-field JSON body, single
channel, loopback broker, all publishes routable:

| variant          | p50 (ms) | p95    | p99    | mean   |
| ---------------- | -------- | ------ | ------ | ------ |
| plain            | 0.1390   | 0.1582 | 0.1801 | 0.1393 |
| mandatory + corr | 0.1374   | 0.1540 | 0.1797 | 0.1398 |

**+0.4 % mean, inside the noise floor.** For any workload that awaits its
publishes, the cost is not measurable.

**Throughput**, 20 000 publishes enqueued in one synchronous loop with no
backpressure (all in flight simultaneously), 60-byte JSON body, single channel,
loopback broker, all publishes routable so returns are not a factor:

| variant          | msg/s  | vs plain |
| ---------------- | ------ | -------- |
| plain            | 97 653 | —        |
| mandatory only   | 88 560 | −9.3 %   |
| mandatory + corr | 85 979 | −12.0 %  |

Most of the cost is `mandatory` itself, not the correlation; the correlation
adds 2.7 points on top. This is a worst-case shape — maximum pipelining, tiny
bodies, zero network latency — chosen so the overhead is visible at all.

**Bookkeeping.** One `Map` entry per in-flight publish, deleted on settle;
`pendingMapLeftover: 0` in every benchmark. Bounded by concurrency, not by
throughput.

**Visibility.** The header is delivered to consumers — it appears in
`properties.headers` on the receiving side and is not stripped. It does not break
headers-exchange routing (verified with `x-match: all`), but it is permanently
part of every message this library publishes, including messages consumed by
systems that are not using this library.

---

## 8. What rung 3 provably does _not_ cover

Measured negative results, not caveats-by-analogy.

1. **`amq.rabbitmq.reply-to` cannot be covered.** RabbitMQ ignores `mandatory`
   on the direct reply-to pseudo-queue. With the requester's connection fully
   closed, a `mandatory` reply publish produced **zero returns** — the reply is
   silently dropped. Since `client.ts` sets `replyTo: DIRECT_REPLY_TO` for every
   RPC it issues, `worker.ts:964` is out of reach _for this library's own RPC
   clients_. The narrower claim is the accurate one: this is a property of
   direct reply-to, not of reply publishing in general — a third-party requester
   that set `replyTo` to a real declared queue would get a normal return if that
   queue were deleted, so `mandatory` is not universally inert on that code path.
2. **An alternate exchange suppresses the return, correctly.** An exchange
   declared with `alternate-exchange` routes its otherwise-unroutable messages
   to the AE; the message lands in the AE's queue and **no return is issued**.
   That is right — the message _was_ routed — but "no return" then means
   "routed somewhere", not "routed to the queue you intended".
3. **`Ok` still does not mean "processed".** A return only fires when the
   exchange matches _zero_ queues. A message dropped by `x-max-length` overflow,
   expired by a queue TTL, or sitting in a queue nobody consumes is routed,
   confirmed, and returns nothing.
4. **The reconnect window is best-effort.** 2669 of 48 000 publishes in the §6
   runs (and 2647 of 50 000 in §5) rejected with `"channel closed"` — their
   routability is unknown, as it is today.

There is also a framing correction worth recording: **"topology never set up in
this environment" is already impossible.** `setupAmqpTopology` runs inside the
channel `setup` callback (`amqp-client.ts:235`) and asserts every exchange,
queue, and binding in the contract on _every_ connect and reconnect. What rung 3
actually guards is narrower than the original framing suggests:

| scenario                                                                                   | covered by                       | rung 3 adds                 |
| ------------------------------------------------------------------------------------------ | -------------------------------- | --------------------------- |
| mistyped routing key inside one contract                                                   | rung 1 (types) + rung 2 (define) | nothing                     |
| topology missing in a fresh environment                                                    | auto-setup on connect            | nothing                     |
| contract-declared binding deleted by an operator                                           | self-heals at the next reconnect | detection during the window |
| **`externalConsumers: true` publisher whose binding never existed, or was decommissioned** | **nothing**                      | **the only check there is** |

---

## Recommendation

**Implement rung 3.**

The reason is the last row of the table above, not a zero-defect measurement
count. `externalConsumers: true` (`contract/src/types.ts:878`) is the escape
hatch rungs 1 and 2 hand to anyone whose consumer lives in another service, and
it is exactly the population most likely to have drifted: another team's deploy,
another repo's contract, a queue decommissioned without telling the publisher.
For those publishers there is **no check at any level today** — define-time,
connect-time, or runtime — and the failure mode is total, permanent, silent
message loss with `publish()` reporting success. Rung 3 is the only thing that
can catch it, and it catches it on the first message.

The measurements support that decision without carrying it. What they establish
is that the mechanism is sound enough to build on: 45 331 confirmed publishes of
mixed routable/unroutable traffic across four forced reconnects produced no
false negative, no false positive, and no return arriving after its own settle
(§6); a TCP-level cut delivering the return and destroying the connection before
the confirm rejects in 2 ms rather than hanging or double-reporting (§5); and
header-free correlation is definitively ruled out (§4). What they do **not**
establish: a probabilistic bound on a high-latency link, and anything at all
about the requeue-branch duplicate-return hazard, which no attempt could
trigger (§5).

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

### Signature changes and internal blast radius

**The type widens unconditionally.** `onUnroutable: "ignore"` changes runtime
behaviour but not the static type, so _every_ consumer of
`AmqpClient.publish`'s result must be updated regardless of the default — unless
the option is encoded in the type system (a generic, or a separate method),
which is more complex and is the alternative worth weighing during
implementation.

Public surface:

| location                              | from                                        | to                                                 |
| ------------------------------------- | ------------------------------------------- | -------------------------------------------------- |
| `AmqpClient.publish`                  | `AsyncResult<void, never>`                  | `AsyncResult<void, UnroutableMessageError>`        |
| `AmqpClient.sendToQueue`              | `AsyncResult<void, never>`                  | `AsyncResult<void, UnroutableMessageError>`        |
| `PublishError` (`interceptors.ts:12`) | `MessageValidationError`                    | `MessageValidationError \| UnroutableMessageError` |
| `TypedAmqpClient.publish`             | `AsyncResult<void, MessageValidationError>` | `AsyncResult<void, PublishError>`                  |

Breaking: every user `errCases` matcher handling only
`P.tag("@amqp-contract/MessageValidationError")` without a `P._` catch-all stops
compiling. Major bump, changeset, upgrade-guide entry.

Internal call sites that break — all four must be fixed in the same change:

| site                             | current shape                                                                                              | why it breaks / what it needs                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `worker.ts:964` `publishReply`   | `.publish(...).recoverDefect(cause => Err<HandlerError>(...))`, declared `AsyncResult<void, HandlerError>` | `recoverDefect` only collapses the defect channel, so the widened `E` survives and the result is `UnroutableMessageError \| HandlerError` — not assignable to `HandlerError`. Needs an explicit `mapErrCases` folding `UnroutableMessageError` into `NonRetryableError`, matching the documented "reply failure → DLQ" policy.                                                                |
| `retry.ts:310` `publishForRetry` | `.publish(...).map(ack).tapDefect(log)`, declared `AsyncResult<void, never>`                               | Declared `E = never` no longer holds. Semantically the `.map` short-circuit already skips the ack on `Err`, so **invariant 1 (retry publishes before ack) survives** — but `tapDefect` will not fire for an `Err`, so the failure would go unlogged. Needs a `tapErrCases` alongside it. Invariant 14's default-exchange republish is routable by construction, so no new failure mode there. |
| `client.ts:487`                  | contract publish, wrapped by interceptors                                                                  | The intended surface. `PublishError` widening flows through `chainInterceptors` and the `PublishInterceptor` type.                                                                                                                                                                                                                                                                            |
| `client.ts:728` `publishRequest` | declared `AsyncResult<void, never>`, feeds `call()`                                                        | Declared `E = never` no longer holds, and `call()`'s error union gains `UnroutableMessageError`. Behaviourally an improvement — a missing RPC queue fails fast instead of waiting out `RpcTimeoutError` — but it is a second breaking union widening, on a separate public method.                                                                                                            |

Additional implementation notes:

- Register the `'return'` listener inside `AmqpClient`'s `defaultSetup`
  (`amqp-client.ts:235`), before the user-setup wrapper at line 257, so it
  survives every reconnect and cannot be displaced by a user-supplied
  `channelOptions.setup`.
- The publish id must be **channel-scoped** and reset per incarnation; reuse the
  existing `channelEpoch` counter (`amqp-client.ts:206`) as its prefix rather
  than inventing a second one.
- Merge the header into `options.headers`; never replace the table.
- Do **not** enable `mandatory` on the RPC reply path (`worker.ts:964`) — per §8
  it is inert for this library's own clients and only costs bytes. Fix the
  _type_ there; leave the behaviour alone.
- Guarding tests to add: (a) an unroutable publish on an
  `externalConsumers: true` contract returns `Err(UnroutableMessageError)` while
  a routable one on the same channel returns `Ok`, both published concurrently;
  (b) a **unit** test that fakes a republish of the same publish id, since §5
  shows the requeue branch cannot be reached from an integration test.

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

All experiments were throwaway `.mjs` scripts run against a standalone
`rabbitmq:4.2.1-management-alpine` container on ports 45672/45673, deleted after
the run. Fourteen experiments across fifteen files:

| script                          | covers                                                               |
| ------------------------------- | -------------------------------------------------------------------- |
| `01-reachability`               | §1 reachability, §2 message dump, §3 first ordering datapoint        |
| `02-concurrency`                | §4 header correlation at N=200 / N=2000                              |
| `03-reconnect`                  | §5 paced 7620 + management-API force close                           |
| `04-inflight-kill`              | §5 50 000-message in-flight burst kill                               |
| `05-alternatives`               | §4 FIFO ordering, §8 alternate exchange, `sendToQueue`, §7 wire cost |
| `06-overhead`                   | §7 throughput                                                        |
| `07-seq-latency`                | §7 sequential latency                                                |
| `08-channel-isolation`          | §1 per-channel isolation across a pooled connection                  |
| `09-direct-reply-to`            | §8 `amq.rabbitmq.reply-to`                                           |
| `10-proxy-split` (+`10b-debug`) | §5 frame-parsing TCP proxy splitting the return from the confirm     |
| `11-real-client`                | §1 end-to-end through the built `AmqpClient`                         |
| `12-fifo-unsound`               | §4 FIFO misattribution under a mid-flight unbind                     |
| `13-mixed-reconnect`            | §6 mixed workload + reconnect, settle-time verdict                   |
| `14-requeue-branch`             | §5 `_messageRejected` branch instrumentation                         |

Raw output for every run is recorded in
`.superpowers/sdd/2026-08-01-h1-unroutable-publish/task-6-report.md`.
