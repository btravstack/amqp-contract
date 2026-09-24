---
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
---

A broker-side publish failure is now a modeled `Err` — the new `PublishError` —
where it used to arrive as a `Defect` carrying a `TechnicalError`.

`AmqpClient.publish` / `sendToQueue`, `TypedAmqpClient.publish` and
`TypedAmqpClient.call` report `PublishError` on the `E` channel, with a
`reason` discriminant naming what core observed:

- `"timeout"` — the message sat buffered past `publishTimeoutMs`;
- `"nacked"` — the broker refused it (`basic.nack`);
- `"channel-closed"` — the channel closed before the message was confirmed.

A full write buffer is **not** a failure: amqp-connection-manager resolves a
confirm-channel publish with `false` only after the broker confirmed the
message, so `publish` answers `Ok` (and logs the backpressure at `debug`)
instead of inviting a duplicate republish. The worker acks the original after
a confirmed retry publish, and acks an RPC request after a confirmed reply, in
that case too.

A broker that is down or overloaded is an operational condition a publisher is
expected to handle, not a bug. Genuine bugs (an unencodable payload, a
rejection core cannot classify) stay on the defect channel.

Migration: every exhaustive matcher over `publish()` / `call()` errors gains a
case — `P.tag(PublishError.tag)` — and `.get()` on core's `AmqpClient.publish`
/ `sendToQueue` result no longer compiles (its `E` is no longer `never`; use
`.getOrThrow()` or handle the error). The client's interceptor error union formerly exported as
`PublishError` (then just `MessageValidationError`) is renamed
`ClientPublishError` (`MessageValidationError | PublishError`); `CallError`
gains `PublishError`. `PublishError` and `PublishFailureReason` are exported
from core and re-exported by the client.
