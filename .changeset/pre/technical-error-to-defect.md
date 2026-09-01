---
"@amqp-contract/client": patch
"@amqp-contract/core": patch
"@amqp-contract/worker": patch
---

Move `TechnicalError` out of the modeled error channel and into the **defect**
channel. Infrastructure/transport failures (connection, publish, consume,
cancel, close, compression/decompression, JSON parse, and thrown/rejected
schema validators) are unexpected, so they now surface as an unthrown `Defect`
(with a `TechnicalError` as the defect's `cause` for logging), never as a
modeled `Err`. The `TechnicalError` class is still exported and used as the
defect cause. Only _anticipated domain_ failures remain in `E`:
`MessageValidationError`, `RpcError`, `RpcTimeoutError`, `RpcCancelledError`,
and the worker's `RetryableError`/`NonRetryableError`.

**Breaking.** Consumers that matched `P.tag("@amqp-contract/TechnicalError")` in
an error matcher (`match`'s `errCases`, `mapErrCases`, `tapErrCases`, …) must
move that handling to the `defect` arm of `match` (or `recoverDefect` /
`tapDefect`). Error channels narrow accordingly: `client.publish(...)` is now
`AsyncResult<void, MessageValidationError>`; `client.call(...)` drops
`TechnicalError` from its union; and `create()`/`close()`/the `AmqpClient`
operations now have an empty modeled channel (`E = never`) — extract their
success with `.get()` (a failed one still panics on the defect) instead of
`.getOrThrow()`.
