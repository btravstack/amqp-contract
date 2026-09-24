---
"@amqp-contract/worker": major
---

Every delivery now ends in one modeled outcome — acked, retried, requeued or
dead-lettered — settled exactly once and read by telemetry, instead of routing
failures through the defect channel.

- An inbound message that fails its schema is the modeled
  `MessageValidationError` (as on the client), no longer a defect wrapping it
  in a `TechnicalError`: the consume span records `MessageValidationError` as
  its exception. It is still dead-lettered on first delivery, never retried.
- A handler failure that was routed (retried or dead-lettered) records a
  failed consume with the handler's own error class, without a defect.
- Defects are reserved for genuine bugs (a handler or middleware that throws)
  and are still dead-lettered.
- Retry routing logs one decision line — `Retrying message (requeue)`,
  `Retrying message (republish)` or `Sending to DLQ: <reason>` — replacing the
  per-mode wording.
