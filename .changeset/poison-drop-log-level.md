---
"@amqp-contract/worker": major
---

The DLX-less poison-message log moved from `warn` to `info` and changed text.
**If you alert on the old string, that alert will silently stop firing.**

Old (level `warn`):

- `Queue does not have DLX configured - message will be lost on nack` —
  `retry.ts`, terminal nack after retries are exhausted or skipped
- `Queue does not have DLX configured - poison message will be lost on nack` —
  `worker.ts`, payload/header validation failure

New (level `info`):

- `Discarding message: queue is declared onPoison: "drop" and has no DLX`
- `Discarding poison message: queue is declared onPoison: "drop" and has no DLX`

Structured fields are unchanged (`queueName`, `deliveryTag`; plus
`consumerName` on the validation path).

Why: `defineContract` now rejects any consumed queue that has neither a
`deadLetter` nor `onPoison: "drop"`, so these lines are reachable only on a
queue whose author declared the drop. A warning that fires on correct,
intentional configuration is noise, and noise is how real warnings get ignored.
The event is still logged, because a directly-nacked message carries no
`x-death` header and this line is the only record it ever arrived.

Also: `sendToDLQ` no longer logs `Sending message to DLQ` on a queue that has no
DLQ. The two outcomes are now mutually exclusive log lines.

To keep an alert on genuine unexpected loss, alert on the `defineContract` throw
at deploy time instead — it fires before a message is ever published.
