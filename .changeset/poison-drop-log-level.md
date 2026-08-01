---
"@amqp-contract/worker": major
---

The DLX-less poison-message log changed text, and a _declared_ drop moved from
`warn` to `info`. **If you alert on the old string, that alert will silently
stop firing** — the new `warn` uses different wording and fires in strictly
fewer cases.

Old — two outcomes, level `warn`:

- `Queue does not have DLX configured - message will be lost on nack` —
  `retry.ts`, terminal nack after retries are exhausted or skipped
- `Queue does not have DLX configured - poison message will be lost on nack` —
  `worker.ts`, payload/header validation failure

New — **three** outcomes, because "no DLX" now splits into declared and
undeclared:

| Queue                      | Level  | Message                                                                                                                                                                                                                            |
| -------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| has a DLX                  | `info` | `Sending message to DLQ` (`retry.ts` only; the validation path nacks without logging and lets the DLX do its job)                                                                                                                  |
| no DLX, `onPoison: "drop"` | `info` | `Discarding message: queue is declared onPoison: "drop" and has no DLX` (`retry.ts`)<br>`Discarding poison message: queue is declared onPoison: "drop" and has no DLX` (`worker.ts`)                                               |
| no DLX, no `onPoison`      | `warn` | `Queue has no dead-letter exchange and no onPoison declaration - message will be lost on nack` (`retry.ts`)<br>`Queue has no dead-letter exchange and no onPoison declaration - poison message will be lost on nack` (`worker.ts`) |

"Has a DLX" means what `defineContract`'s poison-loss check means by it —
`deadLetter: { exchange: … }` **or** an `x-dead-letter-exchange` set through the
raw `arguments` passthrough. Both sites ask one shared predicate, so a queue the
builder accepts as dead-lettering is never logged as an undeclared loss and
never described as discarded.

Structured fields are unchanged (`queueName`, `deliveryTag`; plus
`consumerName` on the validation path).

Why the split: `defineContract` now rejects any consumed queue that has neither
a `deadLetter` nor `onPoison: "drop"`, so the `info` lines are reachable only on
a queue whose author declared the drop. A warning that fires on correct,
intentional configuration is noise, and noise is how real warnings get ignored.
The `warn` survives for the case that is still a genuine accident: a hand-built
`ContractDefinition` that bypassed `defineContract` and carries neither. The
event is always logged, because a directly-nacked message carries no `x-death`
header and this line is the only record it ever arrived.

Also: `sendToDLQ` no longer logs `Sending message to DLQ` on a queue that has no
DLQ. The three outcomes are mutually exclusive log lines.

**If you migrated by broker policy** (`rabbitmqctl set_policy … dead-letter-exchange`)
rather than by a `deadLetter` queue argument, your contract still declares
`onPoison: "drop"`, so the worker logs the _discard_ line while the broker is in
fact dead-lettering the message correctly. The log describes what the contract
knows, not what the broker does. Nothing is lost; the line is expected.

To keep an alert on genuine unexpected loss, alert on the new `warn` wording, or
better on the `defineContract` throw at deploy time — it fires before a message
is ever published.
