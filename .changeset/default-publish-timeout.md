---
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
---

Channels now set a 30s `publishTimeout` by default. Publishes issued during a
broker outage previously buffered without bound and their promises never
settled. Set `publishTimeoutMs` to tune it, or `publishTimeoutMs: null` to
disable.

The timeout surfaces on the **defect** channel, not as a modelled error: publish
models only `MessageValidationError`, so a timed-out publish will not appear in
`errCases` and will panic `.get()` / `.getOrThrow()`. Code with a `defect` arm it
believed unreachable should expect to reach it during an outage.

Note also that a timed-out publish may still have reached the broker — the
timeout drops the message from the unconfirmed set locally, it does not recall
it. Retrying in response can therefore duplicate.
