---
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
---

Channels now set a 30s `publishTimeout` by default. Publishes issued during a
broker outage previously buffered without bound and their promises never
settled. Set `publishTimeoutMs` to tune it, or `publishTimeoutMs: null` to
disable.
