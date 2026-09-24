---
"@amqp-contract/core": major
"@amqp-contract/client": major
---

Topology setup is now role-scoped and has a mode.

- **The client only declares what its publishes need to be routed and
  retained**: its publishers' exchanges, everything they route to
  (exchange-to-exchange bindings, transitively), every queue reachable that
  way with its binding, and the RPC request queues — so a message published
  before any worker started is kept, never confirmed-and-dropped. Those queues
  are declared with the worker's exact arguments, but the client no longer
  declares the consumer's infrastructure (dead-letter exchanges, retry wait
  queues), unrelated queues, or exclusive queues.
- **New `topology` option** on `TypedAmqpClient.create` and `AmqpClient`
  (`TopologyMode`, exported from core and client):
  - `"assert"` (default) — declare, as before;
  - `"passive"` — only `checkExchange` / `checkQueue`, declaring nothing, for
    credentials that may not configure the broker; a missing resource fails
    `create()`;
  - `"none"` — touch nothing (topology provisioned elsewhere).
- `setupAmqpTopology(channel, contract, { mode })` takes the mode too.

The worker keeps asserting the full contract for now.
