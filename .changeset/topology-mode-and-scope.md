---
"@amqp-contract/core": major
"@amqp-contract/client": major
---

Topology setup is now role-scoped and has a mode.

- **The client only declares what a publisher needs**: the exchanges its
  publishers publish to and the exchange-to-exchange bindings forwarding from
  them. It no longer asserts every queue, dead-letter exchange and binding in
  the contract — those are the worker's. Breaking: a message published before
  any worker (or other provisioning) declared its queue is unroutable and
  dropped by the broker; start the worker first, or provision the queues out
  of band.
- **New `topology` option** on `TypedAmqpClient.create` and `AmqpClient`
  (`TopologyMode`, exported from core and client):
  - `"assert"` (default) — declare, as before;
  - `"passive"` — only `checkExchange` / `checkQueue`, declaring nothing, for
    credentials that may not configure the broker; a missing resource fails
    `create()`;
  - `"none"` — touch nothing (topology provisioned elsewhere).
- `setupAmqpTopology(channel, contract, { mode })` takes the mode too.

The worker keeps asserting the full contract for now.
