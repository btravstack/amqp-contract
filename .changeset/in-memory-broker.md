---
"@amqp-contract/testing": minor
"@amqp-contract/client": minor
"@amqp-contract/worker": minor
"@amqp-contract/core": minor
---

`InMemoryAmqpBroker`: run a contract end to end with no Docker.

```ts
import { InMemoryAmqpBroker } from "@amqp-contract/testing";

const broker = new InMemoryAmqpBroker();
const worker = await TypedAmqpWorker.create({
  contract,
  handlers,
  transport: broker.createTransport(contract),
}).getOrThrow();
const client = await TypedAmqpClient.create({
  contract,
  transport: broker.createTransport(contract),
}).getOrThrow();
```

Testing a contract and its handlers meant a container, and a container is a
30-second tax on a question the broker was never going to answer differently.
What runs for real here is everything above the wire: routing, both validation
passes, middleware and interceptors, RPC correlation over direct reply-to,
retry routing, TTL dead-lettering.

**The seam is `AmqpTransport`**, new in `@amqp-contract/core`: the eight
members `TypedAmqpClient` and `TypedAmqpWorker` actually use, out of
`AmqpClient`'s full surface. A compile-time assertion keeps `AmqpClient`
satisfying it, so a signature change there is a type error rather than a
substitute that silently stops matching. Both facades now take
`transport?: AmqpTransport` beside `urls`, and **exactly one is required** —
passing both is refused rather than silently preferring one, because a test
that supplies a transport and inherits a `urls` default would otherwise reach
a real broker while believing it had not.

`urls` becomes optional on both option types. Existing code is unaffected.

The fake is deliberately not kinder than a broker: an unroutable publish is
dropped and confirmed, and a dead-letter exchange with nothing bound loses the
message. Topology refusals, reconnection, flow control and persistence are not
modelled and stay the integration suite's job — the boundary is written down
in the new `how-to/test-without-a-broker` page.
