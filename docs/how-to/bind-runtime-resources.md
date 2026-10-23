# Bind a protocol to runtime resources

Use `defineContractBinding(contract, { exchanges, queues })` when compatible
implementations need separate physical destinations in one vhost. Mapping keys
are original resource names, not publisher or consumer operation keys.

```typescript
import { defineContractBinding } from "@amqp-contract/contract";
const release = defineContractBinding(protocol, {
  queues: { "billing.invoices": "billing.release-a.invoices" },
  exchanges: { "billing.parked": "billing.release-a.parked" },
});
```

Pass the bound contract to the typed client and worker. Message schema objects,
RPC errors, logical operation keys and business routing keys stay unchanged.
The result has physical-name topology keys; the source contract is unchanged.
Only explicitly mapped resources move. In particular, an imported producer's
exchange stays shared unless its owner supplied an explicit destination.

The binding rewrites queue/exchange references, dead-letter and alternate-exchange
arguments, default-exchange queue destinations and RPC request queues. Retry wait
queues and return routes derive from the bound main queue. The default exchange
and direct reply-to keep their transport semantics. Invalid, reserved, unknown,
colliding and overlong names fail at definition time, including derived retries.

Unique queues do not select a tenant's implementation: two matching subscriptions
both receive an event. The application/control plane still selects one admitted
destination per logical subscriber and preserves that decision for retries.
Resource prefixes also do not replace broker permissions or tenant authorization.
