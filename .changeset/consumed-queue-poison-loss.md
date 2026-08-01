---
"@amqp-contract/contract": major
---

`defineContract` now throws when a consumed queue has no dead-letter exchange,
because such a queue silently discards every message its handler rejects.
Declared-but-unconsumed queues (including dead-letter queues themselves) are not
checked.

Three forms satisfy the check: `deadLetter: { exchange: … }`, an explicit
`onPoison: "drop"`, and — least discoverably — an `x-dead-letter-exchange` set
through the raw `arguments` passthrough, which `setupAmqpTopology` forwards to
the broker verbatim. Note that the check verifies a DLX is _declared_, not that
the exchange it names has a bound queue: a dead-letter exchange with no binding
still drops every message routed to it, so declare the dead-letter queue and its
binding too.

**Read this before adding `deadLetter` to a queue that already exists in
production.** A queue's dead-letter configuration is part of its identity:
`deadLetter` becomes the `x-dead-letter-exchange` argument, and RabbitMQ refuses
to redeclare an existing queue with different arguments —
`PRECONDITION_FAILED - inequivalent arg`, a channel-level 406 at worker startup.
Adding `deadLetter` to a live queue therefore fails at deploy time, not at
define time. Your routes out:

- **Declare a new queue with the DLX and migrate consumers to it**, draining the
  old one first. The only option that changes nothing about the running queue.
- **Apply the dead-lettering as a broker policy** (`rabbitmqctl set_policy` with
  `dead-letter-exchange`) instead of a queue argument. Policies are not part of
  queue identity, so they apply to existing queues — but the contract still
  needs `onPoison: "drop"` to pass this check, since it cannot see the policy.
- **`onPoison: "drop"`** if you accept the loss. Correct for a metrics firehose
  or any queue whose rejected messages genuinely have no value; a lie anywhere
  else.

On a queue that does not exist yet, `deadLetter: { exchange: … }` is the right
answer and costs nothing.

See "`defineContract` says my queue has no dead-letter exchange" and
"`PRECONDITION_FAILED - inequivalent arg`" in the troubleshooting guide.
