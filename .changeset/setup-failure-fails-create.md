---
"@amqp-contract/core": patch
---

`create()` fails when the broker refuses the contract's topology, instead of
handing back a ready-looking client whose exchanges and queues do not exist.

`amqp-connection-manager` catches a `setup` rejection, emits it as an `error`
event, and announces the connection anyway (`ChannelWrapper._onConnect`), so
`waitForConnect()` answered `Ok` for a client that had just been told
`406 PRECONDITION_FAILED` — a mismatched queue declaration, a missing exchange,
a permission the credentials lack. The only trace was a log line.

`AmqpClient` now records the first `error` that arrives **before** the wrapper
ever announces a connection — the window in which `setup` is the only thing
that has run — and fails that first `waitForConnect()` with a `Defect` carrying
a `TechnicalError`. A defect rather than a modeled error, unlike the
`ConnectionError` beside it: a topology the broker refuses is a broken
contract, which is a bug rather than an operator's business.

**Only the first connect.** On a reconnect the caller is long gone and the
manager keeps retrying, so the existing log-and-continue is untouched.

It also fails **fast**: the failure is raced against the connect, rather than
noticed after it. A refused topology closes the channel, so waiting for
`connect` meant sitting out the whole 30s connect timeout and then reporting an
unreachable broker — the wrong failure, half a minute late. Measured on the new
integration test: 132 ms.

Closes #675.
