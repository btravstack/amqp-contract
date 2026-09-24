---
"@amqp-contract/core": major
"@amqp-contract/client": major
"@amqp-contract/worker": major
---

A client and a worker no longer share a TCP connection by default.

The process-wide connection pool is now partitioned: `TypedAmqpClient` draws
from a `"client"` pool and `TypedAmqpWorker` from a `"worker"` pool, so the
same URLs give a publisher and a consumer two connections. RabbitMQ blocks a
publishing connection under a memory or disk alarm; a consumer sharing it
would stop acking along with it. Clients still share among themselves, and
workers among themselves.

- `AmqpClient` gains `connectionPool` (the pool partition, default
  `"default"`) and `connection` — an `AmqpConnectionManager` you own, which
  the client only opens a channel on and never closes. `urls` becomes
  optional: pass exactly one of `urls` or `connection`.
- `TypedAmqpClient.create` accepts `connection` too (the new
  `ConnectionSource` type, exported from core and client). Hand the same
  connection to a client and a worker to share one explicitly.
