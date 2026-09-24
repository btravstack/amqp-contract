---
"@amqp-contract/core": minor
"@amqp-contract/client": minor
---

`TypedAmqpClient.isConnected()` (and `AmqpClient.isConnected()` in core) tell a
readiness probe whether the broker connection is up and the client is not
closed. It reads `false` while amqp-connection-manager is reconnecting.
