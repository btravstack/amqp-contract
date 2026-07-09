---
"@amqp-contract/core": patch
---

Make `AmqpClient`'s `TechnicalError` messages actionable: connection failures now hint at verifying the broker is reachable at the configured `urls`, and publish failures include the target exchange and routing key (or queue name) instead of a generic "Failed to publish message".
