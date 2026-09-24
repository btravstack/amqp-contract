---
"@amqp-contract/core": patch
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
"@amqp-contract/contract": patch
"@amqp-contract/testing": patch
---

Publish runtime dependencies (`amqplib`, `amqp-connection-manager`, `@standard-schema/spec`, `@unthrown/standard-schema`, `testcontainers`) with caret ranges instead of exact versions, so consumers can pick up upstream patch and security releases without waiting for an amqp-contract release.
