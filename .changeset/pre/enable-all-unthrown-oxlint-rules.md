---
"@amqp-contract/contract": patch
"@amqp-contract/core": patch
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
"@amqp-contract/asyncapi": patch
"@amqp-contract/testing": patch
---

Internal cleanup from enabling all `@unthrown/oxlint` rules (`no-throw` and `prefer-ensure` on top of the recommended set): redundant `Promise<Result<...>>` return annotations dropped in favor of inference, and every deliberate `throw` site now carries a targeted lint disable naming its reason. No runtime behavior change.
