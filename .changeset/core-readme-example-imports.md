---
"@amqp-contract/core": patch
---

Fixed the README's quick-start example, which called `defineQueueBinding` without importing it — copying it verbatim threw `ReferenceError`. The example is now split into a contract definition and a client-usage snippet, each carrying its own imports.

Every documented `defineContract` example in the repository is now executed in CI with only the imports it shows, so a snippet that a reader cannot copy and run fails the build.
