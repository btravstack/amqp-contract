---
"@amqp-contract/core": patch
"@amqp-contract/client": patch
"@amqp-contract/worker": patch
---

The `unthrown` peer range is now `^5.3.0` on core, client and worker alike.
Client already required 5.3 (`fromExecutor`), and the three packages are
installed together, so the looser `^5.0.0` on core and worker advertised a
combination that could not run.
