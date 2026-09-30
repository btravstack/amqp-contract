---
"@amqp-contract/testing": patch
---

The `vitest` peer range is now `^4 || ^5`. Its fixtures and global setup run
unchanged on Vitest 5, so a project on Vitest 5 no longer fails to install
under strict peer dependencies.
