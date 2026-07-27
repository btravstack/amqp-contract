---
"@amqp-contract/client": patch
"@amqp-contract/core": patch
"@amqp-contract/worker": patch
---

Bump `unthrown` to `5.0.0-beta.5`. This tracks two beta breaking changes:
`match`'s error handler key is renamed `err` → `errCases`, and the bare error
combinators gained the `*Cases` suffix (`mapErr` → `mapErrCases`, `flatMapErr` →
`flatMapErrCases`, `tapErr` → `tapErrCases`). `unthrown` also now declares
`ts-pattern` as a peer dependency, so `ts-pattern` (`^5`) is added to the
packages that build against unthrown. The peer range is raised to
`^5.0.0-beta.5`.
