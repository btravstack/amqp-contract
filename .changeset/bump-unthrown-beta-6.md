---
"@amqp-contract/client": patch
"@amqp-contract/core": patch
"@amqp-contract/worker": patch
---

Bump `unthrown` to `5.0.0-beta.6`, whose exhaustive matcher is now built-in
(same `.with(…)` / `tag` / `P` call-site shape). The compression helper's
`match` import moves from `ts-pattern` to `unthrown`; with that, `ts-pattern`
is removed entirely (catalog entry, dependencies, devDependencies, and the
peerDependencies added for beta.5's peer requirement) — `unthrown` has zero
runtime dependencies, so nothing needs installing alongside it. The `unthrown`
peer range is raised to `^5.0.0-beta.6`.
