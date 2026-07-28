---
"@amqp-contract/client": patch
"@amqp-contract/core": patch
"@amqp-contract/worker": patch
---

Bump `unthrown` to `5.0.0-beta.7` and raise the peer range to `^5.0.0-beta.7`.

Two upstream changes, neither of which alters amqp-contract's own surface:

- The built-in matcher gains `returnType<R>()`, pinning a match's output type so
  every branch is checked against it rather than the result widening to the
  union of the branch returns. It reaches every surface that hands out a
  matcher, including `match`'s `errCases` handler and the five `*ErrCases`
  combinators — so it is available on any amqp-contract result, but nothing here
  requires it.
- `tapErrCases` no longer silently drops a `defect(…)` branch: it now produces a
  `Defect` whose cause is an `AggregateError` of the branch's cause and the
  observed error, matching what a `throw` in the same position already did.
  This codebase has no `tapErrCases` call sites, so nothing changed here — but a
  consumer relying on that branch being dropped will now see a `Defect` surface
  where the pipeline previously carried on with the original `Err`.

The peer floor is raised rather than left at `beta.6` (which the caret range
would already have admitted) so consumers resolve the same beta these packages
were built and tested against — behaviour has shifted between betas on this
line, and a single shared copy is the point of the peer dependency.
