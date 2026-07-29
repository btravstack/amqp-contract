---
"@amqp-contract/client": patch
"@amqp-contract/core": patch
"@amqp-contract/worker": patch
---

Bump `unthrown` to the stable `5.0.0` and raise the peer range to `^5.0.0`.

The betas between `5.0.0-beta.7` and the release carried one change that reaches
call sites:

- **`tag(t)` is gone; the pattern is `P.tag(t)`** (`5.0.0-beta.9`). It was the
  only pattern constructor living loose on the root export while `P._` / `P.any`
  / `P.instanceOf` / `P.when` / `P.union` sat on the namespace, so it moved onto
  `P`. There is no alias. The pattern's type and runtime behaviour are
  unchanged — it still produces `{ _tag: t }`, still narrows to the matching
  variant with its payload, and still composes in grouped patterns and inside
  `P.union`.

  Every matcher example in these packages' TSDoc, READMEs and the docs site is
  updated. In your own code the migration is mechanical — swap `tag` for `P` in
  the `unthrown` import and prefix the call sites:

  ```diff
  - import { tag } from "unthrown";
  + import { P } from "unthrown";

    result.match({
      ok: () => {/* … */},
      errCases: (matcher) =>
  -     matcher.with(tag("@amqp-contract/MessageValidationError"), (error) => {/* … */}),
  +     matcher.with(P.tag("@amqp-contract/MessageValidationError"), (error) => {/* … */}),
      defect: (cause) => { throw cause; },
    });
  ```

`5.0.0-beta.10` also stopped shipping sourcemaps and declaration maps (`files`
already excluded `src/`, so the published maps were dead-ends); `beta.8`,
`beta.11` and `beta.12` were no-ops for core, and the `5.0.0` release itself
added nothing beyond `beta.12`. `@unthrown/vitest` moves to `5.0.0` alongside.

The peer floor is raised to the release rather than left at a beta so consumers
resolve the same copy these packages were built and tested against — the point
of the peer dependency is a single shared `unthrown`.
