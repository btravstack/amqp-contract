// Registers @unthrown/vitest's Result/AsyncResult matchers (toBeOk, toBeOkWith,
// toBeErr, toBeErrTagged, toBeDefect) on Vitest's `expect`. The bare import has
// the side effect of calling `expect.extend(...)`; being part of the TS program
// (via `src/**/*`) also pulls in the `declare module "vitest"` augmentation so
// the matchers are typed in specs.
import "@unthrown/vitest";
