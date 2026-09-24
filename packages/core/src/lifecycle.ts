import { fromSafePromise, OkAsync, type AsyncResult, type NotThenable } from "unthrown";

import type { Logger } from "./logger.js";

/**
 * The `create()` lifecycle shared by `TypedAmqpClient` and `TypedAmqpWorker`:
 * build the instance, run its start-up, and — when start-up fails on either
 * channel — close the instance before reporting the failure, so a failed
 * `create()` never leaks its pooled connection or a consumer it registered.
 *
 * `build` runs inside the safety net, so a synchronous throw (an invalid
 * option, an unparseable URL) becomes a `Defect` instead of escaping.
 *
 * @param name - How the instance names itself in the close-failure warning.
 */
export function startOrClose<T extends { close(): AsyncResult<void, never> }, E>(
  build: () => T & NotThenable<T>,
  start: (instance: T) => AsyncResult<unknown, E>,
  options: { name: string; logger?: Logger | undefined },
): AsyncResult<T, E> {
  return OkAsync(undefined).flatMap(() => {
    const instance = build();
    const started = (async () => {
      const result = await start(instance);
      if (!result.isOk()) {
        const closed = await instance.close();
        if (closed.isDefect()) {
          options.logger?.warn(`Failed to close ${options.name} after a start-up failure`, {
            error: closed.cause,
          });
        }
      }
      // An Err/Defect passes through unchanged, re-shaped to the instance.
      return result.map(() => instance);
    })();
    // `started` never rejects: every step above is a settled Result.
    return fromSafePromise(started).flatMap((result) => result);
  });
}
