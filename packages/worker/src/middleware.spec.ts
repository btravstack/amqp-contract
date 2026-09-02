import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, it } from "vitest";

import { NonRetryableError } from "./errors.js";
import { composeMiddleware, declareMiddleware, type WorkerMiddlewareArgs } from "./middleware.js";

const baseArgs: WorkerMiddlewareArgs<Record<string, unknown>> = {
  message: { payload: { id: "1" }, headers: undefined },
  rawMessage: { properties: { headers: {} } } as unknown as ConsumeMessage,
  handlerName: "processOrder",
  isRpc: false,
  context: {},
};

describe("composeMiddleware", () => {
  it("runs middleware left-to-right with the first as outermost", async () => {
    // GIVEN
    const order: string[] = [];
    const outer = declareMiddleware((_args, next) => {
      order.push("outer:before");
      return next().tap(() => order.push("outer:after"));
    });
    const inner = declareMiddleware((_args, next) => {
      order.push("inner:before");
      return next().tap(() => order.push("inner:after"));
    });

    // WHEN
    const chain = composeMiddleware(outer, inner);
    const result = await chain(baseArgs, () => {
      order.push("handler");
      return OkAsync(undefined);
    });

    // THEN
    expect(result.isOk()).toBe(true);
    expect(order).toEqual([
      "outer:before",
      "inner:before",
      "handler",
      "inner:after",
      "outer:after",
    ]);
  });

  it("accumulates context across the chain", async () => {
    // GIVEN
    const first = declareMiddleware<Record<never, never>, { a: number }>((_args, next) =>
      next({ context: { a: 1 } }),
    );
    const second = declareMiddleware<{ a: number }, { a: number; b: string }>((args, next) =>
      next({ context: { ...args.context, b: `a=${args.context.a}` } }),
    );

    // WHEN
    let seen: Record<string, unknown> | undefined;
    const chain = composeMiddleware(first, second);
    const result = await chain(baseArgs, (opts) => {
      seen = opts?.context;
      return OkAsync(undefined);
    });

    // THEN
    expect(result.isOk()).toBe(true);
    expect(seen).toEqual({ a: 1, b: "a=1" });
  });

  it("merges injected context over the incoming one when a middleware passes only its own fields", async () => {
    // GIVEN — second middleware injects without spreading args.context
    const first = declareMiddleware<Record<never, never>, { a: number }>((_args, next) =>
      next({ context: { a: 1 } }),
    );
    const second = declareMiddleware<{ a: number }, { a: number; b: string }>((_args, next) =>
      // Deliberately not spreading: the dispatcher merges over the current context.
      next({ context: { b: "solo" } as { a: number; b: string } }),
    );

    // WHEN
    let seen: Record<string, unknown> | undefined;
    const chain = composeMiddleware(first, second);
    await chain(baseArgs, (opts) => {
      seen = opts?.context;
      return OkAsync(undefined);
    });

    // THEN
    expect(seen).toEqual({ a: 1, b: "solo" });
  });

  it("short-circuits when a middleware returns without calling next", async () => {
    // GIVEN
    const guard = declareMiddleware((_args, _next) =>
      ErrAsync(new NonRetryableError("blocked by guard")),
    );
    let handlerRan = false;

    // WHEN
    const chain = composeMiddleware(guard);
    const result = await chain(baseArgs, () => {
      handlerRan = true;
      return OkAsync(undefined);
    });

    // THEN
    expect(handlerRan).toBe(false);
    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error.message).toBe("blocked by guard");
    }
  });

  it("threads substituted payloads to inner middleware and the terminal", async () => {
    // GIVEN — outer substitutes, inner observes the substituted payload
    const seen: unknown[] = [];
    const substitute = declareMiddleware((_args, next) => next({ payload: { id: "2" } }));
    const observer = declareMiddleware((args, next) => {
      seen.push(args.message.payload);
      return next();
    });

    // WHEN
    let terminalOpts: { context?: Record<string, unknown>; payload?: unknown } | undefined;
    const chain = composeMiddleware(substitute, observer);
    await chain(baseArgs, (opts) => {
      terminalOpts = opts;
      return OkAsync(undefined);
    });

    // THEN — inner middleware saw the substitution; terminal received it for re-validation
    expect(seen).toEqual([{ id: "2" }]);
    expect(terminalOpts?.payload).toEqual({ id: "2" });
  });

  it("omits payload from the terminal opts when nothing substituted", async () => {
    let terminalOpts: { payload?: unknown } | undefined;
    const passthrough = declareMiddleware((_args, next) => next());
    const chain = composeMiddleware(passthrough);
    await chain(baseArgs, (opts) => {
      terminalOpts = opts;
      return OkAsync(undefined);
    });
    expect(terminalOpts !== undefined && "payload" in terminalOpts).toBe(false);
  });

  it("carries an explicit `undefined` substitution, which is not the same as substituting nothing", async () => {
    // GIVEN a middleware that deliberately substitutes `undefined` — the one
    // value that used to mean "I substituted nothing", so the request was
    // silently dropped and the handler saw the original payload
    const seen: unknown[] = [];
    const blank = declareMiddleware((_args, next) => next({ payload: undefined }));
    const observer = declareMiddleware((args, next) => {
      seen.push(args.message.payload);
      return next();
    });

    // WHEN the chain runs
    let terminalOpts: { context?: Record<string, unknown>; payload?: unknown } | undefined;
    const chain = composeMiddleware(blank, observer);
    await chain(baseArgs, (opts) => {
      terminalOpts = opts;
      return OkAsync(undefined);
    });

    // THEN the inner middleware saw the substitution, and the terminal was
    // handed `payload` as a PRESENT key — which is what sends it back through
    // the payload schema, whose business it then is
    expect({
      seen,
      present: terminalOpts !== undefined && Object.hasOwn(terminalOpts, "payload"),
      value: terminalOpts?.payload,
    }).toEqual({ seen: [undefined], present: true, value: undefined });
  });

  it("exposes dispatch metadata to every middleware", async () => {
    // GIVEN
    const seen: Array<{ handlerName: string; isRpc: boolean }> = [];
    const observer = declareMiddleware((args, next) => {
      seen.push({ handlerName: args.handlerName, isRpc: args.isRpc });
      return next();
    });

    // WHEN
    const chain = composeMiddleware(observer, observer);
    await chain({ ...baseArgs, isRpc: true }, () => OkAsync(undefined));

    // THEN
    expect(seen).toEqual([
      { handlerName: "processOrder", isRpc: true },
      { handlerName: "processOrder", isRpc: true },
    ]);
  });
});
