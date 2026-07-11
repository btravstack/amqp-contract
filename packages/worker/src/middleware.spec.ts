import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, it } from "vitest";
import { nonRetryable } from "./errors.js";
import { composeMiddleware, defineMiddleware, type WorkerMiddlewareArgs } from "./middleware.js";

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
    const outer = defineMiddleware((_args, next) => {
      order.push("outer:before");
      return next().tap(() => order.push("outer:after"));
    });
    const inner = defineMiddleware((_args, next) => {
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
    const first = defineMiddleware<Record<never, never>, { a: number }>((_args, next) =>
      next({ context: { a: 1 } }),
    );
    const second = defineMiddleware<{ a: number }, { a: number; b: string }>((args, next) =>
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
    const first = defineMiddleware<Record<never, never>, { a: number }>((_args, next) =>
      next({ context: { a: 1 } }),
    );
    const second = defineMiddleware<{ a: number }, { a: number; b: string }>((_args, next) =>
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
    const guard = defineMiddleware((_args, _next) => ErrAsync(nonRetryable("blocked by guard")));
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
    const substitute = defineMiddleware((_args, next) => next({ payload: { id: "2" } }));
    const observer = defineMiddleware((args, next) => {
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
    const passthrough = defineMiddleware((_args, next) => next());
    const chain = composeMiddleware(passthrough);
    await chain(baseArgs, (opts) => {
      terminalOpts = opts;
      return OkAsync(undefined);
    });
    expect(terminalOpts !== undefined && "payload" in terminalOpts).toBe(false);
  });

  it("exposes dispatch metadata to every middleware", async () => {
    // GIVEN
    const seen: Array<{ handlerName: string; isRpc: boolean }> = [];
    const observer = defineMiddleware((args, next) => {
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
