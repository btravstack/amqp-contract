import type { RpcError } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import type { AsyncResult } from "unthrown";
import type { HandlerError } from "./errors.js";

/**
 * The middleware context handlers see when no middleware injects anything.
 *
 * `Record<never, never>` rather than `{}` so an empty context is a real
 * "no properties" type instead of the anything-goes empty-object type.
 */
export type EmptyContext = Record<never, never>;

/**
 * Arguments passed to a worker middleware — everything the downstream handler
 * will receive, plus dispatch metadata and the context accumulated by outer
 * middleware.
 */
export type WorkerMiddlewareArgs<TContextIn extends Record<string, unknown> | EmptyContext> = {
  /** The validated message (payload and headers already schema-checked). */
  message: { payload: unknown; headers: unknown };
  /** The raw amqplib message (delivery tag, AMQP properties, raw headers, …). */
  rawMessage: ConsumeMessage;
  /** The `consumers` / `rpcs` key being dispatched. */
  handlerName: string;
  /** True when the handler is an RPC server (its result is published as a reply). */
  isRpc: boolean;
  /** Context accumulated by outer middleware (empty for the outermost one). */
  context: TContextIn;
};

/**
 * Continuation a middleware calls to run the rest of the chain (inner
 * middleware, then the handler). `opts.context` is the full context the
 * downstream sees — pass `{ ...args.context, ...injected }` (or just the
 * injected fields; the dispatcher merges over the current context either way).
 *
 * The returned AsyncResult carries the handler outcome: `undefined` for a
 * regular consumer, the (not yet validated) response for an RPC. A middleware
 * can transform or inspect it before returning.
 */
export type WorkerMiddlewareNext<TContextOut extends Record<string, unknown> | EmptyContext> =
  (opts?: { context?: TContextOut }) => AsyncResult<unknown, HandlerError | RpcError>;

/**
 * A worker middleware: wraps every handler invocation (consumers and RPCs)
 * after message validation.
 *
 * Middleware follow the guard-and-narrow pattern: check something, then call
 * `next({ context })` to run the rest of the chain with typed fields injected
 * into the handler's third argument — or short-circuit by returning without
 * calling `next`:
 *
 * - `Err(retryable(...))` / `Err(nonRetryable(...))` routes through the normal
 *   retry/DLQ pipeline, exactly as if the handler had returned it.
 * - `Err(rpcError(code, data))` (on an RPC with a declared `errors` map)
 *   publishes a typed error reply.
 * - `Ok(value)` skips the handler entirely; for an RPC, `value` is validated
 *   against the response schema and published as the reply (cache pattern).
 *
 * @typeParam TContextIn - context provided by outer middleware
 * @typeParam TContextOut - context this middleware passes downstream
 *
 * @example
 * ```typescript
 * import { defineMiddleware, nonRetryable } from '@amqp-contract/worker';
 * import { Err } from 'unthrown';
 *
 * const auth = defineMiddleware<EmptyContext, { tenantId: string }>((args, next) => {
 *   const tenantId = args.rawMessage.properties.headers?.['x-tenant-id'];
 *   if (typeof tenantId !== 'string') {
 *     return Err(nonRetryable('Missing x-tenant-id header')).toAsync();
 *   }
 *   return next({ context: { tenantId } });
 * });
 * ```
 */
export type WorkerMiddleware<
  TContextIn extends Record<string, unknown> | EmptyContext = EmptyContext,
  TContextOut extends TContextIn = TContextIn,
> = (
  args: WorkerMiddlewareArgs<TContextIn>,
  next: WorkerMiddlewareNext<TContextOut>,
) => AsyncResult<unknown, HandlerError | RpcError>;

/**
 * Widened middleware shape used internally by the dispatcher, where context
 * types have been erased. Public call sites keep the typed
 * {@link WorkerMiddleware} form.
 */
export type AnyWorkerMiddleware = WorkerMiddleware<
  Record<string, unknown>,
  Record<string, unknown>
>;

/**
 * Identity helper that pins a middleware's context types without a variable
 * annotation — `defineMiddleware<In, Out>(fn)` reads better than
 * `const mw: WorkerMiddleware<In, Out> = fn`.
 */
export function defineMiddleware<
  TContextIn extends Record<string, unknown> | EmptyContext = EmptyContext,
  TContextOut extends TContextIn = TContextIn,
>(
  middleware: WorkerMiddleware<TContextIn, TContextOut>,
): WorkerMiddleware<TContextIn, TContextOut> {
  return middleware;
}

/**
 * Compose middleware left-to-right: the first argument is the outermost
 * (runs first, sees the emptiest context), the last is the innermost (its
 * injected context is what handlers receive). Context types accumulate
 * across the chain — each middleware's `TContextIn` must match what the
 * previous one produced.
 *
 * Typed overloads cover up to 8 middleware. For longer chains, nest: a
 * composed chain is itself a `WorkerMiddleware<EmptyContext, T>` and can be
 * the *first* argument of an outer `composeMiddleware` call —
 * `composeMiddleware(composeMiddleware(a, ..., h), i, j)` — preserving
 * context-type accumulation at any length.
 *
 * @example
 * ```typescript
 * const middleware = composeMiddleware(logging, auth, idempotency);
 * // handlers receive the context injected by all three
 * ```
 */
export function composeMiddleware<TA extends Record<string, unknown>>(
  m1: WorkerMiddleware<EmptyContext, TA>,
): WorkerMiddleware<EmptyContext, TA>;
export function composeMiddleware<TA extends Record<string, unknown>, TB extends TA>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
): WorkerMiddleware<EmptyContext, TB>;
export function composeMiddleware<TA extends Record<string, unknown>, TB extends TA, TC extends TB>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
  m3: WorkerMiddleware<TB, TC>,
): WorkerMiddleware<EmptyContext, TC>;
export function composeMiddleware<
  TA extends Record<string, unknown>,
  TB extends TA,
  TC extends TB,
  TD extends TC,
>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
  m3: WorkerMiddleware<TB, TC>,
  m4: WorkerMiddleware<TC, TD>,
): WorkerMiddleware<EmptyContext, TD>;
export function composeMiddleware<
  TA extends Record<string, unknown>,
  TB extends TA,
  TC extends TB,
  TD extends TC,
  TE extends TD,
>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
  m3: WorkerMiddleware<TB, TC>,
  m4: WorkerMiddleware<TC, TD>,
  m5: WorkerMiddleware<TD, TE>,
): WorkerMiddleware<EmptyContext, TE>;
export function composeMiddleware<
  TA extends Record<string, unknown>,
  TB extends TA,
  TC extends TB,
  TD extends TC,
  TE extends TD,
  TF extends TE,
>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
  m3: WorkerMiddleware<TB, TC>,
  m4: WorkerMiddleware<TC, TD>,
  m5: WorkerMiddleware<TD, TE>,
  m6: WorkerMiddleware<TE, TF>,
): WorkerMiddleware<EmptyContext, TF>;
export function composeMiddleware<
  TA extends Record<string, unknown>,
  TB extends TA,
  TC extends TB,
  TD extends TC,
  TE extends TD,
  TF extends TE,
  TG extends TF,
>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
  m3: WorkerMiddleware<TB, TC>,
  m4: WorkerMiddleware<TC, TD>,
  m5: WorkerMiddleware<TD, TE>,
  m6: WorkerMiddleware<TE, TF>,
  m7: WorkerMiddleware<TF, TG>,
): WorkerMiddleware<EmptyContext, TG>;
export function composeMiddleware<
  TA extends Record<string, unknown>,
  TB extends TA,
  TC extends TB,
  TD extends TC,
  TE extends TD,
  TF extends TE,
  TG extends TF,
  TH extends TG,
>(
  m1: WorkerMiddleware<EmptyContext, TA>,
  m2: WorkerMiddleware<TA, TB>,
  m3: WorkerMiddleware<TB, TC>,
  m4: WorkerMiddleware<TC, TD>,
  m5: WorkerMiddleware<TD, TE>,
  m6: WorkerMiddleware<TE, TF>,
  m7: WorkerMiddleware<TF, TG>,
  m8: WorkerMiddleware<TG, TH>,
): WorkerMiddleware<EmptyContext, TH>;
export function composeMiddleware(
  ...middlewares: readonly AnyWorkerMiddleware[]
): AnyWorkerMiddleware {
  return (args, next) => {
    const run = (
      index: number,
      context: Record<string, unknown>,
    ): ReturnType<AnyWorkerMiddleware> =>
      index >= middlewares.length
        ? next({ context })
        : middlewares[index]!({ ...args, context }, (opts) =>
            run(index + 1, { ...context, ...opts?.context }),
          );
    return run(0, args.context);
  };
}
