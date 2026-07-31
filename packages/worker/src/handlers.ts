import type {
  ContractDefinition,
  InferConsumerNames,
  InferRpcNames,
} from "@amqp-contract/contract";

import type { EmptyContext } from "./middleware.js";
import type {
  WorkerInferConsumerHandler,
  WorkerInferConsumerHandlerEntry,
  WorkerInferHandlers,
  WorkerInferRpcHandler,
  WorkerInferRpcHandlerEntry,
} from "./types.js";
import { type ConsumerOptions } from "./worker.js";

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Build the list of available handler-target names — every key under
 * `contract.consumers` plus every key under `contract.rpcs`.
 *
 * @internal Shared with `TypedAmqpWorker.create` for its fail-fast messages.
 */
export function availableHandlerNames<TContract extends ContractDefinition>(
  contract: TContract,
): readonly string[] {
  const consumers = contract.consumers ? Object.keys(contract.consumers) : [];
  const rpcs = contract.rpcs ? Object.keys(contract.rpcs) : [];
  return [...consumers, ...rpcs];
}

function formatAvailable(names: readonly string[]): string {
  return names.length > 0 ? names.join(", ") : "none";
}

/**
 * Validate that a name maps to a contract entry — either a `consumers` key
 * or an `rpcs` key. The two name spaces are disjoint by contract definition.
 */
function validateHandlerTargetExists<TContract extends ContractDefinition>(
  contract: TContract,
  name: string,
): void {
  const consumers = contract.consumers;
  const rpcs = contract.rpcs;

  const isConsumer = !!consumers && Object.hasOwn(consumers, name);
  const isRpc = !!rpcs && Object.hasOwn(rpcs, name);

  if (!isConsumer && !isRpc) {
    const available = formatAvailable(availableHandlerNames(contract));
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error — worker creation fails before any connection is acquired
    throw new Error(
      `Handler target "${name}" not found in contract. Available consumers and RPCs: ${available}`,
    );
  }
}

/**
 * Contract entries (`consumers` and `rpcs` keys) that have no corresponding
 * handler in `handlers`. The type system requires completeness at the public
 * API boundary, but a JavaScript caller, a cast, or a contract that gained an
 * entry without its worker being updated can bypass it — this is the runtime
 * backstop.
 *
 * @internal Shared with `TypedAmqpWorker.create` for its fail-fast check.
 */
export function missingHandlerNames<TContract extends ContractDefinition>(
  contract: TContract,
  handlers: object,
): readonly string[] {
  return availableHandlerNames(contract).filter((name) => !Object.hasOwn(handlers, name));
}

/**
 * Keys in `handlers` that name no `consumers` or `rpcs` entry in the contract.
 * The type system rejects these at the public API boundary, but a spread-built
 * handlers object carrying a stale key, or a contract rename the worker missed,
 * bypasses it — and the key would then be silently ignored (no consumer set
 * up, no error), leaving the message class it was meant to handle unprocessed.
 *
 * @internal Shared with `TypedAmqpWorker.create` for its fail-fast check.
 */
export function unknownHandlerNames<TContract extends ContractDefinition>(
  contract: TContract,
  handlers: object,
): readonly string[] {
  const available = new Set(availableHandlerNames(contract));
  return Object.keys(handlers).filter((name) => !available.has(name));
}

/**
 * Contract entries whose handler is present but not callable — `undefined`,
 * `null`, or a tuple whose first element is not a function.
 * {@link missingHandlerNames} cannot catch these (`Object.hasOwn` sees the
 * key), yet each delivery would defect with an opaque TypeError and
 * poison-loop to the DLQ.
 *
 * @internal Shared with `TypedAmqpWorker.create` for its fail-fast check.
 */
export function invalidHandlerNames<TContract extends ContractDefinition>(
  contract: TContract,
  handlers: object,
): readonly string[] {
  const record = handlers as Record<string, unknown>;
  return availableHandlerNames(contract).filter((name) => {
    if (!Object.hasOwn(record, name)) return false; // reported as missing instead
    const entry = record[name];
    const handler = Array.isArray(entry) ? entry[0] : entry;
    return typeof handler !== "function";
  });
}

/**
 * Validate `handlers` against the contract in both directions: every key in
 * `handlers` maps to a contract entry (a `consumers` or `rpcs` key), and
 * every contract entry has a handler.
 */
function validateHandlers<TContract extends ContractDefinition>(
  contract: TContract,
  handlers: object,
): void {
  // JavaScript callers can pass null/undefined despite the type — surface a
  // clear error instead of the TypeError Object.keys would throw.
  if (handlers === null || typeof handlers !== "object") {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error — worker creation fails before any connection is acquired
    throw new Error(
      "declareHandlers requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
    );
  }
  for (const handlerName of Object.keys(handlers)) {
    validateHandlerTargetExists(contract, handlerName);
  }
  const missing = missingHandlerNames(contract, handlers);
  if (missing.length > 0) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error — worker creation fails before any connection is acquired
    throw new Error(
      `Missing handlers for contract entries: ${missing.join(", ")}. ` +
        "Every `consumers` and `rpcs` key requires a handler.",
    );
  }
  const invalid = invalidHandlerNames(contract, handlers);
  if (invalid.length > 0) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error — worker creation fails before any connection is acquired
    throw new Error(
      `Handlers for contract entries are not functions: ${invalid.join(", ")}. ` +
        "Each handler must be a function or a [handler, options] tuple.",
    );
  }
}

// =============================================================================
// Handler Definitions
// =============================================================================

/**
 * Define a type-safe handler for a specific consumer or RPC in a contract.
 *
 * **Recommended:** This function creates handlers that return
 * `AsyncResult<void, HandlerError>` (consumers) or
 * `AsyncResult<TResponse, HandlerError>` (RPCs), providing explicit error
 * handling and better control over retry behavior.
 *
 * Supports two patterns:
 * 1. Simple handler: just the function
 * 2. Handler with options: `[handler, { prefetch: 10 }]`
 *
 * @template TContract - The contract definition type
 * @template TName - The consumer or RPC name from the contract
 * @param contract - The contract definition containing the consumer or RPC
 * @param name - The name of the consumer or RPC from the contract
 * @param handler - The handler function — for consumers, returns
 *   `AsyncResult<void, HandlerError>`; for RPCs, returns
 *   `AsyncResult<TResponse, HandlerError>`.
 * @param options - Optional consumer options (prefetch)
 * @returns A type-safe handler that can be used with TypedAmqpWorker
 *
 * @example Consumer handler
 * ```typescript
 * import { declareHandler, RetryableError, NonRetryableError } from '@amqp-contract/worker';
 * import { fromPromise } from 'unthrown';
 *
 * const processOrderHandler = declareHandler(
 *   orderContract,
 *   'processOrder',
 *   ({ payload }) =>
 *     fromPromise(
 *       processPayment(payload),
 *       (error) => new RetryableError('Payment failed', error),
 *     ).map(() => undefined),
 * );
 * ```
 *
 * @example RPC handler
 * ```typescript
 * const calculateHandler = declareHandler(
 *   rpcContract,
 *   'calculate',
 *   ({ payload }) => OkAsync({ sum: payload.a + payload.b }),
 * );
 * ```
 */
export function declareHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferConsumerHandler<TContract, TName, TContext>,
): WorkerInferConsumerHandlerEntry<TContract, TName, TContext>;
export function declareHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferConsumerHandler<TContract, TName, TContext>,
  options: ConsumerOptions,
): WorkerInferConsumerHandlerEntry<TContract, TName, TContext>;
export function declareHandler<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferRpcHandler<TContract, TName, TContext>,
): WorkerInferRpcHandlerEntry<TContract, TName, TContext>;
export function declareHandler<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferRpcHandler<TContract, TName, TContext>,
  options: ConsumerOptions,
): WorkerInferRpcHandlerEntry<TContract, TName, TContext>;
export function declareHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract> | InferRpcNames<TContract>,
>(contract: TContract, name: TName, handler: unknown, options?: ConsumerOptions): unknown {
  validateHandlerTargetExists(contract, String(name));

  if (options) {
    return [handler, options];
  }
  return handler;
}

/**
 * Define multiple type-safe handlers for consumers and RPCs in a contract.
 *
 * **Recommended:** This function creates handlers that return
 * `AsyncResult<void, HandlerError>` (consumers) or
 * `AsyncResult<TResponse, HandlerError>` (RPCs), providing explicit error
 * handling and better control over retry behavior.
 *
 * The handlers object must contain exactly one entry per `consumers` and
 * `rpcs` key in the contract — see {@link WorkerInferHandlers}.
 *
 * @template TContract - The contract definition type
 * @param contract - The contract definition containing the consumers and RPCs
 * @param handlers - An object with handler functions for each consumer and RPC
 * @returns A type-safe handlers object that can be used with TypedAmqpWorker
 *
 * @example
 * ```typescript
 * import { declareHandlers, RetryableError } from '@amqp-contract/worker';
 * import { fromPromise, OkAsync } from 'unthrown';
 *
 * const handlers = declareHandlers(orderContract, {
 *   processOrder: ({ payload }) =>
 *     fromPromise(
 *       processPayment(payload),
 *       (error) => new RetryableError('Payment failed', error),
 *     ).map(() => undefined),
 *   calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }),
 * });
 * ```
 */
export function declareHandlers<
  TContract extends ContractDefinition,
  TContext extends Record<string, unknown> | EmptyContext = EmptyContext,
>(
  contract: TContract,
  handlers: WorkerInferHandlers<TContract, TContext>,
): WorkerInferHandlers<TContract, TContext> {
  validateHandlers(contract, handlers);
  return handlers;
}
