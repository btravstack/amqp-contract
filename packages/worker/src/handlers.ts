import type {
  ContractDefinition,
  InferConsumerNames,
  InferRpcNames,
} from "@amqp-contract/contract";
import type {
  WorkerInferConsumerHandler,
  WorkerInferConsumerHandlerEntry,
  WorkerInferHandlers,
  WorkerInferRpcHandler,
  WorkerInferRpcHandlerEntry,
} from "./types.js";
import { ConsumerOptions } from "./worker.js";

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Build the list of available handler-target names — every key under
 * `contract.consumers` plus every key under `contract.rpcs`.
 */
function availableHandlerNames<TContract extends ContractDefinition>(
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
    throw new Error(
      `Handler target "${name}" not found in contract. Available consumers and RPCs: ${available}`,
    );
  }
}

/**
 * Validate that every key in `handlers` maps to a contract entry —
 * either a `consumers` key or an `rpcs` key.
 */
function validateHandlers<TContract extends ContractDefinition>(
  contract: TContract,
  handlers: object,
): void {
  for (const handlerName of Object.keys(handlers)) {
    validateHandlerTargetExists(contract, handlerName);
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
 * import { defineHandler, RetryableError, NonRetryableError } from '@amqp-contract/worker';
 * import { fromPromise, Ok } from 'unthrown';
 *
 * const processOrderHandler = defineHandler(
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
 * const calculateHandler = defineHandler(
 *   rpcContract,
 *   'calculate',
 *   ({ payload }) => Ok({ sum: payload.a + payload.b }).toAsync(),
 * );
 * ```
 */
export function defineHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferConsumerHandler<TContract, TName>,
): WorkerInferConsumerHandlerEntry<TContract, TName>;
export function defineHandler<
  TContract extends ContractDefinition,
  TName extends InferConsumerNames<TContract>,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferConsumerHandler<TContract, TName>,
  options: ConsumerOptions,
): WorkerInferConsumerHandlerEntry<TContract, TName>;
export function defineHandler<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferRpcHandler<TContract, TName>,
): WorkerInferRpcHandlerEntry<TContract, TName>;
export function defineHandler<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
>(
  contract: TContract,
  name: TName,
  handler: WorkerInferRpcHandler<TContract, TName>,
  options: ConsumerOptions,
): WorkerInferRpcHandlerEntry<TContract, TName>;
export function defineHandler<
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
 * import { defineHandlers, RetryableError } from '@amqp-contract/worker';
 * import { fromPromise, Ok } from 'unthrown';
 *
 * const handlers = defineHandlers(orderContract, {
 *   processOrder: ({ payload }) =>
 *     fromPromise(
 *       processPayment(payload),
 *       (error) => new RetryableError('Payment failed', error),
 *     ).map(() => undefined),
 *   calculate: ({ payload }) => Ok({ sum: payload.a + payload.b }).toAsync(),
 * });
 * ```
 */
export function defineHandlers<TContract extends ContractDefinition>(
  contract: TContract,
  handlers: WorkerInferHandlers<TContract>,
): WorkerInferHandlers<TContract> {
  validateHandlers(contract, handlers);
  return handlers;
}
