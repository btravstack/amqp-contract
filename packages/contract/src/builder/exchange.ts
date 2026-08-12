import type { BaseExchangeDefinition, ExchangeDefinition } from "../types.js";
import { _internal_assertKnownKeys, _internal_assertNonEmptyName } from "./validate.js";

/** The four AMQP exchange types. */
type ExchangeType = ExchangeDefinition["type"];

/**
 * Define an AMQP exchange.
 *
 * The `type` selects the routing behaviour, and the return type narrows to
 * match it — `defineExchange('tasks', { type: 'direct' })` is a
 * `DirectExchangeDefinition<'tasks'>`, so the builders that require a specific
 * exchange kind (a direct/topic publisher's routing key, a fanout event's
 * bridge) accept or reject it at compile time.
 *
 * - **topic** (the default) routes on routing-key patterns: `*` matches one
 *   word, `#` matches zero or more, words separated by dots.
 * - **direct** routes on an exact routing-key match — point-to-point.
 * - **fanout** broadcasts to every bound queue, ignoring the routing key.
 * - **headers** routes on header values rather than the routing key.
 *
 * @param name - The name of the exchange
 * @param options - Optional exchange configuration
 * @param options.type - Exchange type (defaults to "topic")
 * @param options.durable - If true, the exchange survives broker restarts (default: true)
 * @param options.autoDelete - If true, the exchange is deleted when no queues are bound
 * @param options.internal - If true, the exchange cannot be directly published to
 * @param options.arguments - Additional AMQP arguments for the exchange
 * @returns An exchange definition narrowed to the requested type
 *
 * @example
 * ```typescript
 * const ordersExchange = defineExchange('orders'); // topic — the default
 * const tasksExchange = defineExchange('tasks', { type: 'direct' });
 * const logsExchange = defineExchange('logs', { type: 'fanout' });
 * const routesExchange = defineExchange('routes', { type: 'headers' });
 * ```
 */
export function defineExchange<TName extends string, TType extends ExchangeType = "topic">(
  name: TName,
  options?: { type?: TType } & Omit<BaseExchangeDefinition, "name" | "type">,
): Extract<ExchangeDefinition<TName>, { type: TType }>;

/*
 * Implementation signature of defineExchange — not part of the public overload
 * set; use the type-specific overloads above. (Deliberately a plain comment:
 * a JSDoc `@internal` here makes TypeDoc drop the whole function from the
 * generated API docs.)
 */
export function defineExchange(
  name: string,
  options?: { type?: "topic" | "direct" | "fanout" | "headers" } & Omit<
    BaseExchangeDefinition,
    "name" | "type"
  >,
): ExchangeDefinition {
  _internal_assertNonEmptyName("Exchange", name);
  _internal_assertKnownKeys("exchange", name, options, [
    "type",
    "durable",
    "autoDelete",
    "internal",
    "arguments",
  ]);
  const type = options?.type ?? "topic";
  if (!["topic", "direct", "fanout", "headers"].includes(type)) {
    // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time config error
    throw new Error(
      `Unknown exchange type "${String(type)}" for exchange "${name}". ` +
        "Allowed types: topic, direct, fanout, headers.",
    );
  }
  return {
    name,
    type,
    durable: true,
    ...options,
  };
}
