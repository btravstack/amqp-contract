import type { MessageDefinition, QueueEntry, RpcDefinition } from "../types.js";

/**
 * Define an RPC operation: a request/response pair flowing over a request
 * queue with replies routed back via RabbitMQ direct reply-to.
 *
 * RPC is bidirectional on both ends — the worker handler consumes the request
 * and produces the response; `client.call(name, request, options)` publishes
 * the request and awaits the typed response. Both sides share the same
 * definition, so request and response schemas cannot drift between them.
 *
 * Plug the result into `defineContract({ rpcs: { name: ... } })`. RPCs do not
 * appear in `publishers` or `consumers`.
 *
 * @param queue - The queue that receives RPC requests. The queue name is
 *   used as the routing key on the AMQP default direct exchange.
 * @param messages.request - Schema validated against incoming request payloads
 *   (server side) and outgoing requests (client side).
 * @param messages.response - Schema validated against handler return values
 *   (server side) and incoming replies (client side).
 *
 * @example
 * ```typescript
 * import { defineQueue, defineMessage, defineRpc, defineContract } from '@amqp-contract/contract';
 * import { z } from 'zod';
 *
 * const calculate = defineRpc(defineQueue('rpc.calculate'), {
 *   request: defineMessage(z.object({ a: z.number(), b: z.number() })),
 *   response: defineMessage(z.object({ sum: z.number() })),
 * });
 *
 * const contract = defineContract({ rpcs: { calculate } });
 *
 * // Server (worker): handler returns the typed response
 * //   handlers: { calculate: ({ payload }) => Ok({ sum: payload.a + payload.b }).toAsync() }
 *
 * // Client: typed call with required timeout
 * //   const result = await client.call('calculate', { a: 1, b: 2 }, { timeoutMs: 5_000 });
 * ```
 */
export function defineRpc<
  TRequestMessage extends MessageDefinition,
  TResponseMessage extends MessageDefinition,
  TQueue extends QueueEntry,
>(
  queue: TQueue,
  messages: { request: TRequestMessage; response: TResponseMessage },
): RpcDefinition<TRequestMessage, TResponseMessage, TQueue> {
  return {
    queue,
    request: messages.request,
    response: messages.response,
  };
}
