import type { MessageDefinition, QueueEntry, RpcDefinition, RpcErrorMap } from "../types.js";
import { _internal_assertKnownKeys, _internal_assertStandardSchema } from "./validate.js";

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
 * @param messages.errors - Optional typed error map: error code → message
 *   definition for the error's `data` payload. Declared errors widen the
 *   handler's `Err` channel (return `Err(rpcError(code, data))`) and the
 *   client's `call()` error union; error data is schema-validated on both
 *   sides. Business errors are replied and acked — never retried.
 *
 * @example
 * ```typescript
 * import { defineQueue, defineMessage, defineRpc, defineContract } from '@amqp-contract/contract';
 * import { z } from 'zod';
 *
 * const getOrder = defineRpc(defineQueue('rpc.get-order'), {
 *   request: defineMessage(z.object({ orderId: z.string() })),
 *   response: defineMessage(z.object({ orderId: z.string(), status: z.string() })),
 *   errors: {
 *     ORDER_NOT_FOUND: defineMessage(z.object({ orderId: z.string() })),
 *   },
 * });
 *
 * const contract = defineContract({ rpcs: { getOrder } });
 *
 * // Server (worker): return the response, or a declared typed error
 * //   handlers: {
 * //     getOrder: ({ payload }) =>
 * //       orders.has(payload.orderId)
 * //         ? OkAsync(orders.get(payload.orderId))
 * //         : ErrAsync(rpcError('ORDER_NOT_FOUND', { orderId: payload.orderId })),
 * //   }
 *
 * // Client: typed call — the error union includes RpcError<'ORDER_NOT_FOUND', { orderId: string }>
 * //   const result = await client.call('getOrder', { orderId: '42' }, { timeoutMs: 5_000 });
 * //   if (result.isErr() && isRpcError(result.error)) console.log(result.error.code);
 * ```
 */
export function defineRpc<
  TRequestMessage extends MessageDefinition,
  TResponseMessage extends MessageDefinition,
  TQueue extends QueueEntry,
  TErrors extends RpcErrorMap | undefined = undefined,
>(
  queue: TQueue,
  messages: { request: TRequestMessage; response: TResponseMessage; errors?: TErrors },
): RpcDefinition<TRequestMessage, TResponseMessage, TQueue, TErrors> {
  _internal_assertKnownKeys("RPC", "(anonymous)", messages, ["request", "response", "errors"]);
  _internal_assertStandardSchema("RPC request payload schema", messages.request?.payload);
  _internal_assertStandardSchema("RPC response payload schema", messages.response?.payload);
  if (messages.errors !== undefined) {
    for (const [code, definition] of Object.entries(messages.errors)) {
      _internal_assertStandardSchema(
        `RPC error "${code}" data schema`,
        (definition as { payload?: unknown } | undefined)?.payload,
      );
    }
  }
  return {
    queue,
    request: messages.request,
    response: messages.response,
    ...(messages.errors !== undefined && { errors: messages.errors }),
  };
}
