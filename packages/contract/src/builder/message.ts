import type { MessageDefinition } from "../types.js";
import { _internal_assertKnownKeys, _internal_assertStandardSchema } from "./validate.js";
import type { StandardSchemaV1 } from "@standard-schema/spec";

/**
 * Define a message definition with payload and optional headers/metadata.
 *
 * A message definition specifies the schema for message payloads and headers using
 * Standard Schema v1 compatible libraries (Zod, Valibot, ArkType, etc.).
 * The schemas are used for automatic validation when publishing or consuming messages.
 *
 * @param payload - The payload schema (must be Standard Schema v1 compatible)
 * @param options - Optional message metadata
 * @param options.headers - Optional header schema for message headers
 * @param options.summary - Brief description for documentation (used in AsyncAPI generation)
 * @param options.description - Detailed description for documentation (used in AsyncAPI generation)
 * @returns A message definition with inferred types
 *
 * @example
 * ```typescript
 * import { z } from 'zod';
 *
 * const orderMessage = defineMessage(
 *   z.object({
 *     orderId: z.string().uuid(),
 *     customerId: z.string().uuid(),
 *     amount: z.number().positive(),
 *     items: z.array(z.object({
 *       productId: z.string(),
 *       quantity: z.number().int().positive(),
 *     })),
 *   }),
 *   {
 *     summary: 'Order created event',
 *     description: 'Emitted when a new order is created in the system'
 *   }
 * );
 * ```
 */
export function defineMessage<
  TPayload extends MessageDefinition["payload"],
  THeaders extends StandardSchemaV1<Record<string, unknown>> | undefined = undefined,
>(
  payload: TPayload,
  options?: {
    headers?: THeaders;
    summary?: string;
    description?: string;
  },
): MessageDefinition<TPayload, THeaders> {
  _internal_assertStandardSchema("Message payload schema", payload);
  _internal_assertKnownKeys("message", "(anonymous)", options, [
    "headers",
    "summary",
    "description",
  ]);
  if (options?.headers !== undefined) {
    _internal_assertStandardSchema("Message headers schema", options.headers);
  }
  return {
    payload,
    ...options,
  };
}
