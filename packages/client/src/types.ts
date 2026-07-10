import type {
  ContractDefinition,
  InferPublisherNames,
  InferRpcNames,
  MessageDefinition,
  PublisherEntry,
  QueueEntry,
  RpcDefinition,
  RpcErrorMap,
} from "@amqp-contract/contract";
import type { RpcError } from "@amqp-contract/core";
import type { StandardSchemaV1 } from "@standard-schema/spec";

/**
 * Infer the TypeScript type from a schema (input side, used for publish payloads).
 */
type InferSchemaInput<TSchema extends StandardSchemaV1> =
  TSchema extends StandardSchemaV1<infer TInput> ? TInput : never;

/**
 * Infer the TypeScript type from a schema (output side, used for validated responses).
 */
type InferSchemaOutput<TSchema extends StandardSchemaV1> =
  TSchema extends StandardSchemaV1<infer _TInput, infer TOutput> ? TOutput : never;

/**
 * Infer publisher message input type.
 * Works with both PublisherDefinition and EventPublisherConfig since both have
 * a `message` property with a `payload` schema.
 */
type PublisherInferInput<TPublisher extends PublisherEntry> = TPublisher extends {
  message: { payload: StandardSchemaV1 };
}
  ? InferSchemaInput<TPublisher["message"]["payload"]>
  : never;

type InferPublishers<TContract extends ContractDefinition> = NonNullable<TContract["publishers"]>;
type InferPublisher<
  TContract extends ContractDefinition,
  TName extends InferPublisherNames<TContract>,
> = InferPublishers<TContract>[TName];

/**
 * Input type accepted by `client.publish(name, ...)` for a specific publisher.
 */
export type ClientInferPublisherInput<
  TContract extends ContractDefinition,
  TName extends InferPublisherNames<TContract>,
> = PublisherInferInput<InferPublisher<TContract, TName>>;

// =============================================================================
// RPC inference (reads from `contract.rpcs`, not `publishers`)
// =============================================================================

type InferRpcs<TContract extends ContractDefinition> = NonNullable<TContract["rpcs"]>;
type InferRpc<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> = InferRpcs<TContract>[TName];

/**
 * Input type accepted by `client.call(name, request, ...)`.
 */
export type ClientInferRpcRequestInput<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<infer TRequest, MessageDefinition>
    ? TRequest extends MessageDefinition
      ? InferSchemaInput<TRequest["payload"]>
      : never
    : never;

/**
 * Output (validated) response type returned by `client.call(name, ...)`.
 */
export type ClientInferRpcResponseOutput<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<MessageDefinition, infer TResponse>
    ? TResponse extends MessageDefinition
      ? InferSchemaOutput<TResponse["payload"]>
      : never
    : never;

/**
 * Typed error union for `client.call(name, ...)` — one `RpcError<code, data>`
 * member per entry in the RPC's declared `errors` map, with `data` typed as
 * the declared schema's *output* (the client re-validates error data when the
 * reply arrives). Resolves to `never` when the RPC declares no errors, so the
 * call's error union stays purely transport-level.
 */
export type ClientInferRpcErrors<
  TContract extends ContractDefinition,
  TName extends InferRpcNames<TContract>,
> =
  InferRpc<TContract, TName> extends RpcDefinition<
    MessageDefinition,
    MessageDefinition,
    QueueEntry,
    infer TErrors
  >
    ? TErrors extends RpcErrorMap
      ? {
          [K in keyof TErrors & string]: RpcError<K, InferSchemaOutput<TErrors[K]["payload"]>>;
        }[keyof TErrors & string]
      : never
    : never;
