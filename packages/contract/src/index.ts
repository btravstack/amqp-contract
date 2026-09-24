export {
  defineCommandConsumer,
  defineCommandPublisher,
  defineConsumer,
  defineContract,
  defineDeadLetterQueue,
  defineEventConsumer,
  defineEventPublisher,
  defineExchangeBinding,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueueBinding,
  defineQueue,
  defineRpc,
} from "./builder/index.js";
import {
  deriveTtlBackoffInfrastructure as _deriveTtlBackoffInfrastructure,
  extractConsumer as _extractConsumer,
  isBridgedPublisherConfig as _isBridgedPublisherConfig,
  isCommandConsumerConfig as _isCommandConsumerConfig,
  isEventConsumerResult as _isEventConsumerResult,
  isEventPublisherConfig as _isEventPublisherConfig,
  ttlBackoffBaseDelay as _ttlBackoffBaseDelay,
  ttlBackoffWaitQueueName as _ttlBackoffWaitQueueName,
} from "./builder/index.js";

// Runtime helpers shared with core / worker / asyncapi, not part of the
// contract-authoring API. They live on `@amqp-contract/contract/internal`;
// these root aliases remain only until the sibling packages switch imports.

/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const deriveTtlBackoffInfrastructure = _deriveTtlBackoffInfrastructure;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const extractConsumer = _extractConsumer;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const isBridgedPublisherConfig = _isBridgedPublisherConfig;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const isCommandConsumerConfig = _isCommandConsumerConfig;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const isEventConsumerResult = _isEventConsumerResult;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const isEventPublisherConfig = _isEventPublisherConfig;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const ttlBackoffBaseDelay = _ttlBackoffBaseDelay;
/** @deprecated Import from "@amqp-contract/contract/internal" — an internal helper with no semver guarantee. */
export const ttlBackoffWaitQueueName = _ttlBackoffWaitQueueName;
export type {
  BindingPattern,
  BridgedPublisherConfig,
  CommandConsumerConfig,
  DeadLetterQueue,
  EventConsumerResult,
  EventPublisherConfig,
  MatchingBindingPattern,
  MatchingRoutingKey,
  RoutableRoutingKey,
  RoutingKey,
} from "./builder/index.js";
export { formatIssue, summarizeIssues } from "./issues.js";
export type {
  AnySchema,
  BaseExchangeDefinition,
  BindingDefinition,
  BridgedPublisherConfigBase,
  ClassicQueueDefinition,
  ClassicQueueOptions,
  CommandConsumerConfigBase,
  CompressionAlgorithm,
  ConsumerDefinition,
  ConsumerEntry,
  ContractDefinition,
  ContractDefinitionInput,
  ContractOutput,
  DeadLetterConfig,
  DefineQueueOptions,
  DefineQueueOptionsWithDeadLetterExchange,
  DirectExchangeDefinition,
  EventConsumerResultBase,
  EventPublisherConfigBase,
  ExchangeBindingDefinition,
  ExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  InferConsumerNames,
  InferPublisherNames,
  InferRpcNames,
  InferSchemaInput,
  InferSchemaOutput,
  MessageDefinition,
  NoneRetryOptions,
  PublisherDefinition,
  PublisherEntry,
  QueueBindingDefinition,
  QueueDefinition,
  QueueDefinitionWithDeadLetterExchange,
  QueueType,
  ImmediateRequeueRetryOptions,
  QuorumQueueDefinition,
  QuorumQueueOptions,
  ResolvedRetryOptions,
  ResolvedTtlBackoffRetryOptions,
  ResolvedImmediateRequeueRetryOptions,
  RetryOptions,
  RpcDefinition,
  RpcErrorDefinition,
  RpcErrorMap,
  TopicExchangeDefinition,
  TtlBackoffInfrastructure,
  TtlBackoffRetryOptions,
  TtlBackoffWaitQueueDefinition,
} from "./types.js";
