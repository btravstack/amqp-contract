/**
 * Call-shape and inference guard for the event/command builders.
 *
 * These builders used to be overload sets split by exchange type, so a
 * mistake like `defineEventPublisher(directExchange, message)` (routingKey
 * forgotten) was reported against whichever overload matched the arity —
 * "DirectExchangeDefinition is not assignable to FanoutExchangeDefinition |
 * HeadersExchangeDefinition" — never mentioning the routing key. Each is now a
 * single signature whose options parameter is chosen by the exchange type, so
 * the error lands on the options.
 *
 * Every valid call shape below must keep its exact inferred type: downstream
 * inference (`defineContract`, the client and worker `Infer*` helpers) reads
 * these return types. The diagnostic wording itself is asserted in
 * `event-command-diagnostics.spec.ts`.
 */
import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";

import type {
  DirectExchangeDefinition,
  ExchangeBindingDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  QueueDefinition,
  QueueDefinitionWithDeadLetterExchange,
  TopicExchangeDefinition,
} from "../types.js";
import {
  type BridgedPublisherConfig,
  type CommandConsumerConfig,
  defineCommandConsumer,
  defineCommandPublisher,
  type EventConsumerResult,
  type EventPublisherConfig,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "./index.js";

const direct = defineExchange("tasks", { type: "direct" });
const topic = defineExchange("orders");
const fanout = defineExchange("logs", { type: "fanout" });
const headers = defineExchange("routes", { type: "headers" });
const fanoutBridge = defineExchange("logs-bridge", { type: "fanout" });
const headersBridge = defineExchange("routes-bridge", { type: "headers" });
const directBridge = defineExchange("tasks-bridge", { type: "direct" });
const topicBridge = defineExchange("orders-bridge");
const dlx = defineExchange("dlx");
const message = defineMessage(z.object({ id: z.string() }));
type M = typeof message;
const queue = defineQueue("q");
type Q = QueueDefinition<"q">;
const dlxQueue = defineQueue("dlx-q", { deadLetter: { exchange: dlx } });

const fanoutEvent = defineEventPublisher(fanout, message);
const headersEvent = defineEventPublisher(headers, message);
const directEvent = defineEventPublisher(direct, message, { routingKey: "task.run" });
const topicEvent = defineEventPublisher(topic, message, { routingKey: "order.created" });

const fanoutCommand = defineCommandConsumer(queue, fanout, message);
const directCommand = defineCommandConsumer(queue, direct, message, { routingKey: "t.run" });
const topicCommand = defineCommandConsumer(queue, topic, message, { routingKey: "order.*" });

describe("defineEventPublisher", () => {
  test("keyless exchanges take no routing key", () => {
    expectTypeOf(fanoutEvent).toEqualTypeOf<
      EventPublisherConfig<M, FanoutExchangeDefinition<"logs">, undefined>
    >();
    expectTypeOf(headersEvent).toEqualTypeOf<
      EventPublisherConfig<M, HeadersExchangeDefinition<"routes">, undefined>
    >();
    expectTypeOf(
      defineEventPublisher(fanout, message, { bindingArguments: {}, externalConsumers: true }),
    ).toEqualTypeOf<EventPublisherConfig<M, FanoutExchangeDefinition<"logs">, undefined>>();
  });

  test("direct and topic exchanges infer the literal routing key", () => {
    expectTypeOf(directEvent).toEqualTypeOf<
      EventPublisherConfig<M, DirectExchangeDefinition<"tasks">, "task.run">
    >();
    expectTypeOf(
      defineEventPublisher(topic, message, {
        routingKey: "order.created",
        externalConsumers: true,
      }),
    ).toEqualTypeOf<EventPublisherConfig<M, TopicExchangeDefinition<"orders">, "order.created">>();
  });

  test("rejects the mistakes", () => {
    // @ts-expect-error — a direct exchange needs a routingKey
    defineEventPublisher(direct, message);
    // @ts-expect-error — a topic exchange needs a routingKey
    defineEventPublisher(topic, message, {});
    // @ts-expect-error — a fanout exchange ignores the routing key
    defineEventPublisher(fanout, message, { routingKey: "x" });
    // @ts-expect-error — a routing key may not contain wildcards
    defineEventPublisher(topic, message, { routingKey: "order.*" });
    // @ts-expect-error — a routing key may not be empty
    defineEventPublisher(direct, message, { routingKey: "" });
  });
});

describe("defineEventConsumer", () => {
  test("unbridged consumers of every exchange type", () => {
    expectTypeOf(defineEventConsumer(fanoutEvent, queue)).toEqualTypeOf<
      EventConsumerResult<M, FanoutExchangeDefinition<"logs">, Q>
    >();
    expectTypeOf(defineEventConsumer(headersEvent, queue, { arguments: {} })).toEqualTypeOf<
      EventConsumerResult<M, HeadersExchangeDefinition<"routes">, Q>
    >();
    expectTypeOf(defineEventConsumer(directEvent, queue)).toEqualTypeOf<
      EventConsumerResult<M, DirectExchangeDefinition<"tasks">, Q>
    >();
    expectTypeOf(defineEventConsumer(topicEvent, queue)).toEqualTypeOf<
      EventConsumerResult<M, TopicExchangeDefinition<"orders">, Q>
    >();
    expectTypeOf(defineEventConsumer(topicEvent, queue, { routingKey: "order.*" })).toEqualTypeOf<
      EventConsumerResult<M, TopicExchangeDefinition<"orders">, Q>
    >();
  });

  test("keeps a dead-letter queue's precise type", () => {
    expectTypeOf(defineEventConsumer(topicEvent, dlxQueue)).toEqualTypeOf<
      EventConsumerResult<
        M,
        TopicExchangeDefinition<"orders">,
        QueueDefinitionWithDeadLetterExchange<"dlx-q", TopicExchangeDefinition<"dlx">>
      >
    >();
  });

  test("bridged consumers carry the bridge", () => {
    expectTypeOf(
      defineEventConsumer(fanoutEvent, queue, { bridgeExchange: fanoutBridge }),
    ).toEqualTypeOf<
      EventConsumerResult<
        M,
        FanoutExchangeDefinition<"logs">,
        Q,
        ExchangeBindingDefinition,
        FanoutExchangeDefinition<"logs-bridge">
      >
    >();
    expectTypeOf(
      defineEventConsumer(headersEvent, queue, { bridgeExchange: headersBridge, arguments: {} }),
    ).toEqualTypeOf<
      EventConsumerResult<
        M,
        HeadersExchangeDefinition<"routes">,
        Q,
        ExchangeBindingDefinition,
        HeadersExchangeDefinition<"routes-bridge">
      >
    >();
    expectTypeOf(
      defineEventConsumer(directEvent, queue, { bridgeExchange: topicBridge }),
    ).toEqualTypeOf<
      EventConsumerResult<
        M,
        DirectExchangeDefinition<"tasks">,
        Q,
        ExchangeBindingDefinition,
        TopicExchangeDefinition<"orders-bridge">
      >
    >();
    expectTypeOf(
      defineEventConsumer(topicEvent, queue, {
        bridgeExchange: directBridge,
        routingKey: "order.*",
      }),
    ).toEqualTypeOf<
      EventConsumerResult<
        M,
        TopicExchangeDefinition<"orders">,
        Q,
        ExchangeBindingDefinition,
        DirectExchangeDefinition<"tasks-bridge">
      >
    >();
  });

  test("rejects the mistakes", () => {
    // @ts-expect-error — a fanout event has no routing key to override
    defineEventConsumer(fanoutEvent, queue, { routingKey: "x" });
    // @ts-expect-error — a direct event's key cannot be overridden
    defineEventConsumer(directEvent, queue, { routingKey: "task.*" });
    // @ts-expect-error — a fanout source needs a fanout bridge
    defineEventConsumer(fanoutEvent, queue, { bridgeExchange: topicBridge });
    // @ts-expect-error — a headers source needs a headers bridge
    defineEventConsumer(headersEvent, queue, { bridgeExchange: fanoutBridge });
    // @ts-expect-error — a topic source needs a direct or topic bridge
    defineEventConsumer(topicEvent, queue, { bridgeExchange: fanoutBridge });
  });
});

describe("defineCommandConsumer", () => {
  test("infers the exchange, binding key and queue", () => {
    expectTypeOf(fanoutCommand).toEqualTypeOf<
      CommandConsumerConfig<M, FanoutExchangeDefinition<"logs">, undefined, Q>
    >();
    expectTypeOf(defineCommandConsumer(queue, headers, message, { arguments: {} })).toEqualTypeOf<
      CommandConsumerConfig<M, HeadersExchangeDefinition<"routes">, undefined, Q>
    >();
    expectTypeOf(directCommand).toEqualTypeOf<
      CommandConsumerConfig<M, DirectExchangeDefinition<"tasks">, "t.run", Q>
    >();
    expectTypeOf(topicCommand).toEqualTypeOf<
      CommandConsumerConfig<M, TopicExchangeDefinition<"orders">, "order.*", Q>
    >();
  });

  test("rejects the mistakes", () => {
    // @ts-expect-error — a direct exchange needs a routingKey
    defineCommandConsumer(queue, direct, message);
    // @ts-expect-error — a topic exchange needs a routingKey
    defineCommandConsumer(queue, topic, message, { arguments: {} });
    // @ts-expect-error — a fanout exchange ignores the routing key
    defineCommandConsumer(queue, fanout, message, { routingKey: "x" });
    // @ts-expect-error — a direct binding key may not contain wildcards
    defineCommandConsumer(queue, direct, message, { routingKey: "t.*" });
    // @ts-expect-error — a binding key may not be empty
    defineCommandConsumer(queue, topic, message, { routingKey: "" });
  });
});

describe("defineCommandPublisher", () => {
  test("unbridged publishers", () => {
    expectTypeOf(defineCommandPublisher(fanoutCommand)).toEqualTypeOf<{
      message: M;
      exchange: FanoutExchangeDefinition<"logs">;
      externalConsumers?: boolean;
    }>();
    expectTypeOf(defineCommandPublisher(directCommand, { externalConsumers: true })).toEqualTypeOf<{
      message: M;
      exchange: DirectExchangeDefinition;
      routingKey: "t.run";
      externalConsumers?: boolean;
    }>();
    expectTypeOf(defineCommandPublisher(topicCommand)).toEqualTypeOf<{
      message: M;
      exchange: TopicExchangeDefinition;
      routingKey: "order.*";
      externalConsumers?: boolean;
    }>();
    expectTypeOf(
      defineCommandPublisher(topicCommand, { routingKey: "order.create" }),
    ).toEqualTypeOf<{
      message: M;
      exchange: TopicExchangeDefinition;
      routingKey: "order.create";
      externalConsumers?: boolean;
    }>();
  });

  test("bridged publishers", () => {
    expectTypeOf(
      defineCommandPublisher(fanoutCommand, { bridgeExchange: fanoutBridge }),
    ).toEqualTypeOf<
      BridgedPublisherConfig<
        M,
        FanoutExchangeDefinition<"logs-bridge">,
        FanoutExchangeDefinition<"logs">
      >
    >();
    expectTypeOf(
      defineCommandPublisher(directCommand, {
        bridgeExchange: topicBridge,
        externalConsumers: true,
      }),
    ).toEqualTypeOf<
      BridgedPublisherConfig<
        M,
        TopicExchangeDefinition<"orders-bridge">,
        DirectExchangeDefinition<"tasks">
      >
    >();
    expectTypeOf(
      defineCommandPublisher(topicCommand, {
        bridgeExchange: directBridge,
        routingKey: "order.create",
      }),
    ).toEqualTypeOf<
      BridgedPublisherConfig<
        M,
        DirectExchangeDefinition<"tasks-bridge">,
        TopicExchangeDefinition<"orders">
      >
    >();
  });

  test("rejects the mistakes", () => {
    // @ts-expect-error — a direct command's key cannot be overridden
    defineCommandPublisher(directCommand, { routingKey: "t.other" });
    // @ts-expect-error — a fanout command has no routing key
    defineCommandPublisher(fanoutCommand, { routingKey: "x" });
    // @ts-expect-error — a routing key may not contain wildcards
    defineCommandPublisher(topicCommand, { routingKey: "order.*" });
    // @ts-expect-error — a fanout target needs a fanout bridge
    defineCommandPublisher(fanoutCommand, { bridgeExchange: directBridge });
  });
});
