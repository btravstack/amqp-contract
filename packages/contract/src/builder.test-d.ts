/**
 * Type tests for routing key and binding pattern validation using Vitest
 * These tests ensure that the type system correctly validates routing keys and patterns
 */

import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";
import type { BindingPattern, MatchingRoutingKey, RoutingKey } from "./builder.js";
import {
  defineCommandConsumer,
  defineCommandPublisher,
  defineConsumer,
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
  defineRpc,
} from "./builder.js";
import type {
  ConsumerDefinition,
  DirectExchangeDefinition,
  FanoutExchangeDefinition,
  HeadersExchangeDefinition,
  PublisherDefinition,
  TopicExchangeDefinition,
} from "./types.js";

describe("RoutingKey type validation", () => {
  test("should accept valid routing keys", () => {
    expectTypeOf<RoutingKey<"order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutingKey<"user-profile.updated">>().toEqualTypeOf<"user-profile.updated">();
    expectTypeOf<
      RoutingKey<"system_event.notification">
    >().toEqualTypeOf<"system_event.notification">();
    expectTypeOf<RoutingKey<"a">>().toEqualTypeOf<"a">();
    expectTypeOf<RoutingKey<"ABC123">>().toEqualTypeOf<"ABC123">();
  });

  test("should reject routing keys with wildcards", () => {
    // * wildcard is not allowed in routing keys
    expectTypeOf<RoutingKey<"order.*">>().toEqualTypeOf<never>();

    // # wildcard is not allowed in routing keys
    expectTypeOf<RoutingKey<"order.#">>().toEqualTypeOf<never>();

    // wildcards in the middle not allowed
    expectTypeOf<RoutingKey<"order.*.created">>().toEqualTypeOf<never>();
  });

  test("should reject empty routing keys", () => {
    // empty is not allowed
    expectTypeOf<RoutingKey<"">>().toEqualTypeOf<never>();
  });
});

describe("BindingPattern type validation", () => {
  test("should accept valid binding patterns with wildcards", () => {
    expectTypeOf<BindingPattern<"order.*">>().toEqualTypeOf<"order.*">();
    expectTypeOf<BindingPattern<"order.#">>().toEqualTypeOf<"order.#">();
    expectTypeOf<BindingPattern<"*.created">>().toEqualTypeOf<"*.created">();
    expectTypeOf<BindingPattern<"#">>().toEqualTypeOf<"#">();
    expectTypeOf<BindingPattern<"*">>().toEqualTypeOf<"*">();
    expectTypeOf<BindingPattern<"order.*.urgent">>().toEqualTypeOf<"order.*.urgent">();
    expectTypeOf<BindingPattern<"order.#.completed">>().toEqualTypeOf<"order.#.completed">();
  });

  test("should accept exact match patterns (concrete routing keys)", () => {
    expectTypeOf<BindingPattern<"order.created">>().toEqualTypeOf<"order.created">();
  });

  test("should reject empty binding patterns", () => {
    // empty is not allowed
    expectTypeOf<BindingPattern<"">>().toEqualTypeOf<never>();
  });
});

describe("MatchingRoutingKey pattern matching", () => {
  test("should match valid routing keys against patterns with * wildcard", () => {
    // * matches exactly one word
    expectTypeOf<MatchingRoutingKey<"order.*", "order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<
      MatchingRoutingKey<"*.created", "order.created">
    >().toEqualTypeOf<"order.created">();
  });

  test("should match valid routing keys against patterns with # wildcard", () => {
    // # matches zero or more words
    expectTypeOf<MatchingRoutingKey<"order.#", "order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<
      MatchingRoutingKey<"order.#", "order.created.urgent">
    >().toEqualTypeOf<"order.created.urgent">();
  });

  test("should match exact routing keys", () => {
    expectTypeOf<
      MatchingRoutingKey<"order.created", "order.created">
    >().toEqualTypeOf<"order.created">();
  });

  test("should reject non-matching routing keys", () => {
    // Wrong prefix
    expectTypeOf<MatchingRoutingKey<"order.*", "user.created">>().toEqualTypeOf<never>();

    // * matches only one word, not multiple
    expectTypeOf<MatchingRoutingKey<"order.*", "order.created.urgent">>().toEqualTypeOf<never>();

    // Wrong suffix
    expectTypeOf<MatchingRoutingKey<"*.created", "order.updated">>().toEqualTypeOf<never>();
  });

  test("should handle # wildcard in the middle of patterns", () => {
    // # matches zero segments
    expectTypeOf<
      MatchingRoutingKey<"order.#.completed", "order.completed">
    >().toEqualTypeOf<"order.completed">();

    // # matches one segment
    expectTypeOf<
      MatchingRoutingKey<"order.#.completed", "order.created.completed">
    >().toEqualTypeOf<"order.created.completed">();

    // # matches two segments
    expectTypeOf<
      MatchingRoutingKey<"order.#.completed", "order.created.urgent.completed">
    >().toEqualTypeOf<"order.created.urgent.completed">();
  });

  test("should reject when # pattern does not match suffix", () => {
    // Missing .completed suffix
    expectTypeOf<MatchingRoutingKey<"order.#.completed", "order.created">>().toEqualTypeOf<never>();

    // Wrong prefix
    expectTypeOf<
      MatchingRoutingKey<"order.#.completed", "user.completed">
    >().toEqualTypeOf<never>();
  });
});

describe("Publisher and Consumer factory types", () => {
  test("defineEventPublisher with direct exchange should accept valid routing keys", () => {
    // Test that the publisher factory method accepts RoutingKey validated routing keys
    // The actual runtime validation will be tested in integration tests
    expectTypeOf<RoutingKey<"order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutingKey<"user-profile.updated">>().toEqualTypeOf<"user-profile.updated">();
  });

  test("defineEventPublisher with topic exchange should accept valid routing keys", () => {
    // Topic exchange routing keys must be valid RoutingKey types
    expectTypeOf<RoutingKey<"order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<RoutingKey<"order.*.urgent">>().not.toEqualTypeOf<"order.*.urgent">(); // Wildcards not allowed in routing keys
  });

  test("defineCommandConsumer with topic exchange should accept valid binding patterns", () => {
    // Topic exchange binding patterns can include wildcards
    expectTypeOf<BindingPattern<"order.*">>().toEqualTypeOf<"order.*">();
    expectTypeOf<BindingPattern<"order.#">>().toEqualTypeOf<"order.#">();
    expectTypeOf<BindingPattern<"order.created">>().toEqualTypeOf<"order.created">();
  });

  test("defineCommandPublisher should accept routing keys matching the consumer pattern", () => {
    // When consumer binding is "order.*", publisher can use any key matching that pattern
    // This is tested via MatchingRoutingKey type
    expectTypeOf<MatchingRoutingKey<"order.*", "order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<MatchingRoutingKey<"order.*", "order.updated">>().toEqualTypeOf<"order.updated">();
    expectTypeOf<MatchingRoutingKey<"order.*", "order.deleted">>().toEqualTypeOf<"order.deleted">();
  });

  test("defineEventConsumer should accept binding patterns for topic exchanges", () => {
    // When publisher uses "order.created", consumer can bind with patterns
    expectTypeOf<BindingPattern<"order.*">>().toEqualTypeOf<"order.*">();
    expectTypeOf<BindingPattern<"order.#">>().toEqualTypeOf<"order.#">();
    expectTypeOf<BindingPattern<"#">>().toEqualTypeOf<"#">();
  });

  test("routing keys must not contain wildcards", () => {
    // Routing keys cannot have * or # - these are only for binding patterns
    expectTypeOf<RoutingKey<"order.*">>().toEqualTypeOf<never>();
    expectTypeOf<RoutingKey<"order.#">>().toEqualTypeOf<never>();
  });

  test("binding patterns can be concrete keys or patterns", () => {
    // BindingPattern accepts both concrete keys and patterns with wildcards
    expectTypeOf<BindingPattern<"order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<BindingPattern<"order.*">>().toEqualTypeOf<"order.*">();
    expectTypeOf<BindingPattern<"order.#">>().toEqualTypeOf<"order.#">();
  });
});

// ---------------------------------------------------------------------------
// ContractOutput type inference
// ---------------------------------------------------------------------------

describe("ContractOutput type inference", () => {
  const ordersExchange = defineExchange("orders");
  const dlx = defineExchange("orders-dlx", { type: "direct" });
  const fanoutExchange = defineExchange("notifications", { type: "fanout" });
  const headersExchange = defineExchange("logs", { type: "headers" });
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx },
    retry: { mode: "immediate-requeue", maxRetries: 3 },
  });
  const notificationQueue = defineQueue("notifications");
  const logQueue = defineQueue("logs");
  const orderMessage = defineMessage(z.object({ orderId: z.string() }));
  const notificationMessage = defineMessage(z.object({ text: z.string() }));
  const logMessage = defineMessage(z.object({ level: z.string() }));

  test("should extract exchanges from EventPublisherConfig in publishers", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
    });

    expectTypeOf(contract.exchanges).toHaveProperty("orders");
  });

  test("should extract queues and binding exchanges from EventConsumerResult", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
      consumers: {
        processOrder: defineEventConsumer(orderCreated, orderQueue),
      },
    });

    expectTypeOf(contract.queues).toHaveProperty("order-processing");
    expectTypeOf(contract.exchanges).toHaveProperty("orders");
    expectTypeOf(contract.bindings).toHaveProperty("processOrderBinding");
  });

  test("should extract DLX exchanges from consumer queue deadLetter", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
      consumers: {
        processOrder: defineEventConsumer(orderCreated, orderQueue),
      },
    });

    // DLX should be auto-extracted into exchanges
    expectTypeOf(contract.exchanges).toHaveProperty("orders-dlx");
  });

  test("should extract CommandConsumerConfig into consumer + binding + exchange", () => {
    const processCommand = defineCommandConsumer(orderQueue, ordersExchange, orderMessage, {
      routingKey: "order.process",
    });
    const contract = defineContract({
      consumers: { processCommand },
    });

    expectTypeOf(contract.consumers).toHaveProperty("processCommand");
    expectTypeOf(contract.consumers.processCommand).toMatchTypeOf<ConsumerDefinition>();
    expectTypeOf(contract.bindings).toHaveProperty("processCommandBinding");
    expectTypeOf(contract.exchanges).toHaveProperty("orders");
    // DLX from queue's deadLetter
    expectTypeOf(contract.exchanges).toHaveProperty("orders-dlx");
  });

  test("should normalize EventPublisherConfig to PublisherDefinition", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
    });

    expectTypeOf(contract.publishers).toHaveProperty("orderCreated");
    expectTypeOf(contract.publishers.orderCreated).toMatchTypeOf<PublisherDefinition>();
  });

  test("should handle plain ConsumerDefinition without generating binding", () => {
    const contract = defineContract({
      consumers: {
        plainConsumer: defineConsumer(notificationQueue, notificationMessage),
      },
    });

    expectTypeOf(contract.consumers).toHaveProperty("plainConsumer");
    expectTypeOf(contract.consumers.plainConsumer).toMatchTypeOf<ConsumerDefinition>();
    expectTypeOf(contract.queues).toHaveProperty("notifications");
    // Plain consumers don't generate bindings
    expectTypeOf(contract.bindings).not.toHaveProperty("plainConsumerBinding");
  });

  test("should handle mixed publisher patterns", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const processCommand = defineCommandConsumer(orderQueue, ordersExchange, orderMessage, {
      routingKey: "order.process",
    });
    const sendCommand = defineCommandPublisher(processCommand);
    const contract = defineContract({
      publishers: {
        orderCreated,
        sendCommand,
        directPublisher: definePublisher(fanoutExchange, notificationMessage),
      },
      consumers: {
        processCommand,
      },
    });

    // All three publisher types present
    expectTypeOf(contract.publishers).toHaveProperty("orderCreated");
    expectTypeOf(contract.publishers).toHaveProperty("sendCommand");
    expectTypeOf(contract.publishers).toHaveProperty("directPublisher");

    // Exchanges from all sources
    expectTypeOf(contract.exchanges).toHaveProperty("orders");
    expectTypeOf(contract.exchanges).toHaveProperty("notifications");
    expectTypeOf(contract.exchanges).toHaveProperty("orders-dlx");
  });

  test("should handle empty contract", () => {
    const contract = defineContract({});

    expectTypeOf(contract.exchanges).toEqualTypeOf<{}>();
    expectTypeOf(contract.queues).toEqualTypeOf<{}>();
    expectTypeOf(contract.bindings).toEqualTypeOf<{}>();
    expectTypeOf(contract.publishers).toEqualTypeOf<{}>();
    expectTypeOf(contract.consumers).toEqualTypeOf<{}>();
  });

  test("should handle fanout exchange without routing key", () => {
    const broadcast = defineEventPublisher(fanoutExchange, notificationMessage);
    const contract = defineContract({
      publishers: { broadcast },
      consumers: {
        receiveNotif: defineEventConsumer(broadcast, notificationQueue),
      },
    });

    expectTypeOf(contract.exchanges).toHaveProperty("notifications");
    expectTypeOf(contract.queues).toHaveProperty("notifications");
    expectTypeOf(contract.bindings).toHaveProperty("receiveNotifBinding");
  });

  test("should handle headers exchange without routing key", () => {
    const logEvent = defineEventPublisher(headersExchange, logMessage);
    const contract = defineContract({
      publishers: { logEvent },
      consumers: {
        receiveLog: defineEventConsumer(logEvent, logQueue),
      },
    });

    expectTypeOf(contract.exchanges).toHaveProperty("logs");
    expectTypeOf(contract.queues).toHaveProperty("logs");
    expectTypeOf(contract.bindings).toHaveProperty("receiveLogBinding");
  });
});

// ---------------------------------------------------------------------------
// ContractOutput strict literal key inference (issue #347)
// ---------------------------------------------------------------------------

describe("ContractOutput strict literal keys", () => {
  const ordersExchange = defineExchange("orders");
  const dlx = defineExchange("orders-dlx", { type: "direct" });
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx },
    retry: { mode: "immediate-requeue", maxRetries: 3 },
  });
  const orderMessage = defineMessage(z.object({ orderId: z.string() }));

  test("exchange keys should be literal string types, not string", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
      consumers: {
        processOrder: defineEventConsumer(orderCreated, orderQueue),
      },
    });

    // Exchange keys should be literal union, not string
    expectTypeOf<keyof typeof contract.exchanges>().toEqualTypeOf<"orders" | "orders-dlx">();
  });

  test("queue keys should be literal string types, not string", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
      consumers: {
        processOrder: defineEventConsumer(orderCreated, orderQueue),
      },
    });

    // Queue keys should be literal, not string
    expectTypeOf<keyof typeof contract.queues>().toEqualTypeOf<"order-processing">();
  });

  test("exchange type discriminator should be narrowed", () => {
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
    });
    const contract = defineContract({
      publishers: { orderCreated },
      consumers: {
        processOrder: defineEventConsumer(orderCreated, orderQueue),
      },
    });

    expectTypeOf(contract.exchanges.orders).toMatchTypeOf<TopicExchangeDefinition>();
    expectTypeOf(contract.exchanges["orders-dlx"]).toMatchTypeOf<DirectExchangeDefinition>();
  });

  test("command consumer should preserve queue and exchange literal types", () => {
    const processCommand = defineCommandConsumer(orderQueue, ordersExchange, orderMessage, {
      routingKey: "order.process",
    });
    const contract = defineContract({
      consumers: { processCommand },
    });

    // Exchange key should be literal
    expectTypeOf<keyof typeof contract.exchanges>().toEqualTypeOf<"orders" | "orders-dlx">();

    // Queue key should be literal
    expectTypeOf<keyof typeof contract.queues>().toEqualTypeOf<"order-processing">();
  });

  test("fanout exchange should preserve literal name", () => {
    const fanoutExchange = defineExchange("notifications", { type: "fanout" });
    const notifMessage = defineMessage(z.object({ text: z.string() }));
    const notifQueue = defineQueue("notifications");
    const broadcast = defineEventPublisher(fanoutExchange, notifMessage);
    const contract = defineContract({
      publishers: { broadcast },
      consumers: {
        receiveNotif: defineEventConsumer(broadcast, notifQueue),
      },
    });

    expectTypeOf(contract.exchanges.notifications).toMatchTypeOf<FanoutExchangeDefinition>();
    expectTypeOf<keyof typeof contract.queues>().toEqualTypeOf<"notifications">();
  });

  test("headers exchange should preserve literal name", () => {
    const headersExchange = defineExchange("logs", { type: "headers" });
    const logMessage = defineMessage(z.object({ level: z.string() }));
    const logQueue = defineQueue("logs");
    const logEvent = defineEventPublisher(headersExchange, logMessage);
    const contract = defineContract({
      publishers: { logEvent },
      consumers: {
        receiveLog: defineEventConsumer(logEvent, logQueue),
      },
    });

    expectTypeOf(contract.exchanges.logs).toMatchTypeOf<HeadersExchangeDefinition>();
    expectTypeOf<keyof typeof contract.queues>().toEqualTypeOf<"logs">();
  });
});

describe("defineRpc typed errors", () => {
  const queue = defineQueue("rpc.orders", { type: "classic", durable: false });
  const request = defineMessage(z.object({ orderId: z.string() }));
  const response = defineMessage(z.object({ status: z.string() }));

  test("captures the declared error map type on the definition", () => {
    const notFound = defineMessage(z.object({ orderId: z.string() }));
    const rpc = defineRpc(queue, { request, response, errors: { ORDER_NOT_FOUND: notFound } });

    expectTypeOf(rpc.errors).toEqualTypeOf<{ ORDER_NOT_FOUND: typeof notFound } | undefined>();
    expectTypeOf<keyof NonNullable<typeof rpc.errors>>().toEqualTypeOf<"ORDER_NOT_FOUND">();
  });

  test("errors type is undefined when no errors are declared", () => {
    const rpc = defineRpc(queue, { request, response });

    expectTypeOf(rpc.errors).toEqualTypeOf<undefined>();
  });

  test("error map survives defineContract", () => {
    const notFound = defineMessage(z.object({ orderId: z.string() }));
    const getOrder = defineRpc(queue, { request, response, errors: { ORDER_NOT_FOUND: notFound } });
    const contract = defineContract({ rpcs: { getOrder } });

    expectTypeOf<
      keyof NonNullable<typeof contract.rpcs.getOrder.errors>
    >().toEqualTypeOf<"ORDER_NOT_FOUND">();
  });
});
