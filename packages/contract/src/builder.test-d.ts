/**
 * Type tests for routing key and binding pattern validation using Vitest
 * These tests ensure that the type system correctly validates routing keys and patterns
 */

import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";

import type {
  BindingPattern,
  MatchingBindingPattern,
  MatchingRoutingKey,
  RoutingKey,
} from "./builder.js";
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
  defineQueueBinding,
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

  test("trailing # matches zero words", () => {
    // Pattern with a trailing # matches the key even when # consumes nothing
    expectTypeOf<
      MatchingRoutingKey<"order.created.#", "order.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<
      MatchingRoutingKey<"order.*.#", "order.created">
    >().toEqualTypeOf<"order.created">();
  });

  test("skips the check when either side is not a compile-time literal", () => {
    // Previously asymmetric: a plain-`string` pattern collapsed to `never`
    // while a plain-`string` key did not. Both now skip.
    expectTypeOf<MatchingRoutingKey<string, "order.created">>().toEqualTypeOf<"order.created">();
    expectTypeOf<MatchingRoutingKey<"order.#", string>>().toEqualTypeOf<string>();
    expectTypeOf<
      MatchingRoutingKey<`order.${string}`, "order.created">
    >().toEqualTypeOf<"order.created">();
  });

  test("still rejects a side that is invalid on its own", () => {
    // Each side's validity is decidable from that side alone; only the match
    // between them becomes undecidable. Deferring the match must not defer
    // these — a wildcard is illegal in a routing key, and "" is not a pattern.
    expectTypeOf<MatchingRoutingKey<string, "order.*">>().toEqualTypeOf<never>();
    expectTypeOf<MatchingRoutingKey<`order.${string}`, "order.*">>().toEqualTypeOf<never>();
    expectTypeOf<MatchingRoutingKey<"", string>>().toEqualTypeOf<never>();
  });
});

describe("MatchingBindingPattern (topic consumer override enforcement)", () => {
  test("resolves to the pattern when it can match the publisher key", () => {
    expectTypeOf<MatchingBindingPattern<"order.*", "order.created">>().toEqualTypeOf<"order.*">();
    expectTypeOf<MatchingBindingPattern<"order.#", "order.created">>().toEqualTypeOf<"order.#">();
    expectTypeOf<MatchingBindingPattern<"#", "order.created">>().toEqualTypeOf<"#">();
    expectTypeOf<
      MatchingBindingPattern<"order.created", "order.created">
    >().toEqualTypeOf<"order.created">();
    expectTypeOf<
      MatchingBindingPattern<"order.created.#", "order.created">
    >().toEqualTypeOf<"order.created.#">();
  });

  test("resolves to a descriptive error string when the pattern can never match", () => {
    expectTypeOf<
      MatchingBindingPattern<"user.*", "order.created">
    >().toEqualTypeOf<"Error: binding pattern 'user.*' can never match the publisher routing key 'order.created'">();
    expectTypeOf<
      MatchingBindingPattern<"order", "order.created">
    >().toEqualTypeOf<"Error: binding pattern 'order' can never match the publisher routing key 'order.created'">();
  });

  test("skips the check for non-literal strings", () => {
    expectTypeOf<MatchingBindingPattern<string, "order.created">>().toEqualTypeOf<string>();
    expectTypeOf<MatchingBindingPattern<"order.*", string>>().toEqualTypeOf<"order.*">();
  });

  test("skips the check for template-literal patterns", () => {
    // These all match at runtime. Deciding them at compile time is not
    // possible, so the check must defer to `defineContract` rather than
    // guess — guessing here rejected a valid contract.
    expectTypeOf<
      MatchingBindingPattern<`${string}.created`, "order.created">
    >().toEqualTypeOf<`${string}.created`>();
    expectTypeOf<
      MatchingBindingPattern<`order.${string}`, "order.created">
    >().toEqualTypeOf<`order.${string}`>();
    expectTypeOf<
      MatchingBindingPattern<`${string}.orders.#`, "acme.orders.created">
    >().toEqualTypeOf<`${string}.orders.#`>();
    expectTypeOf<MatchingBindingPattern<"order.#", `order.${string}`>>().toEqualTypeOf<"order.#">();
    // A union with one undecidable member is undecidable as a whole.
    expectTypeOf<
      MatchingBindingPattern<"order.*" | `x.${string}`, "order.created">
    >().toEqualTypeOf<"order.*" | `x.${string}`>();
  });

  test("still rejects empty patterns", () => {
    expectTypeOf<MatchingBindingPattern<"", "order.created">>().toEqualTypeOf<never>();
  });
});

describe("defineEventConsumer topic routing-key override enforcement", () => {
  const ordersExchange = defineExchange("orders");
  const orderMessage = defineMessage(z.object({ orderId: z.string() }));
  const allOrdersQueue = defineQueue("all-orders");
  const bridgeExchange = defineExchange("billing");
  const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
    routingKey: "order.created",
  });

  test("accepts patterns that can match the publisher routing key", () => {
    defineEventConsumer(orderCreated, allOrdersQueue);
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order.created" });
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order.*" });
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order.#" });
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "#" });
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order.created.#" });
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "*.created" });
  });

  test("accepts a template-literal pattern through the public API", () => {
    // The defect this suite exists to prevent, reproduced end-to-end: a
    // tenant-prefixed pattern matches 'order.created' at runtime, and the
    // library used to fail the build with
    //   "binding pattern '${string}.created' can never match the publisher
    //    routing key 'order.created'".
    const tenantPattern = "acme.created" as `${string}.created`;
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: tenantPattern });

    const suffixPattern = "order.created" as `order.${string}`;
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: suffixPattern });

    defineEventConsumer(orderCreated, allOrdersQueue, {
      bridgeExchange,
      routingKey: tenantPattern,
    });
  });

  test("rejects patterns that can never match the publisher routing key", () => {
    // @ts-expect-error — 'user.*' can never match 'order.created'
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "user.*" });
    // @ts-expect-error — 'order' can never match 'order.created'
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order" });
    // @ts-expect-error — 'order.created.urgent' can never match 'order.created'
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order.created.urgent" });
    // @ts-expect-error — 'order.*.urgent' can never match 'order.created'
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "order.*.urgent" });
    // @ts-expect-error — '' is not a valid binding pattern
    defineEventConsumer(orderCreated, allOrdersQueue, { routingKey: "" });
  });

  test("enforces the same constraint on the bridged topic overload", () => {
    defineEventConsumer(orderCreated, allOrdersQueue, {
      bridgeExchange,
      routingKey: "order.*",
    });
    defineEventConsumer(orderCreated, allOrdersQueue, {
      bridgeExchange,
      routingKey: "order.created",
    });
    defineEventConsumer(orderCreated, allOrdersQueue, {
      bridgeExchange,
      // @ts-expect-error — 'user.*' can never match 'order.created'
      routingKey: "user.*",
    });
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
  // `externalConsumers` rather than a real DLQ + binding: `orderQueue` is shared
  // by every test below, several of which assert exactly which queues and
  // bindings a contract extracts. Threading a dead-letter queue through them all
  // would change the surface those assertions exist to measure.
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx, externalConsumers: true },
    retry: { mode: "immediate-requeue", maxRetries: 3 },
  });
  const notificationQueue = defineQueue("notifications", { onPoison: "drop" });
  const logQueue = defineQueue("logs", { onPoison: "drop" });
  const orderMessage = defineMessage(z.object({ orderId: z.string() }));
  const notificationMessage = defineMessage(z.object({ text: z.string() }));
  const logMessage = defineMessage(z.object({ level: z.string() }));

  test("should extract exchanges from EventPublisherConfig in publishers", () => {
    // Publishers only, deliberately: the point is that the exchange arrives
    // from the publisher and not from some consumer's binding.
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
      externalConsumers: true,
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
    // Publishers only, deliberately: see above.
    const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
      routingKey: "order.created",
      externalConsumers: true,
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
      // `processCommand` only binds `order.process`; without these two the
      // other publishers would reach no queue.
      bindings: {
        orderCreatedBinding: defineQueueBinding(orderQueue, ordersExchange, {
          routingKey: "order.created",
        }),
        notificationsBinding: defineQueueBinding(notificationQueue, fanoutExchange),
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
  // `externalConsumers` rather than a real DLQ + binding: these tests pin exact
  // key unions (`"orders" | "orders-dlx"`, `"order-processing"`), so a
  // dead-letter queue would mean rewriting the very literals under test.
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx, externalConsumers: true },
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
    const notifQueue = defineQueue("notifications", { onPoison: "drop" });
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
    const logQueue = defineQueue("logs", { onPoison: "drop" });
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
  const queue = defineQueue("rpc.orders", { type: "classic", durable: false, onPoison: "drop" });
  const request = defineMessage(z.object({ orderId: z.string() }));
  const response = defineMessage(z.object({ status: z.string() }));

  test("captures the declared error map type on the definition", () => {
    const notFound = { data: z.object({ orderId: z.string() }) };
    const rpc = defineRpc(queue, { request, response, errors: { ORDER_NOT_FOUND: notFound } });

    expectTypeOf(rpc.errors).toEqualTypeOf<{ ORDER_NOT_FOUND: typeof notFound } | undefined>();
    expectTypeOf<keyof NonNullable<typeof rpc.errors>>().toEqualTypeOf<"ORDER_NOT_FOUND">();
  });

  test("errors type is undefined when no errors are declared", () => {
    const rpc = defineRpc(queue, { request, response });

    expectTypeOf(rpc.errors).toEqualTypeOf<undefined>();
  });

  test("error map survives defineContract", () => {
    const notFound = { data: z.object({ orderId: z.string() }) };
    const getOrder = defineRpc(queue, { request, response, errors: { ORDER_NOT_FOUND: notFound } });
    const contract = defineContract({ rpcs: { getOrder } });

    expectTypeOf<
      keyof NonNullable<typeof contract.rpcs.getOrder.errors>
    >().toEqualTypeOf<"ORDER_NOT_FOUND">();
  });
});

describe("standalone topology typing", () => {
  test("standalone queues and exchanges are re-keyed by NAME in the contract output", () => {
    const auditExchange = defineExchange("audit");
    const dlx = defineExchange("orders-dlx", { type: "direct" });
    const dlq = defineQueue("orders-dlq");
    // A real DLQ + binding here: every assertion below is `toHaveProperty`, so
    // the extra queue distorts nothing, and the sibling test already binds this
    // exact key. The explicit `routingKey` is what the binding matches — on a
    // direct exchange nothing else would.
    const orderQueue = defineQueue("order-processing", {
      deadLetter: { exchange: dlx, routingKey: "orders.dlq" },
    });

    const contract = defineContract({
      exchanges: { someLabel: auditExchange },
      queues: { anotherLabel: orderQueue, dlqLabel: dlq },
      bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "orders.dlq" }) },
    });

    // Authoring labels are dropped; resources key by their broker name.
    expectTypeOf(contract.exchanges).toHaveProperty("audit");
    expectTypeOf(contract.queues).toHaveProperty("order-processing");
    expectTypeOf(contract.queues).toHaveProperty("orders-dlq");
    // The standalone queue's DLX is auto-extracted into exchanges.
    expectTypeOf(contract.exchanges).toHaveProperty("orders-dlx");
    expectTypeOf(contract.exchanges).not.toHaveProperty("someLabel");
    expectTypeOf(contract.queues).not.toHaveProperty("anotherLabel");
    expectTypeOf(contract.queues).not.toHaveProperty("dlqLabel");
  });

  test("standalone binding labels are kept verbatim", () => {
    const dlx = defineExchange("orders-dlx", { type: "direct" });
    const dlq = defineQueue("orders-dlq");
    const contract = defineContract({
      queues: { dlq },
      bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "orders.dlq" }) },
    });

    expectTypeOf(contract.bindings).toHaveProperty("dlqBinding");
  });
});
