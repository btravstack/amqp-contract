import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineQueueBinding } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineQueue } from "./queue.js";

const message = defineMessage(z.object({ orderId: z.string() }));
const orders = defineExchange("orders", { type: "topic" });
const auditQueue = defineQueue("audit-log");

describe("defineContract publisher routability", () => {
  it("throws when a publisher's routing key reaches no queue", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "user.#" }),
        },
      }),
    ).toThrow(/orderCreated/);
  });

  it("names the routing key, the exchange, and the declared patterns", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "user.#" }),
        },
      }),
    ).toThrow(/order\.created[\s\S]*orders[\s\S]*user\.#/);
  });

  it("accepts a publisher whose key matches a declared binding", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "order.#" }),
        },
      }),
    ).not.toThrow();
  });

  it("accepts a publisher routable via a consumer-contributed binding", () => {
    // Consumers contribute bindings, so the check must run after they are
    // collected — not while publishers are being processed.
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() =>
      defineContract({
        publishers: { orderCreated },
        consumers: { audit: defineConsumer(auditQueue, message) },
        bindings: {
          audit: defineQueueBinding(auditQueue, orders, { routingKey: "order.*" }),
        },
      }),
    ).not.toThrow();
  });

  it("accepts an unroutable publisher explicitly marked externalConsumers", () => {
    const orderCreated = definePublisher(orders, message, {
      routingKey: "order.created",
      externalConsumers: true,
    });

    expect(() => defineContract({ publishers: { orderCreated } })).not.toThrow();
  });

  it("still throws for a publish-only contract that is NOT marked", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });

    expect(() => defineContract({ publishers: { orderCreated } })).toThrow(/externalConsumers/);
  });
});
