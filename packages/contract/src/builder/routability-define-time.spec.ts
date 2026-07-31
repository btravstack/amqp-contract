import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineExchangeBinding, defineQueueBinding } from "./binding.js";
import { defineCommandConsumer, defineCommandPublisher } from "./command.js";
import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { defineEventPublisher } from "./event.js";
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

/*
 * A multi-hop failure is the case where a naive message lies: the source
 * exchange's own patterns match, so listing them alone sends the reader to a
 * binding that is working fine while the real gap is a queue binding one hop
 * further on.
 */
describe("unroutable error message for multi-hop failures", () => {
  const billing = defineExchange("billing", { type: "topic" });
  const ordersToBilling = defineExchangeBinding(billing, orders, { routingKey: "order.#" });

  const buildBridged = (billingBindings: Record<string, ReturnType<typeof defineQueueBinding>>) => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });
    return () =>
      defineContract({
        publishers: { orderCreated },
        exchanges: { billing },
        bindings: { ordersToBilling, ...billingBindings },
      });
  };

  it("says the key matched on the source and points downstream", () => {
    const build = buildBridged({
      billingAudit: defineQueueBinding(auditQueue, billing, { routingKey: "user.#" }),
    });

    expect(build).toThrow(/The key matches on "orders" and is forwarded on to "billing"/);
    expect(build).toThrow(/no queue binding downstream accepts it/);
    expect(build).toThrow(/the break is not on "orders"/);
  });

  it("lists the declared patterns of every exchange the key reached", () => {
    const build = buildBridged({
      billingAudit: defineQueueBinding(auditQueue, billing, { routingKey: "user.#" }),
    });

    expect(build).toThrow(/Declared on "orders": "order\.#"\./);
    expect(build).toThrow(/Declared on "billing": "user\.#"\./);
  });

  it("reports a downstream exchange that declares nothing at all", () => {
    const build = buildBridged({});

    expect(build).toThrow(/No bindings are declared on "billing"\./);
  });

  it("keeps the single-hop message local — no downstream wording", () => {
    const orderCreated = definePublisher(orders, message, { routingKey: "order.created" });
    const build = () =>
      defineContract({
        publishers: { orderCreated },
        bindings: { audit: defineQueueBinding(auditQueue, orders, { routingKey: "user.#" }) },
      });

    expect(build).toThrow(/Declared on "orders": "user\.#"\./);
    expect(build).not.toThrow(/forwarded on to/);
  });
});

/*
 * The opt-out has to be expressible wherever a publisher can be declared.
 * `defineEventPublisher` and `defineCommandPublisher` reach `defineContract`
 * through their own config shapes, so a field carried only by
 * `definePublisher` would leave a publish-only service written in those
 * styles unable to define a contract at all — the check rejecting valid code.
 */
describe("externalConsumers across the publisher builders", () => {
  describe("defineEventPublisher", () => {
    it("throws for a publish-only event contract that is NOT marked", () => {
      const orderCreated = defineEventPublisher(orders, message, { routingKey: "order.created" });

      expect(() => defineContract({ publishers: { orderCreated } })).toThrow(/externalConsumers/);
    });

    it("accepts a publish-only event contract marked externalConsumers", () => {
      const orderCreated = defineEventPublisher(orders, message, {
        routingKey: "order.created",
        externalConsumers: true,
      });

      expect(() => defineContract({ publishers: { orderCreated } })).not.toThrow();
    });
  });

  describe("defineCommandPublisher", () => {
    const fulfillOrder = defineCommandConsumer(defineQueue("fulfillment"), orders, message, {
      routingKey: "order.fulfill",
    });

    it("throws for a publish-only command contract that is NOT marked", () => {
      // Only the sender is declared here: the queue binding lives with the
      // command's owner, in whichever contract declares it as a consumer.
      const requestFulfillment = defineCommandPublisher(fulfillOrder);

      expect(() => defineContract({ publishers: { requestFulfillment } })).toThrow(
        /externalConsumers/,
      );
    });

    it("accepts a publish-only command contract marked externalConsumers", () => {
      const requestFulfillment = defineCommandPublisher(fulfillOrder, {
        externalConsumers: true,
      });

      expect(() => defineContract({ publishers: { requestFulfillment } })).not.toThrow();
    });

    it("throws for a publish-only bridged command contract that is NOT marked", () => {
      // The bridge only gets the message as far as the target exchange; the
      // target's queue binding is owned by the remote domain.
      const local = defineExchange("local", { type: "topic" });
      const requestFulfillment = defineCommandPublisher(fulfillOrder, { bridgeExchange: local });

      expect(() => defineContract({ publishers: { requestFulfillment } })).toThrow(
        /externalConsumers/,
      );
    });

    it("accepts a publish-only bridged command contract marked externalConsumers", () => {
      const local = defineExchange("local", { type: "topic" });
      const requestFulfillment = defineCommandPublisher(fulfillOrder, {
        bridgeExchange: local,
        externalConsumers: true,
      });

      expect(() => defineContract({ publishers: { requestFulfillment } })).not.toThrow();
    });
  });
});
