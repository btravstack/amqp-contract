import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineExchangeBinding, defineQueueBinding } from "./binding.js";
import { defineCommandConsumer } from "./command.js";
import { defineEventPublisher } from "./event.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineQueue } from "./queue.js";
import { defineRpc } from "./rpc.js";

describe("define-time structural validation", () => {
  describe("defineExchange", () => {
    it("rejects empty names", () => {
      expect(() => defineExchange("")).toThrow("Exchange name must be a non-empty string");
    });

    it("rejects unknown option keys with an actionable message", () => {
      expect(() => defineExchange("orders", { durabel: false } as never)).toThrow(
        'Unknown option "durabel" on exchange "orders". Allowed options: type, durable, autoDelete, internal, arguments.',
      );
    });

    it("rejects unknown exchange types passed through casts", () => {
      expect(() => defineExchange("orders", { type: "topics" } as never)).toThrow(
        'Unknown exchange type "topics" for exchange "orders"',
      );
    });
  });

  describe("defineQueue", () => {
    it("rejects empty names", () => {
      expect(() => defineQueue("")).toThrow("Queue name must be a non-empty string");
    });

    it("rejects unknown option keys", () => {
      expect(() => defineQueue("orders", { durabel: true } as never)).toThrow(
        'Unknown option "durabel" on queue "orders"',
      );
    });

    it("rejects unknown retry option keys", () => {
      expect(() =>
        defineQueue("orders", {
          retry: { mode: "immediate-requeue", maxRetrys: 3 } as never,
        }),
      ).toThrow('Unknown option "maxRetrys" on queue retry config of "orders"');
    });

    it("rejects unknown deadLetter option keys", () => {
      expect(() =>
        defineQueue("orders", {
          deadLetter: { exchange: defineExchange("dlx"), routngKey: "x" } as never,
        }),
      ).toThrow('Unknown option "routngKey" on queue deadLetter config of "orders"');
    });
  });

  describe("defineMessage", () => {
    it("rejects a non-Standard-Schema payload", () => {
      expect(() => defineMessage({ orderId: "not a schema" } as never)).toThrow(
        "Message payload schema must be a Standard Schema v1",
      );
    });

    it("rejects a non-Standard-Schema headers option", () => {
      expect(() => defineMessage(z.object({}), { headers: { plain: "object" } as never })).toThrow(
        "Message headers schema must be a Standard Schema v1",
      );
    });

    it("rejects unknown option keys", () => {
      expect(() => defineMessage(z.object({}), { sumary: "typo" } as never)).toThrow(
        'Unknown option "sumary" on message "(anonymous)"',
      );
    });
  });

  describe("routing keys on direct/topic targets", () => {
    const message = defineMessage(z.object({ id: z.string() }));
    const topicExchange = defineExchange("orders");
    const directExchange = defineExchange("tasks", { type: "direct" });
    const fanoutExchange = defineExchange("logs", { type: "fanout" });
    const queue = defineQueue("orders-queue");

    it("definePublisher rejects a missing routingKey on a topic exchange", () => {
      expect(() => definePublisher(topicExchange, message, {} as never)).toThrow(
        'Publisher on topic exchange "orders" requires a non-empty routingKey (got undefined). ' +
          "Direct and topic exchanges route on the routing key — without one, messages are " +
          "silently unroutable. Fanout and headers exchanges do not use routing keys.",
      );
    });

    it("definePublisher rejects an empty routingKey on a direct exchange", () => {
      expect(() => definePublisher(directExchange, message, { routingKey: "" as never })).toThrow(
        'Publisher on direct exchange "tasks" requires a non-empty routingKey (got "")',
      );
    });

    it("definePublisher accepts a fanout exchange without a routingKey", () => {
      expect(() => definePublisher(fanoutExchange, message)).not.toThrow();
    });

    it("defineQueueBinding rejects a missing routingKey on a topic exchange", () => {
      expect(() => defineQueueBinding(queue, topicExchange, {} as never)).toThrow(
        'Queue binding on topic exchange "orders" requires a non-empty routingKey (got undefined)',
      );
    });

    it("defineQueueBinding rejects an empty routingKey on a direct exchange", () => {
      expect(() => defineQueueBinding(queue, directExchange, { routingKey: "" as never })).toThrow(
        'Queue binding on direct exchange "tasks" requires a non-empty routingKey (got "")',
      );
    });

    it("defineQueueBinding accepts a fanout exchange without a routingKey", () => {
      expect(() => defineQueueBinding(queue, fanoutExchange)).not.toThrow();
    });

    it("defineExchangeBinding rejects a missing routingKey on a topic source exchange", () => {
      expect(() => defineExchangeBinding(fanoutExchange, topicExchange, {} as never)).toThrow(
        'Exchange binding on topic exchange "orders" requires a non-empty routingKey (got undefined)',
      );
    });

    it("defineExchangeBinding rejects an empty routingKey on a direct source exchange", () => {
      expect(() =>
        defineExchangeBinding(fanoutExchange, directExchange, { routingKey: "" as never }),
      ).toThrow(
        'Exchange binding on direct exchange "tasks" requires a non-empty routingKey (got "")',
      );
    });

    it("defineExchangeBinding accepts a fanout source exchange without a routingKey", () => {
      expect(() => defineExchangeBinding(topicExchange, fanoutExchange)).not.toThrow();
    });

    it("defineEventPublisher rejects a missing routingKey on a topic exchange", () => {
      expect(() => defineEventPublisher(topicExchange, message, {} as never)).toThrow(
        'Event publisher on topic exchange "orders" requires a non-empty routingKey (got undefined)',
      );
    });

    it("defineCommandConsumer rejects a missing routingKey on a direct exchange (via its binding)", () => {
      expect(() => defineCommandConsumer(queue, directExchange, message, {} as never)).toThrow(
        'Queue binding on direct exchange "tasks" requires a non-empty routingKey (got undefined)',
      );
    });
  });

  describe("defineRpc", () => {
    const queue = defineQueue("rpc.q", { type: "classic", durable: false });

    it("rejects unknown keys in the messages bag", () => {
      expect(() =>
        defineRpc(queue, {
          request: defineMessage(z.object({})),
          response: defineMessage(z.object({})),
          eror: {},
        } as never),
      ).toThrow('Unknown option "eror" on RPC "(anonymous)"');
    });

    it("rejects error map entries without a Standard Schema payload", () => {
      expect(() =>
        defineRpc(queue, {
          request: defineMessage(z.object({})),
          response: defineMessage(z.object({})),
          errors: { NOT_FOUND: { data: "nope" } } as never,
        }),
      ).toThrow('RPC error "NOT_FOUND" data schema must be a Standard Schema v1');
    });
  });
});
