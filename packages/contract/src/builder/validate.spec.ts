import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
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
          errors: { NOT_FOUND: { payload: "nope" } } as never,
        }),
      ).toThrow('RPC error "NOT_FOUND" data schema must be a Standard Schema v1');
    });
  });
});
