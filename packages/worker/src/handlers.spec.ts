import { Err, Ok } from "unthrown";
import { NonRetryableError, RetryableError } from "./errors.js";
import {
  defineConsumer,
  defineContract,
  defineMessage,
  defineQueue,
  defineRpc,
} from "@amqp-contract/contract";
import { defineHandler, defineHandlers } from "./handlers.js";
import { describe, expect, it } from "vitest";
import type { ConsumeMessage } from "amqplib";
import { z } from "zod";

/**
 * Creates a mock ConsumeMessage for testing purposes.
 */
function createMockConsumeMessage(): ConsumeMessage {
  return {
    content: Buffer.from("{}"),
    fields: {
      consumerTag: "test-consumer-tag",
      deliveryTag: 1,
      redelivered: false,
      exchange: "test-exchange",
      routingKey: "test.key",
    },
    properties: {
      contentType: undefined,
      contentEncoding: undefined,
      headers: {},
      deliveryMode: undefined,
      priority: undefined,
      correlationId: undefined,
      replyTo: undefined,
      expiration: undefined,
      messageId: undefined,
      timestamp: undefined,
      type: undefined,
      userId: undefined,
      appId: undefined,
      clusterId: undefined,
    },
  };
}

describe("handlers", () => {
  // Setup test contract
  const testQueue = defineQueue("test-queue");
  const testMessage = defineMessage(
    z.object({
      id: z.string(),
      data: z.string(),
    }),
  );
  const rpcQueue = defineQueue("rpc-queue");
  const rpcRequest = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const rpcResponse = defineMessage(z.object({ sum: z.number() }));

  const testContract = defineContract({
    consumers: {
      testConsumer: defineConsumer(testQueue, testMessage),
      anotherConsumer: defineConsumer(testQueue, testMessage),
    },
    rpcs: {
      calculate: defineRpc(rpcQueue, { request: rpcRequest, response: rpcResponse }),
    },
  });

  describe("defineHandler (safe handlers)", () => {
    it("should create a simple safe handler without options", () => {
      // GIVEN
      const handler = ({ payload }: { payload: { id: string; data: string } }) => {
        console.log(payload.id);
        return Ok(undefined).toAsync();
      };

      // WHEN
      const result = defineHandler(testContract, "testConsumer", handler);

      // THEN
      expect(result).toBe(handler);
    });

    it("should create a safe handler with prefetch option", () => {
      // GIVEN
      const handler = ({ payload }: { payload: { id: string; data: string } }) => {
        console.log(payload.id);
        return Ok(undefined).toAsync();
      };

      // WHEN
      const result = defineHandler(testContract, "testConsumer", handler, { prefetch: 10 });

      // THEN
      expect(result).toEqual([handler, { prefetch: 10 }]);
    });

    it("should create an RPC handler returning a typed response", () => {
      // GIVEN
      const handler = ({ payload }: { payload: { a: number; b: number } }) =>
        Ok({ sum: payload.a + payload.b }).toAsync();

      // WHEN
      const result = defineHandler(testContract, "calculate", handler);

      // THEN
      expect(result).toBe(handler);
    });

    it("should create an RPC handler with options", () => {
      // GIVEN
      const handler = ({ payload }: { payload: { a: number; b: number } }) =>
        Ok({ sum: payload.a + payload.b }).toAsync();

      // WHEN
      const result = defineHandler(testContract, "calculate", handler, { prefetch: 5 });

      // THEN
      expect(result).toEqual([handler, { prefetch: 5 }]);
    });

    it("should throw error if name is not in contract (mentioning both consumers and RPCs)", () => {
      // GIVEN
      const handler = ({ payload }: { payload: { id: string; data: string } }) => {
        console.log(payload.id);
        return Ok(undefined).toAsync();
      };

      // WHEN/THEN
      expect(() => {
        // @ts-expect-error Testing runtime validation with invalid name
        defineHandler(testContract, "nonExistent", handler);
      }).toThrow(
        'Handler target "nonExistent" not found in contract. Available consumers and RPCs: testConsumer, anotherConsumer, calculate',
      );
    });
  });

  describe("defineHandlers (safe handlers)", () => {
    it("should create multiple safe handlers spanning consumers and RPCs", () => {
      // GIVEN
      const handlers = {
        testConsumer: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.id);
          return Ok(undefined).toAsync();
        },
        anotherConsumer: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.data);
          return Ok(undefined).toAsync();
        },
        calculate: ({ payload }: { payload: { a: number; b: number } }) =>
          Ok({ sum: payload.a + payload.b }).toAsync(),
      };

      // WHEN
      const result = defineHandlers(testContract, handlers);

      // THEN
      expect(result).toBe(handlers);
    });

    it("should throw error if a handler key is not in contract (consumers ∪ rpcs)", () => {
      // GIVEN
      const handlers = {
        testConsumer: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.id);
          return Ok(undefined).toAsync();
        },
        anotherConsumer: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.data);
          return Ok(undefined).toAsync();
        },
        calculate: ({ payload }: { payload: { a: number; b: number } }) =>
          Ok({ sum: payload.a + payload.b }).toAsync(),
        nonExistent: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.data);
          return Ok(undefined).toAsync();
        },
      };

      // WHEN/THEN — cast to bypass type-system check; runtime guard is what's under test
      expect(() => {
        defineHandlers(testContract, handlers as never);
      }).toThrow(
        'Handler target "nonExistent" not found in contract. Available consumers and RPCs: testConsumer, anotherConsumer, calculate',
      );
    });

    it("should throw error if a contract entry has no handler (reverse completeness)", () => {
      // GIVEN — only one of the three contract entries has a handler
      const handlers = {
        testConsumer: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.id);
          return Ok(undefined).toAsync();
        },
      };

      // WHEN/THEN — cast to bypass type-system check; runtime guard is what's under test
      expect(() => {
        defineHandlers(testContract, handlers as never);
      }).toThrow(
        "Missing handlers for contract entries: anotherConsumer, calculate. " +
          "Every `consumers` and `rpcs` key requires a handler.",
      );
    });

    it("should throw a clear error if handlers is null or undefined", () => {
      // WHEN/THEN — JavaScript callers can pass nullish handlers despite the types
      expect(() => {
        defineHandlers(testContract, null as never);
      }).toThrow(
        "defineHandlers requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
      );
      expect(() => {
        defineHandlers(testContract, undefined as never);
      }).toThrow(
        "defineHandlers requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
      );
    });
  });

  describe("safe handlers error handling", () => {
    it("should allow returning RetryableError from safe handler", () => {
      // GIVEN
      const handler = (
        _message: { payload: { id: string; data: string } },
        _rawMessage: ConsumeMessage,
      ) => {
        return Err(new RetryableError("Transient failure")).toAsync();
      };

      // WHEN
      const result = defineHandler(testContract, "testConsumer", handler);

      // THEN - handler should be created successfully
      expect(result).toBe(handler);

      // Verify the handler returns the expected error
      const handlerResult = (result as typeof handler)(
        { payload: { id: "1", data: "test" } },
        createMockConsumeMessage(),
      );
      expect(handlerResult).toBeDefined();
    });

    it("should allow returning NonRetryableError from safe handler", () => {
      // GIVEN
      const handler = (
        _message: { payload: { id: string; data: string } },
        _rawMessage: ConsumeMessage,
      ) => {
        return Err(new NonRetryableError("Invalid message")).toAsync();
      };

      // WHEN
      const result = defineHandler(testContract, "testConsumer", handler);

      // THEN - handler should be created successfully
      expect(result).toBe(handler);

      // Verify the handler returns the expected error
      const handlerResult = (result as typeof handler)(
        { payload: { id: "1", data: "test" } },
        createMockConsumeMessage(),
      );
      expect(handlerResult).toBeDefined();
    });
  });
});
