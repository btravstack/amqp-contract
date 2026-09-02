import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, it } from "vitest";
import { z } from "zod";

import { NonRetryableError, RetryableError } from "./errors.js";
import { declareHandler, declareHandlers } from "./handlers.js";

describe("handlers", () => {
  // Setup test contract
  const dlx = defineExchange("test-dlx");
  const testQueue = defineQueue("test-queue", { deadLetter: { exchange: dlx } });
  const testMessage = defineMessage(
    z.object({
      id: z.string(),
      data: z.string(),
    }),
  );
  const rpcQueue = defineQueue("rpc-queue", { deadLetter: { exchange: dlx } });
  const rpcRequest = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const rpcResponse = defineMessage(z.object({ sum: z.number() }));

  // `test-dlx` is topic and neither queue sets a dead-letter routing key, so
  // `#` catches whatever key the rejected message arrived with. Without a queue
  // bound here, defineContract rejects the contract.
  const dlq = defineQueue("test-dlq");

  const testContract = defineContract({
    consumers: {
      testConsumer: defineConsumer(testQueue, testMessage),
      anotherConsumer: defineConsumer(testQueue, testMessage),
    },
    rpcs: {
      calculate: defineRpc(rpcQueue, { request: rpcRequest, response: rpcResponse }),
    },
    queues: { dlq },
    bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "#" }) },
  });

  describe("declareHandler (safe handlers)", () => {
    it("should create a simple safe handler without options", () => {
      // GIVEN
      const handler = (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
        console.log(payload.id);
        return OkAsync(undefined);
      };

      // WHEN
      const result = declareHandler(testContract, "testConsumer", handler);

      // THEN
      expect(result).toBe(handler);
    });

    it("should create a safe handler with prefetch option", () => {
      // GIVEN
      const handler = (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
        console.log(payload.id);
        return OkAsync(undefined);
      };

      // WHEN
      const result = declareHandler(testContract, "testConsumer", handler, { prefetch: 10 });

      // THEN
      expect(result).toEqual([handler, { prefetch: 10 }]);
    });

    it("should create an RPC handler returning a typed response", () => {
      // GIVEN
      const handler = (_: unknown, { payload }: { payload: { a: number; b: number } }) =>
        OkAsync({ sum: payload.a + payload.b });

      // WHEN
      const result = declareHandler(testContract, "calculate", handler);

      // THEN
      expect(result).toBe(handler);
    });

    it("should create an RPC handler with options", () => {
      // GIVEN
      const handler = (_: unknown, { payload }: { payload: { a: number; b: number } }) =>
        OkAsync({ sum: payload.a + payload.b });

      // WHEN
      const result = declareHandler(testContract, "calculate", handler, { prefetch: 5 });

      // THEN
      expect(result).toEqual([handler, { prefetch: 5 }]);
    });

    it("should throw error if name is not in contract (mentioning both consumers and RPCs)", () => {
      // GIVEN
      const handler = (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
        console.log(payload.id);
        return OkAsync(undefined);
      };

      // WHEN/THEN
      expect(() => {
        // @ts-expect-error Testing runtime validation with invalid name
        declareHandler(testContract, "nonExistent", handler);
      }).toThrow(
        'Handler target "nonExistent" not found in contract. Available consumers and RPCs: testConsumer, anotherConsumer, calculate',
      );
    });
  });

  describe("declareHandlers (safe handlers)", () => {
    it("should create multiple safe handlers spanning consumers and RPCs", () => {
      // GIVEN
      const handlers = {
        testConsumer: (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.id);
          return OkAsync(undefined);
        },
        anotherConsumer: (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.data);
          return OkAsync(undefined);
        },
        calculate: (_: unknown, { payload }: { payload: { a: number; b: number } }) =>
          OkAsync({ sum: payload.a + payload.b }),
      };

      // WHEN
      const result = declareHandlers(testContract, handlers);

      // THEN
      expect(result).toBe(handlers);
    });

    it("should throw error if a handler key is not in contract (consumers ∪ rpcs)", () => {
      // GIVEN
      const handlers = {
        testConsumer: (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.id);
          return OkAsync(undefined);
        },
        anotherConsumer: (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.data);
          return OkAsync(undefined);
        },
        calculate: (_: unknown, { payload }: { payload: { a: number; b: number } }) =>
          OkAsync({ sum: payload.a + payload.b }),
        nonExistent: ({ payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.data);
          return OkAsync(undefined);
        },
      };

      // WHEN/THEN — cast to bypass type-system check; runtime guard is what's under test
      expect(() => {
        declareHandlers(testContract, handlers as never);
      }).toThrow(
        'Handler target "nonExistent" not found in contract. Available consumers and RPCs: testConsumer, anotherConsumer, calculate',
      );
    });

    it("should throw error if a contract entry has no handler (reverse completeness)", () => {
      // GIVEN — only one of the three contract entries has a handler
      const handlers = {
        testConsumer: (_: unknown, { payload }: { payload: { id: string; data: string } }) => {
          console.log(payload.id);
          return OkAsync(undefined);
        },
      };

      // WHEN/THEN — cast to bypass type-system check; runtime guard is what's under test
      expect(() => {
        declareHandlers(testContract, handlers as never);
      }).toThrow(
        "Missing handlers for contract entries: anotherConsumer, calculate. " +
          "Every `consumers` and `rpcs` key requires a handler.",
      );
    });

    it("should throw when a handler entry exists but is not a function", () => {
      // GIVEN — every key present, but one is explicitly undefined and one is
      // a tuple whose first element is not callable. `Object.hasOwn` sees the
      // keys, so the missing-handlers guard cannot catch these; without this
      // check every delivery would defect with an opaque TypeError.
      const handlers = {
        testConsumer: undefined,
        anotherConsumer: ["not-a-function", { prefetch: 5 }],
        calculate: () => OkAsync({ sum: 0 }),
      };

      // WHEN/THEN — cast to bypass type-system check; runtime guard is what's under test
      expect(() => {
        declareHandlers(testContract, handlers as never);
      }).toThrow(
        "Handlers for contract entries are not functions: testConsumer, anotherConsumer. " +
          "Each handler must be a function or a [handler, options] tuple.",
      );
    });

    it("should throw a clear error if handlers is null or undefined", () => {
      // WHEN/THEN — JavaScript callers can pass nullish handlers despite the types
      expect(() => {
        declareHandlers(testContract, null as never);
      }).toThrow(
        "declareHandlers requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
      );
      expect(() => {
        declareHandlers(testContract, undefined as never);
      }).toThrow(
        "declareHandlers requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
      );
    });
  });

  describe("safe handlers error handling", () => {
    it("should allow returning RetryableError from safe handler", () => {
      // GIVEN
      const handler = () => {
        return ErrAsync(new RetryableError("Transient failure"));
      };

      // WHEN
      const result = declareHandler(testContract, "testConsumer", handler);

      // THEN - handler should be created successfully
      expect(result).toBe(handler);

      // Verify the handler returns the expected error
      const handlerResult = (result as typeof handler)();
      expect(handlerResult).toBeDefined();
    });

    it("should allow returning NonRetryableError from safe handler", () => {
      // GIVEN
      const handler = () => {
        return ErrAsync(new NonRetryableError("Invalid message"));
      };

      // WHEN
      const result = declareHandler(testContract, "testConsumer", handler);

      // THEN - handler should be created successfully
      expect(result).toBe(handler);

      // Verify the handler returns the expected error
      const handlerResult = (result as typeof handler)();
      expect(handlerResult).toBeDefined();
    });
  });
});
