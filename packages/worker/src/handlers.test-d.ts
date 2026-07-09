/**
 * Type tests for handler payload inference on the typed worker.
 * These guard the flagship DX promise: handler payloads are fully inferred
 * from the contract, and wrong handler maps are compile errors.
 */

import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import { Ok } from "unthrown";
import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";
import { defineHandlers } from "./handlers.js";
import type { WorkerInferConsumedMessage } from "./types.js";

const ordersExchange = defineExchange("orders");
const orderProcessingQueue = defineQueue("order-processing");
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);
const orderCreatedEvent = defineEventPublisher(ordersExchange, orderMessage, {
  routingKey: "order.created",
});

const contract = defineContract({
  publishers: {
    orderCreated: orderCreatedEvent,
  },
  consumers: {
    processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue),
  },
});

describe("handler payload inference", () => {
  test("should infer the consumed message payload type from the contract", () => {
    expectTypeOf<
      WorkerInferConsumedMessage<typeof contract, "processOrder">["payload"]
    >().toEqualTypeOf<{
      orderId: string;
      amount: number;
    }>();
  });

  test("should infer the payload inside a handler", () => {
    defineHandlers(contract, {
      processOrder: ({ payload }) => {
        expectTypeOf(payload).toEqualTypeOf<{ orderId: string; amount: number }>();
        return Ok(undefined).toAsync();
      },
    });
  });

  test("should reject access to properties not in the schema", () => {
    defineHandlers(contract, {
      processOrder: ({ payload }) => {
        expectTypeOf(payload).not.toHaveProperty("nonExistent");
        return Ok(undefined).toAsync();
      },
    });
  });

  test("should reject handler maps that don't match the contract", () => {
    defineHandlers(contract, {
      // @ts-expect-error — `unknownConsumer` is not defined in the contract
      unknownConsumer: () => Ok(undefined).toAsync(),
    });

    // @ts-expect-error — missing handler for `processOrder`
    defineHandlers(contract, {});
  });
});
