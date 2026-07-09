/**
 * Type tests for message-payload inference on the typed client.
 * These guard the flagship DX promise: `client.publish` payloads are fully
 * inferred from the contract, and wrong shapes are compile errors.
 */

import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineMessage,
  defineQueue,
} from "@amqp-contract/contract";
import type { TechnicalError } from "@amqp-contract/core";
import type { AsyncResult } from "unthrown";
import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";
import type { TypedAmqpClient } from "./client.js";
import type { MessageValidationError } from "./errors.js";
import type { ClientInferPublisherInput } from "./types.js";

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

declare const client: TypedAmqpClient<typeof contract>;

describe("publish payload inference", () => {
  test("should infer the publisher input type from the contract", () => {
    expectTypeOf<ClientInferPublisherInput<typeof contract, "orderCreated">>().toEqualTypeOf<{
      orderId: string;
      amount: number;
    }>();
  });

  test("should accept a valid payload and return a typed AsyncResult", () => {
    expectTypeOf(
      client.publish("orderCreated", { orderId: "ORD-123", amount: 99.99 }),
    ).toEqualTypeOf<AsyncResult<void, TechnicalError | MessageValidationError>>();
  });

  test("should reject invalid payloads at compile time", () => {
    // @ts-expect-error — missing required field `amount`
    client.publish("orderCreated", { orderId: "ORD-123" });

    // @ts-expect-error — `orderId` must be a string
    client.publish("orderCreated", { orderId: 123, amount: 99.99 });

    // @ts-expect-error — excess property not in the schema
    client.publish("orderCreated", { orderId: "ORD-123", amount: 99.99, extra: true });
  });

  test("should reject unknown publisher names at compile time", () => {
    // @ts-expect-error — `unknownPublisher` is not defined in the contract
    client.publish("unknownPublisher", { orderId: "ORD-123", amount: 99.99 });
  });
});
