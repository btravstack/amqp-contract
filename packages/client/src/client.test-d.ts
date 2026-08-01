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
  defineRpc,
} from "@amqp-contract/contract";
import type { RpcError } from "@amqp-contract/core";
import type { AsyncResult } from "unthrown";
import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";

import type { TypedAmqpClient } from "./client.js";
import type { MessageValidationError, RpcCancelledError, RpcTimeoutError } from "./errors.js";
import type {
  ClientInferCallError,
  ClientInferPublisherInput,
  ClientInferRpcErrors,
  ClientInferRpcRequestInput,
  ClientInferRpcResponseOutput,
} from "./types.js";

const ordersExchange = defineExchange("orders");
const orderProcessingQueue = defineQueue("order-processing", { onPoison: "drop" });
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

// RPC contract with defaults/transforms so input and output types differ, and
// a declared error map so the call error union carries typed RpcError members.
const rpcQueue = defineQueue("rpc.get-order", { onPoison: "drop" });
const getOrder = defineRpc(rpcQueue, {
  request: defineMessage(
    z.object({ orderId: z.string(), includeItems: z.boolean().default(false) }),
  ),
  response: defineMessage(z.object({ orderId: z.string(), status: z.string() })),
  errors: {
    ORDER_NOT_FOUND: { data: z.object({ orderId: z.string() }), message: "Order not found" },
    FORBIDDEN: { data: z.object({ reason: z.string() }) },
  },
});
const plainRpc = defineRpc(defineQueue("rpc.plain", { onPoison: "drop" }), {
  request: defineMessage(z.object({ a: z.number() })),
  response: defineMessage(z.object({ b: z.number() })),
});

const rpcContract = defineContract({ rpcs: { getOrder, plainRpc } });

declare const rpcClient: TypedAmqpClient<typeof rpcContract>;

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
    ).toEqualTypeOf<AsyncResult<void, MessageValidationError>>();
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

describe("RPC call inference", () => {
  test("infers the request INPUT type (defaults optional on the way in)", () => {
    expectTypeOf<ClientInferRpcRequestInput<typeof rpcContract, "getOrder">>().toEqualTypeOf<{
      orderId: string;
      includeItems?: boolean | undefined;
    }>();
  });

  test("infers the response OUTPUT type", () => {
    expectTypeOf<ClientInferRpcResponseOutput<typeof rpcContract, "getOrder">>().toEqualTypeOf<{
      orderId: string;
      status: string;
    }>();
  });

  test("infers the declared error union with per-code data types", () => {
    expectTypeOf<ClientInferRpcErrors<typeof rpcContract, "getOrder">>().toEqualTypeOf<
      RpcError<"ORDER_NOT_FOUND", { orderId: string }> | RpcError<"FORBIDDEN", { reason: string }>
    >();
  });

  test("an RPC without declared errors has a purely transport-level error union", () => {
    expectTypeOf<ClientInferRpcErrors<typeof rpcContract, "plainRpc">>().toEqualTypeOf<never>();
    expectTypeOf<ClientInferCallError<typeof rpcContract, "plainRpc">>().toEqualTypeOf<
      MessageValidationError | RpcTimeoutError | RpcCancelledError
    >();
  });

  test("call() returns the fully typed AsyncResult", () => {
    expectTypeOf(rpcClient.call("getOrder", { orderId: "42" }, { timeoutMs: 5_000 })).toEqualTypeOf<
      AsyncResult<
        { orderId: string; status: string },
        | MessageValidationError
        | RpcTimeoutError
        | RpcCancelledError
        | RpcError<"ORDER_NOT_FOUND", { orderId: string }>
        | RpcError<"FORBIDDEN", { reason: string }>
      >
    >();
  });

  test("rejects invalid requests, names, and missing timeout at compile time", () => {
    // @ts-expect-error — `orderId` must be a string
    rpcClient.call("getOrder", { orderId: 42 }, { timeoutMs: 5_000 });

    // @ts-expect-error — unknown RPC name
    rpcClient.call("unknownRpc", { orderId: "42" }, { timeoutMs: 5_000 });

    // @ts-expect-error — `timeoutMs` is required (RPC without a timeout is a footgun)
    rpcClient.call("getOrder", { orderId: "42" }, {});

    // @ts-expect-error — options object itself is required
    rpcClient.call("getOrder", { orderId: "42" });
  });
});
