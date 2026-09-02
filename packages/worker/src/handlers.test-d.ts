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
  defineRpc,
} from "@amqp-contract/contract";
import type { RpcError } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync, type AsyncResult } from "unthrown";
import { describe, expectTypeOf, test } from "vitest";
import { z } from "zod";

import type { HandlerError } from "./errors.js";
import { declareHandler, declareHandlers } from "./handlers.js";
import type {
  WorkerInferConsumedMessage,
  WorkerInferRpcErrorConstructors,
  WorkerInferRpcErrors,
  WorkerInferRpcResponse,
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

// Consumer with typed headers (with a defaulted field so output ≠ input).
const headersMessage = defineMessage(z.object({ id: z.string() }), {
  headers: z.object({
    "x-tenant-id": z.string(),
    "x-priority": z.string().default("normal"),
  }),
});
const headersEvent = defineEventPublisher(ordersExchange, headersMessage, {
  routingKey: "order.headers",
});
const headersContract = defineContract({
  publishers: { headersPublisher: headersEvent },
  consumers: {
    withHeaders: defineEventConsumer(headersEvent, defineQueue("headers-q", { onPoison: "drop" })),
  },
});

// RPC contract: response with a defaulted field (handler returns the INPUT
// shape; the worker validates before replying) and a declared error map.
const getOrder = defineRpc(defineQueue("rpc.get-order", { onPoison: "drop" }), {
  request: defineMessage(z.object({ orderId: z.string() })),
  response: defineMessage(z.object({ status: z.string(), cached: z.boolean().default(false) })),
  errors: {
    ORDER_NOT_FOUND: { data: z.object({ orderId: z.string() }), message: "Order not found" },
  },
});
const rpcContract = defineContract({
  consumers: { processOrder: defineEventConsumer(orderCreatedEvent, orderProcessingQueue) },
  rpcs: { getOrder },
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
    declareHandlers(contract, {
      processOrder: (_, { payload }) => {
        expectTypeOf(payload).toEqualTypeOf<{ orderId: string; amount: number }>();
        return OkAsync(undefined);
      },
    });
  });

  test("should reject access to properties not in the schema", () => {
    declareHandlers(contract, {
      processOrder: (_, { payload }) => {
        expectTypeOf(payload).not.toHaveProperty("nonExistent");
        return OkAsync(undefined);
      },
    });
  });

  test("should reject handler maps that don't match the contract", () => {
    declareHandlers(contract, {
      // @ts-expect-error — `unknownConsumer` is not defined in the contract
      unknownConsumer: () => OkAsync(undefined),
    });

    // @ts-expect-error — missing handler for `processOrder`
    declareHandlers(contract, {});
  });

  test("infers typed headers (validated OUTPUT: defaults applied)", () => {
    declareHandlers(headersContract, {
      withHeaders: (_, { headers }) => {
        expectTypeOf(headers).toEqualTypeOf<{ "x-tenant-id": string; "x-priority": string }>();
        return OkAsync(undefined);
      },
    });
  });

  test("accepts [handler, options] tuple entries but rejects invalid options", () => {
    declareHandlers(contract, {
      processOrder: [
        (_, { payload }) => {
          expectTypeOf(payload).toEqualTypeOf<{ orderId: string; amount: number }>();
          return OkAsync(undefined);
        },
        { prefetch: 10 },
      ],
    });

    declareHandlers(contract, {
      // @ts-expect-error — noAck is not a worker consumer option (it would
      // break the ack-exactly-once invariant)
      processOrder: [() => OkAsync(undefined), { noAck: true }],
    });
  });
});

describe("RPC handler inference", () => {
  test("infers the response as the schema INPUT (defaults optional; worker validates before replying)", () => {
    expectTypeOf<WorkerInferRpcResponse<typeof rpcContract, "getOrder">>().toEqualTypeOf<{
      status: string;
      cached?: boolean | undefined;
    }>();
  });

  test("infers the typed error union and constructor bag from the declared errors map", () => {
    expectTypeOf<WorkerInferRpcErrors<typeof rpcContract, "getOrder">>().toEqualTypeOf<
      RpcError<"ORDER_NOT_FOUND", { orderId: string }>
    >();
    expectTypeOf<WorkerInferRpcErrorConstructors<typeof rpcContract, "getOrder">>().toEqualTypeOf<{
      ORDER_NOT_FOUND: (
        data: { orderId: string },
        message?: string,
      ) => RpcError<"ORDER_NOT_FOUND", { orderId: string }>;
    }>();
  });

  test("RPC handlers get typed payload, helpers.errors, and a checked return type", () => {
    declareHandlers(rpcContract, {
      processOrder: () => OkAsync(undefined),
      getOrder: ({ errors }, { payload }) => {
        expectTypeOf(payload).toEqualTypeOf<{ orderId: string }>();
        if (payload.orderId === "missing") {
          return ErrAsync(errors.ORDER_NOT_FOUND({ orderId: payload.orderId }));
        }
        return OkAsync({ status: "shipped" });
      },
    });

    declareHandlers(rpcContract, {
      processOrder: () => OkAsync(undefined),
      // @ts-expect-error — RPC handler must return the response shape, not void
      getOrder: () => OkAsync(undefined),
    });

    declareHandlers(rpcContract, {
      processOrder: () => OkAsync(undefined),
      // @ts-expect-error — wrong response shape
      getOrder: () => OkAsync({ wrong: true }),
    });
  });

  test("declareHandler overloads cover consumer and RPC names, with and without options", () => {
    const consumerHandler = declareHandler(contract, "processOrder", (_, { payload }) => {
      expectTypeOf(payload).toEqualTypeOf<{ orderId: string; amount: number }>();
      return OkAsync(undefined);
    });
    expectTypeOf(consumerHandler).not.toBeNever();

    const rpcHandler = declareHandler(rpcContract, "getOrder", () => OkAsync({ status: "ok" }), {
      prefetch: 5,
    });
    expectTypeOf(rpcHandler).not.toBeNever();

    // @ts-expect-error — unknown name is rejected across both overload families
    declareHandler(contract, "nope", () => OkAsync(undefined));
  });

  test("middleware context threads into the handler's helpers", () => {
    type Ctx = { tenantId: string };
    declareHandlers<typeof contract, Ctx>(contract, {
      processOrder: (helpers) => {
        expectTypeOf(helpers.context).toEqualTypeOf<Ctx>();
        return OkAsync(undefined);
      },
    });
  });

  test("the raw delivery rides the helpers record, not a third parameter", () => {
    declareHandlers(contract, {
      processOrder: ({ raw }, { payload }) => {
        expectTypeOf(raw).toEqualTypeOf<ConsumeMessage>();
        expectTypeOf(payload).toEqualTypeOf<{ orderId: string; amount: number }>();
        return OkAsync(undefined);
      },
    });
  });

  test("a handler takes no third parameter", () => {
    declareHandlers(contract, {
      // @ts-expect-error — helpers and message are the only two parameters
      processOrder: (_helpers, _message, _raw: ConsumeMessage) => OkAsync(undefined),
    });
  });

  test("handler error channel accepts HandlerError and declared RpcErrors only", () => {
    declareHandlers(rpcContract, {
      processOrder: () => OkAsync(undefined),
      getOrder: (): AsyncResult<
        { status: string },
        HandlerError | RpcError<"ORDER_NOT_FOUND", { orderId: string }>
      > => OkAsync({ status: "ok" }),
    });
  });
});
