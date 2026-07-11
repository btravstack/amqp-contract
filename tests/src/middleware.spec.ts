import { isRpcError, TypedAmqpClient, type PublishInterceptor } from "@amqp-contract/client";
import {
  ContractDefinition,
  defineCommandConsumer,
  defineCommandPublisher,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineRpc,
} from "@amqp-contract/contract";
import { it as baseIt } from "@amqp-contract/testing/extension";
import {
  composeMiddleware,
  defineMiddleware,
  nonRetryable,
  rpcError,
  TypedAmqpWorker,
  type CreateWorkerOptions,
  type EmptyContext,
} from "@amqp-contract/worker";
import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect } from "vitest";
import { z } from "zod";

const it = baseIt.extend<{
  workerFactory: <
    TContract extends ContractDefinition,
    TContext extends Record<string, unknown> | EmptyContext,
  >(
    options: Omit<CreateWorkerOptions<TContract, TContext>, "urls">,
  ) => Promise<TypedAmqpWorker<TContract>>;
  clientFactory: <TContract extends ContractDefinition>(
    options: Omit<Parameters<typeof TypedAmqpClient.create<TContract>>[0], "urls">,
  ) => Promise<TypedAmqpClient<TContract>>;
}>({
  workerFactory: async ({ amqpConnectionUrl }, use) => {
    const workers: Array<TypedAmqpWorker<ContractDefinition>> = [];
    try {
      await use(async (options) => {
        const worker = await TypedAmqpWorker.create({
          ...options,
          urls: [amqpConnectionUrl],
        }).getOrThrow();
        workers.push(worker as TypedAmqpWorker<ContractDefinition>);
        return worker;
      });
    } finally {
      await Promise.all(
        workers.map((w) =>
          w
            .close()
            .getOrThrow()
            .catch(() => undefined),
        ),
      );
    }
  },
  clientFactory: async ({ amqpConnectionUrl }, use) => {
    const clients: Array<TypedAmqpClient<ContractDefinition>> = [];
    try {
      await use(async (options) => {
        const client = await TypedAmqpClient.create({
          ...options,
          urls: [amqpConnectionUrl],
        }).getOrThrow();
        clients.push(client as TypedAmqpClient<ContractDefinition>);
        return client;
      });
    } finally {
      await Promise.all(
        clients.map((c) =>
          c
            .close()
            .getOrThrow()
            .catch(() => undefined),
        ),
      );
    }
  },
});

const buildConsumerContract = (suffix: string) => {
  const exchange = defineExchange(`orders-${suffix}`, { type: "topic", durable: false });
  const queue = defineQueue(`orders-${suffix}`, { type: "classic", durable: false });
  const message = defineMessage(z.object({ orderId: z.string() }));
  const processOrder = defineCommandConsumer(queue, exchange, message, {
    routingKey: "order.process",
  });
  const createOrder = defineCommandPublisher(processOrder);
  return defineContract({
    publishers: { createOrder },
    consumers: { processOrder },
  });
};

const buildRpcContract = (suffix: string) => {
  const queue = defineQueue(`rpc-${suffix}`, { type: "classic", durable: false });
  const calculate = defineRpc(queue, {
    request: defineMessage(z.object({ a: z.number(), b: z.number() })),
    response: defineMessage(z.object({ sum: z.number() })),
    errors: {
      BLOCKED: defineMessage(z.object({ reason: z.string() })),
    },
  });
  return defineContract({ rpcs: { calculate } });
};

describe("worker middleware", () => {
  it("injects typed context that handlers receive as third argument", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("mw-context");

    const middleware = composeMiddleware(
      defineMiddleware<EmptyContext, { tenantId: string }>((args, next) => {
        const tenantId = args.rawMessage.properties.headers?.["x-tenant-id"];
        return next({ context: { tenantId: typeof tenantId === "string" ? tenantId : "unknown" } });
      }),
      defineMiddleware<{ tenantId: string }, { tenantId: string; greeting: string }>((args, next) =>
        next({ context: { ...args.context, greeting: `hi ${args.context.tenantId}` } }),
      ),
    );

    let resolveSeen!: (value: { tenantId: string; greeting: string }) => void;
    const seen = new Promise<{ tenantId: string; greeting: string }>((res) => {
      resolveSeen = res;
    });

    await workerFactory({
      contract,
      middleware,
      handlers: {
        processOrder: (_message, _raw, { context }) => {
          resolveSeen(context);
          return OkAsync(undefined);
        },
      },
    });
    const client = await clientFactory({ contract });

    await client
      .publish("createOrder", { orderId: "1" }, { headers: { "x-tenant-id": "acme" } })
      .getOrThrow();

    await expect(seen).resolves.toEqual({ tenantId: "acme", greeting: "hi acme" });
  });

  it("short-circuits the handler when middleware returns an error", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("mw-short-circuit");

    let handlerRan = false;
    let resolveBlocked!: () => void;
    const blocked = new Promise<void>((res) => {
      resolveBlocked = res;
    });

    await workerFactory({
      contract,
      middleware: defineMiddleware((_args, _next) => {
        resolveBlocked();
        return ErrAsync(nonRetryable("blocked by middleware"));
      }),
      handlers: {
        processOrder: () => {
          handlerRan = true;
          return OkAsync(undefined);
        },
      },
    });
    const client = await clientFactory({ contract });

    await client.publish("createOrder", { orderId: "1" }).getOrThrow();

    await blocked;
    // Give the dispatch loop a beat to (not) run the handler after the guard.
    await new Promise((res) => setTimeout(res, 100));
    expect(handlerRan).toBe(false);
  });

  it("middleware wraps RPC handlers and can short-circuit with a typed RPC error", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildRpcContract("mw-rpc");

    await workerFactory({
      contract,
      middleware: defineMiddleware((args, next) => {
        if (args.rawMessage.properties.headers?.["x-blocked"] === "yes") {
          return ErrAsync(rpcError("BLOCKED", { reason: "header said so" }));
        }
        return next();
      }),
      handlers: {
        calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }),
      },
    });
    const client = await clientFactory({ contract });

    const ok = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 5_000 });
    expect(ok.isOk()).toBe(true);

    const blocked = await client.call(
      "calculate",
      { a: 1, b: 2 },
      { timeoutMs: 5_000, publishOptions: { headers: { "x-blocked": "yes" } } },
    );
    expect(blocked.isErr()).toBe(true);
    if (blocked.isErr()) {
      expect(isRpcError(blocked.error)).toBe(true);
      if (isRpcError(blocked.error)) {
        expect(blocked.error.code).toBe("BLOCKED");
        expect(blocked.error.data).toEqual({ reason: "header said so" });
      }
    }
  });
});

describe("client interceptors", () => {
  it("publish interceptors stamp headers the consumer can observe", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("pub-interceptor");

    let resolveHeaders!: (headers: Record<string, unknown> | undefined) => void;
    const headersSeen = new Promise<Record<string, unknown> | undefined>((res) => {
      resolveHeaders = res;
    });

    await workerFactory({
      contract,
      handlers: {
        processOrder: (_message, raw) => {
          resolveHeaders(raw.properties.headers);
          return OkAsync(undefined);
        },
      },
    });

    const stampTrace: PublishInterceptor = (args, next) =>
      next({
        options: {
          ...args.options,
          headers: { ...args.options.headers, traceparent: "00-abc-def-01" },
        },
      });

    const client = await clientFactory({ contract, publishInterceptors: [stampTrace] });

    await client.publish("createOrder", { orderId: "1" }).getOrThrow();

    await expect(headersSeen).resolves.toMatchObject({ traceparent: "00-abc-def-01" });
  });

  it("call interceptors wrap the RPC round trip and can patch the request", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildRpcContract("call-interceptor");

    await workerFactory({
      contract,
      handlers: {
        calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }),
      },
    });

    const observed: string[] = [];
    const client = await clientFactory({
      contract,
      callInterceptors: [
        (args, next) => {
          observed.push(`before:${args.rpcName}`);
          // Patch the request: double both operands.
          const request = args.request as { a: number; b: number };
          return next({ request: { a: request.a * 2, b: request.b * 2 } }).tap(() =>
            observed.push("after"),
          );
        },
      ],
    });

    const result = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 5_000 });

    expect(result.isOk()).toBe(true);
    if (result.isOk()) {
      expect(result.value).toEqual({ sum: 6 });
    }
    expect(observed).toEqual(["before:calculate", "after"]);
  });
});

describe("createContext and handler helpers", () => {
  it("seeds the middleware chain with the createContext result", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("create-context");

    let resolveSeen!: (value: Record<string, unknown>) => void;
    const seen = new Promise<Record<string, unknown>>((res) => {
      resolveSeen = res;
    });

    await workerFactory({
      contract,
      createContext: (info) => ({
        requestId: `${info.handlerName}-${String(info.rawMessage.fields.deliveryTag)}`,
      }),
      middleware: defineMiddleware<{ requestId: string }, { requestId: string; stamped: boolean }>(
        (args, next) => next({ context: { ...args.context, stamped: true } }),
      ),
      handlers: {
        processOrder: (_message, _raw, { context }) => {
          resolveSeen(context);
          return OkAsync(undefined);
        },
      },
    });
    const client = await clientFactory({ contract });

    await client.publish("createOrder", { orderId: "1" }).getOrThrow();

    await expect(seen).resolves.toEqual({ requestId: "processOrder-1", stamped: true });
  });

  it("routes a throwing createContext to the DLQ path without running the handler", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("create-context-throws");

    let handlerRan = false;
    await workerFactory({
      contract,
      createContext: () => {
        throw new Error("dependency graph exploded");
      },
      handlers: {
        processOrder: () => {
          handlerRan = true;
          return OkAsync(undefined);
        },
      },
    });
    const client = await clientFactory({ contract });

    await client.publish("createOrder", { orderId: "1" }).getOrThrow();

    await new Promise((res) => setTimeout(res, 300));
    expect(handlerRan).toBe(false);
  });

  it("RPC handlers build typed errors via the helpers.errors constructor bag", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildRpcContract("errors-bag");

    await workerFactory({
      contract,
      handlers: {
        calculate: ({ payload }, _raw, { errors }) =>
          payload.a < 0
            ? ErrAsync(errors.BLOCKED({ reason: "negative" }))
            : OkAsync({ sum: payload.a + payload.b }),
      },
    });
    const client = await clientFactory({ contract });

    const blocked = await client.call("calculate", { a: -1, b: 2 }, { timeoutMs: 5_000 });

    expect(blocked.isErr()).toBe(true);
    if (blocked.isErr() && isRpcError(blocked.error)) {
      expect(blocked.error.code).toBe("BLOCKED");
      expect(blocked.error.data).toEqual({ reason: "negative" });
    }
  });
});

describe("middleware payload substitution", () => {
  it("re-validates substituted payloads before the handler runs", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("substitution");

    let resolveSeen!: (value: unknown) => void;
    const seen = new Promise<unknown>((res) => {
      resolveSeen = res;
    });

    await workerFactory({
      contract,
      middleware: defineMiddleware((args, next) => {
        const payload = args.message.payload as { orderId: string };
        return next({ payload: { orderId: `${payload.orderId}-rewritten` } });
      }),
      handlers: {
        processOrder: ({ payload }) => {
          resolveSeen(payload);
          return OkAsync(undefined);
        },
      },
    });
    const client = await clientFactory({ contract });

    await client.publish("createOrder", { orderId: "1" }).getOrThrow();

    await expect(seen).resolves.toEqual({ orderId: "1-rewritten" });
  });

  it("blocks handler execution when the substitution fails the schema", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildConsumerContract("substitution-invalid");

    let handlerRan = false;
    await workerFactory({
      contract,
      middleware: defineMiddleware((_args, next) => next({ payload: { wrong: "shape" } })),
      handlers: {
        processOrder: () => {
          handlerRan = true;
          return OkAsync(undefined);
        },
      },
    });
    const client = await clientFactory({ contract });

    await client.publish("createOrder", { orderId: "1" }).getOrThrow();

    await new Promise((res) => setTimeout(res, 300));
    expect(handlerRan).toBe(false);
  });
});
