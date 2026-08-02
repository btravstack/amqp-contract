import {
  isRpcError,
  MessageValidationError,
  RpcCancelledError,
  type RpcError,
  RpcTimeoutError,
  TypedAmqpClient,
} from "@amqp-contract/client";
import {
  type ContractDefinition,
  type TopicExchangeDefinition,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { TechnicalError } from "@amqp-contract/core";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { rpcError, TypedAmqpWorker } from "@amqp-contract/worker";
import { ErrAsync, fromSafePromise, OkAsync } from "unthrown";
import { describe, expect, vi } from "vitest";
import { z } from "zod";

const it = baseIt.extend<{
  workerFactory: <TContract extends ContractDefinition>(
    contract: TContract,
    handlers: Parameters<typeof TypedAmqpWorker.create<TContract>>[0]["handlers"],
  ) => Promise<TypedAmqpWorker<TContract>>;
  clientFactory: <TContract extends ContractDefinition>(
    contract: TContract,
  ) => Promise<TypedAmqpClient<TContract>>;
}>({
  workerFactory: async ({ amqpConnectionUrl }, use) => {
    const workers: Array<TypedAmqpWorker<ContractDefinition>> = [];
    try {
      await use(async (contract, handlers) => {
        const worker = await TypedAmqpWorker.create({
          contract,
          handlers,
          urls: [amqpConnectionUrl],
        }).get();
        workers.push(worker as TypedAmqpWorker<ContractDefinition>);
        return worker;
      });
    } finally {
      await Promise.all(
        workers.map((w) =>
          w
            // Short drain: the cancellation test parks a handler on a
            // never-resolving promise, and cleanup must not wait the full
            // default drain timeout for it.
            .close({ drainTimeoutMs: 500 })
            .get()
            .catch(() => undefined),
        ),
      );
    }
  },
  clientFactory: async ({ amqpConnectionUrl }, use) => {
    const clients: Array<TypedAmqpClient<ContractDefinition>> = [];
    try {
      await use(async (contract) => {
        const client = await TypedAmqpClient.create({
          contract,
          urls: [amqpConnectionUrl],
        }).get();
        clients.push(client as TypedAmqpClient<ContractDefinition>);
        return client;
      });
    } finally {
      await Promise.all(
        clients.map((c) =>
          c
            .close()
            .get()
            .catch(() => undefined),
        ),
      );
    }
  },
});

const rpcDlx = defineExchange("rpc-dlx", { durable: false });

const buildContract = (queueName: string) => {
  const queue = defineQueue(queueName, {
    type: "classic",
    durable: false,
    deadLetter: { exchange: rpcDlx },
  });
  // A DLX with nothing bound discards every message routed to it, so declare
  // the dead-letter queue and bind it on the key the DLX will see.
  const dlq = defineQueue(`${queueName}-dlq`, { type: "classic", durable: false });
  const request = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const response = defineMessage(z.object({ sum: z.number() }));
  const calculate = defineRpc(queue, { request, response });
  return defineContract({
    rpcs: { calculate },
    queues: { dlq },
    bindings: { dlqBinding: defineQueueBinding(dlq, rpcDlx, { routingKey: "#" }) },
  });
};

describe("TypedAmqpClient RPC", () => {
  it("round-trips a request and validated response", async ({ workerFactory, clientFactory }) => {
    const contract = buildContract("rpc.calculate.success");

    await workerFactory(contract, {
      calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 5_000 });

    expect(result.isOk()).toBe(true);
    if (result.isOk()) {
      expect(result.value).toEqual({ sum: 5 });
    }
  });

  it("returns RpcTimeoutError when no server is running", async ({ clientFactory }) => {
    const contract = buildContract("rpc.calculate.timeout");
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 200 });

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(RpcTimeoutError);
    }
  });

  it("returns RpcTimeoutError when the server replies with the wrong shape", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildContract("rpc.calculate.bad-shape");

    await workerFactory(contract, {
      // Cast through unknown to deliberately return a wrong shape — the worker's
      // response-schema validation drops the reply, so the client times out.
      calculate: () => OkAsync({ wrong: "shape" } as unknown as { sum: number }),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 500 });

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(RpcTimeoutError);
    }
  });

  it("returns RpcCancelledError for in-flight calls when the client is closed", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildContract("rpc.calculate.cancel");

    // The worker uses a never-resolving Future so the request reaches the
    // broker and the handler starts, but no reply is ever published. Closing
    // the client mid-flight is the only way out.
    let handlerStarted: () => void = () => undefined;
    const handlerStartedPromise = new Promise<void>((resolve) => {
      handlerStarted = resolve;
    });
    await workerFactory(contract, {
      calculate: () => {
        handlerStarted();
        // AsyncResult wrapping a never-resolving promise — the worker holds
        // the message until the channel is torn down by the test fixture cleanup.
        return fromSafePromise<{ sum: number }>(new Promise(() => undefined));
      },
    });

    const client = await clientFactory(contract);

    const callFuture = client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 10_000 });

    // Wait until the request has reached the worker — at that point we know
    // the publish has completed and the pending-call entry is registered.
    await handlerStartedPromise;

    await client.close().get();

    const result = await callFuture;
    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(RpcCancelledError);
    }
  });

  it("rejects requests that fail schema validation", async ({ clientFactory }) => {
    const contract = buildContract("rpc.calculate.bad-request");
    const client = await clientFactory(contract);

    const result = await client
      // Intentional shape violation cast through unknown.
      .call("calculate", { a: "nope" } as unknown as { a: number; b: number }, {
        timeoutMs: 5_000,
      });

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(MessageValidationError);
    }
  });

  it.for([
    { label: "zero", value: 0 },
    { label: "negative", value: -1 },
    { label: "NaN", value: Number.NaN },
    { label: "Infinity", value: Number.POSITIVE_INFINITY },
    { label: "above setTimeout max", value: 2_147_483_648 },
  ])("rejects timeoutMs=$label up front", async ({ value }, { clientFactory }) => {
    const contract = buildContract(`rpc.calculate.invalid-timeout-${value}`);
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: value });

    // An invalid timeout is a programming/infrastructure fault, so it surfaces
    // as a Defect (with a TechnicalError cause), not a modeled Err.
    expect(result.isDefect()).toBe(true);
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
    }
  });
});

const buildErrorContract = (queueName: string) => {
  const queue = defineQueue(queueName, {
    type: "classic",
    durable: false,
    deadLetter: { exchange: rpcDlx },
  });
  // A DLX with nothing bound discards every message routed to it, so declare
  // the dead-letter queue and bind it on the key the DLX will see.
  const dlq = defineQueue(`${queueName}-dlq`, { type: "classic", durable: false });
  const request = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const response = defineMessage(z.object({ sum: z.number() }));
  const calculate = defineRpc(queue, {
    request,
    response,
    errors: {
      NEGATIVE_NUMBERS: { data: z.object({ a: z.number(), b: z.number() }) },
      LIMIT_EXCEEDED: { data: z.object({ limit: z.number() }) },
    },
  });
  return defineContract({
    rpcs: { calculate },
    queues: { dlq },
    bindings: { dlqBinding: defineQueueBinding(dlq, rpcDlx, { routingKey: "#" }) },
  });
};

describe("TypedAmqpClient RPC typed errors", () => {
  it("round-trips a declared typed error with validated data", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildErrorContract("rpc.calculate.typed-error");

    await workerFactory(contract, {
      calculate: ({ payload }) =>
        payload.a < 0 || payload.b < 0
          ? ErrAsync(
              rpcError("NEGATIVE_NUMBERS", { a: payload.a, b: payload.b }, "negatives rejected"),
            )
          : OkAsync({ sum: payload.a + payload.b }),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: -1, b: 2 }, { timeoutMs: 5_000 });

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(isRpcError(result.error)).toBe(true);
      if (isRpcError(result.error)) {
        expect(result.error.code).toBe("NEGATIVE_NUMBERS");
        expect(result.error.data).toEqual({ a: -1, b: 2 });
        expect(result.error.message).toBe("negatives rejected");
      }
    }
  });

  it("still resolves success replies on an RPC that declares errors", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildErrorContract("rpc.calculate.typed-error-ok");

    await workerFactory(contract, {
      calculate: ({ payload }) =>
        payload.a < 0 || payload.b < 0
          ? ErrAsync(rpcError("NEGATIVE_NUMBERS", { a: payload.a, b: payload.b }))
          : OkAsync({ sum: payload.a + payload.b }),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 5_000 });

    expect(result.isOk()).toBe(true);
    if (result.isOk()) {
      expect(result.value).toEqual({ sum: 5 });
    }
  });

  it("discriminates between multiple declared error codes", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildErrorContract("rpc.calculate.typed-error-codes");

    await workerFactory(contract, {
      calculate: ({ payload }) =>
        payload.a + payload.b > 100
          ? ErrAsync(rpcError("LIMIT_EXCEEDED", { limit: 100 }))
          : OkAsync({ sum: payload.a + payload.b }),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 60, b: 60 }, { timeoutMs: 5_000 });

    expect(result.isErr()).toBe(true);
    if (result.isErr() && isRpcError(result.error)) {
      expect(result.error.code).toBe("LIMIT_EXCEEDED");
      if (result.error.code === "LIMIT_EXCEEDED") {
        expect(result.error.data).toEqual({ limit: 100 });
      }
    }
  });

  it("times out when the handler returns an undeclared error code (worker DLQ path)", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildErrorContract("rpc.calculate.undeclared-error");

    await workerFactory(contract, {
      // Cast through unknown to deliberately bypass the type system — the
      // worker refuses to publish an undeclared code, so the client times out.
      calculate: () =>
        ErrAsync(
          rpcError("NOT_DECLARED", {}) as unknown as RpcError<"LIMIT_EXCEEDED", { limit: number }>,
        ),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 500 });

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(RpcTimeoutError);
    }
  });

  it("times out when the error data fails its declared schema (worker DLQ path)", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildErrorContract("rpc.calculate.invalid-error-data");

    await workerFactory(contract, {
      // Wrong data shape for the declared code — worker-side validation drops
      // the reply before publishing, so the client times out.
      calculate: () =>
        ErrAsync(rpcError("LIMIT_EXCEEDED", { wrong: "shape" } as unknown as { limit: number })),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 500 });

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(RpcTimeoutError);
    }
  });
});

// INVARIANT 9 guards: reply preconditions and undeclared codes must route the
// REQUEST to the DLQ — never produce a malformed reply. The timeout tests
// above prove "no reply"; these prove the DLQ half.
describe("TypedAmqpClient RPC DLQ routing", () => {
  const buildDlqErrorContract = (queueName: string, dlx: TopicExchangeDefinition) => {
    const queue = defineQueue(queueName, {
      type: "classic",
      durable: false,
      deadLetter: { exchange: dlx, routingKey: `${queueName}.dlq` },
    });
    // A DLX with nothing bound discards every message routed to it, so declare
    // the dead-letter queue and bind it on the key the DLX will see.
    const dlq = defineQueue(`${queueName}-dlq`, { type: "classic", durable: false });
    const request = defineMessage(z.object({ a: z.number(), b: z.number() }));
    const response = defineMessage(z.object({ sum: z.number() }));
    const calculate = defineRpc(queue, {
      request,
      response,
      errors: {
        LIMIT_EXCEEDED: { data: z.object({ limit: z.number() }) },
      },
    });
    return defineContract({
      rpcs: { calculate },
      queues: { dlq },
      bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: `${queueName}.dlq` }) },
    });
  };

  it("routes a request to the DLQ when the handler returns an undeclared error code", async ({
    workerFactory,
    clientFactory,
    amqpChannel,
  }) => {
    const dlx = defineExchange("rpc-undeclared-dlx", { durable: false });
    const contract = buildDlqErrorContract("rpc.calculate.undeclared-dlq", dlx);

    await workerFactory(contract, {
      calculate: () =>
        ErrAsync(
          rpcError("NOT_DECLARED", {}) as unknown as RpcError<"LIMIT_EXCEEDED", { limit: number }>,
        ),
    });
    const client = await clientFactory(contract);

    await amqpChannel.assertQueue("rpc-undeclared-dlq-q", { durable: false });
    await amqpChannel.bindQueue(
      "rpc-undeclared-dlq-q",
      dlx.name,
      "rpc.calculate.undeclared-dlq.dlq",
    );

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 500 });
    expect(result.isErr()).toBe(true);

    const dlqMsg = await vi.waitFor(
      async () => {
        const msg = await amqpChannel.get("rpc-undeclared-dlq-q", { noAck: false });
        if (!msg) throw new Error("request not yet in DLQ");
        return msg;
      },
      { timeout: 3_000 },
    );
    amqpChannel.ack(dlqMsg);
    expect(JSON.parse(dlqMsg.content.toString())).toEqual({ a: 1, b: 1 });
  });

  it("routes a request without replyTo/correlationId to the DLQ instead of replying", async ({
    workerFactory,
    amqpChannel,
  }) => {
    const dlx = defineExchange("rpc-noreply-dlx", { durable: false });
    const contract = buildDlqErrorContract("rpc.calculate.noreply-dlq", dlx);

    let handlerCalls = 0;
    await workerFactory(contract, {
      calculate: ({ payload }) => {
        handlerCalls++;
        return OkAsync({ sum: payload.a + payload.b });
      },
    });

    await amqpChannel.assertQueue("rpc-noreply-dlq-q", { durable: false });
    await amqpChannel.bindQueue("rpc-noreply-dlq-q", dlx.name, "rpc.calculate.noreply-dlq.dlq");

    // Publish straight to the RPC queue with NO replyTo/correlationId — a
    // misbehaving caller the worker must not crash on and must not answer.
    amqpChannel.sendToQueue(
      "rpc.calculate.noreply-dlq",
      Buffer.from(JSON.stringify({ a: 2, b: 3 })),
    );

    const dlqMsg = await vi.waitFor(
      async () => {
        const msg = await amqpChannel.get("rpc-noreply-dlq-q", { noAck: false });
        if (!msg) throw new Error("reply-less request not yet in DLQ");
        return msg;
      },
      { timeout: 3_000 },
    );
    amqpChannel.ack(dlqMsg);
    expect(JSON.parse(dlqMsg.content.toString())).toEqual({ a: 2, b: 3 });
    expect(handlerCalls).toBeGreaterThanOrEqual(1);
  });
});
