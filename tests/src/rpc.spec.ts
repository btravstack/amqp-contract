import {
  MessageValidationError,
  RpcCancelledError,
  RpcTimeoutError,
  TypedAmqpClient,
} from "@amqp-contract/client";
import {
  ContractDefinition,
  defineContract,
  defineMessage,
  defineQueue,
  defineRpc,
} from "@amqp-contract/contract";
import { TechnicalError } from "@amqp-contract/core";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { fromSafePromise, ok } from "unthrown";
import { describe, expect } from "vitest";
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
        const worker = (
          await TypedAmqpWorker.create({
            contract,
            handlers,
            urls: [amqpConnectionUrl],
          })
        ).unwrap();
        workers.push(worker as TypedAmqpWorker<ContractDefinition>);
        return worker;
      });
    } finally {
      await Promise.all(
        workers.map((w) =>
          w
            .close()
            .then((r) => r.unwrap())
            .catch(() => undefined),
        ),
      );
    }
  },
  clientFactory: async ({ amqpConnectionUrl }, use) => {
    const clients: Array<TypedAmqpClient<ContractDefinition>> = [];
    try {
      await use(async (contract) => {
        const client = (
          await TypedAmqpClient.create({
            contract,
            urls: [amqpConnectionUrl],
          })
        ).unwrap();
        clients.push(client as TypedAmqpClient<ContractDefinition>);
        return client;
      });
    } finally {
      await Promise.all(
        clients.map((c) =>
          c
            .close()
            .then((r) => r.unwrap())
            .catch(() => undefined),
        ),
      );
    }
  },
});

const buildContract = (queueName: string) => {
  const queue = defineQueue(queueName, { type: "classic", durable: false });
  const request = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const response = defineMessage(z.object({ sum: z.number() }));
  const calculate = defineRpc(queue, { request, response });
  return defineContract({ rpcs: { calculate } });
};

describe("TypedAmqpClient RPC", () => {
  it("round-trips a request and validated response", async ({ workerFactory, clientFactory }) => {
    const contract = buildContract("rpc.calculate.success");

    await workerFactory(contract, {
      calculate: ({ payload }) => ok({ sum: payload.a + payload.b }).toAsync(),
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
      calculate: () => ok({ wrong: "shape" } as unknown as { sum: number }).toAsync(),
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

    (await client.close()).unwrap();

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

    expect(result.isErr()).toBe(true);
    if (result.isErr()) {
      expect(result.error).toBeInstanceOf(TechnicalError);
    }
  });
});
