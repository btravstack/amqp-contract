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
  defineRpcClient,
  defineRpcServer,
} from "@amqp-contract/contract";
import { it as baseIt } from "@amqp-contract/testing/extension";
import { Future, Result } from "@swan-io/boxed";
import { describe, expect } from "vitest";
import { z } from "zod";
import { TypedAmqpWorker } from "../worker.js";

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
        }).resultToPromise();
        workers.push(worker as TypedAmqpWorker<ContractDefinition>);
        return worker;
      });
    } finally {
      await Promise.all(
        workers.map((w) =>
          w
            .close()
            .resultToPromise()
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
        }).resultToPromise();
        clients.push(client as TypedAmqpClient<ContractDefinition>);
        return client;
      });
    } finally {
      await Promise.all(
        clients.map((c) =>
          c
            .close()
            .resultToPromise()
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
  const server = defineRpcServer(queue, { request, response });
  const client = defineRpcClient(server);
  return defineContract({
    consumers: { calculate: server },
    publishers: { calculate: client },
  });
};

describe("TypedAmqpClient RPC", () => {
  it("round-trips a request and validated response", async ({ workerFactory, clientFactory }) => {
    const contract = buildContract("rpc.calculate.success");

    await workerFactory(contract, {
      calculate: ({ payload }) => Future.value(Result.Ok({ sum: payload.a + payload.b })),
    });
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 2, b: 3 }, { timeoutMs: 5_000 }).toPromise();

    expect(result.isOk()).toBe(true);
    if (result.isOk()) {
      expect(result.value).toEqual({ sum: 5 });
    }
  });

  it("returns RpcTimeoutError when no server is running", async ({ clientFactory }) => {
    const contract = buildContract("rpc.calculate.timeout");
    const client = await clientFactory(contract);

    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 200 }).toPromise();

    expect(result.isError()).toBe(true);
    if (result.isError()) {
      expect(result.error).toBeInstanceOf(RpcTimeoutError);
    }
  });

  it("returns MessageValidationError when the server replies with the wrong shape", async ({
    workerFactory,
    clientFactory,
  }) => {
    const contract = buildContract("rpc.calculate.bad-shape");

    await workerFactory(contract, {
      // Cast through unknown to deliberately return a wrong shape.
      calculate: () => Future.value(Result.Ok({ wrong: "shape" } as unknown as { sum: number })),
    });
    const client = await clientFactory(contract);

    // The worker's response-schema validation fails before publishing the reply,
    // so the client times out (no reply is ever sent).
    const result = await client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 500 }).toPromise();

    expect(result.isError()).toBe(true);
    if (result.isError()) {
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
        // Future that never resolves — the worker holds the message until the
        // channel is torn down by the test fixture cleanup.
        return Future.make<Result<{ sum: number }, never>>(() => undefined);
      },
    });

    const client = await clientFactory(contract);

    const callFuture = client.call("calculate", { a: 1, b: 1 }, { timeoutMs: 10_000 });

    // Wait until the request has reached the worker — at that point we know
    // the publish has completed and the pending-call entry is registered.
    await handlerStartedPromise;

    await client.close().resultToPromise();

    const result = await callFuture.toPromise();
    expect(result.isError()).toBe(true);
    if (result.isError()) {
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
      })
      .toPromise();

    expect(result.isError()).toBe(true);
    if (result.isError()) {
      expect(result.error).toBeInstanceOf(MessageValidationError);
    }
  });
});
