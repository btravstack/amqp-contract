import type { EventEmitter } from "node:events";

import {
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  defineRpc,
} from "@amqp-contract/contract";
import { _internal_resetConnections } from "@amqp-contract/core/internal";
import type { ConsumeMessage } from "amqplib";
import { ErrAsync, OkAsync } from "unthrown";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { RetryableError } from "./errors.js";
import { TypedAmqpWorker } from "./worker.js";

/**
 * A reply publish that fails on the broker side (core's modeled
 * `PublishError`) must route the request to the DLQ as a `NonRetryableError`
 * — the caller has already timed out; retrying re-runs the handler against
 * nobody. A confirmed publish that merely leaves the write buffer full is NOT
 * a failure: the reply was delivered, so the request is acked.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  addSetup: ReturnType<typeof vi.fn>;
  consume: ReturnType<typeof vi.fn>;
  cancel: ReturnType<typeof vi.fn>;
  publish: ReturnType<typeof vi.fn>;
  sendToQueue: ReturnType<typeof vi.fn>;
  ack: ReturnType<typeof vi.fn>;
  nack: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.addSetup = vi.fn(() => Promise.resolve());
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "tag-1" }));
  w.cancel = vi.fn(() => Promise.resolve());
  w.publish = vi.fn();
  w.sendToQueue = vi.fn();
  w.ack = vi.fn();
  w.nack = vi.fn();
  fakes.wrapper = w;
  return {
    default: {
      connect: vi.fn(() => ({
        createChannel: vi.fn(() => w),
        on: vi.fn(),
        removeListener: vi.fn(),
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const rpcDlx = defineExchange("rpc-dlx");
// `rpc-dlx` is topic and the queue sets no dead-letter routing key, so `#`
// catches whatever key the rejected request arrived with.
const rpcDlq = defineQueue("rpc.calculate-dlq", { type: "classic", durable: false });

const contract = defineContract({
  rpcs: {
    calculate: defineRpc(
      defineQueue("rpc.calculate", {
        type: "classic",
        durable: false,
        deadLetter: { exchange: rpcDlx },
        // Deliberately retry-configured: an RPC request must still not retry.
        retry: { mode: "immediate-requeue", maxRetries: 3 },
      }),
      {
        request: defineMessage(z.object({ a: z.number(), b: z.number() })),
        response: defineMessage(z.object({ sum: z.number() })),
      },
    ),
  },
  queues: { rpcDlq },
  bindings: { rpcDlqBinding: defineQueueBinding(rpcDlq, rpcDlx, { routingKey: "#" }) },
});

function rpcRequestMessage(replyTo = "amq.rabbitmq.reply-to"): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify({ a: 1, b: 2 })),
    fields: {
      consumerTag: "tag-1",
      deliveryTag: 1,
      redelivered: false,
      exchange: "",
      routingKey: "rpc.calculate",
    },
    properties: {
      contentType: "application/json",
      headers: {},
      replyTo,
      correlationId: "corr-1",
    },
  } as unknown as ConsumeMessage;
}

describe("RPC reply publish failure routing", () => {
  beforeEach(async () => {
    wrapper().publish.mockReset();
    wrapper().consume.mockClear();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await _internal_resetConnections();
  });

  it("INVARIANT: a reply publish the broker refuses (PublishError) nacks the request to the DLQ, never acks", async () => {
    // Core classifies the rejection as PublishError("nacked"); publishReply
    // maps it to a NonRetryableError so the request is routed to the DLQ.
    wrapper().publish.mockRejectedValue(new Error("message nacked"));
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };

    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { calculate: ({ input: { payload } }) => OkAsync({ sum: payload.a + payload.b }) },
      urls: ["amqp://localhost"],
      logger,
    }).getOrThrow();

    const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
      msg: ConsumeMessage | null,
    ) => Promise<void>;
    expect(consumeCallback).toBeTypeOf("function");

    await consumeCallback(rpcRequestMessage());

    // The reply publish was attempted, then the request went to the DLQ:
    // exactly one nack with requeue=false, and no ack (the ack-exactly-once
    // and NonRetryableError invariants both hold on this path).
    expect(wrapper().publish).toHaveBeenCalledTimes(1);
    expect(wrapper().nack).toHaveBeenCalledTimes(1);
    expect(wrapper().nack).toHaveBeenCalledWith(expect.anything(), false, false);
    expect(wrapper().ack).not.toHaveBeenCalled();

    // It must be the MODELED routing (PublishError → NonRetryableError →
    // handleError → sendToDLQ), not the defensive terminal-defect fallback:
    // the fallback would produce the same nack but would mean the recovery
    // was dropped and reply failures no longer follow HandlerError routing.
    expect(logger.info).toHaveBeenCalledWith("Sending message to DLQ", expect.anything());
    expect(logger.error).not.toHaveBeenCalledWith(
      "Message processing failed with a defect; nacking message",
      expect.anything(),
    );

    await worker.close().get();
  });

  it.for([true, false])(
    "acks the request when the reply publish is confirmed (write buffer full: %s)",
    async (bufferHasRoom) => {
      wrapper().publish.mockResolvedValue(bufferHasRoom);

      const worker = await TypedAmqpWorker.create({
        contract,
        handlers: {
          calculate: ({ input: { payload } }) => OkAsync({ sum: payload.a + payload.b }),
        },
        urls: ["amqp://localhost"],
      }).getOrThrow();

      const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
        msg: ConsumeMessage | null,
      ) => Promise<void>;

      await consumeCallback(rpcRequestMessage());

      expect(wrapper().publish).toHaveBeenCalledTimes(1);
      expect(wrapper().ack).toHaveBeenCalledTimes(1);
      expect(wrapper().nack).not.toHaveBeenCalled();

      await worker.close().get();
    },
  );
});

async function deliver(
  message: ConsumeMessage,
  options: {
    handler?: Parameters<
      typeof TypedAmqpWorker.create<typeof contract>
    >[0]["handlers"]["calculate"];
    allowReplyTo?: (replyTo: string) => boolean;
  } = {},
) {
  const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: {
      calculate:
        options.handler ?? (({ input: { payload } }) => OkAsync({ sum: payload.a + payload.b })),
    },
    urls: ["amqp://localhost"],
    logger,
    rpc: { allowReplyTo: options.allowReplyTo },
  }).getOrThrow();
  const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
    msg: ConsumeMessage | null,
  ) => Promise<void>;
  await consumeCallback(message);
  await worker.close().get();
  return logger;
}

describe("RPC replyTo allowlist", () => {
  beforeEach(async () => {
    wrapper().publish.mockReset();
    wrapper().publish.mockResolvedValue(true);
    wrapper().consume.mockClear();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await _internal_resetConnections();
  });

  it("replies to a direct reply-to address by default", async () => {
    await deliver(rpcRequestMessage("amq.rabbitmq.reply-to.g1h2AA.abc"));

    expect(wrapper().publish).toHaveBeenCalledWith(
      "",
      "amq.rabbitmq.reply-to.g1h2AA.abc",
      expect.anything(),
      expect.anything(),
    );
    expect(wrapper().ack).toHaveBeenCalledTimes(1);
  });

  it("INVARIANT: a replyTo outside the allowlist is dead-lettered with a logged reason, never replied to", async () => {
    const logger = await deliver(rpcRequestMessage("orders"));

    expect([
      wrapper().publish.mock.calls.length,
      wrapper().ack.mock.calls.length,
      wrapper().nack.mock.calls.map((call) => call.slice(1)),
    ]).toEqual([0, 0, [[false, false]]]);
    expect(logger.error).toHaveBeenCalledWith(
      "RPC request has a replyTo the worker does not allow; dead-lettering it",
      expect.objectContaining({ replyTo: "orders" }),
    );
  });

  it("replies to another address when `rpc.allowReplyTo` accepts it", async () => {
    await deliver(rpcRequestMessage("my-replies"), {
      allowReplyTo: (replyTo) => replyTo === "my-replies",
    });

    expect(wrapper().publish).toHaveBeenCalledWith(
      "",
      "my-replies",
      expect.anything(),
      expect.anything(),
    );
  });
});

describe("RPC requests are not retried", () => {
  beforeEach(async () => {
    wrapper().publish.mockReset();
    wrapper().consume.mockClear();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await _internal_resetConnections();
  });

  it("INVARIANT: a RetryableError from an RPC handler dead-letters the request even on a retry-configured queue", async () => {
    await deliver(rpcRequestMessage(), {
      handler: () => ErrAsync(new RetryableError("transient")),
    });

    expect([
      wrapper().publish.mock.calls.length,
      wrapper().nack.mock.calls.map((call) => call.slice(1)),
    ]).toEqual([0, [[false, false]]]);
  });
});
