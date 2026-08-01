import type { EventEmitter } from "node:events";

import {
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineRpc,
} from "@amqp-contract/contract";
import { _internal_resetConnections } from "@amqp-contract/core/internal";
import type { ConsumeMessage } from "amqplib";
import { OkAsync } from "unthrown";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { TypedAmqpWorker } from "./worker.js";

/**
 * Guard for the buffer-full unification's one deliberate exception: core now
 * surfaces EVERY publish-side infrastructure failure (full write buffer
 * included) as a Defect, and `publishReply` is the single site that recovers
 * that defect into a modeled `NonRetryableError` — because a reply that
 * cannot be published must route the request to the DLQ (the caller has
 * already timed out; retrying re-runs the handler against nobody). If the
 * recovery is dropped, the defect would bypass `handleError` and the request
 * would never be nacked to the DLQ — this test pins the routing.
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
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

const rpcDlx = defineExchange("rpc-dlx");

const contract = defineContract({
  rpcs: {
    calculate: defineRpc(
      defineQueue("rpc.calculate", {
        type: "classic",
        durable: false,
        deadLetter: { exchange: rpcDlx },
      }),
      {
        request: defineMessage(z.object({ a: z.number(), b: z.number() })),
        response: defineMessage(z.object({ sum: z.number() })),
      },
    ),
  },
});

function rpcRequestMessage(): ConsumeMessage {
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
      replyTo: "amq.rabbitmq.reply-to",
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

  it("INVARIANT: a reply publish that fails at the core layer (write buffer full) nacks the request to the DLQ, never acks", async () => {
    // Core absorbs the wrapper's boolean: `false` (buffer full) becomes a
    // Defect inside AmqpClient.publish. publishReply must recover it into a
    // NonRetryableError so handleError routes the request to the DLQ.
    wrapper().publish.mockResolvedValue(false);
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };

    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }) },
      urls: ["amqp://localhost"],
      logger,
    }).get();

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

    // It must be the MODELED routing (recoverDefect → NonRetryableError →
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

  it("acks the request when the reply publish succeeds (control)", async () => {
    wrapper().publish.mockResolvedValue(true);

    const worker = await TypedAmqpWorker.create({
      contract,
      handlers: { calculate: ({ payload }) => OkAsync({ sum: payload.a + payload.b }) },
      urls: ["amqp://localhost"],
    }).get();

    const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
      msg: ConsumeMessage | null,
    ) => Promise<void>;

    await consumeCallback(rpcRequestMessage());

    expect(wrapper().publish).toHaveBeenCalledTimes(1);
    expect(wrapper().ack).toHaveBeenCalledTimes(1);
    expect(wrapper().nack).not.toHaveBeenCalled();

    await worker.close().get();
  });
});
