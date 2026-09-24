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
import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { ConsumeMessage } from "amqplib";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { TypedAmqpClient } from "./client.js";

/**
 * Guard for the audit's RPC-timeout finding: `handleRpcReply` used to delete
 * the pending entry and clear the caller's timer BEFORE the async response
 * validation ran. A reply whose schema validates slowly (or never settles)
 * therefore left the caller hanging past `timeoutMs` — the timer was already
 * gone. The timeout must keep running until the final result is delivered:
 * a never-settling validator resolves the call with Err(RpcTimeoutError) at
 * the budget.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  consume: ReturnType<typeof vi.fn>;
  cancel: ReturnType<typeof vi.fn>;
  publish: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "reply-tag" }));
  w.cancel = vi.fn(() => Promise.resolve());
  w.publish = vi.fn(() => Promise.resolve(true));
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

/** A Standard Schema whose async validation never settles. */
const neverSettlingSchema: StandardSchemaV1 = {
  "~standard": {
    version: 1,
    vendor: "test",
    validate: () => new Promise(() => {}),
  },
};

const rpcDlx = defineExchange("rpc-dlx");
const rpcDlq = defineQueue("rpc.calculate-dlq", { type: "classic", durable: false });
const requestSchema = z.object({ a: z.number(), b: z.number() });

function makeContract(responseSchema: StandardSchemaV1) {
  return defineContract({
    rpcs: {
      calculate: defineRpc(
        defineQueue("rpc.calculate", {
          type: "classic",
          durable: false,
          deadLetter: { exchange: rpcDlx },
        }),
        {
          request: defineMessage(requestSchema),
          response: defineMessage(responseSchema),
        },
      ),
    },
    queues: { rpcDlq },
    // `rpc-dlx` is topic and the queue sets no dead-letter routing key, so `#`
    // catches whatever key the rejected request arrived with.
    bindings: { rpcDlqBinding: defineQueueBinding(rpcDlq, rpcDlx, { routingKey: "#" }) },
  });
}

function replyMessage(correlationId: string, body: unknown): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(body)),
    fields: { consumerTag: "reply-tag", deliveryTag: 1, redelivered: false },
    properties: { correlationId, headers: {} },
  } as unknown as ConsumeMessage;
}

/** The consume callback registered on the direct-reply-to pseudo-queue. */
function replyCallback(): (msg: ConsumeMessage | null) => void {
  const call = wrapper().consume.mock.calls[0] as unknown[];
  return call[1] as (msg: ConsumeMessage | null) => void;
}

/** The correlationId stamped on the published RPC request. */
function publishedCorrelationId(): string {
  const call = wrapper().publish.mock.calls[0] as unknown[];
  return (call[3] as { correlationId: string }).correlationId;
}

describe("RPC timeout vs slow reply validation", () => {
  beforeEach(async () => {
    wrapper().removeAllListeners();
    wrapper().consume.mockClear();
    wrapper().publish.mockClear();
    await _internal_resetConnections();
  });

  it("INVARIANT: a reply whose response validation never settles still resolves Err(RpcTimeoutError) at timeoutMs", async () => {
    const client = await TypedAmqpClient.create({
      contract: makeContract(neverSettlingSchema),
      urls: ["amqp://localhost"],
    }).getOrThrow();

    const pending = client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 200 });

    // Wait for the request publish, then deliver a reply that will hang in
    // the (never-settling) response validator.
    await vi.waitFor(() => expect(wrapper().publish).toHaveBeenCalledTimes(1));
    replyCallback()(replyMessage(publishedCorrelationId(), { sum: 3 }));

    const startedAt = Date.now();
    const result = await pending;
    expect(result).toBeErrTagged("@amqp-contract/RpcTimeoutError");
    // Settled via the still-armed timer, not some other (later) mechanism.
    expect(Date.now() - startedAt).toBeLessThan(2_000);

    await client.close().get();
  });

  it("resolves Ok when the reply validates normally (control)", async () => {
    const client = await TypedAmqpClient.create({
      contract: makeContract(z.object({ sum: z.number() })),
      urls: ["amqp://localhost"],
    }).getOrThrow();

    const pending = client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 1_000 });

    await vi.waitFor(() => expect(wrapper().publish).toHaveBeenCalledTimes(1));
    replyCallback()(replyMessage(publishedCorrelationId(), { sum: 3 }));

    const result = await pending;
    expect(result).toBeOkWith({ sum: 3 });

    await client.close().get();
  });
});

describe("RPC request publish failures", () => {
  beforeEach(async () => {
    wrapper().removeAllListeners();
    wrapper().consume.mockClear();
    wrapper().publish.mockClear();
    await _internal_resetConnections();
  });

  it("INVARIANT: a request the broker side refuses resolves Err(PublishError) at once, not a timeout", async () => {
    const client = await TypedAmqpClient.create({
      contract: makeContract(z.object({ sum: z.number() })),
      urls: ["amqp://localhost"],
    }).getOrThrow();
    wrapper().publish.mockResolvedValueOnce(false);

    const result = await client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 60_000 });

    expect(result).toBeErrWith(
      expect.objectContaining({ _tag: "@amqp-contract/PublishError", reason: "buffer-full" }),
    );

    await client.close().get();
  });

  it("INVARIANT: the request expires at the caller's timeoutMs, so an unconsumed request is dropped by the broker", async () => {
    const client = await TypedAmqpClient.create({
      contract: makeContract(z.object({ sum: z.number() })),
      urls: ["amqp://localhost"],
    }).getOrThrow();

    const pending = client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 1_234 });
    await vi.waitFor(() => expect(wrapper().publish).toHaveBeenCalledTimes(1));

    const options = (wrapper().publish.mock.calls[0] as unknown[])[3];
    expect(options).toMatchObject({ expiration: "1234" });

    await client.close().get();
    await pending;
  });

  it("records the round trip on the RPC histogram, not the publish histogram", async () => {
    const publishHistogram = { record: vi.fn() };
    const rpcHistogram = { record: vi.fn() };
    const telemetry = {
      getTracer: () => undefined,
      getPublishCounter: () => undefined,
      getConsumeCounter: () => undefined,
      getPublishLatencyHistogram: () => publishHistogram as never,
      getConsumeLatencyHistogram: () => undefined,
      getLateRpcReplyCounter: () => undefined,
      getRpcCallLatencyHistogram: () => rpcHistogram as never,
    };
    const client = await TypedAmqpClient.create({
      contract: makeContract(z.object({ sum: z.number() })),
      urls: ["amqp://localhost"],
      telemetry,
    }).getOrThrow();

    const pending = client.call("calculate", { a: 1, b: 2 }, { timeoutMs: 1_000 });
    await vi.waitFor(() => expect(wrapper().publish).toHaveBeenCalledTimes(1));
    replyCallback()(replyMessage(publishedCorrelationId(), { sum: 3 }));
    await pending;

    expect(rpcHistogram.record).toHaveBeenCalledTimes(1);
    expect(rpcHistogram.record).toHaveBeenCalledWith(
      expect.any(Number),
      expect.objectContaining({ success: true, "messaging.destination.name": "rpc.calculate" }),
    );
    expect(publishHistogram.record).not.toHaveBeenCalled();

    await client.close().get();
  });
});
