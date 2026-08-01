import type { EventEmitter } from "node:events";

import {
  defineExchange,
  defineMessage,
  defineQueue,
  type ContractDefinition,
} from "@amqp-contract/contract";
import { _internal_resetConnections } from "@amqp-contract/core/internal";
import type { ConsumeMessage } from "amqplib";
import { OkAsync } from "unthrown";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { TypedAmqpWorker } from "./worker.js";

/**
 * Coverage for the validation path's terminal-nack logging in
 * `TypedAmqpWorker.parseAndValidateOrNack` — the twin of `retry.ts`'s
 * `sendToDLQ`, which `retry.spec.ts` pins.
 *
 * The two sites deliberately differ in one respect: `sendToDLQ` logs the
 * DLX-configured case too (`"Sending message to DLQ"`), because it is the
 * terminal step of the retry pipeline and the hand-off is worth recording.
 * `parseAndValidateOrNack` stays silent when a DLX is configured — the message
 * is already being reported by the `error`-level validation failure logged just
 * above it, and the DLX does the rest. So this file pins two branches, not
 * three, plus that silence.
 *
 * The undeclared case is only reachable through a hand-built
 * `ContractDefinition` that never passed through `defineContract` — which now
 * rejects such a queue. That bypass is constructed literally here, because it
 * is exactly what a user doing it would write.
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
  w.publish = vi.fn(() => true);
  w.sendToQueue = vi.fn(() => true);
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

const message = defineMessage(z.object({ id: z.string() }));
const ordersDlx = defineExchange("orders-dlx", { durable: false });

/**
 * Hand-built so the queue can carry neither a DLX nor `onPoison` —
 * `defineContract` rejects exactly that shape, which is the point.
 */
function contractWithQueue(queue: ReturnType<typeof defineQueue>): ContractDefinition {
  return {
    queues: { orders: queue },
    consumers: { processOrder: { queue, message } },
  } as unknown as ContractDefinition;
}

/** A payload that fails the schema, so validation nacks it. */
function invalidMessage(): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify({ id: 42 })),
    fields: {
      consumerTag: "tag-1",
      deliveryTag: 1,
      redelivered: false,
      exchange: "",
      routingKey: "orders",
    },
    properties: { contentType: "application/json", headers: {} },
  } as unknown as ConsumeMessage;
}

async function consumeInvalid(contract: ContractDefinition) {
  const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: { processOrder: () => OkAsync(undefined) },
    urls: ["amqp://localhost"],
    logger,
  } as never).get();

  const consumeCallback = wrapper().consume.mock.calls[0]?.[1] as (
    msg: ConsumeMessage | null,
  ) => Promise<void>;
  expect(consumeCallback).toBeTypeOf("function");
  await consumeCallback(invalidMessage());

  return { logger, worker };
}

describe("validation-path terminal-nack logging", () => {
  beforeEach(async () => {
    wrapper().consume.mockClear();
    wrapper().ack.mockClear();
    wrapper().nack.mockClear();
    await _internal_resetConnections();
  });

  it('logs a declared discard at info — never warn — when the queue is onPoison: "drop"', async () => {
    const { logger, worker } = await consumeInvalid(
      contractWithQueue(
        defineQueue("orders", { type: "classic", durable: false, onPoison: "drop" }),
      ),
    );

    expect(logger.info).toHaveBeenCalledWith(
      'Discarding poison message: queue is declared onPoison: "drop" and has no DLX',
      expect.objectContaining({ queueName: "orders", consumerName: "processOrder" }),
    );
    // A deliberate configuration must not raise an operational warning.
    expect(logger.warn).not.toHaveBeenCalled();
    expect(wrapper().nack).toHaveBeenCalledTimes(1);

    await worker.close().get();
  });

  it("keeps the warning when the queue carries neither a DLX nor onPoison", async () => {
    // Only reachable by bypassing defineContract — the accident the guard
    // exists to catch, so it must stay loud.
    const { logger, worker } = await consumeInvalid(
      contractWithQueue(defineQueue("orders", { type: "classic", durable: false })),
    );

    expect(logger.warn).toHaveBeenCalledWith(
      "Queue has no dead-letter exchange and no onPoison declaration - poison message will be lost on nack",
      expect.objectContaining({ queueName: "orders", consumerName: "processOrder" }),
    );
    // It must NOT claim a declaration the queue does not carry.
    expect(logger.info).not.toHaveBeenCalledWith(
      expect.stringContaining("onPoison"),
      expect.anything(),
    );

    await worker.close().get();
  });

  it("stays silent about the poison policy when a DLX is configured", async () => {
    const { logger, worker } = await consumeInvalid(
      contractWithQueue(
        defineQueue("orders", {
          type: "classic",
          durable: false,
          deadLetter: { exchange: ordersDlx },
        }),
      ),
    );

    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.info).not.toHaveBeenCalledWith(
      expect.stringContaining("onPoison"),
      expect.anything(),
    );
    // Still nacked — the DLX, not a log line, is what preserves the message.
    expect(wrapper().nack).toHaveBeenCalledTimes(1);

    await worker.close().get();
  });
});
