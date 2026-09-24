import {
  defineContract,
  defineEventConsumer,
  defineEventPublisher,
  defineExchange,
  defineExchangeBinding,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import type { Channel } from "amqplib";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { publisherTopology, setupAmqpTopology } from "./setup.js";

const orders = defineExchange("orders");
const audit = defineExchange("audit");
const dlx = defineExchange("orders-dlx");
const unrelated = defineExchange("unrelated");
const dlq = defineQueue("orders-dlq");
const processing = defineQueue("order-processing", { deadLetter: { exchange: dlx } });
const created = defineEventPublisher(orders, defineMessage(z.object({ id: z.string() })), {
  routingKey: "order.created",
});

const contract = defineContract({
  exchanges: { unrelated, audit },
  publishers: { created },
  consumers: { process: defineEventConsumer(created, processing) },
  queues: { dlq },
  bindings: {
    dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "#" }),
    forward: defineExchangeBinding(audit, orders, { routingKey: "#" }),
  },
});

function fakeChannel() {
  const ok = (..._args: unknown[]) => Promise.resolve({});
  return {
    assertExchange: vi.fn(ok),
    checkExchange: vi.fn(ok),
    assertQueue: vi.fn(ok),
    checkQueue: vi.fn(ok),
    bindQueue: vi.fn(ok),
    bindExchange: vi.fn(ok),
  };
}

const calledNames = (fn: ReturnType<typeof vi.fn>) =>
  fn.mock.calls.map((call) => call[0] as string).sort();

describe("setupAmqpTopology modes", () => {
  it('"assert" (default) declares everything', async () => {
    const channel = fakeChannel();

    await setupAmqpTopology(channel as unknown as Channel, contract);

    expect([
      calledNames(channel.assertExchange),
      calledNames(channel.assertQueue),
      channel.bindQueue.mock.calls.length + channel.bindExchange.mock.calls.length,
    ]).toEqual([
      ["audit", "orders", "orders-dlx", "unrelated"],
      ["order-processing", "orders-dlq"],
      3,
    ]);
  });

  it('"passive" only checks existence: nothing declared, nothing bound', async () => {
    const channel = fakeChannel();

    await setupAmqpTopology(channel as unknown as Channel, contract, { mode: "passive" });

    expect([
      calledNames(channel.checkExchange),
      calledNames(channel.checkQueue),
      channel.assertExchange.mock.calls.length +
        channel.assertQueue.mock.calls.length +
        channel.bindQueue.mock.calls.length +
        channel.bindExchange.mock.calls.length,
    ]).toEqual([
      ["audit", "orders", "orders-dlx", "unrelated"],
      ["order-processing", "orders-dlq"],
      0,
    ]);
  });

  it('"none" touches nothing', async () => {
    const channel = fakeChannel();

    await setupAmqpTopology(channel as unknown as Channel, contract, { mode: "none" });

    expect(Object.values(channel).every((fn) => fn.mock.calls.length === 0)).toBe(true);
  });
});

describe("publisherTopology", () => {
  it("keeps the publisher's exchanges and what they forward to — no queues, no DLX", async () => {
    const channel = fakeChannel();

    await setupAmqpTopology(channel as unknown as Channel, publisherTopology(contract));

    expect([
      calledNames(channel.assertExchange),
      channel.assertQueue.mock.calls.length + channel.bindQueue.mock.calls.length,
      channel.bindExchange.mock.calls.map((call) => [call[0], call[1]]),
    ]).toEqual([["audit", "orders"], 0, [["audit", "orders"]]]);
  });
});
