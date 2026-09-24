import type { AmqpClient } from "@amqp-contract/core";
import type { ConsumeMessage } from "amqplib";
import { describe, expect, it, vi } from "vitest";

import { type Outcome, settle } from "./outcome.js";

const msg = { fields: { deliveryTag: 1 } } as ConsumeMessage;
const error = new Error("boom");

function client() {
  const ack = vi.fn();
  const nack = vi.fn();
  return { amqpClient: { ack, nack } as unknown as AmqpClient, ack, nack };
}

describe("settle", () => {
  it.for([
    [{ kind: "acked" }, [[msg, { deliveryEpoch: 7 }]], []],
    [{ kind: "retried", error }, [[msg, { deliveryEpoch: 7 }]], []],
    [{ kind: "requeued", error, reason: "r" }, [], [[msg, { requeue: true, deliveryEpoch: 7 }]]],
    [
      { kind: "dead-lettered", error, reason: "r" },
      [],
      [[msg, { requeue: false, deliveryEpoch: 7 }]],
    ],
  ] as const satisfies ReadonlyArray<readonly [Outcome, unknown, unknown]>)(
    "INVARIANT: settles a '%o' outcome exactly once, stamped with the delivery epoch",
    ([outcome, acks, nacks]) => {
      const { amqpClient, ack, nack } = client();

      settle(amqpClient, msg, outcome, 7, undefined);

      expect([ack.mock.calls, nack.mock.calls]).toEqual([acks, nacks]);
    },
  );

  it("never throws when the channel refuses the settle (closing)", () => {
    const { amqpClient, nack } = client();
    nack.mockImplementation(() => {
      throw new Error("Channel closed");
    });
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };

    settle(amqpClient, msg, { kind: "dead-lettered", error, reason: "r" }, 7, logger);

    expect(logger.warn).toHaveBeenCalledTimes(1);
  });
});
