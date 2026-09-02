import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
} from "@amqp-contract/contract";
import { it } from "@amqp-contract/testing/extension";
import { TypedAmqpWorker } from "@amqp-contract/worker";
import { OkAsync } from "unthrown";
import { describe, expect } from "vitest";
import { z } from "zod";

/**
 * The dial succeeding while the topology does not exist (#675).
 *
 * amqp-connection-manager catches a `setup` rejection, emits it as an `error`
 * event, and announces the connection anyway — so `waitForConnect()` used to
 * answer `Ok` for a worker whose exchanges and queues had never been declared.
 * A real broker is the only honest way to prove the fix: the refusal has to be
 * RabbitMQ's own `406 PRECONDITION_FAILED`, not a simulated one.
 */
describe("initial topology setup failure", () => {
  it("fails create() when the broker refuses the contract's own queue declaration", async ({
    amqpChannel,
    amqpConnectionUrl,
  }) => {
    // GIVEN a queue that already exists as DURABLE on the broker
    await amqpChannel.assertQueue("setup-clash", { durable: true });

    // ...and a contract declaring the same queue as transient — the classic
    // operator mistake, which RabbitMQ answers with 406 PRECONDITION_FAILED
    const exchange = defineExchange("setup-clash-x", { type: "topic", durable: false });
    const dlx = defineExchange("setup-clash-dlx", { type: "topic", durable: false });
    const dlq = defineQueue("setup-clash-dlq", { type: "classic", durable: false });
    const queue = defineQueue("setup-clash", {
      type: "classic",
      durable: false,
      deadLetter: { exchange: dlx },
    });
    const message = defineMessage(z.object({ id: z.string() }));
    const contract = defineContract({
      consumers: { onClash: defineConsumer(queue, message, { exchange, routingKey: "clash" }) },
      queues: { dlq },
      bindings: { dlqBinding: defineQueueBinding(dlq, dlx, { routingKey: "#" }) },
    });

    // WHEN a worker is created against it
    const created = await TypedAmqpWorker.create({
      contract,
      handlers: { onClash: () => OkAsync(undefined) },
      urls: [amqpConnectionUrl],
      connectTimeoutMs: 5_000,
    });

    // THEN it is a defect, not a ready worker: a topology the broker refuses
    // is a broken contract, which is a bug rather than an operator's business.
    // The matchers are not registered in this workspace, so the channel is
    // read directly — and the cause is asserted, so a defect for some other
    // reason (an unreachable broker, say) cannot pass this test.
    expect({
      defect: created.isDefect(),
      cause: created.isDefect() ? String((created.cause as Error).cause) : undefined,
    }).toEqual({
      defect: true,
      // The cause names the queue the broker refused, so a defect raised for
      // any other reason — an unreachable broker, a bad option — cannot pass
      cause: expect.stringContaining("Failed to setup queues: setup-clash"),
    });
  });
});
