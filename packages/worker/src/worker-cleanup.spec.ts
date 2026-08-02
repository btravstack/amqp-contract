import {
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  defineQueue,
  defineQueueBinding,
  type ContractDefinition,
} from "@amqp-contract/contract";
import { TechnicalError } from "@amqp-contract/core";
import {
  _internal_getConnectionCount,
  _internal_resetConnections,
} from "@amqp-contract/core/internal";
import { OkAsync } from "unthrown";
import { beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";

import { TypedAmqpWorker } from "./worker.js";

const ordersDlx = defineExchange("orders-dlx");
// `orders-dlx` is topic and the queue sets no dead-letter routing key, so `#`
// catches whatever key the rejected message arrived with. Without a queue bound
// here, defineContract rejects the contract.
const ordersDlq = defineQueue("orders-dlq", { type: "classic", durable: false });

describe("TypedAmqpWorker.create cleanup", () => {
  beforeEach(async () => {
    await _internal_resetConnections();
  });

  it("releases the pooled connection when waitForConnect times out", async () => {
    const contract: ContractDefinition = {};

    // Port 1 is closed on every reasonable host; amqp-connection-manager will
    // retry forever, so the timeout is what forces create() to fail.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: {},
      urls: ["amqp://localhost:1"],
      connectTimeoutMs: 200,
    });

    expect(result).toBeDefect();
    expect(_internal_getConnectionCount()).toBe(0);
  });

  it("fails fast without acquiring a connection when handlers are missing", async () => {
    const contract = defineContract({
      consumers: {
        processOrder: defineConsumer(
          defineQueue("orders", {
            type: "classic",
            durable: false,
            deadLetter: { exchange: ordersDlx },
          }),
          defineMessage(z.object({ orderId: z.string() })),
        ),
      },
      queues: { ordersDlq },
      bindings: { ordersDlqBinding: defineQueueBinding(ordersDlq, ordersDlx, { routingKey: "#" }) },
    });

    // Cast to bypass the type-level completeness requirement; the runtime
    // guard is what's under test. The URL is never dialed — the guard runs
    // before the AmqpClient is constructed.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: {} as never,
      urls: ["amqp://localhost:1"],
    });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      expect((result.cause as TechnicalError).message).toBe(
        "Missing handlers for contract entries: processOrder. " +
          "Every `consumers` and `rpcs` key requires a handler.",
      );
    }
    expect(_internal_getConnectionCount()).toBe(0);
  });

  it("fails fast when a handler entry exists but is not a function", async () => {
    const contract = defineContract({
      consumers: {
        processOrder: defineConsumer(
          defineQueue("orders", {
            type: "classic",
            durable: false,
            deadLetter: { exchange: ordersDlx },
          }),
          defineMessage(z.object({ orderId: z.string() })),
        ),
      },
      queues: { ordersDlq },
      bindings: { ordersDlqBinding: defineQueueBinding(ordersDlq, ordersDlx, { routingKey: "#" }) },
    });

    // `{ processOrder: undefined }` passes the missing-handlers guard
    // (`Object.hasOwn` sees the key) but would make every delivery defect
    // with an opaque TypeError — create() must reject it up front, before
    // any connection is acquired.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: { processOrder: undefined } as never,
      urls: ["amqp://localhost:1"],
    });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      expect((result.cause as TechnicalError).message).toBe(
        "Handlers for contract entries are not functions: processOrder. " +
          "Each handler must be a function or a [handler, options] tuple.",
      );
    }
    expect(_internal_getConnectionCount()).toBe(0);
  });

  it("returns a Defect (does not throw) when handlers is missing entirely", async () => {
    const contract: ContractDefinition = {};

    // Cast to bypass the type system — a JavaScript caller can omit handlers,
    // and create() must stay throw-free per the error model.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: undefined as never,
      urls: ["amqp://localhost:1"],
    });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      expect((result.cause as TechnicalError).message).toBe(
        "TypedAmqpWorker.create requires a `handlers` object with one handler per `consumers` and `rpcs` entry",
      );
    }
    expect(_internal_getConnectionCount()).toBe(0);
  });

  it("fails fast when a handler key names no contract entry, before acquiring a connection", async () => {
    const contract = defineContract({
      consumers: {
        processOrder: defineConsumer(
          defineQueue("orders", {
            type: "classic",
            durable: false,
            deadLetter: { exchange: ordersDlx },
          }),
          defineMessage(z.object({ orderId: z.string() })),
        ),
      },
      queues: { ordersDlq },
      bindings: { ordersDlqBinding: defineQueueBinding(ordersDlq, ordersDlx, { routingKey: "#" }) },
    });

    // A stale key in a spread-built handlers object (or a rename that missed
    // the worker) would otherwise be silently ignored — no consumer set up,
    // no error, the message class it was meant to handle goes unprocessed.
    const result = await TypedAmqpWorker.create({
      contract,
      handlers: {
        processOrder: () => OkAsync(),
        processTypo: () => OkAsync(),
      } as never,
      urls: ["amqp://localhost:1"],
    });

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBeInstanceOf(TechnicalError);
      const message = (result.cause as TechnicalError).message;
      expect(message).toContain("processTypo");
      expect(message).toContain("processOrder");
    }
    expect(_internal_getConnectionCount()).toBe(0);
  });
});
