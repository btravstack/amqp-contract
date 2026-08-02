import { describe, expect, it } from "vitest";
import { z } from "zod";

import type {
  BindingDefinition,
  DirectExchangeDefinition,
  TopicExchangeDefinition,
} from "../types.js";
import { defineQueueBinding } from "./binding.js";
import { defineConsumer } from "./consumer.js";
import { defineContract } from "./contract.js";
import { _internal_resolveDeadLetterRoutability } from "./dead-letter-routability.js";
import { defineExchange } from "./exchange.js";
import { defineMessage } from "./message.js";
import { definePublisher } from "./publisher.js";
import { defineQueue } from "./queue.js";
import { defineRpc } from "./rpc.js";

const topicDlx = defineExchange("orders-dlx", { type: "topic" });
const directDlx = defineExchange("orders-dlx-direct", { type: "direct" });
const fanoutDlx = defineExchange("orders-dlx-fanout", { type: "fanout" });
const dlq = defineQueue("orders-dlq", { onPoison: "drop" });

/** Widened so topic and direct exchanges share one helper. */
function bindingTo(
  exchange: DirectExchangeDefinition | TopicExchangeDefinition,
  routingKey: string,
): BindingDefinition {
  return { type: "queue", queue: dlq, exchange, routingKey } as BindingDefinition;
}

describe("_internal_resolveDeadLetterRoutability", () => {
  it("row 1: skips a queue with no deadLetter config", () => {
    const queue = defineQueue("order-processing", { onPoison: "drop" });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("skipped-undecidable");
  });

  it("row 1: skips a DLX supplied only through the arguments passthrough", () => {
    // A bare exchange NAME — there is no ExchangeDefinition to look bindings up on.
    const queue = defineQueue("order-processing", {
      arguments: { "x-dead-letter-exchange": "orders-dlx" },
    });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("skipped-undecidable");
  });

  it("row 2: skips when externalConsumers is declared", () => {
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, externalConsumers: true },
    });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("skipped-external");
  });

  it("row 3: a matching routingKey on a topic DLX is routable", () => {
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, routingKey: "order.failed" },
    });
    const bindings = [bindingTo(topicDlx, "order.#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });

  it("row 3: a routingKey matching no binding is unroutable", () => {
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, routingKey: "order.failed" },
    });
    const bindings = [bindingTo(topicDlx, "user.#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("unroutable");
  });

  it("row 3: a direct DLX bound with '#' is unroutable — '#' is a topic wildcard", () => {
    // Measured against a real broker: topic + '#' receives 1, direct + '#' receives 0.
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: directDlx, routingKey: "order.failed" },
    });
    const bindings = [bindingTo(directDlx, "#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("unroutable");
  });

  it("row 3: a fanout DLX with any binding is routable, key or no key", () => {
    const queue = defineQueue("order-processing", { deadLetter: { exchange: fanoutDlx } });
    const bindings = [{ type: "queue", queue: dlq, exchange: fanoutDlx }] as BindingDefinition[];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });

  it("row 3: a fanout DLX with no bindings at all is unroutable", () => {
    const queue = defineQueue("order-processing", { deadLetter: { exchange: fanoutDlx } });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("unroutable");
  });

  it("row 4: an unset routingKey accepts any declared binding", () => {
    // The key reaching the DLX is the message's original, unknowable here — so
    // presence of a binding is all this row claims. A deliberate false negative.
    const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlx } });
    const bindings = [bindingTo(topicDlx, "user.#")];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });

  it("row 4: an unset routingKey with nothing bound is unroutable", () => {
    // This is the defect the whole guard exists for.
    const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlx } });
    expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("unroutable");
  });

  it("follows an exchange-to-exchange forward out of the DLX", () => {
    const archive = defineExchange("archive", { type: "topic" });
    const queue = defineQueue("order-processing", {
      deadLetter: { exchange: topicDlx, routingKey: "order.failed" },
    });
    const bindings = [
      { type: "exchange", source: topicDlx, destination: archive, routingKey: "#" },
      { type: "queue", queue: dlq, exchange: archive, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_resolveDeadLetterRoutability(queue, bindings)).toBe("routable");
  });
});

describe("defineContract dead-letter routability", () => {
  const message = defineMessage(z.object({ orderId: z.string() }));
  const orders = defineExchange("orders", { type: "topic" });

  /** A routable publisher and binding, so only the DLX check can fail. */
  function contractWith(queue: ReturnType<typeof defineQueue>, extra: object = {}) {
    return {
      publishers: {
        orderCreated: definePublisher(orders, message, { routingKey: "order.created" }),
      },
      consumers: { processOrder: defineConsumer(queue, message) },
      bindings: {
        processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
      },
      ...extra,
    };
  }

  it("throws when the dead-letter exchange has nothing bound to it", () => {
    const dlx = defineExchange("orders-dlx-bare", { type: "topic" });
    const queue = defineQueue("order-processing-bare", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).toThrow(/orders-dlx-bare/);
    expect(() => defineContract(contractWith(queue))).toThrow(/order-processing-bare/);
  });

  it("does NOT tell the author to add a deadLetter config — they have one", () => {
    // The remedy that does not apply is the failure mode this project has hit
    // three times: a confidently wrong pointer sends the reader after the wrong
    // thing entirely.
    const dlx = defineExchange("orders-dlx-advice", { type: "topic" });
    const queue = defineQueue("order-processing-advice", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).not.toThrow(/add .*deadLetter/i);
    expect(() => defineContract(contractWith(queue))).toThrow(/externalConsumers/);
  });

  it("accepts a dead-letter exchange with a bound dead-letter queue", () => {
    const dlx = defineExchange("orders-dlx-bound", { type: "topic" });
    const boundDlq = defineQueue("orders-dlq-bound", { onPoison: "drop" });
    const queue = defineQueue("order-processing-bound", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract(
        contractWith(queue, {
          queues: { boundDlq },
          bindings: {
            processOrder: defineQueueBinding(queue, orders, { routingKey: "order.created" }),
            dlq: defineQueueBinding(boundDlq, dlx, { routingKey: "#" }),
          },
        }),
      ),
    ).not.toThrow();
  });

  it("accepts an unbound dead-letter exchange marked externalConsumers", () => {
    const dlx = defineExchange("orders-dlx-external", { type: "topic" });
    const queue = defineQueue("order-processing-external", {
      deadLetter: { exchange: dlx, externalConsumers: true },
    });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("checks an rpc's queue too, not only consumers", () => {
    const dlx = defineExchange("rpc-dlx-bare", { type: "topic" });
    const rpcQueue = defineQueue("rpc-processing-bare", { deadLetter: { exchange: dlx } });

    expect(() =>
      defineContract({
        rpcs: {
          calculate: defineRpc(rpcQueue, { request: message, response: message }),
        },
      }),
    ).toThrow(/rpc-processing-bare/);
  });
});
