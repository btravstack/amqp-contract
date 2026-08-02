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

  /*
   * Row 4 decides without the shared resolver, because the key that will reach
   * the DLX is unknowable — so it has to consult the alternate-exchange
   * predicate itself. Counting bindings alone rejected a DLX the broker routes
   * correctly, the same false positive the publisher check already had.
   */
  describe("row 4: alternate-exchange", () => {
    const directDlxWithAe = defineExchange("orders-dlx-direct-ae", {
      type: "direct",
      arguments: { "alternate-exchange": "catch-all" },
    });
    const topicDlxWithAe = defineExchange("orders-dlx-topic-ae", {
      type: "topic",
      arguments: { "alternate-exchange": "catch-all" },
    });

    it("a direct DLX declaring one is routable with nothing bound", () => {
      const queue = defineQueue("order-processing", {
        deadLetter: { exchange: directDlxWithAe },
      });
      expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("routable");
    });

    it("the same direct DLX without one is unroutable — the control", () => {
      // Identical in every way but the argument, so the verdict above can only
      // come from the alternate exchange.
      const queue = defineQueue("order-processing", { deadLetter: { exchange: directDlx } });
      expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("unroutable");
    });

    it("a topic DLX declaring one is routable with nothing bound", () => {
      const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlxWithAe } });
      expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("routable");
    });

    it("the same topic DLX without one is unroutable — the control", () => {
      const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlx } });
      expect(_internal_resolveDeadLetterRoutability(queue, [])).toBe("unroutable");
    });

    it("stays routable when the declared bindings all reject the key", () => {
      // Row 4 accepts any binding anyway; asserted so the alternate-exchange
      // path is not silently doing nothing here.
      const queue = defineQueue("order-processing", { deadLetter: { exchange: topicDlxWithAe } });
      expect(
        _internal_resolveDeadLetterRoutability(queue, [bindingTo(topicDlxWithAe, "user.#")]),
      ).toBe("routable");
    });
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

  it("accepts an unbound dead-letter exchange declaring an alternate-exchange", () => {
    // The broker hands what matches nothing to the alternate exchange instead
    // of discarding it, so there is nothing to reject.
    const dlx = defineExchange("orders-dlx-ae", {
      type: "direct",
      arguments: { "alternate-exchange": "catch-all" },
    });
    const queue = defineQueue("order-processing-ae", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).not.toThrow();
  });

  it("warns that '#' matches nothing when the dead-letter exchange is direct", () => {
    // Row 4 accepts ANY binding when the queue sets no dead-letter routing key,
    // so a reader who answers this error with `#` on a direct exchange passes
    // the check and still routes nothing. The message is the only warning.
    const dlx = defineExchange("orders-dlx-direct-hint", { type: "direct" });
    const queue = defineQueue("order-processing-direct-hint", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).toThrow(/"#" is a topic wildcard/);
    expect(() => defineContract(contractWith(queue))).toThrow(/matches nothing/);
  });

  it("does NOT mention '#' when the dead-letter exchange is topic — there it is correct", () => {
    const dlx = defineExchange("orders-dlx-topic-hint", { type: "topic" });
    const queue = defineQueue("order-processing-topic-hint", { deadLetter: { exchange: dlx } });

    expect(() => defineContract(contractWith(queue))).toThrow(/orders-dlx-topic-hint/);
    expect(() => defineContract(contractWith(queue))).not.toThrow(/topic wildcard/);
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
