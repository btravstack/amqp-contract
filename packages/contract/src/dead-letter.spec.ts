import { describe, expect, it } from "vitest";

import { defineExchange } from "./builder/exchange.js";
import { defineQueue } from "./builder/queue.js";
import { _internal_queueHasDeadLetterExchange } from "./dead-letter.js";

const dlx = defineExchange("orders-dlx", { type: "topic" });

describe("_internal_queueHasDeadLetterExchange", () => {
  it("is true for the typed `deadLetter` form", () => {
    expect(
      _internal_queueHasDeadLetterExchange(
        defineQueue("orders", { deadLetter: { exchange: dlx } }),
      ),
    ).toBe(true);
  });

  it("is true for a DLX set through the raw `arguments` passthrough", () => {
    // setupAmqpTopology spreads `queue.arguments` into the declare arguments,
    // so this queue genuinely dead-letters on the broker.
    expect(
      _internal_queueHasDeadLetterExchange(
        defineQueue("orders", { arguments: { "x-dead-letter-exchange": "orders-dlx" } }),
      ),
    ).toBe(true);
  });

  it("is false for a queue with no dead-letter declaration at all", () => {
    expect(_internal_queueHasDeadLetterExchange(defineQueue("orders"))).toBe(false);
  });

  it("is false for an empty or non-string `x-dead-letter-exchange` argument", () => {
    expect(
      _internal_queueHasDeadLetterExchange(
        defineQueue("orders", { arguments: { "x-dead-letter-exchange": "" } }),
      ),
    ).toBe(false);
    expect(
      _internal_queueHasDeadLetterExchange(
        defineQueue("orders", { arguments: { "x-dead-letter-exchange": 42 } }),
      ),
    ).toBe(false);
  });

  it('does not confuse `onPoison: "drop"` with dead-lettering', () => {
    // A declared drop is a different fact: the queue loses the message on
    // purpose. Callers branch on it separately, so the predicate must not
    // absorb it.
    expect(_internal_queueHasDeadLetterExchange(defineQueue("orders", { onPoison: "drop" }))).toBe(
      false,
    );
  });
});
