import { describe, expect, it } from "vitest";

import type { BindingDefinition, ExchangeDefinition } from "../types.js";
import { defineExchange } from "./exchange.js";
import { defineQueue } from "./queue.js";
import {
  _internal_declaredPatternsFor,
  _internal_isPublisherRoutable,
  _internal_resolvePublisherRoutability,
} from "./routability.js";

const ordersTopic = defineExchange("orders", { type: "topic" });
const ordersDirect = defineExchange("orders-direct", { type: "direct" });
const broadcast = defineExchange("broadcast", { type: "fanout" });
const billing = defineExchange("billing", { type: "topic" });
const q = defineQueue("audit-log");

// Widened to ExchangeDefinition so topic exchanges with different name
// literals (`orders`, `billing`) share one helper.
function queueBinding(exchange: ExchangeDefinition, routingKey: string): BindingDefinition {
  return { type: "queue", queue: q, exchange, routingKey } as BindingDefinition;
}

describe("_internal_isPublisherRoutable", () => {
  it("is routable when a topic queue binding matches the key", () => {
    const bindings = [queueBinding(ordersTopic, "order.#")];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(true);
  });

  it("is NOT routable when no topic binding matches the key", () => {
    const bindings = [queueBinding(ordersTopic, "user.#")];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(false);
  });

  it("is NOT routable when there are no bindings at all", () => {
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", [])).toBe(false);
  });

  it("requires exact equality on a direct exchange", () => {
    const bindings = [
      { type: "queue", queue: q, exchange: ordersDirect, routingKey: "order.created" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersDirect, "order.created", bindings)).toBe(true);
    expect(_internal_isPublisherRoutable(ordersDirect, "order.*", bindings)).toBe(false);
  });

  it("treats any binding on a fanout exchange as routable", () => {
    const bindings = [{ type: "queue", queue: q, exchange: broadcast }] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(broadcast, undefined, bindings)).toBe(true);
  });

  it("is NOT routable on a fanout exchange with no bindings", () => {
    expect(_internal_isPublisherRoutable(broadcast, undefined, [])).toBe(false);
  });

  it("follows an exchange-to-exchange forward to a queue (bridged publisher)", () => {
    // orders --order.#--> billing --#--> queue
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "order.#" },
      { type: "queue", queue: q, exchange: billing, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(true);
  });

  it("is NOT routable when the forward exists but the destination has no matching queue", () => {
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "order.#" },
      { type: "queue", queue: q, exchange: billing, routingKey: "user.#" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(false);
  });

  it("terminates on a cyclic exchange graph", () => {
    // orders -> billing -> orders, with no queue anywhere.
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "#" },
      { type: "exchange", source: billing, destination: ordersTopic, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_isPublisherRoutable(ordersTopic, "order.created", bindings)).toBe(false);
  });
});

/*
 * An alternate exchange is the broker's own answer to unroutable messages, so
 * a publisher on an exchange that declares one cannot lose a message the way
 * this check exists to prevent. Rejecting it was a false positive on a
 * documented feature.
 */
describe("alternate-exchange", () => {
  const withAe = defineExchange("orders-ae", {
    type: "topic",
    arguments: { "alternate-exchange": "catch-all" },
  });

  it("is routable with no bindings at all when the exchange declares one", () => {
    expect(_internal_isPublisherRoutable(withAe, "order.created", [])).toBe(true);
  });

  it("is routable when the declared bindings all reject the key", () => {
    const bindings = [queueBinding(withAe, "user.#")];
    expect(_internal_isPublisherRoutable(withAe, "order.created", bindings)).toBe(true);
  });

  it("does not require the alternate exchange to be declared in this contract", () => {
    // The argument names an exchange, and the name commonly resolves to
    // another service's topology. Following it would reject every such
    // contract — the false positive this check must not produce.
    expect(_internal_resolvePublisherRoutability(withAe, "order.created", [])).toEqual({
      routable: true,
      reachedExchanges: ["orders-ae"],
    });
  });

  it("honours an alternate exchange declared on a downstream hop", () => {
    // orders --order.#--> billing-ae, which declares its own alternate.
    const billingAe = defineExchange("billing-ae", {
      type: "topic",
      arguments: { "alternate-exchange": "catch-all" },
    });
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billingAe, routingKey: "order.#" },
    ] as BindingDefinition[];

    expect(_internal_resolvePublisherRoutability(ordersTopic, "order.created", bindings)).toEqual({
      routable: true,
      reachedExchanges: ["orders", "billing-ae"],
    });
  });

  it("still rejects an exchange whose arguments carry no alternate-exchange", () => {
    const other = defineExchange("orders-other-args", {
      type: "topic",
      arguments: { "x-custom": "value" },
    });
    expect(_internal_isPublisherRoutable(other, "order.created", [])).toBe(false);
  });
});

describe("_internal_resolvePublisherRoutability", () => {
  it("reports only the source exchange when the key never leaves it", () => {
    const bindings = [queueBinding(ordersTopic, "user.#")];
    expect(_internal_resolvePublisherRoutability(ordersTopic, "order.created", bindings)).toEqual({
      routable: false,
      reachedExchanges: ["orders"],
    });
  });

  it("reports every exchange the key reached when it is forwarded but still lost", () => {
    // The distinction the error message depends on: "orders" accepted the key
    // and forwarded it; the missing binding is on "billing".
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "order.#" },
      { type: "queue", queue: q, exchange: billing, routingKey: "user.#" },
    ] as BindingDefinition[];
    expect(_internal_resolvePublisherRoutability(ordersTopic, "order.created", bindings)).toEqual({
      routable: false,
      reachedExchanges: ["orders", "billing"],
    });
  });

  it("does not follow a forward whose pattern rejects the key", () => {
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "user.#" },
    ] as BindingDefinition[];
    expect(_internal_resolvePublisherRoutability(ordersTopic, "order.created", bindings)).toEqual({
      routable: false,
      reachedExchanges: ["orders"],
    });
  });

  it("terminates on a cyclic graph and reports the cycle's exchanges once each", () => {
    const bindings = [
      { type: "exchange", source: ordersTopic, destination: billing, routingKey: "#" },
      { type: "exchange", source: billing, destination: ordersTopic, routingKey: "#" },
    ] as BindingDefinition[];
    expect(_internal_resolvePublisherRoutability(ordersTopic, "order.created", bindings)).toEqual({
      routable: false,
      reachedExchanges: ["orders", "billing"],
    });
  });

  it("reports routable as soon as a queue is found", () => {
    const bindings = [queueBinding(ordersTopic, "order.#")];
    expect(
      _internal_resolvePublisherRoutability(ordersTopic, "order.created", bindings).routable,
    ).toBe(true);
  });
});

describe("_internal_declaredPatternsFor", () => {
  it("lists the patterns declared on an exchange, for the error message", () => {
    const bindings = [
      queueBinding(ordersTopic, "user.#"),
      queueBinding(ordersTopic, "audit.*"),
      queueBinding(billing, "order.#"),
    ];
    expect(_internal_declaredPatternsFor("orders", bindings)).toEqual(["user.#", "audit.*"]);
  });

  it("returns an empty list when nothing is declared on the exchange", () => {
    expect(_internal_declaredPatternsFor("orders", [])).toEqual([]);
  });
});
