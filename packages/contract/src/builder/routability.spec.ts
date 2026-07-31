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
