import { describe, expect, expectTypeOf, it } from "vitest";
import { z } from "zod";

import {
  defineContract,
  defineExchange,
  defineQueue,
  defineMessage,
  defineEventPublisher,
  defineEventConsumer,
  defineQueueBinding,
  defineExchangeBinding,
  defineRpc,
} from "./builder/index.js";
import { deriveTtlBackoffInfrastructure } from "./builder/ttl-backoff.js";
import { defineContractBinding } from "./contract-binding.js";

describe("defineContractBinding", () => {
  it("should bind private topology while preserving foreign exchanges, keys and schemas", () => {
    // GIVEN
    const source = defineExchange("source");
    const privateExchange = defineExchange("private");
    const dlx = defineExchange("parked");
    const parked = defineQueue("parked", { onPoison: "drop" });
    const queue = defineQueue("inbox", {
      deadLetter: { exchange: dlx },
      retry: { mode: "ttl-backoff", maxRetries: 1, initialDelayMs: 100, jitter: false },
    });
    const message = defineMessage(z.object({ id: z.string() }));
    const event = defineEventPublisher(source, message, { routingKey: "event.v1" });
    const contract = defineContract({
      publishers: { event },
      consumers: { handle: defineEventConsumer(event, queue) },
      queues: { parked },
      exchanges: { privateExchange },
      bindings: {
        parked: defineQueueBinding(parked, dlx, { routingKey: "#" }),
        bridge: defineExchangeBinding(privateExchange, source, { routingKey: "event.#" }),
      },
      rpcs: {
        ask: defineRpc(defineQueue("rpc", { onPoison: "drop" }), {
          request: defineMessage(z.string()),
          response: defineMessage(z.string()),
        }),
      },
    });
    // WHEN
    const bound = defineContractBinding(contract, {
      exchanges: { private: "release.private", parked: "release.parked" },
      queues: { inbox: "release.inbox", parked: "release.parked", rpc: "release.rpc" },
    });
    // THEN
    expect({
      source: bound.publishers.event.exchange.name,
      key: bound.publishers.event.routingKey,
      queue: bound.consumers.handle.queue.name,
      dlx: bound.consumers.handle.queue.deadLetter?.exchange.name,
      parked: bound.bindings.parked.queue.name,
      bridge: bound.bindings.bridge.destination.name,
      rpc: bound.rpcs.ask.queue.name,
      sameSchema: bound.consumers.handle.message === contract.consumers.handle.message,
      original: contract.consumers.handle.queue.name,
      retry: deriveTtlBackoffInfrastructure(bound.consumers.handle.queue)?.waitQueues.map(
        (wait) => wait.name,
      ),
    }).toEqual({
      source: "source",
      key: "event.v1",
      queue: "release.inbox",
      dlx: "release.parked",
      parked: "release.parked",
      bridge: "release.private",
      rpc: "release.rpc",
      sameSchema: true,
      original: "inbox",
      retry: ["release.inbox-wait-100ms"],
    });
    expectTypeOf(bound.consumers.handle.message).toEqualTypeOf(contract.consumers.handle.message);
    expectTypeOf(bound.queues["release.inbox"].name).toEqualTypeOf<"release.inbox">();
    expectTypeOf(bound.publishers.event.exchange.name).toEqualTypeOf<"source">();
  });

  it.each([
    { missing: "target" },
    { first: "second" },
    { first: "" },
    { first: " amq" },
    { first: "amq.reserved" },
    { first: "é".repeat(128) },
  ])("should reject invalid queue mappings %j", (queues) => {
    // GIVEN
    const contract = defineContract({
      queues: { first: defineQueue("first"), second: defineQueue("second") },
    });
    // WHEN / THEN
    expect(() => defineContractBinding(contract, { queues })).toThrow("defineContractBinding");
  });

  it("should reject collisions with derived retry resources", () => {
    // GIVEN
    const contract = defineContract({
      queues: {
        first: defineQueue("first", {
          retry: { mode: "ttl-backoff", maxRetries: 1, initialDelayMs: 100 },
        }),
        second: defineQueue("second"),
      },
    });
    // WHEN / THEN
    expect(() =>
      defineContractBinding(contract, { queues: { first: "bound", second: "bound-wait-100ms" } }),
    ).toThrow("collision");
  });

  it("should rewrite only resource-bearing arguments", () => {
    // GIVEN
    const contract = defineContract({
      exchanges: {
        first: defineExchange("first", {
          arguments: { "alternate-exchange": "second", business: "second" },
        }),
        second: defineExchange("second"),
      },
      queues: {
        first: defineQueue("first", {
          arguments: {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "second",
            business: "second",
          },
        }),
        second: defineQueue("second"),
      },
    });
    // WHEN
    const bound = defineContractBinding(contract, {
      exchanges: { second: "physical.exchange" },
      queues: { second: "physical.queue" },
    });
    // THEN
    expect({
      exchange: bound.exchanges.first.arguments,
      queue: bound.queues.first.arguments,
    }).toEqual({
      exchange: { "alternate-exchange": "physical.exchange", business: "second" },
      queue: {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "physical.queue",
        business: "second",
      },
    });
  });
});
