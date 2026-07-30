import { describe, expect, it } from "vitest";
import { z } from "zod";

import { defineContract } from "./contract.js";
import { defineMessage } from "./message.js";
import { defineQueue } from "./queue.js";
import { defineRpc } from "./rpc.js";

describe("defineRpc", () => {
  const queue = defineQueue("rpc.calculate", { type: "classic", durable: false });
  const request = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const response = defineMessage(z.object({ sum: z.number() }));

  it("returns an RpcDefinition carrying the queue, request, and response", () => {
    // WHEN
    const rpc = defineRpc(queue, { request, response });

    // THEN
    expect(rpc).toEqual({ queue, request, response });
    // No errors declared → no `errors` key at all (not `errors: undefined`),
    // so consumers of the definition can use `"errors" in rpc` checks.
    expect(Object.hasOwn(rpc, "errors")).toBe(false);
  });

  it("carries the declared error map through to the definition", () => {
    // GIVEN
    const notFound = { data: z.object({ id: z.string() }), message: "Not found" };
    const limitExceeded = { data: z.object({ limit: z.number() }) };

    // WHEN
    const rpc = defineRpc(queue, {
      request,
      response,
      errors: { NOT_FOUND: notFound, LIMIT_EXCEEDED: limitExceeded },
    });

    // THEN
    expect(rpc).toEqual({
      queue,
      request,
      response,
      errors: { NOT_FOUND: notFound, LIMIT_EXCEEDED: limitExceeded },
    });
  });

  it("rejects an error entry that is not { data, message? }", () => {
    // GIVEN — the pre-3.0 shape passed a MessageDefinition; the unknown-key
    // check turns that mistake into an actionable error.
    const legacyEntry = defineMessage(z.object({ id: z.string() }));

    // WHEN / THEN
    expect(() =>
      defineRpc(queue, {
        request,
        response,
        errors: { NOT_FOUND: legacyEntry as never },
      }),
    ).toThrow(/Unknown option "payload"/);
  });

  describe("defineContract integration", () => {
    it("auto-extracts the RPC's queue and exposes it under contract.rpcs", () => {
      // GIVEN
      const calculate = defineRpc(queue, { request, response });

      // WHEN
      const contract = defineContract({
        rpcs: { calculate },
      });

      // THEN
      expect(contract).toMatchObject({
        // Queue registered, default exchange skipped — RPC routes implicitly
        // via the AMQP default exchange with the queue name as routing key.
        queues: { "rpc.calculate": queue },
        rpcs: { calculate: { queue, request, response } },
        // RPCs do not appear in publishers / consumers; no bindings either.
        publishers: {},
        consumers: {},
        bindings: {},
      });
    });

    it("exposes the RPC's error map under contract.rpcs", () => {
      // GIVEN
      const notFound = { data: z.object({ id: z.string() }) };
      const calculate = defineRpc(queue, { request, response, errors: { NOT_FOUND: notFound } });

      // WHEN
      const contract = defineContract({ rpcs: { calculate } });

      // THEN
      expect(contract.rpcs.calculate.errors).toEqual({ NOT_FOUND: notFound });
    });

    it("throws when an RPC name collides with a consumer name", () => {
      // GIVEN
      const calculate = defineRpc(queue, { request, response });
      const consumerQueue = defineQueue("consumer-queue", { type: "classic", durable: false });
      const consumer = { queue: consumerQueue, message: request };

      // WHEN / THEN
      expect(() =>
        defineContract({
          consumers: { calculate: consumer },
          rpcs: { calculate },
        }),
      ).toThrow(/name collision between consumers and rpcs/);
    });

    it("auto-extracts the RPC's dead-letter exchange when configured", () => {
      // GIVEN
      const dlx = { name: "rpc.dlx", type: "topic" as const, durable: true };
      const dlqQueue = defineQueue("rpc.with-dlx", {
        type: "classic",
        durable: false,
        deadLetter: { exchange: dlx },
      });
      const calculate = defineRpc(dlqQueue, { request, response });

      // WHEN
      const contract = defineContract({
        rpcs: { calculate },
      });

      // THEN
      expect(contract.exchanges).toMatchObject({ "rpc.dlx": dlx });
    });
  });
});
