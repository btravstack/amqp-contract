import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineContract } from "./contract.js";
import { defineMessage } from "./message.js";
import { defineQueue } from "./queue.js";
import { defineRpcClient, defineRpcServer, isRpcClientConfig, isRpcServerConfig } from "./rpc.js";

describe("RPC builders", () => {
  const queue = defineQueue("rpc.calculate", { type: "classic", durable: false });
  const request = defineMessage(z.object({ a: z.number(), b: z.number() }));
  const response = defineMessage(z.object({ sum: z.number() }));

  describe("defineRpcServer", () => {
    it("creates an RpcServerConfig with both schemas and the queue", () => {
      // WHEN
      const server = defineRpcServer(queue, { request, response });

      // THEN
      expect(server).toEqual({
        __brand: "RpcServerConfig",
        queue,
        requestMessage: request,
        responseMessage: response,
        consumer: {
          queue,
          message: request,
          responseMessage: response,
        },
      });
    });

    it("is recognised by isRpcServerConfig", () => {
      // WHEN
      const server = defineRpcServer(queue, { request, response });

      // THEN
      expect({ server: isRpcServerConfig(server), empty: isRpcServerConfig({}) }).toEqual({
        server: true,
        empty: false,
      });
    });
  });

  describe("defineRpcClient", () => {
    it("creates an RpcClientConfig that publishes to the default exchange with the queue name as routing key", () => {
      // GIVEN
      const server = defineRpcServer(queue, { request, response });

      // WHEN
      const client = defineRpcClient(server);

      // THEN
      expect(client).toEqual({
        __brand: "RpcClientConfig",
        requestMessage: request,
        responseMessage: response,
        publisher: {
          exchange: { name: "", type: "direct" },
          routingKey: "rpc.calculate",
          message: request,
          responseMessage: response,
        },
      });
    });

    it("is recognised by isRpcClientConfig", () => {
      // GIVEN
      const server = defineRpcServer(queue, { request, response });

      // WHEN
      const client = defineRpcClient(server);

      // THEN
      expect({ client: isRpcClientConfig(client), empty: isRpcClientConfig({}) }).toEqual({
        client: true,
        empty: false,
      });
    });
  });

  describe("defineContract integration", () => {
    it("auto-extracts the RPC server's queue and the RPC client's publisher", () => {
      // GIVEN
      const server = defineRpcServer(queue, { request, response });
      const client = defineRpcClient(server);

      // WHEN
      const contract = defineContract({
        consumers: { calculate: server },
        publishers: { calculate: client },
      });

      // THEN
      expect(contract).toMatchObject({
        // Queue registered, default exchange skipped
        queues: { "rpc.calculate": queue },
        // Consumer carries the response schema
        consumers: { calculate: { responseMessage: response } },
        // Publisher targets the default exchange with the queue name as routing key
        publishers: {
          calculate: {
            exchange: { name: "", type: "direct" },
            routingKey: "rpc.calculate",
            responseMessage: response,
          },
        },
        // No binding is created — the default exchange handles routing implicitly
        bindings: {},
      });
    });
  });
});
