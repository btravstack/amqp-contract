import { once } from "node:events";
import net, { type AddressInfo } from "node:net";

import type { ContractDefinition } from "@amqp-contract/contract";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AmqpClient, type AmqpClientOptions } from "./amqp-client.js";
import { ConnectionError } from "./errors.js";
import { startOrClose } from "./lifecycle.js";

/**
 * A broker that accepts the TCP connection and never answers the AMQP
 * handshake. No Docker: the peer is a `node:net` server that reads and never
 * writes.
 */
async function silentPeer() {
  const sockets = new Set<net.Socket>();
  let accepted = 0;
  const server = net.createServer((socket) => {
    accepted += 1;
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => undefined);
    socket.resume();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  return {
    url: `amqp://127.0.0.1:${port}`,
    accepted: () => accepted,
    open: () => sockets.size,
    close: () => {
      for (const socket of sockets) socket.destroy();
      server.close();
    },
  };
}

async function refusedUrl(): Promise<string> {
  const server = net.createServer().listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  server.close();
  await once(server, "close");
  return `amqp://127.0.0.1:${port}`;
}

/** What `TypedAmqpClient.create` / `TypedAmqpWorker.create` do with core. */
function create(options: AmqpClientOptions) {
  return startOrClose(
    () => new AmqpClient({} as ContractDefinition, options),
    (client) => client.waitForConnect(),
    { name: "client" },
  );
}

async function timed<T>(work: PromiseLike<T>): Promise<{ result: T; elapsedMs: number }> {
  const started = performance.now();
  const result = await work;
  return { result, elapsedMs: performance.now() - started };
}

describe("create() against a peer that never answers the AMQP handshake", () => {
  const peers: Array<{ close: () => void }> = [];
  afterEach(() => {
    for (const peer of peers.splice(0)) peer.close();
  });

  it("answers ConnectionError at connectTimeoutMs, naming the dial's own timeout", async () => {
    // GIVEN
    const peer = await silentPeer();
    peers.push(peer);

    // WHEN
    const { result, elapsedMs } = await timed(create({ urls: [peer.url], connectTimeoutMs: 400 }));

    // THEN
    expect(result).toBeErrWith(
      expect.objectContaining({
        constructor: ConnectionError,
        message: expect.stringContaining("connect ETIMEDOUT"),
      }),
    );
    expect(elapsedMs).toBeLessThan(1_500);
  });

  it("leaves no socket open to the peer once create() has failed", async () => {
    // GIVEN
    const peer = await silentPeer();
    peers.push(peer);

    // WHEN
    await create({ urls: [peer.url], connectTimeoutMs: 400 });

    // THEN
    expect({ accepted: peer.accepted(), open: peer.open() }).toEqual({ accepted: 1, open: 0 });
  });

  it("keeps a caller's own socket timeout, and still answers at connectTimeoutMs", async () => {
    // GIVEN
    const peer = await silentPeer();
    peers.push(peer);

    // WHEN
    const { result, elapsedMs } = await timed(
      create({
        urls: [peer.url],
        connectTimeoutMs: 300,
        connectionOptions: { connectionOptions: { timeout: 60_000 } },
      }),
    );

    // THEN
    expect({
      failed: result.isErr() && result.error instanceof ConnectionError,
      inTime: elapsedMs < 1_500,
      dialStillOpen: peer.open(),
    }).toEqual({ failed: true, inTime: true, dialStillOpen: 1 });
  });

  it("arms no socket timeout when connectTimeoutMs is null, and close() does not wait for the dial", async () => {
    // GIVEN
    const peer = await silentPeer();
    peers.push(peer);
    const client = new AmqpClient({} as ContractDefinition, {
      urls: [peer.url],
      connectTimeoutMs: null,
    });
    await vi.waitUntil(() => peer.accepted() === 1);

    // WHEN
    const closed = await client.close();

    // THEN
    expect({
      closed: closed.isOk(),
      socketTimeout: client.getConnection().connectionOptions?.timeout,
    }).toEqual({ closed: true, socketTimeout: undefined });
  });

  it("still answers a refused port with its ECONNREFUSED cause", async () => {
    // GIVEN
    const url = await refusedUrl();

    // WHEN
    const result = await create({ urls: [url], connectTimeoutMs: 300 });

    // THEN
    expect(result).toBeErrWith(
      expect.objectContaining({
        constructor: ConnectionError,
        message: expect.stringContaining("ECONNREFUSED"),
      }),
    );
  });
});
