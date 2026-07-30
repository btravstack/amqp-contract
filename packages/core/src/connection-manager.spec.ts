import { beforeEach, describe, expect, it, vi } from "vitest";

import { ConnectionManagerSingleton } from "./connection-manager.js";

// The pool never needs a real broker: stub `amqp.connect` to hand back a
// fresh fake manager per call so identity checks distinguish connections.
vi.mock("amqp-connection-manager", () => ({
  default: {
    connect: vi.fn(() => ({
      close: vi.fn(() => Promise.resolve()),
    })),
  },
}));

describe("ConnectionManagerSingleton pooling invariants", () => {
  beforeEach(async () => {
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it("shares one connection across leases with the same key and closes it on the last release", async () => {
    const pool = ConnectionManagerSingleton.getInstance();
    const a = pool.acquire(["amqp://localhost"]);
    const b = pool.acquire(["amqp://localhost"]);

    expect(a.connection).toBe(b.connection);
    expect(pool._getConnectionCountForTesting()).toBe(1);

    await a.release();
    expect(pool._getConnectionCountForTesting()).toBe(1);
    expect(a.connection.close).not.toHaveBeenCalled();

    await b.release();
    expect(pool._getConnectionCountForTesting()).toBe(0);
    expect(b.connection.close).toHaveBeenCalledTimes(1);
  });

  it("a double release is a no-op and cannot close the connection under another live lease", async () => {
    const pool = ConnectionManagerSingleton.getInstance();
    const doubleCloser = pool.acquire(["amqp://localhost"]);
    const survivor = pool.acquire(["amqp://localhost"]);

    await doubleCloser.release();
    await doubleCloser.release(); // the buggy version underflowed here

    // The survivor's shared connection must still be pooled and open.
    expect(pool._getConnectionCountForTesting()).toBe(1);
    expect(survivor.connection.close).not.toHaveBeenCalled();

    await survivor.release();
    expect(pool._getConnectionCountForTesting()).toBe(0);
  });

  it("an acquire racing the last release gets a FRESH connection, not the closing one", async () => {
    const pool = ConnectionManagerSingleton.getInstance();
    const last = pool.acquire(["amqp://localhost"]);

    // Make close slow so the release is genuinely in flight.
    let finishClose!: () => void;
    (last.connection.close as ReturnType<typeof vi.fn>).mockReturnValue(
      new Promise<void>((resolve) => {
        finishClose = resolve;
      }),
    );

    const releasing = last.release();
    // The entry is removed synchronously before close() is awaited, so a
    // concurrent acquire creates a new connection instead of receiving the
    // one that is shutting down (which would never reconnect).
    const fresh = pool.acquire(["amqp://localhost"]);
    expect(fresh.connection).not.toBe(last.connection);

    finishClose();
    await releasing;

    // The racing acquire's entry must have survived the release.
    expect(pool._getConnectionCountForTesting()).toBe(1);
    await fresh.release();
    expect(pool._getConnectionCountForTesting()).toBe(0);
  });

  it("a rejecting close still evicts the entry so the key is not poisoned", async () => {
    const pool = ConnectionManagerSingleton.getInstance();
    const lease = pool.acquire(["amqp://localhost"]);
    (lease.connection.close as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("close failed"),
    );

    await expect(lease.release()).rejects.toThrow("close failed");
    expect(pool._getConnectionCountForTesting()).toBe(0);

    // The key is reusable: the next acquire creates a fresh connection.
    const fresh = pool.acquire(["amqp://localhost"]);
    expect(fresh.connection).not.toBe(lease.connection);
    await fresh.release();
  });
});

describe("connection key: function-valued options", () => {
  beforeEach(async () => {
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  it("INVARIANT: two acquires differing only in a function option get separate connections", () => {
    // JSON.stringify drops function values, so before the fix two clients
    // with different `findServers` callbacks (or different amqplib
    // `credentials` objects, whose behavior lives in a `response()` method)
    // collapsed onto one pooled connection — pinning the second caller to
    // the first caller's behavior.
    const pool = ConnectionManagerSingleton.getInstance();
    const a = pool.acquire(["amqp://localhost"], { findServers: () => "amqp://a" });
    const b = pool.acquire(["amqp://localhost"], { findServers: () => "amqp://b" });

    expect(a.connection).not.toBe(b.connection);
    expect(pool._getConnectionCountForTesting()).toBe(2);
  });

  it("the same function reference still shares the pooled connection", () => {
    const pool = ConnectionManagerSingleton.getInstance();
    const findServers = () => "amqp://a";
    const a = pool.acquire(["amqp://localhost"], { findServers });
    const b = pool.acquire(["amqp://localhost"], { findServers });

    expect(a.connection).toBe(b.connection);
    expect(pool._getConnectionCountForTesting()).toBe(1);
  });
});
