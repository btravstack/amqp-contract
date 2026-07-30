import amqp, {
  type AmqpConnectionManager,
  type AmqpConnectionManagerOptions,
  type ConnectionUrl,
} from "amqp-connection-manager";

/**
 * A held reference to a pooled connection.
 */
export type ConnectionLease = {
  /** The shared connection this lease holds a reference to. */
  connection: AmqpConnectionManager;
  /**
   * Release this lease. Idempotent — a double `release()` (e.g. a client
   * whose `close()` runs on both a `finally` and an error path) is a no-op,
   * so it can never underflow the pool's reference count and close a
   * connection out from under another live client.
   */
  release: () => Promise<void>;
};

/**
 * Connection manager singleton for sharing AMQP connections across clients.
 *
 * This singleton implements connection pooling to avoid creating multiple connections
 * to the same broker, which is a RabbitMQ best practice. Connections are identified
 * by their URLs and connection options; each {@link acquire} returns a lease,
 * and the connection is only closed when every lease has been released.
 *
 * @example
 * ```typescript
 * const manager = ConnectionManagerSingleton.getInstance();
 * const lease = manager.acquire(['amqp://localhost']);
 * // ... use lease.connection ...
 * await lease.release();
 * ```
 */
export class ConnectionManagerSingleton {
  private static instance: ConnectionManagerSingleton;
  private connections: Map<string, AmqpConnectionManager> = new Map();
  /** Stable identity tokens for function-valued options (see {@link deepSort}). */
  private readonly functionIdentities = new WeakMap<object, number>();
  private nextFunctionIdentity = 1;
  private refCounts: Map<string, number> = new Map();

  private constructor() {}

  /**
   * Get the singleton instance of the connection manager.
   *
   * @returns The singleton instance
   */
  static getInstance(): ConnectionManagerSingleton {
    if (!ConnectionManagerSingleton.instance) {
      ConnectionManagerSingleton.instance = new ConnectionManagerSingleton();
    }
    return ConnectionManagerSingleton.instance;
  }

  /**
   * Acquire a lease on the pooled connection for the given URLs and options.
   *
   * If a connection already exists with the same URLs and options, it is
   * reused; otherwise a new one is created. The returned lease's `release`
   * is idempotent, and when the LAST lease is released the connection is
   * removed from the pool *before* it is closed — a concurrent `acquire`
   * during the close therefore creates a fresh connection instead of
   * receiving one that is shutting down (and would never reconnect).
   *
   * @param urls - AMQP broker URL(s)
   * @param connectionOptions - Optional connection configuration
   */
  acquire(
    urls: ConnectionUrl[],
    connectionOptions?: AmqpConnectionManagerOptions,
  ): ConnectionLease {
    const key = this.createConnectionKey(urls, connectionOptions);

    let connection = this.connections.get(key);
    if (connection === undefined) {
      connection = amqp.connect(urls, connectionOptions);
      this.connections.set(key, connection);
      this.refCounts.set(key, 0);
    }
    this.refCounts.set(key, (this.refCounts.get(key) ?? 0) + 1);

    const leased = connection;
    let released = false;
    return {
      connection: leased,
      release: async (): Promise<void> => {
        if (released) return;
        released = true;

        // If another acquire re-created this key after a full release cycle,
        // the map may hold a DIFFERENT connection — only act on pool state
        // when it still refers to the connection this lease was minted for.
        if (this.connections.get(key) !== leased) return;

        const refCount = this.refCounts.get(key) ?? 0;
        if (refCount <= 1) {
          // Last lease: remove from the pool FIRST (synchronously), then
          // close. Entries are gone even if `close()` rejects, so the key is
          // never poisoned by a failed close.
          this.connections.delete(key);
          this.refCounts.delete(key);
          await leased.close();
        } else {
          this.refCounts.set(key, refCount - 1);
        }
      },
    };
  }

  /**
   * Create a unique key for a connection based on URLs and options.
   *
   * The key is deterministic: same URLs and options always produce the same key,
   * enabling connection reuse.
   *
   * @param urls - AMQP broker URL(s)
   * @param connectionOptions - Optional connection configuration
   * @returns A unique string key identifying the connection
   */
  private createConnectionKey(
    urls: ConnectionUrl[],
    connectionOptions?: AmqpConnectionManagerOptions,
  ): string {
    // Create a deterministic key from URLs and options.
    // Use JSON.stringify for URLs to avoid ambiguity (e.g., ['a,b'] vs ['a', 'b']).
    //
    // INTENTIONAL: URL order is part of the connection key — `['a','b']` and
    // `['b','a']` get *different* pooled connections. amqp-connection-manager
    // treats the URL list as a failover list where the first entry is the
    // preferred broker, so two callers passing the same set of URLs in
    // different orders are asking for semantically different connections (one
    // prefers `a`, the other prefers `b`). Do not "fix" this by sorting — it
    // would silently merge those two connections and pin one caller's failover
    // preference onto the other.
    const urlsStr = JSON.stringify(urls);
    // Sort object keys for deterministic serialization of connection options
    const optsStr = connectionOptions ? this.serializeOptions(connectionOptions) : "";
    return `${urlsStr}::${optsStr}`;
  }

  /**
   * Serialize connection options to a deterministic string.
   *
   * @param options - Connection options to serialize
   * @returns A JSON string with sorted keys for deterministic comparison
   */
  private serializeOptions(options: AmqpConnectionManagerOptions): string {
    // Create a deterministic string representation by deeply sorting all object keys
    const sorted = this.deepSort(options);
    return JSON.stringify(sorted);
  }

  /**
   * Deep sort an object's keys for deterministic serialization.
   *
   * @param value - The value to deep sort (can be object, array, or primitive)
   * @returns The value with all object keys sorted alphabetically
   */
  private deepSort(value: unknown): unknown {
    if (Array.isArray(value)) {
      return value.map((item) => this.deepSort(item));
    }

    // JSON.stringify silently drops function values, so two option bags
    // differing only in a callback (`findServers`, a `credentials` object
    // whose behavior lives in its `response()` method) would collapse onto
    // one pooled connection. Substitute a stable per-reference identity
    // token instead: options carrying a function are shared only when it is
    // literally the same function.
    if (typeof value === "function") {
      let id = this.functionIdentities.get(value);
      if (id === undefined) {
        id = this.nextFunctionIdentity++;
        this.functionIdentities.set(value, id);
      }
      return `[function#${id}]`;
    }

    if (value !== null && typeof value === "object") {
      const obj = value as Record<string, unknown>;
      const sortedKeys = Object.keys(obj).sort();
      const result: Record<string, unknown> = {};

      for (const key of sortedKeys) {
        result[key] = this.deepSort(obj[key]);
      }

      return result;
    }

    return value;
  }

  /**
   * Get the number of active pooled connections.
   *
   * @internal
   */
  _getConnectionCountForTesting(): number {
    return this.connections.size;
  }

  /**
   * Reset all cached connections (for testing purposes)
   * @internal
   */
  async _resetForTesting(): Promise<void> {
    // Close all connections before clearing
    const closePromises = Array.from(this.connections.values()).map((conn) => conn.close());
    await Promise.all(closePromises);
    this.connections.clear();
    this.refCounts.clear();
  }
}

/**
 * Number of active pooled connections. Test-only helper — exposed in lieu of
 * the underlying singleton, which is intentionally not part of the public API
 * (mutating it from outside the library can break in-flight clients sharing a
 * connection).
 *
 * @internal
 */
export function _internal_getConnectionCount(): number {
  return ConnectionManagerSingleton.getInstance()._getConnectionCountForTesting();
}

/**
 * Close every pooled connection and clear ref-counts. Test-only helper.
 *
 * @internal
 */
export function _internal_resetConnections(): Promise<void> {
  return ConnectionManagerSingleton.getInstance()._resetForTesting();
}
