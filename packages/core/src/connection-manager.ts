import amqp, {
  AmqpConnectionManager,
  AmqpConnectionManagerOptions,
  ConnectionUrl,
} from "amqp-connection-manager";

/**
 * Connection manager singleton for sharing AMQP connections across clients.
 *
 * This singleton implements connection pooling to avoid creating multiple connections
 * to the same broker, which is a RabbitMQ best practice. Connections are identified
 * by their URLs and connection options, and reference counting ensures connections
 * are only closed when all clients have released them.
 *
 * @example
 * ```typescript
 * const manager = ConnectionManagerSingleton.getInstance();
 * const connection = manager.getConnection(['amqp://localhost']);
 * // ... use connection ...
 * await manager.releaseConnection(['amqp://localhost']);
 * ```
 */
export class ConnectionManagerSingleton {
  private static instance: ConnectionManagerSingleton;
  private connections: Map<string, AmqpConnectionManager> = new Map();
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
   * Get or create a connection for the given URLs and options.
   *
   * If a connection already exists with the same URLs and options, it is reused
   * and its reference count is incremented. Otherwise, a new connection is created.
   *
   * @param urls - AMQP broker URL(s)
   * @param connectionOptions - Optional connection configuration
   * @returns The AMQP connection manager instance
   */
  getConnection(
    urls: ConnectionUrl[],
    connectionOptions?: AmqpConnectionManagerOptions,
  ): AmqpConnectionManager {
    // Create a key based on URLs and connection options
    const key = this.createConnectionKey(urls, connectionOptions);

    if (!this.connections.has(key)) {
      const connection = amqp.connect(urls, connectionOptions);
      this.connections.set(key, connection);
      this.refCounts.set(key, 0);
    }

    // Increment reference count
    this.refCounts.set(key, (this.refCounts.get(key) ?? 0) + 1);

    return this.connections.get(key)!;
  }

  /**
   * Release a connection reference.
   *
   * Decrements the reference count for the connection. If the count reaches zero,
   * the connection is closed and removed from the pool.
   *
   * @param urls - AMQP broker URL(s) used to identify the connection
   * @param connectionOptions - Optional connection configuration used to identify the connection
   * @returns A promise that resolves when the connection is released (and closed if necessary)
   */
  async releaseConnection(
    urls: ConnectionUrl[],
    connectionOptions?: AmqpConnectionManagerOptions,
  ): Promise<void> {
    const key = this.createConnectionKey(urls, connectionOptions);
    const refCount = this.refCounts.get(key) ?? 0;

    if (refCount <= 1) {
      // Last reference - close and remove connection
      const connection = this.connections.get(key);
      if (connection) {
        await connection.close();
        this.connections.delete(key);
        this.refCounts.delete(key);
      }
    } else {
      // Decrement reference count
      this.refCounts.set(key, refCount - 1);
    }
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
    // Create a deterministic key from URLs and options
    // Use JSON.stringify for URLs to avoid ambiguity (e.g., ['a,b'] vs ['a', 'b'])
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
export function _getConnectionCountForTesting(): number {
  return ConnectionManagerSingleton.getInstance()._getConnectionCountForTesting();
}

/**
 * Close every pooled connection and clear ref-counts. Test-only helper.
 *
 * @internal
 */
export function _resetConnectionsForTesting(): Promise<void> {
  return ConnectionManagerSingleton.getInstance()._resetForTesting();
}
