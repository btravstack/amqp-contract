import type { EventEmitter } from "node:events";

import type { ContractDefinition } from "@amqp-contract/contract";
import {
  type Context,
  type ContextManager,
  context,
  propagation,
  ROOT_CONTEXT,
  type Span,
  trace,
  TraceFlags,
} from "@opentelemetry/api";
import type { ConsumeMessage } from "amqplib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AmqpClient } from "./amqp-client.js";
import { ConnectionManagerSingleton } from "./connection-manager.js";
import { _internal_resetTelemetryCache, runWithTraceContext } from "./telemetry.js";

/**
 * A consumer's span must continue its publisher's trace: core stamps the
 * active context into the message headers on publish and runs each delivery
 * inside the context extracted from them. Without an OTel SDK all of it is a
 * no-op, and a broken propagator never reaches the data path.
 */

type FakeWrapper = EventEmitter & {
  waitForConnect: () => Promise<void>;
  close: () => Promise<void>;
  publish: ReturnType<typeof vi.fn>;
  consume: ReturnType<typeof vi.fn>;
};
const fakes = vi.hoisted(() => ({ wrapper: undefined as unknown }));
const wrapper = (): FakeWrapper => fakes.wrapper as FakeWrapper;

vi.mock("amqp-connection-manager", async () => {
  const { EventEmitter: Emitter } = await import("node:events");
  const w = new Emitter() as FakeWrapper;
  w.waitForConnect = () => Promise.resolve();
  w.close = () => Promise.resolve();
  w.publish = vi.fn(() => Promise.resolve(true));
  w.consume = vi.fn(() => Promise.resolve({ consumerTag: "tag" }));
  fakes.wrapper = w;
  return {
    default: {
      connect: vi.fn(() => ({
        createChannel: vi.fn(() => w),
        on: vi.fn(),
        removeListener: vi.fn(),
        close: vi.fn(() => Promise.resolve()),
      })),
    },
  };
});

/** Synchronous stack-based context manager — enough for `context.with`. */
class StackContextManager implements ContextManager {
  private stack: Context[] = [ROOT_CONTEXT];
  active(): Context {
    return this.stack.at(-1) ?? ROOT_CONTEXT;
  }
  with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
    ctx: Context,
    fn: F,
    thisArg?: ThisParameterType<F>,
    ...args: A
  ): ReturnType<F> {
    this.stack.push(ctx);
    try {
      return fn.call(thisArg, ...args);
    } finally {
      this.stack.pop();
    }
  }
  bind<T>(_ctx: Context, target: T): T {
    return target;
  }
  enable(): this {
    return this;
  }
  disable(): this {
    return this;
  }
}

/** A one-header propagator: `x-trace-id` carries the active span's traceId. */
const traceIdPropagator = {
  inject(ctx: Context, carrier: Record<string, unknown>) {
    const span = trace.getSpan(ctx);
    if (span) carrier["x-trace-id"] = span.spanContext().traceId;
  },
  extract(ctx: Context, carrier: Record<string, unknown>) {
    const traceId = carrier["x-trace-id"];
    return typeof traceId === "string"
      ? trace.setSpan(
          ctx,
          trace.wrapSpanContext({ traceId, spanId: "b7ad6b7169203331", traceFlags: 1 }),
        )
      : ctx;
  },
  fields: () => ["x-trace-id"],
};

const TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
const producerSpan = (): Span =>
  trace.wrapSpanContext({
    traceId: TRACE_ID,
    spanId: "00f067aa0ba902b7",
    traceFlags: TraceFlags.SAMPLED,
  });

const contract = {} as ContractDefinition;
const target = { exchange: "orders", routingKey: "order.created" };

describe("trace context propagation", () => {
  beforeEach(async () => {
    wrapper().publish.mockClear();
    wrapper().consume.mockClear();
    _internal_resetTelemetryCache();
    await ConnectionManagerSingleton.getInstance()._resetForTesting();
  });

  afterEach(() => {
    propagation.disable();
    context.disable();
  });

  it("is a no-op without an SDK: headers are left untouched", async () => {
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await runWithTraceContext(undefined, producerSpan(), () =>
      client.publish(target, { id: "1" }, { headers: { a: 1 } }),
    );

    expect((wrapper().publish.mock.calls[0] as unknown[])[3]).toEqual({ headers: { a: 1 } });
    await client.close();
  });

  it("INVARIANT: publish stamps the active span's context, and consume runs the callback inside it", async () => {
    context.setGlobalContextManager(new StackContextManager());
    propagation.setGlobalPropagator(traceIdPropagator);
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    await runWithTraceContext(undefined, producerSpan(), () =>
      client.publish(target, { id: "1" }, { headers: { a: 1 } }),
    );
    const published = (wrapper().publish.mock.calls[0] as unknown[])[3] as {
      headers: Record<string, unknown>;
    };

    let seenTraceId: string | undefined;
    await client.consume("q", () => {
      seenTraceId = trace.getSpan(context.active())?.spanContext().traceId;
    });
    const deliver = (wrapper().consume.mock.calls[0] as unknown[])[1] as (
      msg: ConsumeMessage,
    ) => void;
    deliver({ properties: { headers: published.headers } } as unknown as ConsumeMessage);

    expect(published.headers).toEqual({ a: 1, "x-trace-id": TRACE_ID });
    expect(seenTraceId).toBe(TRACE_ID);
    await client.close();
  });

  it("INVARIANT: a throwing propagator never reaches the data path", async () => {
    context.setGlobalContextManager(new StackContextManager());
    propagation.setGlobalPropagator({
      inject: () => {
        throw new Error("propagator bug: inject");
      },
      extract: () => {
        throw new Error("propagator bug: extract");
      },
      fields: () => [],
    });
    const client = new AmqpClient(contract, { urls: ["amqp://localhost"] });

    const published = await client.publish(target, { id: "1" });
    const ran = runWithTraceContext({ "x-trace-id": "x" }, undefined, () => "handler ran");

    expect([published.isOk(), ran]).toEqual([true, "handler ran"]);
    await client.close();
  });

  it("rethrows a throw from the wrapped function itself, unchanged", () => {
    context.setGlobalContextManager(new StackContextManager());
    const bug = new Error("handler bug");

    expect(() =>
      runWithTraceContext(undefined, producerSpan(), () => {
        throw bug;
      }),
    ).toThrow(bug);
  });
});
