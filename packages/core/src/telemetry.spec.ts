import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  type TelemetryProvider,
  _internal_resetTelemetryCache,
  defaultTelemetryProvider,
  endSpanError,
  endSpanSuccess,
  recordConsumeMetric,
  recordPublishMetric,
  startConsumeSpan,
  startPublishSpan,
} from "./telemetry.js";

/** Provider where all instruments are unavailable */
const noopProvider: TelemetryProvider = {
  getTracer: () => undefined,
  getPublishCounter: () => undefined,
  getConsumeCounter: () => undefined,
  getPublishLatencyHistogram: () => undefined,
  getConsumeLatencyHistogram: () => undefined,
  getLateRpcReplyCounter: () => undefined,
};

function createMockSpan() {
  return { end: vi.fn(), setStatus: vi.fn(), recordException: vi.fn(), setAttribute: vi.fn() };
}

function createMockTracer() {
  const span = createMockSpan();
  const tracer = { startSpan: vi.fn().mockReturnValue(span) };
  return { tracer, span };
}

function providerWithTracer(
  tracer: ReturnType<typeof createMockTracer>["tracer"],
): TelemetryProvider {
  return {
    ...noopProvider,
    getTracer: () => tracer as unknown as ReturnType<TelemetryProvider["getTracer"]>,
  };
}

describe("Telemetry", () => {
  beforeEach(() => {
    _internal_resetTelemetryCache();
  });

  afterEach(() => {
    _internal_resetTelemetryCache();
  });

  describe("defaultTelemetryProvider", () => {
    it("should return a TelemetryProvider object", () => {
      expect(defaultTelemetryProvider).toBeDefined();
      expect(typeof defaultTelemetryProvider.getTracer).toBe("function");
      expect(typeof defaultTelemetryProvider.getPublishCounter).toBe("function");
      expect(typeof defaultTelemetryProvider.getConsumeCounter).toBe("function");
      expect(typeof defaultTelemetryProvider.getPublishLatencyHistogram).toBe("function");
      expect(typeof defaultTelemetryProvider.getConsumeLatencyHistogram).toBe("function");
    });

    it("should return tracer when OpenTelemetry is available", () => {
      // Since @opentelemetry/api is installed as a dev dependency,
      // the provider should return a tracer
      const tracer = defaultTelemetryProvider.getTracer();
      expect(tracer).toBeDefined();
    });
  });

  describe("startPublishSpan", () => {
    it("should return undefined when tracer is not available", () => {
      const span = startPublishSpan(noopProvider, "test-exchange", "test.key");
      expect(span).toBeUndefined();
    });

    it("should create span with correct attributes when tracer is available", () => {
      const { tracer, span } = createMockTracer();

      const result = startPublishSpan(providerWithTracer(tracer), "test-exchange", "test.key", {
        "amqp.publisher.name": "testPublisher",
      });

      expect(result).toBe(span);
      expect(tracer.startSpan).toHaveBeenCalledTimes(1);
      expect(tracer.startSpan).toHaveBeenCalledWith("test-exchange publish", {
        kind: 3, // SpanKind.PRODUCER
        attributes: expect.objectContaining({
          "messaging.system": "rabbitmq",
          "messaging.destination.name": "test-exchange",
          "messaging.destination.kind": "exchange",
          "messaging.operation": "publish",
          "messaging.rabbitmq.destination.routing_key": "test.key",
          "amqp.publisher.name": "testPublisher",
        }),
      });
    });

    it("should not include routing key when undefined", () => {
      const { tracer } = createMockTracer();

      startPublishSpan(providerWithTracer(tracer), "test-exchange", undefined);

      expect(tracer.startSpan).toHaveBeenCalledWith("test-exchange publish", {
        kind: 3,
        attributes: expect.not.objectContaining({
          "messaging.rabbitmq.destination.routing_key": expect.anything(),
        }),
      });
    });
  });

  describe("startConsumeSpan", () => {
    it("should return undefined when tracer is not available", () => {
      const span = startConsumeSpan(noopProvider, "test-queue", "testConsumer");
      expect(span).toBeUndefined();
    });

    it("should create span with correct attributes when tracer is available", () => {
      const { tracer, span } = createMockTracer();

      const result = startConsumeSpan(providerWithTracer(tracer), "test-queue", "testConsumer", {
        "messaging.rabbitmq.message.delivery_tag": 1,
      });

      expect(result).toBe(span);
      expect(tracer.startSpan).toHaveBeenCalledTimes(1);
      expect(tracer.startSpan).toHaveBeenCalledWith("test-queue process", {
        kind: 4, // SpanKind.CONSUMER
        attributes: expect.objectContaining({
          "messaging.system": "rabbitmq",
          "messaging.destination.name": "test-queue",
          "messaging.destination.kind": "queue",
          "messaging.operation": "process",
          "amqp.consumer.name": "testConsumer",
          "messaging.rabbitmq.message.delivery_tag": 1,
        }),
      });
    });
  });

  describe("endSpanSuccess", () => {
    it("should do nothing when span is undefined", () => {
      expect(() => endSpanSuccess(undefined)).not.toThrow();
    });

    it("should end span without status when OpenTelemetry is not available", () => {
      const mockSpan = createMockSpan();

      endSpanSuccess(mockSpan as unknown as Parameters<typeof endSpanSuccess>[0]);

      expect(mockSpan.end).toHaveBeenCalledTimes(1);
    });
  });

  describe("endSpanError", () => {
    it("should do nothing when span is undefined", () => {
      expect(() => endSpanError(undefined, new Error("Test error"))).not.toThrow();
    });

    it("should end span when called with error", () => {
      const mockSpan = createMockSpan();

      endSpanError(mockSpan as unknown as Parameters<typeof endSpanError>[0], new Error("Test"));

      expect(mockSpan.end).toHaveBeenCalledTimes(1);
    });
  });

  describe("recordPublishMetric", () => {
    it("should do nothing when counter and histogram are undefined", () => {
      expect(() =>
        recordPublishMetric(noopProvider, "test-exchange", "test.key", true, 100),
      ).not.toThrow();
    });

    it("should record counter and histogram when available", () => {
      const mockCounter = { add: vi.fn() };
      const mockHistogram = { record: vi.fn() };

      const provider: TelemetryProvider = {
        ...noopProvider,
        getPublishCounter: () =>
          mockCounter as unknown as ReturnType<TelemetryProvider["getPublishCounter"]>,
        getPublishLatencyHistogram: () =>
          mockHistogram as unknown as ReturnType<TelemetryProvider["getPublishLatencyHistogram"]>,
      };

      recordPublishMetric(provider, "test-exchange", "test.key", true, 150);

      expect(mockCounter.add).toHaveBeenCalledWith(1, {
        "messaging.system": "rabbitmq",
        "messaging.destination.name": "test-exchange",
        "messaging.rabbitmq.destination.routing_key": "test.key",
        success: true,
      });

      expect(mockHistogram.record).toHaveBeenCalledWith(150, {
        "messaging.system": "rabbitmq",
        "messaging.destination.name": "test-exchange",
        "messaging.rabbitmq.destination.routing_key": "test.key",
        success: true,
      });
    });
  });

  describe("throwing telemetry providers", () => {
    // Telemetry runs inside the client's and worker's Result chains — a
    // throwing user-supplied provider must degrade to "no telemetry", never
    // escape into the data path (where it would convert a successful publish
    // into a defect or crash the consume callback).
    const throwingProvider: TelemetryProvider = {
      getTracer: () => {
        throw new Error("provider bug: getTracer");
      },
      getPublishCounter: () => {
        throw new Error("provider bug: getPublishCounter");
      },
      getConsumeCounter: () => {
        throw new Error("provider bug: getConsumeCounter");
      },
      getPublishLatencyHistogram: () => {
        throw new Error("provider bug: getPublishLatencyHistogram");
      },
      getConsumeLatencyHistogram: () => {
        throw new Error("provider bug: getConsumeLatencyHistogram");
      },
      getLateRpcReplyCounter: () => {
        throw new Error("provider bug: getLateRpcReplyCounter");
      },
    };

    it("startPublishSpan/startConsumeSpan return undefined instead of throwing", () => {
      expect(startPublishSpan(throwingProvider, "x", "rk")).toBeUndefined();
      expect(startConsumeSpan(throwingProvider, "q", "consumer")).toBeUndefined();
    });

    it("record* helpers swallow provider throws", () => {
      expect(() => recordPublishMetric(throwingProvider, "x", "rk", true, 10)).not.toThrow();
      expect(() => recordConsumeMetric(throwingProvider, "q", "consumer", true, 10)).not.toThrow();
    });

    it("endSpanSuccess/endSpanError swallow a throwing span", () => {
      const throwingSpan = {
        setStatus: () => {
          throw new Error("span bug");
        },
        setAttribute: () => {
          throw new Error("span bug");
        },
        recordException: () => {
          throw new Error("span bug");
        },
        end: () => {
          throw new Error("span bug");
        },
      };
      const span = throwingSpan as unknown as ReturnType<typeof startPublishSpan>;
      expect(() => endSpanSuccess(span)).not.toThrow();
      expect(() => endSpanError(span, new Error("op failed"))).not.toThrow();
    });
  });

  describe("recordConsumeMetric", () => {
    it("should do nothing when counter and histogram are undefined", () => {
      expect(() =>
        recordConsumeMetric(noopProvider, "test-queue", "testConsumer", false, 200),
      ).not.toThrow();
    });

    it("should record counter and histogram when available", () => {
      const mockCounter = { add: vi.fn() };
      const mockHistogram = { record: vi.fn() };

      const provider: TelemetryProvider = {
        ...noopProvider,
        getConsumeCounter: () =>
          mockCounter as unknown as ReturnType<TelemetryProvider["getConsumeCounter"]>,
        getConsumeLatencyHistogram: () =>
          mockHistogram as unknown as ReturnType<TelemetryProvider["getConsumeLatencyHistogram"]>,
      };

      recordConsumeMetric(provider, "test-queue", "testConsumer", false, 250);

      expect(mockCounter.add).toHaveBeenCalledWith(1, {
        "messaging.system": "rabbitmq",
        "messaging.destination.name": "test-queue",
        "amqp.consumer.name": "testConsumer",
        success: false,
      });

      expect(mockHistogram.record).toHaveBeenCalledWith(250, {
        "messaging.system": "rabbitmq",
        "messaging.destination.name": "test-queue",
        "amqp.consumer.name": "testConsumer",
        success: false,
      });
    });
  });
});
