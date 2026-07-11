import { createRequire } from "node:module";
import {
  type Attributes,
  type Counter,
  type Histogram,
  type Span,
  type Tracer,
} from "@opentelemetry/api";

/**
 * SpanKind values from OpenTelemetry.
 * Defined as constants to avoid runtime dependency when types are used.
 * @see https://opentelemetry.io/docs/specs/otel/trace/api/#spankind
 */
const SpanKind = {
  /** Producer span represents a message producer */
  PRODUCER: 3,
  /** Consumer span represents a message consumer */
  CONSUMER: 4,
} as const;

/**
 * Semantic conventions for AMQP messaging following OpenTelemetry standards.
 * @see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
 */
export const MessagingSemanticConventions = {
  // Messaging attributes
  MESSAGING_SYSTEM: "messaging.system",
  MESSAGING_DESTINATION: "messaging.destination.name",
  MESSAGING_DESTINATION_KIND: "messaging.destination.kind",
  MESSAGING_OPERATION: "messaging.operation",

  // AMQP/RabbitMQ specific attributes
  MESSAGING_RABBITMQ_ROUTING_KEY: "messaging.rabbitmq.destination.routing_key",
  MESSAGING_RABBITMQ_MESSAGE_DELIVERY_TAG: "messaging.rabbitmq.message.delivery_tag",
  AMQP_PUBLISHER_NAME: "amqp.publisher.name",
  AMQP_CONSUMER_NAME: "amqp.consumer.name",

  // Error attributes
  ERROR_TYPE: "error.type",

  // Values
  MESSAGING_SYSTEM_RABBITMQ: "rabbitmq",
  MESSAGING_DESTINATION_KIND_EXCHANGE: "exchange",
  MESSAGING_DESTINATION_KIND_QUEUE: "queue",
  MESSAGING_OPERATION_PUBLISH: "publish",
  MESSAGING_OPERATION_PROCESS: "process",
} as const;

/**
 * Telemetry provider for AMQP operations.
 * Uses lazy loading to gracefully handle cases where OpenTelemetry is not installed.
 */
export type TelemetryProvider = {
  /**
   * Get a tracer instance for creating spans.
   * Returns undefined if OpenTelemetry is not available.
   */
  getTracer: () => Tracer | undefined;

  /**
   * Get a counter for messages published.
   * Returns undefined if OpenTelemetry is not available.
   */
  getPublishCounter: () => Counter | undefined;

  /**
   * Get a counter for messages consumed.
   * Returns undefined if OpenTelemetry is not available.
   */
  getConsumeCounter: () => Counter | undefined;

  /**
   * Get a histogram for publish latency.
   * Returns undefined if OpenTelemetry is not available.
   */
  getPublishLatencyHistogram: () => Histogram | undefined;

  /**
   * Get a histogram for consume/process latency.
   * Returns undefined if OpenTelemetry is not available.
   */
  getConsumeLatencyHistogram: () => Histogram | undefined;

  /**
   * Get a counter for RPC replies that arrive after the caller has gone away
   * (timeout, cancellation, or unknown correlationId). Returns undefined if
   * OpenTelemetry is not available.
   */
  getLateRpcReplyCounter: () => Counter | undefined;
};

/**
 * Instrumentation scope name for amqp-contract.
 */
const INSTRUMENTATION_SCOPE_NAME = "@amqp-contract";

/**
 * Instrumentation scope version, sourced from this package's package.json so
 * the OTel meter version always tracks the released library version. We use
 * `createRequire` rather than a JSON import attribute so the same source builds
 * to ESM, CJS, and runs under bundlers that don't yet understand
 * `import … with { type: "json" }`.
 */
const INSTRUMENTATION_SCOPE_VERSION: string = (() => {
  try {
    const localRequire = createRequire(import.meta.url);
    const pkg = localRequire("../package.json") as { version?: string };
    return pkg.version ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
})();

// Cache for OpenTelemetry API module and instruments
let otelApi: typeof import("@opentelemetry/api") | null | undefined;
let cachedTracer: Tracer | undefined;
let cachedPublishCounter: Counter | undefined;
let cachedConsumeCounter: Counter | undefined;
let cachedPublishLatencyHistogram: Histogram | undefined;
let cachedConsumeLatencyHistogram: Histogram | undefined;
let cachedLateRpcReplyCounter: Counter | undefined;

/**
 * Try to load the OpenTelemetry API module.
 * Returns null if the module is not available.
 */
function tryLoadOpenTelemetryApi(): typeof import("@opentelemetry/api") | null {
  if (otelApi === undefined) {
    try {
      // Dynamic import using require to avoid bundler issues
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      otelApi = require("@opentelemetry/api") as typeof import("@opentelemetry/api");
    } catch {
      otelApi = null;
    }
  }
  return otelApi;
}

/**
 * Get or create a tracer instance.
 */
function getTracer(): Tracer | undefined {
  if (cachedTracer !== undefined) {
    return cachedTracer;
  }

  const api = tryLoadOpenTelemetryApi();
  if (!api) {
    return undefined;
  }

  cachedTracer = api.trace.getTracer(INSTRUMENTATION_SCOPE_NAME, INSTRUMENTATION_SCOPE_VERSION);
  return cachedTracer;
}

/**
 * Get or create a meter and its instruments.
 */
function getMeterInstruments(): {
  publishCounter: Counter | undefined;
  consumeCounter: Counter | undefined;
  publishLatencyHistogram: Histogram | undefined;
  consumeLatencyHistogram: Histogram | undefined;
  lateRpcReplyCounter: Counter | undefined;
} {
  if (cachedPublishCounter !== undefined) {
    return {
      publishCounter: cachedPublishCounter,
      consumeCounter: cachedConsumeCounter,
      publishLatencyHistogram: cachedPublishLatencyHistogram,
      consumeLatencyHistogram: cachedConsumeLatencyHistogram,
      lateRpcReplyCounter: cachedLateRpcReplyCounter,
    };
  }

  const api = tryLoadOpenTelemetryApi();
  if (!api) {
    return {
      publishCounter: undefined,
      consumeCounter: undefined,
      publishLatencyHistogram: undefined,
      consumeLatencyHistogram: undefined,
      lateRpcReplyCounter: undefined,
    };
  }

  const meter = api.metrics.getMeter(INSTRUMENTATION_SCOPE_NAME, INSTRUMENTATION_SCOPE_VERSION);

  cachedPublishCounter = meter.createCounter("amqp.client.messages.published", {
    description: "Number of messages published to AMQP broker",
    unit: "{message}",
  });

  cachedConsumeCounter = meter.createCounter("amqp.worker.messages.consumed", {
    description: "Number of messages consumed from AMQP broker",
    unit: "{message}",
  });

  cachedPublishLatencyHistogram = meter.createHistogram("amqp.client.publish.duration", {
    description: "Duration of message publish operations",
    unit: "ms",
  });

  cachedConsumeLatencyHistogram = meter.createHistogram("amqp.worker.process.duration", {
    description: "Duration of message processing operations",
    unit: "ms",
  });

  cachedLateRpcReplyCounter = meter.createCounter("amqp.client.rpc.late_reply", {
    description:
      "RPC replies received after the caller stopped waiting (timeout, cancellation, or unknown correlationId)",
    unit: "{message}",
  });

  return {
    publishCounter: cachedPublishCounter,
    consumeCounter: cachedConsumeCounter,
    publishLatencyHistogram: cachedPublishLatencyHistogram,
    consumeLatencyHistogram: cachedConsumeLatencyHistogram,
    lateRpcReplyCounter: cachedLateRpcReplyCounter,
  };
}

/**
 * Default telemetry provider that uses OpenTelemetry API if available.
 */
export const defaultTelemetryProvider: TelemetryProvider = {
  getTracer,
  getPublishCounter: () => getMeterInstruments().publishCounter,
  getConsumeCounter: () => getMeterInstruments().consumeCounter,
  getPublishLatencyHistogram: () => getMeterInstruments().publishLatencyHistogram,
  getConsumeLatencyHistogram: () => getMeterInstruments().consumeLatencyHistogram,
  getLateRpcReplyCounter: () => getMeterInstruments().lateRpcReplyCounter,
};

/**
 * Create a span for a publish operation.
 * Returns undefined if OpenTelemetry is not available.
 */
export function startPublishSpan(
  provider: TelemetryProvider,
  exchangeName: string,
  routingKey: string | undefined,
  attributes?: Attributes,
): Span | undefined {
  const tracer = provider.getTracer();
  if (!tracer) {
    return undefined;
  }

  const spanName = `${exchangeName} publish`;

  return tracer.startSpan(spanName, {
    kind: SpanKind.PRODUCER,
    attributes: {
      [MessagingSemanticConventions.MESSAGING_SYSTEM]:
        MessagingSemanticConventions.MESSAGING_SYSTEM_RABBITMQ,
      [MessagingSemanticConventions.MESSAGING_DESTINATION]: exchangeName,
      [MessagingSemanticConventions.MESSAGING_DESTINATION_KIND]:
        MessagingSemanticConventions.MESSAGING_DESTINATION_KIND_EXCHANGE,
      [MessagingSemanticConventions.MESSAGING_OPERATION]:
        MessagingSemanticConventions.MESSAGING_OPERATION_PUBLISH,
      ...(routingKey
        ? { [MessagingSemanticConventions.MESSAGING_RABBITMQ_ROUTING_KEY]: routingKey }
        : {}),
      ...attributes,
    },
  });
}

/**
 * Create a span for a consume/process operation.
 * Returns undefined if OpenTelemetry is not available.
 */
export function startConsumeSpan(
  provider: TelemetryProvider,
  queueName: string,
  consumerName: string,
  attributes?: Attributes,
): Span | undefined {
  const tracer = provider.getTracer();
  if (!tracer) {
    return undefined;
  }

  const spanName = `${queueName} process`;

  return tracer.startSpan(spanName, {
    kind: SpanKind.CONSUMER,
    attributes: {
      [MessagingSemanticConventions.MESSAGING_SYSTEM]:
        MessagingSemanticConventions.MESSAGING_SYSTEM_RABBITMQ,
      [MessagingSemanticConventions.MESSAGING_DESTINATION]: queueName,
      [MessagingSemanticConventions.MESSAGING_DESTINATION_KIND]:
        MessagingSemanticConventions.MESSAGING_DESTINATION_KIND_QUEUE,
      [MessagingSemanticConventions.MESSAGING_OPERATION]:
        MessagingSemanticConventions.MESSAGING_OPERATION_PROCESS,
      [MessagingSemanticConventions.AMQP_CONSUMER_NAME]: consumerName,
      ...attributes,
    },
  });
}

/**
 * End a span with success status.
 */
export function endSpanSuccess(span: Span | undefined): void {
  if (!span) {
    return;
  }

  const api = tryLoadOpenTelemetryApi();
  if (api) {
    span.setStatus({ code: api.SpanStatusCode.OK });
  }
  span.end();
}

/**
 * End a span with error status.
 */
export function endSpanError(span: Span | undefined, error: Error): void {
  if (!span) {
    return;
  }

  const api = tryLoadOpenTelemetryApi();
  if (api) {
    span.setStatus({ code: api.SpanStatusCode.ERROR, message: error.message });
    span.recordException(error);
    span.setAttribute(MessagingSemanticConventions.ERROR_TYPE, error.name);
  }
  span.end();
}

/**
 * Record a publish metric.
 */
export function recordPublishMetric(
  provider: TelemetryProvider,
  exchangeName: string,
  routingKey: string | undefined,
  success: boolean,
  durationMs: number,
): void {
  const publishCounter = provider.getPublishCounter();
  const publishLatencyHistogram = provider.getPublishLatencyHistogram();

  const attributes: Attributes = {
    [MessagingSemanticConventions.MESSAGING_SYSTEM]:
      MessagingSemanticConventions.MESSAGING_SYSTEM_RABBITMQ,
    [MessagingSemanticConventions.MESSAGING_DESTINATION]: exchangeName,
    ...(routingKey
      ? { [MessagingSemanticConventions.MESSAGING_RABBITMQ_ROUTING_KEY]: routingKey }
      : {}),
    success: success,
  };

  publishCounter?.add(1, attributes);
  publishLatencyHistogram?.record(durationMs, attributes);
}

/**
 * Record a consume metric.
 */
export function recordConsumeMetric(
  provider: TelemetryProvider,
  queueName: string,
  consumerName: string,
  success: boolean,
  durationMs: number,
): void {
  const consumeCounter = provider.getConsumeCounter();
  const consumeLatencyHistogram = provider.getConsumeLatencyHistogram();

  const attributes: Attributes = {
    [MessagingSemanticConventions.MESSAGING_SYSTEM]:
      MessagingSemanticConventions.MESSAGING_SYSTEM_RABBITMQ,
    [MessagingSemanticConventions.MESSAGING_DESTINATION]: queueName,
    [MessagingSemanticConventions.AMQP_CONSUMER_NAME]: consumerName,
    success: success,
  };

  consumeCounter?.add(1, attributes);
  consumeLatencyHistogram?.record(durationMs, attributes);
}

/**
 * Record an RPC reply that arrived after the caller stopped waiting.
 *
 * @param reason - Why the reply was orphaned. `"unknown-correlation-id"` is
 *   the typical "caller already timed out" case; `"missing-correlation-id"`
 *   means the broker delivered a reply with no correlationId at all (a
 *   protocol violation by the responder).
 */
export function recordLateRpcReply(
  provider: TelemetryProvider,
  reason: "unknown-correlation-id" | "missing-correlation-id",
): void {
  const counter = provider.getLateRpcReplyCounter();

  const attributes: Attributes = {
    [MessagingSemanticConventions.MESSAGING_SYSTEM]:
      MessagingSemanticConventions.MESSAGING_SYSTEM_RABBITMQ,
    reason,
  };

  counter?.add(1, attributes);
}

/**
 * Reset the cached OpenTelemetry API module and instruments.
 * For testing purposes only.
 * @internal
 */
export function _internal_resetTelemetryCache(): void {
  otelApi = undefined;
  cachedTracer = undefined;
  cachedPublishCounter = undefined;
  cachedConsumeCounter = undefined;
  cachedPublishLatencyHistogram = undefined;
  cachedConsumeLatencyHistogram = undefined;
  cachedLateRpcReplyCounter = undefined;
}

/** @deprecated Renamed to {@link _internal_resetTelemetryCache} per the org `_internal_` convention. */
export function _resetTelemetryCacheForTesting(): void {
  _internal_resetTelemetryCache();
}
