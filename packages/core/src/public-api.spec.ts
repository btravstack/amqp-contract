import { describe, expect, it } from "vitest";

import * as publicApi from "./index.js";
import * as internal from "./internal.js";

describe("@amqp-contract/core entry points", () => {
  it("keeps implementation helpers off the public root — they live on /internal", () => {
    expect(Object.keys(publicApi).sort()).toEqual([
      "AmqpClient",
      "ConnectionError",
      "DEFAULT_CONNECT_TIMEOUT_MS",
      "DEFAULT_MAX_MESSAGE_BYTES",
      "DEFAULT_PREFETCH",
      "DEFAULT_PUBLISH_TIMEOUT_MS",
      "MessageValidationError",
      "MessagingSemanticConventions",
      "PublishError",
      "RPC_ERROR_CODE_HEADER",
      "RpcError",
      "TechnicalError",
      "defaultTelemetryProvider",
      "isConnectionError",
      "isMessageValidationError",
      "isRpcError",
      "isTechnicalError",
      "rpcError",
    ]);
  });

  it("moved helpers are still exported, from /internal", () => {
    expect(Object.keys(internal)).toEqual(
      expect.arrayContaining([
        "safeJsonParse",
        "setupAmqpTopology",
        "startConsumeSpan",
        "startPublishSpan",
        "endSpanError",
        "endSpanSuccess",
        "recordConsumeMetric",
        "recordLateRpcReply",
        "recordPublishMetric",
        "technicalDefect",
      ]),
    );
  });
});
