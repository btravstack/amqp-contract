import { describe, expect, it } from "vitest";

import * as root from "./index.js";
import * as internal from "./internal.js";

/**
 * The cross-package runtime helpers moved to `@amqp-contract/contract/internal`.
 * The package root keeps deprecated aliases until core / worker / asyncapi
 * switch their imports; each alias must be the very same function, so the two
 * import paths can never disagree.
 */
describe("@amqp-contract/contract/internal", () => {
  const moved = [
    "deriveTtlBackoffInfrastructure",
    "extractConsumer",
    "isBridgedPublisherConfig",
    "isCommandConsumerConfig",
    "isEventConsumerResult",
    "isEventPublisherConfig",
    "ttlBackoffBaseDelay",
    "ttlBackoffWaitQueueName",
  ] as const;

  it.each(moved)("exports %s, identical to the deprecated root alias", (name) => {
    expect(typeof internal[name]).toBe("function");
    expect(root[name]).toBe(internal[name]);
  });
});
