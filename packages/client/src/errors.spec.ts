import { describe, expect, it } from "vitest";

import { RpcCancelledError, RpcTimeoutError } from "./errors.js";

describe("client error classes", () => {
  it("print their own name and message at the top of their stack", () => {
    const timeout = new RpcTimeoutError("calculate", 50);
    const cancelled = new RpcCancelledError("calculate");

    expect([timeout.stack?.split("\n")[0], cancelled.stack?.split("\n")[0]]).toEqual([
      `RpcTimeoutError: ${timeout.message}`,
      `RpcCancelledError: ${cancelled.message}`,
    ]);
  });

  it("expose their _tag as a static", () => {
    expect([RpcTimeoutError.tag, RpcCancelledError.tag]).toEqual([
      new RpcTimeoutError("x", 1)._tag,
      new RpcCancelledError("x")._tag,
    ]);
  });
});
