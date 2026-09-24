import { P, match } from "unthrown";
import { describe, expect, it } from "vitest";

import { ConnectionError, MessageValidationError, RpcError, TechnicalError } from "./errors.js";

describe("amqp-contract error classes", () => {
  it.for([
    { name: "TechnicalError", error: new TechnicalError("boom") },
    { name: "ConnectionError", error: new ConnectionError("boom") },
    { name: "RpcError", error: new RpcError("CODE", {}, "boom") },
  ])("$name prints its own name and message at the top of its stack", ({ name, error }) => {
    // unthrown's TaggedError captures the stack before name/message exist,
    // which used to leave a bare "Error" header on every one of them.
    expect(error.stack?.split("\n")[0]).toBe(`${name}: boom`);
  });

  it("MessageValidationError's stack header carries its rendered message", () => {
    const error = new MessageValidationError("orderCreated", []);

    expect(error.stack?.split("\n")[0]).toBe(`MessageValidationError: ${error.message}`);
  });

  it("exposes each _tag as a static, so a matcher needs no raw string", () => {
    const describeError = (error: TechnicalError | ConnectionError) =>
      match(error)
        .with(P.tag(TechnicalError.tag), () => "technical")
        .with(P.tag(ConnectionError.tag), () => "connection")
        .exhaustive();

    expect([
      describeError(new ConnectionError("x")),
      describeError(new TechnicalError("x")),
      MessageValidationError.tag,
      RpcError.tag,
    ]).toEqual([
      "connection",
      "technical",
      "@amqp-contract/MessageValidationError",
      "@amqp-contract/RpcError",
    ]);
  });
});
