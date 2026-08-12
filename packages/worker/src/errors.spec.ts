import { describe, expect, it } from "vitest";

import {
  isHandlerError,
  NonRetryableError,
  RetryableError,
  qualifyNonRetryable,
  qualifyRetryable,
} from "./errors.js";
import type { HandlerError } from "./errors.js";

describe("HandlerError tagged union", () => {
  it("RetryableError is a handler error and a real Error", () => {
    const error = new RetryableError("test");
    expect(isHandlerError(error)).toBe(true);
    expect(error).toBeInstanceOf(Error);
    expect(error._tag).toBe("@amqp-contract/RetryableError");
    expect(error.name).toBe("RetryableError");
  });

  it("NonRetryableError is a handler error and a real Error", () => {
    const error = new NonRetryableError("test");
    expect(isHandlerError(error)).toBe(true);
    expect(error).toBeInstanceOf(Error);
    expect(error._tag).toBe("@amqp-contract/NonRetryableError");
    expect(error.name).toBe("NonRetryableError");
  });

  it("HandlerError narrows by name discriminator", () => {
    const errors: HandlerError[] = [new RetryableError("retry"), new NonRetryableError("dlq")];
    for (const error of errors) {
      if (error.name === "RetryableError") {
        expect(error).toBeInstanceOf(RetryableError);
      } else {
        expect(error).toBeInstanceOf(NonRetryableError);
      }
    }
  });
});

describe("Factory Functions", () => {
  describe("qualifyRetryable", () => {
    it("builds a qualifier that wraps the cause in a RetryableError", () => {
      const qualify = qualifyRetryable("service unavailable");
      const cause = new Error("ECONNREFUSED");

      const error = qualify(cause);

      expect(error).toBeInstanceOf(RetryableError);
      expect(error.message).toBe("service unavailable");
      expect(error.cause).toBe(cause);
    });

    it("works as a fromPromise qualifier", async () => {
      const { fromPromise } = await import("unthrown");
      const result = await fromPromise(
        Promise.reject(new Error("boom")),
        qualifyRetryable("upstream failed"),
      );

      expect(result.isErr()).toBe(true);
      if (result.isErr()) {
        expect(result.error).toBeInstanceOf(RetryableError);
        expect(result.error.message).toBe("upstream failed");
      }
    });
  });

  describe("qualifyNonRetryable", () => {
    it("builds a qualifier that wraps the cause in a NonRetryableError", () => {
      const qualify = qualifyNonRetryable("permanently declined");
      const cause = { code: "CARD_DECLINED" };

      const error = qualify(cause);

      expect(error).toBeInstanceOf(NonRetryableError);
      expect(error.message).toBe("permanently declined");
      expect(error.cause).toBe(cause);
    });
  });
});
