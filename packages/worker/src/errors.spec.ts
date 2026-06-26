import {
  NonRetryableError,
  RetryableError,
  isHandlerError,
  isNonRetryableError,
  isRetryableError,
  nonRetryable,
  retryable,
} from "./errors.js";
import type { HandlerError } from "./errors.js";
import { describe, expect, it } from "vitest";

describe("Type Guards", () => {
  describe("isRetryableError", () => {
    it("should return true for RetryableError instances", () => {
      const error = new RetryableError("test message");
      expect(isRetryableError(error)).toBe(true);
    });

    it("should return false for NonRetryableError instances", () => {
      const error = new NonRetryableError("test message");
      expect(isRetryableError(error)).toBe(false);
    });

    it("should return false for plain Error instances", () => {
      const error = new Error("test message");
      expect(isRetryableError(error)).toBe(false);
    });

    it("should return false for non-error values", () => {
      expect(isRetryableError(null)).toBe(false);
      expect(isRetryableError(undefined)).toBe(false);
      expect(isRetryableError("string")).toBe(false);
      expect(isRetryableError(123)).toBe(false);
      expect(isRetryableError({})).toBe(false);
    });
  });

  describe("isNonRetryableError", () => {
    it("should return true for NonRetryableError instances", () => {
      const error = new NonRetryableError("test message");
      expect(isNonRetryableError(error)).toBe(true);
    });

    it("should return false for RetryableError instances", () => {
      const error = new RetryableError("test message");
      expect(isNonRetryableError(error)).toBe(false);
    });

    it("should return false for plain Error instances", () => {
      const error = new Error("test message");
      expect(isNonRetryableError(error)).toBe(false);
    });

    it("should return false for non-error values", () => {
      expect(isNonRetryableError(null)).toBe(false);
      expect(isNonRetryableError(undefined)).toBe(false);
      expect(isNonRetryableError("string")).toBe(false);
      expect(isNonRetryableError(123)).toBe(false);
      expect(isNonRetryableError({})).toBe(false);
    });
  });

  describe("isHandlerError", () => {
    it("should return true for RetryableError instances", () => {
      const error = new RetryableError("test message");
      expect(isHandlerError(error)).toBe(true);
    });

    it("should return true for NonRetryableError instances", () => {
      const error = new NonRetryableError("test message");
      expect(isHandlerError(error)).toBe(true);
    });

    it("should return false for plain Error instances", () => {
      const error = new Error("test message");
      expect(isHandlerError(error)).toBe(false);
    });

    it("should return false for non-error values", () => {
      expect(isHandlerError(null)).toBe(false);
      expect(isHandlerError(undefined)).toBe(false);
      expect(isHandlerError("string")).toBe(false);
      expect(isHandlerError(123)).toBe(false);
      expect(isHandlerError({})).toBe(false);
    });
  });
});

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
  describe("retryable", () => {
    it("should create a RetryableError with message", () => {
      const error = retryable("test message");
      expect(error).toBeInstanceOf(RetryableError);
      expect(error.message).toBe("test message");
      expect(error.name).toBe("RetryableError");
      expect(error.cause).toBeUndefined();
    });

    it("should create a RetryableError with message and cause", () => {
      const cause = new Error("underlying error");
      const error = retryable("test message", cause);
      expect(error).toBeInstanceOf(RetryableError);
      expect(error.message).toBe("test message");
      expect(error.cause).toBe(cause);
    });

    it("should preserve non-Error cause values", () => {
      const cause = { code: "TIMEOUT", details: "Connection timed out" };
      const error = retryable("test message", cause);
      expect(error.cause).toBe(cause);
    });
  });

  describe("nonRetryable", () => {
    it("should create a NonRetryableError with message", () => {
      const error = nonRetryable("test message");
      expect(error).toBeInstanceOf(NonRetryableError);
      expect(error.message).toBe("test message");
      expect(error.name).toBe("NonRetryableError");
      expect(error.cause).toBeUndefined();
    });

    it("should create a NonRetryableError with message and cause", () => {
      const cause = new Error("underlying error");
      const error = nonRetryable("test message", cause);
      expect(error).toBeInstanceOf(NonRetryableError);
      expect(error.message).toBe("test message");
      expect(error.cause).toBe(cause);
    });

    it("should preserve non-Error cause values", () => {
      const cause = { code: "VALIDATION_FAILED", field: "email" };
      const error = nonRetryable("test message", cause);
      expect(error.cause).toBe(cause);
    });
  });
});
