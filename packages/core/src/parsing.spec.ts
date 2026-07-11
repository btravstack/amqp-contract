import { describe, expect, it } from "vitest";
import { TechnicalError } from "./errors.js";
import { safeJsonParse } from "./parsing.js";

describe("safeJsonParse", () => {
  it("parses a valid JSON buffer to its value", () => {
    const buffer = Buffer.from(JSON.stringify({ a: 1, b: "two" }));
    const result = safeJsonParse(buffer, () => new TechnicalError("unused"));

    expect(result).toBeOk();
    expect(
      result.getOrElse((e) => {
        throw e;
      }),
    ).toEqual({ a: 1, b: "two" });
  });

  it("parses primitive JSON values", () => {
    const rethrow = (e: unknown): never => {
      throw e;
    };
    expect(safeJsonParse(Buffer.from("42"), () => new Error()).getOrElse(rethrow)).toBe(42);
    expect(safeJsonParse(Buffer.from('"hello"'), () => new Error()).getOrElse(rethrow)).toBe(
      "hello",
    );
    expect(safeJsonParse(Buffer.from("null"), () => new Error()).getOrElse(rethrow)).toBeNull();
  });

  it("invokes the error mapper with the underlying parse error and returns Err", () => {
    const buffer = Buffer.from("{not json}");
    const seen: unknown[] = [];
    const result = safeJsonParse(buffer, (raw) => {
      seen.push(raw);
      return new TechnicalError("Failed to parse JSON", raw);
    });

    expect(result).toBeErr();
    expect(seen).toHaveLength(1);
    expect(seen[0]).toBeInstanceOf(SyntaxError);
    if (!result.isErr()) throw new Error("expected Err");
    const err = result.error;
    expect(err).toBeInstanceOf(TechnicalError);
    expect(err.message).toBe("Failed to parse JSON");
    expect(err.cause).toBeInstanceOf(SyntaxError);
  });

  it("preserves the caller-supplied error type (not just TechnicalError)", () => {
    class CustomError extends Error {
      constructor(public readonly raw: unknown) {
        super("custom");
      }
    }

    const result = safeJsonParse(Buffer.from("oops"), (raw) => new CustomError(raw));
    expect(result).toBeErr();
    if (!result.isErr()) throw new Error("expected Err");
    expect(result.error).toBeInstanceOf(CustomError);
  });
});
