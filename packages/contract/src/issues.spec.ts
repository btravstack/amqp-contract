import { describe, expect, it } from "vitest";
import { formatIssue, summarizeIssues } from "./issues.js";

describe("formatIssue", () => {
  it("renders path segments joined with dots", () => {
    expect(formatIssue({ message: "expected string", path: ["order", "id"] })).toBe(
      "order.id: expected string",
    );
  });

  it("handles PathSegment objects with a key property", () => {
    expect(formatIssue({ message: "required", path: [{ key: "items" }, { key: 0 }] })).toBe(
      "items.0: required",
    );
  });

  it("renders root-level issues as the bare message", () => {
    expect(formatIssue({ message: "invalid payload" })).toBe("invalid payload");
    expect(formatIssue({ message: "invalid payload", path: [] })).toBe("invalid payload");
  });
});

describe("summarizeIssues", () => {
  it("joins issues with semicolons up to the limit", () => {
    const issues = [
      { message: "a is required", path: ["a"] },
      { message: "b is required", path: ["b"] },
    ];
    expect(summarizeIssues(issues)).toBe("a: a is required; b: b is required");
  });

  it("truncates with a (+N more) suffix beyond the limit", () => {
    const issues = [1, 2, 3, 4, 5].map((n) => ({ message: `issue ${n}` }));
    expect(summarizeIssues(issues, 2)).toBe("issue 1; issue 2 (+3 more)");
  });

  it("is defensive on empty input", () => {
    expect(summarizeIssues([])).toBe("no issues");
  });
});
