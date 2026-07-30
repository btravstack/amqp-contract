import { describe, expect, it } from "vitest";

import { technicalDefect } from "./defect.js";
import { TechnicalError } from "./errors.js";

describe("technicalDefect", () => {
  it("mints a Defect-carrying Result whose cause is the given TechnicalError", () => {
    const error = new TechnicalError("boom");

    const result = technicalDefect(error);

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBe(error);
    }
  });

  it("lifts to an AsyncResult defect via .toAsync() (the async call-site shape)", async () => {
    const error = new TechnicalError("boom");

    const result = await technicalDefect(error).toAsync();

    expect(result).toBeDefect();
    if (result.isDefect()) {
      expect(result.cause).toBe(error);
    }
  });
});
