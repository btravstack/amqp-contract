import { ErrAsync, OkAsync } from "unthrown";
import { describe, expect, it, vi } from "vitest";

import { ConnectionError, TechnicalError } from "./errors.js";
import { startOrClose } from "./lifecycle.js";

const instance = (close = vi.fn(() => OkAsync(undefined))) => ({ close });

describe("startOrClose (the shared create() lifecycle)", () => {
  it("resolves the started instance and leaves it open", async () => {
    const built = instance();

    const result = await startOrClose(
      () => built,
      () => OkAsync(undefined),
      { name: "client" },
    );

    expect(result).toBeOkWith(built);
    expect(built.close).not.toHaveBeenCalled();
  });

  it("INVARIANT: a failed start-up closes the instance before reporting the failure", async () => {
    const built = instance();
    const failure = new ConnectionError("unreachable");

    const result = await startOrClose(
      () => built,
      () => ErrAsync(failure),
      { name: "worker" },
    );

    expect(result).toBeErrWith(failure);
    expect(built.close).toHaveBeenCalledTimes(1);
  });

  it("warns (and still reports the start-up failure) when that close itself fails", async () => {
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const built = instance(
      vi.fn(() =>
        OkAsync(undefined).flatMap(() => {
          // oxlint-disable-next-line unthrown/no-throw -- simulating a close defect
          throw new TechnicalError("close failed");
        }),
      ),
    );

    const result = await startOrClose(
      () => built,
      () => ErrAsync(new ConnectionError("unreachable")),
      { name: "worker", logger },
    );

    expect(result).toBeErrTagged("@amqp-contract/ConnectionError");
    expect(logger.warn).toHaveBeenCalledWith(
      "Failed to close worker after a start-up failure",
      expect.objectContaining({ error: expect.any(TechnicalError) }),
    );
  });

  it("turns a throwing build into a Defect instead of a raw throw", async () => {
    const result = await startOrClose(
      (): ReturnType<typeof instance> => {
        // oxlint-disable-next-line unthrown/no-throw -- simulating a constructor throw
        throw new TechnicalError("bad option");
      },
      () => OkAsync(undefined),
      { name: "client" },
    );

    expect(result).toBeDefectWith(expect.objectContaining({ message: "bad option" }));
  });
});
