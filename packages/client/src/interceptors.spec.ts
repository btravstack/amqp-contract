import { TechnicalError } from "@amqp-contract/core";
import { Err, Ok, type AsyncResult } from "unthrown";
import { describe, expect, it } from "vitest";
import {
  chainInterceptors,
  type PublishError,
  type PublishInterceptor,
  type PublishInterceptorArgs,
} from "./interceptors.js";

const baseArgs: PublishInterceptorArgs = {
  publisherName: "orderCreated",
  message: { orderId: "1" },
  options: {},
};

describe("chainInterceptors", () => {
  it("runs interceptors left-to-right with the first as outermost", async () => {
    // GIVEN
    const order: string[] = [];
    const make =
      (label: string): PublishInterceptor =>
      (_args, next) => {
        order.push(`${label}:before`);
        return next().tap(() => order.push(`${label}:after`));
      };

    // WHEN
    const result = await chainInterceptors([make("outer"), make("inner")], baseArgs, () => {
      order.push("terminal");
      return Ok(undefined).toAsync();
    });

    // THEN
    expect(result.isOk()).toBe(true);
    expect(order).toEqual([
      "outer:before",
      "inner:before",
      "terminal",
      "inner:after",
      "outer:after",
    ]);
  });

  it("applies patches so inner interceptors and the terminal see them", async () => {
    // GIVEN
    const stampHeader: PublishInterceptor = (args, next) =>
      next({ options: { ...args.options, headers: { "x-trace": "abc" } } });
    const raisePriority: PublishInterceptor = (args, next) =>
      next({ options: { ...args.options, priority: 7 } });

    // WHEN
    let finalArgs: PublishInterceptorArgs | undefined;
    await chainInterceptors([stampHeader, raisePriority], baseArgs, (args) => {
      finalArgs = args;
      return Ok(undefined).toAsync();
    });

    // THEN — the inner interceptor received the stamped header and kept it
    expect(finalArgs?.options).toEqual({ headers: { "x-trace": "abc" }, priority: 7 });
    expect(finalArgs?.message).toEqual({ orderId: "1" });
    expect(finalArgs?.publisherName).toBe("orderCreated");
  });

  it("short-circuits when an interceptor returns without calling next", async () => {
    // GIVEN
    const block: PublishInterceptor = () => Err(new TechnicalError("blocked")).toAsync();
    let terminalRan = false;

    // WHEN
    const result = await chainInterceptors([block], baseArgs, () => {
      terminalRan = true;
      return Ok(undefined).toAsync();
    });

    // THEN
    expect(terminalRan).toBe(false);
    expect(result.isErr()).toBe(true);
  });

  it("supports retrying by calling next more than once", async () => {
    // GIVEN
    let attempts = 0;
    const retryOnce: PublishInterceptor = (_args, next) =>
      next().orElse(
        (error): AsyncResult<void, PublishError> => (attempts < 2 ? next() : Err(error).toAsync()),
      );

    // WHEN
    const result = await chainInterceptors([retryOnce], baseArgs, () => {
      attempts += 1;
      return attempts < 2
        ? Err(new TechnicalError("transient")).toAsync()
        : Ok(undefined).toAsync();
    });

    // THEN
    expect(attempts).toBe(2);
    expect(result.isOk()).toBe(true);
  });

  it("runs the terminal directly when no interceptors are configured", async () => {
    // WHEN
    const result = await chainInterceptors([], baseArgs, (args) =>
      args.publisherName === "orderCreated"
        ? Ok(undefined).toAsync()
        : Err(new TechnicalError("wrong args")).toAsync(),
    );

    // THEN
    expect(result.isOk()).toBe(true);
  });
});
