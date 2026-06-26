import { TaggedError } from "unthrown";

export { MessageValidationError } from "@amqp-contract/core";

/**
 * Returned from `TypedAmqpClient.call()` when the configured `timeoutMs` elapses
 * before the RPC server publishes a reply with the matching `correlationId`.
 *
 * The pending call is removed from the in-memory correlation map; if a reply
 * arrives after the timeout it is dropped (and a debug log is emitted by the
 * client if a logger is configured). Carries a `_tag` of `"RpcTimeoutError"`.
 */
export class RpcTimeoutError extends TaggedError("RpcTimeoutError")<{
  message: string;
  rpcName: string;
  timeoutMs: number;
}> {
  constructor(rpcName: string, timeoutMs: number) {
    super({
      message: `RPC call to "${rpcName}" timed out after ${timeoutMs}ms with no reply received`,
      rpcName,
      timeoutMs,
    });
  }
}

/**
 * Returned from any in-flight RPC call when the client is closed before the
 * reply is received. The correlation map is cleared on close and every pending
 * caller's promise resolves with `err(RpcCancelledError)`. Carries a `_tag` of
 * `"RpcCancelledError"`.
 */
export class RpcCancelledError extends TaggedError("RpcCancelledError")<{
  message: string;
  rpcName: string;
}> {
  constructor(rpcName: string) {
    super({
      message: `RPC call to "${rpcName}" was cancelled because the client was closed`,
      rpcName,
    });
  }
}
