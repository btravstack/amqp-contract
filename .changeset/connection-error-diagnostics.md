---
"@amqp-contract/core": minor
"@amqp-contract/client": minor
"@amqp-contract/worker": minor
---

Error diagnostics:

- A connect timeout's `ConnectionError` now carries the last `connectFailed`
  error from amqp-connection-manager as its `cause` (and names it in the
  message) — `ECONNREFUSED` or `ACCESS_REFUSED` instead of only "timed out".
  The first failed dial is also logged at `warn`, so a wrong URL shows up
  immediately rather than when the connect timeout expires.
- Every amqp-contract error class (`TechnicalError`, `ConnectionError`,
  `MessageValidationError`, `RpcError`, `RpcTimeoutError`,
  `RpcCancelledError`) now prints its own `Name: message` at the top of its
  stack instead of a bare `Error`.
- Each of those classes exposes its `_tag` as a static, so a matcher can write
  `P.tag(ConnectionError.tag)` instead of the raw `"@amqp-contract/…"` string.
