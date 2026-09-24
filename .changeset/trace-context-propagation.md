---
"@amqp-contract/core": minor
"@amqp-contract/client": minor
---

Trace context now crosses the broker. On publish, core injects the active
OpenTelemetry context into the message headers (through whatever propagator
the application registered — W3C `traceparent` with the standard SDK setup),
and the client publishes and sends RPC requests with its producer span active,
so that context is the producer span's. On consume, core runs each delivery
inside the context extracted from those headers, so a consumer span continues
the publisher's trace.

Without `@opentelemetry/api` or a registered SDK every step is a no-op and the
headers are left untouched; a throwing propagator or context manager degrades
to "no propagation" and never reaches the data path.
