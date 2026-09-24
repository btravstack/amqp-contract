---
"@amqp-contract/worker": minor
---

Handler type errors are short. A handler is checked against a small named
alias over its resolved message — `ConsumerHandler<{ to: string; }, …>` or
`RpcHandler<TRequest, TResponse, …>` — so an async handler, a missing return
or a wrong payload field reports a few lines instead of the whole contract
type (`WorkerInferConsumerHandlerEntry<ContractOutput<{ publishers: … }>>`).
`ConsumerHandler`, `ConsumerHandlerEntry`, `RpcHandler` and `RpcHandlerEntry`
are exported for typing a handler by its payload directly.
