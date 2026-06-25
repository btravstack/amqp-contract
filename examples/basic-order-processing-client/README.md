# Basic Order Processing - Client

Publisher application demonstrating type-safe AMQP message publishing.

📖 **[Full documentation →](https://btravstack.github.io/amqp-contract/examples/basic-order-processing)**

## Quick Start

```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:4-management

# Run the client
pnpm --filter @amqp-contract-examples/basic-order-processing-client dev
```

## Environment Variables

| Variable    | Default                 | Description                   |
| ----------- | ----------------------- | ----------------------------- |
| `AMQP_URL`  | `amqp://localhost:5672` | RabbitMQ connection URL       |
| `LOG_LEVEL` | `info`                  | Log level (info, debug, etc.) |

For detailed documentation, visit the **[website](https://btravstack.github.io/amqp-contract/examples/basic-order-processing)**.
