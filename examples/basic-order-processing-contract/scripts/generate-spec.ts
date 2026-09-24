import { AsyncAPIGenerator } from "@amqp-contract/asyncapi";

import { orderContract } from "../src/index.js";

// Zod 4 schemas convert natively via Standard JSON Schema; no converter needed.
const generator = new AsyncAPIGenerator();

export const spec = await generator.generate(orderContract, {
  info: {
    title: "Order Processing API",
    version: "1.0.0",
    description: "Type-safe AMQP messaging for order processing",
  },
  servers: {
    production: {
      host: "rabbitmq.example.com:5672",
      protocol: "amqp",
      description: "Production server",
    },
    development: {
      host: "localhost:5672",
      protocol: "amqp",
      description: "Local development",
    },
  },
});
