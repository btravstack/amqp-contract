/**
 * Global setup module for starting RabbitMQ test containers
 *
 * This module provides a Vitest globalSetup function that automatically starts
 * a RabbitMQ container with the management plugin before tests run, and stops
 * it after all tests complete.
 *
 * The RabbitMQ image can be configured via the `RABBITMQ_IMAGE` environment variable.
 * By default, it uses the public Docker Hub image (`rabbitmq:4.2.1-management-alpine`).
 *
 * @module global-setup
 * @packageDocumentation
 */

import { GenericContainer, Wait } from "testcontainers";
import type { TestProject } from "vitest/node";

/**
 * Default RabbitMQ Docker image to use for testing.
 * Can be overridden via RABBITMQ_IMAGE environment variable.
 */
const DEFAULT_RABBITMQ_IMAGE = "rabbitmq:4.2.1-management-alpine";

declare module "vitest" {
  // oxlint-disable-next-line typescript/consistent-type-definitions -- declaration merging into vitest's ProvidedContext only works on an interface
  export interface ProvidedContext {
    __TESTCONTAINERS_RABBITMQ_IP__: string;
    __TESTCONTAINERS_RABBITMQ_PORT_5672__: number;
    __TESTCONTAINERS_RABBITMQ_PORT_15672__: number;
    __TESTCONTAINERS_RABBITMQ_USERNAME__: string;
    __TESTCONTAINERS_RABBITMQ_PASSWORD__: string;
  }
}

/**
 * Setup function for Vitest globalSetup
 *
 * Starts a RabbitMQ container before all tests and provides connection details
 * to tests via Vitest's provide API. The container is automatically stopped
 * and cleaned up after all tests complete.
 *
 * This function should be configured in your `vitest.config.ts`:
 *
 * @example
 * ```typescript
 * import { defineConfig } from "vitest/config";
 *
 * export default defineConfig({
 *   test: {
 *     globalSetup: ["@amqp-contract/testing/global-setup"],
 *   },
 * });
 * ```
 *
 * @param provide - Function to provide context values to tests
 * @returns Cleanup function that stops the RabbitMQ container
 */
export default async function setup({ provide }: TestProject) {
  const rabbitmqImage = process.env["RABBITMQ_IMAGE"] ?? DEFAULT_RABBITMQ_IMAGE;

  console.log("🐳 Starting RabbitMQ test environment...");
  console.log(`📦 Using RabbitMQ image: ${rabbitmqImage}`);

  // Start RabbitMQ container with management plugin
  const rabbitmqContainer = await new GenericContainer(rabbitmqImage)
    .withExposedPorts(5672, 15672)
    .withEnvironment({
      RABBITMQ_DEFAULT_USER: "guest",
      RABBITMQ_DEFAULT_PASS: "guest",
    })
    .withWaitStrategy(
      Wait.forAll([Wait.forLogMessage(/Server startup complete/), Wait.forListeningPorts()]),
    )
    .start();

  console.log("✅ RabbitMQ container started");

  const __TESTCONTAINERS_RABBITMQ_IP__ = rabbitmqContainer.getHost();
  const __TESTCONTAINERS_RABBITMQ_PORT_5672__ = rabbitmqContainer.getMappedPort(5672);
  const __TESTCONTAINERS_RABBITMQ_PORT_15672__ = rabbitmqContainer.getMappedPort(15672);
  const __TESTCONTAINERS_RABBITMQ_USERNAME__ = "guest";
  const __TESTCONTAINERS_RABBITMQ_PASSWORD__ = "guest";

  // Provide context values with type assertions to work around TypeScript limitations
  provide("__TESTCONTAINERS_RABBITMQ_IP__", __TESTCONTAINERS_RABBITMQ_IP__);
  provide("__TESTCONTAINERS_RABBITMQ_PORT_5672__", __TESTCONTAINERS_RABBITMQ_PORT_5672__);
  provide("__TESTCONTAINERS_RABBITMQ_PORT_15672__", __TESTCONTAINERS_RABBITMQ_PORT_15672__);
  provide("__TESTCONTAINERS_RABBITMQ_USERNAME__", __TESTCONTAINERS_RABBITMQ_USERNAME__);
  provide("__TESTCONTAINERS_RABBITMQ_PASSWORD__", __TESTCONTAINERS_RABBITMQ_PASSWORD__);

  console.log(
    `🚀 RabbitMQ test environment is ready at ${__TESTCONTAINERS_RABBITMQ_IP__}:${__TESTCONTAINERS_RABBITMQ_PORT_5672__}`,
  );
  console.log(
    `📊 RabbitMQ management console available at http://${__TESTCONTAINERS_RABBITMQ_IP__}:${__TESTCONTAINERS_RABBITMQ_PORT_15672__}`,
  );

  // Return cleanup function that stops the container
  return async () => {
    console.log("🧹 Stopping RabbitMQ test environment...");
    await rabbitmqContainer.stop();
    console.log("✅ RabbitMQ container stopped");
  };
}
