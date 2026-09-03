/**
 * @amqp-contract/testing - Testing utilities for AMQP contracts
 *
 * This package provides Vitest integration for testing AMQP applications
 * with automatic RabbitMQ container management and test fixtures.
 *
 * @packageDocumentation
 */

export { default as globalSetup } from "./global-setup.js";
export { it } from "./extension.js";
export { InMemoryAmqpBroker } from "./in-memory.js";
