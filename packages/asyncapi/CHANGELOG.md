# @amqp-contract/asyncapi

## 0.22.0

### Patch Changes

- Updated dependencies [203ad3a]
  - @amqp-contract/contract@0.22.0

## 0.21.0

### Minor Changes

- Retry system and configuration normalization

  ### **Changes Overview**

  #### 🔄 **Retry System Overhaul**
  1. **None retry**
  - Introduced new `none` retry option to represent the "no retry" mode
  - Changed queue builder default from `ttl-backoff` retry to `none` retry
  - Removed implicit TTL-backoff infrastructure creation when no retry config specified
  - Worker error handler now detects `none` retry mode and rejects failed messages without retry
  - Aligns with "explicit over implicit" configuration philosophy
  2. **Immediate-requeue retry**
  - Migrated from `quorum-native` (quorum-only) to `immediate-requeue` (universal)
  - Now works with both quorum and classic queues
  - Improved handling: quorum uses native `x-delivery-count`, classic uses custom headers
  - Simplified API: `maxRetries` parameter replaces `deliveryLimit`
  3. **TTL-backoff via headers exchanges**
  - Replaced DLX routing with headers exchange infrastructure
  - Preserves original routing keys through retry flow
  - Eliminates dangerous infinite retry loop behavior
  - Configurable infrastructure names (`waitQueueName`, `waitExchangeName`, `retryExchangeName`)

  #### ⚙️ **Configuration Normalization**
  4. **Exchange configuration normalization**
  - Exchange `type` defaults to `topic` (most used)
  - `durable` defaults to `true` (production-friendly)
  - Added support for headers exchange types
  - Reduced verbosity while supporting all exchange types
  5. **Queue configuration normalization**
  - Queue `type` defaults to `quorum` (modern choice)
  - `durable` defaults to `true` (production-friendly)
  - `autoDelete` mode restricted to classic queues only (like `exclusive` and `maxPriority`)
  - Better type safety and runtime validation of queue options
  - Removed over-specific queue definition helpers: `defineQuorumQueue()`, `defineTtlBackoffQueue()`
  - Removed `deliveryLimit` in favor of `maxRetries`
  - Retry config consolidated at queue level

  #### 🎯 **New Features**
  6. **Default publish/consumer options**
  - Added `defaultPublishOptions` to `TypedAmqpClient`
    - Set once, applies to all publishes (can be overridden per-call)
    - `persistent` defaults to `true` (production-friendly)
  - Added `defaultConsumerOptions` to `TypedAmqpWorker`
    - Set once, applies to all consumers (can be overridden per-consumer handler)
  - Removed custom prefetch implementation in favor of built-in configuration in `amqp-connection-manager`
  - Eliminates configuration repetition across codebase

  #### 🐛 **Type Safety Improvements**
  7. **Handler type safety fix**
  - Consumer handler payloads and headers now properly typed from schema output types
  - Removed unnecessary type extraction utilities

  ***

  ### **Breaking Changes**

  ⚠️ **Users upgrading will need to:**
  1. Configure TTL-backoff explicitly, since queues now default to no retry
  2. Migrate TTL-backoff queue names if using custom infrastructure naming
  3. Change `mode: "quorum-native"` to `mode: "immediate-requeue"`
  4. Replace `deliveryLimit` with `maxRetries` in retry config
  5. Replace `type` parameter from `defineExchange()` calls with `type` options property (defaults to `topic`)
  6. Replace `defineQuorumQueue()` and `defineTtlBackoffQueue()` helpers with generic `defineQueue()`

  ***

  ### **Before/After Examples**

  **Exchange Definition**

  ```typescript
  // Before
  defineExchange("orders", "topic", { durable: true });

  // After
  defineExchange("orders"); // topic + durable by default
  ```

  **Queue Definition**

  ```typescript
  // Before
  defineQueue("orders", { durable: true });

  // After
  defineQueue("orders"); // quorum + durable by default
  ```

  **Retry Configuration**

  ```typescript
  // Before: TTL-backoff created automatically
  defineQueue("orders"); // Had retry: ttl-backoff by default

  // After: No retry by default
  defineQueue("orders"); // Now has no retry by default

  // To enable TTL-backoff retry, explicitly opt-in:
  defineQueue("orders", {
    retry: { mode: "ttl-backoff", maxRetries: 3 },
  });

  // Before: "quorum-native" with deliveryLimit (for quorum queues only)
  defineQueue("orders", {
    type: "quorum",
    deliveryLimit: 3,
    retry: { mode: "quorum-native" },
  });

  // After: "immediate-requeue" with maxRetries (for any queue)
  defineQueue("orders", {
    retry: { mode: "immediate-requeue", maxRetries: 3 },
  });
  ```

  **Default Publish/Consumer Options**

  ```typescript
  // Default publish options in client
  const client = await TypedAmqpClient.create({
    contract,
    urls: ["amqp://localhost"],
    defaultPublishOptions: { priority: 5 },
  });

  // Default consumer options in worker
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers,
    urls: ["amqp://localhost"],
    defaultConsumerOptions: { prefetch: 10 },
  });
  ```

  ***

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.21.0

## 0.20.0

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.20.0

## 0.19.0

### Patch Changes

- @amqp-contract/contract@0.19.0

## 0.18.0

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.18.0

## 0.17.0

### Patch Changes

- Updated dependencies [22242a4]
  - @amqp-contract/contract@0.17.0

## 0.16.0

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.16.0

## 0.15.0

### Minor Changes

- Simplify contract definition API and preserve literal types in ContractOutput

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.15.0

## 0.14.0

### Minor Changes

- feat: add Event/Command Pattern API for intuitive messaging patterns

  ### New Features

  **Event Pattern** - For broadcasting events to multiple consumers:
  - `defineEventPublisher(exchange, message, options)` - Define an event publisher
  - `defineEventConsumer(eventPublisher, queue, options)` - Subscribe to an event (auto-generates binding)

  **Command Pattern** - For task queues with single consumer:
  - `defineCommandConsumer(queue, exchange, message, options)` - Define a command consumer (auto-generates binding)
  - `defineCommandPublisher(commandConsumer, options)` - Create a publisher for a command

  **Helper Functions**:
  - `extractConsumer(entry)` - Extract ConsumerDefinition from any ConsumerEntry type

  ### Breaking Changes
  - Removed `definePublisherFirst` and `defineConsumerFirst` (replaced by Event/Command patterns)

  ### Example

  ```typescript
  // Event pattern: one publisher, many consumers
  const orderCreated = defineEventPublisher(ordersExchange, orderMessage, {
    routingKey: "order.created",
  });

  const processOrder = defineEventConsumer(orderCreated, orderQueue);
  const notifyOrder = defineEventConsumer(orderCreated, notificationQueue);

  // Command pattern: many publishers, one consumer
  const shipOrder = defineCommandConsumer(shippingQueue, ordersExchange, shipMessage, {
    routingKey: "order.ship",
  });

  const sendShipOrder = defineCommandPublisher(shipOrder);

  // Use in contract - bindings are auto-generated
  const contract = defineContract({
    exchanges: { orders: ordersExchange },
    queues: { orderQueue, notificationQueue, shippingQueue },
    publishers: { orderCreated, sendShipOrder },
    consumers: { processOrder, notifyOrder, shipOrder },
  });
  ```

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.14.0

## 0.13.0

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.13.0

## 0.12.0

### Minor Changes

- ## New Features

  ### Testing Package Exports
  - Added main export entry point for `@amqp-contract/testing` - users can now `import { it, globalSetup } from '@amqp-contract/testing'`

  ### OpenTelemetry Documentation
  - Added comprehensive OpenTelemetry observability guide covering traces, metrics, and configuration

  ### CI Improvements
  - Added security audit job to CI pipeline
  - Added bundle size monitoring with GitHub Step Summary reporting

  ## Improvements

  ### Contract Package Refactoring
  - Split `builder.ts` (1,911 lines) into modular files for better maintainability:
    - `builder/exchange.ts` - defineExchange
    - `builder/queue.ts` - defineQueue, extractQueue
    - `builder/message.ts` - defineMessage
    - `builder/binding.ts` - defineQueueBinding, defineExchangeBinding
    - `builder/publisher.ts` - definePublisher
    - `builder/consumer.ts` - defineConsumer
    - `builder/contract.ts` - defineContract
    - `builder/publisher-first.ts` - definePublisherFirst
    - `builder/consumer-first.ts` - defineConsumerFirst
    - `builder/ttl-backoff.ts` - defineTtlBackoffRetryInfrastructure
    - `builder/routing-types.ts` - RoutingKey, BindingPattern types
  - All existing imports continue to work (backward compatible)

  ### Worker Package
  - Improved compression validation error messages with helpful context:
    - Shows received encoding
    - Lists supported encodings (gzip, deflate)
    - Suggests checking publisher configuration

  ## Security
  - Fixed high severity vulnerability in `preact` dependency (CVE in vitepress transitive dependency)

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.12.0

## 0.11.0

### Minor Changes

- feat: move retry configuration from worker to contract level

  **Breaking Change:** Retry configuration has moved from handler-level to queue-level.

  ### Before (0.10.x)

  ```typescript
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: [handler, { retry: { maxRetries: 3, initialDelayMs: 1000 } }],
    },
    urls: ["amqp://localhost"],
  });
  ```

  ### After (0.11.0)

  ```typescript
  // Configure retry at queue level in the contract
  const orderQueue = defineQueue("order-processing", {
    deadLetter: { exchange: dlx },
    retry: {
      mode: "ttl-backoff",
      maxRetries: 3,
      initialDelayMs: 1000,
    },
  });

  // Worker no longer specifies retry options
  const worker = await TypedAmqpWorker.create({
    contract,
    handlers: {
      processOrder: handler,
    },
    urls: ["amqp://localhost"],
  });
  ```

  ### Key Changes
  - **Retry types moved to contract package**: `RetryOptions`, `TtlBackoffRetryOptions`, `QuorumNativeRetryOptions` are now exported from `@amqp-contract/contract`
  - **Queue-level retry configuration**: Use `retry` option in `defineQueue()` instead of handler tuples
  - **Automatic TTL-backoff infrastructure**: `defineContract()` automatically generates wait queues and bindings for TTL-backoff mode
  - **`extractQueue()` helper**: Use this to access queue properties from `QueueWithTtlBackoffInfrastructure` wrapper
  - **Removed `setupWaitQueues`**: Wait queues are now created by `setupAmqpTopology` like any other queue

  ### Migration Guide
  1. Move `retry` configuration from handler options to queue definition
  2. Add `mode: "ttl-backoff"` or `mode: "quorum-native"` to your retry config
  3. Remove handler tuple syntax `[handler, { retry: ... }]` - just use `handler` directly
  4. Use `extractQueue()` when accessing queue properties if using TTL-backoff mode

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.11.0

## 0.10.0

### Patch Changes

- Automatically bind main queue to DLX for retry flow

  The worker now automatically creates a binding from the Dead Letter Exchange (DLX) to the main queue using the queue name as the routing key. This completes the retry flow: DLX → wait queue → DLX → main queue.

  Users no longer need to manually create a `waitBinding` in their contracts when implementing retry logic. The binding is now handled automatically by the worker setup process.

- Updated dependencies
  - @amqp-contract/contract@0.10.0

## 0.9.0

### Minor Changes

- Add OpenTelemetry instrumentation for spans and metrics

  This release adds comprehensive OpenTelemetry instrumentation support for AMQP operations:
  - **Automatic tracing**: Distributed tracing spans for publish and consume operations with semantic conventions following OpenTelemetry standards
  - **Metrics collection**: Counters and histograms for message throughput and latency monitoring
  - **Optional dependency**: OpenTelemetry is an optional peer dependency that is gracefully loaded when available
  - **Zero configuration**: Instrumentation automatically integrates with your existing OpenTelemetry setup
  - **Semantic conventions**: Follows OpenTelemetry messaging semantic conventions for AMQP/RabbitMQ

  Key features:
  - Producer and consumer spans with proper span kinds
  - Message metadata tracking (message ID, routing key, delivery tag, payload size)
  - Error tracking with error types and attributes
  - Performance metrics for publish and consume operations
  - Compatible with any OpenTelemetry-compliant APM solution

  See the documentation for configuration details and usage examples.

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.9.0

## 0.8.0

### Patch Changes

- @amqp-contract/contract@0.8.0

## 0.7.0

### Minor Changes

- Release version 0.7.0 with runtime message compression support for AMQP payloads.

  This release adds the ability to compress messages at runtime using gzip or deflate algorithms. Key features include:
  - Added `CompressionAlgorithm` type supporting 'gzip' and 'deflate'
  - Added optional `compression` parameter to the `publish()` method for runtime compression
  - Automatic decompression in workers based on content-encoding header
  - Backward compatible - no compression by default
  - New sample demonstrating compression usage

  See PR #225 for complete details.

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.7.0

## 0.6.0

### Minor Changes

- Restructure repository to follow vitest pattern with docs as workspace package

  This release includes a major refactoring of the repository structure:
  - Move documentation to workspace package for better integration
  - Simplify docs build workflow
  - Remove orchestration scripts in favor of turbo
  - Improve overall project organization following vitest pattern

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.6.0

## 0.5.0

### Minor Changes

- Add routing key parameters with type validation for all exchange types

  This release introduces comprehensive routing key parameter support with compile-time type validation:

  **New Features:**
  - Added routing key parameter support for topic and direct exchanges
  - Implemented type-level validation for routing keys and binding patterns
    - `RoutingKey<T>` type validates routing key format and character set
    - `BindingPattern<T>` type validates AMQP pattern syntax (\*, #)
    - `MatchingRoutingKey<Pattern, Key>` validates key matches pattern
  - Enhanced `definePublisherFirst` and `defineConsumerFirst` functions:
    - `createPublisher()` accepts routing key parameter for topic exchanges
    - `createConsumer()` accepts optional routing key pattern
  - Routing key validation ensures AMQP compliance at compile-time:
    - Validates allowed characters (a-z, A-Z, 0-9, -, \_)
    - Validates proper segment formatting with dot separators
    - Implements AMQP topic exchange pattern matching logic

  **Type Safety Improvements:**
  - When consumer uses pattern with wildcards (e.g., "order.\*"), publishers can use any matching string
  - When consumer uses concrete key, publishers must use exact same key
  - When publisher uses concrete key, consumers can use any pattern
  - Pattern matching logic:
    - `*` matches exactly one word
    - `#` matches zero or more words

  **Usage Example:**

  ```typescript
  // Topic exchange with routing key parameters
  const consumer = defineConsumerFirst(
    topicExchange,
    "order.*", // Pattern with wildcard
    orderSchema,
  );

  // Publishers can specify concrete keys matching the pattern
  const publisher = consumer.createPublisher("order.created");

  // Or define publisher first with concrete key
  const publisher2 = definePublisherFirst(
    topicExchange,
    "order.updated", // Concrete routing key
    orderSchema,
  );

  // Consumers can subscribe with any pattern
  const consumer2 = publisher2.createConsumer("order.*");
  ```

  This feature provides end-to-end type safety for routing keys and binding patterns, catching configuration errors at compile time rather than runtime.

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.5.0

## 0.4.0

### Minor Changes

- Release version 0.4.0

  This release includes stability improvements and prepares the packages for wider adoption.

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.4.0

## 0.3.5

### Patch Changes

- @amqp-contract/contract@0.3.5

## 0.3.4

### Patch Changes

- Add generic type parameters to NestJS module forRoot/forRootAsync methods

  This change replaces ConfigurableModuleBuilder with manual forRoot/forRootAsync implementations that support generic type parameters. This enables full type safety for worker handlers and client publishers based on the specific contract type.

  **BREAKING CHANGE**: MODULE_OPTIONS_TOKEN is now a Symbol instead of string|symbol union

- Updated dependencies
  - @amqp-contract/contract@0.3.4

## 0.3.3

### Patch Changes

- @amqp-contract/contract@0.3.3

## 0.3.2

### Patch Changes

- Add optional Logger interface for message publishing and consumption

  This release introduces an optional Logger interface that allows users to integrate their preferred logging framework with amqp-contract:

  **New Features:**
  - Added `Logger` interface in `@amqp-contract/core` with debug, info, warn, and error methods
  - Added `LoggerContext` type for structured logging context
  - Client and Worker now accept an optional `logger` option to enable message logging
  - NestJS modules support logger injection

  **Usage:**

  ```typescript
  // Simple console logger implementation
  const logger: Logger = {
    debug: (message, context) => console.debug(message, context),
    info: (message, context) => console.info(message, context),
    warn: (message, context) => console.warn(message, context),
    error: (message, context) => console.error(message, context),
  };

  // Use with client
  const client = await TypedAmqpClient.create({
    contract,
    urls,
    logger,
  });

  // Use with worker
  const worker = await TypedAmqpWorker.create({
    contract,
    urls,
    logger,
  });
  ```

- Updated dependencies
  - @amqp-contract/contract@0.3.2

## 0.3.1

### Patch Changes

- @amqp-contract/contract@0.3.1

## 0.3.0

### Minor Changes

- Add waitForConnectionReady feature

  This release introduces connection readiness handling with the following changes:

  **Breaking Changes:**
  - `TypedAmqpClient.create()` now returns `Future<Result<TypedAmqpClient, TechnicalError>>` instead of directly returning the client instance
  - `TypedAmqpWorker.create()` now returns `Future<Result<TypedAmqpWorker, TechnicalError>>` instead of directly returning the worker instance

  **New Features:**
  - Added `waitForConnectionReady()` method to ensure AMQP connection is established before operations
  - Improved error handling with explicit Result types for connection failures

  **Migration Guide:**
  Update your client/worker creation code to handle the new async Result type:

  Before:

  ```typescript
  const client = TypedAmqpClient.create({ contract, urls });
  ```

  After:

  ```typescript
  const result = await TypedAmqpClient.create({ contract, urls });
  if (result.isError()) {
    // Handle connection error
    console.error("Failed to create client:", result.getError());
    return;
  }
  const client = result.get();
  ```

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.3.0

## 0.2.1

### Patch Changes

- Documentation improvements including TypeDoc-generated API documentation and standardized package READMEs with badges and documentation links.
- Updated dependencies
  - @amqp-contract/contract@0.2.1

## 0.2.0

### Minor Changes

- Extract AMQP setup logic into core package

  This release introduces a new `@amqp-contract/core` package that centralizes AMQP infrastructure setup logic. The core package provides a `setupInfra` function that handles the creation of exchanges, queues, and bindings, eliminating code duplication across client and worker packages.

  **New Features:**
  - New `@amqp-contract/core` package with centralized AMQP setup logic
  - `setupInfra` function for creating exchanges, queues, and bindings from contract definitions

  **Changes:**
  - Updated `@amqp-contract/client` to use core setup function
  - Updated `@amqp-contract/worker` to use core setup function
  - All packages are now versioned together as a fixed group

  **Migration:**
  No breaking changes. Existing code will continue to work as before. The core package is used internally by client and worker packages.

### Patch Changes

- Updated dependencies
  - @amqp-contract/contract@0.2.0

## 0.1.4

### Patch Changes

- @amqp-contract/contract@0.1.4

## 0.1.3

### Patch Changes

- Add exchange-to-exchange binding support
- Updated dependencies
  - @amqp-contract/contract@0.1.3

## 0.1.2

### Patch Changes

- Fix: configurable module type
- Updated dependencies
  - @amqp-contract/contract@0.1.2

## 0.1.1

### Patch Changes

- 498358d: Patch version bump for all packages
- Updated dependencies [498358d]
  - @amqp-contract/contract@0.1.1

## 0.1.0

### Patch Changes

- @amqp-contract/contract@0.1.0

## 0.0.6

### Patch Changes

- Release version 0.0.6 for all packages
- Updated dependencies
  - @amqp-contract/contract@0.0.6

## 0.0.5

### Patch Changes

- Refactor to use factory pattern with static create() methods. Remove unnecessary type casts and improve internal implementation.
- Updated dependencies
  - @amqp-contract/contract@0.0.5

## 0.0.4

### Patch Changes

- Release version 0.0.4
- Updated dependencies
  - @amqp-contract/contract@0.0.4

## 0.0.3

### Patch Changes

- Documentation updates and API improvements for 0.0.4 release
- Updated dependencies
  - @amqp-contract/contract@0.0.3

## 0.0.2

### Patch Changes

- Release version 0.0.2
- Updated dependencies
  - @amqp-contract/contract@0.0.2
