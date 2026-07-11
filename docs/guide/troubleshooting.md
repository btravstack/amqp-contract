---
title: Troubleshooting - Common Issues and Solutions
description: Solutions to common issues with amqp-contract, RabbitMQ, TypeScript, and AMQP messaging. Troubleshoot connection, validation, type errors, and performance problems.
---

# Troubleshooting

Common issues and their solutions when using **amqp-contract**.

## Connection Issues

### "Connection refused" or "ECONNREFUSED"

**Symptoms:**

```
Error: connect ECONNREFUSED 127.0.0.1:5672
```

**Cause:** RabbitMQ is not running or not accessible at the specified URL.

**Solutions:**

1. **Check if RabbitMQ is running:**

   ```bash
   # Using Docker
   docker ps | grep rabbitmq

   # Check if port 5672 is listening
   netstat -an | grep 5672
   # or
   lsof -i :5672
   ```

2. **Start RabbitMQ:**

   ```bash
   # Using Docker
   docker start rabbitmq

   # Or create a new container
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
   ```

3. **Verify connection URL:**

   ```typescript
   // ✅ Correct format
   urls: ["amqp://localhost"];
   urls: ["amqp://localhost:5672"];
   urls: ["amqp://user:password@localhost:5672"];

   // ❌ Common mistakes
   urls: ["localhost"]; // Missing protocol
   urls: ["amqp://localhost:15672"]; // Wrong port (15672 is for management UI)
   ```

4. **Check firewall/network:**
   ```bash
   # Test connection
   telnet localhost 5672
   # or
   nc -zv localhost 5672
   ```

### "Authentication failed" or "ACCESS_REFUSED"

**Symptoms:**

```
Error: ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN
```

**Cause:** Invalid credentials or user doesn't exist.

**Solutions:**

1. **Use correct credentials:**

   ```typescript
   // Default RabbitMQ credentials
   urls: ["amqp://guest:guest@localhost"];

   // Custom credentials
   urls: ["amqp://myuser:mypassword@localhost"];
   ```

2. **Create user in RabbitMQ:**

   ```bash
   # Using Docker
   docker exec rabbitmq rabbitmqctl add_user myuser mypassword
   docker exec rabbitmq rabbitmqctl set_permissions -p / myuser ".*" ".*" ".*"
   docker exec rabbitmq rabbitmqctl set_user_tags myuser administrator
   ```

3. **Check RabbitMQ Management UI:**
   - Open [http://localhost:15672](http://localhost:15672)
   - Login with your credentials
   - Go to "Admin" → "Users" to verify user exists

### "Channel closed" or "Connection closed"

**Symptoms:**

```
Error: Channel closed
Error: Connection closed: 320 (CONNECTION-FORCED)
```

**Cause:** RabbitMQ closed the connection/channel, often due to errors or resource limits.

**Solutions:**

1. **Check RabbitMQ logs:**

   ```bash
   # Using Docker
   docker logs rabbitmq
   ```

2. **Verify resource limits:**
   - Check memory and disk space in RabbitMQ Management UI
   - Look for alarms under "Admin" tab

3. **Handle errors properly:**

   ```typescript
   const result = await client.publish("sendEmail", message);

   result.match({
     ok: () => console.log("Published"),
     err: (error) => {
       console.error("Failed:", error);
       // Don't ignore errors!
     },
     defect: (cause) => {
       throw cause;
     },
   });
   ```

4. **Graceful shutdown:**
   ```typescript
   process.on("SIGINT", async () => {
     await worker.close().getOrThrow();
     await client.close().getOrThrow();
     process.exit(0);
   });
   ```

## TypeScript Errors

### "Type 'X' is not assignable to type 'Y'"

**Symptoms:**

```typescript
Type '{ to: string; subject: string; }' is not assignable to type 'EmailMessage'.
Property 'body' is missing.
```

**Cause:** Message doesn't match the schema defined in your contract.

**Solution:** Provide all required fields:

```typescript
// ❌ Missing required field
await client.publish("sendEmail", {
  to: "user@example.com",
  subject: "Hello",
  // Missing 'body'
});

// ✅ All required fields
await client.publish("sendEmail", {
  to: "user@example.com",
  subject: "Hello",
  body: "Welcome!",
});
```

### "Property 'X' does not exist on type 'Y'"

**Symptoms:**

```typescript
Property 'orderId' does not exist on type 'never'.
```

**Cause:** TypeScript cannot infer types properly from the contract.

**Solutions:**

1. **Ensure contract is properly typed:**

   ```typescript
   // ✅ Export contract as const
   export const contract = defineContract({
     // ...
   });

   // ❌ Don't use 'any' or lose type information
   export const contract: any = defineContract({
     // ...
   });
   ```

2. **Use correct type inference:**

   ```typescript
   import type { ClientInferPublisherInput } from "@amqp-contract/contract";
   import { contract } from "./contract.js";

   type EmailInput = ClientInferPublisherInput<typeof contract, "sendEmail">;
   ```

3. **Check consumer handler types:**

   ```typescript
   import { Ok } from "unthrown";

   // ✅ Payload is automatically typed
   handlers: {
     processEmail: ({ payload }) => {
       console.log(payload.to);  // Type-safe!
       return Ok(undefined).toAsync();
     },
   }
   ```

### "Cannot find module" or "Module not found"

**Symptoms:**

```
Error [ERR_MODULE_NOT_FOUND]: Cannot find module './contract'
```

**Cause:** Missing `.js` extension in imports (required for ESM).

**Solution:** Always use `.js` extensions:

```typescript
// ❌ Missing extension
import { contract } from "./contract";

// ✅ With extension
import { contract } from "./contract.js";
```

::: tip
Even though your file is `contract.ts`, you must import it as `contract.js` when using ESM!
:::

### "moduleResolution" or "module" errors

**Symptoms:**

```
Module resolution kind 'Node' is not supported for ES6 module output.
```

**Cause:** Incorrect TypeScript configuration.

**Solution:** Update `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "esModuleInterop": true,
    "strict": true
  }
}
```

## Validation Errors

### "Validation failed: expected string, received number"

**Symptoms:**

```
Error: Validation failed: [
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "number",
    "path": ["orderId"]
  }
]
```

**Cause:** Message payload doesn't match the Zod/Valibot/ArkType schema.

**Solution:** Ensure data types match the schema:

```typescript
// Schema
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
);

// ❌ Wrong types
await client.publish("orderCreated", {
  orderId: 123, // Should be string!
  amount: "99.99", // Should be number!
});

// ✅ Correct types
await client.publish("orderCreated", {
  orderId: "ORD-123", // String
  amount: 99.99, // Number
});
```

### "Required field missing"

**Symptoms:**

```
Error: Validation failed: [
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "undefined",
    "path": ["customerId"]
  }
]
```

**Cause:** Publishing incomplete message with missing required fields.

**Solution:** Provide all required fields:

```typescript
// Schema
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    customerId: z.string(),
    amount: z.number(),
  }),
);

// ❌ Missing customerId
await client.publish("orderCreated", {
  orderId: "ORD-123",
  amount: 99.99,
});

// ✅ All fields present
await client.publish("orderCreated", {
  orderId: "ORD-123",
  customerId: "CUST-456",
  amount: 99.99,
});
```

### "Additional property not allowed"

**Symptoms:**

```
Error: Validation failed: Unrecognized key(s) in object: 'extraField'
```

**Cause:** Sending fields not defined in schema.

**Solution:** Remove extra fields or update schema:

```typescript
// Option 1: Remove extra field
await client.publish("orderCreated", {
  orderId: "ORD-123",
  amount: 99.99,
  // extraField: "value",  // Remove this
});

// Option 2: Update schema to allow it
const orderMessage = defineMessage(
  z.object({
    orderId: z.string(),
    amount: z.number(),
    metadata: z.record(z.unknown()).optional(), // Allow extra data
  }),
);
```

## Performance Issues

### High memory usage

**Symptoms:**

- Application memory usage grows over time
- RabbitMQ shows high memory consumption
- Out of memory errors

**Causes & Solutions:**

1. **Too many concurrent connections:**

   ```typescript
   // ❌ Creating new connection for each operation
   async function publishMessage() {
     const client = await TypedAmqpClient.create({
       contract,
       urls: ["amqp://localhost"],
     }).getOrThrow();
     await client.publish("sendEmail", message).getOrThrow();
     await client.close().getOrThrow();
   }

   // ✅ Reuse connection
   const client = await TypedAmqpClient.create({
     contract,
     urls: ["amqp://localhost"],
   }).getOrThrow();

   async function publishMessage() {
     await client.publish("sendEmail", message).getOrThrow();
   }
   ```

2. **Not closing connections:**

   ```typescript
   // ✅ Always close connections
   process.on("SIGINT", async () => {
     await worker.close().getOrThrow();
     await client.close().getOrThrow();
     process.exit(0);
   });
   ```

3. **Large message payloads:**
   ```typescript
   // Consider using message compression
   // See: /guide/message-compression
   ```

### Slow message processing

**Symptoms:**

- Messages accumulate in queues
- Consumer can't keep up with publisher
- High CPU usage

**Causes & Solutions:**

1. **Synchronous/blocking handlers:**

   ```typescript
   // ❌ Blocking operation
   handlers: {
     processOrder: ({ payload }) => {
       const qualify = (error: unknown) => new RetryableError("Processing failed", error);
       return fromPromise(fetch("http://slow-api.com/process"), qualify)  // Slow!
         .flatMap((result) => fromPromise(processResult(result), qualify))
         .map(() => undefined);
     },
   }

   // ✅ Use prefetch to control concurrency
   const worker = await TypedAmqpWorker.create({
     contract,
     handlers: {
       processOrder: [
         ({ payload }) => { /* ... */ },
         { prefetch: 10 }, // Process up to 10 messages concurrently
       ],
     },
   }).getOrThrow();
   ```

2. **Heavy computation in handlers:**

   ```typescript
   // ✅ Offload heavy work
   handlers: {
     processImage: ({ payload }) => {
       // Queue heavy work to worker pool
       return fromPromise(jobQueue.add("process-image", payload), (error) => new RetryableError("Failed to queue job", error))
         .map(() => undefined);
     },
   }
   ```

3. **Inefficient queries:**

   ```typescript
   // ❌ N+1 query problem
   handlers: {
     processOrder: ({ payload }) => {
       // Inefficient - multiple sequential queries
       return fromPromise(
         (async () => {
           for (const item of payload.items) {
             await db.query("SELECT * FROM products WHERE id = ?", [item.id]);
           }
         })(),
         (error) => new RetryableError("Query failed", error),
       ).map(() => undefined);
     },
   }

   // ✅ Batch queries
   handlers: {
     processOrder: ({ payload }) => {
       const ids = payload.items.map(item => item.id);
       return fromPromise(
         db.query("SELECT * FROM products WHERE id IN (?)", [ids]),
         (error) => new RetryableError("Query failed", error),
       ).map(() => undefined);
     },
   }
   ```

### Connection timeouts

**Symptoms:**

```
Error: Connection timeout
```

**Cause:** Network latency or RabbitMQ overload.

**Solutions:**

1. **Increase timeout:**

   ```typescript
   const client = await TypedAmqpClient.create({
     contract,
     urls: ["amqp://localhost"],
     connectionOptions: {
       connectionOptions: {
         timeout: 10000, // 10 seconds
       },
     },
   }).getOrThrow();
   ```

2. **Use connection pooling:**
   - See [Connection Sharing](/guide/connection-sharing)

3. **Check RabbitMQ performance:**
   - Monitor queue depths
   - Check message rates
   - Review resource usage

## RabbitMQ Configuration

### Queue not declared

**Symptoms:**

```
Error: Queue 'order-processing' not found
```

**Cause:** Contract definition mismatch or queue not created.

**Solutions:**

1. **Ensure queue is in contract:**

   ```typescript
   const contract = defineContract({
     queues: {
       orderProcessing: defineQueue("order-processing"),
     },
     // ...
   });
   ```

2. **Check RabbitMQ Management UI:**
   - Go to "Queues" tab
   - Verify queue exists
   - Check queue properties match contract

3. **Let amqp-contract create resources:**
   ```typescript
   // Resources are automatically created when client/worker starts
   const client = await TypedAmqpClient.create({
     contract,
     urls: ["amqp://localhost"],
   }).getOrThrow();
   ```

### Messages not routing

**Symptoms:**

- Messages published successfully but not received
- Queue remains empty

**Causes & Solutions:**

1. **Routing key mismatch:**

   ```typescript
   // Publisher
   const publisher = definePublisher(exchange, message, {
     routingKey: "order.created", // ⚠️ Must match binding
   });

   // Binding
   const binding = defineQueueBinding(queue, exchange, {
     routingKey: "order.created", // ⚠️ Must match publisher
   });
   ```

2. **Wrong exchange type:**

   ```typescript
   // ❌ Direct exchange with topic pattern
   const exchange = defineExchange("orders", { type: "direct" });
   // ...
   routingKey: "order.*"; // Won't work with direct exchange!

   // ✅ Use topic exchange for patterns
   const exchange = defineExchange("orders", { type: "topic" });
   // ...
   routingKey: "order.*"; // Works with topic exchange!
   ```

3. **Verify in RabbitMQ Management UI:**
   - Go to exchange → Bindings
   - Verify queue is bound with correct routing key
   - Use "Publish message" to test routing

### Exchange or queue already exists with different properties

**Symptoms:**

```
Error: PRECONDITION_FAILED - inequivalent arg 'durable' for exchange 'orders'
```

**Cause:** Trying to declare exchange/queue with properties that differ from existing one.

**Solutions:**

1. **Delete and recreate:**

   ```bash
   # Using RabbitMQ Management UI:
   # - Go to Exchanges/Queues tab
   # - Delete the resource
   # - Restart your application
   ```

2. **Match existing properties:**

   ```typescript
   // Find existing properties in RabbitMQ Management UI
   // Update contract to match:
   const exchange = defineExchange("orders", {
     durable: true, // Match existing
   });
   ```

3. **Use different names:**
   ```typescript
   // If you can't delete, use a new name
   const exchange = defineExchange("orders-v2");
   ```

## Still Having Issues?

If your problem isn't listed here:

1. **Check GitHub Issues:**
   - [Search existing issues](https://github.com/btravstack/amqp-contract/issues)
   - [Open a new issue](https://github.com/btravstack/amqp-contract/issues/new)

2. **Review Documentation:**
   - [Core Concepts](/guide/core-concepts)
   - [Client Usage](/guide/client-usage)
   - [Worker Usage](/guide/worker-usage)

3. **Check Examples:**
   - [Basic Order Processing](/examples/basic-order-processing)

4. **Debug Mode:**

   ```typescript
   // Enable debug logging (if available in your logger)
   process.env.DEBUG = "amqp-contract:*";
   ```

5. **RabbitMQ Logs:**
   ```bash
   # Check RabbitMQ logs for errors
   docker logs rabbitmq
   ```

::: tip Need More Help?
When asking for help, please provide:

- amqp-contract version (check `package.json`)
- Node.js version (`node --version`)
- TypeScript version (`npx tsc --version`)
- RabbitMQ version
- Complete error message and stack trace
- Minimal reproduction code
  :::
