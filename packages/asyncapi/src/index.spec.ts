import {
  type ContractDefinition,
  defineConsumer,
  defineContract,
  defineExchange,
  defineMessage,
  definePublisher,
  defineQueue,
} from "@amqp-contract/contract";
import { Parser } from "@asyncapi/parser";
import { experimental_ArkTypeToJsonSchemaConverter } from "@orpc/arktype";
import { experimental_ValibotToJsonSchemaConverter } from "@orpc/valibot";
import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";
import { type } from "arktype";
import * as v from "valibot";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import { AsyncAPIGenerator } from "./index.js";

describe("AsyncAPIGenerator", () => {
  describe("with Zod schemas", () => {
    it("should generate valid AsyncAPI 3.0 document with Zod schemas", async () => {
      // GIVEN
      const orderExchange = defineExchange("orders");
      const orderQueue = defineQueue("order-processing");

      const orderSchema = z.object({
        orderId: z.string(),
        customerId: z.string(),
        amount: z.number().positive(),
        createdAt: z.string().datetime(),
      });

      const orderMessage = defineMessage(orderSchema, {
        summary: "Order created event",
        description: "Event published when a new order is created",
      });

      const contract = defineContract({
        publishers: {
          orderCreated: definePublisher(orderExchange, orderMessage, {
            routingKey: "order.created",
          }),
        },
        consumers: {
          processOrder: defineConsumer(orderQueue, orderMessage),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new ZodToJsonSchemaConverter()],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: {
          title: "Order Processing API",
          version: "1.0.0",
          description: "Order processing messaging API",
        },
        servers: {
          development: {
            host: "localhost:5672",
            protocol: "amqp",
            description: "Development RabbitMQ server",
          },
        },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "order-processing": {
              "address": "order-processing",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "is": "queue",
                  "queue": {
                    "durable": true,
                    "name": "order-processing",
                    "type": "quorum",
                    "vhost": "/",
                  },
                },
              },
              "description": "AMQP Queue: order-processing",
              "messages": {
                "processOrderMessage": {
                  "contentType": "application/json",
                  "description": "Event published when a new order is created",
                  "payload": {
                    "properties": {
                      "amount": {
                        "exclusiveMinimum": 0,
                        "type": "number",
                      },
                      "createdAt": {
                        "format": "date-time",
                        "type": "string",
                      },
                      "customerId": {
                        "type": "string",
                      },
                      "orderId": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "orderId",
                      "customerId",
                      "amount",
                      "createdAt",
                    ],
                    "type": "object",
                  },
                  "summary": "Order created event",
                },
              },
              "title": "order-processing",
            },
            "orders": {
              "address": "orders",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "orders",
                    "type": "topic",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: orders (topic)",
              "messages": {
                "orderCreatedMessage": {
                  "contentType": "application/json",
                  "description": "Event published when a new order is created",
                  "payload": {
                    "properties": {
                      "amount": {
                        "exclusiveMinimum": 0,
                        "type": "number",
                      },
                      "createdAt": {
                        "format": "date-time",
                        "type": "string",
                      },
                      "customerId": {
                        "type": "string",
                      },
                      "orderId": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "orderId",
                      "customerId",
                      "amount",
                      "createdAt",
                    ],
                    "type": "object",
                  },
                  "summary": "Order created event",
                },
              },
              "title": "orders",
            },
          },
          "components": {
            "messages": {
              "orderCreatedMessage": {
                "contentType": "application/json",
                "description": "Event published when a new order is created",
                "payload": {
                  "properties": {
                    "amount": {
                      "exclusiveMinimum": 0,
                      "type": "number",
                    },
                    "createdAt": {
                      "format": "date-time",
                      "type": "string",
                    },
                    "customerId": {
                      "type": "string",
                    },
                    "orderId": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "orderId",
                    "customerId",
                    "amount",
                    "createdAt",
                  ],
                  "type": "object",
                },
                "summary": "Order created event",
              },
              "processOrderMessage": {
                "contentType": "application/json",
                "description": "Event published when a new order is created",
                "payload": {
                  "properties": {
                    "amount": {
                      "exclusiveMinimum": 0,
                      "type": "number",
                    },
                    "createdAt": {
                      "format": "date-time",
                      "type": "string",
                    },
                    "customerId": {
                      "type": "string",
                    },
                    "orderId": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "orderId",
                    "customerId",
                    "amount",
                    "createdAt",
                  ],
                  "type": "object",
                },
                "summary": "Order created event",
              },
            },
          },
          "info": {
            "description": "Order processing messaging API",
            "title": "Order Processing API",
            "version": "1.0.0",
          },
          "operations": {
            "orderCreated": {
              "action": "send",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "cc": [
                    "order.created",
                  ],
                  "deliveryMode": 2,
                },
              },
              "channel": {
                "$ref": "#/channels/orders",
              },
              "description": "Routing key: order.created",
              "messages": [
                {
                  "$ref": "#/channels/orders/messages/orderCreatedMessage",
                },
              ],
              "summary": "Publish to orders",
            },
            "processOrder": {
              "action": "receive",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                },
              },
              "channel": {
                "$ref": "#/channels/order-processing",
              },
              "messages": [
                {
                  "$ref": "#/channels/order-processing/messages/processOrderMessage",
                },
              ],
              "summary": "Consume from order-processing",
            },
          },
          "servers": {
            "development": {
              "description": "Development RabbitMQ server",
              "host": "localhost:5672",
              "protocol": "amqp",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });

    it("should handle message with headers", async () => {
      // GIVEN
      const exchange = defineExchange("events", { type: "fanout" });

      const payloadSchema = z.object({
        eventId: z.string(),
        data: z.string(),
      });

      const headersSchema = z.object({
        correlationId: z.string(),
        timestamp: z.number(),
      });

      const message = defineMessage(payloadSchema, {
        headers: headersSchema,
        summary: "Event with headers",
      });

      const contract = defineContract({
        publishers: {
          sendEvent: definePublisher(exchange, message),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new ZodToJsonSchemaConverter()],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: { title: "Events API", version: "1.0.0" },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "events": {
              "address": "events",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "events",
                    "type": "fanout",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: events (fanout)",
              "messages": {
                "sendEventMessage": {
                  "contentType": "application/json",
                  "headers": {
                    "properties": {
                      "correlationId": {
                        "type": "string",
                      },
                      "timestamp": {
                        "type": "number",
                      },
                    },
                    "required": [
                      "correlationId",
                      "timestamp",
                    ],
                    "type": "object",
                  },
                  "payload": {
                    "properties": {
                      "data": {
                        "type": "string",
                      },
                      "eventId": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "eventId",
                      "data",
                    ],
                    "type": "object",
                  },
                  "summary": "Event with headers",
                },
              },
              "title": "events",
            },
          },
          "components": {
            "messages": {
              "sendEventMessage": {
                "contentType": "application/json",
                "headers": {
                  "properties": {
                    "correlationId": {
                      "type": "string",
                    },
                    "timestamp": {
                      "type": "number",
                    },
                  },
                  "required": [
                    "correlationId",
                    "timestamp",
                  ],
                  "type": "object",
                },
                "payload": {
                  "properties": {
                    "data": {
                      "type": "string",
                    },
                    "eventId": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "eventId",
                    "data",
                  ],
                  "type": "object",
                },
                "summary": "Event with headers",
              },
            },
          },
          "info": {
            "title": "Events API",
            "version": "1.0.0",
          },
          "operations": {
            "sendEvent": {
              "action": "send",
              "channel": {
                "$ref": "#/channels/events",
              },
              "messages": [
                {
                  "$ref": "#/channels/events/messages/sendEventMessage",
                },
              ],
              "summary": "Publish to events",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("with Valibot schemas", () => {
    it("should generate valid AsyncAPI 3.0 document with Valibot schemas", async () => {
      // GIVEN
      const notificationExchange = defineExchange("notifications", {
        type: "direct",
      });
      const notificationQueue = defineQueue("notification-queue");

      const notificationSchema = v.object({
        notificationId: v.string(),
        userId: v.string(),
        message: v.string(),
        type: v.picklist(["email", "sms", "push"]),
      });

      const notificationMessage = defineMessage(notificationSchema, {
        summary: "Notification event",
      });

      const contract = defineContract({
        publishers: {
          sendNotification: definePublisher(notificationExchange, notificationMessage, {
            routingKey: "notification.send",
          }),
        },
        consumers: {
          processNotification: defineConsumer(notificationQueue, notificationMessage),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new experimental_ValibotToJsonSchemaConverter()],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: {
          title: "Notification API",
          version: "1.0.0",
        },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "notification-queue": {
              "address": "notification-queue",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "is": "queue",
                  "queue": {
                    "durable": true,
                    "name": "notification-queue",
                    "type": "quorum",
                    "vhost": "/",
                  },
                },
              },
              "description": "AMQP Queue: notification-queue",
              "messages": {
                "processNotificationMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "properties": {
                      "message": {
                        "type": "string",
                      },
                      "notificationId": {
                        "type": "string",
                      },
                      "type": {
                        "enum": [
                          "email",
                          "sms",
                          "push",
                        ],
                        "type": "string",
                      },
                      "userId": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "notificationId",
                      "userId",
                      "message",
                      "type",
                    ],
                    "type": "object",
                  },
                  "summary": "Notification event",
                },
              },
              "title": "notification-queue",
            },
            "notifications": {
              "address": "notifications",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "notifications",
                    "type": "direct",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: notifications (direct)",
              "messages": {
                "sendNotificationMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "properties": {
                      "message": {
                        "type": "string",
                      },
                      "notificationId": {
                        "type": "string",
                      },
                      "type": {
                        "enum": [
                          "email",
                          "sms",
                          "push",
                        ],
                        "type": "string",
                      },
                      "userId": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "notificationId",
                      "userId",
                      "message",
                      "type",
                    ],
                    "type": "object",
                  },
                  "summary": "Notification event",
                },
              },
              "title": "notifications",
            },
          },
          "components": {
            "messages": {
              "processNotificationMessage": {
                "contentType": "application/json",
                "payload": {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "properties": {
                    "message": {
                      "type": "string",
                    },
                    "notificationId": {
                      "type": "string",
                    },
                    "type": {
                      "enum": [
                        "email",
                        "sms",
                        "push",
                      ],
                      "type": "string",
                    },
                    "userId": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "notificationId",
                    "userId",
                    "message",
                    "type",
                  ],
                  "type": "object",
                },
                "summary": "Notification event",
              },
              "sendNotificationMessage": {
                "contentType": "application/json",
                "payload": {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "properties": {
                    "message": {
                      "type": "string",
                    },
                    "notificationId": {
                      "type": "string",
                    },
                    "type": {
                      "enum": [
                        "email",
                        "sms",
                        "push",
                      ],
                      "type": "string",
                    },
                    "userId": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "notificationId",
                    "userId",
                    "message",
                    "type",
                  ],
                  "type": "object",
                },
                "summary": "Notification event",
              },
            },
          },
          "info": {
            "title": "Notification API",
            "version": "1.0.0",
          },
          "operations": {
            "processNotification": {
              "action": "receive",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                },
              },
              "channel": {
                "$ref": "#/channels/notification-queue",
              },
              "messages": [
                {
                  "$ref": "#/channels/notification-queue/messages/processNotificationMessage",
                },
              ],
              "summary": "Consume from notification-queue",
            },
            "sendNotification": {
              "action": "send",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "cc": [
                    "notification.send",
                  ],
                  "deliveryMode": 2,
                },
              },
              "channel": {
                "$ref": "#/channels/notifications",
              },
              "description": "Routing key: notification.send",
              "messages": [
                {
                  "$ref": "#/channels/notifications/messages/sendNotificationMessage",
                },
              ],
              "summary": "Publish to notifications",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("with ArkType schemas", () => {
    it("should generate valid AsyncAPI 3.0 document with ArkType schemas", async () => {
      // GIVEN
      const paymentExchange = defineExchange("payments");
      const paymentQueue = defineQueue("payment-processing");

      const paymentSchema = type({
        paymentId: "string",
        orderId: "string",
        amount: "number",
        currency: "'USD' | 'EUR' | 'GBP'",
        status: "'pending' | 'completed' | 'failed'",
      });

      const paymentMessage = defineMessage(paymentSchema, {
        summary: "Payment event",
        description: "Event for payment processing",
      });

      const contract = defineContract({
        publishers: {
          paymentCreated: definePublisher(paymentExchange, paymentMessage, {
            routingKey: "payment.created",
          }),
        },
        consumers: {
          processPayment: defineConsumer(paymentQueue, paymentMessage),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new experimental_ArkTypeToJsonSchemaConverter()],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: {
          title: "Payment API",
          version: "1.0.0",
        },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "payment-processing": {
              "address": "payment-processing",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "is": "queue",
                  "queue": {
                    "durable": true,
                    "name": "payment-processing",
                    "type": "quorum",
                    "vhost": "/",
                  },
                },
              },
              "description": "AMQP Queue: payment-processing",
              "messages": {
                "processPaymentMessage": {
                  "contentType": "application/json",
                  "description": "Event for payment processing",
                  "payload": {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "properties": {
                      "amount": {
                        "type": "number",
                      },
                      "currency": {
                        "enum": [
                          "EUR",
                          "GBP",
                          "USD",
                        ],
                      },
                      "orderId": {
                        "type": "string",
                      },
                      "paymentId": {
                        "type": "string",
                      },
                      "status": {
                        "enum": [
                          "completed",
                          "failed",
                          "pending",
                        ],
                      },
                    },
                    "required": [
                      "amount",
                      "currency",
                      "orderId",
                      "paymentId",
                      "status",
                    ],
                    "type": "object",
                  },
                  "summary": "Payment event",
                },
              },
              "title": "payment-processing",
            },
            "payments": {
              "address": "payments",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "payments",
                    "type": "topic",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: payments (topic)",
              "messages": {
                "paymentCreatedMessage": {
                  "contentType": "application/json",
                  "description": "Event for payment processing",
                  "payload": {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "properties": {
                      "amount": {
                        "type": "number",
                      },
                      "currency": {
                        "enum": [
                          "EUR",
                          "GBP",
                          "USD",
                        ],
                      },
                      "orderId": {
                        "type": "string",
                      },
                      "paymentId": {
                        "type": "string",
                      },
                      "status": {
                        "enum": [
                          "completed",
                          "failed",
                          "pending",
                        ],
                      },
                    },
                    "required": [
                      "amount",
                      "currency",
                      "orderId",
                      "paymentId",
                      "status",
                    ],
                    "type": "object",
                  },
                  "summary": "Payment event",
                },
              },
              "title": "payments",
            },
          },
          "components": {
            "messages": {
              "paymentCreatedMessage": {
                "contentType": "application/json",
                "description": "Event for payment processing",
                "payload": {
                  "$schema": "https://json-schema.org/draft/2020-12/schema",
                  "properties": {
                    "amount": {
                      "type": "number",
                    },
                    "currency": {
                      "enum": [
                        "EUR",
                        "GBP",
                        "USD",
                      ],
                    },
                    "orderId": {
                      "type": "string",
                    },
                    "paymentId": {
                      "type": "string",
                    },
                    "status": {
                      "enum": [
                        "completed",
                        "failed",
                        "pending",
                      ],
                    },
                  },
                  "required": [
                    "amount",
                    "currency",
                    "orderId",
                    "paymentId",
                    "status",
                  ],
                  "type": "object",
                },
                "summary": "Payment event",
              },
              "processPaymentMessage": {
                "contentType": "application/json",
                "description": "Event for payment processing",
                "payload": {
                  "$schema": "https://json-schema.org/draft/2020-12/schema",
                  "properties": {
                    "amount": {
                      "type": "number",
                    },
                    "currency": {
                      "enum": [
                        "EUR",
                        "GBP",
                        "USD",
                      ],
                    },
                    "orderId": {
                      "type": "string",
                    },
                    "paymentId": {
                      "type": "string",
                    },
                    "status": {
                      "enum": [
                        "completed",
                        "failed",
                        "pending",
                      ],
                    },
                  },
                  "required": [
                    "amount",
                    "currency",
                    "orderId",
                    "paymentId",
                    "status",
                  ],
                  "type": "object",
                },
                "summary": "Payment event",
              },
            },
          },
          "info": {
            "title": "Payment API",
            "version": "1.0.0",
          },
          "operations": {
            "paymentCreated": {
              "action": "send",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "cc": [
                    "payment.created",
                  ],
                  "deliveryMode": 2,
                },
              },
              "channel": {
                "$ref": "#/channels/payments",
              },
              "description": "Routing key: payment.created",
              "messages": [
                {
                  "$ref": "#/channels/payments/messages/paymentCreatedMessage",
                },
              ],
              "summary": "Publish to payments",
            },
            "processPayment": {
              "action": "receive",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                },
              },
              "channel": {
                "$ref": "#/channels/payment-processing",
              },
              "messages": [
                {
                  "$ref": "#/channels/payment-processing/messages/processPaymentMessage",
                },
              ],
              "summary": "Consume from payment-processing",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("with multiple schema libraries", () => {
    it("should handle contract with mixed schema types", async () => {
      // GIVEN
      const exchange = defineExchange("mixed");

      const zodSchema = z.object({
        id: z.string(),
        value: z.number(),
      });

      const valibotSchema = v.object({
        id: v.string(),
        data: v.string(),
      });

      const zodMessage = defineMessage(zodSchema);
      const valibotMessage = defineMessage(valibotSchema);

      const contract = defineContract({
        publishers: {
          publishZod: definePublisher(exchange, zodMessage, {
            routingKey: "zod.event",
          }),
          publishValibot: definePublisher(exchange, valibotMessage, {
            routingKey: "valibot.event",
          }),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [
          new ZodToJsonSchemaConverter(),
          new experimental_ValibotToJsonSchemaConverter(),
        ],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: {
          title: "Mixed Schema API",
          version: "1.0.0",
        },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "mixed": {
              "address": "mixed",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "mixed",
                    "type": "topic",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: mixed (topic)",
              "messages": {
                "publishValibotMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "properties": {
                      "data": {
                        "type": "string",
                      },
                      "id": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "id",
                      "data",
                    ],
                    "type": "object",
                  },
                },
                "publishZodMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "properties": {
                      "id": {
                        "type": "string",
                      },
                      "value": {
                        "type": "number",
                      },
                    },
                    "required": [
                      "id",
                      "value",
                    ],
                    "type": "object",
                  },
                },
              },
              "title": "mixed",
            },
          },
          "components": {
            "messages": {
              "publishValibotMessage": {
                "contentType": "application/json",
                "payload": {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "properties": {
                    "data": {
                      "type": "string",
                    },
                    "id": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "id",
                    "data",
                  ],
                  "type": "object",
                },
              },
              "publishZodMessage": {
                "contentType": "application/json",
                "payload": {
                  "properties": {
                    "id": {
                      "type": "string",
                    },
                    "value": {
                      "type": "number",
                    },
                  },
                  "required": [
                    "id",
                    "value",
                  ],
                  "type": "object",
                },
              },
            },
          },
          "info": {
            "title": "Mixed Schema API",
            "version": "1.0.0",
          },
          "operations": {
            "publishValibot": {
              "action": "send",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "cc": [
                    "valibot.event",
                  ],
                  "deliveryMode": 2,
                },
              },
              "channel": {
                "$ref": "#/channels/mixed",
              },
              "description": "Routing key: valibot.event",
              "messages": [
                {
                  "$ref": "#/channels/mixed/messages/publishValibotMessage",
                },
              ],
              "summary": "Publish to mixed",
            },
            "publishZod": {
              "action": "send",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "cc": [
                    "zod.event",
                  ],
                  "deliveryMode": 2,
                },
              },
              "channel": {
                "$ref": "#/channels/mixed",
              },
              "description": "Routing key: zod.event",
              "messages": [
                {
                  "$ref": "#/channels/mixed/messages/publishZodMessage",
                },
              ],
              "summary": "Publish to mixed",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("without schema converters", () => {
    it("should generate document with generic object schemas", async () => {
      // GIVEN
      const exchange = defineExchange("generic", { type: "fanout" });

      const schema = z.object({
        id: z.string(),
      });

      const message = defineMessage(schema);

      const contract = defineContract({
        publishers: {
          publish: definePublisher(exchange, message),
        },
      });

      const generator = new AsyncAPIGenerator();

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: {
          title: "Generic API",
          version: "1.0.0",
        },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "generic": {
              "address": "generic",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "generic",
                    "type": "fanout",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: generic (fanout)",
              "messages": {
                "publishMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "type": "object",
                  },
                },
              },
              "title": "generic",
            },
          },
          "components": {
            "messages": {
              "publishMessage": {
                "contentType": "application/json",
                "payload": {
                  "type": "object",
                },
              },
            },
          },
          "info": {
            "title": "Generic API",
            "version": "1.0.0",
          },
          "operations": {
            "publish": {
              "action": "send",
              "channel": {
                "$ref": "#/channels/generic",
              },
              "messages": [
                {
                  "$ref": "#/channels/generic/messages/publishMessage",
                },
              ],
              "summary": "Publish to generic",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("channel and operation generation", () => {
    it("should generate correct AMQP bindings for queues", async () => {
      // GIVEN - Use classic queue to test all explicit options
      const queue = defineQueue("test-queue", {
        type: "classic",
        durable: false,
        exclusive: true,
        autoDelete: true,
        maxPriority: 10,
      });

      const contract: ContractDefinition = {
        queues: { testQueue: queue },
      };

      const generator = new AsyncAPIGenerator();

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: { title: "Test", version: "1.0.0" },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "testQueue": {
              "address": "test-queue",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "is": "queue",
                  "queue": {
                    "autoDelete": true,
                    "durable": false,
                    "exclusive": true,
                    "maxPriority": 10,
                    "name": "test-queue",
                    "type": "classic",
                    "vhost": "/",
                  },
                },
              },
              "description": "AMQP Queue: test-queue",
              "title": "test-queue",
            },
          },
          "components": {
            "messages": {},
          },
          "info": {
            "title": "Test",
            "version": "1.0.0",
          },
          "operations": {},
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });

    it("should generate correct AMQP bindings for exchanges", async () => {
      // GIVEN
      const exchange = defineExchange("test-exchange");

      const contract: ContractDefinition = {
        exchanges: { testExchange: exchange },
      };

      const generator = new AsyncAPIGenerator();

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: { title: "Test", version: "1.0.0" },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "testExchange": {
              "address": "test-exchange",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "test-exchange",
                    "type": "topic",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: test-exchange (topic)",
              "title": "test-exchange",
            },
          },
          "components": {
            "messages": {},
          },
          "info": {
            "title": "Test",
            "version": "1.0.0",
          },
          "operations": {},
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });

    it("should include routing keys in operation descriptions", async () => {
      // GIVEN
      const exchange = defineExchange("orders");
      const schema = z.object({ id: z.string() });
      const message = defineMessage(schema);

      const contract = defineContract({
        publishers: {
          orderCreated: definePublisher(exchange, message, {
            routingKey: "order.created",
          }),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new ZodToJsonSchemaConverter()],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: { title: "Test", version: "1.0.0" },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "orders": {
              "address": "orders",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "orders",
                    "type": "topic",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: orders (topic)",
              "messages": {
                "orderCreatedMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "properties": {
                      "id": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "id",
                    ],
                    "type": "object",
                  },
                },
              },
              "title": "orders",
            },
          },
          "components": {
            "messages": {
              "orderCreatedMessage": {
                "contentType": "application/json",
                "payload": {
                  "properties": {
                    "id": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "id",
                  ],
                  "type": "object",
                },
              },
            },
          },
          "info": {
            "title": "Test",
            "version": "1.0.0",
          },
          "operations": {
            "orderCreated": {
              "action": "send",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "cc": [
                    "order.created",
                  ],
                  "deliveryMode": 2,
                },
              },
              "channel": {
                "$ref": "#/channels/orders",
              },
              "description": "Routing key: order.created",
              "messages": [
                {
                  "$ref": "#/channels/orders/messages/orderCreatedMessage",
                },
              ],
              "summary": "Publish to orders",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("edge cases", () => {
    it("should handle empty contract", async () => {
      // GIVEN
      const contract = defineContract({});

      const generator = new AsyncAPIGenerator();

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: { title: "Empty", version: "1.0.0" },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {},
          "components": {
            "messages": {},
          },
          "info": {
            "title": "Empty",
            "version": "1.0.0",
          },
          "operations": {},
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });

    it("should handle fanout exchanges without routing keys", async () => {
      // GIVEN
      const exchange = defineExchange("fanout-exchange", { type: "fanout" });
      const schema = z.object({ id: z.string() });
      const message = defineMessage(schema);

      const contract = defineContract({
        publishers: {
          broadcast: definePublisher(exchange, message),
        },
      });

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new ZodToJsonSchemaConverter()],
      });

      // WHEN
      const asyncapiDoc = await generator.generate(contract, {
        info: { title: "Fanout Test", version: "1.0.0" },
      });

      // THEN
      expect(asyncapiDoc).toMatchInlineSnapshot(`
        {
          "asyncapi": "3.1.0",
          "channels": {
            "fanout-exchange": {
              "address": "fanout-exchange",
              "bindings": {
                "amqp": {
                  "bindingVersion": "0.3.0",
                  "exchange": {
                    "durable": true,
                    "name": "fanout-exchange",
                    "type": "fanout",
                    "vhost": "/",
                  },
                  "is": "routingKey",
                },
              },
              "description": "AMQP Exchange: fanout-exchange (fanout)",
              "messages": {
                "broadcastMessage": {
                  "contentType": "application/json",
                  "payload": {
                    "properties": {
                      "id": {
                        "type": "string",
                      },
                    },
                    "required": [
                      "id",
                    ],
                    "type": "object",
                  },
                },
              },
              "title": "fanout-exchange",
            },
          },
          "components": {
            "messages": {
              "broadcastMessage": {
                "contentType": "application/json",
                "payload": {
                  "properties": {
                    "id": {
                      "type": "string",
                    },
                  },
                  "required": [
                    "id",
                  ],
                  "type": "object",
                },
              },
            },
          },
          "info": {
            "title": "Fanout Test",
            "version": "1.0.0",
          },
          "operations": {
            "broadcast": {
              "action": "send",
              "channel": {
                "$ref": "#/channels/fanout-exchange",
              },
              "messages": [
                {
                  "$ref": "#/channels/fanout-exchange/messages/broadcastMessage",
                },
              ],
              "summary": "Publish to fanout-exchange",
            },
          },
        }
      `);

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(asyncapiDoc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("DLX, retry, and bridge surfacing", () => {
    it("emits DLX arguments and retry summary on the queue channel", async () => {
      const dlx = defineExchange("orders-dlx", { type: "direct" });
      const exchange = defineExchange("orders");
      const queue = defineQueue("orders-q", {
        deadLetter: { exchange: dlx, routingKey: "orders.dead" },
        retry: { mode: "ttl-backoff", maxRetries: 5, initialDelayMs: 1000 },
      });
      const message = defineMessage(z.object({ id: z.string() }));
      const consumer = defineConsumer(queue, message);

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new ZodToJsonSchemaConverter()],
      });

      const doc = await generator.generate(
        defineContract({
          publishers: { sent: definePublisher(exchange, message, { routingKey: "order.created" }) },
          consumers: { processOrder: consumer },
        }) as unknown as ContractDefinition,
        { info: { title: "DLX Test", version: "1.0.0" } },
      );

      const queueChannel = doc.channels?.["orders-q"] as unknown as Record<string, unknown>;
      const queueBinding = (queueChannel["bindings"] as Record<string, unknown>)["amqp"] as Record<
        string,
        unknown
      >;
      const queueMeta = queueBinding["queue"] as Record<string, unknown>;
      const args = queueMeta["arguments"] as Record<string, unknown>;

      expect(args["x-dead-letter-exchange"]).toBe("orders-dlx");
      expect(args["x-dead-letter-routing-key"]).toBe("orders.dead");
      expect(queueChannel["description"]).toMatch(/Dead-letters to 'orders-dlx'/);
      expect(queueChannel["description"]).toMatch(/Retry: ttl-backoff, max 5 attempts/);
      expect(queueChannel["x-amqp-retry"]).toMatchObject({
        mode: "ttl-backoff",
        maxRetries: 5,
        initialDelayMs: 1000,
      });

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(doc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });

    it("represents exchange-to-exchange bindings in source and destination channels", async () => {
      const orders = defineExchange("orders");
      const billing = defineExchange("billing");
      const billingQueue = defineQueue("billing-orders");
      const message = defineMessage(z.object({ id: z.string() }));

      // Build a contract by hand to avoid pulling in defineEventConsumer's
      // bridge wiring — we just want a binding of type "exchange" between
      // the two exchanges.
      const contract = defineContract({
        publishers: { sent: definePublisher(orders, message, { routingKey: "order.created" }) },
        consumers: {
          process: defineConsumer(billingQueue, message),
        },
      }) as unknown as ContractDefinition;
      // Inject the e2e binding manually.
      contract.bindings = {
        ...contract.bindings,
        ordersToBilling: {
          type: "exchange",
          source: orders,
          destination: billing,
          routingKey: "order.#",
        } as never,
      };
      contract.exchanges = { ...contract.exchanges, billing };

      const generator = new AsyncAPIGenerator({
        schemaConverters: [new ZodToJsonSchemaConverter()],
      });

      const doc = await generator.generate(contract, {
        info: { title: "Bridge Test", version: "1.0.0" },
      });

      const ordersChannel = doc.channels?.["orders"] as unknown as Record<string, unknown>;
      const billingChannel = doc.channels?.["billing"] as unknown as Record<string, unknown>;

      expect(ordersChannel["description"]).toMatch(/forwards to 'billing'/);
      expect(ordersChannel["x-amqp-exchange-bindings"]).toMatchObject({
        forwardsTo: [{ destination: "billing", routingKey: "order.#" }],
      });

      expect(billingChannel["description"]).toMatch(/receives from 'orders'/);
      expect(billingChannel["x-amqp-exchange-bindings"]).toMatchObject({
        receivesFrom: [{ source: "orders", routingKey: "order.#" }],
      });

      const parser = new Parser();
      await expect(parser.parse(JSON.stringify(doc))).resolves.toEqual(
        expect.objectContaining({ diagnostics: [] }),
      );
    });
  });

  describe("strict converter mode", () => {
    it("throws when a payload schema cannot be converted and failOnMissingConverter=true", async () => {
      const exchange = defineExchange("orders");
      const message = defineMessage(z.object({ id: z.string() }));
      const generator = new AsyncAPIGenerator({
        schemaConverters: [],
        failOnMissingConverter: true,
      });

      await expect(
        generator.generate(
          defineContract({
            publishers: { sent: definePublisher(exchange, message, { routingKey: "x" }) },
          }),
          { info: { title: "Strict", version: "1.0.0" } },
        ),
      ).rejects.toThrow(/No schema converter matched/);
    });
  });
});
