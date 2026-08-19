import { randomUUID } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

import express from "express";

import {
  orderTopic,
  statusTopic,
} from "../../packages/messaging/config.js";
import { createOrderCreated } from "../../packages/messaging/events.js";
import {
  createKafka,
  createProducer,
} from "../../packages/messaging/kafka.js";
import { log } from "../../packages/messaging/logger.js";

const service = "order-api";
const app = express();
const kafka = createKafka(service);
const producer = createProducer(kafka);
const activityConsumer = kafka.consumer({ groupId: "order-api-activity-view" });
const orders = new Map();
const activities = [];
const publicDirectory = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "public",
);

app.use(express.json());
app.use(express.static(publicDirectory));

app.get("/health", (_request, response) => response.json({ status: "ok" }));
app.get("/orders", (_request, response) => response.json([...orders.values()]));
app.get("/activity", (_request, response) => response.json(activities));

app.post("/orders", async (request, response) => {
  const startedAt = Date.now();
  const correlationId = request.header("x-correlation-id") ?? randomUUID();
  const { customerId, items } = request.body;

  if (!customerId || !Array.isArray(items) || items.length === 0) {
    return response.status(400).json({
      error: "customerId and at least one item are required",
      correlationId,
    });
  }

  const order = {
    orderId: randomUUID(),
    customerId,
    items,
    status: "created",
    createdAt: new Date().toISOString(),
  };
  const event = createOrderCreated(order, correlationId);

  orders.set(order.orderId, order);

  try {
    await producer.send({
      topic: orderTopic,
      messages: [{ key: order.orderId, value: JSON.stringify(event) }],
    });

    log(service, "order created and event published", {
      eventId: event.eventId,
      orderId: order.orderId,
      correlationId,
      responseTimeMs: Date.now() - startedAt,
    });
    addActivity({
      occurredAt: new Date().toISOString(),
      service,
      state: "published",
      eventId: event.eventId,
      orderId: order.orderId,
      correlationId,
    });

    return response.status(201).json({
      order,
      eventId: event.eventId,
      correlationId,
      responseTimeMs: Date.now() - startedAt,
    });
  } catch (error) {
    log(service, "event publish failed", {
      orderId: order.orderId,
      correlationId,
      error: error.message,
    });
    return response.status(503).json({
      error: "Order was saved but its event could not be published",
      orderId: order.orderId,
      correlationId,
    });
  }
});

const port = Number(process.env.PORT ?? 3000);

await Promise.all([producer.connect(), activityConsumer.connect()]);
await activityConsumer.subscribe({ topic: statusTopic, fromBeginning: true });
await activityConsumer.run({
  eachMessage: async ({ message }) => addActivity(JSON.parse(message.value.toString())),
});
const server = app.listen(port, () => log(service, "listening", { port }));

function addActivity(activity) {
  activities.unshift(activity);
  if (activities.length > 200) activities.length = 200;
}

async function shutdown() {
  server.close();
  await Promise.allSettled([producer.disconnect(), activityConsumer.disconnect()]);
  process.exit(0);
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
