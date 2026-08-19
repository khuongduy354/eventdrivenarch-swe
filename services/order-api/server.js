import { randomUUID } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

import express from "express";

import {
  deadLetterTopic,
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
const deadLetterConsumer = kafka.consumer({ groupId: "order-api-dlt-view" });
const orders = new Map();
const activities = [];
const publishedEvents = [];
const deadLetters = [];
const publicDirectory = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "public",
);

app.use(express.json());
app.use(express.static(publicDirectory));

app.get("/health", (_request, response) => response.json({ status: "ok" }));
app.get("/orders", (_request, response) => response.json([...orders.values()]));
app.get("/activity", (_request, response) => response.json(activities));
app.get("/events", (_request, response) => response.json(publishedEvents));
app.get("/dead-letters", (_request, response) => response.json(deadLetters));

app.post("/orders", async (request, response) => {
  const startedAt = Date.now();
  const correlationId = request.header("x-correlation-id") ?? randomUUID();
  const { customerId, items, demo } = request.body;

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
  const requestedDelayMs = Number(demo?.inventoryDelayMs ?? 0);
  const demoBehavior = {
    failAnalytics: demo?.failAnalytics === true,
    inventoryDelayMs: Number.isFinite(requestedDelayMs)
      ? Math.min(Math.max(requestedDelayMs, 0), 30000)
      : 0,
  };
  const event = createOrderCreated({ ...order, demo: demoBehavior }, correlationId);

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
    publishedEvents.unshift(event);
    if (publishedEvents.length > 100) publishedEvents.length = 100;

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

await Promise.all([
  producer.connect(),
  activityConsumer.connect(),
  deadLetterConsumer.connect(),
]);
await activityConsumer.subscribe({ topic: statusTopic, fromBeginning: true });
await deadLetterConsumer.subscribe({ topic: deadLetterTopic, fromBeginning: true });
await activityConsumer.run({
  eachMessage: async ({ message }) => addActivity(JSON.parse(message.value.toString())),
});
await deadLetterConsumer.run({
  eachMessage: async ({ message }) => {
    deadLetters.unshift(JSON.parse(message.value.toString()));
    if (deadLetters.length > 100) deadLetters.length = 100;
  },
});
const server = app.listen(port, () => log(service, "listening", { port }));

function addActivity(activity) {
  activities.unshift(activity);
  if (activities.length > 200) activities.length = 200;
}

async function shutdown() {
  server.close();
  await Promise.allSettled([
    producer.disconnect(),
    activityConsumer.disconnect(),
    deadLetterConsumer.disconnect(),
  ]);
  process.exit(0);
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
