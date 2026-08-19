import { randomUUID } from "node:crypto";

export function createOrderCreated(order, correlationId) {
  return {
    eventId: randomUUID(),
    eventType: "OrderCreated",
    occurredAt: new Date().toISOString(),
    correlationId,
    data: order,
  };
}

export function parseOrderCreated(value) {
  const event = JSON.parse(value.toString());

  if (
    event.eventType !== "OrderCreated" ||
    !event.eventId ||
    !event.data?.orderId
  ) {
    throw new Error("Invalid OrderCreated event");
  }

  return event;
}
