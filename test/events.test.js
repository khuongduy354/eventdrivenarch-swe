import assert from "node:assert/strict";
import test from "node:test";

import { createOrderCreated, parseOrderCreated } from "../src/shared/events.js";

test("creates and parses an OrderCreated event", () => {
  const order = { orderId: "order-1", customerId: "customer-1", items: [{}] };
  const event = createOrderCreated(order, "request-1");

  assert.deepEqual(parseOrderCreated(Buffer.from(JSON.stringify(event))).data, order);
  assert.equal(event.eventType, "OrderCreated");
  assert.equal(event.correlationId, "request-1");
  assert.ok(event.eventId);
});

test("rejects malformed events", () => {
  assert.throws(
    () => parseOrderCreated(Buffer.from('{"eventType":"Other"}')),
    /Invalid OrderCreated event/,
  );
});
