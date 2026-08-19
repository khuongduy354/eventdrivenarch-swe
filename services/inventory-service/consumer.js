import { runOrderConsumer } from "../../packages/messaging/consumer.js";

const defaultDelayMs = Number(process.env.INVENTORY_DELAY_MS ?? 0);

await runOrderConsumer({
  service: "inventory-service",
  handle: async (event) => {
    const delayMs = event.data.demo?.inventoryDelayMs ?? defaultDelayMs;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  },
});
