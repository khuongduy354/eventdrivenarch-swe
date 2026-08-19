import { runOrderConsumer } from "../../packages/messaging/consumer.js";

const delayMs = Number(process.env.INVENTORY_DELAY_MS ?? 8000);

await runOrderConsumer({
  service: "inventory-service",
  handle: async () => {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  },
});
