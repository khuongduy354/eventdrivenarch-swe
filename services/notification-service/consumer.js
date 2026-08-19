import { runOrderConsumer } from "../../packages/messaging/consumer.js";

await runOrderConsumer({
  service: "notification-service",
  handle: async () => {
    await new Promise((resolve) => setTimeout(resolve, 250));
  },
});
