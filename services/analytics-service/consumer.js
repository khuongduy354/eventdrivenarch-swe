import { runOrderConsumer } from "../../packages/messaging/consumer.js";

const fail = process.env.ANALYTICS_FAIL === "true";

await runOrderConsumer({
  service: "analytics-service",
  handle: async () => {
    if (fail) {
      throw new Error("Simulated analytics failure");
    }
  },
});
