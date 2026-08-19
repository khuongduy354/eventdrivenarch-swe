import { runOrderConsumer } from "../../packages/messaging/consumer.js";

const defaultFail = process.env.ANALYTICS_FAIL === "true";

await runOrderConsumer({
  service: "analytics-service",
  handle: async (event) => {
    const fail = event.data.demo?.failAnalytics ?? defaultFail;
    if (fail) {
      throw new Error("Simulated analytics failure");
    }
  },
});
