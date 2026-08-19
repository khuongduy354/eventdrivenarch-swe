import { spawn } from "node:child_process";

const processes = [
  ["services/order-api/server.js", {}],
  ["services/notification-service/consumer.js", {}],
  ["services/inventory-service/consumer.js", {}],
  [
    "services/analytics-service/consumer.js",
    { ANALYTICS_FAIL: process.env.ANALYTICS_FAIL ?? "false" },
  ],
  ["services/dead-letter-monitor/consumer.js", {}],
].map(([entrypoint, extraEnv]) =>
  spawn(process.execPath, [entrypoint], {
    env: { ...process.env, ...extraEnv },
    stdio: "inherit",
  }),
);

function shutdown() {
  for (const child of processes) child.kill("SIGTERM");
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
