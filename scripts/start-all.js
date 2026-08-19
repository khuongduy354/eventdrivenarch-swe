import { spawn } from "node:child_process";

const processes = [
  ["src/order-api/server.js", {}],
  ["src/consumers/notification.js", {}],
  ["src/consumers/inventory.js", {}],
  [
    "src/consumers/analytics.js",
    { ANALYTICS_FAIL: process.env.ANALYTICS_FAIL ?? "true" },
  ],
  ["src/consumers/dead-letter.js", {}],
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
