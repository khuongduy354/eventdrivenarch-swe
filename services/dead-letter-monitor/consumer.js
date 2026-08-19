import { deadLetterTopic } from "../../packages/messaging/config.js";
import { createKafka } from "../../packages/messaging/kafka.js";
import { log } from "../../packages/messaging/logger.js";

const service = "dead-letter-monitor";
const consumer = createKafka(service).consumer({ groupId: service });

await consumer.connect();
await consumer.subscribe({ topic: deadLetterTopic, fromBeginning: true });
log(service, "consumer started", { topic: deadLetterTopic });

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

await consumer.run({
  eachMessage: async ({ message }) => {
    const failure = JSON.parse(message.value.toString());
    log(service, "dead-letter event received", failure);
  },
});

async function shutdown() {
  await consumer.disconnect();
  process.exit(0);
}
