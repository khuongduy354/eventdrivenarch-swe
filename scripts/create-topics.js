import { deadLetterTopic, orderTopic } from "../packages/messaging/config.js";
import { createKafka } from "../packages/messaging/kafka.js";

const admin = createKafka("topic-setup").admin();

await admin.connect();
await admin.createTopics({
  waitForLeaders: true,
  topics: [
    { topic: orderTopic, numPartitions: 3, replicationFactor: 1 },
    { topic: deadLetterTopic, numPartitions: 1, replicationFactor: 1 },
  ],
});
console.log(`Topics ready: ${orderTopic}, ${deadLetterTopic}`);
await admin.disconnect();
