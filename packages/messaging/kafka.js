import { Kafka, logLevel, Partitioners } from "kafkajs";

import { brokers } from "./config.js";

export function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

export function createProducer(kafka) {
  return kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });
}
