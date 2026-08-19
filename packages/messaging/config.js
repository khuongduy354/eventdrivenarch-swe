export const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
export const orderTopic = "order.created";
export const deadLetterTopic = "order.created.dlt";
export const statusTopic = "order.processing-status";
export const maxAttempts = Number(process.env.MAX_ATTEMPTS ?? 3);
