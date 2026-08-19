import {
  deadLetterTopic,
  maxAttempts,
  orderTopic,
  statusTopic,
} from "./config.js";
import { createKafka, createProducer } from "./kafka.js";
import { log } from "./logger.js";
import { parseOrderCreated } from "./events.js";

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

export async function runOrderConsumer({ service, handle }) {
  const kafka = createKafka(service);
  const consumer = kafka.consumer({ groupId: service });
  const producer = createProducer(kafka);
  const processedEventIds = new Set();

  await Promise.all([consumer.connect(), producer.connect()]);
  await consumer.subscribe({ topic: orderTopic, fromBeginning: true });

  log(service, "consumer started", { topic: orderTopic });

  const shutdown = async () => {
    await Promise.allSettled([consumer.disconnect(), producer.disconnect()]);
    process.exit(0);
  };

  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      let event;

      try {
        event = parseOrderCreated(message.value);
      } catch (error) {
        await publishDeadLetter(producer, {
          service,
          partition,
          offset: message.offset,
          attempts: 0,
          error: error.message,
          originalValue: message.value?.toString(),
        });
        return;
      }

      if (processedEventIds.has(event.eventId)) {
        log(service, "duplicate ignored", identifiers(event));
        return;
      }

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        try {
          log(service, "processing started", { ...identifiers(event), attempt });
          await emitStatus(producer, service, event, "processing", { attempt });
          await handle(event, attempt);
          processedEventIds.add(event.eventId);
          log(service, "processing completed", { ...identifiers(event), attempt });
          await emitStatus(producer, service, event, "completed", { attempt });
          return;
        } catch (error) {
          log(service, "processing failed", {
            ...identifiers(event),
            attempt,
            error: error.message,
          });
          await emitStatus(producer, service, event, "failed", {
            attempt,
            error: error.message,
          });

          if (attempt < maxAttempts) {
            await emitStatus(producer, service, event, "retrying", {
              attempt,
              nextAttempt: attempt + 1,
            });
            await sleep(2 ** (attempt - 1) * 1000);
          } else {
            await publishDeadLetter(producer, {
              service,
              partition,
              offset: message.offset,
              attempts: attempt,
              error: error.message,
              failedAt: new Date().toISOString(),
              event,
            });
            await emitStatus(producer, service, event, "dead-lettered", {
              attempt,
              error: error.message,
            });
          }
        }
      }
    },
  });
}

async function emitStatus(producer, service, event, state, details = {}) {
  try {
    await producer.send({
      topic: statusTopic,
      messages: [
        {
          key: event.data.orderId,
          value: JSON.stringify({
            occurredAt: new Date().toISOString(),
            service,
            state,
            ...identifiers(event),
            ...details,
          }),
        },
      ],
    });
  } catch (error) {
    log(service, "status publish failed", {
      ...identifiers(event),
      state,
      error: error.message,
    });
  }
}

async function publishDeadLetter(producer, failure) {
  const { service, ...details } = failure;
  const payload = { ...details, failedService: service };

  await producer.send({
    topic: deadLetterTopic,
    messages: [
      {
        key: failure.event?.data?.orderId ?? service,
        value: JSON.stringify(payload),
      },
    ],
  });

  log(service, "sent to dead-letter topic", {
    eventId: failure.event?.eventId,
    orderId: failure.event?.data?.orderId,
    attempts: failure.attempts,
    error: failure.error,
  });
}

function identifiers(event) {
  return {
    eventId: event.eventId,
    orderId: event.data.orderId,
    correlationId: event.correlationId,
  };
}
