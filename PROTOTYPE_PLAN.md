# Event-Driven Order Processing Prototype Plan

## Objective

Build a small Node.js and Kafka prototype that proves:

1. Creating an order does not wait for downstream processing.
2. One `OrderCreated` event can be processed by multiple independent services.
3. A slow or failing consumer does not fail order creation or stop other consumers.
4. Failed processing is retried a bounded number of times and then retained in a dead-letter topic (DLT).
5. Logs provide clear evidence for every required behavior.

## Scope

The prototype will implement the assignment requirements and prioritize observable error handling. It will not implement a transactional outbox relay within the 120-minute scope. The resulting DB-to-Kafka dual-write risk will be documented as a known limitation and production improvement.

## Architecture

```text
Client
  | POST /orders
  v
Order API
  |-- save order
  |-- publish OrderCreated (key: orderId)
  `-- return HTTP 201 without waiting for consumers
             |
             v
      order.created topic
        |-- notification-service consumer group
        |-- inventory-service consumer group (deliberately slow)
        `-- analytics-service consumer group (deliberately fails)
                                                |
                                                | bounded retries
                                                v
                                       order.created.dlt
                                                |
                                                v
                                           DLT consumer
```

Each service uses a different consumer-group ID so every service receives the event. Multiple instances of the same service use the same group ID so Kafka distributes partitions between those instances.

## Components

### Order API

`POST /orders` will:

1. Validate the request.
2. Generate an `orderId`, `eventId`, and `correlationId`.
3. Save the order.
4. Publish `OrderCreated`, keyed by `orderId`.
5. Return `201 Created` after Kafka acknowledges the publish.
6. Never wait for notification, inventory, or analytics processing.

Waiting for Kafka to accept the event is different from waiting for downstream services to finish; the latter is the asynchronous-service-calling requirement.

### Notification consumer

- Consumer group: `notification-service`
- Simulates sending an order confirmation.
- Normally succeeds quickly.

### Inventory consumer

- Consumer group: `inventory-service`
- Deliberately waits for a configured number of seconds.
- Proves that a slow consumer does not delay the API or other consumer groups.

### Analytics consumer

- Consumer group: `analytics-service`
- Supports a deterministic demo failure mode.
- Retries failed processing up to three times with backoff.
- Publishes an exhausted failure to `order.created.dlt`.

### DLT consumer

- Consumes `order.created.dlt`.
- Logs the original event, failed service, attempt count, error, and failure time.
- Demonstrates that an unprocessable event is retained for investigation or replay.

## Event Contract

```json
{
  "eventId": "evt-123",
  "eventType": "OrderCreated",
  "occurredAt": "2026-08-19T08:00:00Z",
  "correlationId": "req-456",
  "data": {
    "orderId": "order-789",
    "customerId": "customer-1",
    "items": [
      {
        "productId": "product-1",
        "quantity": 2
      }
    ]
  }
}
```

- `eventId` supports deduplication and tracing.
- `correlationId` connects API and consumer logs.
- `orderId` is the Kafka message key, preserving ordering for one order within a partition.
- `occurredAt` distinguishes event time from processing time.

## Error-Handling Design

For each consumer:

```text
Receive event
  |-- event already processed -> commit offset
  `-- process event
        |-- success -> record eventId -> commit offset
        `-- failure
              |-- attempts remain -> backoff and retry
              `-- attempts exhausted -> publish to DLT -> commit original offset
```

Initial retry schedule:

```text
Attempt 1 fails -> wait 1 second
Attempt 2 fails -> wait 2 seconds
Attempt 3 fails -> publish to DLT
```

Implementation rules:

- Do not treat an event as successfully consumed before processing succeeds.
- Commit the original offset only after processing succeeds or the DLT publish succeeds.
- Bound retry attempts so poison events do not retry forever.
- Include `eventId`, `orderId`, `correlationId`, service name, and attempt in logs.
- Use `eventId` for basic idempotency because at-least-once processing can produce duplicates.
- Document that inline retry temporarily blocks that consumer group's partition. Production retry topics are outside the initial scope.

## Proposed Repository Structure

```text
.
|-- docker-compose.yml
|-- Dockerfile
|-- package.json
|-- README.md
|-- PROTOTYPE_PLAN.md
|-- services
|   |-- order-api
|   |-- notification-service
|   |-- inventory-service
|   |-- analytics-service
|   `-- dead-letter-monitor
|-- packages
|   `-- messaging
|-- scripts
|   |-- create-topics.js
|   `-- demo.sh
`-- evidence
    |-- api-response.json
    `-- failure-demo.log
```

Each directory under `services/` is an independently running process and Docker Compose service. `packages/messaging` contains the shared event contract and Kafka plumbing used by this prototype monorepo.

## Implementation Sequence

### 1. Infrastructure

- Add Docker Compose configuration for Kafka.
- Add shared Kafka producer and consumer configuration.
- Create `order.created` and `order.created.dlt`.
- Verify local producer and consumer connectivity.

### 2. Happy path

- Implement `POST /orders`.
- Publish `OrderCreated`.
- Implement notification, inventory, and analytics consumers.
- Assign a different consumer group to each service.
- Verify that all services receive the same event.

### 3. Asynchronous behavior

- Add a deliberate delay to inventory processing.
- Log the API response time and all consumer timestamps.
- Verify that HTTP `201` is returned before inventory completes.

### 4. Failure behavior

- Add deterministic analytics failure configuration.
- Add bounded retries and backoff.
- Publish exhausted failures to the DLT.
- Add the DLT consumer.
- Verify that notification and inventory still complete normally.

### 5. Reliability and evidence

- Add structured, correlated logs.
- Add basic `eventId` deduplication.
- Add a repeatable demo script.
- Save successful and failure logs or screenshots under `evidence/`.
- Complete the README using the checklist below.

## Demo Plan

### Scenario A: normal processing

1. Start Kafka, the API, and all consumers.
2. Create an order.
3. Show the API returning immediately.
4. Show notification and analytics completing.
5. Show inventory completing several seconds later.

### Scenario B: failed consumer

1. Enable deterministic analytics failure mode.
2. Create another order.
3. Show the API still returning `201`.
4. Show notification and inventory succeeding independently.
5. Show analytics retrying three times.
6. Show the exhausted event arriving in the DLT.

Expected log timeline:

```text
10:00:00.100 order-api: published evt-123
10:00:00.110 order-api: returned HTTP 201
10:00:00.300 notification: completed evt-123
10:00:01.000 analytics: attempt 1 failed
10:00:03.000 analytics: attempt 2 failed
10:00:05.000 analytics: sent evt-123 to DLT
10:00:08.300 inventory: completed evt-123
```

## README Write-Up Checklist

The final README must contain more than setup instructions. It must explain the architecture, decisions, trade-offs, and evidence.

### Required sections

1. **Problem statement**: why downstream order work should not run synchronously in the order request.
2. **Architecture diagram**: API, Kafka topic, consumer groups, retries, and DLT.
3. **Component responsibilities**: what every component produces, consumes, and demonstrates.
4. **Event contract**: all `OrderCreated` fields and their purpose.
5. **Requirement mapping**: connect each assignment requirement to implementation evidence.
6. **Key decisions and trade-offs**: record why the team chose each major design.
7. **Error handling**: retries, DLT, idempotency, group isolation, and ordering.
8. **Setup and run instructions**: exact commands from a clean environment.
9. **Demo instructions**: copy-paste commands and expected output for both scenarios.
10. **Known limitations and production improvements**: be explicit about intentionally omitted production features.

### Requirement mapping table

| Requirement | Implementation and evidence |
| --- | --- |
| Asynchronous service calling | API returns after publishing and does not wait for consumers |
| Fan-out | Three different consumer groups receive `OrderCreated` |
| Slow component isolation | Inventory waits without delaying the API or other groups |
| Failure isolation | Analytics failure does not affect notification or inventory |
| Error handling | Bounded retries followed by a DLT |
| Extensibility | A new service can subscribe with another consumer group |

### Decisions to document

| Decision | Reason | Trade-off |
| --- | --- | --- |
| Node.js | Team familiarity and rapid implementation | Kafka itself is language-independent |
| Kafka | Team familiarity with consumer groups and partitions | More infrastructure than an in-memory queue |
| Separate group per service | Every service receives the event | Each group maintains independent offsets |
| Same group for service replicas | Load balancing and horizontal scaling | Parallelism is limited by partition count |
| `orderId` partition key | Per-order ordering | No global ordering guarantee |
| Controlled offset commits | Failed work is not silently treated as complete | Consumer logic becomes more complex |
| Three bounded attempts | Prevents infinite poison-event loops | Inline retry temporarily blocks one group partition |
| Dead-letter topic | Retains exhausted failures | Requires inspection and replay procedures |
| At-least-once processing | Practical Kafka delivery model | Consumers must tolerate duplicates |
| No transactional outbox | Keeps the prototype achievable in 120 minutes | A crash between DB commit and publish can lose an event |

### Known limitations to disclose

- No transactional outbox relay.
- Prototype idempotency storage may not survive restarts.
- Inline retry temporarily blocks a consumer-group partition.
- DLT replay is manual.
- Kafka authentication, encryption, metrics, and schema registry are omitted.
- Production improvements would include durable idempotency storage, retry topics, automated DLT replay, tracing, monitoring, and an outbox when publication loss is unacceptable.

## Suggested Team Split

- **Member 1:** Kafka/Docker setup and Order API producer.
- **Member 2:** Consumers, consumer groups, and slow-processing demonstration.
- **Member 3:** Retry/DLT flow, logs, demo evidence, and README documentation.

The team should integrate early so the final demo proves the complete event flow rather than three isolated components.
