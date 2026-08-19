Software Architecture Assignment

> Implementation planning: see [PROTOTYPE_PLAN.md](./PROTOTYPE_PLAN.md).

# Bài tập: Event-Driven Architecture

Thiết kế và hiện thực một prototype nhỏ để chứng minh cách áp dụng Event-Driven Architecture trong bài toán xử lý sau khi đơn hàng được tạo.

Thời gian

120 phút

Hình thức

Làm theo nhóm

Số thành viên

3 sinh viên

## Phần 1. Tự tìm hiểu lý thuyết

Event-Driven Architecture

Nhóm tự nghiên cứu Event-Driven Architecture ở mức đủ để có thể giải thích và áp dụng vào bài toán ở Phần 2.

Khi trình bày, nhóm cần làm rõ tối thiểu:

-   Event-Driven Architecture giải quyết loại vấn đề gì trong thiết kế hệ thống.
-   Các thành phần cơ bản trong một hệ thống hướng sự kiện và cách chúng tương tác.
-   Sự khác nhau giữa synchronous communication và asynchronous communication.
-   Một sự kiện có thể được nhiều thành phần xử lý như thế nào.
-   Những vấn đề có thể phát sinh khi message/event không được xử lý như mong đợi.

Nhóm được tự quyết định cách tổ chức phần trình bày. Không bắt buộc trình bày theo danh sách định nghĩa.

## Phần 2. Bài toán

Sales System

Một hệ thống bán hàng hiện có chức năng tiếp nhận đơn hàng. Sau khi một đơn hàng được tạo thành công, hệ thống cần thực hiện thêm một số tác vụ liên quan đến đơn hàng đó.

Các tác vụ này không cần hoàn thành trước khi người dùng nhận được kết quả tạo đơn hàng. Một tác vụ có thể xử lý chậm, tạm thời không hoạt động hoặc gặp lỗi mà không nên làm chức năng tạo đơn hàng thất bại.

Trong tương lai, hệ thống có thể cần bổ sung thêm các chức năng khác cũng phản ứng khi đơn hàng được tạo, chẳng hạn phục vụ chăm sóc khách hàng, thống kê kinh doanh, đồng bộ dữ liệu hoặc các nghiệp vụ mở rộng khác.

Yêu cầu thực hiện:

1.  Đề xuất kiến trúc sử dụng Event-Driven Architecture cho bài toán trên.
2.  Implement một prototype chứng minh kiến trúc đề xuất hoạt động.
3.  Thể hiện được asynchronous service calling: thao tác tạo đơn hàng không phải chờ toàn bộ tác vụ xử lý sau đó hoàn tất.
4.  Thể hiện được fan-out: một sự kiện hoặc thông điệp liên quan đến việc tạo đơn hàng có thể được nhiều thành phần độc lập xử lý.
5.  Thể hiện được việc một thành phần xử lý chậm hoặc lỗi không làm toàn bộ luồng tạo đơn hàng thất bại.

Nhóm tự quyết định:

-   Cách chia service/component trong prototype.
-   Event/message cần sử dụng và nội dung dữ liệu đi kèm.
-   Thành phần nào tạo ra event/message và thành phần nào xử lý event/message.
-   Công nghệ hoặc cơ chế dùng để truyền event/message.
-   Cách chứng minh luồng xử lý là bất đồng bộ và có khả năng mở rộng thêm thành phần xử lý mới.

## Yêu cầu demo

-   Chạy được prototype trên máy khi demo.
-   Tạo được một đơn hàng mẫu và quan sát được các xử lý phát sinh sau đó.
-   Chứng minh thao tác tạo đơn hàng trả kết quả trước khi các xử lý sau đó hoàn tất.
-   Chứng minh một lần tạo đơn hàng có thể kích hoạt nhiều xử lý độc lập.
-   Chứng minh khi một xử lý bị chậm hoặc lỗi, các phần còn lại vẫn có thể tiếp tục theo thiết kế của nhóm.

## Deliverables

-   Source code prototype.
-   File README hướng dẫn cách chạy và cách demo.
-   Sơ đồ hoặc mô tả ngắn kiến trúc nhóm đề xuất.
-   Mô tả các quyết định chính: cách chia component, event/message, cơ chế truyền message và cách xử lý khi có lỗi.
-   Minh chứng demo: log, ảnh chụp màn hình hoặc video ngắn thể hiện các yêu cầu demo.

---

# Prototype: Event-Driven Order Processing

The implementation uses Node.js and Kafka to process work after an order is created. See [PROTOTYPE_PLAN.md](./PROTOTYPE_PLAN.md) for the implementation plan.

## Architecture

```text
POST /orders
     |
     v
 Order API -- order.created --> Kafka
                                  |-- notification-service group
                                  |-- inventory-service group (8-second delay)
                                  `-- analytics-service group (demo failure)
                                                   |
                                                   `--> order.created.dlt
```

The API waits for Kafka to accept `OrderCreated`, but it does not wait for any consumer to finish. Each service has a different consumer group, so every service receives the event. Replicas of one service would share its group and divide partitions.

## Requirements

| Requirement | How the prototype demonstrates it |
| --- | --- |
| Asynchronous service calling | `POST /orders` returns after publishing, before consumers finish |
| Fan-out | Notification, inventory, and analytics use separate consumer groups |
| Slow component isolation | Inventory deliberately waits eight seconds |
| Failure isolation | Analytics can fail while the other groups continue |
| Error handling | Consumers retry three times, then publish to a DLT |
| Extensibility | A new service subscribes using another consumer group |

## Event

`OrderCreated` contains:

- `eventId` for tracing and basic deduplication.
- `correlationId` for connecting API and consumer logs.
- `orderId` as the Kafka key, preserving per-order partition ordering.
- `occurredAt` and the order payload.

The dead-letter record additionally contains the failed service, attempt count, error, partition, offset, and failure time.

## Run

The simplest path runs the complete microservices prototype in Docker:

```bash
docker compose up --build
```

Compose starts Kafka, creates the topics, then starts the API and four independent worker services. Analytics failure is enabled by default.

In another terminal:

```bash
npm run demo
```

The API response should appear immediately. Notification completes quickly, analytics retries and enters the DLT, and inventory completes after eight seconds.

Open `http://localhost:3000` for a small browser UI that creates and lists orders.

For a successful analytics run, change `ANALYTICS_FAIL` to `"false"` in `docker-compose.yml`, then restart Compose.

To run the Node.js services directly instead, keep Kafka running and use:

```bash
npm install
docker compose up -d kafka
npm run topics
ANALYTICS_FAIL=false npm run start:all
```

Services can also be run separately with `npm run api`, `npm run notification`, `npm run inventory`, `npm run analytics`, and `npm run dlt`.

## Project Structure

```text
services/
├── order-api/                 # HTTP API and demo UI
├── notification-service/      # Independent Kafka consumer
├── inventory-service/         # Independent slow consumer
├── analytics-service/         # Independent failing consumer
└── dead-letter-monitor/       # DLT consumer
packages/
└── messaging/                 # Shared Kafka/event contract code
scripts/                       # Topic setup and local demo helpers
docker-compose.yml             # Complete microservices environment
```

Every directory under `services/` is a separately running process and container. The shared package avoids duplicating the event contract and Kafka retry plumbing inside this small monorepo.

Cleanup:

```bash
docker compose down -v
```

## Error Handling

- Processing is attempted at most three times with short exponential backoff.
- A successfully processed event is remembered by `eventId` for the lifetime of the consumer process.
- An exhausted or malformed event is published to `order.created.dlt`.
- The original callback only completes after successful processing or successful DLT publication.
- Consumer groups isolate failures: analytics retries do not stop notification or inventory.

## Decisions and Trade-offs

| Decision | Reason | Trade-off |
| --- | --- | --- |
| Node.js | Team familiarity and fast prototyping | Kafka is language-independent |
| Kafka | Familiar consumer-group and partition model | More infrastructure than an in-memory queue |
| Separate group per service | Every service receives the event | Each group stores independent offsets |
| Same group for replicas | Load balancing | Parallelism is limited by partition count |
| `orderId` message key | Preserve ordering for one order | No global ordering guarantee |
| Bounded inline retries | Small and easy to demonstrate | A retry temporarily blocks that group partition |
| Dead-letter topic | Preserve exhausted failures | Replay is manual in this prototype |
| At-least-once processing | Practical Kafka failure model | Consumers must tolerate duplicates |
| No transactional outbox | Keep the 120-minute prototype focused | A crash between saving and publishing can lose an event |

## Known Limitations

- Orders and processed-event IDs are stored in memory and do not survive restart.
- There is no transactional outbox relay.
- Publishing to the DLT and committing the original offset are not one atomic operation, so duplicates remain possible.
- Retry topics, automated DLT replay, authentication, schema registry, metrics, and distributed tracing are intentionally omitted.

Production improvements would add durable order/idempotency storage, a transactional outbox when event loss is unacceptable, retry topics, automated DLT tooling, monitoring, and security.

## Demo Evidence

- [API response](./evidence/api-response.json): the API returned in tens of milliseconds.
- [Failure demo log](./evidence/failure-demo.log): notification succeeds, inventory completes independently, analytics retries three times, and the event reaches the DLT.

## Tests

```bash
npm test
```
