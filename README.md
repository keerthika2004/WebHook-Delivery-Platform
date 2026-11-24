# Hooklane — Reliable Webhook Delivery Platform

> **Honest disclaimer:** Hooklane is a **learning project**, not a commercial product. Battle-tested platforms like [Svix](https://www.svix.com/) and [Hookdeck](https://hookdeck.com/) already solve webhook delivery in production. Hooklane was built from scratch to deeply understand the distributed-systems mechanics behind webhook infrastructure — transactional outbox, non-blocking Kafka retries, Redis Lua circuit breakers and rate limiters, SSRF-safe HTTP dispatch, and real measured failure/latency numbers.

---

## Mental Model: The Courier Office

| Courier Office Analogy | Hooklane Component |
| :--- | :--- |
| Shop handing in a parcel | Customer's backend sending an **Event** (`POST /api/v1/events`) |
| Reception counter | **`webhook-api`** service |
| Register books | **PostgreSQL 16** tables (`applications`, `endpoints`, `events`, `deliveries`, `delivery_attempts`) |
| "To be sent" tray | **`outbox`** table (polled and relayed to Kafka) |
| Conveyor belt with 12 lanes | **Apache Kafka** topic `webhook.deliveries` (12 partitions) |
| Delivery person | **`webhook-worker`** service |
| Customer's house & knock on door | **Endpoint** URL & outgoing HTTP **Webhook** |
| "Nobody home, try again later" | Non-blocking **Retry Topics** (`retry-0` … `retry-4`) |
| "Undeliverable" shelf | **Dead-Letter Queue / Topic** (`webhook.deliveries.dlt`, status `DEAD`) |
| Wax seal on the envelope | **HMAC-SHA256 signature** (`webhook-signature` header) |
| Quick-notes whiteboard | **Redis 7** (API-key cache, circuit breaker state, token-bucket rate limits, live SSE pub/sub) |
| Fake house we can make slow or broken | **`chaos-receiver`** service |

---

## Why Two Services (and Not One)?

Hooklane is split into four Maven modules:

1. **`webhook-common`** — Shared library (not a runnable app): Kafka message contracts, Standard Webhooks HMAC signer/verifier, URL safety checks.
2. **`webhook-api`** — Accepts events over HTTP, runs the transactional outbox relay, serves REST endpoints and live SSE updates. **Latency-sensitive; scales on incoming HTTP traffic / CPU.**
3. **`webhook-worker`** — Consumes from Kafka and dispatches HTTP webhooks to customer endpoints. **Throughput/IO-bound; scales on Kafka consumer lag (via KEDA).**
4. **`chaos-receiver`** — Controllable test receiver (`OK`, `SLOW`, `FLAKY`, `HANG`, fixed HTTP status) used to verify zero message loss and failure isolation.

We run two services (`webhook-api` and `webhook-worker`) **because they scale on completely different signals** — not because microservices are trendy. A spike in slow customer endpoints increases worker lag without slowing down event ingestion at the API reception counter.

---

## Key Design Decisions

1. **Ordering (`webhook-sequence`, no strict per-endpoint blocking):**
   We do **not** guarantee strict per-endpoint delivery ordering. Kafka messages use `endpoint_id` as the partition key, so deliveries to the same endpoint stay in order on the happy path. When an attempt fails, the message moves to a retry topic so later messages for that endpoint (and other endpoints) can proceed instead of stalling behind a broken delivery. Every delivery carries a monotonically increasing `webhook-sequence` header (`deliveries.seq`) so receivers that care about ordering can detect out-of-order arrivals. *(Note: manual replays create a new delivery with a new, higher `webhook-sequence`; receivers implementing "highest sequence wins" should also inspect event timestamps / replay metadata.)*
2. **Delivery Guarantee (At-Least-Once):**
   Hooklane guarantees **at-least-once** delivery. Duplicates are possible (for example, if a worker crashes after an HTTP `200` before committing its Kafka offset, or if the outbox relay crashes right after Kafka acknowledges a batch); silent event loss is not. Receivers deduplicate using the `webhook-id` header, which stays identical across all retry attempts of the same delivery.
3. **Transactional Outbox (No Dual Writes):**
   `POST /api/v1/events` never writes to Postgres and Kafka in the same HTTP request. Instead, a single Postgres transaction inserts the `events`, `deliveries`, and `outbox` rows atomically. A background relay polls unpublished `outbox` rows (`FOR UPDATE SKIP LOCKED`), publishes them to Kafka with `acks=all`, and marks `published_at` only after broker acknowledgement.
4. **Non-Blocking Retries:**
   Workers never call `Thread.sleep()` waiting to retry a failing endpoint. Retryable failures (`5xx`, `408`, `429`, network timeouts) are routed through delayed Kafka retry topics (`webhook.deliveries.retry-0` through `retry-4` with exponential backoff: `5s`, `15s`, `45s`, `2m15s`, `6m45s`) before landing on `webhook.deliveries.dlt` (`DEAD`). Permanent failures (`4xx` other than `408`/`429`, disabled/deleted endpoints, SSRF-blocked IPs) go straight to `DEAD` without wasting retries.
5. **Standard Webhooks Signing:**
   Outgoing webhooks follow the [Standard Webhooks](https://www.standardwebhooks.com/) signing format:
   - `webhook-id`: delivery UUID
   - `webhook-timestamp`: Unix epoch seconds
   - `webhook-signature`: `v1,<base64(HMAC-SHA256(secret, "{id}.{timestamp}.{body}"))>` (supports space-separated signatures during secret rotation and verifies in constant time).

---

## Measured Reliability & Performance Results

*(Populated as each milestone exit test is executed.)*

- **Milestone 1 — Zero-Loss Worker Crash Test:** *Pending Milestone 1*
- **Milestone 2 — Noisy-Neighbor / Hanging Endpoint Isolation (Before vs. After p95):** *Pending Milestone 2*
- **Milestone 6 — AWS EKS + k6 Load Test Benchmarks:** *Pending Milestone 6*
