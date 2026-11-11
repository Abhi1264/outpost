# Outpost

An asynchronous financial ledger with webhook delivery.

Outpost records money movement with double-entry bookkeeping, then notifies subscribed HTTP endpoints after the write commits. The two jobs share one rule: a webhook is never promised unless the ledger row exists.

This document is the system design. It is written so a new engineer can run the service, call the API, and understand the invariants without reading every class first.

## Goals

- Never create or destroy money. Every posting is one debit and one credit of the same amount.
- Never let a customer balance go below zero, even under concurrent load.
- Make retries safe. The same `Idempotency-Key` returns the original success, not a second posting.
- Deliver webhooks at least once, with a signed payload, backoff, and a dead-letter queue after the last attempt.
- Survive a crash between “ledger written” and “webhook sent.” The outbox table is the source of truth; Kafka is a wake-up signal.

## Non-goals

- Multi-currency transfers. Source and destination must share a currency.
- Event-type filtering on subscriptions. Every active subscription receives `transaction.completed`.
- OAuth, API keys, or multi-tenant RBAC. The API uses HTTP Basic.
- A public admin UI.

## Stack

| Piece       | Version       | Role                                 |
| ----------- | ------------- | ------------------------------------ |
| Java        | 26            | Runtime. Virtual threads are on.     |
| Spring Boot | 4.1.2         | HTTP, JPA, Kafka, Flyway             |
| PostgreSQL  | 18            | Ledger, outbox, idempotency store    |
| Kafka       | 4.3.1 (KRaft) | Dispatch topic and dead-letter topic |

Schema lives in `src/main/resources/db/migration/V1__init_schema.sql`. Flyway applies it on startup. Hibernate only validates; it does not create tables.

## How it works

```
Client ──HTTP──► API ──tx──► accounts + ledger_entries + webhook_outbox
                              │
                              └─ AFTER COMMIT ──► Kafka dispatch topic
                                                       │
                                                       ▼
                                              WebhookDispatcher
                                                       │
                                                       ▼
                                              subscriber URL
```

Three paths matter. They are independent except at the outbox write.

### 1. Double-entry ledger

Every deposit, withdrawal, or transfer posts **exactly two** journal rows under one `transaction_id`:

| Operation | Debit | Credit |
| --- | --- | --- |
| Deposit | system clearing account | customer |
| Withdrawal | customer | system clearing account |
| Transfer | source customer | destination customer |

`DEBIT` decreases a balance. `CREDIT` increases it. Amounts are `BigDecimal` with scale 4 and `RoundingMode.HALF_EVEN`. `float` and `double` are not used.

Customer accounts cannot go negative. That is checked in Java under a row lock and again by a Postgres `CHECK` constraint.

Money that enters or leaves the ledger is booked against a **system clearing account**. There are eight clearing accounts per currency (`SYS-CLEARING-USD-0` … `SYS-CLEARING-USD-7`). A customer always maps to the same shard, so deposits and withdrawals for different customers do not serialize on one hot row. Across a currency, `sum(customer balances) + sum(clearing balances) = 0`.

**Concurrency.** Every balance change takes `SELECT … FOR UPDATE` on the involved rows. Lock order is fixed: customer accounts by ascending account number, then the clearing account. That is what keeps bidirectional transfers from deadlocking.

**What one transaction writes.** The two journal rows, the updated balances, and one `webhook_outbox` row per active subscription. If the transaction rolls back, none of those exist.

### 2. Idempotency

POST requests may send `Idempotency-Key`. The filter only runs when the caller is authenticated.

| Situation | Result |
| --- | --- |
| First success (2xx) | Response stored in `idempotency_keys`. |
| Same key, same body | Stored response replayed. Header `Idempotent-Replayed: true`. |
| Same key, different body | `422`. |
| Same key, transaction still open | Blocks on the primary key until that transaction commits, then replays or returns `422`. |
| First attempt failed (4xx / 5xx) | Nothing stored. The key can be reused. |

The stored row commits in the same database transaction as the journal. A crash cannot leave a posting without its replay record.

Keys are scoped as `<username>:<header>`. A SHA-256 of method + URI + body is what “same request” means. Stored keys older than `outpost.idempotency.ttl` (default 24h) are purged hourly.

### 3. Webhook delivery

This is a transactional outbox, not “publish Kafka inside the ledger transaction.”

1. `OutboxWriter` inserts `PENDING` rows in the same database transaction as the journal.
2. After commit, `OutboxRelay` publishes each outbox id to `outpost.webhook.dispatch`.
3. A poller republishes due rows every 5 seconds. That covers Kafka publish failures, retries, and workers that died mid-delivery.
4. `WebhookDispatcher` atomically claims the row (`PENDING` → `PROCESSING`) and POSTs the payload. Duplicate Kafka messages are ignored if the claim returns 0.
5. 2xx → `DELIVERED`. A 3xx is a permanent failure. Transient failure → `PENDING` with `next_retry_at`. Permanent failure or exhausted retries → `FAILED`. That row is committed before a copy is sent to `outpost.webhook.dlq`.

Rows stuck in `PROCESSING` longer than 5 minutes are treated as a crashed worker and returned to `PENDING`.

**Retry schedule.** Default: 10s, 30s, 2m, 10m, 1h, each ±20% jitter. That is one first attempt plus five retries.

**What is retried.** 5xx, timeouts, connection errors, 408, and 429. 3xx, other 4xx, and an invalid or blocked target URL fail immediately.

**Signature.** Every delivery sends:

```
X-Signature-256: sha256=<hex(HMAC-SHA256(raw body, secret))>
X-Webhook-Id: <outbox uuid>
X-Webhook-Event: transaction.completed
X-Webhook-Attempt: <n>
Content-Type: application/json
```

The secret is shown once, when the subscription is created. Recipients should recompute the HMAC over the raw body and compare it to the header.

**Payload shape.**

```json
{
  "eventType": "transaction.completed",
  "occurredAt": "2026-09-12T22:42:45.550851Z",
  "data": {
    "transactionId": "…",
    "type": "DEPOSIT",
    "currency": "USD",
    "amount": 100.5000,
    "occurredAt": "…",
    "entries": [
      { "accountNumber": "SYS-CLEARING-USD-3", "entryType": "DEBIT", "amount": 100.5000, "runningBalance": -100.5000, "createdAt": "…" },
      { "accountNumber": "ACC307412619537", "entryType": "CREDIT", "amount": 100.5000, "runningBalance": 100.5000, "createdAt": "…" }
    ]
  }
}
```

The dead-letter record includes `outboxId`, `eventType`, `targetUrl`, `attempts`, `lastError`, and `payload`. It does not include the secret. The `FAILED` outbox row is that record; it remains if the Kafka publish is rejected.

## API

All routes except `/actuator/health/**` require HTTP Basic. Errors use RFC 9457 Problem Details (`application/problem+json`).

Amounts are decimal strings or numbers with at most 15 integer digits and 4 fraction digits, and must be ≥ `0.0001`. Currency is three uppercase letters.

### Accounts and postings

| Method | Path | Body | Success |
| --- | --- | --- | --- |
| `POST` | `/api/v1/accounts` | `{ "currency": "USD" }` | `201` account |
| `GET` | `/api/v1/accounts/{accountNumber}` | | `200` account |
| `GET` | `/api/v1/accounts/{accountNumber}/entries` | `?page=&size=` (default size 50) | `200` journal page |
| `POST` | `/api/v1/accounts/{accountNumber}/deposits` | `{ "amount": 100.5 }` | `201` transaction |
| `POST` | `/api/v1/accounts/{accountNumber}/withdrawals` | `{ "amount": 10 }` | `201` transaction |
| `POST` | `/api/v1/transfers` | `{ "fromAccountNumber", "toAccountNumber", "amount" }` | `201` transaction |

Account response:

```json
{
  "id": "9ea8ce19-7533-48b4-80b0-0eda395273b4",
  "accountNumber": "ACC307412619537",
  "currency": "USD",
  "balance": 0.0000,
  "createdAt": "2026-09-12T22:42:45.297556Z"
}
```

Common status codes: `400` invalid body, `401` missing or bad credentials, `404` unknown customer account, `409` constraint clash, `422` insufficient funds, currency mismatch, or reused idempotency key with a different body.

### Webhook subscriptions

| Method   | Path                    | Body                           | Success                                        |
| -------- | ----------------------- | ------------------------------ | ---------------------------------------------- |
| `POST`   | `/api/v1/webhooks`      | `{ "targetUrl": "https://…" }` | `201` subscription **with** `secret`           |
| `GET`    | `/api/v1/webhooks`      |                                | `200` this client's subscriptions (no secrets) |
| `DELETE` | `/api/v1/webhooks/{id}` |                                | `204` deactivate                               |

`targetUrl` must start with `http://` or `https://`. Delete is a soft deactivate: later postings skip that URL.

### Example

```bash
export AUTH='-u outpost:local-dev-secret'

curl -s $AUTH -H 'Content-Type: application/json' \
  -d '{"currency":"USD"}' localhost:8080/api/v1/accounts

curl -s $AUTH -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: deposit-001' \
  -d '{"amount":100.5}' localhost:8080/api/v1/accounts/ACC000000000001/deposits
```

## Run locally

Needs JDK 26, Maven (or `./mvnw`), Docker.

```bash
# Postgres and Kafka only, app on the host
docker compose up -d postgres kafka
# if 5432 is already taken:
# POSTGRES_PORT=5433 docker compose up -d postgres kafka

./mvnw spring-boot:run -Dspring-boot.run.profiles=local

# App, Postgres, and Kafka together (API password local-dev-secret)
docker compose up -d --build
```

The `local` profile sets the API password to `local-dev-secret`. User is `outpost`. Without that profile you must set `OUTPOST_API_PASSWORD`; there is no default.

Health: `GET /actuator/health` (no auth).

Tests start their own Postgres and Kafka via Testcontainers. Docker must be running.

```bash
./mvnw test
```

The suite hits the real HTTP API. It checks concurrent deposits and transfers, overdraft rejection, idempotent replay, HMAC signatures, retry on 500/503, immediate fail on 410, and DLQ publish after the last attempt.

## Benchmark

`bench/PostingBench.java` is not part of `./mvnw test`. With the app already up:

```bash
java bench/PostingBench.java
```

It opens 8 USD accounts, warms each with one deposit, then posts 400 idempotent deposits of `1.0000` from 8 threads. Each deposit uses its own key. Zero webhook subscriptions, so this is the write path only.

One run on an Apple M3 (8 cores, 8 GB, arm64), app on the host (`local` profile, Java 27) against compose Postgres 18 and Kafka 4.3.1: **61.4/s**, **p50 120 ms**, **p99 294 ms**, 400 of 400 succeeded.

## Configuration

Defaults are in `src/main/resources/application.properties`.

| Variable / property | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/outpost` | |
| `DATABASE_USER` / `DATABASE_PASSWORD` | `outpost` / `outpost` | |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `OUTPOST_API_USER` | `outpost` | |
| `OUTPOST_API_PASSWORD` | *(required)* | Set by `local` profile in development |
| `outpost.webhook.backoff` | `10s,30s,2m,10m,1h` | Delay before retry N is `backoff[N-1]` |
| `outpost.webhook.jitter` | `0.2` | ±20% |
| `outpost.webhook.poll-interval` | `PT5S` | ISO-8601, read by `@Scheduled` |
| `outpost.idempotency.ttl` | `24h` | |

Virtual threads: `spring.threads.virtual.enabled=true`. That covers Tomcat, scheduled jobs, and Kafka listeners.

## Layout

```
src/main/java/com/ledger
├── config/        security, Kafka topics
├── common/        exceptions → Problem Details
├── ledger/        accounts, journal, posting service, HTTP
├── idempotency/   Idempotency-Key filter
└── webhook/       subscriptions, outbox, relay, dispatcher, HMAC
```

`src/test/java/com/ledger` holds the integration tests. They share one Spring context and one WireMock server.

## Invariants the tests enforce

1. Concurrent deposits leave `balance == sum(credits) − sum(debits)` on the customer account.
2. Twenty-five concurrent $10 withdrawals from a $100 account: ten succeed, fifteen return 422, final balance is 0.
3. One hundred bidirectional transfers neither deadlock nor change the pair’s combined balance.
4. Cross-currency transfer is rejected; balances do not move.
5. A reused idempotency key does not post a second journal pair.
6. A delivered webhook is signed with the subscription secret and is not POSTed twice for the same attempt.
7. 503 then 500 then 200: three attempts, then `DELIVERED`.
8. Four 500s: `FAILED`, four HTTP calls, one DLQ message.
9. 410: one HTTP call, then `FAILED`.
