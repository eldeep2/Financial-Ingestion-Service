# Financial Client Data Ingestion Service

> A production-grade Spring Boot microservice for ingesting financial client data from remote ZIP archives. Built with enterprise architecture principles, resilience patterns, and operational observability.

[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%2B-blue)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Setup & Installation](#setup--installation)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Scalability & Performance](#scalability--performance)
- [Monitoring & Observability](#monitoring--observability)
- [Architectural Decision Records](#architectural-decision-records)
- [Known Limitations & Roadmap](#known-limitations--roadmap)
- [Production Checklist](#production-checklist)

---

## Overview

The Ingestion Service exposes a single webhook endpoint that accepts a URL pointing to a remote ZIP archive. The service downloads, streams, parses, and persists nested financial data (clients → accounts → holdings) into PostgreSQL with full idempotency guarantees.

**Tech Stack**

| Layer | Technology |
|-------|-----------|
| Runtime | Java 21 |
| Framework | Spring Boot 3.3 |
| Database | PostgreSQL 13+ (H2 for local dev) |
| ORM | Spring Data JPA / Hibernate |
| Connection Pool | HikariCP |
| API Docs | Swagger / OpenAPI 3 |
| Build | Maven 3.8+ |

---

## Architecture

### System Flow

```
┌─────────────────────────────────────────────────────────────┐
│                      External System                        │
│               (Uploads client data ZIP via URL)             │
└───────────────────────────┬─────────────────────────────────┘
                            │  POST /api/v1/webhooks/ingest
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   WebhookController                         │
│   • Validates request payload                               │
│   • Generates UUID correlation ID for tracing               │
│   • Orchestrates download and persistence pipeline          │
└──────────┬──────────────────────────────┬───────────────────┘
           │                              │
           ▼                              ▼
┌─────────────────────┐      ┌────────────────────────────────┐
│ ZipProcessingService│      │    DataPersistenceService      │
│                     │      │                                │
│  • Download ZIP     │      │  • UPSERT clients              │
│  • Stream-extract   │      │  • UPSERT accounts             │
│  • Parse JSON       │      │  • Batch-insert holdings       │
│  • Error isolation  │      │  • Transaction management      │
└─────────┬───────────┘      └──────────────┬─────────────────┘
          └──────────────┬──────────────────┘
                         ▼
          ┌──────────────────────────────┐
          │      JPA Repository Layer    │
          │  ClientRepository            │
          │  AccountRepository (UPSERT)  │
          │  HoldingRepository (Batch)   │
          └──────────────┬───────────────┘
                         ▼
          ┌──────────────────────────────┐
          │       PostgreSQL             │
          │  clients · accounts ·        │
          │  holdings (indexed)          │
          └──────────────────────────────┘
```

### Package Structure

```
com.Financialingestion
├── controller/              # REST endpoints
│   └── WebhookController
├── service/                 # Business logic
│   ├── ZipProcessingService
│   └── DataPersistenceService
├── repository/              # Data access layer
│   ├── ClientRepository
│   ├── AccountRepository
│   └── HoldingRepository
├── entity/                  # JPA entities
│   ├── Client
│   ├── Account
│   └── Holding
├── dto/                     # Request/Response objects
│   ├── WebhookRequest
│   ├── WebhookResponse
│   ├── ClientDto
│   ├── AccountDto
│   └── HoldingDto
├── mapper/                  # Entity ↔ DTO mappers
│   ├── ClientMapper
│   ├── AccountMapper
│   └── HoldingMapper
├── exception/               # Custom exceptions & global handler
│   ├── DownloadException
│   ├── ZipProcessingException
│   ├── JsonParsingException
│   ├── ErrorResponse
│   └── GlobalExceptionHandler
└── config/                  # Spring configuration
    ├── OpenApiConfig
    ├── RestClientConfig
    ├── AsyncConfig
    ├── JacksonConfig
    └── WebMvcConfig
```

---

## Data Model

```
┌──────────────────┐
│     clients      │  1 ──────────────────────── N
│                  │
│  client_id (PK)  │          ┌──────────────────────┐
│  first_name      │          │       accounts        │  1 ─────── N
│  last_name       │          │                      │
│  email           │          │  account_id (UNIQUE) │     ┌─────────────────┐
│  advisor_id      │          │  account_type        │     │    holdings     │
│  last_updated    │          │  custodian           │     │                 │
│  created_at      │          │  opened_date         │     │  id (UUID, PK)  │
│  updated_at      │          │  status              │     │  ticker         │
└──────────────────┘          │  cash_balance        │     │  cusip          │
                              │  total_value         │     │  quantity       │
                              │  created_at          │     │  market_value   │
                              │  updated_at          │     │  cost_basis     │
                              └──────────────────────┘     │  price          │
                                                           │  asset_class    │
                                                           │  created_at     │
                                                           │  updated_at     │
                                                           └─────────────────┘
```

**Key constraints:**
- `account_id` carries a `UNIQUE NOT NULL` constraint and is the ON CONFLICT target for upserts. The surrogate PK is a `BIGSERIAL` for stable row identity and clean integer FK joins (see [ADR-008](#adr-008-account_id-as-globally-unique-business-key)).
- Holdings are keyed by a generated UUID; they are replaced entirely per account on each ingestion (see [ADR-004](#adr-004-holdings-delete--insert-over-upsert)).

---

## Setup & Installation

### Prerequisites

- Java 21+
- Maven 3.8+
- PostgreSQL 13+ (production) or H2 (local dev — zero config)

### Database Setup (PostgreSQL)

```bash
# Create database and user
createdb Financial_ingestion
createuser ingestion_app
psql -c "ALTER USER ingestion_app WITH PASSWORD 'secure_password';"
psql -c "GRANT ALL PRIVILEGES ON DATABASE Financial_ingestion TO ingestion_app;"

# Apply schema
psql -U postgres -d Financial_ingestion -f src/main/resources/db/schema.sql
```

### Build

```bash
mvn clean install
```

### Run Locally (H2 — no database setup needed)

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=local"
```

| Interface | URL |
|-----------|-----|
| API Endpoint | http://localhost:8081/api/v1/webhooks/ingest |
| Swagger UI | http://localhost:8081/swagger-ui.html |
| H2 Console | http://localhost:8081/h2-console |

H2 connection details: JDBC URL `jdbc:h2:mem:testdb`, User `sa`, Password *(empty)*

### Run with PostgreSQL

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=Financial_ingestion
export DB_USER=postgres
export DB_PASSWORD=postgres

mvn spring-boot:run
```

### Docker

```bash
# Build image
mvn spring-boot:build-image -Dspring-boot.build-image.imageName=Financial-ingestion:latest

# Run container
docker run -d \
  --name Financial-ingestion \
  -p 8081:8081 \
  -e DB_HOST=postgres \
  -e DB_PORT=5432 \
  -e DB_NAME=Financial_ingestion \
  -e DB_USER=postgres \
  -e DB_PASSWORD=postgres \
  Financial-ingestion:latest
```

---

## API Reference

### `POST /api/v1/webhooks/ingest`

Accepts a JSON payload containing a URL to a remote ZIP file. Downloads, parses, and persists all client/account/holding records atomically per client.

**Request**

```bash
curl -X POST http://localhost:8081/api/v1/webhooks/ingest \
  -H "Content-Type: application/json" \
  -d '{"fileUrl": "https://example.com/data/clients-export-2025-03-02.zip"}'
```

**Success — 202 Accepted**

```json
{
  "status": "ACCEPTED",
  "clientsProcessed": 150,
  "accountsProcessed": 325,
  "holdingsProcessed": 1200,
  "filesProcessed": 150,
  "processedAt": "2025-03-02T14:30:00Z",
  "message": "Successfully ingested 150 clients with 325 accounts and 1200 holdings",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Error — 400 Bad Request**

```json
{
  "status": 400,
  "error": "BAD_REQUEST",
  "message": "Invalid webhook payload: file_url is required",
  "timestamp": "2025-03-02T14:30:00Z",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Error Reference

| HTTP | Code | Cause | Resolution |
|------|------|-------|-----------|
| 400 | `BAD_REQUEST` | Missing or malformed `fileUrl` | Verify request JSON format |
| 422 | `ZIP_PROCESSING_ERROR` | Corrupt, empty, or password-protected ZIP | Validate ZIP integrity |
| 422 | `JSON_PARSING_ERROR` | JSON in ZIP does not match expected schema | Validate JSON structure |
| 502 | `DOWNLOAD_FAILED` | Network error or unreachable URL | Check URL accessibility |
| 500 | `INTERNAL_ERROR` | Database or unexpected runtime error | Inspect logs and DB connectivity |

### Expected ZIP Payload Structure

```
clients-export-2025-03-02.zip
├── client-CLT-29481.json
├── client-CLT-29482.json
└── client-CLT-29483.json
```

Each file is a self-contained client record with nested accounts and holdings:

```json
{
  "client_id": "CLT-29481",
  "first_name": "Jane",
  "last_name": "Smith",
  "email": "jane.smith@example.com",
  "advisor_id": "ADV-0052",
  "last_updated": "2025-03-02T14:30:00Z",
  "accounts": [
    {
      "account_id": "ACC-10042",
      "account_type": "INDIVIDUAL",
      "custodian": "Apex Clearing",
      "opened_date": "2023-03-15",
      "status": "ACTIVE",
      "cash_balance": 2450.75,
      "total_value": 61100.75,
      "holdings": [
        {
          "ticker": "VTI",
          "cusip": "922908769",
          "description": "Vanguard Total Stock Market ETF",
          "quantity": 150.0,
          "market_value": 38250.00,
          "cost_basis": 33750.00,
          "price": 255.00,
          "asset_class": "US_EQUITY"
        }
      ]
    }
  ]
}
```

---

## Testing

### Automated Tests

Integration tests run against H2 (no external database needed):

```bash
mvn test
```

### Manual End-to-End Test

```bash
# 1. Create a test JSON file
cat > test-client.json << 'EOF'
{
  "client_id": "CLT-TEST-001",
  "first_name": "Test",
  "last_name": "Client",
  "email": "test@example.com",
  "advisor_id": "ADV-001",
  "last_updated": "2025-03-02T14:30:00Z",
  "accounts": [
    {
      "account_id": "ACC-TEST-001",
      "account_type": "INDIVIDUAL",
      "custodian": "Test Custodian",
      "opened_date": "2023-01-01",
      "status": "ACTIVE",
      "cash_balance": 1000.00,
      "total_value": 10000.00,
      "holdings": []
    }
  ]
}
EOF

# 2. Package and serve
zip test-data.zip test-client.json
python3 -m http.server 8888 &

# 3. Trigger ingestion
curl -X POST http://localhost:8081/api/v1/webhooks/ingest \
  -H "Content-Type: application/json" \
  -d '{"fileUrl": "http://localhost:8888/test-data.zip"}'
```

---

## Scalability & Performance

### Database Throughput

- **Batch inserts**: Holdings are inserted in configurable batches (default: 50) to minimize round-trips
- **UPSERT atomicity**: `ON CONFLICT ... DO UPDATE` eliminates separate SELECT + INSERT/UPDATE cycles
- **JDBC batching**: Enabled globally to reduce per-statement overhead
- **Indexes**: `client_id`, `account_id`, and `ticker` columns are indexed for UPSERT and future read queries

### Connection Management

- **HikariCP**: 10 connections (prod), 5 (local)
- **Timeouts**: 10s connect, 60s read — prevents connection exhaustion on slow partners
- **Queue capacity**: 100 concurrent requests before rejection

### Resilience

- **Per-client transaction isolation** (`REQUIRES_NEW`): one malformed record does not roll back the batch
- **Idempotent operations**: safe to retry — clients and accounts are upserted, holdings are refreshed
- **Stateless controller layer**: horizontally scalable behind a load balancer without session state

### Tuning for High Volume (10,000+ records)

```yaml
# application.yml overrides
hibernate:
  jdbc:
    batch_size: 100

spring:
  datasource:
    hikari:
      maximum-pool-size: 50

ingestionExecutor:
  corePoolSize: 20
  maxPoolSize: 100
  queueCapacity: 500
```

---

## Monitoring & Observability

### Logging

- Structured log format with ISO8601 timestamps and thread context
- `correlationId` injected into MDC for end-to-end request tracing across all service layers
- Correlation ID also returned in the `X-Correlation-ID` response header for client-side tracing
- Log rotation: daily, max 1GB — configured in `logback-spring.xml`

### Actuator Endpoints

```bash
# Health check
curl http://localhost:8081/actuator/health

# Runtime metrics (DB pool, JVM, HTTP)
curl http://localhost:8081/actuator/metrics
```

---

## Architectural Decision Records

This section documents the significant design decisions made during development, including the tradeoffs considered and rationale for each choice.

---

### ADR-001: PostgreSQL over SQLite/H2 for Production

**Decision**: PostgreSQL is the production database; H2 is limited to local development.

**Rationale**:
- Concurrent async writes make SQLite unsafe — it uses file-level locking
- Native `ON CONFLICT` UPSERT is idiomatic and performant in PostgreSQL
- Financial data demands full ACID guarantees; PostgreSQL is battle-tested in banking and capital markets
- HikariCP connection pooling integrates optimally with PostgreSQL

**Tradeoff**: Higher operational setup complexity compared to an embedded database, justified by the correctness and performance requirements of financial data ingestion.

---

### ADR-002: Synchronous Response over True Async Processing

**Decision**: Requests are processed synchronously; the API returns a `202 Accepted` response with full statistics inline.

**Rationale**:
- The specification requires `clientsProcessed`, `accountsProcessed`, and `holdingsProcessed` in the response body
- True async processing would return a `jobId` and require a separate polling API — out of scope for MVP
- Wrapping synchronous work in `CompletableFuture.join()` adds complexity with zero benefit
- An `AsyncConfig` executor bean is already wired and ready for Phase 2

**Tradeoff**: Longer HTTP response times for large ingestions, in exchange for a simpler, spec-compliant implementation.

---

### ADR-003: `REQUIRES_NEW` Transaction Isolation per Client

**Decision**: Each client record is processed within its own independent transaction using `TransactionTemplate` with `REQUIRES_NEW` propagation.

**Rationale**:
- One malformed record must not roll back hundreds of valid records already persisted
- Financial ingestion pipelines must be resilient — partial success is an acceptable and explicitly reported outcome
- `REQUIRES_NEW` is the standard pattern in batch processing pipelines for this exact reason
- Programmatic `TransactionTemplate` gives finer-grained control than annotation-based `@Transactional`

**Tradeoff**: Higher instantaneous DB connection usage during large batches, in exchange for fault isolation per record.

---

### ADR-004: Holdings DELETE + INSERT over UPSERT

**Decision**: On each ingestion, all holdings for an account are deleted and a fresh batch is inserted.

**Rationale**:
- Holdings do not have a natural, stable composite business key suitable for UPSERT (ticker alone is not unique per account if a position is split across multiple lots)
- DELETE + INSERT guarantees a clean, consistent state with no stale records from previous ingestions
- Simpler than implementing per-holding merge logic
- Batch size of 50 keeps database round-trips minimal
- The delete and re-insert happen within the same transaction, so the brief intermediate state is never visible externally

**Tradeoff**: Holdings history is not preserved across ingestions. If historical holding snapshots become a requirement, this strategy should be revisited.

---

### ADR-005: `ZipInputStream` Streaming over `ZipFile`

**Decision**: ZIP entries are extracted via `ZipInputStream` (streaming) rather than `ZipFile` (full in-memory load).

**Rationale**:
- `ZipFile` loads the entire archive into memory — unsafe for files exceeding 100MB
- `ZipInputStream` processes one entry at a time with a fixed 8KB buffer, keeping memory usage constant regardless of archive size
- At ~1,000 clients and ~2KB per JSON file, total payload is modest (~2MB), but streaming is future-proof against partners sending larger archives without notice

**Tradeoff**: Single-pass only — ZIP entries cannot be re-read. If multi-pass processing is required in future, a temporary file strategy would be needed.

---

### ADR-006: `byte[]` Download over `InputStream` RestTemplate

**Decision**: ZIPs are downloaded as `byte[]` via `getForEntity(url, byte[].class)` rather than as an `InputStream`.

**Rationale**:
- `restTemplate.getForObject(url, InputStream.class)` has no viable message converter in Spring
- The connection closes before the stream is fully read, causing silent data corruption
- The `byte[]` approach ensures the full download completes before wrapping in `ByteArrayInputStream` for streaming extraction
- A pre-processing size cap prevents memory exhaustion on oversized files

**Tradeoff**: The full ZIP is held in memory momentarily during download. This is acceptable for files under ~500MB and is mitigated by the size cap check.

---

### ADR-007: `@Modifying(clearAutomatically = true)` on UPSERT Queries

**Decision**: All native UPSERT queries are annotated with `@Modifying(flushAutomatically = true, clearAutomatically = true)`.

**Rationale**:
- After a native SQL UPSERT, Hibernate's first-level cache (EntityManager) still holds the pre-UPSERT entity state
- A subsequent `findByClientId()` or `findByAccountId()` call would return `Optional.empty()` from stale cache, causing a `NullPointerException`
- `clearAutomatically = true` forces L1 cache eviction after the native query executes
- `flushAutomatically = true` ensures pending dirty entities are written to the database before the native query runs
- Without this, the failure mode is an intermittent NPE in production that is difficult to reproduce and debug

**Tradeoff**: Slight performance overhead from cache clearing, which is a necessary cost for data correctness.

---

### ADR-008: `account_id` as Globally Unique Business Key

**Decision**: `account_id` is a `UNIQUE NOT NULL` column on the accounts table. The PK is a surrogate `BIGSERIAL`. UPSERT targets `ON CONFLICT (account_id)`.

**Rationale**:
- Real custodians (Apex Clearing, Fidelity, Schwab) assign globally unique account numbers — the `ACC-XXXXX` format implies a sequential global ID, not a per-client local ID
- `ON CONFLICT (account_id)` on a UNIQUE constraint is functionally equivalent to `ON CONFLICT` on a PK in PostgreSQL
- A surrogate `BIGSERIAL` PK keeps row identity stable if a partner ever reissues an `account_id`, and yields cleaner integer FK joins compared to VARCHAR FK joins

**Assumption**: One account belongs to exactly one client. If this assumption is invalidated, the UNIQUE constraint on `account_id` alone should be replaced with a composite `UNIQUE (client_id, account_id)`.

---

### ADR-009: No Input Validation on Financial Values

**Decision**: Negative quantities, null market values, and unrecognized asset classes are accepted without rejection.

**Rationale**:
- The specification does not define validation rules for financial field values
- Negative quantities are legitimate in finance (short positions, corrections)
- Rejecting records based on undocumented assumptions would silently discard valid data
- Better to ingest all data and allow downstream systems to apply business-specific validation rules

**Known Risk**: Invalid data may reach the database. Mitigation: add field-level validation in Phase 2 after clarifying business rules with the data partner.

---

### ADR-010: UUID Correlation ID per Request

**Decision**: A UUID `correlationId` is generated at the start of each webhook request, injected into SLF4J MDC, and returned in both the response body and `X-Correlation-ID` response header.

**Rationale**:
- Financial services operations require end-to-end request traceability
- MDC propagates the ID across `ZipProcessingService` and `DataPersistenceService` log entries for the same request
- Returning the ID in both the body and header allows both the calling system and client-side tooling to correlate logs
- MDC is thread-local, making it safe for concurrent requests without cross-contamination

---

## Known Limitations & Roadmap

### Current Limitations

| Limitation | Notes |
|-----------|-------|
| Single database only | No distributed transactions |
| Synchronous processing | No message queue (Kafka planned in Phase 2) |
| No authentication | OAuth2/JWT out of scope for MVP |
| No pagination | Full result sets held in memory (acceptable for ≤10k records) |
| Password-protected ZIPs | Not supported |
| H2 profile | Local development only; not production-safe |

### Phase 2

- [ ] Async batch processing with `CompletableFuture`
- [ ] Kafka-based event streaming for ingestion jobs
- [ ] PostgreSQL read replicas for failover
- [ ] Input validation with Hibernate Validator (pending business rule clarification)
- [ ] Spring Security with OAuth2 / JWT

### Phase 3

- [ ] Distributed tracing (Jaeger / Zipkin)
- [ ] Dead-letter queue for permanently failed records
- [ ] Client data encryption at rest
- [ ] Audit trail with temporal tables
- [ ] GraphQL API

---

## Production Checklist

- [ ] PostgreSQL database created with appropriate credentials
- [ ] Schema applied (`schema.sql`)
- [ ] Environment variables configured (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
- [ ] SSL/TLS enabled for HTTPS termination
- [ ] Spring Security integrated (OAuth2)
- [ ] Monitoring and alerting configured (Prometheus + Grafana recommended)
- [ ] Backup and recovery procedures documented and tested
- [ ] Load testing completed at target throughput
- [ ] Security audit completed
- [ ] Log aggregation pipeline connected (ELK / Datadog / CloudWatch)

---

## Troubleshooting

**`Connection refused` on startup**
```
Cause:    PostgreSQL not running
Solution: pg_ctl -D /usr/local/var/postgres start
```

**`UPSERT fails with unique constraint violation`**
```
Cause:    Duplicate client_id or account_id in source data
Solution: Identify and deduplicate records with the same business key
```

**`Not a valid zip file` during extraction**
```
Cause:    Remote URL does not return a valid ZIP archive
Solution: Verify the URL manually: curl -I <file_url>; validate with: unzip -t file.zip
```

**Out-of-memory during large ZIP processing**
```
Cause:    ZipInputStream not being used; archive loaded into memory entirely
Solution: Confirm ZipInputStream usage in ZipProcessingService; verify buffer size = 8192
```

---

## Author

**Ahmed M. Eldeep**
Principal Full Stack Engineer & Cloud Architect
[linkedin.com/in/eldeep](https://linkedin.com/in/eldeep)

---

© 2026 Ahmed M. Eldeep. All rights reserved.
*Source code is proprietary and not available for redistribution.*
