# SPEC.md — Kill It Twice v1

> Status: **v1 — pre-implementation**. This document is the contract for building the system. Open items are marked. Decisions already locked are marked LOCKED.

## 1. Goal

Build a data replication pipeline:

```
Postgres (source) → Pipeline → Elasticsearch (search index)
                            → RabbitMQ (change stream) → independent Consumer
```

The pipeline runs **backfill** and **incremental sync concurrently**. A functional Angular UI observes and controls the system. `make verify` proves five failure gates by actually breaking the system.

This is not a “happy path” demo. Graders will kill containers, stop sinks, and inject poison records.

## 2. Delivery guarantee (LOCKED)

**Effectively-once**: the pipeline delivers **at-least-once**; sinks are **idempotent**.

- Elasticsearch document `_id` = record `id`. Writes apply only when `incoming.version >= stored.version`.
- RabbitMQ message id = `recordId:version`. Consumer inserts into `consumed_events(event_id PRIMARY KEY)` before ack; redelivery is a no-op.

True exactly-once across both sinks is **not** claimed.

## 3. Dataset (LOCKED)

- **1,000,000** source records.
- Batch size **500**.
- Max **2** batches in flight.
- Rationale: large enough that “load everything into memory” fails; small enough that `make verify` finishes on Docker Desktop. Capacity numbers in README come from measured runs, not estimates.

## 4. Architecture (LOCKED)

| Component | Role |
|-----------|------|
| Postgres | Source table `records`; also stores `checkpoints`, `dlq`, `consumed_events` |
| Redis | Leader lock, throughput counters, circuit-breaker state |
| Elasticsearch | Search sink (current state), single-node, security off |
| RabbitMQ | Change stream |
| `apps/pipeline` | NestJS: workers, sinks, DLQ, HTTP control plane, Prometheus metrics |
| `apps/consumer` | NestJS: Rabbit consumer + dedupe |
| `apps/ui` | Angular: status, records, control, simulate |

### Cursors

- **Backfill**: `WHERE id > :cursor ORDER BY id LIMIT 500`
- **Incremental**: `WHERE (updated_at, id) > (:ts, :id) ORDER BY updated_at, id LIMIT 500`

A row changed after backfill, or inserted behind the backfill cursor, is picked up by incremental. Overlap is safe via version checks.

### Checkpoint rule

The cursor advances only after every record in the batch is either accepted by the sink **or** stored in `dlq` with full payload, error, batch id, record id, and version.

`docker kill -s KILL` mid-batch loses only the open (uncommitted) batch. Restart resumes from the last committed checkpoint.

### Sink outage (G3)

When Elasticsearch is unreachable: open a circuit, sleep with exponential backoff + jitter (start ~1s, cap ~15s — **exact numbers OPEN until measured**). Do not busy-loop. Bound unacked batches. Resume when ES returns.

### Partial batch failure (G4)

Elasticsearch `_bulk` is handled **per-item**. Successes stay. Failures go to DLQ. Cursor still advances. No batch-wide rollback.

Poison records: source rows with `poison = true`, rejected by an ES ingest pipeline. Pipeline does **not** pre-filter. Main seed has no poison rows. Verify inserts a 500-row side batch with 3 poison rows for G4 **after** G2, so duplicate counts stay clean.

## 5. Gates (LOCKED)

| Gate | Success |
|------|---------|
| G1 Crash recovery | Kill pipeline mid-backfill; resume from checkpoint; 0 lost |
| G2 No duplicates | After chaos: source count matches sinks; 0 dupes (or harmless idempotent replay) |
| G3 Sink outage | Stop ES 60s; no loss; no busy-loop; auto-recover |
| G4 Partial batch | 497 written, 3 in DLQ with replay context |
| G5 Observability | Status API + metrics + UI answer: position, throughput, lag, DLQ depth, health |

`make verify` / `./verify.sh` prints PASS/FAIL lines. Failed gates stay FAIL and are explained in README.

## 6. HTTP control plane (LOCKED)

Pipeline exposes:

- `GET /api/status` — cursors, throughput, lag, DLQ depth, circuit, health
- `GET /metrics` — Prometheus
- `GET /api/records`, `GET /api/records/:id`, `GET /api/changes`
- `POST /api/control/backfill/{start|stop}`, `POST /api/control/incremental/{start|stop}`, `POST /api/control/settings`
- `GET /api/dlq`, `POST /api/dlq/:id/replay`
- `POST /api/simulate/search-sink`, `/poison`, `/changes`

UI routes: `/`, `/records`, `/control`, `/simulate`. Poll ~2s.

G1/G3 in verify use real `docker kill` / `docker stop`. Simulate endpoints exist for UI demos without mounting Docker socket.

## 7. Repo layout (LOCKED)

```
docker-compose.yml
Makefile
verify.sh
SPEC.md
AGENTS.md
README.md          (written after measured verify)
apps/pipeline/
apps/consumer/
apps/ui/
infra/postgres/init.sql
```

## 8. Open items (to resolve during implementation)

1. Exact backoff schedule after first G3 run.
2. Seed duration and whether 1M is too slow on the host machine (may reduce only with README justification).
3. Poison mechanism details (ingest pipeline processor choice).
4. Whether Redis circuit state is primary or Postgres is authoritative for UI — prefer Redis for hot state, Postgres for durable DLQ/checkpoints.
5. Leader lock TTL (~5s renew ~2s) — confirm under Docker Desktop.

## 9. Explicit non-goals

NiFi, ClickHouse, S3, multi-node ES, auth, polished product UI, true cross-sink exactly-once.

## 10. Spec evolution

This file must appear in git **before** application code. Later commits revise it when a real run forces a change. Do not invent divergence stories for README.
