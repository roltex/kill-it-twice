# SPEC.md — Kill It Twice v2

> Status: **v2 — post-verify**. Evolved from v1 after measured runs. Changes from v1 are listed in §11.

## 1. Goal

Build a data replication pipeline:

```
Postgres (source) → Pipeline → Elasticsearch (search index)
                            → RabbitMQ (change stream) → independent Consumer
```

The pipeline runs **backfill** and **incremental sync concurrently**. A functional Angular UI observes and controls the system. `make verify` proves five failure gates by actually breaking the system.

## 2. Delivery guarantee (LOCKED)

**Effectively-once**: the pipeline delivers **at-least-once**; sinks are **idempotent**.

- Elasticsearch document `_id` = record `id`. Index API uses `version_type=external_gte` so older versions cannot win; version conflicts count as success.
- RabbitMQ message id = `recordId:version`. Consumer inserts into `consumed_events(event_id PRIMARY KEY)` before ack; redelivery increments `duplicate_hits` and is otherwise a no-op.

True exactly-once across both sinks is **not** claimed.

## 3. Dataset (LOCKED)

- **1,000,000** source records.
- Seeded in **50,000-row chunks** (single 1M `generate_series` insert destabilized Docker Desktop on the build host).
- Batch size **500**.
- Max **2** batches in flight per mode.
- Rationale: large enough that “load everything into memory” fails; small enough that `make verify` finishes on Docker Desktop (~7 minutes including G3’s 60s outage).

## 4. Architecture (LOCKED)

| Component | Role |
|-----------|------|
| Postgres | Source `records`; durable `checkpoints`, `dlq`, `consumed_events` |
| Redis | Leader lock, throughput counters, circuit-breaker state |
| Elasticsearch | Search sink, single-node, security off, 512MB heap |
| RabbitMQ | Change stream |
| `apps/pipeline` | NestJS workers, sinks, DLQ, HTTP API, Prometheus metrics |
| `apps/consumer` | Independent consumer + dedupe |
| `apps/ui` | Angular: status, records, control, simulate |

### Cursors

- **Backfill**: `WHERE id > :cursor ORDER BY id LIMIT 500`
- **Incremental**: `WHERE (updated_at, id) > (:ts, :id) ORDER BY updated_at, id LIMIT 500`
- **Incremental watermark at first boot**: if checkpoint is still the epoch sentinel `(0, 1970-01-01)`, initialize to `NOW()` so backfill owns history and incremental only sees post-boot changes. Overlap during live updates remains safe via version checks.

### Checkpoint rule

The cursor advances only after every record in the batch is either accepted by Elasticsearch **or** stored in `dlq`, **and** Rabbit confirms succeed.

`docker kill -s SIGKILL` mid-batch loses only the open (uncommitted) batch. Restart resumes from the last committed Postgres checkpoint. Pipeline/consumer use `restart: "no"` so verify’s kill is not masked by Docker auto-restart.

### Sink outage (G3)

When Elasticsearch is unreachable **or** returns an incomplete bulk response: open circuit, sleep with exponential backoff + jitter (**start 1s, cap 15s** — measured: ~8–9 backoff sleeps during a 60s outage). Do not busy-loop. Do not advance checkpoint. Resume when ES answers again.

### Partial batch failure (G4)

Elasticsearch `_bulk` is handled **per-item**. Successes stay. Failures go to DLQ with payload, error, batch id, record id, version. Cursor still advances. No batch-wide rollback.

Poison records: source rows with `poison = true`, rejected by ES ingest pipeline `reject-poison` (`fail` processor). Pipeline does **not** pre-filter. Main seed has no poison rows. Verify inserts a 500-row side batch with 3 poison rows for G4 **after** G2.

### Incomplete bulk = transport failure

If `_bulk` returns fewer item results than submitted docs (observed while ES was shutting down), treat as transport error: open circuit, do **not** advance checkpoint, do **not** publish to Rabbit. Empty/partial success was the v1→v2 bug that lost ~24k docs on the first verify attempt.

## 5. Gates (LOCKED)

| Gate | Success (measured) |
|------|---------------------|
| G1 Crash recovery | Kill mid-backfill; resume from Postgres checkpoint; 0 lost |
| G2 No duplicates | 1,000,000 source / 1,000,000 ES / 0 dupes; effectively-once |
| G3 Sink outage | 60s ES down after ping fails; cursor_delta=0; backoff sleeps ≥2; auto-recover; 0 lost |
| G4 Partial batch | 497 written, 3 in DLQ with replay context |
| G5 Observability | `/api/status`, `/metrics`, UI shell answer position/throughput/lag/DLQ/health |

## 6. HTTP control plane (LOCKED)

Unchanged from v1: status, metrics, records, changes, control, DLQ replay, simulate, verify poison-batch, admin refresh.

## 7. Repo layout (LOCKED)

Unchanged from v1.

## 8. Resolved open items

1. Backoff: start 1000ms, cap 15000ms — confirmed under G3 (~8–9 sleeps / 60s).
2. Seed: 1M in 50k chunks; full verify ~7 minutes on Docker Desktop.
3. Poison: ES ingest `fail` processor on `ctx.poison == true`.
4. Hot state in Redis; durable checkpoints/DLQ in Postgres.
5. Leader lock TTL 5s, renew 2s.

## 9. Explicit non-goals

NiFi, ClickHouse, S3, multi-node ES, auth, polished product UI, true cross-sink exactly-once, Docker socket mounted into the app.

## 10. Spec evolution

- **v1** (commit before code): contract only.
- **v2** (this file): after verify forced three design corrections — see §11.

## 11. Changes from v1

1. **Incremental watermark**: v1 implied epoch start; that double-scanned 1M rows through Rabbit and starved progress. v2 initializes watermark to boot time when still at the epoch sentinel.
2. **Incomplete bulk handling**: v1 said “per-item errors”; it did not say empty bulk responses must not advance the cursor. First verify lost 24k ES docs during G3 shutdown. v2 requires `results.length == batch.length` or treat as transport failure.
3. **Seed chunking + restart policy**: v1 assumed a single insert and `unless-stopped`. Docker Desktop died on a 1M insert; auto-restart masked G1. v2 seeds in chunks and sets `restart: "no"` for pipeline/consumer.
