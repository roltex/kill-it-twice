# AGENTS.md — Working in this repository

Instructions for any AI agent (or human) working on Kill It Twice.

## What this project is

A Postgres → Elasticsearch + RabbitMQ replication system with concurrent backfill and incremental sync. Graders care about failure behavior (five gates) and honest spec history more than UI polish.

Read `SPEC.md` first. Treat it as the contract. If you change a LOCKED decision, update SPEC.md in the same commit and explain why.

## Layout

| Path | Purpose |
|------|---------|
| `apps/pipeline` | NestJS pipeline: workers, sinks, DLQ, HTTP API, metrics |
| `apps/consumer` | NestJS RabbitMQ consumer + `consumed_events` dedupe |
| `apps/ui` | Angular control UI (status, records, control, simulate) |
| `infra/postgres/init.sql` | Schema for source, checkpoints, dlq, consumed_events |
| `docker-compose.yml` | Full local stack |
| `Makefile` / `verify.sh` | seed + gate verification |
| `SPEC.md` | Spec (must evolve in git history) |
| `README.md` | Run instructions, ADRs, capacity, AI divergence |

## Conventions

- TypeScript / NestJS for services; Angular for UI.
- Delivery guarantee: **effectively-once** (at-least-once + idempotent sinks). Do not claim exactly-once.
- Checkpoint in Postgres only after every record in a batch is accepted or DLQ’d.
- Batch size 500; max 2 in flight unless SPEC is updated with measured justification.
- Poison rows use `poison = true` + ES ingest rejection. Do not silently drop them in the pipeline.
- Kill recovery must survive `SIGKILL` (no reliance on graceful shutdown hooks for durability).

## What not to touch without reason

- Do not rewrite SPEC retroactively to match code without recording the real change.
- Do not weaken verify gates to force PASS.
- Do not mount the Docker socket into the app for “simulate”; verify uses host Docker; UI simulate uses app-level toggles.
- Do not add NiFi / ClickHouse / S3 / auth unless SPEC changes.

## How to verify your work

```bash
docker compose up -d --build
make seed          # or ./scripts via Makefile
make verify        # must print G1–G5 PASS/FAIL
```

A gate that fails stays FAIL. Fix the system or document the failure honestly in README.

After UI changes: exercise `/`, `/records`, `/control`, `/simulate` in the browser.

## Commit discipline

1. SPEC / AGENTS before application code (already done for v1).
2. Infra → pipeline/consumer → UI → verify → measured README.
3. When a run forces a design change: update SPEC.md, then code, then README divergence section with the real case.

## Interview note

Reviewers will ask about one SPEC↔code divergence and whether it was a decision or an accident. Prefer explicit, documented decisions.
