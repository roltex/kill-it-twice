#!/usr/bin/env bash
# Seed 1,000,000 records into Postgres (idempotent if already seeded).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${SEED_COUNT:-1000000}"

echo "==> Waiting for Postgres..."
for i in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U optio -d optio >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

COUNT=$(docker compose exec -T postgres psql -U optio -d optio -tAc "SELECT COUNT(*) FROM records;")
COUNT=$(echo "$COUNT" | tr -d '[:space:]')

if [[ "$COUNT" -ge "$TARGET" ]]; then
  echo "==> Already seeded ($COUNT rows). Skipping."
  exit 0
fi

echo "==> Seeding $TARGET records (current=$COUNT)..."
docker compose exec -T postgres psql -U optio -d optio -v ON_ERROR_STOP=1 <<SQL
INSERT INTO records (email, name, payload, version, poison, updated_at, created_at)
SELECT
  'user' || g || '@example.com',
  'User ' || g,
  jsonb_build_object('seq', g, 'tier', (g % 5)),
  1,
  FALSE,
  NOW(),
  NOW()
FROM generate_series(($COUNT)::bigint + 1, $TARGET) AS g;
SQL

NEW_COUNT=$(docker compose exec -T postgres psql -U optio -d optio -tAc "SELECT COUNT(*) FROM records;")
echo "==> Seed complete: $NEW_COUNT rows"
