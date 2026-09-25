#!/usr/bin/env bash
# Proves G1–G5 by actually killing the pipeline and stopping Elasticsearch.
set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

BATCH=500
SEED_COUNT="${SEED_COUNT:-1000000}"
PASS=0
FAIL=0
declare -a LINES=()

note() { echo "==> $*"; }

record() {
  local gate="$1" status="$2" detail="$3"
  LINES+=("$gate $status $detail")
  if [[ "$status" == PASS ]]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
  echo "$gate $status $detail"
}

field() {
  local blob="$1" key="$2"
  printf '%s\n' "$blob" | awk -F= -v k="$key" '$1==k {print $2}' | tr -d '\r'
}

fetch_status() {
  curl -sf --max-time 20 http://localhost:3000/api/status.txt || true
}

wait_health() {
  local i
  for i in $(seq 1 120); do
    if curl -sf --max-time 5 http://localhost:3000/health >/dev/null; then
      return 0
    fi
    # If container exited, try to bring it back once during bootstrap.
    if (( i % 15 == 0 )); then
      docker compose up -d pipeline >/dev/null 2>&1 || true
    fi
    sleep 2
  done
  return 1
}

wait_es() {
  local i
  for i in $(seq 1 60); do
    if curl -sf --max-time 5 http://localhost:9200 >/dev/null; then
      return 0
    fi
    sleep 2
  done
  return 1
}

note "Building and starting the stack"
docker compose up -d --build

note "Waiting for Postgres"
for i in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U optio -d optio >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

note "Resetting replication state and seeding $SEED_COUNT rows"
docker compose stop pipeline consumer >/dev/null 2>&1 || true
docker compose exec -T postgres psql -U optio -d optio -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE records, dlq, consumed_events RESTART IDENTITY;
UPDATE checkpoints SET cursor_id = 0, cursor_ts = TIMESTAMPTZ '1970-01-01', updated_at = NOW();
SQL
curl -sf -X DELETE "http://localhost:9200/records" >/dev/null || true
docker compose exec -T redis redis-cli FLUSHDB >/dev/null
docker compose exec -T rabbitmq rabbitmqctl purge_queue optio.changes >/dev/null 2>&1 || true
SEED_COUNT="$SEED_COUNT" bash scripts/seed.sh
docker compose start pipeline consumer
if ! wait_health; then
  echo "pipeline did not become healthy"
  docker compose logs --tail=80 pipeline || true
  exit 1
fi

note "G1: waiting until backfill passes 40%"
TARGET=""
SAMPLE=""
start_ts=$(date +%s)
while true; do
  SAMPLE="$(fetch_status)"
  max_id="$(field "$SAMPLE" max_id)"
  cursor="$(field "$SAMPLE" backfill_cursor)"
  if [[ -n "${max_id:-}" && "$max_id" -gt 0 ]]; then
    TARGET=$((max_id * 40 / 100))
    if [[ "$cursor" -ge "$TARGET" ]]; then
      break
    fi
  fi
  if (( $(date +%s) - start_ts > 1800 )); then
    record "G1 resume after kill ............" FAIL "(timed out before 40%, cursor=${cursor:-0})"
    break
  fi
  sleep 3
done

G1_CURSOR="$(field "$SAMPLE" backfill_cursor)"
G1_READ="$(field "$SAMPLE" last_read)"
G1_OK=1
if [[ -z "${G1_CURSOR:-}" || "$G1_CURSOR" -le 0 ]]; then
  G1_OK=0
fi

if [[ "$G1_OK" == 1 ]]; then
  note "G1: docker kill pipeline at cursor=$G1_CURSOR read=$G1_READ"
  docker compose kill -s SIGKILL pipeline
  sleep 1
  # Durable truth is the Postgres checkpoint after the process is dead — not a slightly stale status poll.
  G1_CP="$(docker compose exec -T postgres psql -U optio -d optio -tAc \
    "SELECT cursor_id FROM checkpoints WHERE mode='backfill';" | tr -d '[:space:]')"
  docker compose up -d --no-deps pipeline >/dev/null 2>&1 || true
  if ! wait_health; then
    G1_OK=0
    G1_RESUME=0
  else
    sleep 1
    RESUME="$(fetch_status)"
    G1_RESUME="$(field "$RESUME" resumed_from)"
    if [[ -z "${G1_CP:-}" || "$G1_CP" -le 0 ]]; then
      G1_OK=0
    elif [[ -z "${G1_RESUME:-}" || "$G1_RESUME" -le 0 ]]; then
      G1_OK=0
    elif (( G1_RESUME != G1_CP )); then
      G1_OK=0
    elif (( G1_CP + BATCH < G1_READ && G1_READ - G1_CP > BATCH * 2 )); then
      # Read tip may be ahead of durable CP by at most the in-flight window.
      G1_OK=0
    fi
    G1_CURSOR="$G1_CP"
  fi
fi

note "G3: stopping Elasticsearch for 60s"
docker compose stop -t 5 elasticsearch >/dev/null 2>&1 || docker stop optio-elasticsearch-1 >/dev/null 2>&1 || true
# Wait until ES is actually unreachable before measuring outage behavior.
for i in $(seq 1 30); do
  if ! curl -sf --max-time 2 http://localhost:9200 >/dev/null; then
    break
  fi
  sleep 1
done
PRE_G3="$(fetch_status)"
G3_CURSOR="$(field "$PRE_G3" backfill_cursor)"
G3_SLEEPS="$(field "$PRE_G3" backoff_sleeps)"
sleep 60
MID_G3="$(fetch_status)"
G3_CURSOR_MID="$(field "$MID_G3" backfill_cursor)"
G3_SLEEPS_MID="$(field "$MID_G3" backoff_sleeps)"
G3_DELTA_SLEEP=$(( ${G3_SLEEPS_MID:-0} - ${G3_SLEEPS:-0} ))
G3_DELTA_CURSOR=$(( ${G3_CURSOR_MID:-0} - ${G3_CURSOR:-0} ))
note "G3: starting Elasticsearch (sleeps=$G3_DELTA_SLEEP cursor_delta=$G3_DELTA_CURSOR)"
recover_start=$(date +%s)
docker compose start elasticsearch >/dev/null 2>&1 || docker compose up -d elasticsearch >/dev/null 2>&1 || true
G3_RECOVERED=0
if wait_es; then
  while true; do
    NOW="$(fetch_status)"
    circuit="$(field "$NOW" circuit_open)"
    cursor_now="$(field "$NOW" backfill_cursor)"
    max_now="$(field "$NOW" max_id)"
    if [[ "$circuit" == "0" && -n "${cursor_now:-}" ]]; then
      if (( cursor_now > G3_CURSOR_MID )) || [[ -n "${max_now:-}" && "$cursor_now" -ge "$max_now" && "$max_now" -gt 0 ]]; then
        G3_RECOVERED=1
        break
      fi
    fi
    if (( $(date +%s) - recover_start > 180 )); then
      break
    fi
    sleep 2
  done
fi
G3_RECOVERY_SECS=$(( $(date +%s) - recover_start ))

note "Waiting for backfill to finish"
DONE=""
start_ts=$(date +%s)
while true; do
  DONE="$(fetch_status)"
  cursor="$(field "$DONE" backfill_cursor)"
  max_id="$(field "$DONE" max_id)"
  if [[ -n "${max_id:-}" && "$max_id" -gt 0 && "$cursor" -ge "$max_id" ]]; then
    break
  fi
  if (( $(date +%s) - start_ts > 2400 )); then
    note "backfill did not finish (cursor=${cursor:-0} max=${max_id:-0})"
    break
  fi
  sleep 5
done

note "Waiting for the consumer to catch up"
start_ts=$(date +%s)
SOURCE_COUNT=0
DISTINCT=0
DEDUPED=0
while true; do
  SOURCE_COUNT="$(docker compose exec -T postgres psql -U optio -d optio -tAc "SELECT COUNT(*) FROM records;" | tr -d '[:space:]')"
  DISTINCT="$(docker compose exec -T postgres psql -U optio -d optio -tAc "SELECT COUNT(DISTINCT record_id) FROM consumed_events;" | tr -d '[:space:]')"
  DEDUPED="$(docker compose exec -T postgres psql -U optio -d optio -tAc "SELECT COALESCE(SUM(duplicate_hits),0) FROM consumed_events;" | tr -d '[:space:]')"
  if [[ -n "$SOURCE_COUNT" && -n "$DISTINCT" && "$SOURCE_COUNT" -gt 0 && "$DISTINCT" -ge "$SOURCE_COUNT" ]]; then
    break
  fi
  if (( $(date +%s) - start_ts > 1200 )); then
    note "consumer lag remains source=$SOURCE_COUNT distinct=$DISTINCT"
    break
  fi
  sleep 5
done

curl -sf -X POST http://localhost:3000/api/admin/refresh >/dev/null || true
sleep 1
ES_COUNT="$(curl -sf http://localhost:9200/records/_count | node -e "let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>{try{console.log(JSON.parse(s).count)}catch(e){console.log(-1)}})")"
ES_COUNT="${ES_COUNT:- -1}"
ES_COUNT="$(echo "$ES_COUNT" | tr -d '[:space:]')"

LOST=$((SOURCE_COUNT - ES_COUNT))
if (( LOST < 0 )); then
  DUPES=$((ES_COUNT - SOURCE_COUNT))
  LOST=0
else
  DUPES=0
fi
if [[ "$DISTINCT" != "$SOURCE_COUNT" ]]; then
  DUPES=$((DUPES + 1))
fi

if [[ "$G1_OK" == 1 && "$LOST" == 0 && "$ES_COUNT" == "$SOURCE_COUNT" ]]; then
  record "G1 resume after kill ............" PASS "(killed at ${G1_READ} / resumed at ${G1_RESUME}, checkpoint=${G1_CURSOR}, 0 lost)"
else
  record "G1 resume after kill ............" FAIL "(killed at ${G1_READ:-0} / resumed at ${G1_RESUME:-0}, checkpoint=${G1_CURSOR:-0}, lost=${LOST})"
fi

if [[ "$ES_COUNT" == "$SOURCE_COUNT" && "$DISTINCT" == "$SOURCE_COUNT" && "$DUPES" == 0 ]]; then
  record "G2 no duplicates ................" PASS "(${SOURCE_COUNT} source / ${ES_COUNT} sink / 0 dupes, effectively-once, deduped_redeliveries=${DEDUPED})"
else
  record "G2 no duplicates ................" FAIL "(source=${SOURCE_COUNT} sink=${ES_COUNT} distinct_events=${DISTINCT} dupes=${DUPES})"
fi

G3_OK=1
if (( G3_DELTA_SLEEP < 2 || G3_DELTA_SLEEP > 200 )); then G3_OK=0; fi
if (( G3_DELTA_CURSOR > BATCH * 2 )); then G3_OK=0; fi
if [[ "$G3_RECOVERED" != 1 ]]; then G3_OK=0; fi
if (( LOST != 0 )); then G3_OK=0; fi
if [[ "$G3_OK" == 1 ]]; then
  record "G3 sink outage .................." PASS "(60s down, 0 lost, recovered in ${G3_RECOVERY_SECS}s, backoff_sleeps=${G3_DELTA_SLEEP})"
else
  record "G3 sink outage .................." FAIL "(60s down, lost=${LOST}, recovered=${G3_RECOVERED} in ${G3_RECOVERY_SECS}s, sleeps=${G3_DELTA_SLEEP}, cursor_delta=${G3_DELTA_CURSOR})"
fi

note "G4: partial batch of 500 with 3 poison rows"
BEFORE_ES="$ES_COUNT"
BEFORE_DLQ="$(field "$(fetch_status)" dlq_open)"
cat > /tmp/optio-poison.json <<'JSON'
{"count":500,"poison":3}
JSON
curl -sf -X POST http://localhost:3000/api/verify/poison-batch \
  -H 'content-type: application/json' \
  --data-binary @/tmp/optio-poison.json >/dev/null
start_ts=$(date +%s)
G4_DLQ=0
G4_ES_DELTA=-1
while true; do
  curl -sf -X POST http://localhost:3000/api/admin/refresh >/dev/null || true
  now_dlq="$(field "$(fetch_status)" dlq_open)"
  now_es="$(curl -sf http://localhost:9200/records/_count | node -e "let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>{try{console.log(JSON.parse(s).count)}catch(e){console.log(-1)}})" | tr -d '[:space:]')"
  G4_DLQ=$(( ${now_dlq:-0} - ${BEFORE_DLQ:-0} ))
  G4_ES_DELTA=$(( ${now_es:-0} - BEFORE_ES ))
  if (( G4_DLQ >= 3 && G4_ES_DELTA >= 497 )); then
    break
  fi
  if (( $(date +%s) - start_ts > 300 )); then
    break
  fi
  sleep 3
done

DLQ_CONTEXT="$(docker compose exec -T postgres psql -U optio -d optio -tAc \
  "SELECT COUNT(*) FROM dlq WHERE status='open' AND payload ? 'email' AND length(error)>0 AND length(batch_id)>0 AND record_id IS NOT NULL;" | tr -d '[:space:]')"

if (( G4_ES_DELTA == 497 && G4_DLQ == 3 && DLQ_CONTEXT >= 3 )); then
  record "G4 partial batch failure ........" PASS "(497 written, 3 in DLQ)"
else
  record "G4 partial batch failure ........" FAIL "(written_delta=${G4_ES_DELTA}, dlq_delta=${G4_DLQ}, context_rows=${DLQ_CONTEXT})"
fi

note "G5: observability"
STATUS_JSON="$(curl -sf http://localhost:3000/api/status || true)"
METRICS="$(curl -sf http://localhost:3000/metrics || true)"
UI="$(curl -sf http://localhost:4200/ || true)"
G5_OK=1
echo "$STATUS_JSON" | grep -q '"health"' || G5_OK=0
echo "$STATUS_JSON" | grep -q 'throughput_per_sec' || G5_OK=0
echo "$STATUS_JSON" | grep -q 'lag_count' || G5_OK=0
echo "$STATUS_JSON" | grep -q 'dlq_open' || G5_OK=0
echo "$STATUS_JSON" | grep -q 'cursor_id' || G5_OK=0
echo "$METRICS" | grep -q 'optio_backfill_cursor' || G5_OK=0
echo "$METRICS" | grep -q 'optio_throughput_per_sec' || G5_OK=0
echo "$METRICS" | grep -q 'optio_incremental_lag_count' || G5_OK=0
echo "$METRICS" | grep -q 'optio_dlq_open' || G5_OK=0
echo "$METRICS" | grep -q 'optio_health' || G5_OK=0
echo "$UI" | grep -q 'throughput' || G5_OK=0
echo "$UI" | grep -q 'lag' || G5_OK=0
echo "$UI" | grep -q 'DLQ' || G5_OK=0
echo "$UI" | grep -q 'health' || G5_OK=0
echo "$UI" | grep -q 'backfill' || G5_OK=0
if [[ "$G5_OK" == 1 ]]; then
  record "G5 observability ................" PASS ""
else
  record "G5 observability ................" FAIL "(status, metrics, or UI shell missing required fields)"
fi

echo
echo "-------- verify report --------"
for line in "${LINES[@]}"; do
  echo "$line"
done
echo "PASS=$PASS FAIL=$FAIL"
echo "guarantee=effectively-once"

if (( FAIL > 0 )); then
  exit 1
fi
exit 0
