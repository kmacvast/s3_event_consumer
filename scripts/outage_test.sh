#!/usr/bin/env bash
#
# Proves the Kafka-offset / Iceberg-commit contract against the real local stack:
#
#   1. Take the Iceberg REST catalog away mid-ingestion.
#   2. Publish events. The consumer buffers them, fails to commit, retries with
#      backoff, and ultimately stops non-zero WITHOUT committing Kafka offsets.
#   3. Bring the catalog back and restart the consumer.
#   4. The same events are replayed from Kafka and land in the table.
#
# This exercises the single-consumer architecture only. It does not cover Kafka
# rebalance behaviour, and it runs against the local stack, not against a real
# VAST Event Broker or VAST S3 warehouse. Records may also be duplicated on
# replay if a failure lands between an Iceberg commit and its offset commit —
# this is at-least-once, not exactly-once.
#
#     ./scripts/outage_test.sh
#
# Requires Docker and requirements-iceberg.txt. Separate from the unit suite.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE="docker compose -f docker/docker-compose.yml"
CONFIG="${CONFIG:-s3_consumer_config.json}"
NAMESPACE="${ICEBERG_NAMESPACE:-s3_events}"
TABLE="${ICEBERG_TABLE:-object_events}"
BROKER="${VAST_KAFKA_BROKER:-localhost:19092}"
PYTHON="${PYTHON:-python3}"
GROUP="outage-test-$$"
TOPIC="outage-events-$$"
EVENT_COUNT="${EVENT_COUNT:-30}"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass() { printf '  \033[32mok\033[0m   %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
info() { printf '       %s\n' "$1"; }

trino_query() {
    $COMPOSE exec -T trino trino --execute "$1" --output-format=TSV 2>/dev/null
}

count_rows() {
    trino_query "SELECT count(*) FROM iceberg.$NAMESPACE.$TABLE" | tr -d '"' | tail -1
}

# Committed offset for our throwaway consumer group, or "-" when never committed.
committed_offset() {
    GROUP="$GROUP" TOPIC="$TOPIC" BROKER="$BROKER" $PYTHON - <<'PYEOF' 2>/dev/null
import os
from confluent_kafka import Consumer, TopicPartition
c = Consumer({"bootstrap.servers": os.environ["BROKER"],
              "group.id": os.environ["GROUP"], "enable.auto.commit": False})
try:
    committed = c.committed([TopicPartition(os.environ["TOPIC"], 0)], timeout=15)
    offset = committed[0].offset if committed else -1001
    print("" if offset is None or offset < 0 else offset)
finally:
    c.close()
PYEOF
}

# A config on its own topic and consumer group, so this test cannot disturb the
# offsets of the main demo.
build_config() {
    $PYTHON - "$CONFIG" "$WORK/config.json" "$TOPIC" "$GROUP" "$1" <<'PYEOF'
import json, os, sys
source, target, topic, group, batch_size = sys.argv[1:6]
document = json.load(open(source))
document["topic"] = topic
document["kafka_config"]["group.id"] = group
document["kafka_config"]["bootstrap.servers"] = os.environ["VAST_KAFKA_BROKER"]
document["iceberg"]["batch_size"] = int(batch_size)
document["iceberg"]["flush_interval_seconds"] = 3
document["iceberg"]["max_flush_attempts"] = 3
document["iceberg"]["retry_backoff_seconds"] = 2
json.dump(document, open(target, "w"), indent=2)
PYEOF
}

# --------------------------------------------------------------------------- #
step "Preparing"
# --------------------------------------------------------------------------- #

docker info >/dev/null 2>&1 || fail "the Docker daemon is not running"
$PYTHON -c "import pyiceberg" 2>/dev/null || fail "pyiceberg missing"
$COMPOSE up -d --wait >/dev/null 2>&1
pass "stack healthy"

build_config 10
info "topic:          $TOPIC"
info "consumer group: $GROUP"

# Make sure the table exists before the catalog is taken away.
$PYTHON s3_event_consumer.py --config "$WORK/config.json" --check >/dev/null 2>&1 \
    || fail "--check failed against a healthy catalog"
ROWS_START=$(count_rows)
pass "table ready, $ROWS_START row(s) to begin with"

# --------------------------------------------------------------------------- #
step "Starting the consumer while the catalog is still healthy"
# --------------------------------------------------------------------------- #

# The sink opens at startup and needs the catalog, so the consumer must be
# running *before* the outage. This is what exercises the runtime failure path
# rather than the (separately tested) startup failure path.
$PYTHON s3_event_consumer.py --config "$WORK/config.json" --no-color \
    >"$WORK/out.txt" 2>"$WORK/err.txt" &
OUTAGE_PID=$!

DEADLINE=$((SECONDS + 60))
until grep -q "Waiting for events" "$WORK/err.txt" 2>/dev/null; do
    [ $SECONDS -lt $DEADLINE ] || fail "the consumer never subscribed; see $WORK/err.txt"
    kill -0 "$OUTAGE_PID" 2>/dev/null || fail "the consumer died at startup; see $WORK/err.txt"
    sleep 1
done
pass "consumer subscribed and idle, catalog healthy"

# --------------------------------------------------------------------------- #
step "Taking the Iceberg catalog down mid-run"
# --------------------------------------------------------------------------- #

$COMPOSE stop iceberg-rest >/dev/null 2>&1
pass "iceberg-rest stopped while the consumer is running"

# --------------------------------------------------------------------------- #
step "Publishing $EVENT_COUNT events into the outage"
# --------------------------------------------------------------------------- #

$PYTHON scripts/publish_test_events.py \
    --bootstrap-servers "$BROKER" \
    --topic "$TOPIC" --count "$EVENT_COUNT" --interval 0 --well-formed-only --seed 11 >/dev/null \
    || fail "could not publish events"
pass "$EVENT_COUNT events published to $TOPIC"

# The consumer buffers them, fails every append, backs off, and gives up.
DEADLINE=$((SECONDS + 180))
while kill -0 "$OUTAGE_PID" 2>/dev/null; do
    [ $SECONDS -lt $DEADLINE ] || fail "the consumer never gave up; see $WORK/err.txt"
    sleep 2
done
set +e
wait "$OUTAGE_PID"
OUTAGE_EXIT=$?
set -e

info "exit code: $OUTAGE_EXIT"
[ "$OUTAGE_EXIT" -ne 0 ] || fail "the consumer exited 0 despite never writing to Iceberg"
pass "consumer exited non-zero"

grep -q "Iceberg append failed" "$WORK/err.txt" || {
    printf '\n--- consumer stderr ---\n'; tail -20 "$WORK/err.txt"; printf -- '---\n'
    fail "no append failure was reported"
}
pass "append failures reported"

grep -q "NOT committing their Kafka offsets" "$WORK/err.txt" \
    || fail "the consumer did not say it was withholding offsets"
pass "consumer stated it was withholding Kafka offsets"

RETRIES=$(grep -c "Iceberg append failed" "$WORK/err.txt" || true)
info "append attempts: $RETRIES"
[ "$RETRIES" -le 10 ] || fail "$RETRIES retries looks like a hot loop, not bounded backoff"
pass "retries bounded ($RETRIES attempts, not a hot loop)"

grep -qE "max_flush_attempts|max_buffered_records" "$WORK/err.txt" \
    || fail "the consumer did not report hitting a bound"
pass "stopped on a documented bound"

# The heart of it: Kafka must not have been told anything was consumed.
OFFSET=$(committed_offset)
info "committed offset for $GROUP: ${OFFSET:-<none>}"
if [ -n "$OFFSET" ] && [ "$OFFSET" != "-" ] && [ "$OFFSET" != "0" ]; then
    fail "Kafka offsets were advanced to $OFFSET despite no Iceberg commit"
fi
pass "NO Kafka offsets committed for the failed batch"

# The row count cannot be checked here: Trino reads through the same REST
# catalog that is currently stopped. It is verified immediately after recovery,
# below, which proves the same thing.

# --------------------------------------------------------------------------- #
step "Restoring the Iceberg catalog"
# --------------------------------------------------------------------------- #

$COMPOSE start iceberg-rest >/dev/null 2>&1
DEADLINE=$((SECONDS + 120))
until curl -fsS "${ICEBERG_CATALOG_URI_HOST:-http://localhost:8181}/v1/config" >/dev/null 2>&1; do
    [ $SECONDS -lt $DEADLINE ] || fail "iceberg-rest did not come back"
    sleep 2
done
pass "iceberg-rest healthy again"

# Trino caches catalog metadata briefly; give it a moment to reconnect.
DEADLINE=$((SECONDS + 90))
until ROWS_AFTER_OUTAGE=$(count_rows 2>/dev/null) && [ -n "$ROWS_AFTER_OUTAGE" ]; do
    [ $SECONDS -lt $DEADLINE ] || fail "Trino could not query the table after recovery"
    sleep 3
done

[ "$ROWS_AFTER_OUTAGE" -eq "$ROWS_START" ] \
    || fail "rows appeared during the outage: $ROWS_START -> $ROWS_AFTER_OUTAGE"
pass "nothing was written during the outage (still $ROWS_AFTER_OUTAGE rows)"

# --------------------------------------------------------------------------- #
step "Replaying: the same events must now be ingested"
# --------------------------------------------------------------------------- #

$PYTHON s3_event_consumer.py --config "$WORK/config.json" --no-color \
    >"$WORK/out2.txt" 2>"$WORK/err2.txt" &
REPLAY_PID=$!

DEADLINE=$((SECONDS + 120))
while [ $SECONDS -lt $DEADLINE ]; do
    NOW=$(count_rows 2>/dev/null || echo "$ROWS_START")
    [ "$NOW" -ge "$((ROWS_START + EVENT_COUNT))" ] && break
    sleep 2
done

kill -INT "$REPLAY_PID" 2>/dev/null || true
set +e
wait "$REPLAY_PID"
REPLAY_EXIT=$?
set -e

info "exit code: $REPLAY_EXIT"
[ "$REPLAY_EXIT" -eq 0 ] || fail "the replay run exited $REPLAY_EXIT; see $WORK/err2.txt"
pass "replay run exited 0"

ROWS_END=$(count_rows)
GAINED=$((ROWS_END - ROWS_START))
info "rows: $ROWS_START -> $ROWS_END (+$GAINED)"

[ "$GAINED" -ge "$EVENT_COUNT" ] \
    || fail "only $GAINED of $EVENT_COUNT events survived the outage — data was lost"
pass "all $EVENT_COUNT events survived the outage and were ingested"

if [ "$GAINED" -gt "$EVENT_COUNT" ]; then
    info "note: $((GAINED - EVENT_COUNT)) duplicate row(s) — expected under at-least-once"
fi

OFFSET=$(committed_offset)
info "committed offset for $GROUP: ${OFFSET:-<none>}"
[ "$OFFSET" = "$EVENT_COUNT" ] \
    || fail "expected the committed offset to be $EVENT_COUNT, got ${OFFSET:-<none>}"
pass "Kafka offsets committed only after the Iceberg commit succeeded"

# --------------------------------------------------------------------------- #
step "Cleaning up the throwaway consumer group"
# --------------------------------------------------------------------------- #

GROUP="$GROUP" BROKER="$BROKER" $PYTHON - >/dev/null 2>&1 <<'PYEOF' || true
import os
from confluent_kafka.admin import AdminClient
admin = AdminClient({"bootstrap.servers": os.environ["BROKER"]})
admin.delete_consumer_groups([os.environ["GROUP"]], request_timeout=30)[os.environ["GROUP"]].result()
PYEOF
pass "consumer group removed"

printf '\n\033[32mOutage test passed.\033[0m Replay after an Iceberg outage recovered every event.\n'
printf 'Single-consumer, local stack only: Kafka rebalance and the VAST lab are not covered.\n'
