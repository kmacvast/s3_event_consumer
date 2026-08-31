#!/usr/bin/env bash
#
# End-to-end smoke test for the local Iceberg demo environment.
#
# Deliberately SEPARATE from the unit test suite: this one needs Docker, and
# `python3 -m unittest discover -s tests` must never need anything but Python.
#
#     ./scripts/smoke_test.sh
#
# It brings the stack up if it is not already running, records the current row
# count, publishes synthetic S3 events, runs the consumer until they are all
# written, and checks that the row count grew by exactly that many rows and that
# a new Iceberg snapshot was created. Leaves the stack running so you can poke at
# it; pass --down to tear it down (and delete the data) at the end.
#
# Requires: docker, and a Python environment with requirements.txt AND
# requirements-iceberg.txt installed.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE="docker compose -f docker/docker-compose.yml"
CONFIG="s3_consumer_config.local-iceberg.json"
PYTHON="${PYTHON:-python3}"
EVENT_COUNT="${EVENT_COUNT:-40}"
TEARDOWN=0
[ "${1:-}" = "--down" ] && TEARDOWN=1

pass() { printf '  \033[32mok\033[0m   %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

trino_query() {
    # Prints the query result as bare TSV, no header, no formatting.
    $COMPOSE exec -T trino trino --execute "$1" --output-format=TSV 2>/dev/null
}

count_rows() {
    trino_query "SELECT count(*) FROM iceberg.s3_events.object_events" | tr -d '"' | tail -1
}

count_snapshots() {
    trino_query 'SELECT count(*) FROM iceberg.s3_events."object_events$snapshots"' | tr -d '"' | tail -1
}

# --------------------------------------------------------------------------- #
step "Checking prerequisites"
# --------------------------------------------------------------------------- #

command -v docker >/dev/null || fail "docker is not installed"
docker info >/dev/null 2>&1 || fail "the Docker daemon is not running"
pass "docker is available"

$PYTHON -c "import confluent_kafka" 2>/dev/null || fail "confluent-kafka missing: pip install -r requirements.txt"
$PYTHON -c "import pyiceberg" 2>/dev/null || fail "pyiceberg missing: pip install -r requirements-iceberg.txt"
pass "confluent-kafka and pyiceberg are importable ($PYTHON)"

# --------------------------------------------------------------------------- #
step "Bringing the stack up"
# --------------------------------------------------------------------------- #

$COMPOSE up -d --wait
pass "all containers healthy"

# --------------------------------------------------------------------------- #
step "Creating the table if it does not exist yet"
# --------------------------------------------------------------------------- #

$PYTHON s3_event_consumer.py --config "$CONFIG" --check >/dev/null 2>&1 \
    || fail "--check failed; run it by hand to see why"
pass "catalog reachable and table present"

BEFORE_ROWS=$(count_rows)
BEFORE_SNAPS=$(count_snapshots)
echo "  rows before:      $BEFORE_ROWS"
echo "  snapshots before: $BEFORE_SNAPS"

# --------------------------------------------------------------------------- #
step "Publishing $EVENT_COUNT synthetic S3 events"
# --------------------------------------------------------------------------- #

$PYTHON scripts/publish_test_events.py \
    --count "$EVENT_COUNT" --interval 0 --well-formed-only --seed 1 \
    || fail "could not publish events"
pass "$EVENT_COUNT events published"

# --------------------------------------------------------------------------- #
step "Running the consumer until the events are written"
# --------------------------------------------------------------------------- #

# The consumer runs until interrupted, so give it a bounded window and then send
# SIGINT — which also exercises the graceful-shutdown flush path.
CONSUMER_LOG=$(mktemp)
$PYTHON s3_event_consumer.py --config "$CONFIG" --no-color >/dev/null 2>"$CONSUMER_LOG" &
CONSUMER_PID=$!

DEADLINE=$((SECONDS + 90))
while [ $SECONDS -lt $DEADLINE ]; do
    NOW=$(count_rows 2>/dev/null || echo "$BEFORE_ROWS")
    [ "$NOW" -ge "$((BEFORE_ROWS + EVENT_COUNT))" ] && break
    sleep 2
done

kill -INT "$CONSUMER_PID" 2>/dev/null || true
wait "$CONSUMER_PID" 2>/dev/null || true
pass "consumer stopped cleanly (SIGINT)"

grep -q "Iceberg sink closed" "$CONSUMER_LOG" \
    || fail "the consumer did not report a clean Iceberg shutdown; see $CONSUMER_LOG"
pass "shutdown flush reported"

grep -q "0 still unwritten" "$CONSUMER_LOG" \
    || fail "the consumer finished with unwritten records; see $CONSUMER_LOG"
pass "no records left unwritten"

# --------------------------------------------------------------------------- #
step "Verifying the table"
# --------------------------------------------------------------------------- #

AFTER_ROWS=$(count_rows)
AFTER_SNAPS=$(count_snapshots)
echo "  rows after:      $AFTER_ROWS"
echo "  snapshots after: $AFTER_SNAPS"

GAINED=$((AFTER_ROWS - BEFORE_ROWS))
[ "$GAINED" -eq "$EVENT_COUNT" ] \
    || fail "expected $EVENT_COUNT new rows, got $GAINED"
pass "row count grew by exactly $EVENT_COUNT"

[ "$AFTER_SNAPS" -gt "$BEFORE_SNAPS" ] || fail "no new Iceberg snapshot was created"
pass "new snapshot(s) created: $((AFTER_SNAPS - BEFORE_SNAPS))"

# Batching is the point: far fewer snapshots than events.
[ "$((AFTER_SNAPS - BEFORE_SNAPS))" -lt "$EVENT_COUNT" ] \
    || fail "one snapshot per event — batching is not working"
pass "batched: $((AFTER_SNAPS - BEFORE_SNAPS)) snapshot(s) for $EVENT_COUNT events"

NULL_BUCKETS=$(trino_query "SELECT count(*) FROM iceberg.s3_events.object_events WHERE bucket IS NULL" | tr -d '"' | tail -1)
[ "$NULL_BUCKETS" = "0" ] || fail "$NULL_BUCKETS well-formed events produced a NULL bucket"
pass "every well-formed event was flattened with a bucket name"

# --------------------------------------------------------------------------- #
step "Verifying the object-store layout"
# --------------------------------------------------------------------------- #

LAYOUT=$($COMPOSE exec -T minio mc ls --recursive local/warehouse/ 2>/dev/null \
    || $COMPOSE exec -T minio sh -c 'mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null && mc ls --recursive l/warehouse/')

grep -q "metadata.json" <<<"$LAYOUT" || fail "no Iceberg metadata.json in the warehouse"
pass "Iceberg metadata.json present"
grep -q "\.avro" <<<"$LAYOUT" || fail "no Avro manifest files in the warehouse"
pass "Avro manifest files present"
grep -q "\.parquet" <<<"$LAYOUT" || fail "no Parquet data files in the warehouse"
pass "Parquet data files present"
grep -q "ingest_time_day=" <<<"$LAYOUT" || fail "no partition directory in the warehouse"
pass "hidden partitioning visible as ingest_time_day=..."

# --------------------------------------------------------------------------- #
step "Verifying the Iceberg-disabled path still works"
# --------------------------------------------------------------------------- #

# Captured to a variable rather than piped into `grep -q`: under `set -o
# pipefail`, grep -q closes the pipe on its first match, the writer dies of
# SIGPIPE, and the pipeline reports that failure despite the match.
NO_ICEBERG_OUTPUT=$($PYTHON s3_event_consumer.py --config "$CONFIG" --no-iceberg --check 2>&1)
grep -q "Iceberg sink disabled by --no-iceberg" <<<"$NO_ICEBERG_OUTPUT" \
    || fail "--no-iceberg did not disable the Iceberg sink"
pass "--no-iceberg runs console-only"

grep -q "Connecting to Iceberg catalog" <<<"$NO_ICEBERG_OUTPUT" \
    && fail "--no-iceberg still contacted the catalog"
pass "--no-iceberg never contacts the catalog"

# A config with no iceberg section at all must look exactly as it did before
# Iceberg support existed: not one line mentioning it.
PLAIN_CONFIG=$(mktemp -t plain_config).json
$PYTHON - "$CONFIG" "$PLAIN_CONFIG" <<'PYEOF'
import json, sys
document = json.load(open(sys.argv[1]))
document.pop("iceberg", None)
document.pop("_comment", None)
# The demo group is literally named "...-iceberg-demo"; rename it so the
# assertion below tests the program's own output, not our config values.
document["kafka_config"]["group.id"] = "plain-console-check"
json.dump(document, open(sys.argv[2], "w"))
PYEOF
PLAIN_OUTPUT=$($PYTHON s3_event_consumer.py --config "$PLAIN_CONFIG" --check 2>&1)
rm -f "$PLAIN_CONFIG"
grep -qi "iceberg" <<<"$PLAIN_OUTPUT" \
    && fail "a config without an 'iceberg' section still mentioned Iceberg"
pass "no 'iceberg' section: output never mentions Iceberg"

rm -f "$CONSUMER_LOG"

if [ "$TEARDOWN" -eq 1 ]; then
    step "Tearing down"
    $COMPOSE down -v
    pass "stack removed"
fi

printf '\n\033[32mSmoke test passed.\033[0m %s rows in the table.\n' "$AFTER_ROWS"
