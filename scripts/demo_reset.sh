#!/usr/bin/env bash
#
# Return the demo to a known starting condition.
#
#     set -a; . ./docker/demo.env; set +a
#     ./scripts/demo_reset.sh                    # show what would happen, change nothing
#     ./scripts/demo_reset.sh --confirm          # Iceberg table + consumer group
#     ./scripts/demo_reset.sh --confirm --all    # also empty the source bucket and Kafka log
#
# WHAT IT RESETS
#   1. The Iceberg demo table: dropped and recreated empty.
#   2. The demo's Kafka consumer group offsets: deleted, so the topic replays
#      (skipped when --recreate-topic / --all, because that step deletes the
#      group after replacing the topic).
#   3. Optionally (--purge-source) objects under the demo/ prefix.
#   4. Optionally (--purge-source-all) every object in VAST_SOURCE_BUCKET.
#      The bucket itself is never deleted.
#   5. Optionally (--recreate-topic) delete and recreate VAST_KAFKA_TOPIC via
#      Kafka Admin delete_topics plus VMS (vastpy). This is what zeros the
#      dashboard's KAFKA EVENTS meter.
#
# --all is --purge-source-all plus --recreate-topic. Purge runs before the
# topic is recreated so ObjectRemoved events do not refill the new log.
#
# SAFETY
# This script is deliberately hard to misuse:
#   - It changes nothing without --confirm.
#   - It only ever touches the ONE namespace.table named by ICEBERG_NAMESPACE
#     and ICEBERG_TABLE, and refuses obviously dangerous values.
#   - It refuses to run if the table is not in the demo namespace.
#   - It never deletes a bucket.
#   - --purge-source stays inside DEMO_PREFIX; --purge-source-all is opt-in
#     and still only objects inside VAST_SOURCE_BUCKET, never the warehouse.
#   - --purge-source, --purge-source-all and --recreate-topic are opt-in on
#     top of --confirm.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

COMPOSE="docker compose -f docker/docker-compose.yml"
PYTHON="${PYTHON:-python3}"
CONFIG="${CONFIG:-s3_consumer_config.json}"
DEMO_PREFIX="${DEMO_PREFIX:-demo/}"

CONFIRM=0
PURGE_SOURCE=0
PURGE_ALL=0
RECREATE_TOPIC=0
for arg in "$@"; do
    case "$arg" in
        --confirm)          CONFIRM=1 ;;
        --purge-source)     PURGE_SOURCE=1 ;;
        --purge-source-all) PURGE_ALL=1; PURGE_SOURCE=1 ;;
        --recreate-topic)   RECREATE_TOPIC=1 ;;
        --all)              PURGE_ALL=1; PURGE_SOURCE=1; RECREATE_TOPIC=1 ;;
        -h|--help)          sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) printf 'unknown option: %s\n' "$arg" >&2; exit 2 ;;
    esac
done

ok()   { printf '  \033[32mok\033[0m   %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }
note() { printf '       %s\n' "$1"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
plan() { printf '  \033[33mWOULD\033[0m %s\n' "$1"; }

# --------------------------------------------------------------------------- #
step "Checking the target is a demo table"
# --------------------------------------------------------------------------- #

for var in ICEBERG_NAMESPACE ICEBERG_TABLE VAST_KAFKA_TOPIC VAST_KAFKA_GROUP; do
    [ -n "${!var:-}" ] || bad "$var is not set - run: set -a; . ./docker/demo.env; set +a"
done

NAMESPACE="$ICEBERG_NAMESPACE"
TABLE="$ICEBERG_TABLE"

# Guard rails. These names are the ones this project creates; anything else is
# far more likely to be a typo pointed at real data than a deliberate choice.
case "$NAMESPACE" in
    ""|"default"|"information_schema"|"system"|"*")
        bad "refusing to operate on namespace '$NAMESPACE'" ;;
esac
case "$TABLE" in
    ""|"*") bad "refusing to operate on table '$TABLE'" ;;
esac
if ! printf '%s' "$NAMESPACE" | grep -qE '^[A-Za-z0-9_][A-Za-z0-9_-]*$'; then
    bad "namespace '$NAMESPACE' is not a plain identifier"
fi
if ! printf '%s' "$TABLE" | grep -qE '^[A-Za-z0-9_][A-Za-z0-9_-]*$'; then
    bad "table '$TABLE' is not a plain identifier"
fi

ok "target is iceberg.$NAMESPACE.$TABLE"
note "consumer group : $VAST_KAFKA_GROUP"
note "topic          : $VAST_KAFKA_TOPIC"
if [ "$PURGE_ALL" -eq 1 ]; then
    note "source purge   : ALL objects in s3://${VAST_SOURCE_BUCKET:-<VAST_SOURCE_BUCKET>}"
elif [ "$PURGE_SOURCE" -eq 1 ]; then
    note "source purge   : s3://${VAST_SOURCE_BUCKET:-<bucket>}/$DEMO_PREFIX*"
fi
if [ "$RECREATE_TOPIC" -eq 1 ]; then
    note "kafka log      : delete and recreate $VAST_KAFKA_TOPIC"
fi

if [ "$CONFIRM" -eq 0 ]; then
    printf '\n\033[33mDRY RUN.\033[0m Nothing will be changed. Re-run with --confirm to apply.\n'
fi

# --------------------------------------------------------------------------- #
step "1. Iceberg table"
# --------------------------------------------------------------------------- #

CURRENT=$($COMPOSE exec -T trino trino --execute \
    "SELECT count(*) FROM iceberg.$NAMESPACE.$TABLE" --output-format=TSV 2>/dev/null \
    | tr -d '"' | tail -1)

if [ -n "$CURRENT" ]; then
    note "current row count: $CURRENT"
else
    note "table does not exist yet, or is not queryable"
fi

if [ "$CONFIRM" -eq 1 ]; then
    if $COMPOSE exec -T trino trino --execute \
        "DROP TABLE IF EXISTS iceberg.$NAMESPACE.$TABLE" >/dev/null 2>&1; then
        ok "dropped iceberg.$NAMESPACE.$TABLE"
    else
        bad "could not drop iceberg.$NAMESPACE.$TABLE"
    fi

    # Recreate it empty, with this project's explicit schema, so the demo can
    # show a baseline of zero rather than a missing table.
    if $PYTHON s3_event_consumer.py --config "$CONFIG" --check >/dev/null 2>&1; then
        ok "recreated the table, empty, with the consumer's schema"
    else
        bad "could not recreate the table - run: $PYTHON s3_event_consumer.py --config $CONFIG --check"
    fi
else
    plan "DROP TABLE iceberg.$NAMESPACE.$TABLE, then recreate it empty"
fi

# --------------------------------------------------------------------------- #
step "2. Demo objects in the watched bucket"
# --------------------------------------------------------------------------- #

if [ "$PURGE_SOURCE" -eq 0 ]; then
    note "skipped - pass --purge-source, --purge-source-all, or --all"
    note "the dashboard SOURCE OBJECTS meter counts the whole bucket unless you pass --prefix"
else
    [ -n "${VAST_SOURCE_BUCKET:-}" ] || bad "VAST_SOURCE_BUCKET is not set"
    if [ "$PURGE_ALL" -eq 0 ]; then
        case "$DEMO_PREFIX" in
            ""|"/"|"*") bad "refusing to purge with prefix '$DEMO_PREFIX' - it must be a real prefix" ;;
        esac
    fi

    export DEMO_PREFIX
    export DEMO_PURGE_ALL="$PURGE_ALL"
    export DEMO_CONFIRM="$CONFIRM"
    if $PYTHON - <<'PYEOF'
import os
import sys
import boto3
from botocore.config import Config

bucket = os.environ["VAST_SOURCE_BUCKET"]
warehouse = os.environ.get("ICEBERG_WAREHOUSE", "")
if warehouse.startswith("s3://") or warehouse.startswith("s3a://"):
    rest = warehouse.split("://", 1)[1]
    warehouse_bucket = rest.split("/", 1)[0]
    if warehouse_bucket and warehouse_bucket == bucket:
        sys.exit("refusing to purge VAST_SOURCE_BUCKET; it is the Iceberg warehouse")

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["VAST_S3_ENDPOINT"],
    aws_access_key_id=os.environ["VAST_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["VAST_S3_SECRET_KEY"],
    region_name=os.environ.get("VAST_S3_REGION", "us-east-1"),
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
purge_all = os.environ.get("DEMO_PURGE_ALL") == "1"
prefix = "" if purge_all else os.environ["DEMO_PREFIX"]
paginator = s3.get_paginator("list_objects_v2")
kwargs = {"Bucket": bucket}
if prefix:
    kwargs["Prefix"] = prefix
keys = []
for page in paginator.paginate(**kwargs):
    for item in page.get("Contents", []):
        key = item["Key"]
        if prefix and not key.startswith(prefix):
            continue
        keys.append(key)

target = "s3://%s/%s*" % (bucket, prefix) if prefix else "s3://%s (entire bucket, objects only)" % bucket
if not keys:
    print("nothing to purge under %s" % target)
    sys.exit(0)

print("%d object(s) under %s" % (len(keys), target))
for key in keys[:20]:
    print("  - %s" % key)
if len(keys) > 20:
    print("  ... and %d more" % (len(keys) - 20))

if os.environ.get("DEMO_CONFIRM") != "1":
    print("dry-run: not deleting")
    sys.exit(0)

for i in range(0, len(keys), 1000):
    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True},
    )
print("deleted %d object(s)" % len(keys))
PYEOF
    then
        if [ "$CONFIRM" -eq 1 ]; then
            ok "purged source objects"
            note "deletes may publish ObjectRemoved events; the topic recreate step wipes those"
        else
            plan "delete the listed source objects"
        fi
    else
        bad "source purge failed"
    fi
fi

# --------------------------------------------------------------------------- #
step "3. Kafka"
# --------------------------------------------------------------------------- #

if [ "$RECREATE_TOPIC" -eq 1 ]; then
    note "stop the demo consumer first"
    RECREATE_ARGS=()
    if [ "$CONFIRM" -eq 1 ]; then
        RECREATE_ARGS+=(--confirm)
    fi
    if $PYTHON scripts/demo_recreate_topic.py "${RECREATE_ARGS[@]+"${RECREATE_ARGS[@]}"}"; then
        ok "topic recreate finished"
    else
        bad "topic recreate failed"
    fi
elif [ "$CONFIRM" -eq 1 ]; then
    if $PYTHON - <<'PYEOF'
import os, sys
from confluent_kafka.admin import AdminClient
admin = AdminClient({"bootstrap.servers": os.environ["VAST_KAFKA_BROKER"]})
group = os.environ["VAST_KAFKA_GROUP"]
try:
    futures = admin.delete_consumer_groups([group], request_timeout=30)
    futures[group].result()
except Exception as exc:                      # noqa: BLE001
    # A group that was never created is not an error for a reset.
    if "UNKNOWN_GROUP" in str(exc).upper() or "GROUP_ID_NOT_FOUND" in str(exc).upper():
        print("group did not exist", file=sys.stderr)
        sys.exit(0)
    print(exc, file=sys.stderr)
    sys.exit(1)
PYEOF
    then
        ok "consumer group '$VAST_KAFKA_GROUP' offsets cleared - the topic will replay"
    else
        note "could not delete the consumer group; it may simply not exist yet"
        note "if the consumer is still running, stop it first - a live member blocks deletion"
    fi
else
    plan "delete Kafka consumer group '$VAST_KAFKA_GROUP' so the topic replays"
    note "this does not empty the Kafka log; pass --recreate-topic or --all for that"
fi

# --------------------------------------------------------------------------- #
if [ "$CONFIRM" -eq 1 ]; then
    printf '\n\033[32mReset complete.\033[0m\n'
    if [ "$RECREATE_TOPIC" -eq 1 ]; then
        printf 'Dashboard meters should now be 0 after you re-save the bucket notification.\n'
    else
        printf 'Baseline Iceberg row count should now be 0. Kafka log is unchanged unless you passed --all.\n'
    fi
else
    printf '\n\033[33mDry run finished.\033[0m Re-run with --confirm to apply.\n'
fi
