#!/usr/bin/env bash
#
# Return the demo to a known starting condition.
#
#     set -a; . ./docker/demo.env; set +a
#     ./scripts/demo_reset.sh              # show what would happen, change nothing
#     ./scripts/demo_reset.sh --confirm    # actually do it
#
# WHAT IT RESETS
#   1. The Iceberg demo table: dropped and recreated empty.
#   2. The demo's Kafka consumer group offsets: deleted, so the topic replays.
#   3. Optionally (--purge-source) the demo objects previously written into the
#      watched bucket under the demo prefix.
#
# SAFETY
# This script is deliberately hard to misuse:
#   - It changes nothing without --confirm.
#   - It only ever touches the ONE namespace.table named by ICEBERG_NAMESPACE
#     and ICEBERG_TABLE, and refuses obviously dangerous values.
#   - It refuses to run if the table is not in the demo namespace.
#   - It never deletes a bucket, and never deletes anything outside the demo
#     key prefix in the watched bucket.
#   - --purge-source is opt-in on top of --confirm and lists every key first.
#
# It cannot delete arbitrary VAST data: there is no code path here that removes
# a bucket, and object deletion is restricted to DEMO_PREFIX inside
# VAST_SOURCE_BUCKET.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

COMPOSE="docker compose -f docker/docker-compose.yml"
PYTHON="${PYTHON:-python3}"
CONFIG="${CONFIG:-s3_consumer_config.json}"
DEMO_PREFIX="${DEMO_PREFIX:-demo/}"

CONFIRM=0
PURGE_SOURCE=0
for arg in "$@"; do
    case "$arg" in
        --confirm)       CONFIRM=1 ;;
        --purge-source)  PURGE_SOURCE=1 ;;
        -h|--help)       sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
step "2. Kafka consumer group offsets"
# --------------------------------------------------------------------------- #

# Deleting the group makes the next run replay the topic from the beginning
# (auto.offset.reset: earliest), which is what gives a repeatable demo without
# deleting a single event from VAST.
if [ "$CONFIRM" -eq 1 ]; then
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
fi

# --------------------------------------------------------------------------- #
step "3. Demo objects in the watched bucket"
# --------------------------------------------------------------------------- #

if [ "$PURGE_SOURCE" -eq 0 ]; then
    note "skipped - pass --purge-source to also remove s3://$VAST_SOURCE_BUCKET/$DEMO_PREFIX*"
    note "leaving them is usually fine: the demo writes new keys each run"
else
    [ -n "${VAST_SOURCE_BUCKET:-}" ] || bad "VAST_SOURCE_BUCKET is not set"
    case "$DEMO_PREFIX" in
        ""|"/"|"*") bad "refusing to purge with prefix '$DEMO_PREFIX' - it must be a real prefix" ;;
    esac

    export DEMO_PREFIX
    KEYS=$($PYTHON - <<'PYEOF'
import os
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["VAST_S3_ENDPOINT"],
    aws_access_key_id=os.environ["VAST_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["VAST_S3_SECRET_KEY"],
    region_name=os.environ.get("VAST_S3_REGION", "us-east-1"),
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
prefix = os.environ["DEMO_PREFIX"]
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=os.environ["VAST_SOURCE_BUCKET"], Prefix=prefix):
    for item in page.get("Contents", []):
        # Belt and braces: never emit a key that escaped the prefix.
        if item["Key"].startswith(prefix):
            print(item["Key"])
PYEOF
    )
    COUNT=$(printf '%s' "$KEYS" | grep -c . || true)

    if [ "$COUNT" -eq 0 ]; then
        ok "nothing to purge under s3://$VAST_SOURCE_BUCKET/$DEMO_PREFIX"
    else
        note "$COUNT object(s) under s3://$VAST_SOURCE_BUCKET/$DEMO_PREFIX:"
        printf '%s\n' "$KEYS" | head -20 | sed 's/^/         - /'
        [ "$COUNT" -gt 20 ] && note "... and $((COUNT - 20)) more"

        if [ "$CONFIRM" -eq 1 ]; then
            KEYFILE=$(mktemp -t demo_reset_keys.XXXXXX)
            printf '%s\n' "$KEYS" > "$KEYFILE"
            if $PYTHON - "$KEYFILE" <<'PYEOF'
import os, sys
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["VAST_S3_ENDPOINT"],
    aws_access_key_id=os.environ["VAST_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["VAST_S3_SECRET_KEY"],
    region_name=os.environ.get("VAST_S3_REGION", "us-east-1"),
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
prefix = os.environ["DEMO_PREFIX"]
bucket = os.environ["VAST_SOURCE_BUCKET"]
with open(sys.argv[1], encoding="utf-8") as handle:
    keys = [line.strip() for line in handle if line.strip()]
# Re-check the prefix here too. Deleting is the one irreversible thing this
# script does, so the guard is repeated rather than trusted from upstream.
unsafe = [k for k in keys if not k.startswith(prefix)]
if unsafe:
    sys.exit("refusing to delete keys outside %r: %s" % (prefix, unsafe[:3]))
for i in range(0, len(keys), 1000):
    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True},
    )
print("deleted %d object(s)" % len(keys))
PYEOF
            then
                rm -f "$KEYFILE"
                ok "purged $COUNT demo object(s)"
                note "note: these deletions may themselves raise ObjectRemoved events"
            else
                rm -f "$KEYFILE"
                bad "purge failed"
            fi
        else
            plan "delete those $COUNT object(s)"
        fi
    fi
fi

# --------------------------------------------------------------------------- #
if [ "$CONFIRM" -eq 1 ]; then
    printf '\n\033[32mReset complete.\033[0m Baseline row count should now be 0.\n'
    printf 'Verify with:\n'
    printf '  %s exec -T trino trino --execute "SELECT count(*) FROM iceberg.%s.%s"\n' \
        "$COMPOSE" "$NAMESPACE" "$TABLE"
else
    printf '\n\033[33mDry run finished.\033[0m Re-run with --confirm to apply.\n'
fi
