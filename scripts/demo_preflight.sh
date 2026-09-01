#!/usr/bin/env bash
#
# Customer-demo preflight. Run this BEFORE the customer arrives.
#
#     set -a; . ./docker/demo.env; set +a
#     ./scripts/demo_preflight.sh
#
# Checks every layer the demo depends on and prints one line per check. Exits
# non-zero if anything that would break the demo is wrong, so it can be wired
# into a wrapper script.
#
# It NEVER prints an access key, a secret key, or any credential. Endpoints and
# bucket names are printed, because you need to see what you are pointed at.
#
# Read-only with one deliberate exception, clearly marked below: the Iceberg
# table initialisation check creates the namespace and table if they do not
# exist. That is a write to your catalog. Skip it with --no-table-init.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

COMPOSE="docker compose -f docker/docker-compose.yml"
PYTHON="${PYTHON:-python3}"
CONFIG="${CONFIG:-s3_consumer_config.json}"
TABLE_INIT=1
[ "${1:-}" = "--no-table-init" ] && TABLE_INIT=0

PASS=0
WARN=0
FAIL=0

ok()   { printf '  \033[32m PASS \033[0m %s\n' "$1"; PASS=$((PASS + 1)); }
warn() { printf '  \033[33m WARN \033[0m %s\n' "$1"; WARN=$((WARN + 1)); }
bad()  { printf '  \033[31m FAIL \033[0m %s\n' "$1"; FAIL=$((FAIL + 1)); }
note() { printf '         %s\n' "$1"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# Mask everything but the first four characters of a secret.
mask() {
    local value="${1:-}"
    if [ -z "$value" ]; then printf '<unset>'; return; fi
    printf '%.4s%s' "$value" "…($((${#value})) chars)"
}

printf '\033[1mVAST S3 -> Apache Iceberg demo preflight\033[0m\n'
printf 'repository: %s\n' "$REPO_ROOT"
printf 'commit:     %s\n' "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo 'not a git checkout')"

# --------------------------------------------------------------------------- #
step "1. Environment"
# --------------------------------------------------------------------------- #

REQUIRED_VARS=(
    VAST_S3_ENDPOINT VAST_S3_ACCESS_KEY VAST_S3_SECRET_KEY VAST_S3_REGION
    VAST_SOURCE_BUCKET VAST_KAFKA_BROKER VAST_KAFKA_TOPIC VAST_KAFKA_GROUP
    ICEBERG_WAREHOUSE ICEBERG_CATALOG_URI_HOST ICEBERG_NAMESPACE ICEBERG_TABLE
)
MISSING=()
for var in "${REQUIRED_VARS[@]}"; do
    [ -n "${!var:-}" ] || MISSING+=("$var")
done

if [ ${#MISSING[@]} -eq 0 ]; then
    ok "all ${#REQUIRED_VARS[@]} required variables are set"
else
    bad "unset: ${MISSING[*]}"
    note "run: set -a; . ./docker/demo.env; set +a"
fi

for var in VAST_S3_ENDPOINT VAST_KAFKA_BROKER ICEBERG_WAREHOUSE ICEBERG_NAMESPACE ICEBERG_TABLE VAST_SOURCE_BUCKET; do
    note "$(printf '%-24s %s' "$var" "${!var:-<unset>}")"
done
note "$(printf '%-24s %s' "VAST_S3_ACCESS_KEY" "$(mask "${VAST_S3_ACCESS_KEY:-}")")"
note "$(printf '%-24s %s' "VAST_S3_SECRET_KEY" "$(mask "${VAST_S3_SECRET_KEY:-}")")"

# The warehouse must not live in the bucket being watched, or the demo would
# generate events about its own writes, forever.
WAREHOUSE_BUCKET="$(printf '%s' "${ICEBERG_WAREHOUSE:-}" | sed -E 's#^s3a?://##; s#/.*##')"
if [ -n "$WAREHOUSE_BUCKET" ] && [ "$WAREHOUSE_BUCKET" = "${VAST_SOURCE_BUCKET:-}" ]; then
    bad "ICEBERG_WAREHOUSE is inside VAST_SOURCE_BUCKET ($WAREHOUSE_BUCKET)"
    note "the warehouse writes would trigger new events, without end - use a separate bucket"
else
    [ -n "$WAREHOUSE_BUCKET" ] && ok "warehouse bucket '$WAREHOUSE_BUCKET' is separate from the watched bucket"
fi

# --------------------------------------------------------------------------- #
step "2. Python environment"
# --------------------------------------------------------------------------- #

if PYVER=$($PYTHON -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null); then
    ok "python $PYVER ($PYTHON)"
else
    bad "$PYTHON is not runnable"
fi

if KVER=$($PYTHON -c 'import confluent_kafka; print(confluent_kafka.version()[0])' 2>/dev/null); then
    # VAST documents support for the Confluent Kafka Python client 2.4 - 2.8.
    case "$KVER" in
        2.4*|2.5*|2.6*|2.7*|2.8*) ok "confluent-kafka $KVER (within VAST's supported 2.4-2.8 window)" ;;
        *) warn "confluent-kafka $KVER is outside VAST's documented 2.4-2.8 support window"
           note "reinstall with: $PYTHON -m pip install -r requirements.txt" ;;
    esac
else
    bad "confluent-kafka is not importable - pip install -r requirements.txt"
fi

if IVER=$($PYTHON -c 'import pyiceberg; print(pyiceberg.__version__)' 2>/dev/null); then
    ok "pyiceberg $IVER"
else
    bad "pyiceberg is not importable - pip install -r requirements-iceberg.txt"
fi

$PYTHON -c 'import pyarrow' 2>/dev/null && ok "pyarrow importable" || bad "pyarrow is not importable"

# --------------------------------------------------------------------------- #
step "3. Consumer configuration"
# --------------------------------------------------------------------------- #

if [ ! -f "$CONFIG" ]; then
    bad "$CONFIG not found"
    note "cp s3_consumer_config.vast-demo.example.json $CONFIG"
else
    if OUT=$($PYTHON - "$CONFIG" 2>&1 <<'PYEOF'
import sys
from pathlib import Path
sys.path.insert(0, ".")
from s3events.config import load_app_config
c = load_app_config(Path(sys.argv[1]))
print("OK", c.iceberg.table_identifier if c.iceberg else "<iceberg disabled>")
PYEOF
    ); then
        ok "$CONFIG parses and every env: reference resolves"
        note "target table: ${OUT#OK }"
    else
        bad "$CONFIG did not load"
        note "$(printf '%s' "$OUT" | tail -3)"
    fi
fi

# --------------------------------------------------------------------------- #
step "4. VAST S3 reachability"
# --------------------------------------------------------------------------- #

if [ -n "${VAST_S3_ENDPOINT:-}" ]; then
    # An S3 endpoint answering anonymously with 403 AccessDenied is healthy:
    # it proves TLS, DNS and routing all work.
    CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$VAST_S3_ENDPOINT" 2>/dev/null)
    case "$CODE" in
        200|403|400) ok "VAST S3 endpoint answered HTTP $CODE" ;;
        000) bad "could not reach $VAST_S3_ENDPOINT (DNS, routing, or TLS)"
             note "if this is HTTPS with an internal CA, see the runbook's TLS section"
             note "diagnose: curl -v --max-time 10 $VAST_S3_ENDPOINT" ;;
        *)   warn "VAST S3 endpoint answered HTTP $CODE" ;;
    esac

    # Signed request against both buckets, using the same credentials the demo
    # will use. This is the check that actually proves the keys work.
    if $PYTHON -c 'import boto3' 2>/dev/null; then
        bucket="${VAST_SOURCE_BUCKET:-}"
        if [ -n "$bucket" ]; then
            if $PYTHON - "$bucket" >/dev/null 2>&1 <<'PYEOF'
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
s3.head_bucket(Bucket=sys.argv[1])
PYEOF
            then
                ok "authenticated to VAST S3 bucket '$bucket'"
            else
                bad "could not access VAST S3 bucket '$bucket' with the supplied keys"
                note "check the key pair, the bucket name, and the user's permissions"
            fi
        fi
        if [ -n "$WAREHOUSE_BUCKET" ]; then
            if $PYTHON - "$WAREHOUSE_BUCKET" >/dev/null 2>&1 <<'PYEOF'
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
s3.head_bucket(Bucket=sys.argv[1])
PYEOF
            then
                ok "authenticated to Iceberg warehouse bucket '$WAREHOUSE_BUCKET'"
            else
                bad "could not access warehouse bucket '$WAREHOUSE_BUCKET'"
            fi
        fi
    else
        warn "boto3 not installed - skipped the authenticated VAST S3 checks"
        note "pip install boto3 for a much stronger preflight"
    fi
fi

# --------------------------------------------------------------------------- #
step "5. VAST Kafka-compatible Event Broker"
# --------------------------------------------------------------------------- #

if [ -n "${VAST_KAFKA_BROKER:-}" ]; then
    REACHED=0
    IFS=',' read -ra BROKERS <<< "$VAST_KAFKA_BROKER"
    for broker in "${BROKERS[@]}"; do
        host="${broker%:*}"; port="${broker##*:}"
        if [ "$host" = "$port" ]; then
            warn "no port in '$broker' - VAST does not document a default; set it explicitly"
            continue
        fi
        if command -v nc >/dev/null 2>&1 && nc -z -w 5 "$host" "$port" 2>/dev/null; then
            ok "TCP reachable: $broker"; REACHED=$((REACHED + 1))
        else
            bad "TCP unreachable: $broker"
        fi
    done

    # Ask the broker for metadata, and confirm the topic exists. VAST does not
    # auto-create topics, so a missing topic is a hard stop.
    if [ "$REACHED" -gt 0 ] && $PYTHON -c 'import confluent_kafka' 2>/dev/null; then
        if TOPICS=$($PYTHON - 2>/dev/null <<'PYEOF'
import os
from confluent_kafka.admin import AdminClient
admin = AdminClient({"bootstrap.servers": os.environ["VAST_KAFKA_BROKER"]})
md = admin.list_topics(timeout=15)
print("\n".join(sorted(md.topics)))
PYEOF
        ); then
            ok "Event Broker answered a metadata request"
            if printf '%s\n' "$TOPICS" | grep -qx -- "${VAST_KAFKA_TOPIC:-}"; then
                ok "topic '$VAST_KAFKA_TOPIC' exists"
            else
                bad "topic '$VAST_KAFKA_TOPIC' does not exist on the broker"
                note "VAST does not auto-create topics - create it in the VAST UI or CLI"
                note "topics seen: $(printf '%s' "$TOPICS" | tr '\n' ' ' | cut -c1-160)"
            fi
        else
            bad "the Event Broker did not answer a metadata request"
            note "if the broker requires SASL, add security.protocol and sasl.* to kafka_config"
        fi
    fi
fi

# --------------------------------------------------------------------------- #
step "6. Supporting containers"
# --------------------------------------------------------------------------- #

if ! docker info >/dev/null 2>&1; then
    bad "the Docker daemon is not running"
else
    ok "docker daemon reachable"
    for service in iceberg-rest trino sqlpad; do
        state=$($COMPOSE ps --format '{{.Service}} {{.State}}' 2>/dev/null | awk -v s="$service" '$1 == s {print $2}')
        case "$state" in
            running) ok "container '$service' is running" ;;
            "")      bad "container '$service' is not up - docker compose ... up -d --wait" ;;
            *)       bad "container '$service' is '$state'" ;;
        esac
    done
fi

# --------------------------------------------------------------------------- #
step "7. Catalog, Trino and SQLPad"
# --------------------------------------------------------------------------- #

CATALOG_URL="${ICEBERG_CATALOG_URI_HOST:-http://localhost:8181}"
if curl -fsS --max-time 10 "$CATALOG_URL/v1/config" >/dev/null 2>&1; then
    ok "Iceberg REST catalog answering at $CATALOG_URL"
else
    bad "Iceberg REST catalog not answering at $CATALOG_URL"
    note "diagnose: docker compose -f docker/docker-compose.yml logs --tail 50 iceberg-rest"
fi

if curl -fsS --max-time 10 "http://localhost:8080/v1/info" >/dev/null 2>&1; then
    STARTED=$(curl -fsS --max-time 10 "http://localhost:8080/v1/info" 2>/dev/null | grep -o '"starting":[a-z]*' | cut -d: -f2)
    if [ "$STARTED" = "false" ]; then
        ok "Trino is up and finished starting"
    else
        warn "Trino is still starting - wait for it before querying"
    fi
else
    bad "Trino not answering on :8080"
    note "diagnose: docker compose -f docker/docker-compose.yml logs --tail 50 trino"
fi

if curl -fsS --max-time 10 -o /dev/null "http://localhost:3000" 2>/dev/null; then
    ok "SQLPad answering on :3000"
else
    bad "SQLPad not answering on :3000"
fi

# Prove Trino can actually reach the catalog, not merely that it is running.
if $COMPOSE exec -T trino trino --execute "SHOW SCHEMAS FROM iceberg" >/dev/null 2>&1; then
    ok "Trino can read the Iceberg catalog"
else
    bad "Trino cannot read the Iceberg catalog"
    note "diagnose: $COMPOSE exec -T trino trino --execute 'SHOW SCHEMAS FROM iceberg'"
fi

# --------------------------------------------------------------------------- #
step "8. Iceberg table"
# --------------------------------------------------------------------------- #

if [ "$TABLE_INIT" -eq 1 ]; then
    note "this step CREATES the namespace and table if they are missing"
    if $PYTHON s3_event_consumer.py --config "$CONFIG" --check >/dev/null 2>&1; then
        ok "Iceberg table ${ICEBERG_NAMESPACE:-?}.${ICEBERG_TABLE:-?} initialises"
    else
        bad "Iceberg table initialisation failed"
        note "diagnose: $PYTHON s3_event_consumer.py --config $CONFIG --check"
    fi

    if ROWS=$($COMPOSE exec -T trino trino --execute \
        "SELECT count(*) FROM iceberg.${ICEBERG_NAMESPACE}.${ICEBERG_TABLE}" \
        --output-format=TSV 2>/dev/null | tr -d '"' | tail -1); then
        [ -n "$ROWS" ] && ok "table is queryable from Trino - $ROWS row(s) now" \
                       || warn "could not read the row count from Trino"
    fi
else
    note "skipped (--no-table-init)"
fi

# --------------------------------------------------------------------------- #
printf '\n\033[1m== Result\033[0m\n'
printf '  %d passed, %d warning(s), %d failure(s)\n' "$PASS" "$WARN" "$FAIL"

if [ "$FAIL" -gt 0 ]; then
    printf '\n\033[31mNOT READY.\033[0m Fix the failures above before the customer arrives.\n'
    exit 1
fi
if [ "$WARN" -gt 0 ]; then
    printf '\n\033[33mREADY, with warnings.\033[0m Read them before you start.\n'
    exit 0
fi
printf '\n\033[32mREADY.\033[0m Every check passed.\n'
