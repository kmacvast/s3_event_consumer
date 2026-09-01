#!/bin/bash
# Render the Iceberg catalog properties from the environment, then hand off to
# Trino's own entrypoint.
#
# Trino reads catalog properties from files, and those files would otherwise
# have to contain the VAST endpoint and access keys. Rendering them here, into
# the container's own filesystem at start-up, keeps every VAST-specific value
# in the environment and out of the repository.
set -euo pipefail

TEMPLATE=/templates/iceberg.properties.template
TARGET=/etc/trino/catalog/iceberg.properties

for required in ICEBERG_CATALOG_URI ICEBERG_WAREHOUSE VAST_S3_ENDPOINT VAST_S3_ACCESS_KEY VAST_S3_SECRET_KEY; do
    if [ -z "${!required:-}" ]; then
        echo "trino entrypoint: $required is not set. See docker/demo.env.example." >&2
        exit 1
    fi
done

mkdir -p "$(dirname "$TARGET")"
# envsubst is not in the image; expand only the variables we know about so a
# literal $ anywhere else in the template survives untouched.
python3 - "$TEMPLATE" "$TARGET" <<'PYEOF'
import os, re, sys
template, target = sys.argv[1], sys.argv[2]
allowed = (
    "ICEBERG_CATALOG_URI", "ICEBERG_WAREHOUSE",
    "VAST_S3_ENDPOINT", "VAST_S3_REGION",
    "VAST_S3_ACCESS_KEY", "VAST_S3_SECRET_KEY",
)
text = open(template, encoding="utf-8").read()
for name in allowed:
    text = text.replace("${%s}" % name, os.environ.get(name, ""))
leftover = re.findall(r"\$\{([A-Z_]+)\}", text)
if leftover:
    sys.exit("trino entrypoint: unexpanded variables in template: %s" % ", ".join(sorted(set(leftover))))
open(target, "w", encoding="utf-8").write(text)
PYEOF

echo "trino entrypoint: rendered $TARGET for ${VAST_S3_ENDPOINT}"
exec /usr/lib/trino/bin/run-trino
