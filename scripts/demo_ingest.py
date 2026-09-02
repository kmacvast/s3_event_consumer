#!/usr/bin/env python3
"""Write objects into the watched VAST S3 bucket to drive a live demo.

Run this in its own terminal while the consumer is subscribed and
``scripts/demo_watch.py`` is drawing the cascade. Each PUT is a real object
in the source bucket, so VAST publishes a real S3 event onto the topic.

    set -a; . ./docker/demo.env; set +a
    python3 scripts/demo_ingest.py --count 200 --interval 0.15

Keys land under ``demo/`` by default: that is the only prefix
``scripts/demo_reset.sh --purge-source`` will delete. The body is a tiny CSV
on purpose. The point is a stream of ObjectCreated events, not a benchmark.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_PREFIX = "demo/"
DEFAULT_COUNT = 50
DEFAULT_INTERVAL = 0.2


def open_s3_client(endpoint: str, access_key: str, secret_key: str, region: str):
    from botocore.config import Config

    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    try:
        import boto3
    except ImportError:
        from botocore.session import get_session

        return get_session().create_client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=cfg,
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=cfg,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PUT objects into the watched VAST S3 bucket to drive the Iceberg demo.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        metavar="N",
        help=f"objects to write (default: {DEFAULT_COUNT}). 0 means until Ctrl-C",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        metavar="SECONDS",
        help=f"pause between PUTs (default: {DEFAULT_INTERVAL:g})",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        metavar="KEY",
        help=f"key prefix inside the source bucket (default: {DEFAULT_PREFIX})",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="override VAST_SOURCE_BUCKET",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.count < 0:
        print("error: --count must be >= 0", file=sys.stderr)
        return 2
    if args.interval < 0:
        print("error: --interval must be >= 0", file=sys.stderr)
        return 2

    bucket = args.bucket or os.environ.get("VAST_SOURCE_BUCKET", "")
    endpoint = os.environ.get("VAST_S3_ENDPOINT", "")
    access_key = os.environ.get("VAST_S3_ACCESS_KEY", "")
    secret_key = os.environ.get("VAST_S3_SECRET_KEY", "")
    region = os.environ.get("VAST_S3_REGION", "us-east-1")
    missing = [
        name
        for name, value in (
            ("VAST_SOURCE_BUCKET", bucket),
            ("VAST_S3_ENDPOINT", endpoint),
            ("VAST_S3_ACCESS_KEY", access_key),
            ("VAST_S3_SECRET_KEY", secret_key),
        )
        if not value
    ]
    if missing:
        print(
            "error: unset: "
            + ", ".join(missing)
            + ". run: set -a; . ./docker/demo.env; set +a",
            file=sys.stderr,
        )
        return 1

    prefix = args.prefix
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    if prefix in ("", "/"):
        print("error: --prefix must be a real key prefix, not empty or /", file=sys.stderr)
        return 2

    try:
        client = open_s3_client(endpoint, access_key, secret_key, region)
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not create the S3 client: {exc}", file=sys.stderr)
        return 1

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    planned = "until Ctrl-C" if args.count == 0 else str(args.count)
    print(
        f"writing {planned} object(s) to s3://{bucket}/{prefix} "
        f"every {args.interval:g}s  (Ctrl-C to stop)",
        flush=True,
    )

    written = 0
    try:
        while args.count == 0 or written < args.count:
            written += 1
            key = f"{prefix}ingest-{run_id}-{written:05d}.csv"
            body = (
                f"sensor,reading\n"
                f"demo,{written}\n"
            ).encode("utf-8")
            client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")
            print(f"  put {written:>4}  s3://{bucket}/{key}", flush=True)
            if args.interval and (args.count == 0 or written < args.count):
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nstopped after {written} object(s)", flush=True)
        return 0

    print(f"done: {written} object(s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
