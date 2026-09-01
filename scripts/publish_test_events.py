#!/usr/bin/env python3
"""Publish synthetic VAST-shaped S3 event notifications to a Kafka topic.

Stands in for "PUT an object into a watched VAST bucket" so the Iceberg demo can
be driven end to end on a laptop, against the local Kafka in
``docker/docker-compose.yml``. Against a real cluster you would write objects to
the bucket instead and let VAST publish the events.

    python3 scripts/publish_test_events.py --count 50

Some events are deliberately incomplete — missing ``size``, missing ``eTag``, an
unparseable ``eventTime``, an empty ``Records`` list, a payload that is not an
S3 event at all — because the point of the flattening code is that it copes.
Pass ``--well-formed-only`` to publish only complete events.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import sys
import time

from confluent_kafka import Producer

BUCKETS = ["demo-data", "research-raw", "telemetry"]
PREFIXES = ["ingest/", "logs/2026/08/", "images/", ""]
# VAST prefixes eventName with "s3:" and reports eventSource as "vast:s3".
# Matching the real payload here means anything validated against this
# generator is validated against the shape VAST actually publishes.
EVENT_NAMES = [
    "s3:ObjectCreated:Put",
    "s3:ObjectCreated:Post",
    "s3:ObjectCreated:CompleteMultipartUpload",
    "s3:ObjectRemoved:Delete",
]


def well_formed_event(index: int) -> dict:
    """A complete S3 event notification, the shape the demo mostly sees."""
    bucket = random.choice(BUCKETS)
    key = f"{random.choice(PREFIXES)}object-{index:05d}.dat"
    return {
        "Records": [
            {
                "eventVersion": "2.2",
                "eventSource": "vast:s3",
                "awsRegion": "us-east-1",
                "eventTime": dt.datetime.now(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "eventName": random.choice(EVENT_NAMES),
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {"name": bucket, "arn": f"arn:vast:s3:::{bucket}"},
                    "object": {
                        "key": key,
                        "size": random.randint(1, 5_000_000),
                        "eTag": f"{random.getrandbits(128):032x}",
                        "sequencer": f"{random.getrandbits(64):016X}",
                    },
                },
            }
        ]
    }


def awkward_event(index: int) -> dict:
    """One of the payload shapes the flattening code has to survive."""
    bucket = random.choice(BUCKETS)
    key = f"awkward/object-{index:05d}.dat"

    return random.choice(
        [
            # No size and no eTag.
            {
                "Records": [
                    {
                        "eventName": "s3:ObjectCreated:Put",
                        "eventTime": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "s3": {"bucket": {"name": bucket}, "object": {"key": key}},
                    }
                ]
            },
            # Unparseable event time, and a size sent as a string.
            {
                "Records": [
                    {
                        "eventName": "s3:ObjectCreated:Put",
                        "eventTime": "not a timestamp",
                        "s3": {
                            "bucket": {"name": bucket},
                            "object": {"key": key, "size": "4096"},
                        },
                    }
                ]
            },
            # Two records in one message.
            {
                "Records": [
                    {
                        "eventName": "s3:ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": bucket},
                            "object": {"key": f"{key}.a", "size": 11},
                        },
                    },
                    {
                        "eventName": "s3:ObjectRemoved:Delete",
                        "s3": {"bucket": {"name": bucket}, "object": {"key": f"{key}.b"}},
                    },
                ]
            },
            # A key needing URL-decoding.
            {
                "Records": [
                    {
                        "eventName": "s3:ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": bucket},
                            "object": {"key": "reports/quarterly+report%20final.pdf", "size": 9},
                        },
                    }
                ]
            },
            # The connectivity test event VAST fires when a notification is
            # saved. Deliberately not the Records envelope.
            {
                "Service": "Vast S3",
                "Event": "s3:TestEvent",
                "Time": dt.datetime.now(dt.timezone.utc).isoformat(),
                "Bucket": bucket,
                "RequestId": f"{random.getrandbits(64):016X}",
                "HostId": f"{random.getrandbits(64):016X}",
            },
            # An explicitly empty record list.
            {"Records": []},
            # Not an S3 event notification at all.
            {"hello": "world", "index": index},
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="s3-events")
    parser.add_argument("--count", type=int, default=20, help="how many messages to publish")
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="seconds to wait between messages (default: 0.1)",
    )
    parser.add_argument(
        "--awkward-ratio",
        type=float,
        default=0.2,
        help="fraction of messages that use an incomplete or odd payload shape",
    )
    parser.add_argument(
        "--well-formed-only",
        action="store_true",
        help="publish only complete S3 event notifications",
    )
    parser.add_argument("--seed", type=int, help="seed the generator for reproducible payloads")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.seed is not None:
        random.seed(args.seed)

    # v4 only: the compose broker advertises "localhost", which resolves to ::1
    # first on macOS, and librdkafka logs a failed IPv6 attempt before falling
    # back. Nothing is broken by it, but it looks like an error in a demo.
    producer = Producer(
        {"bootstrap.servers": args.bootstrap_servers, "broker.address.family": "v4"}
    )
    ratio = 0.0 if args.well_formed_only else args.awkward_ratio

    delivered = 0
    failed = 0

    def on_delivery(err, msg):
        nonlocal delivered, failed
        if err is None:
            delivered += 1
        else:
            failed += 1
            print(f"delivery failed: {err}", file=sys.stderr)

    for index in range(args.count):
        event = awkward_event(index) if random.random() < ratio else well_formed_event(index)
        producer.produce(
            args.topic,
            value=json.dumps(event).encode("utf-8"),
            callback=on_delivery,
        )
        producer.poll(0)
        if args.interval > 0:
            time.sleep(args.interval)

    producer.flush(30)
    print(
        f"published {delivered} message(s) to '{args.topic}' on {args.bootstrap_servers}"
        + (f", {failed} failed" if failed else "")
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
