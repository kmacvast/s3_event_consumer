#!/usr/bin/env python3
"""Delete and recreate one VAST Event Broker topic via the VMS Python SDK.

The Kafka log is what ``scripts/demo_watch.py`` reports as KAFKA EVENTS:
retained messages, not the current object count. ``scripts/demo_reset.sh``
drops the Iceberg table and the consumer group. It does not truncate that
log. VAST has no auto topic create, so the only way to empty it is to delete
the topic and create it again with the same name.

    python3 -m pip install vastpy
    set -a; . ./docker/demo.env; set +a
    python3 scripts/demo_recreate_topic.py              # dry run
    python3 scripts/demo_recreate_topic.py --confirm    # apply

Stop the demo consumer first. A live member can block consumer-group
deletion, and a subscriber can interfere with topic deletion.

After recreate, re-save the source bucket notification in VMS. The topic
name is the same, but a freshly created topic has an empty log, and saving
the notification publishes VAST's connectivity test event onto it.

This does not drop Iceberg, and it does not delete S3 objects.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable

DEFAULT_PARTITIONS = 1
DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
MIN_RETENTION_MS = 6 * 60 * 60 * 1000
DEFAULT_TIMEOUT = 60.0
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

OK = "\033[32mok\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WOULD = "\033[33mWOULD\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"
YELLOW = "\033[33m"
GREEN = "\033[32m"


@dataclass(frozen=True)
class TopicSpec:
    database_name: str
    name: str
    topic_partitions: int
    retention_ms: int | None = None
    schema_name: str | None = None
    message_timestamp_type: str | None = None


@dataclass(frozen=True)
class Settings:
    address: str
    user: str | None
    password: str | None
    token: str | None
    tenant: str | None
    cert_file: str | None
    cert_server_name: str | None
    database_name: str
    topic: str
    schema_name: str | None
    partitions: int | None
    retention_ms: int | None
    confirm: bool
    keep_group: bool
    group: str
    broker: str
    timeout: float


class ScriptError(Exception):
    """Operator-facing failure; ``main`` prints the message and exits 1."""


def env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return default


def strip_vms_address(address: str) -> str:
    """vastpy prepends https://, so drop a scheme and any trailing slash."""
    value = address.strip()
    for prefix in ("https://", "http://"):
        if value.lower().startswith(prefix):
            value = value[len(prefix) :]
            break
    return value.rstrip("/")


def require_safe_name(value: str, label: str) -> str:
    text = value.strip()
    if not text or text == "*" or "/" in text or not SAFE_NAME.match(text):
        raise ScriptError(f"refusing to operate on {label} {value!r}")
    return text


def as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def first_field(record: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


def iter_topic_records(raw: Any) -> list[dict[str, Any]]:
    """Flatten list, show, and envelope responses into topic dicts."""
    if raw is None or raw == b"" or raw == "":
        return []
    if isinstance(raw, (list, tuple)):
        found: list[dict[str, Any]] = []
        for item in raw:
            found.extend(iter_topic_records(item))
        return found
    if not isinstance(raw, dict):
        return []
    for key in ("results", "data", "topics", "items"):
        if key in raw:
            return iter_topic_records(raw[key])
    if "name" in raw:
        return [raw]
    return []


def find_named_topic(records: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for record in records:
        if str(record.get("name", "")) == name:
            return record
    return None


def spec_from_record(
    record: dict[str, Any],
    *,
    database_name: str,
    name: str,
    schema_name: str | None,
    partitions: int | None,
    retention_ms: int | None,
) -> TopicSpec:
    record_partitions = as_int(
        first_field(record, "topic_partitions", "partitions", "num_partitions")
    )
    record_retention = as_int(first_field(record, "retention_ms", "retention"))
    record_schema = first_field(record, "schema_name", "schema")
    record_ts = first_field(record, "message_timestamp_type")
    chosen_partitions = partitions or record_partitions or DEFAULT_PARTITIONS
    if chosen_partitions < 1:
        raise ScriptError(f"topic_partitions must be >= 1, not {chosen_partitions}")
    chosen_retention = DEFAULT_RETENTION_MS if retention_ms is None else retention_ms
    if retention_ms is None and record_retention is not None:
        chosen_retention = record_retention
    chosen_schema = schema_name or (str(record_schema) if record_schema else None)
    chosen_ts = str(record_ts) if record_ts else None
    return TopicSpec(
        database_name=database_name,
        name=name,
        topic_partitions=chosen_partitions,
        retention_ms=chosen_retention,
        schema_name=chosen_schema,
        message_timestamp_type=chosen_ts,
    )


def default_spec(
    *,
    database_name: str,
    name: str,
    schema_name: str | None,
    partitions: int | None,
    retention_ms: int | None,
) -> TopicSpec:
    chosen_partitions = partitions or DEFAULT_PARTITIONS
    if chosen_partitions < 1:
        raise ScriptError(f"topic_partitions must be >= 1, not {chosen_partitions}")
    chosen_retention = DEFAULT_RETENTION_MS if retention_ms is None else retention_ms
    return TopicSpec(
        database_name=database_name,
        name=name,
        topic_partitions=chosen_partitions,
        retention_ms=chosen_retention,
        schema_name=schema_name,
    )


def validate_retention_ms(value: int | None) -> None:
    if value is None:
        return
    if value == -1:
        return
    if value < MIN_RETENTION_MS:
        raise ScriptError(
            f"retention-ms {value} is below the VAST minimum of {MIN_RETENTION_MS} "
            "(6 hours); pass -1 for no retention"
        )


def is_missing(exc: BaseException) -> bool:
    status = getattr(exc, "status", None)
    if status in (404,):
        return True
    message = str(exc).lower()
    needles = (
        "not found",
        "does not exist",
        "no such topic",
        "unknown topic",
        "topic not exist",
    )
    return any(needle in message for needle in needles)


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    interval: float = 0.5,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> bool:
    deadline = clock() + timeout
    while True:
        if predicate():
            return True
        if clock() >= deadline:
            return False
        sleeper(interval)


def topics_delete_endpoint(client: Any) -> Any:
    """Return the VMS ``/topics/delete/`` resource.

    vastpy binds ``.delete`` as HTTP DELETE on the current path. The topics
    API is ``DELETE /topics/delete/`` with a JSON body, so the ``delete``
    path segment has to be indexed, not called as a method.
    """
    return client.topics["delete"]


def list_topics(client: Any, database_name: str, name: str | None = None) -> list[dict[str, Any]]:
    kwargs: dict[str, Any] = {"database_name": database_name, "page_size": 1000}
    if name:
        kwargs["name"] = name
    return iter_topic_records(client.topics.get(**kwargs))


def show_topic(client: Any, database_name: str, name: str) -> dict[str, Any] | None:
    try:
        records = iter_topic_records(
            client.topics.show.get(database_name=database_name, name=name)
        )
    except Exception as exc:  # noqa: BLE001
        if is_missing(exc):
            return None
        raise
    return find_named_topic(records, name) or (records[0] if records else None)


def load_existing_topic(client: Any, database_name: str, name: str) -> dict[str, Any] | None:
    shown = show_topic(client, database_name, name)
    if shown is not None:
        return shown
    return find_named_topic(list_topics(client, database_name, name), name) or find_named_topic(
        list_topics(client, database_name), name
    )


def delete_topic(client: Any, spec: TopicSpec) -> None:
    body: dict[str, Any] = {"database_name": spec.database_name, "name": spec.name}
    if spec.schema_name:
        body["schema_name"] = spec.schema_name
    try:
        topics_delete_endpoint(client).delete(**body)
    except Exception as exc:  # noqa: BLE001
        if is_missing(exc):
            return
        message = str(exc).lower()
        if not spec.schema_name and "schema" in message:
            raise ScriptError(
                f"topic delete was rejected ({exc}). Set VAST_KAFKA_SCHEMA to the "
                "schema under the Event Broker database (shown in the UI as "
                "Kafka-Compatible Broker Topics)"
            ) from exc
        raise


def create_topic(client: Any, spec: TopicSpec) -> None:
    body: dict[str, Any] = {
        "database_name": spec.database_name,
        "name": spec.name,
        "topic_partitions": spec.topic_partitions,
    }
    if spec.retention_ms is not None:
        body["retention_ms"] = spec.retention_ms
    if spec.message_timestamp_type:
        body["message_timestamp_type"] = spec.message_timestamp_type
    client.topics.post(**body)


def delete_consumer_group(broker: str, group: str) -> str:
    """Delete a Kafka consumer group. Returns 'deleted', 'absent', or raises."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": broker})
    futures = admin.delete_consumer_groups([group], request_timeout=30)
    try:
        futures[group].result()
    except Exception as exc:  # noqa: BLE001
        text = str(exc).upper()
        if "UNKNOWN_GROUP" in text or "GROUP_ID_NOT_FOUND" in text:
            return "absent"
        raise
    return "deleted"


def open_vms_client(settings: Settings) -> Any:
    try:
        from vastpy import VASTClient
    except ImportError as exc:
        raise ScriptError(
            "vastpy is not installed. Install the VAST VMS SDK:\n"
            "  python3 -m pip install vastpy"
        ) from exc
    kwargs: dict[str, Any] = {"address": settings.address}
    if settings.token:
        kwargs["token"] = settings.token
    else:
        kwargs["user"] = settings.user
        kwargs["password"] = settings.password
    if settings.tenant:
        kwargs["tenant"] = settings.tenant
    if settings.cert_file:
        kwargs["cert_file"] = settings.cert_file
    if settings.cert_server_name:
        kwargs["cert_server_name"] = settings.cert_server_name
    return VASTClient(**kwargs)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete and recreate one VAST Event Broker topic via vastpy "
            "(VMS REST). Dry run unless --confirm is passed."
        )
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="actually delete and recreate the topic (default: dry run)",
    )
    parser.add_argument(
        "--keep-group",
        action="store_true",
        help="do not delete VAST_KAFKA_GROUP after recreating the topic",
    )
    parser.add_argument(
        "--database",
        default="",
        metavar="NAME",
        help="Event Broker database (default: VAST_KAFKA_DATABASE)",
    )
    parser.add_argument(
        "--topic",
        default="",
        metavar="NAME",
        help="topic name (default: VAST_KAFKA_TOPIC)",
    )
    parser.add_argument(
        "--schema",
        default="",
        metavar="NAME",
        help="optional schema_name for DELETE (default: VAST_KAFKA_SCHEMA)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        metavar="N",
        help="partition count for the new topic (default: copy existing, else 1)",
    )
    parser.add_argument(
        "--retention-ms",
        type=int,
        default=None,
        metavar="MS",
        help="retention in milliseconds (default: copy existing, else 7 days). -1 means none",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        metavar="SECONDS",
        help=f"how long to wait for delete/create to become visible (default: {DEFAULT_TIMEOUT:g})",
    )
    parser.add_argument("--address", default="", help="VMS address (default: VAST_VMS_ADDRESS or VMS_ADDRESS)")
    parser.add_argument("--user", default="", help="VMS user (default: VAST_VMS_USER or VMS_USER)")
    parser.add_argument("--password", default="", help="VMS password (default: VAST_VMS_PASSWORD or VMS_PASSWORD)")
    parser.add_argument("--token", default="", help="VMS API token (default: VAST_VMS_TOKEN or VMS_TOKEN)")
    parser.add_argument("--tenant", default="", help="VMS tenant (default: VAST_VMS_TENANT or VMS_TENANT_NAME)")
    parser.add_argument("--cert-file", default="", help="optional CA bundle for VMS TLS")
    parser.add_argument("--cert-server-name", default="", help="optional TLS server name for VMS")
    return parser.parse_args(argv)


def settings_from_args(args: argparse.Namespace) -> Settings:
    address = strip_vms_address(
        args.address or env_first("VAST_VMS_ADDRESS", "VMS_ADDRESS")
    )
    token = args.token or env_first("VAST_VMS_TOKEN", "VMS_TOKEN")
    user = args.user or env_first("VAST_VMS_USER", "VMS_USER")
    password = args.password or env_first("VAST_VMS_PASSWORD", "VMS_PASSWORD")
    tenant = args.tenant or env_first("VAST_VMS_TENANT", "VMS_TENANT_NAME") or None
    cert_file = args.cert_file or env_first("VAST_VMS_CERT_FILE") or None
    cert_server_name = args.cert_server_name or env_first("VAST_VMS_CERT_SERVER_NAME") or None
    database_raw = args.database or env_first("VAST_KAFKA_DATABASE")
    if not database_raw:
        raise ScriptError(
            "VAST_KAFKA_DATABASE is not set. It is the VAST Database named after "
            "your Kafka-enabled view (DataBase -> that database -> Kafka-Compatible "
            "Broker Topics). Add it to docker/demo.env, then: "
            "set -a; . ./docker/demo.env; set +a"
        )
    topic_raw = args.topic or env_first("VAST_KAFKA_TOPIC")
    if not topic_raw:
        raise ScriptError(
            "VAST_KAFKA_TOPIC is not set. Run: set -a; . ./docker/demo.env; set +a"
        )
    database_name = require_safe_name(database_raw, "database")
    topic = require_safe_name(topic_raw, "topic")
    schema_raw = args.schema or env_first("VAST_KAFKA_SCHEMA")
    schema_name = require_safe_name(schema_raw, "schema") if schema_raw else None
    if not address:
        raise ScriptError(
            "VMS address is not set. Export VAST_VMS_ADDRESS or VMS_ADDRESS "
            "(the VMS hostname or VIP, without https://)"
        )
    if token:
        user = None
        password = None
    elif not user or not password:
        raise ScriptError(
            "VMS credentials are not set. Export VAST_VMS_TOKEN, or "
            "VAST_VMS_USER and VAST_VMS_PASSWORD (vastpy-cli names VMS_TOKEN / "
            "VMS_USER / VMS_PASSWORD also work)"
        )
    if args.partitions is not None and args.partitions < 1:
        raise ScriptError(f"--partitions must be >= 1, not {args.partitions}")
    validate_retention_ms(args.retention_ms)
    return Settings(
        address=address,
        user=user,
        password=password,
        token=token or None,
        tenant=tenant,
        cert_file=cert_file,
        cert_server_name=cert_server_name,
        database_name=database_name,
        topic=topic,
        schema_name=schema_name,
        partitions=args.partitions,
        retention_ms=args.retention_ms,
        confirm=bool(args.confirm),
        keep_group=bool(args.keep_group),
        group=env_first("VAST_KAFKA_GROUP"),
        broker=env_first("VAST_KAFKA_BROKER"),
        timeout=float(args.timeout),
    )


def ok(message: str) -> None:
    print(f"  {OK}   {message}")


def bad(message: str) -> None:
    print(f"  {FAIL} {message}", file=sys.stderr)


def note(message: str) -> None:
    print(f"       {message}")


def step(message: str) -> None:
    print(f"\n{BOLD}== {message}{RESET}")


def plan(message: str) -> None:
    print(f"  {WOULD} {message}")


def recreate_topic(
    client: Any,
    existing: dict[str, Any] | None,
    spec: TopicSpec,
    *,
    confirm: bool,
    timeout: float,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    if existing is None:
        note(f"topic {spec.database_name}/{spec.name} is not present")
        if not confirm:
            plan(
                f"create {spec.database_name}/{spec.name} "
                f"partitions={spec.topic_partitions} retention_ms={spec.retention_ms}"
            )
            return
        create_topic(client, spec)
        if timeout > 0 and not wait_until(
            lambda: load_existing_topic(client, spec.database_name, spec.name) is not None,
            timeout,
            sleeper=sleeper,
        ):
            raise ScriptError(
                f"created {spec.database_name}/{spec.name}, but it was not visible "
                f"after {timeout:g}s"
            )
        ok(f"created {spec.database_name}/{spec.name}")
        return

    note(
        f"current: partitions={spec.topic_partitions} "
        f"retention_ms={spec.retention_ms}"
        + (f" schema={spec.schema_name}" if spec.schema_name else "")
    )
    if not confirm:
        plan(f"DELETE {spec.database_name}/{spec.name}")
        plan(
            f"create {spec.database_name}/{spec.name} "
            f"partitions={spec.topic_partitions} retention_ms={spec.retention_ms}"
        )
        return

    delete_topic(client, spec)
    if timeout > 0 and not wait_until(
        lambda: load_existing_topic(client, spec.database_name, spec.name) is None,
        timeout,
        sleeper=sleeper,
    ):
        raise ScriptError(
            f"deleted {spec.database_name}/{spec.name}, but it was still visible "
            f"after {timeout:g}s"
        )
    ok(f"deleted {spec.database_name}/{spec.name}")
    create_topic(client, spec)
    if timeout > 0 and not wait_until(
        lambda: load_existing_topic(client, spec.database_name, spec.name) is not None,
        timeout,
        sleeper=sleeper,
    ):
        raise ScriptError(
            f"created {spec.database_name}/{spec.name}, but it was not visible "
            f"after {timeout:g}s"
        )
    ok(
        f"created {spec.database_name}/{spec.name} "
        f"partitions={spec.topic_partitions} retention_ms={spec.retention_ms}"
    )


def maybe_delete_group(settings: Settings) -> None:
    if settings.keep_group:
        note("skipped consumer group (--keep-group)")
        return
    if not settings.group:
        note("skipped consumer group (VAST_KAFKA_GROUP is not set)")
        return
    if not settings.broker:
        note("skipped consumer group (VAST_KAFKA_BROKER is not set)")
        return
    if not settings.confirm:
        plan(f"delete Kafka consumer group {settings.group!r} so offsets cannot skip the new log")
        return
    try:
        result = delete_consumer_group(settings.broker, settings.group)
    except Exception as exc:  # noqa: BLE001
        note(f"could not delete consumer group {settings.group!r}: {exc}")
        note("if the consumer is still running, stop it first; a live member blocks deletion")
        return
    if result == "absent":
        ok(f"consumer group {settings.group!r} did not exist")
    else:
        ok(f"deleted consumer group {settings.group!r}")


def main(argv: list[str] | None = None) -> int:
    try:
        settings = settings_from_args(parse_args(argv))
    except ScriptError as exc:
        bad(str(exc))
        return 1

    step("Checking the target is one Event Broker topic")
    ok(f"VMS            : {settings.address}")
    ok(f"database       : {settings.database_name}")
    ok(f"topic          : {settings.topic}")
    if settings.schema_name:
        note(f"schema         : {settings.schema_name}")
    note("auth           : " + ("API token" if settings.token else f"user {settings.user}"))
    note("stop the demo consumer before applying this")
    if not settings.confirm:
        print(f"\n{YELLOW}DRY RUN.{RESET} Nothing will be changed. Re-run with --confirm to apply.")

    try:
        client = open_vms_client(settings)
        step("1. Event Broker topic")
        existing = load_existing_topic(client, settings.database_name, settings.topic)
        if existing is None:
            spec = default_spec(
                database_name=settings.database_name,
                name=settings.topic,
                schema_name=settings.schema_name,
                partitions=settings.partitions,
                retention_ms=settings.retention_ms,
            )
        else:
            spec = spec_from_record(
                existing,
                database_name=settings.database_name,
                name=settings.topic,
                schema_name=settings.schema_name,
                partitions=settings.partitions,
                retention_ms=settings.retention_ms,
            )
        recreate_topic(
            client,
            existing,
            spec,
            confirm=settings.confirm,
            timeout=settings.timeout,
        )
        step("2. Kafka consumer group offsets")
        maybe_delete_group(settings)
    except ScriptError as exc:
        bad(str(exc))
        return 1
    except Exception as exc:  # noqa: BLE001
        bad(f"{type(exc).__name__}: {exc}")
        return 1

    if settings.confirm:
        print(f"\n{GREEN}Topic recreated.{RESET} The Kafka log for this topic is empty.")
        note("re-save the source bucket notification in VMS so events keep landing here")
        note("Iceberg and S3 were not touched; use scripts/demo_reset.sh for those")
    else:
        print(f"\n{YELLOW}Dry run finished.{RESET} Re-run with --confirm to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
