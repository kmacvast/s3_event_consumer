#!/usr/bin/env python3
"""Consume VAST S3 event notifications from a Kafka-compatible Event Broker.

Subscribes to a single Kafka topic, decodes each message as JSON and hands the
decoded event to every configured *sink*:

    Kafka -> decode JSON -> console sink   (always on, unchanged behaviour)
                         -> Iceberg sink   (optional, off unless configured)

Operational messages (startup banner, warnings, errors) go to stderr through the
standard ``logging`` module; event payloads go to stdout, so the payload stream
can be piped or redirected on its own.

With no ``iceberg`` section in the configuration file — or with that section
disabled — this behaves exactly as it did before Iceberg support existed,
including librdkafka's own automatic offset commits.

When the Iceberg sink *is* enabled, automatic offset commits are turned off and
offsets are committed by hand, only after the corresponding records have been
appended to Iceberg. That makes delivery **at-least-once**: nothing is
acknowledged to Kafka that is not already in the table, but a failure between
the Iceberg commit and the offset commit replays those records and duplicates
them. There is no deduplication yet.

Usage:
    python3 s3_event_consumer.py [--config PATH] [--no-color] [--no-iceberg] [--check]
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from s3events.config import (
    DEFAULT_CONFIG_FILENAME,
    EXAMPLE_CONFIG_FILENAME,
    AppConfig,
    ConfigError,
    IcebergConfig,
    load_app_config,
    load_config,
)
from s3events.flatten import flatten_event
from s3events.sinks import (
    ConsoleSink,
    EventDispatcher,
    SinkError,
    SinkFatalError,
    render_event,
    use_color,
)

LOG = logging.getLogger("s3_event_consumer")
# librdkafka's own log output is routed here instead of being written straight
# to stderr by the C library.
KAFKA_LOG = LOG.getChild("librdkafka")

# How long consumer.poll() blocks before returning control to the main loop.
# This is also the worst-case delay between Ctrl-C and the process exiting, and
# the granularity at which a buffering sink's flush interval is checked.
POLL_TIMEOUT_SECONDS = 1.0

# Longest raw payload echoed into a warning about an undecodable message.
MAX_LOGGED_PAYLOAD = 500

# Re-exported so the names this module has always exposed keep working:
# load_config, render_event, use_color, ConfigError, and the config filenames.
__all__ = [
    "DEFAULT_CONFIG_FILENAME",
    "EXAMPLE_CONFIG_FILENAME",
    "ConfigError",
    "LOG",
    "build_dispatcher",
    "consume",
    "make_offset_committer",
    "load_app_config",
    "load_config",
    "main",
    "render_event",
    "show_event",
    "use_color",
]


# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #

_broker_down_reported = False
_interrupted = False


def request_stop(signum: int, frame: Any) -> None:
    """Ctrl-C handler: ask the poll loop to finish.

    A KeyboardInterrupt raised while blocked inside consumer.poll() surfaces as
    an opaque ``SystemError`` on Python older than 3.12, so set a flag and let
    the loop exit after its current poll instead of raising through the C call.
    """
    global _interrupted
    _interrupted = True


def log_kafka_error(error: KafkaError) -> None:
    """Report the first 'no broker reachable' event.

    librdkafka calls this many times a second while a broker is unreachable, and
    already logs the underlying connection error through KAFKA_LOG. This adds one
    human-readable summary the first time it happens, then stays quiet.
    """
    global _broker_down_reported
    if error.code() == KafkaError._ALL_BROKERS_DOWN and not _broker_down_reported:
        _broker_down_reported = True
        LOG.error(
            "No Kafka broker reachable. Check 'bootstrap.servers', that the Event "
            "Broker VIP pool is reachable from this host, and any firewall rules."
        )


def consume(consumer: Consumer, color: bool, dispatcher: EventDispatcher | None = None) -> None:
    """Poll the subscribed topic and dispatch each event, until interrupted.

    ``color`` is honoured only when no dispatcher is supplied, in which case a
    console-only dispatcher is built — that is the original signature, kept so
    existing callers still work.
    """
    if dispatcher is None:
        dispatcher = EventDispatcher([ConsoleSink(color)])

    while not _interrupted:
        message = consumer.poll(timeout=POLL_TIMEOUT_SECONDS)

        # Before handling the message, and on every idle poll, so a buffering
        # sink can flush on elapsed time even when the topic goes quiet.
        dispatcher.tick()

        if message is None:
            continue

        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            if error.fatal():
                raise KafkaException(error)
            LOG.error("Kafka error while consuming: %s", error.str())
            continue

        handle_message(message, dispatcher)


def decode_message(message: Any) -> tuple[Any, str] | None:
    """Decode one Kafka message payload as JSON.

    Returns:
        (event, raw_payload), or None when there is nothing usable to dispatch.
        Never raises, whatever the broker delivers.
    """
    where = f"{message.topic()}[{message.partition()}]@{message.offset()}"

    payload = message.value()
    if payload is None:
        LOG.warning("Ignoring message with no payload (%s).", where)
        return None

    raw = payload.decode("utf-8", errors="replace")
    try:
        event = json.loads(raw)
    except json.JSONDecodeError as exc:
        LOG.warning(
            "Message payload is not valid JSON (%s): %s. Raw payload: %s",
            where,
            exc.msg,
            raw[:MAX_LOGGED_PAYLOAD],
        )
        return None

    LOG.info("Event received (%s, %d bytes)", where, len(payload))
    return event, raw


def handle_message(message: Any, dispatcher: EventDispatcher) -> None:
    """Decode one Kafka message and send it to every sink."""
    decoded = decode_message(message)
    if decoded is None:
        return

    event, raw = decoded
    rows = flatten_event(
        event,
        raw,
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
    )
    dispatcher.dispatch(event, rows, raw)


def show_event(message: Any, color: bool) -> None:
    """Decode one Kafka message and print its event payload.

    The original single-message display path, kept as-is for callers that only
    want the console behaviour.
    """
    decoded = decode_message(message)
    if decoded is None:
        return
    print(render_event(decoded[0], color), end="\n\n", flush=True)


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #


def make_offset_committer(consumer: Consumer) -> Any:
    """Return a callable that commits the given Kafka offsets synchronously.

    Handed to the Iceberg sink, which calls it only after a batch has been
    appended to the table. Offsets arrive as ``{(topic, partition): next_offset}``
    — Kafka records the offset to *resume from*, which is one past the last
    message written.
    """

    def commit(offsets: dict[tuple[str, int], int]) -> None:
        partitions = [
            TopicPartition(topic, partition, next_offset)
            for (topic, partition), next_offset in sorted(offsets.items())
        ]
        consumer.commit(offsets=partitions, asynchronous=False)

    return commit


def build_dispatcher(
    config: AppConfig, color: bool, offset_committer: Any = None
) -> EventDispatcher:
    """Build the sink chain: console always, Iceberg when configured.

    ``offset_committer`` is passed to the Iceberg sink so it can acknowledge
    Kafka only after a successful append. Omitting it — as ``--check`` does —
    means nothing is ever committed.
    """
    sinks: list[Any] = [ConsoleSink(color)]

    if config.iceberg is not None:
        # Imported here rather than at module scope so a consumer running
        # without Iceberg never pays for the import, and the standalone
        # executable does not bundle PyIceberg. See sinks/iceberg.py.
        from s3events.sinks.iceberg import IcebergSink

        sinks.append(IcebergSink(config.iceberg, offset_committer=offset_committer))

    return EventDispatcher(sinks)


def apply_manual_offset_commits(kafka_conf: dict[str, Any]) -> dict[str, Any]:
    """Turn off librdkafka's automatic offset commits, for the Iceberg path.

    Automatic commits advance the read position on a timer, with no relation to
    whether the records reached Iceberg — exactly the acknowledge-then-lose
    window this avoids. Only called when the Iceberg sink is enabled; the
    console-only path keeps librdkafka's defaults untouched.
    """
    conf = dict(kafka_conf)
    existing = conf.get("enable.auto.commit")
    if str(existing).lower() in ("true", "1") if existing is not None else False:
        LOG.warning(
            "Overriding 'kafka_config.enable.auto.commit': the Iceberg sink commits "
            "offsets itself, only after records are durable in the table."
        )
    conf["enable.auto.commit"] = False
    return conf


def log_iceberg_settings(iceberg: IcebergConfig) -> None:
    """Report the Iceberg configuration, with credentials redacted."""
    LOG.info("Iceberg table:      %s", iceberg.table_identifier)
    LOG.info("Iceberg catalog:    %s", iceberg.catalog_properties.get("uri", "<no uri>"))
    LOG.info(
        "Iceberg batching:   up to %d record(s) or %.1fs, whichever comes first",
        iceberg.batch_size,
        iceberg.flush_interval_seconds,
    )
    LOG.debug("Iceberg catalog properties: %s", iceberg.safe_properties())


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume VAST S3 event notifications from a Kafka-compatible Event Broker.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(DEFAULT_CONFIG_FILENAME),
        metavar="PATH",
        help=f"path to the JSON configuration file (default: {DEFAULT_CONFIG_FILENAME})",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="disable colourised output (also honours the NO_COLOR environment variable)",
    )
    parser.add_argument(
        "--no-iceberg",
        action="store_true",
        help="ignore the 'iceberg' section and run console-only, whatever the config says",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the configuration, open every sink, then exit without consuming",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    # PyIceberg, botocore and urllib3 are chatty at INFO. Keep our own Iceberg
    # messages — table creation, commits — without their per-request noise.
    for noisy in ("pyiceberg", "botocore", "boto3", "urllib3", "s3fs", "fsspec", "aiobotocore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    try:
        config = load_app_config(args.config)
    except ConfigError as exc:
        LOG.error("%s", exc)
        return 1

    if args.no_iceberg and config.iceberg is not None:
        LOG.info("Iceberg sink disabled by --no-iceberg.")
        config = AppConfig(
            kafka_config=config.kafka_config, topic=config.topic, iceberg=None, source=config.source
        )

    color = use_color(args.no_color)

    LOG.info("Configuration:      %s", args.config)
    LOG.info("Bootstrap servers:  %s", config.kafka_config["bootstrap.servers"])
    LOG.info("Consumer group:     %s", config.kafka_config["group.id"])
    LOG.info("Topic:              %s", config.topic)
    if config.iceberg is not None:
        log_iceberg_settings(config.iceberg)

    if args.check:
        return run_check(config, color)

    kafka_conf = dict(config.kafka_config)
    if config.iceberg is not None:
        # Offsets are committed by hand, after each Iceberg append. Untouched
        # when the Iceberg sink is off, so the console-only path keeps exactly
        # the librdkafka behaviour it always had.
        kafka_conf = apply_manual_offset_commits(kafka_conf)
    kafka_conf["error_cb"] = log_kafka_error
    kafka_conf["logger"] = KAFKA_LOG

    try:
        consumer = Consumer(kafka_conf)
    except (KafkaException, ValueError, TypeError) as exc:
        LOG.error("Could not create the Kafka consumer: %s", exc)
        return 1

    # The consumer exists but has not subscribed, so nothing has joined a
    # consumer group yet: an unreachable catalog is still reported before any
    # Kafka state is touched.
    committer = make_offset_committer(consumer) if config.iceberg is not None else None
    dispatcher = build_dispatcher(config, color, committer)

    try:
        dispatcher.open()
    except SinkError as exc:
        LOG.error("Cannot start: %s", exc)
        consumer.close()
        return 1

    signal.signal(signal.SIGINT, request_stop)

    exit_code = 0
    try:
        consumer.subscribe([config.topic])
        LOG.info("Subscribed to '%s'. Waiting for events (Ctrl-C to stop).", config.topic)
        consume(consumer, color, dispatcher)
        LOG.info("Interrupted, shutting down.")
    except SinkFatalError as exc:
        # The sink still holds the records and their offsets were never
        # committed, so they stay on the topic for the next run.
        LOG.error("Stopping: %s", exc)
        exit_code = 1
    except KafkaException as exc:
        LOG.error("Fatal Kafka error: %s", exc)
        exit_code = 1
    finally:
        # Sinks first: the final flush must reach Iceberg, and its offsets must
        # be committed, before the Kafka consumer goes away.
        if not dispatcher.close():
            exit_code = 1
        LOG.info("Closing Kafka consumer.")
        consumer.close()

    return exit_code


def run_check(config: AppConfig, color: bool) -> int:
    """Validate the configuration and open every sink, then stop.

    No Kafka consumer is created, so no offsets can be committed and no consumer
    group is joined.
    """
    dispatcher = build_dispatcher(config, color)
    try:
        dispatcher.open()
    except SinkError as exc:
        LOG.error("Cannot start: %s", exc)
        return 1

    LOG.info("Configuration and sinks are usable. Exiting because --check was given.")
    return 0 if dispatcher.close() else 1


if __name__ == "__main__":
    sys.exit(main())
