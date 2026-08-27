#!/usr/bin/env python3
"""Consume VAST S3 event notifications from a Kafka-compatible Event Broker.

Subscribes to a single Kafka topic, decodes each message as JSON and
pretty-prints the event payload so an S3 event notification is easy to read
during a live demonstration.

Operational messages (startup banner, warnings, errors) go to stderr through the
standard ``logging`` module; event payloads go to stdout, so the payload stream
can be piped or redirected on its own.

Usage:
    python3 s3_event_consumer.py [--config PATH] [--no-color]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import JsonLexer

LOG = logging.getLogger("s3_event_consumer")
# librdkafka's own log output is routed here instead of being written straight
# to stderr by the C library.
KAFKA_LOG = LOG.getChild("librdkafka")

DEFAULT_CONFIG_FILENAME = "s3_consumer_config.json"
EXAMPLE_CONFIG_FILENAME = "s3_consumer_config.example.json"

# How long consumer.poll() blocks before returning control to the main loop.
# This is also the worst-case delay between Ctrl-C and the process exiting.
POLL_TIMEOUT_SECONDS = 1.0

# Longest raw payload echoed into a warning about an undecodable message.
MAX_LOGGED_PAYLOAD = 500


class ConfigError(Exception):
    """Raised when the configuration file is missing, malformed or incomplete."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


def load_config(path: Path) -> tuple[dict[str, Any], str]:
    """Read and validate the JSON configuration file.

    Returns:
        (kafka_conf, topic) — the librdkafka settings and the topic to consume.

    Raises:
        ConfigError: with a message that says how to fix the problem.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError(
            f"Configuration file not found: {path}\n"
            f"Copy {EXAMPLE_CONFIG_FILENAME} to {DEFAULT_CONFIG_FILENAME} and "
            f"edit it for your environment."
        ) from None

    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"{path} is not valid JSON: {exc.msg} (line {exc.lineno}, column {exc.colno})"
        ) from None

    if not isinstance(document, dict):
        raise ConfigError(f"{path} must contain a JSON object at the top level.")

    kafka_conf = document.get("kafka_config")
    if not isinstance(kafka_conf, dict) or not kafka_conf:
        raise ConfigError(
            f"{path}: 'kafka_config' must be a non-empty JSON object of librdkafka "
            f"settings (see {EXAMPLE_CONFIG_FILENAME})."
        )

    topic = document.get("topic")
    if not isinstance(topic, str) or not topic.strip():
        raise ConfigError(
            f"{path}: 'topic' must be a non-empty string naming the Kafka topic to consume."
        )

    for key in ("bootstrap.servers", "group.id"):
        value = kafka_conf.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f"{path}: 'kafka_config.{key}' must be a non-empty string.")

    # Angle brackets are never valid in a host:port (IPv6 uses square brackets),
    # so their presence means the shipped placeholders were never replaced.
    servers = kafka_conf["bootstrap.servers"]
    if "<" in servers or ">" in servers:
        raise ConfigError(
            f"{path}: 'kafka_config.bootstrap.servers' still contains placeholders "
            f"({servers!r}). Replace them with the endpoints and port of your VAST "
            f"Kafka Event Broker."
        )

    return dict(kafka_conf), topic.strip()


# --------------------------------------------------------------------------- #
# Event presentation
# --------------------------------------------------------------------------- #


def use_color(no_color: bool) -> bool:
    """Colourise only on an interactive terminal, unless explicitly disabled."""
    if no_color or os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


def render_event(event: Any, color: bool = False) -> str:
    """Return the event as pretty-printed JSON, syntax-highlighted when wanted."""
    pretty = json.dumps(event, indent=2)
    if not color:
        return pretty
    return highlight(pretty, JsonLexer(), TerminalFormatter()).strip()


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


def consume(consumer: Consumer, color: bool) -> None:
    """Poll the subscribed topic and display each event, until interrupted."""
    while not _interrupted:
        message = consumer.poll(timeout=POLL_TIMEOUT_SECONDS)
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

        show_event(message, color)


def show_event(message: Any, color: bool) -> None:
    """Decode one Kafka message and print its event payload."""
    where = f"{message.topic()}[{message.partition()}]@{message.offset()}"

    payload = message.value()
    if payload is None:
        LOG.warning("Ignoring message with no payload (%s).", where)
        return

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
        return

    LOG.info("Event received (%s, %d bytes)", where, len(payload))
    print(render_event(event, color), end="\n\n", flush=True)


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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    try:
        kafka_conf, topic = load_config(args.config)
    except ConfigError as exc:
        LOG.error("%s", exc)
        return 1

    kafka_conf["error_cb"] = log_kafka_error
    kafka_conf["logger"] = KAFKA_LOG

    LOG.info("Configuration:      %s", args.config)
    LOG.info("Bootstrap servers:  %s", kafka_conf["bootstrap.servers"])
    LOG.info("Consumer group:     %s", kafka_conf["group.id"])
    LOG.info("Topic:              %s", topic)

    try:
        consumer = Consumer(kafka_conf)
    except (KafkaException, ValueError, TypeError) as exc:
        LOG.error("Could not create the Kafka consumer: %s", exc)
        return 1

    signal.signal(signal.SIGINT, request_stop)

    try:
        consumer.subscribe([topic])
        LOG.info("Subscribed to '%s'. Waiting for events (Ctrl-C to stop).", topic)
        consume(consumer, use_color(args.no_color))
        LOG.info("Interrupted, shutting down.")
    except KafkaException as exc:
        LOG.error("Fatal Kafka error: %s", exc)
        return 1
    finally:
        LOG.info("Closing Kafka consumer.")
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
