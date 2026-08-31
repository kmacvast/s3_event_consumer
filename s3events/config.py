"""Configuration loading and validation.

The configuration file is a JSON object with two required keys — ``kafka_config``
and ``topic`` — and an optional ``iceberg`` section. When ``iceberg`` is absent,
or present with ``"enabled": false``, the consumer behaves exactly as it did
before the section existed.

Two entry points are offered on purpose:

``load_config``
    Returns ``(kafka_conf, topic)``. The original signature, kept so existing
    callers and tests are unaffected.
``load_app_config``
    Returns an :class:`AppConfig` that also carries the optional
    :class:`IcebergConfig`.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_FILENAME = "s3_consumer_config.json"
EXAMPLE_CONFIG_FILENAME = "s3_consumer_config.example.json"

# Catalog property values written as "env:NAME" are read from the environment at
# load time, so real access keys need never be written into a file.
ENV_REFERENCE_PREFIX = "env:"

# Catalog property names whose values must never reach a log line. Matched as a
# case-insensitive substring, so "s3.secret-access-key", "client.secret" and
# "token" are all covered.
SECRET_NAME_PATTERNS = ("secret", "password", "token", "credential", "access-key-id", "access_key_id")

REDACTED = "<redacted>"

# Defaults for the buffered Iceberg writer. One snapshot per Kafka event would
# produce an unusable metadata tree, so records accumulate until either bound is
# reached.
DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL_SECONDS = 10.0

# A failed Iceberg commit keeps its records buffered and retries, so the retry
# has to be bounded in both time and memory or a catalog outage becomes an
# unbounded hot loop. After DEFAULT_MAX_FLUSH_ATTEMPTS consecutive failures the
# sink gives up and the consumer stops non-zero, leaving the Kafka offsets
# uncommitted so the records are replayed on the next run.
DEFAULT_MAX_FLUSH_ATTEMPTS = 5
DEFAULT_RETRY_BACKOFF_SECONDS = 5.0

# Backoff doubles from retry_backoff_seconds and is capped here, so a long
# outage does not push the next attempt hours away.
MAX_RETRY_BACKOFF_SECONDS = 60.0

# Ceiling on buffered-but-unwritten records, as a multiple of batch_size, unless
# max_buffered_records says otherwise. Reaching it means events are arriving
# faster than they can be committed; the sink stops rather than dropping them.
DEFAULT_BUFFER_LIMIT_MULTIPLE = 10

DEFAULT_NAMESPACE = "s3_events"
DEFAULT_TABLE = "object_events"


class ConfigError(Exception):
    """Raised when the configuration file is missing, malformed or incomplete."""


@dataclass(frozen=True)
class IcebergConfig:
    """Validated contents of the optional ``iceberg`` configuration section."""

    namespace: str
    table: str
    catalog_name: str
    catalog_properties: dict[str, str]
    batch_size: int = DEFAULT_BATCH_SIZE
    flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS
    create_if_missing: bool = True
    max_flush_attempts: int = DEFAULT_MAX_FLUSH_ATTEMPTS
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS
    max_buffered_records: int = 0  # 0 means "derive from batch_size"

    def __post_init__(self) -> None:
        if self.max_buffered_records <= 0:
            object.__setattr__(
                self,
                "max_buffered_records",
                self.batch_size * DEFAULT_BUFFER_LIMIT_MULTIPLE,
            )

    @property
    def table_identifier(self) -> str:
        return f"{self.namespace}.{self.table}"

    def safe_properties(self) -> dict[str, str]:
        """Catalog properties with every secret value replaced by a placeholder."""
        return {key: (REDACTED if is_secret_name(key) else value) for key, value in self.catalog_properties.items()}


@dataclass(frozen=True)
class AppConfig:
    """Everything the consumer needs to run."""

    kafka_config: dict[str, Any]
    topic: str
    iceberg: IcebergConfig | None = None
    source: Path | None = field(default=None, compare=False)

    @property
    def iceberg_enabled(self) -> bool:
        return self.iceberg is not None


def is_secret_name(name: str) -> bool:
    """True when a property name suggests its value is a credential."""
    lowered = name.lower()
    return any(pattern in lowered for pattern in SECRET_NAME_PATTERNS)


def resolve_env_reference(value: str, where: str) -> str:
    """Expand an ``env:NAME`` reference, leaving any other string untouched."""
    if not value.startswith(ENV_REFERENCE_PREFIX):
        return value

    name = value[len(ENV_REFERENCE_PREFIX) :].strip()
    if not name:
        raise ConfigError(f"{where}: 'env:' reference is missing an environment variable name.")

    try:
        return os.environ[name]
    except KeyError:
        raise ConfigError(
            f"{where}: refers to environment variable {name!r}, which is not set. "
            f"Export it before starting the consumer, or replace the reference with a literal value."
        ) from None


def _read_document(path: Path) -> dict[str, Any]:
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

    return document


def _parse_kafka(document: dict[str, Any], path: Path) -> tuple[dict[str, Any], str]:
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


def _positive_number(section: dict[str, Any], key: str, default: float, where: str) -> float:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{where}.{key} must be a number, not {type(value).__name__}.")
    if value <= 0:
        raise ConfigError(f"{where}.{key} must be greater than zero (got {value}).")
    return float(value)


def _identifier(section: dict[str, Any], key: str, default: str, where: str) -> str:
    value = section.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}.{key} must be a non-empty string.")

    name = value.strip()
    if "<" in name or ">" in name:
        raise ConfigError(
            f"{where}.{key} still contains placeholders ({name!r}). Replace them with a real name."
        )
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", name):
        raise ConfigError(
            f"{where}.{key} must contain only letters, digits, underscores and hyphens "
            f"and must not start with a hyphen (got {name!r})."
        )
    return name


def _parse_iceberg(document: dict[str, Any], path: Path) -> IcebergConfig | None:
    section = document.get("iceberg")
    if section is None:
        return None

    where = f"{path}: 'iceberg'"
    if not isinstance(section, dict):
        raise ConfigError(f"{where} must be a JSON object, not {type(section).__name__}.")

    enabled = section.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ConfigError(f"{where}.enabled must be true or false.")
    if not enabled:
        return None

    catalog = section.get("catalog")
    if not isinstance(catalog, dict) or not catalog:
        raise ConfigError(
            f"{where}.catalog must be a non-empty JSON object of PyIceberg catalog "
            f"properties, including at least 'type' and 'uri' for a REST catalog "
            f"(see {EXAMPLE_CONFIG_FILENAME})."
        )

    properties: dict[str, str] = {}
    for key, value in catalog.items():
        if isinstance(value, bool):
            # PyIceberg reads catalog properties as strings; JSON true/false is a
            # natural thing to write for a flag such as s3.path-style-access.
            properties[key] = "true" if value else "false"
        elif isinstance(value, (int, float)):
            properties[key] = str(value)
        elif isinstance(value, str):
            properties[key] = resolve_env_reference(value, f"{where}.catalog.{key}")
        else:
            raise ConfigError(
                f"{where}.catalog.{key} must be a string, number or boolean, "
                f"not {type(value).__name__}."
            )

    uri = properties.get("uri", "")
    if not uri.strip():
        raise ConfigError(f"{where}.catalog.uri must be a non-empty string naming the catalog endpoint.")
    if "<" in uri or ">" in uri:
        raise ConfigError(
            f"{where}.catalog.uri still contains placeholders ({uri!r}). Replace them "
            f"with the address of your Iceberg REST catalog."
        )

    for key, value in properties.items():
        if is_secret_name(key) and ("<" in value or ">" in value):
            raise ConfigError(
                f"{where}.catalog.{key} still contains placeholders. Replace it with a real "
                f"credential, or with an 'env:NAME' reference to an environment variable."
            )

    catalog_name = _identifier(section, "catalog_name", "s3_events", where)
    namespace = _identifier(section, "namespace", DEFAULT_NAMESPACE, where)
    table = _identifier(section, "table", DEFAULT_TABLE, where)

    batch_size = _positive_number(section, "batch_size", DEFAULT_BATCH_SIZE, where)
    if batch_size != int(batch_size):
        raise ConfigError(f"{where}.batch_size must be a whole number (got {batch_size}).")

    flush_interval = _positive_number(
        section, "flush_interval_seconds", DEFAULT_FLUSH_INTERVAL_SECONDS, where
    )

    create_if_missing = section.get("create_if_missing", True)
    if not isinstance(create_if_missing, bool):
        raise ConfigError(f"{where}.create_if_missing must be true or false.")

    max_attempts = _positive_number(
        section, "max_flush_attempts", DEFAULT_MAX_FLUSH_ATTEMPTS, where
    )
    if max_attempts != int(max_attempts):
        raise ConfigError(f"{where}.max_flush_attempts must be a whole number (got {max_attempts}).")

    retry_backoff = _positive_number(
        section, "retry_backoff_seconds", DEFAULT_RETRY_BACKOFF_SECONDS, where
    )

    default_buffer_limit = int(batch_size) * DEFAULT_BUFFER_LIMIT_MULTIPLE
    max_buffered = _positive_number(
        section, "max_buffered_records", default_buffer_limit, where
    )
    if max_buffered != int(max_buffered):
        raise ConfigError(
            f"{where}.max_buffered_records must be a whole number (got {max_buffered})."
        )
    if int(max_buffered) < int(batch_size):
        raise ConfigError(
            f"{where}.max_buffered_records ({int(max_buffered)}) must be at least "
            f"batch_size ({int(batch_size)}), or a full batch could never be buffered."
        )

    return IcebergConfig(
        namespace=namespace,
        table=table,
        catalog_name=catalog_name,
        catalog_properties=properties,
        batch_size=int(batch_size),
        flush_interval_seconds=flush_interval,
        create_if_missing=create_if_missing,
        max_flush_attempts=int(max_attempts),
        retry_backoff_seconds=retry_backoff,
        max_buffered_records=int(max_buffered),
    )


def load_app_config(path: Path) -> AppConfig:
    """Read and validate the whole configuration file.

    Raises:
        ConfigError: with a message that says how to fix the problem.
    """
    document = _read_document(path)
    kafka_conf, topic = _parse_kafka(document, path)
    iceberg = _parse_iceberg(document, path)
    return AppConfig(kafka_config=kafka_conf, topic=topic, iceberg=iceberg, source=path)


def load_config(path: Path) -> tuple[dict[str, Any], str]:
    """Read and validate the Kafka half of the configuration file.

    Returns:
        (kafka_conf, topic) — the librdkafka settings and the topic to consume.

    Raises:
        ConfigError: with a message that says how to fix the problem.
    """
    document = _read_document(path)
    return _parse_kafka(document, path)
