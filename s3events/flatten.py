"""Turn a decoded S3 event notification into flat rows.

The payload of one Kafka message normally holds an S3 event notification in the
AWS-compatible shape:

.. code-block:: json

    {"Records": [{"eventName": "s3:ObjectCreated:Put",
                  "eventSource": "vast:s3",
                  "eventTime": "2026-01-01T09:03:41.859365Z",
                  "s3": {"bucket": {"name": "demo-data"},
                         "object": {"key": "hello.txt", "size": 12, "eTag": "..."}}}]}

Note the two VAST-specific details, both confirmed against VAST's published
record format: ``eventName`` carries an ``s3:`` prefix, and ``eventSource`` is
``vast:s3`` rather than ``aws:s3``. Nothing here filters on either value - the
strings are stored as they arrive - but anything downstream that matches on them
needs to know.

One message can therefore carry several records, and each record becomes one
row. Nothing here assumes any particular field is present: the exact payload
schema comes from VAST and varies by release, so every lookup is defensive and a
missing field becomes ``None`` rather than an exception.

The single hard requirement is that a row can always be produced. Even a payload
that is not an object at all still yields one row, carrying the Kafka
coordinates and the raw JSON, so nothing observed on the topic is silently lost.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote_plus

# Keys tried in order when reading each field, because the exact spelling varies
# between S3 implementations and VAST releases.
_OBJECT_SIZE_KEYS = ("size", "objectSize", "Size", "contentLength")
_OBJECT_ETAG_KEYS = ("eTag", "etag", "ETag")
_OBJECT_KEY_KEYS = ("key", "Key", "objectKey")
_BUCKET_NAME_KEYS = ("name", "Name", "bucketName")
_EVENT_NAME_KEYS = ("eventName", "EventName", "event_name", "eventType")
_EVENT_TIME_KEYS = ("eventTime", "EventTime", "event_time", "eventTimestamp")
_EVENT_SOURCE_KEYS = ("eventSource", "EventSource", "event_source")

# Range of a 64-bit signed integer, the width of an Iceberg `long`. Values
# outside it are dropped rather than allowed to overflow the column.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


@dataclass(frozen=True)
class EventRow:
    """One flattened S3 event record, one row in the Iceberg table."""

    ingest_time: dt.datetime
    kafka_topic: str | None
    kafka_partition: int | None
    kafka_offset: int | None
    record_index: int
    event_name: str | None
    event_time: dt.datetime | None
    event_source: str | None
    bucket: str | None
    object_key: str | None
    object_size: int | None
    object_etag: str | None
    raw_event: str

    def as_dict(self) -> dict[str, Any]:
        """Column name to value, in the order the Iceberg schema declares them."""
        return {
            "ingest_time": self.ingest_time,
            "kafka_topic": self.kafka_topic,
            "kafka_partition": self.kafka_partition,
            "kafka_offset": self.kafka_offset,
            "record_index": self.record_index,
            "event_name": self.event_name,
            "event_time": self.event_time,
            "event_source": self.event_source,
            "bucket": self.bucket,
            "object_key": self.object_key,
            "object_size": self.object_size,
            "object_etag": self.object_etag,
            "raw_event": self.raw_event,
        }


def utc_now() -> dt.datetime:
    """Current time, timezone-aware. Separated out so tests can replace it."""
    return dt.datetime.now(dt.timezone.utc)


def _mapping(value: Any) -> dict[str, Any]:
    """``value`` when it is a JSON object, otherwise an empty one."""
    return value if isinstance(value, dict) else {}


def _first_string(source: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    """First key present with a usable scalar value, rendered as a string."""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    return None


def _first_int(source: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    """First key present holding an integer that fits an Iceberg ``long``.

    Accepts the numeric strings some producers emit for sizes. Booleans are
    rejected even though ``bool`` is an ``int`` in Python.
    """
    for key in keys:
        value = source.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            number = value
        elif isinstance(value, float):
            if value != int(value):
                continue
            number = int(value)
        elif isinstance(value, str):
            try:
                number = int(value.strip())
            except ValueError:
                continue
        else:
            continue

        if _INT64_MIN <= number <= _INT64_MAX:
            return number
    return None


def parse_timestamp(value: Any) -> dt.datetime | None:
    """Best-effort ISO-8601 or epoch timestamp, normalised to UTC.

    Returns None for anything unparseable — an unreadable ``eventTime`` must not
    cost us the rest of the row.
    """
    if isinstance(value, bool) or value is None:
        return None

    if isinstance(value, (int, float)):
        # Heuristic: values far beyond the year 3000 in seconds are milliseconds.
        seconds = float(value)
        if abs(seconds) > 1e11:
            seconds /= 1000.0
        try:
            return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    # fromisoformat before 3.11 rejects a trailing 'Z' and sub-second precision
    # other than 3 or 6 digits, both of which appear in real S3 event payloads.
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, _, tail = text.partition(".")
        digits = ""
        for char in tail:
            if char.isdigit():
                digits += char
            else:
                break
        remainder = tail[len(digits) :]
        if digits:
            text = f"{head}.{digits[:6].ljust(6, '0')}{remainder}"

    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        # S3 event times are UTC; an absent offset is not a local time.
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _as_vast_test_event(event: Any) -> dict[str, Any] | None:
    """Recognise the connectivity test event VAST fires on a new notification.

    Saving an S3 bucket notification makes VAST immediately publish a test event
    to prove the wiring works. It is deliberately *not* the ``Records`` envelope
    of a real object event::

        {"Service": "Vast S3", "Event": "s3:TestEvent", "Time": "...",
         "Bucket": "...", "RequestId": "...", "HostId": "..."}

    Without this, it would flatten into a row with every S3 column null, which
    looks like a fault during a demo. Mapping it onto the normal columns makes
    the first row in a fresh table self-explanatory instead.
    """
    if not isinstance(event, dict):
        return None
    if "Records" in event or "records" in event:
        return None

    name = event.get("Event")
    service = event.get("Service")
    if not isinstance(name, str) or not name.strip():
        return None
    if not isinstance(service, str) or "s3" not in service.lower():
        return None

    record: dict[str, Any] = {"eventName": name, "eventSource": service}
    bucket = event.get("Bucket")
    if isinstance(bucket, str) and bucket.strip():
        record["s3"] = {"bucket": {"name": bucket}}
    when = event.get("Time")
    if when is not None:
        record["eventTime"] = when
    return record


def _records_of(event: Any) -> list[Any]:
    """The list of S3 records in a decoded payload.

    Falls back to treating the whole payload as a single record when it is an
    object without a usable ``Records`` list, so an unfamiliar VAST payload
    shape still produces a row.
    """
    if isinstance(event, dict):
        for key in ("Records", "records"):
            value = event.get(key)
            if isinstance(value, list):
                # An explicit but empty Records list means "no records", which is
                # different from "this payload has no Records key".
                return value

        test_event = _as_vast_test_event(event)
        if test_event is not None:
            return [test_event]

        return [event]
    return [event]


def flatten_event(
    event: Any,
    raw_payload: str,
    *,
    topic: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
    ingest_time: dt.datetime | None = None,
) -> list[EventRow]:
    """Flatten one decoded Kafka payload into table rows.

    Args:
        event: the decoded JSON payload, of any shape.
        raw_payload: the payload exactly as it arrived, stored verbatim on every
            row produced from it so nothing is lost to the flattening.
        topic, partition, offset: Kafka coordinates of the message.
        ingest_time: overrides the wall clock, for tests and for keeping every
            row from one message on the same timestamp.

    Returns:
        One row per S3 record. Always at least one row, never raises.
    """
    stamp = ingest_time or utc_now()
    rows: list[EventRow] = []

    for index, record in enumerate(_records_of(event)):
        mapping = _mapping(record)
        s3 = _mapping(mapping.get("s3") or mapping.get("S3"))
        bucket = _mapping(s3.get("bucket") or s3.get("Bucket"))
        obj = _mapping(s3.get("object") or s3.get("Object"))

        object_key = _first_string(obj, _OBJECT_KEY_KEYS)
        if object_key is not None:
            # S3 event notifications URL-encode the key, and '+' means a space.
            object_key = unquote_plus(object_key)

        rows.append(
            EventRow(
                ingest_time=stamp,
                kafka_topic=topic,
                kafka_partition=partition,
                kafka_offset=offset,
                record_index=index,
                event_name=_first_string(mapping, _EVENT_NAME_KEYS),
                event_time=parse_timestamp(_first_string(mapping, _EVENT_TIME_KEYS)),
                event_source=_first_string(mapping, _EVENT_SOURCE_KEYS),
                bucket=_first_string(bucket, _BUCKET_NAME_KEYS),
                object_key=object_key,
                object_size=_first_int(obj, _OBJECT_SIZE_KEYS),
                object_etag=_first_string(obj, _OBJECT_ETAG_KEYS),
                raw_event=raw_payload,
            )
        )

    if not rows:
        # An explicit empty "Records": [] list. Record that the message arrived.
        rows.append(
            EventRow(
                ingest_time=stamp,
                kafka_topic=topic,
                kafka_partition=partition,
                kafka_offset=offset,
                record_index=0,
                event_name=None,
                event_time=None,
                event_source=None,
                bucket=None,
                object_key=None,
                object_size=None,
                object_etag=None,
                raw_event=raw_payload,
            )
        )

    return rows
