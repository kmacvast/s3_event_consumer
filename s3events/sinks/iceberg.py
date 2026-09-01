"""Buffered Apache Iceberg sink.

Writes flattened S3 event metadata into an Iceberg table whose data files live
in a VAST S3 bucket, through an Iceberg REST catalog.

Three design points are worth stating outright.

**Batching.** Every Iceberg commit produces a snapshot and at least one data
file. Committing once per Kafka event would produce thousands of one-row Parquet
files and an unreadable snapshot history. Records therefore accumulate in memory
and are written when either bound is reached: ``batch_size`` records buffered, or
``flush_interval_seconds`` elapsed since the last write. The interval is driven
by :meth:`tick`, which the poll loop calls once per iteration — including idle
ones — so a trickle of events still lands promptly. No background thread is
involved: flushing happens on the poll loop's own thread, which keeps ordering
obvious and the whole thing testable.

**Iceberg commits before Kafka offsets.** Each buffered record's Kafka position
is tracked alongside it. A batch is written to Iceberg *first*; only once that
append has succeeded are the corresponding Kafka offsets committed, through the
``offset_committer`` callback the consumer supplies. Nothing is ever
acknowledged to Kafka that is not already durable in Iceberg. This gives
**at-least-once** delivery, not exactly-once: a crash between the Iceberg commit
and the offset commit replays those records and duplicates them in the table.
See ``docs/iceberg-demo.md`` for what that means in practice.

**A failed write keeps its records.** A failed append does not drop the batch.
The records and their offsets stay buffered and the append is retried, with
exponential backoff so a catalog outage does not become a hot loop. Because the
offsets were never committed, the records are still replayable from Kafka. The
retry is bounded in two ways — ``max_flush_attempts`` consecutive failures, and
``max_buffered_records`` accumulated — and hitting either raises
:class:`SinkFatalError`, stopping the consumer non-zero with the offsets still
uncommitted. Stopping is the point: it is what leaves those events on the topic
to be reprocessed, rather than acknowledged and gone.

This has been validated for a single consumer against a single-partition topic
(``scripts/outage_test.sh``). Kafka rebalance behaviour — a partition reassigned
while records are buffered here — is not covered, and none of it has been run
against a real VAST Event Broker yet. See "Validation status" in
``docs/iceberg-demo.md``.

PyIceberg is imported inside :meth:`open`, not at module scope. That keeps
``import s3events.sinks.iceberg`` free of a PyArrow dependency, so the
non-Iceberg path — including the standalone executable, which does not bundle
PyIceberg — never needs it installed.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Callable

from s3events.config import MAX_RETRY_BACKOFF_SECONDS, IcebergConfig
from s3events.flatten import EventRow
from s3events.sinks.base import SinkError, SinkFatalError

if TYPE_CHECKING:  # pragma: no cover - typing only, never imported at runtime
    from pyiceberg.table import Table

LOG = logging.getLogger("s3_event_consumer.iceberg")

#: Maps (topic, partition) to the next offset Kafka should read from.
OffsetMap = dict[tuple[str, int], int]

#: Called with an :data:`OffsetMap` once a batch is durable in Iceberg.
OffsetCommitter = Callable[[OffsetMap], None]

INSTALL_HINT = (
    "PyIceberg is not installed. Iceberg support is an optional extra:\n"
    "    python3 -m pip install -r requirements-iceberg.txt\n"
    "The prebuilt standalone executables do not bundle it — see the packaging "
    "note in README.md. Run from Python source to use the Iceberg sink, or set "
    "'iceberg.enabled' to false in the configuration file."
)


def build_schema() -> Any:
    """The table's explicit Iceberg schema.

    Declared field by field rather than inferred from a sample payload: event
    JSON varies between VAST releases, and a schema that changes shape with
    whatever arrived first is not a table anyone can query. Everything except
    the ingestion timestamp and the raw payload is optional, because no field of
    an S3 event notification is guaranteed to be present.
    """
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestamptzType,
    )

    return Schema(
        NestedField(1, "ingest_time", TimestamptzType(), required=True,
                    doc="When the consumer received the Kafka message."),
        NestedField(2, "kafka_topic", StringType(), required=False),
        NestedField(3, "kafka_partition", IntegerType(), required=False),
        NestedField(4, "kafka_offset", LongType(), required=False),
        NestedField(5, "record_index", IntegerType(), required=False,
                    doc="Position within the message's Records array."),
        NestedField(6, "event_name", StringType(), required=False,
                    doc="For example ObjectCreated:Put."),
        NestedField(7, "event_time", TimestamptzType(), required=False,
                    doc="Event time reported by S3, when the payload carries one."),
        NestedField(8, "event_source", StringType(), required=False),
        NestedField(9, "bucket", StringType(), required=False),
        NestedField(10, "object_key", StringType(), required=False),
        NestedField(11, "object_size", LongType(), required=False,
                    doc="Object size in bytes, when the payload carries one."),
        NestedField(12, "object_etag", StringType(), required=False),
        NestedField(13, "raw_event", StringType(), required=True,
                    doc="The Kafka message payload verbatim, so no detail lost "
                        "in flattening is unrecoverable."),
    )


def build_partition_spec() -> Any:
    """Partition by ingestion day.

    Coarse on purpose. A demo produces far too little data to justify anything
    finer, and a day partition still shows up as a readable
    ``ingest_time_day=YYYY-MM-DD`` directory in the object store, which makes
    Iceberg's hidden partitioning visible when you go looking at the files.
    """
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import DayTransform

    return PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ingest_time_day")
    )


class IcebergSink:
    """Buffers flattened event rows and appends them to an Iceberg table.

    Args:
        config: the validated ``iceberg`` configuration section.
        clock: monotonic time source, replaced in tests.
        offset_committer: called with an :data:`OffsetMap` after each successful
            Iceberg append. When omitted, no offsets are committed — used by
            ``--check`` and by tests.
    """

    name = "iceberg"

    def __init__(
        self,
        config: IcebergConfig,
        clock: Any = time.monotonic,
        offset_committer: OffsetCommitter | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._commit_offsets = offset_committer

        self._buffer: list[EventRow] = []
        # Highest Kafka offset per (topic, partition) among the buffered rows.
        # Held alongside the buffer and cleared with it, so offsets can only be
        # committed for records that made it into Iceberg.
        self._pending_offsets: OffsetMap = {}

        self._catalog: Any = None
        self._table: Table | None = None
        self._arrow_schema: Any = None
        self._needs_reload = False

        self._last_flush = clock()
        self._failures = 0
        self._retry_after = 0.0
        self._committed = 0
        self._commits = 0

    # -- state, mostly for tests and for the shutdown summary --------------- #

    @property
    def pending(self) -> int:
        """Rows buffered but not yet written."""
        return len(self._buffer)

    @property
    def pending_offsets(self) -> OffsetMap:
        """Kafka positions of the buffered rows, as next-offset-to-read."""
        return {key: offset + 1 for key, offset in self._pending_offsets.items()}

    @property
    def committed(self) -> int:
        """Rows successfully written to the table."""
        return self._committed

    @property
    def commits(self) -> int:
        """Number of successful appends, one Iceberg snapshot each."""
        return self._commits

    @property
    def consecutive_failures(self) -> int:
        """Failed append attempts since the last success."""
        return self._failures

    # -- lifecycle ---------------------------------------------------------- #

    def open(self) -> None:
        """Load the catalog, then the table, creating it when configured to.

        Raises:
            SinkError: when PyIceberg is missing, the catalog is unreachable, or
                the table cannot be loaded or created.
        """
        self._catalog = self._load_catalog()
        self._table = self._load_table(self._catalog)
        self._arrow_schema = self._table.schema().as_arrow()
        self._last_flush = self._clock()

    def _load_catalog(self) -> Any:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:
            raise SinkError(f"{INSTALL_HINT}\n(underlying error: {exc})") from None

        config = self._config
        LOG.info(
            "Connecting to Iceberg catalog '%s' at %s",
            config.catalog_name,
            config.catalog_properties.get("uri", "<no uri>"),
        )
        LOG.debug("Catalog properties: %s", config.safe_properties())

        try:
            return load_catalog(config.catalog_name, **config.catalog_properties)
        except Exception as exc:  # noqa: BLE001 - PyIceberg raises a wide range here
            raise SinkError(
                f"Could not connect to the Iceberg catalog at "
                f"{config.catalog_properties.get('uri', '<no uri>')}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    def _load_table(self, catalog: Any) -> Any:
        from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

        config = self._config
        identifier = config.table_identifier

        try:
            table = catalog.load_table(identifier)
        except (NoSuchTableError, NoSuchNamespaceError) as exc:
            if not config.create_if_missing:
                raise SinkError(
                    f"Iceberg table '{identifier}' does not exist and "
                    f"'iceberg.create_if_missing' is false: {exc}"
                ) from None
            table = self._create_table(catalog)
        except Exception as exc:  # noqa: BLE001 - catalog errors vary by backend
            raise SinkError(
                f"Could not load Iceberg table '{identifier}': {type(exc).__name__}: {exc}"
            ) from exc
        else:
            LOG.info("Using existing Iceberg table %s at %s", identifier, table.location())
            self._warn_about_schema_drift(table)

        return table

    def _warn_about_schema_drift(self, table: Any) -> None:
        """Compare an adopted table's columns with the ones this sink writes.

        Rows are built against whatever schema the table already has. PyArrow
        silently drops keys the schema does not mention and fills missing
        columns with nulls, so a table that someone else created under the same
        name ingests quietly wrong data. Say so loudly instead — but do not
        refuse to run, because the operator may have added columns on purpose.
        """
        try:
            actual = {field.name for field in table.schema().fields}
        except Exception:  # noqa: BLE001 - never let a diagnostic break startup
            return

        expected = {field.name for field in build_schema().fields}
        missing = sorted(expected - actual)
        if missing:
            LOG.warning(
                "Iceberg table %s is missing column(s) this consumer writes: %s. "
                "Those values will be discarded on every append. The table was "
                "probably created by something else, or by an older version.",
                self._config.table_identifier,
                ", ".join(missing),
            )

        extra = sorted(actual - expected)
        if extra:
            LOG.warning(
                "Iceberg table %s has column(s) this consumer never writes: %s. "
                "They will be null in every new row.",
                self._config.table_identifier,
                ", ".join(extra),
            )

    def _create_table(self, catalog: Any) -> Any:
        config = self._config
        identifier = config.table_identifier

        try:
            catalog.create_namespace_if_not_exists(config.namespace)
            table = catalog.create_table_if_not_exists(
                identifier,
                schema=build_schema(),
                partition_spec=build_partition_spec(),
            )
        except Exception as exc:  # noqa: BLE001 - catalog errors vary by backend
            raise SinkError(
                f"Could not create Iceberg table '{identifier}': {type(exc).__name__}: {exc}"
            ) from exc

        LOG.info("Created Iceberg table %s at %s", identifier, table.location())
        return table

    # -- the event path ----------------------------------------------------- #

    def handle(self, event: Any, rows: list[EventRow], raw_payload: str) -> None:
        """Buffer one message's rows, and remember where they came from.

        Raises:
            SinkFatalError: when the buffer has grown past ``max_buffered_records``
                without a successful write.
        """
        self._buffer.extend(rows)
        self._track_offsets(rows)

        if len(self._buffer) >= self._config.batch_size:
            self.flush(reason="batch size reached")

        # Checked after the flush attempt, so a healthy sink never trips it.
        if len(self._buffer) >= self._config.max_buffered_records:
            raise SinkFatalError(
                f"{len(self._buffer)} record(s) are buffered and unwritten, at or past the "
                f"max_buffered_records limit of {self._config.max_buffered_records}. "
                f"Events are arriving faster than Iceberg is accepting them. Stopping "
                f"without committing their Kafka offsets, so they stay on the topic and "
                f"can be replayed once Iceberg is healthy."
            )

    def _track_offsets(self, rows: list[EventRow]) -> None:
        """Record the highest Kafka offset per partition among these rows."""
        for row in rows:
            if row.kafka_topic is None or row.kafka_partition is None or row.kafka_offset is None:
                continue
            key = (row.kafka_topic, row.kafka_partition)
            current = self._pending_offsets.get(key)
            if current is None or row.kafka_offset > current:
                self._pending_offsets[key] = row.kafka_offset

    def tick(self) -> None:
        """Flush buffered rows once the flush interval, or a retry backoff, elapses.

        Raises:
            SinkFatalError: when the bounded retry has been exhausted.
        """
        now = self._clock()

        if not self._buffer:
            # Nothing pending: restart the clock so the first record after an
            # idle stretch gets a full interval to accumulate company.
            self._last_flush = now
            return

        if self._failures:
            # Retrying a failed batch: the backoff deadline governs, not the
            # flush interval. This is what stops a catalog outage becoming a
            # hot loop against an unavailable endpoint.
            if now >= self._retry_after:
                self.flush(reason=f"retry {self._failures + 1}/{self._config.max_flush_attempts}")
            return

        if now - self._last_flush >= self._config.flush_interval_seconds:
            self.flush(reason="flush interval elapsed")

    def flush(self, reason: str = "explicit flush", force: bool = False) -> bool:
        """Append the buffered rows to Iceberg, then commit their Kafka offsets.

        Order matters and is the whole point: Iceberg first, Kafka second. A
        failed append leaves the buffer and its offsets untouched so the records
        are retried, and — because their offsets were never committed — remain
        replayable from Kafka if the process ultimately gives up.

        Args:
            reason: what triggered this flush, for the log line.
            force: ignore the retry backoff. Used only at shutdown, where there
                is no later opportunity to try again.

        Returns:
            True when rows were written, False when there was nothing to write,
            the write failed, or a retry backoff is still in effect.

        Raises:
            SinkFatalError: when this failure was the last permitted attempt.
        """
        if not self._buffer:
            return False

        # Respect the backoff whatever called us. Without this, a batch that
        # keeps filling would retry on every incoming message and defeat the
        # backoff that tick() applies.
        if not force and self._failures and self._clock() < self._retry_after:
            return False

        if self._table is None:
            raise SinkFatalError(
                f"The Iceberg sink was never opened, so {len(self._buffer)} record(s) "
                f"cannot be written. Stopping without committing their Kafka offsets."
            )

        count = len(self._buffer)
        try:
            self._append(self._buffer)
        except Exception as exc:  # noqa: BLE001 - any write error is retried, not fatal yet
            return self._record_failure(exc, reason, count)

        # Durable in Iceberg. Only now may Kafka be told we are done with these.
        offsets = self.pending_offsets
        self._buffer = []
        self._pending_offsets = {}
        self._failures = 0
        self._last_flush = self._clock()
        self._committed += count
        self._commits += 1

        LOG.info(
            "Committed %d record(s) to %s (%s); %d record(s) total in this session.",
            count,
            self._config.table_identifier,
            reason,
            self._committed,
        )
        self._commit_kafka_offsets(offsets)
        return True

    def _append(self, rows: list[EventRow]) -> None:
        import pyarrow as pa

        if self._needs_reload:
            # The previous attempt failed. Reload the table so a retry picks up
            # a fresh catalog session and current metadata, rather than
            # repeatedly replaying a stale one.
            self._table = self._catalog.load_table(self._config.table_identifier)
            self._arrow_schema = self._table.schema().as_arrow()
            self._needs_reload = False

        table = pa.Table.from_pylist([row.as_dict() for row in rows], schema=self._arrow_schema)
        self._table.append(table)

    def _record_failure(self, exc: Exception, reason: str, count: int) -> bool:
        """Keep the batch, schedule a retry, or give up if out of attempts."""
        self._failures += 1
        self._needs_reload = True

        attempts = self._config.max_flush_attempts
        if self._failures >= attempts:
            raise SinkFatalError(
                f"Iceberg append failed {self._failures} time(s) in a row, the configured "
                f"max_flush_attempts. Last error: {type(exc).__name__}: {exc}. "
                f"{count} record(s) remain unwritten and their Kafka offsets have NOT been "
                f"committed, so they stay on the topic and will be reprocessed when the "
                f"consumer is restarted against a healthy catalog."
            ) from exc

        # Exponential backoff from the configured base, capped.
        backoff = min(
            self._config.retry_backoff_seconds * (2 ** (self._failures - 1)),
            MAX_RETRY_BACKOFF_SECONDS,
        )
        self._retry_after = self._clock() + backoff

        LOG.error(
            "Iceberg append failed (%s), attempt %d of %d: %s: %s. "
            "Keeping %d record(s) buffered and NOT committing their Kafka offsets; "
            "retrying in %.0fs.",
            reason,
            self._failures,
            attempts,
            type(exc).__name__,
            exc,
            count,
            backoff,
        )
        return False

    def _commit_kafka_offsets(self, offsets: OffsetMap) -> None:
        """Acknowledge to Kafka the records now durable in Iceberg."""
        if self._commit_offsets is None or not offsets:
            return

        try:
            self._commit_offsets(offsets)
        except Exception as exc:  # noqa: BLE001 - the data is safe; this only risks a replay
            LOG.error(
                "Iceberg commit succeeded but committing Kafka offsets failed: %s: %s. "
                "The records are durable; they may be reprocessed, and would then be "
                "duplicated in the table.",
                type(exc).__name__,
                exc,
            )
            return

        LOG.debug(
            "Committed Kafka offsets: %s",
            ", ".join(f"{t}[{p}]@{o}" for (t, p), o in sorted(offsets.items())),
        )

    def close(self) -> None:
        """Flush anything still buffered, then report what happened.

        Called from the consumer's shutdown path before the Kafka consumer is
        closed, so a graceful Ctrl-C writes the final partial batch to Iceberg
        and commits its offsets. One final attempt is made, ignoring any retry
        backoff, because the process is going away either way.

        Raises:
            SinkError: when records remain unwritten. The consumer must exit
                non-zero, leaving those offsets uncommitted for replay.
        """
        unwritten = 0
        failure: Exception | None = None

        if self._buffer:
            LOG.info("Flushing %d buffered record(s) before shutdown.", len(self._buffer))
            # Whether or not the retries are already exhausted, this is the last
            # chance, so bypass both the backoff and the attempt ceiling for one
            # final try.
            self._failures = 0
            self._retry_after = 0.0
            try:
                self.flush(reason="shutdown", force=True)
            except SinkFatalError as exc:
                # max_flush_attempts of 1 turns the single retry into a fatal.
                failure = exc
            unwritten = len(self._buffer)

        LOG.info(
            "Iceberg sink closed: %d record(s) written in %d commit(s), %d still unwritten.",
            self._committed,
            self._commits,
            unwritten,
        )

        if unwritten:
            raise SinkError(
                f"Shutdown flush failed: {unwritten} record(s) could not be written to "
                f"{self._config.table_identifier}"
                + (f" ({failure})" if failure else "")
                + ". Their Kafka offsets have NOT been committed, so they remain on the "
                "topic and will be reprocessed on the next run. Exiting non-zero."
            )

        self._table = None
