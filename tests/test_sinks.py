"""Tests for the sink layer: dispatch, console, and the buffered Iceberg writer.

PyIceberg is mocked throughout. Nothing here needs Docker, Kafka, VAST, MinIO or
a live catalog — see scripts/smoke_test.sh for the integration path.

    python3 -m unittest discover -s tests -v
"""

import contextlib
import datetime as dt
import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import s3_event_consumer as consumer  # noqa: E402
from s3events.config import IcebergConfig  # noqa: E402
from s3events.flatten import EventRow, flatten_event  # noqa: E402
from s3events.sinks import (  # noqa: E402
    ConsoleSink,
    EventDispatcher,
    SinkError,
    SinkFatalError,
)
from s3events.sinks.iceberg import IcebergSink  # noqa: E402

EVENT = {
    "Records": [
        {
            "eventName": "ObjectCreated:Put",
            "s3": {"bucket": {"name": "demo-data"}, "object": {"key": "hello.txt", "size": 12}},
        }
    ]
}
RAW = json.dumps(EVENT)


def config(**overrides):
    defaults = dict(
        namespace="s3_events",
        table="object_events",
        catalog_name="demo",
        catalog_properties={"type": "rest", "uri": "http://localhost:8181"},
        batch_size=3,
        flush_interval_seconds=5.0,
        create_if_missing=True,
    )
    defaults.update(overrides)
    return IcebergConfig(**defaults)


def rows(count=1, offset=0):
    return flatten_event(EVENT, RAW, topic="s3-events", partition=0, offset=offset) * count


class FakeClock:
    """A monotonic clock the tests advance by hand."""

    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class RecordingSink:
    """A sink that remembers what it was asked to do."""

    def __init__(self, name="recorder", fail_on=None):
        self.name = name
        self.fail_on = fail_on or set()
        self.calls = []

    def _record(self, what):
        self.calls.append(what)
        if what in self.fail_on:
            raise RuntimeError(f"{self.name} cannot {what}")

    def open(self):
        self._record("open")

    def handle(self, event, rows, raw_payload):
        self._record("handle")

    def tick(self):
        self._record("tick")

    def close(self):
        self._record("close")


# --------------------------------------------------------------------------- #
# Dispatcher
# --------------------------------------------------------------------------- #


class DispatcherTestCase(unittest.TestCase):
    def test_every_sink_receives_every_event(self):
        a, b = RecordingSink("a"), RecordingSink("b")
        dispatcher = EventDispatcher([a, b])
        dispatcher.open()
        dispatcher.dispatch(EVENT, rows(), RAW)
        dispatcher.close()
        self.assertEqual(a.calls, ["open", "handle", "close"])
        self.assertEqual(b.calls, ["open", "handle", "close"])

    def test_sinks_are_closed_in_reverse_order(self):
        order = []
        a, b = RecordingSink("a"), RecordingSink("b")
        a.close = lambda: order.append("a")
        b.close = lambda: order.append("b")
        EventDispatcher([a, b]).close()
        self.assertEqual(order, ["b", "a"])

    def test_a_failing_sink_does_not_stop_the_others(self):
        broken = RecordingSink("broken", fail_on={"handle"})
        healthy = RecordingSink("healthy")
        dispatcher = EventDispatcher([broken, healthy])
        with self.assertLogs("s3_event_consumer", level="ERROR") as logs:
            dispatcher.dispatch(EVENT, rows(), RAW)
        self.assertIn("handle", healthy.calls)
        self.assertTrue(any("broken" in line for line in logs.output))

    def test_a_failing_sink_does_not_stop_ticks_or_closes(self):
        broken = RecordingSink("broken", fail_on={"tick", "close"})
        healthy = RecordingSink("healthy")
        dispatcher = EventDispatcher([broken, healthy])
        with self.assertLogs("s3_event_consumer", level="ERROR"):
            dispatcher.tick()
        with self.assertLogs("s3_event_consumer", level="ERROR"):
            dispatcher.close()
        self.assertEqual(healthy.calls, ["tick", "close"])

    def test_open_failures_propagate(self):
        """A sink that cannot start is a startup failure, not something to log past."""
        dispatcher = EventDispatcher([RecordingSink("broken", fail_on={"open"})])
        with self.assertRaises(RuntimeError):
            dispatcher.open()

    def test_open_failure_closes_sinks_already_opened(self):
        healthy = RecordingSink("healthy")
        dispatcher = EventDispatcher([healthy, RecordingSink("broken", fail_on={"open"})])
        with self.assertRaises(RuntimeError):
            dispatcher.open()
        self.assertEqual(healthy.calls, ["open", "close"])


# --------------------------------------------------------------------------- #
# Console sink
# --------------------------------------------------------------------------- #


class ConsoleSinkTestCase(unittest.TestCase):
    def output_for(self, event, color=False):
        sink = ConsoleSink(color)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            sink.open()
            sink.handle(event, flatten_event(event, json.dumps(event)), json.dumps(event))
            sink.tick()
            sink.close()
        return out.getvalue()

    def test_event_is_printed_as_pretty_json(self):
        printed = self.output_for(EVENT)
        self.assertEqual(json.loads(printed), EVENT)
        self.assertIn("\n  ", printed)

    def test_output_is_separated_by_a_blank_line(self):
        self.assertTrue(self.output_for(EVENT).endswith("\n\n"))

    def test_colour_adds_ansi_escapes(self):
        self.assertIn("\033[", self.output_for(EVENT, color=True))

    def test_tick_and_close_print_nothing_extra(self):
        sink = ConsoleSink(False)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            sink.tick()
            sink.close()
        self.assertEqual(out.getvalue(), "")


# --------------------------------------------------------------------------- #
# Iceberg sink — batching
# --------------------------------------------------------------------------- #


class IcebergSinkTestCase(unittest.TestCase):
    """Every test here drives a sink whose table is a Mock."""

    def make_sink(self, clock=None, track_offsets=False, **overrides):
        """A sink whose table is a Mock and whose append is recorded, not real.

        With track_offsets, `sink.offset_commits` collects every offset map the
        sink hands to its committer — that is the record of what Kafka was told,
        and when.
        """
        committer = None
        commits = []
        if track_offsets:
            committer = commits.append

        sink = IcebergSink(
            config(**overrides), clock=clock or FakeClock(), offset_committer=committer
        )
        sink._table = mock.Mock()
        sink._arrow_schema = mock.sentinel.arrow_schema
        # Bypass PyArrow entirely: record the rows each append was given.
        sink.appended = []
        sink.offset_commits = commits
        sink._append = lambda rows: sink.appended.append(list(rows))
        return sink

    def fail_appends_on(self, sink, exc=None):
        """Make every subsequent append fail."""
        error = exc or RuntimeError("catalog unavailable")

        def explode(rows):
            raise error

        sink._append = explode
        return sink

    def feed(self, sink, count, start=0):
        for offset in range(start, start + count):
            batch = flatten_event(EVENT, RAW, topic="s3-events", partition=0, offset=offset)
            sink.handle(EVENT, batch, RAW)


class BatchingTestCase(IcebergSinkTestCase):
    def test_nothing_is_written_before_the_batch_is_full(self):
        sink = self.make_sink(batch_size=3)
        self.feed(sink, 2)
        self.assertEqual(sink.appended, [])
        self.assertEqual(sink.pending, 2)
        self.assertEqual(sink.committed, 0)

    def test_reaching_the_batch_size_writes_once(self):
        sink = self.make_sink(batch_size=3)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 3)
        self.assertEqual(len(sink.appended), 1)
        self.assertEqual(len(sink.appended[0]), 3)
        self.assertEqual(sink.pending, 0)
        self.assertEqual(sink.committed, 3)
        self.assertEqual(sink.commits, 1)

    def test_one_snapshot_per_batch_not_per_event(self):
        sink = self.make_sink(batch_size=5)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 20)
        self.assertEqual(sink.commits, 4)
        self.assertEqual(sink.committed, 20)

    def test_a_multi_record_message_counts_every_record(self):
        sink = self.make_sink(batch_size=3)
        event = {"Records": [{"s3": {"object": {"key": "a"}}}, {"s3": {"object": {"key": "b"}}}]}
        sink.handle(event, flatten_event(event, json.dumps(event)), json.dumps(event))
        self.assertEqual(sink.pending, 2)

    def test_the_batch_can_overshoot_when_one_message_carries_several_records(self):
        sink = self.make_sink(batch_size=2)
        event = {"Records": [{"s3": {}}, {"s3": {}}, {"s3": {}}]}
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.handle(event, flatten_event(event, json.dumps(event)), json.dumps(event))
        self.assertEqual(len(sink.appended[0]), 3)
        self.assertEqual(sink.pending, 0)

    def test_rows_are_written_in_arrival_order(self):
        sink = self.make_sink(batch_size=3)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 3)
        self.assertEqual([row.kafka_offset for row in sink.appended[0]], [0, 1, 2])


class FlushIntervalTestCase(IcebergSinkTestCase):
    def test_tick_before_the_interval_writes_nothing(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=100, flush_interval_seconds=5.0)
        self.feed(sink, 2)
        clock.advance(4.9)
        sink.tick()
        self.assertEqual(sink.appended, [])
        self.assertEqual(sink.pending, 2)

    def test_tick_after_the_interval_writes_the_partial_batch(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=100, flush_interval_seconds=5.0)
        self.feed(sink, 2)
        clock.advance(5.0)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO") as logs:
            sink.tick()
        self.assertEqual(len(sink.appended), 1)
        self.assertEqual(sink.committed, 2)
        self.assertTrue(any("flush interval" in line for line in logs.output))

    def test_an_idle_tick_writes_nothing_and_creates_no_snapshot(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, flush_interval_seconds=5.0)
        for _ in range(100):
            clock.advance(1.0)
            sink.tick()
        self.assertEqual(sink.appended, [])
        self.assertEqual(sink.commits, 0)

    def test_the_interval_restarts_after_a_write(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=100, flush_interval_seconds=5.0)
        self.feed(sink, 1)
        clock.advance(5.0)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.tick()
        self.feed(sink, 1)
        clock.advance(4.0)
        sink.tick()
        self.assertEqual(len(sink.appended), 1)

    def test_an_idle_stretch_does_not_immediately_flush_the_next_record(self):
        """The first record after a quiet period gets a full interval of company."""
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=100, flush_interval_seconds=5.0)
        clock.advance(3600.0)
        sink.tick()
        self.feed(sink, 1)
        clock.advance(1.0)
        sink.tick()
        self.assertEqual(sink.appended, [])


class ExplicitFlushTestCase(IcebergSinkTestCase):
    def test_flushing_an_empty_buffer_is_a_no_op(self):
        sink = self.make_sink()
        self.assertFalse(sink.flush())
        self.assertEqual(sink.appended, [])
        self.assertEqual(sink.commits, 0)

    def test_flush_reports_whether_it_wrote(self):
        sink = self.make_sink(batch_size=100)
        self.feed(sink, 1)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.assertTrue(sink.flush())
        self.assertFalse(sink.flush())


class ShutdownTestCase(IcebergSinkTestCase):
    def test_close_writes_the_pending_partial_batch(self):
        sink = self.make_sink(batch_size=100)
        self.feed(sink, 7)
        self.assertEqual(sink.pending, 7)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.close()
        self.assertEqual(sink.committed, 7)
        self.assertEqual(sink.pending, 0)
        self.assertEqual(len(sink.appended), 1)

    def test_close_with_an_empty_buffer_writes_nothing(self):
        sink = self.make_sink()
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.close()
        self.assertEqual(sink.appended, [])
        self.assertEqual(sink.commits, 0)

    def test_close_reports_the_session_totals(self):
        sink = self.make_sink(batch_size=2)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO") as logs:
            self.feed(sink, 4)
            sink.close()
        summary = logs.output[-1]
        self.assertIn("4 record(s) written", summary)
        self.assertIn("2 commit(s)", summary)
        self.assertIn("0 still unwritten", summary)

    def test_shutdown_through_the_dispatcher_flushes(self):
        sink = self.make_sink(batch_size=100)
        dispatcher = EventDispatcher([ConsoleSink(False), sink])
        with contextlib.redirect_stdout(io.StringIO()):
            dispatcher.dispatch(EVENT, rows(), RAW)
            with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
                dispatcher.close()
        self.assertEqual(sink.committed, 1)


class WriteFailureTestCase(IcebergSinkTestCase):
    """A failed write keeps its records. Nothing is ever dropped."""

    def test_a_failed_write_keeps_the_batch_buffered(self):
        sink = self.make_sink(batch_size=2)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR") as logs:
            self.feed(sink, 2)
        self.assertEqual(sink.pending, 2, "the batch must not be discarded")
        self.assertEqual(sink.committed, 0)
        self.assertEqual(sink.commits, 0)
        self.assertTrue(any("catalog unavailable" in line for line in logs.output))
        self.assertTrue(any("NOT committing their Kafka offsets" in line for line in logs.output))

    def test_a_failed_write_never_commits_kafka_offsets(self):
        sink = self.make_sink(batch_size=2, track_offsets=True)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 2)
        self.assertEqual(sink.offset_commits, [])

    def test_records_survive_until_a_write_succeeds(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=2, track_offsets=True, retry_backoff_seconds=5.0)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 2)
        self.assertEqual(sink.pending, 2)

        # Catalog comes back. The retry writes the original records.
        sink._append = lambda rows: sink.appended.append(list(rows))
        clock.advance(5.0)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.tick()

        self.assertEqual(sink.pending, 0)
        self.assertEqual(sink.committed, 2)
        self.assertEqual([r.kafka_offset for r in sink.appended[0]], [0, 1])
        self.assertEqual(sink.offset_commits, [{("s3-events", 0): 2}])

    def test_the_buffer_keeps_growing_across_repeated_failures(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=2, max_flush_attempts=100)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            for round_number in range(3):
                self.feed(sink, 2, start=round_number * 2)
                clock.advance(120.0)
        self.assertEqual(sink.pending, 6, "no record may be discarded on failure")

    def test_a_write_failure_does_not_stop_the_console_sink(self):
        sink = self.make_sink(batch_size=1)
        self.fail_appends_on(sink)
        dispatcher = EventDispatcher([ConsoleSink(False), sink])
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
                dispatcher.dispatch(EVENT, rows(), RAW)
        self.assertEqual(json.loads(out.getvalue()), EVENT)
        self.assertEqual(sink.pending, 1)

    def test_flushing_a_sink_that_was_never_opened_is_fatal_not_a_drop(self):
        sink = IcebergSink(config(batch_size=1))
        with self.assertRaises(SinkFatalError) as ctx:
            self.feed(sink, 1)
        self.assertIn("never opened", str(ctx.exception))
        self.assertEqual(sink.pending, 1)


class BoundedRetryTestCase(IcebergSinkTestCase):
    """Retries must be bounded and observable, never a hot loop."""

    def test_backoff_prevents_a_retry_on_every_tick(self):
        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=2, retry_backoff_seconds=5.0, max_flush_attempts=100
        )
        attempts = {"n": 0}

        def count_and_fail(rows):
            attempts["n"] += 1
            raise RuntimeError("catalog unavailable")

        sink._append = count_and_fail
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 2)
        self.assertEqual(attempts["n"], 1)

        # A hundred ticks inside the backoff window must not retry even once.
        for _ in range(100):
            clock.advance(0.01)
            sink.tick()
        self.assertEqual(attempts["n"], 1)

        clock.advance(5.0)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            sink.tick()
        self.assertEqual(attempts["n"], 2)

    def test_incoming_messages_cannot_bypass_the_backoff(self):
        """A filling buffer must not retry per message and defeat the backoff."""
        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=1, retry_backoff_seconds=30.0,
            max_flush_attempts=100, max_buffered_records=100,
        )
        attempts = {"n": 0}

        def count_and_fail(rows):
            attempts["n"] += 1
            raise RuntimeError("catalog unavailable")

        sink._append = count_and_fail
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 1)
        self.assertEqual(attempts["n"], 1)

        for offset in range(1, 20):
            self.feed(sink, 1, start=offset)
        self.assertEqual(attempts["n"], 1, "each new message must not trigger a retry")
        self.assertEqual(sink.pending, 20)

    def test_backoff_grows_exponentially(self):
        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=1, retry_backoff_seconds=5.0, max_flush_attempts=100
        )
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 1)
        deadlines = [sink._retry_after]
        for _ in range(3):
            clock.now = sink._retry_after
            with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
                sink.tick()
            deadlines.append(sink._retry_after)
        waits = [round(b - a, 3) for a, b in zip(deadlines, deadlines[1:])]
        self.assertEqual(waits, [10.0, 20.0, 40.0])

    def test_backoff_is_capped(self):
        from s3events.config import MAX_RETRY_BACKOFF_SECONDS

        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=1, retry_backoff_seconds=1000.0, max_flush_attempts=100
        )
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 1)
        self.assertEqual(sink._retry_after - clock.now, MAX_RETRY_BACKOFF_SECONDS)

    def test_exhausting_the_attempts_is_fatal_and_keeps_the_records(self):
        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=2, max_flush_attempts=3, track_offsets=True
        )
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 2)          # attempt 1
            clock.advance(1000.0)
            sink.tick()                 # attempt 2

        clock.advance(1000.0)
        with self.assertRaises(SinkFatalError) as ctx:   # attempt 3
            sink.tick()

        message = str(ctx.exception)
        self.assertIn("max_flush_attempts", message)
        self.assertIn("NOT been committed", message)
        self.assertEqual(sink.pending, 2, "records must survive giving up")
        self.assertEqual(sink.offset_commits, [])

    def test_the_failure_count_resets_after_a_success(self):
        clock = FakeClock()
        sink = self.make_sink(clock=clock, batch_size=1, max_flush_attempts=3)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 1)
        self.assertEqual(sink.consecutive_failures, 1)

        sink._append = lambda rows: sink.appended.append(list(rows))
        clock.advance(1000.0)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.tick()
        self.assertEqual(sink.consecutive_failures, 0)

    def test_the_buffer_limit_is_fatal_rather_than_dropping(self):
        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=2, max_buffered_records=6, max_flush_attempts=100
        )
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            with self.assertRaises(SinkFatalError) as ctx:
                self.feed(sink, 10)
        message = str(ctx.exception)
        self.assertIn("max_buffered_records", message)
        self.assertIn("replayed", message)
        self.assertGreaterEqual(sink.pending, 6, "records must not be dropped at the limit")

    def test_a_healthy_sink_never_trips_the_buffer_limit(self):
        sink = self.make_sink(batch_size=2, max_buffered_records=4)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 100)
        self.assertEqual(sink.committed, 100)
        self.assertEqual(sink.pending, 0)


class OffsetCommitTestCase(IcebergSinkTestCase):
    """Iceberg first, Kafka second. Never the other way round."""

    def test_offsets_are_not_committed_before_the_iceberg_commit(self):
        sink = self.make_sink(batch_size=5, track_offsets=True)
        self.feed(sink, 4)
        self.assertEqual(sink.pending, 4)
        self.assertEqual(sink.offset_commits, [], "nothing may be acknowledged yet")
        self.assertEqual(sink.appended, [])

    def test_a_successful_flush_commits_the_next_offset_to_read(self):
        sink = self.make_sink(batch_size=3, track_offsets=True)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 3)          # offsets 0, 1, 2
        # Kafka records the offset to resume from: one past the last written.
        self.assertEqual(sink.offset_commits, [{("s3-events", 0): 3}])

    def test_the_iceberg_append_happens_before_the_offset_commit(self):
        order = []
        sink = self.make_sink(batch_size=2)
        sink._append = lambda rows: order.append("iceberg")
        sink._commit_offsets = lambda offsets: order.append("kafka")
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 2)
        self.assertEqual(order, ["iceberg", "kafka"])

    def test_offsets_are_tracked_per_partition(self):
        sink = self.make_sink(batch_size=4, track_offsets=True)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            for topic, partition, offset in [
                ("s3-events", 0, 10),
                ("s3-events", 1, 5),
                ("s3-events", 0, 11),
                ("s3-events", 1, 3),   # out of order: must not move partition 1 back
            ]:
                batch = flatten_event(EVENT, RAW, topic=topic, partition=partition, offset=offset)
                sink.handle(EVENT, batch, RAW)
        self.assertEqual(
            sink.offset_commits,
            [{("s3-events", 0): 12, ("s3-events", 1): 6}],
            "the highest offset seen per partition, plus one",
        )

    def test_a_lower_offset_does_not_move_the_watermark_backwards(self):
        sink = self.make_sink(batch_size=100, track_offsets=True)
        self.feed(sink, 1, start=50)
        self.feed(sink, 1, start=10)
        self.assertEqual(sink.pending_offsets, {("s3-events", 0): 51})

    def test_rows_without_kafka_coordinates_are_not_tracked(self):
        sink = self.make_sink(batch_size=1, track_offsets=True)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.handle(EVENT, flatten_event(EVENT, RAW), RAW)
        self.assertEqual(sink.committed, 1)
        self.assertEqual(sink.offset_commits, [], "nothing to acknowledge")

    def test_pending_offsets_are_cleared_with_the_buffer(self):
        sink = self.make_sink(batch_size=2, track_offsets=True)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 2)
        self.assertEqual(sink.pending_offsets, {})

    def test_pending_offsets_survive_a_failed_flush(self):
        sink = self.make_sink(batch_size=2, track_offsets=True)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 2)
        self.assertEqual(sink.pending_offsets, {("s3-events", 0): 2})
        self.assertEqual(sink.offset_commits, [])

    def test_a_failing_offset_commit_does_not_lose_the_iceberg_write(self):
        """The data is durable; a failed acknowledgement only risks a replay."""
        sink = self.make_sink(batch_size=2)

        def refuse(offsets):
            raise RuntimeError("kafka commit rejected")

        sink._commit_offsets = refuse
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR") as logs:
            self.feed(sink, 2)
        self.assertEqual(sink.committed, 2)
        self.assertEqual(sink.pending, 0)
        self.assertTrue(any("duplicated" in line for line in logs.output))

    def test_no_committer_means_no_commits_attempted(self):
        sink = self.make_sink(batch_size=2)   # track_offsets=False
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.feed(sink, 2)
        self.assertEqual(sink.committed, 2)


class GracefulShutdownOffsetTestCase(IcebergSinkTestCase):
    def test_shutdown_flushes_then_commits(self):
        order = []
        sink = self.make_sink(batch_size=100)
        sink._append = lambda rows: order.append("iceberg")
        sink._commit_offsets = lambda offsets: order.append("kafka")
        self.feed(sink, 3)
        self.assertEqual(order, [], "nothing written or acknowledged before shutdown")
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.close()
        self.assertEqual(order, ["iceberg", "kafka"])

    def test_shutdown_commits_the_right_offsets(self):
        sink = self.make_sink(batch_size=100, track_offsets=True)
        self.feed(sink, 7)
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.close()
        self.assertEqual(sink.offset_commits, [{("s3-events", 0): 7}])

    def test_a_failed_shutdown_flush_raises_and_commits_nothing(self):
        sink = self.make_sink(batch_size=100, track_offsets=True)
        self.feed(sink, 3)
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            with self.assertRaises(SinkError) as ctx:
                sink.close()
        message = str(ctx.exception)
        self.assertIn("NOT been committed", message)
        self.assertIn("non-zero", message)
        self.assertEqual(sink.offset_commits, [])
        self.assertEqual(sink.pending, 3, "records stay for replay")

    def test_shutdown_bypasses_an_active_retry_backoff(self):
        """One last attempt, even mid-backoff — there is no later chance."""
        clock = FakeClock()
        sink = self.make_sink(
            clock=clock, batch_size=2, retry_backoff_seconds=3600.0, max_flush_attempts=100,
            track_offsets=True,
        )
        self.fail_appends_on(sink)
        with self.assertLogs("s3_event_consumer.iceberg", level="ERROR"):
            self.feed(sink, 2)
        self.assertEqual(sink.pending, 2)

        sink._append = lambda rows: sink.appended.append(list(rows))
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            sink.close()          # deep inside the hour-long backoff
        self.assertEqual(sink.committed, 2)
        self.assertEqual(sink.offset_commits, [{("s3-events", 0): 2}])

    def test_an_unclean_close_is_reported_through_the_dispatcher(self):
        sink = self.make_sink(batch_size=100)
        self.feed(sink, 2)
        self.fail_appends_on(sink)
        dispatcher = EventDispatcher([ConsoleSink(False), sink])
        with self.assertLogs("s3_event_consumer", level="ERROR"):
            self.assertFalse(dispatcher.close(), "close must report the failure")

    def test_a_clean_close_is_reported_through_the_dispatcher(self):
        sink = self.make_sink(batch_size=100)
        self.feed(sink, 2)
        dispatcher = EventDispatcher([ConsoleSink(False), sink])
        with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
            self.assertTrue(dispatcher.close())


class FatalPropagationTestCase(unittest.TestCase):
    """SinkFatalError must reach the poll loop; ordinary errors must not."""

    def test_dispatch_propagates_a_fatal_error(self):
        sink = RecordingSink("fatal")
        sink.handle = mock.Mock(side_effect=SinkFatalError("stop now"))
        with self.assertRaises(SinkFatalError):
            EventDispatcher([sink]).dispatch(EVENT, rows(), RAW)

    def test_tick_propagates_a_fatal_error(self):
        sink = RecordingSink("fatal")
        sink.tick = mock.Mock(side_effect=SinkFatalError("stop now"))
        with self.assertRaises(SinkFatalError):
            EventDispatcher([sink]).tick()

    def test_close_does_not_propagate_a_fatal_error(self):
        """Every sink must still get its chance to shut down."""
        broken = RecordingSink("fatal")
        broken.close = mock.Mock(side_effect=SinkFatalError("stop now"))
        healthy = RecordingSink("healthy")
        dispatcher = EventDispatcher([healthy, broken])
        self.assertFalse(dispatcher.close())
        self.assertEqual(healthy.calls, ["close"])

    def test_an_ordinary_sink_error_is_still_logged_not_raised(self):
        sink = RecordingSink("noisy")
        sink.handle = mock.Mock(side_effect=SinkError("just a hiccup"))
        with self.assertLogs("s3_event_consumer", level="ERROR"):
            EventDispatcher([sink]).dispatch(EVENT, rows(), RAW)


# --------------------------------------------------------------------------- #
# Iceberg sink — startup
# --------------------------------------------------------------------------- #


class FakeNoSuchTableError(Exception):
    pass


class FakeNoSuchNamespaceError(Exception):
    pass


@contextlib.contextmanager
def fake_pyiceberg(catalog):
    """Install a minimal fake PyIceberg into sys.modules for the duration.

    Every submodule the sink imports is stubbed, so these tests exercise the
    startup logic whether or not the real PyIceberg is installed.
    """
    exceptions = mock.Mock()
    exceptions.NoSuchTableError = FakeNoSuchTableError
    exceptions.NoSuchNamespaceError = FakeNoSuchNamespaceError

    catalog_module = mock.Mock()
    catalog_module.load_catalog = mock.Mock(return_value=catalog)

    modules = {
        "pyiceberg": mock.Mock(),
        "pyiceberg.catalog": catalog_module,
        "pyiceberg.exceptions": exceptions,
        "pyiceberg.schema": mock.Mock(),
        "pyiceberg.types": mock.Mock(),
        "pyiceberg.partitioning": mock.Mock(),
        "pyiceberg.transforms": mock.Mock(),
    }
    with mock.patch.dict(sys.modules, modules):
        yield catalog_module


def fake_table(location="s3://warehouse/s3_events/object_events"):
    table = mock.Mock()
    table.location.return_value = location
    table.schema.return_value.as_arrow.return_value = mock.sentinel.arrow_schema
    return table


class OpenTestCase(unittest.TestCase):
    def test_an_existing_table_is_loaded(self):
        table = fake_table()
        catalog = mock.Mock()
        catalog.load_table.return_value = table

        sink = IcebergSink(config())
        with fake_pyiceberg(catalog) as module:
            with self.assertLogs("s3_event_consumer.iceberg", level="INFO") as logs:
                sink.open()

        module.load_catalog.assert_called_once_with("demo", type="rest", uri="http://localhost:8181")
        catalog.load_table.assert_called_once_with("s3_events.object_events")
        catalog.create_table_if_not_exists.assert_not_called()
        self.assertTrue(any("Using existing Iceberg table" in line for line in logs.output))

    def test_a_missing_table_is_created_with_an_explicit_schema(self):
        catalog = mock.Mock()
        catalog.load_table.side_effect = FakeNoSuchTableError("nope")
        catalog.create_table_if_not_exists.return_value = fake_table()

        sink = IcebergSink(config())
        with fake_pyiceberg(catalog):
            with self.assertLogs("s3_event_consumer.iceberg", level="INFO") as logs:
                sink.open()

        catalog.create_namespace_if_not_exists.assert_called_once_with("s3_events")
        _, kwargs = catalog.create_table_if_not_exists.call_args
        self.assertIn("schema", kwargs)
        self.assertIn("partition_spec", kwargs)
        self.assertTrue(any("Created Iceberg table" in line for line in logs.output))

    def test_a_missing_namespace_is_created_too(self):
        catalog = mock.Mock()
        catalog.load_table.side_effect = FakeNoSuchNamespaceError("nope")
        catalog.create_table_if_not_exists.return_value = fake_table()

        sink = IcebergSink(config())
        with fake_pyiceberg(catalog):
            with self.assertLogs("s3_event_consumer.iceberg", level="INFO"):
                sink.open()
        catalog.create_namespace_if_not_exists.assert_called_once_with("s3_events")

    def test_create_if_missing_false_refuses_to_create(self):
        catalog = mock.Mock()
        catalog.load_table.side_effect = FakeNoSuchTableError("nope")

        sink = IcebergSink(config(create_if_missing=False))
        with fake_pyiceberg(catalog):
            with self.assertRaises(SinkError) as ctx:
                sink.open()
        catalog.create_table_if_not_exists.assert_not_called()
        self.assertIn("create_if_missing", str(ctx.exception))

    def test_an_unreachable_catalog_is_a_sink_error(self):
        sink = IcebergSink(config())
        with fake_pyiceberg(mock.Mock()) as module:
            module.load_catalog.side_effect = ConnectionError("connection refused")
            with self.assertRaises(SinkError) as ctx:
                sink.open()
        message = str(ctx.exception)
        self.assertIn("http://localhost:8181", message)
        self.assertIn("connection refused", message)

    def test_a_failing_table_creation_is_a_sink_error(self):
        catalog = mock.Mock()
        catalog.load_table.side_effect = FakeNoSuchTableError("nope")
        catalog.create_table_if_not_exists.side_effect = RuntimeError("access denied")

        sink = IcebergSink(config())
        with fake_pyiceberg(catalog):
            with self.assertRaises(SinkError) as ctx:
                sink.open()
        self.assertIn("access denied", str(ctx.exception))

    def test_an_unexpected_load_error_is_a_sink_error_not_a_create(self):
        catalog = mock.Mock()
        catalog.load_table.side_effect = RuntimeError("catalog is on fire")

        sink = IcebergSink(config())
        with fake_pyiceberg(catalog):
            with self.assertRaises(SinkError) as ctx:
                sink.open()
        catalog.create_table_if_not_exists.assert_not_called()
        self.assertIn("on fire", str(ctx.exception))

    def test_a_missing_pyiceberg_gives_an_actionable_message(self):
        sink = IcebergSink(config())
        real_import = __import__

        def no_pyiceberg(name, *args, **kwargs):
            if name.startswith("pyiceberg"):
                raise ImportError(f"No module named {name!r}")
            return real_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=no_pyiceberg):
            with self.assertRaises(SinkError) as ctx:
                sink.open()

        message = str(ctx.exception)
        self.assertIn("requirements-iceberg.txt", message)
        self.assertIn("standalone", message)

    def test_the_catalog_uri_is_logged_but_credentials_are_not(self):
        catalog = mock.Mock()
        catalog.load_table.return_value = fake_table()
        properties = {
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.secret-access-key": "hunter2",
        }
        sink = IcebergSink(config(catalog_properties=properties))
        with fake_pyiceberg(catalog):
            with self.assertLogs("s3_event_consumer.iceberg", level="INFO") as logs:
                sink.open()
        joined = "\n".join(logs.output)
        self.assertIn("http://localhost:8181", joined)
        self.assertNotIn("hunter2", joined)


try:  # The three schema assertions below need the real PyIceberg types.
    import pyiceberg  # noqa: F401

    HAVE_PYICEBERG = True
except ImportError:  # pragma: no cover - depends on the local install
    HAVE_PYICEBERG = False


@unittest.skipUnless(HAVE_PYICEBERG, "PyIceberg is not installed (optional extra)")
class SchemaTestCase(unittest.TestCase):
    """The schema is explicit, so its shape is worth pinning down."""

    def test_schema_columns_match_the_flattened_row(self):
        from s3events.sinks.iceberg import build_schema

        schema = build_schema()
        names = [field.name for field in schema.fields]
        row = EventRow(
            ingest_time=dt.datetime.now(dt.timezone.utc),
            kafka_topic=None, kafka_partition=None, kafka_offset=None,
            record_index=0, event_name=None, event_time=None, event_source=None,
            bucket=None, object_key=None, object_size=None, object_etag=None,
            raw_event="{}",
        )
        self.assertEqual(names, list(row.as_dict()))

    def test_only_ingest_time_and_raw_event_are_required(self):
        from s3events.sinks.iceberg import build_schema

        required = {f.name for f in build_schema().fields if f.required}
        self.assertEqual(required, {"ingest_time", "raw_event"})

    def test_partitioned_by_ingest_day(self):
        from s3events.sinks.iceberg import build_partition_spec

        spec = build_partition_spec()
        self.assertEqual([f.name for f in spec.fields], ["ingest_time_day"])


# --------------------------------------------------------------------------- #
# Wiring in the entry point
# --------------------------------------------------------------------------- #


class BuildDispatcherTestCase(unittest.TestCase):
    def make_config(self, iceberg=None):
        from s3events.config import AppConfig

        return AppConfig(
            kafka_config={"bootstrap.servers": "h:9092", "group.id": "g"},
            topic="s3-events",
            iceberg=iceberg,
        )

    def test_without_iceberg_only_the_console_sink_is_built(self):
        dispatcher = consumer.build_dispatcher(self.make_config(), color=False)
        self.assertEqual([s.name for s in dispatcher.sinks], ["console"])

    def test_with_iceberg_the_console_sink_comes_first(self):
        dispatcher = consumer.build_dispatcher(self.make_config(config()), color=False)
        self.assertEqual([s.name for s in dispatcher.sinks], ["console", "iceberg"])

    def test_the_console_sink_honours_the_colour_decision(self):
        self.assertTrue(consumer.build_dispatcher(self.make_config(), color=True).sinks[0].color)
        self.assertFalse(consumer.build_dispatcher(self.make_config(), color=False).sinks[0].color)

    def test_no_iceberg_config_means_pyiceberg_is_never_imported(self):
        """The default path must not need PyIceberg installed."""
        real_import = __import__

        def no_pyiceberg(name, *args, **kwargs):
            if name.startswith("pyiceberg") or name == "pyarrow":
                raise AssertionError(f"{name} must not be imported on the default path")
            return real_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=no_pyiceberg):
            dispatcher = consumer.build_dispatcher(self.make_config(), color=False)
            with contextlib.redirect_stdout(io.StringIO()):
                dispatcher.open()
                dispatcher.dispatch(EVENT, rows(), RAW)
                dispatcher.tick()
                dispatcher.close()


if __name__ == "__main__":
    unittest.main()
