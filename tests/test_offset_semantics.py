"""Tests for how the consumer acknowledges Kafka.

The rule these enforce: **nothing is acknowledged to Kafka that is not already
durable in Iceberg**, and with Iceberg off, librdkafka's own offset handling is
left completely alone.

Everything is mocked — no broker, no catalog, no Docker.

    python3 -m unittest discover -s tests -v
"""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import s3_event_consumer as consumer  # noqa: E402
from s3events.config import AppConfig, IcebergConfig  # noqa: E402
from s3events.sinks import SinkError  # noqa: E402

KAFKA_ONLY = {
    "kafka_config": {"bootstrap.servers": "10.0.0.10:9092", "group.id": "demo"},
    "topic": "s3-events",
}
WITH_ICEBERG = {
    **KAFKA_ONLY,
    "iceberg": {
        "enabled": True,
        "batch_size": 2,
        "catalog": {"type": "rest", "uri": "http://localhost:8181"},
    },
}


def iceberg_config(**overrides):
    defaults = dict(
        namespace="s3_events",
        table="object_events",
        catalog_name="demo",
        catalog_properties={"type": "rest", "uri": "http://localhost:8181"},
        batch_size=2,
    )
    defaults.update(overrides)
    return IcebergConfig(**defaults)


# --------------------------------------------------------------------------- #
# enable.auto.commit
# --------------------------------------------------------------------------- #


class AutoCommitTestCase(unittest.TestCase):
    """Automatic commits are the acknowledge-then-lose window we are closing."""

    def test_manual_commits_disable_auto_commit(self):
        conf = consumer.apply_manual_offset_commits({"bootstrap.servers": "h:9092"})
        self.assertIs(conf["enable.auto.commit"], False)

    def test_the_original_config_is_not_mutated(self):
        original = {"bootstrap.servers": "h:9092"}
        consumer.apply_manual_offset_commits(original)
        self.assertNotIn("enable.auto.commit", original)

    def test_other_settings_are_passed_through(self):
        conf = consumer.apply_manual_offset_commits(
            {"bootstrap.servers": "h:9092", "sasl.mechanism": "PLAIN"}
        )
        self.assertEqual(conf["sasl.mechanism"], "PLAIN")

    def test_an_explicit_true_is_overridden_with_a_warning(self):
        with self.assertLogs(consumer.LOG, level="WARNING") as logs:
            conf = consumer.apply_manual_offset_commits(
                {"bootstrap.servers": "h:9092", "enable.auto.commit": True}
            )
        self.assertIs(conf["enable.auto.commit"], False)
        self.assertTrue(any("enable.auto.commit" in line for line in logs.output))

    def test_an_explicit_false_is_not_warned_about(self):
        conf = consumer.apply_manual_offset_commits(
            {"bootstrap.servers": "h:9092", "enable.auto.commit": False}
        )
        self.assertIs(conf["enable.auto.commit"], False)


# --------------------------------------------------------------------------- #
# The committer callback
# --------------------------------------------------------------------------- #


class OffsetCommitterTestCase(unittest.TestCase):
    def test_offsets_become_topic_partitions_and_commit_synchronously(self):
        fake_consumer = mock.Mock()
        commit = consumer.make_offset_committer(fake_consumer)
        commit({("s3-events", 0): 42, ("s3-events", 1): 7})

        _, kwargs = fake_consumer.commit.call_args
        self.assertIs(kwargs["asynchronous"], False)
        self.assertEqual(
            sorted((tp.topic, tp.partition, tp.offset) for tp in kwargs["offsets"]),
            [("s3-events", 0, 42), ("s3-events", 1, 7)],
        )

    def test_committing_nothing_is_still_a_no_op_call(self):
        fake_consumer = mock.Mock()
        consumer.make_offset_committer(fake_consumer)({})
        _, kwargs = fake_consumer.commit.call_args
        self.assertEqual(kwargs["offsets"], [])


# --------------------------------------------------------------------------- #
# main(), with Kafka and PyIceberg mocked out
# --------------------------------------------------------------------------- #


class MainTestCase(unittest.TestCase):
    """Drives main() end to end against a fake broker and a fake sink."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        # main() installs a SIGINT handler and sets a module global.
        self.addCleanup(setattr, consumer, "_interrupted", False)
        # main() calls logging.basicConfig, which would attach a stderr handler
        # and spray the suite's output with startup banners. The log *records*
        # still reach assertLogs, which is what these tests inspect.
        patcher = mock.patch("logging.basicConfig")
        patcher.start()
        self.addCleanup(patcher.stop)

    def write_config(self, document):
        path = Path(self._tmp.name) / "config.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        return path

    @contextlib.contextmanager
    def run_main(self, document, sink=None, argv_extra=(), consumer_factory=None):
        """Run main() with Consumer and the Iceberg sink replaced.

        Yields (exit_code_holder, fake_consumer). The poll loop stops after one
        empty poll, standing in for an immediate Ctrl-C.
        """
        path = self.write_config(document)
        fake_consumer = mock.Mock()
        fake_consumer.poll.return_value = None

        def stop_after_first_poll(*args, **kwargs):
            consumer._interrupted = True
            return None

        fake_consumer.poll.side_effect = stop_after_first_poll

        factory = consumer_factory or (lambda conf: fake_consumer)
        result = {}

        patches = [mock.patch.object(consumer, "Consumer", side_effect=factory)]
        if sink is not None:
            patches.append(
                mock.patch("s3events.sinks.iceberg.IcebergSink", return_value=sink)
            )

        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            consumer._interrupted = False
            result["code"] = consumer.main([*argv_extra, "--config", str(path), "--no-color"])

        yield result, fake_consumer

    # -- Iceberg disabled: existing behaviour, untouched -------------------- #

    def test_without_iceberg_auto_commit_is_left_alone(self):
        seen = {}

        def capture(conf):
            seen.update(conf)
            fake = mock.Mock()
            fake.poll.side_effect = lambda *a, **k: (
                setattr(consumer, "_interrupted", True),
                None,
            )[1]
            return fake

        with self.run_main(KAFKA_ONLY, consumer_factory=capture) as (result, _):
            pass
        self.assertEqual(result["code"], 0)
        self.assertNotIn(
            "enable.auto.commit",
            seen,
            "the console-only path must not touch librdkafka's offset handling",
        )

    def test_without_iceberg_no_offsets_are_committed_by_hand(self):
        with self.run_main(KAFKA_ONLY) as (result, fake_consumer):
            pass
        self.assertEqual(result["code"], 0)
        fake_consumer.commit.assert_not_called()

    def test_without_iceberg_nothing_mentions_iceberg_on_stderr(self):
        """The compatibility gate: normal mode looks exactly as it always did."""
        path = self.write_config(KAFKA_ONLY)
        fake = mock.Mock()
        fake.poll.side_effect = lambda *a, **k: (
            setattr(consumer, "_interrupted", True),
            None,
        )[1]

        with mock.patch.object(consumer, "Consumer", return_value=fake):
            with self.assertLogs(consumer.LOG, level="DEBUG") as logs:
                consumer._interrupted = False
                with contextlib.redirect_stdout(io.StringIO()):
                    code = consumer.main(["--config", str(path), "--no-color"])

        self.assertEqual(code, 0)
        joined = "\n".join(logs.output)
        self.assertNotIn("Iceberg", joined)
        self.assertNotIn("iceberg", joined)

    def test_without_iceberg_the_dispatcher_has_only_the_console_sink(self):
        config = AppConfig(kafka_config=KAFKA_ONLY["kafka_config"], topic="s3-events")
        dispatcher = consumer.build_dispatcher(config, color=False)
        self.assertEqual([s.name for s in dispatcher.sinks], ["console"])

    # -- Iceberg enabled: manual commits ------------------------------------ #

    def test_with_iceberg_auto_commit_is_disabled(self):
        seen = {}

        def capture(conf):
            seen.update(conf)
            fake = mock.Mock()
            fake.poll.side_effect = lambda *a, **k: (
                setattr(consumer, "_interrupted", True),
                None,
            )[1]
            return fake

        sink = FakeIcebergSink()
        with self.run_main(WITH_ICEBERG, sink=sink, consumer_factory=capture) as (result, _):
            pass
        self.assertEqual(result["code"], 0)
        self.assertIs(seen["enable.auto.commit"], False)

    def test_with_iceberg_the_sink_gets_a_committer_wired_to_the_consumer(self):
        fake_consumer = mock.Mock()
        committer = consumer.make_offset_committer(fake_consumer)
        config = AppConfig(
            kafka_config=KAFKA_ONLY["kafka_config"],
            topic="s3-events",
            iceberg=iceberg_config(),
        )
        dispatcher = consumer.build_dispatcher(config, color=False, offset_committer=committer)
        iceberg_sink = dispatcher.sinks[1]
        self.assertEqual(iceberg_sink.name, "iceberg")

        # Calling the sink's committer must reach the Kafka consumer.
        iceberg_sink._commit_offsets({("s3-events", 0): 5})
        fake_consumer.commit.assert_called_once()

    def test_check_mode_never_commits_anything(self):
        config = AppConfig(
            kafka_config=KAFKA_ONLY["kafka_config"],
            topic="s3-events",
            iceberg=iceberg_config(),
        )
        dispatcher = consumer.build_dispatcher(config, color=False)
        self.assertIsNone(dispatcher.sinks[1]._commit_offsets)

    # -- exit codes --------------------------------------------------------- #

    def test_a_clean_shutdown_exits_zero(self):
        with self.run_main(WITH_ICEBERG, sink=FakeIcebergSink()) as (result, _):
            pass
        self.assertEqual(result["code"], 0)

    def test_a_failed_shutdown_flush_exits_non_zero(self):
        sink = FakeIcebergSink(close_error=SinkError("3 record(s) could not be written"))
        with self.assertLogs(consumer.LOG, level="ERROR"):
            with self.run_main(WITH_ICEBERG, sink=sink) as (result, fake_consumer):
                pass
        self.assertEqual(result["code"], 1, "unwritten records must fail the run")
        fake_consumer.commit.assert_not_called()

    def test_a_fatal_sink_error_during_consumption_exits_non_zero(self):
        from s3events.sinks import SinkFatalError

        sink = FakeIcebergSink(tick_error=SinkFatalError("retries exhausted"))
        with self.assertLogs(consumer.LOG, level="ERROR") as logs:
            with self.run_main(WITH_ICEBERG, sink=sink) as (result, fake_consumer):
                pass
        self.assertEqual(result["code"], 1)
        fake_consumer.commit.assert_not_called()
        self.assertTrue(any("retries exhausted" in line for line in logs.output))

    def test_the_kafka_consumer_is_always_closed(self):
        sink = FakeIcebergSink(close_error=SinkError("nope"))
        with self.assertLogs(consumer.LOG, level="ERROR"):
            with self.run_main(WITH_ICEBERG, sink=sink) as (_, fake_consumer):
                pass
        fake_consumer.close.assert_called_once()

    def test_sinks_are_closed_before_the_kafka_consumer(self):
        """The final flush and its offset commit must happen while Kafka is alive."""
        order = []
        sink = FakeIcebergSink(on_close=lambda: order.append("sink"))
        fake_consumer = mock.Mock()
        fake_consumer.poll.side_effect = lambda *a, **k: (
            setattr(consumer, "_interrupted", True),
            None,
        )[1]
        fake_consumer.close.side_effect = lambda: order.append("kafka")

        path = self.write_config(WITH_ICEBERG)
        with mock.patch.object(consumer, "Consumer", return_value=fake_consumer):
            with mock.patch("s3events.sinks.iceberg.IcebergSink", return_value=sink):
                consumer._interrupted = False
                with contextlib.redirect_stdout(io.StringIO()):
                    consumer.main(["--config", str(path), "--no-color"])
        self.assertEqual(order, ["sink", "kafka"])


class FakeIcebergSink:
    """Stands in for IcebergSink in main()-level tests."""

    name = "iceberg"

    def __init__(self, close_error=None, tick_error=None, on_close=None):
        self.close_error = close_error
        self.tick_error = tick_error
        self.on_close = on_close
        self.opened = False

    def open(self):
        self.opened = True

    def handle(self, event, rows, raw_payload):
        pass

    def tick(self):
        if self.tick_error is not None:
            raise self.tick_error

    def close(self):
        if self.on_close is not None:
            self.on_close()
        if self.close_error is not None:
            raise self.close_error


if __name__ == "__main__":
    unittest.main()
