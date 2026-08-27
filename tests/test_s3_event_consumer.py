"""Tests for configuration parsing and event rendering.

Run from the repository root with:

    python3 -m unittest discover -s tests -v
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import s3_event_consumer as consumer  # noqa: E402

VALID_CONFIG = {
    "kafka_config": {
        "bootstrap.servers": "10.0.0.10:9092,10.0.0.11:9092",
        "group.id": "s3-event-demo",
        "auto.offset.reset": "earliest",
    },
    "topic": "s3-events",
}


class ConfigTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "config.json"

    def write(self, text):
        self.path.write_text(text, encoding="utf-8")
        return self.path

    def write_json(self, document):
        return self.write(json.dumps(document))

    def assertRejects(self, document, needle):
        with self.assertRaises(consumer.ConfigError) as ctx:
            consumer.load_config(self.write_json(document))
        self.assertIn(needle, str(ctx.exception))

    def test_valid_config(self):
        kafka_conf, topic = consumer.load_config(self.write_json(VALID_CONFIG))
        self.assertEqual(topic, "s3-events")
        self.assertEqual(kafka_conf["group.id"], "s3-event-demo")
        self.assertEqual(kafka_conf["auto.offset.reset"], "earliest")

    def test_topic_is_stripped(self):
        document = json.loads(json.dumps(VALID_CONFIG))
        document["topic"] = "  s3-events  "
        _, topic = consumer.load_config(self.write_json(document))
        self.assertEqual(topic, "s3-events")

    def test_missing_file(self):
        missing = Path(self._tmp.name) / "does-not-exist.json"
        with self.assertRaises(consumer.ConfigError) as ctx:
            consumer.load_config(missing)
        self.assertIn(consumer.EXAMPLE_CONFIG_FILENAME, str(ctx.exception))

    def test_invalid_json(self):
        with self.assertRaises(consumer.ConfigError) as ctx:
            consumer.load_config(self.write('{"topic": "s3-events",}'))
        self.assertIn("not valid JSON", str(ctx.exception))

    def test_top_level_must_be_object(self):
        with self.assertRaises(consumer.ConfigError) as ctx:
            consumer.load_config(self.write("[]"))
        self.assertIn("JSON object", str(ctx.exception))

    def test_missing_kafka_config(self):
        self.assertRejects({"topic": "s3-events"}, "kafka_config")

    def test_empty_kafka_config(self):
        self.assertRejects({"kafka_config": {}, "topic": "s3-events"}, "kafka_config")

    def test_missing_topic(self):
        self.assertRejects({"kafka_config": VALID_CONFIG["kafka_config"]}, "'topic'")

    def test_blank_topic(self):
        self.assertRejects({**VALID_CONFIG, "topic": "   "}, "'topic'")

    def test_missing_bootstrap_servers(self):
        self.assertRejects(
            {"kafka_config": {"group.id": "demo"}, "topic": "s3-events"},
            "bootstrap.servers",
        )

    def test_missing_group_id(self):
        self.assertRejects(
            {"kafka_config": {"bootstrap.servers": "10.0.0.10:9092"}, "topic": "s3-events"},
            "group.id",
        )

    # -- bootstrap.servers must not still contain <ANGLE_BRACKET> placeholders --

    def test_unedited_placeholders_are_rejected(self):
        document = json.loads(json.dumps(VALID_CONFIG))
        document["kafka_config"]["bootstrap.servers"] = "<KAFKA_VIP_1>:<KAFKA_PORT>"
        self.assertRejects(document, "still contains placeholders")

    def test_real_hostnames_are_accepted(self):
        """Values that a name-based check might have wrongly rejected."""
        for servers in (
            "broker.your-company.example:9092",       # contains "your-"
            "kafka.example.com:9092",                 # contains "example.com"
            "[fe80::1]:9092",                         # IPv6 uses square brackets
            "10.0.0.10:19092,10.0.0.11:19092",        # non-conventional port
        ):
            with self.subTest(servers=servers):
                document = json.loads(json.dumps(VALID_CONFIG))
                document["kafka_config"]["bootstrap.servers"] = servers
                kafka_conf, _ = consumer.load_config(self.write_json(document))
                self.assertEqual(kafka_conf["bootstrap.servers"], servers)

    def test_shipped_example_config_is_valid_json_but_must_be_edited(self):
        example = Path(__file__).resolve().parent.parent / consumer.EXAMPLE_CONFIG_FILENAME
        document = json.loads(example.read_text(encoding="utf-8"))
        self.assertIn("kafka_config", document)
        self.assertIn("topic", document)
        with self.assertRaises(consumer.ConfigError):
            consumer.load_config(example)


class RenderTestCase(unittest.TestCase):
    EVENT = {"Records": [{"eventName": "ObjectCreated:Put", "s3": {"object": {"key": "demo.txt"}}}]}

    def test_plain_render_is_pretty_json(self):
        rendered = consumer.render_event(self.EVENT, color=False)
        self.assertEqual(json.loads(rendered), self.EVENT)
        self.assertIn("\n  ", rendered)
        self.assertNotIn("\033[", rendered)

    def test_colour_render_adds_ansi_escapes(self):
        rendered = consumer.render_event(self.EVENT, color=True)
        self.assertIn("\033[", rendered)


class ColorDecisionTestCase(unittest.TestCase):
    class Tty(io.StringIO):
        def isatty(self):
            return True

    def setUp(self):
        patcher = mock.patch.dict(os.environ, {}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_no_color_flag_disables_colour(self):
        with contextlib.redirect_stdout(self.Tty()):
            self.assertFalse(consumer.use_color(True))

    def test_no_color_env_var_disables_colour(self):
        os.environ["NO_COLOR"] = "1"
        with contextlib.redirect_stdout(self.Tty()):
            self.assertFalse(consumer.use_color(False))

    def test_redirected_stdout_disables_colour(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(consumer.use_color(False))

    def test_terminal_enables_colour(self):
        with contextlib.redirect_stdout(self.Tty()):
            self.assertTrue(consumer.use_color(False))


class FakeMessage:
    """Stands in for a confluent_kafka Message in the display path."""

    def __init__(self, value, topic="s3-events", partition=0, offset=17):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class ShowEventTestCase(unittest.TestCase):
    """show_event must never raise, whatever the broker delivers."""

    def show(self, value):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertLogs(consumer.LOG, level="DEBUG") as logs:
                consumer.show_event(FakeMessage(value), color=False)
        return out.getvalue(), logs.output

    def test_valid_event_is_printed(self):
        event = {"Records": [{"eventName": "ObjectCreated:Put"}]}
        stdout, logs = self.show(json.dumps(event).encode("utf-8"))
        self.assertEqual(json.loads(stdout), event)
        self.assertTrue(any("Event received" in line for line in logs))

    def test_malformed_json_is_reported_not_raised(self):
        stdout, logs = self.show(b"not json at all")
        self.assertEqual(stdout, "")
        self.assertTrue(any("not valid JSON" in line for line in logs))
        self.assertTrue(any("s3-events[0]@17" in line for line in logs))

    def test_empty_payload_is_reported(self):
        stdout, logs = self.show(None)
        self.assertEqual(stdout, "")
        self.assertTrue(any("no payload" in line for line in logs))

    def test_non_utf8_payload_is_reported_as_bad_json(self):
        stdout, logs = self.show(b"\xff\xfe\x00binary")
        self.assertEqual(stdout, "")
        self.assertTrue(any("not valid JSON" in line for line in logs))

    def test_long_malformed_payload_is_truncated_in_the_log(self):
        _, logs = self.show(b"x" * 5000)
        self.assertLess(len(logs[0]), 1000)


if __name__ == "__main__":
    unittest.main()
