"""Tests for Event Broker topic delete+recreate via vastpy.

No VMS, no broker. The live vastpy client is not imported here.

    python3 -m unittest tests.test_demo_recreate_topic -v
"""

from __future__ import annotations

import importlib.util
import io
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location(
    "demo_recreate_topic", ROOT / "scripts" / "demo_recreate_topic.py"
)
assert SPEC is not None and SPEC.loader is not None
mod = importlib.util.module_from_spec(SPEC)
sys.modules["demo_recreate_topic"] = mod
SPEC.loader.exec_module(mod)


class FakeMissing(Exception):
    def __init__(self, message: str = "not found"):
        super().__init__(message)
        self.status = 404


class FakeDeleteEndpoint:
    def __init__(self, parent: "FakeVms"):
        self.parent = parent

    def delete(self, **kwargs):
        self.parent.deleted.append(kwargs)
        name = kwargs["name"]
        self.parent.store.pop(name, None)


class FakeShow:
    def __init__(self, parent: "FakeVms"):
        self.parent = parent

    def get(self, **kwargs):
        name = kwargs.get("name")
        record = self.parent.store.get(name)
        if record is None:
            raise FakeMissing()
        return dict(record)


class FakeTopics:
    def __init__(self, parent: "FakeVms"):
        self.parent = parent
        self.show = FakeShow(parent)

    def get(self, **kwargs):
        name = kwargs.get("name")
        if name:
            record = self.parent.store.get(name)
            return [dict(record)] if record else []
        return [dict(item) for item in self.parent.store.values()]

    def post(self, **kwargs):
        self.parent.created.append(kwargs)
        name = kwargs["name"]
        self.parent.store[name] = dict(kwargs)
        return dict(kwargs)

    def __getitem__(self, key: str) -> FakeDeleteEndpoint:
        if key != "delete":
            raise KeyError(key)
        return FakeDeleteEndpoint(self.parent)


class FakeVms:
    def __init__(self, topics: list[dict] | None = None):
        self.store = {item["name"]: dict(item) for item in topics or []}
        self.deleted: list[dict] = []
        self.created: list[dict] = []
        self.topics = FakeTopics(self)


class StripAddressTests(unittest.TestCase):
    def test_strips_scheme_and_slash(self):
        self.assertEqual(mod.strip_vms_address("https://vms.example.com/"), "vms.example.com")

    def test_leaves_host_port(self):
        self.assertEqual(mod.strip_vms_address("vms.example.com:443"), "vms.example.com:443")

    def test_http_scheme(self):
        self.assertEqual(mod.strip_vms_address("http://10.0.0.5"), "10.0.0.5")


class SafeNameTests(unittest.TestCase):
    def test_allows_demo_topic(self):
        self.assertEqual(mod.require_safe_name("kmacs-topic-02", "topic"), "kmacs-topic-02")

    def test_rejects_empty_and_wildcard(self):
        with self.assertRaises(mod.ScriptError):
            mod.require_safe_name("", "topic")
        with self.assertRaises(mod.ScriptError):
            mod.require_safe_name("*", "topic")
        with self.assertRaises(mod.ScriptError):
            mod.require_safe_name("db/name", "database")

    def test_rejects_whitespace(self):
        with self.assertRaises(mod.ScriptError):
            mod.require_safe_name("kmacs topic", "topic")


class TopicRecordTests(unittest.TestCase):
    def test_unwraps_results_envelope(self):
        records = mod.iter_topic_records(
            {"results": [{"database_name": "kafkatopics", "name": "kmacs-topic-02"}]}
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "kmacs-topic-02")

    def test_list_passthrough(self):
        records = mod.iter_topic_records([{"name": "a"}, {"name": "b"}])
        self.assertEqual([r["name"] for r in records], ["a", "b"])

    def test_spec_copies_partitions_and_retention(self):
        spec = mod.spec_from_record(
            {
                "name": "kmacs-topic-02",
                "topic_partitions": 3,
                "retention_ms": 21600000,
                "schema_name": "kafka_topics",
            },
            database_name="kafkatopics",
            name="kmacs-topic-02",
            schema_name=None,
            partitions=None,
            retention_ms=None,
        )
        self.assertEqual(spec.topic_partitions, 3)
        self.assertEqual(spec.retention_ms, 21600000)
        self.assertEqual(spec.schema_name, "kafka_topics")

    def test_spec_overrides_win(self):
        spec = mod.spec_from_record(
            {"name": "t", "topic_partitions": 3, "retention_ms": 1},
            database_name="db",
            name="t",
            schema_name="s",
            partitions=1,
            retention_ms=604800000,
        )
        self.assertEqual(spec.topic_partitions, 1)
        self.assertEqual(spec.retention_ms, 604800000)
        self.assertEqual(spec.schema_name, "s")

    def test_missing_defaults(self):
        spec = mod.default_spec(
            database_name="db",
            name="t",
            schema_name=None,
            partitions=None,
            retention_ms=None,
        )
        self.assertEqual(spec.topic_partitions, 1)
        self.assertEqual(spec.retention_ms, mod.DEFAULT_RETENTION_MS)


class WaitTests(unittest.TestCase):
    def test_true_immediately(self):
        self.assertTrue(mod.wait_until(lambda: True, timeout=0, interval=0, sleeper=lambda _: None))

    def test_timeout(self):
        clock = iter([0.0, 0.0, 1.0]).__next__
        self.assertFalse(
            mod.wait_until(lambda: False, timeout=1, interval=0, sleeper=lambda _: None, clock=clock)
        )


class RecreateFlowTests(unittest.TestCase):
    def test_dry_run_does_not_mutate(self):
        client = FakeVms(
            [
                {
                    "database_name": "kafkatopics",
                    "name": "kmacs-topic-02",
                    "topic_partitions": 1,
                    "retention_ms": 604800000,
                }
            ]
        )
        spec = mod.spec_from_record(
            client.store["kmacs-topic-02"],
            database_name="kafkatopics",
            name="kmacs-topic-02",
            schema_name=None,
            partitions=None,
            retention_ms=None,
        )
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            mod.recreate_topic(
                client,
                client.store["kmacs-topic-02"],
                spec,
                confirm=False,
                timeout=0,
            )
        self.assertEqual(client.deleted, [])
        self.assertEqual(client.created, [])
        self.assertIn("kmacs-topic-02", client.store)
        self.assertIn("WOULD", buf.getvalue())

    def test_confirm_deletes_then_creates_with_same_partitions(self):
        client = FakeVms(
            [
                {
                    "database_name": "kafkatopics",
                    "name": "kmacs-topic-02",
                    "topic_partitions": 4,
                    "retention_ms": 21600000,
                    "schema_name": "kafka_topics",
                }
            ]
        )
        spec = mod.spec_from_record(
            client.store["kmacs-topic-02"],
            database_name="kafkatopics",
            name="kmacs-topic-02",
            schema_name=None,
            partitions=None,
            retention_ms=None,
        )
        with mock.patch("sys.stdout", io.StringIO()):
            mod.recreate_topic(client, client.store["kmacs-topic-02"], spec, confirm=True, timeout=0)
        self.assertEqual(len(client.deleted), 1)
        self.assertEqual(client.deleted[0]["name"], "kmacs-topic-02")
        self.assertEqual(client.deleted[0]["database_name"], "kafkatopics")
        self.assertEqual(client.deleted[0]["schema_name"], "kafka_topics")
        self.assertEqual(len(client.created), 1)
        self.assertEqual(client.created[0]["topic_partitions"], 4)
        self.assertEqual(client.created[0]["retention_ms"], 21600000)
        self.assertEqual(client.created[0]["name"], "kmacs-topic-02")
        self.assertIn("kmacs-topic-02", client.store)

    def test_missing_topic_creates_only(self):
        client = FakeVms()
        spec = mod.default_spec(
            database_name="kafkatopics",
            name="kmacs-topic-02",
            schema_name=None,
            partitions=None,
            retention_ms=None,
        )
        with mock.patch("sys.stdout", io.StringIO()):
            mod.recreate_topic(client, None, spec, confirm=True, timeout=0)
        self.assertEqual(client.deleted, [])
        self.assertEqual(client.created[0]["topic_partitions"], 1)
        self.assertEqual(client.created[0]["retention_ms"], mod.DEFAULT_RETENTION_MS)

    def test_delete_uses_indexed_path_not_http_method_on_topics(self):
        client = FakeVms([{"name": "kmacs-topic-02", "database_name": "kafkatopics"}])
        spec = mod.TopicSpec(database_name="kafkatopics", name="kmacs-topic-02", topic_partitions=1)
        mod.delete_topic(client, spec)
        self.assertEqual(client.deleted[0]["name"], "kmacs-topic-02")


class SettingsTests(unittest.TestCase):
    def _env(self, **extra):
        base = {
            "VAST_KAFKA_TOPIC": "kmacs-topic-02",
            "VAST_KAFKA_DATABASE": "kafkatopics",
            "VAST_VMS_ADDRESS": "https://vms.example.com/",
            "VAST_VMS_USER": "admin",
            "VAST_VMS_PASSWORD": "secret",
            "VAST_KAFKA_GROUP": "vast-iceberg-demo",
            "VAST_KAFKA_BROKER": "broker:9092",
        }
        base.update(extra)
        return base

    def test_dry_run_default_and_address_strip(self):
        args = mod.parse_args([])
        with mock.patch.dict(os.environ, self._env(), clear=True):
            settings = mod.settings_from_args(args)
        self.assertFalse(settings.confirm)
        self.assertEqual(settings.address, "vms.example.com")
        self.assertEqual(settings.topic, "kmacs-topic-02")
        self.assertEqual(settings.database_name, "kafkatopics")

    def test_token_excludes_password(self):
        args = mod.parse_args(["--confirm"])
        with mock.patch.dict(os.environ, self._env(VAST_VMS_TOKEN="tok", VAST_VMS_USER="", VAST_VMS_PASSWORD=""), clear=True):
            settings = mod.settings_from_args(args)
        self.assertTrue(settings.confirm)
        self.assertEqual(settings.token, "tok")
        self.assertIsNone(settings.user)
        self.assertIsNone(settings.password)

    def test_missing_database_refused(self):
        args = mod.parse_args([])
        env = self._env()
        del env["VAST_KAFKA_DATABASE"]
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(mod.ScriptError) as raised:
                mod.settings_from_args(args)
        self.assertIn("VAST_KAFKA_DATABASE", str(raised.exception))

    def test_retention_below_minimum_refused(self):
        args = mod.parse_args(["--retention-ms", "1000"])
        with mock.patch.dict(os.environ, self._env(), clear=True):
            with self.assertRaises(mod.ScriptError):
                mod.settings_from_args(args)

    def test_vastpy_cli_env_names(self):
        args = mod.parse_args([])
        with mock.patch.dict(
            os.environ,
            {
                "VAST_KAFKA_TOPIC": "s3-events",
                "VAST_KAFKA_DATABASE": "kafkatopics",
                "VMS_ADDRESS": "vms",
                "VMS_TOKEN": "tok",
            },
            clear=True,
        ):
            settings = mod.settings_from_args(args)
        self.assertEqual(settings.address, "vms")
        self.assertEqual(settings.token, "tok")


class MainTests(unittest.TestCase):
    def test_main_dry_run_uses_client_without_mutating(self):
        client = FakeVms(
            [
                {
                    "database_name": "kafkatopics",
                    "name": "kmacs-topic-02",
                    "topic_partitions": 1,
                    "retention_ms": 604800000,
                }
            ]
        )
        env = {
            "VAST_KAFKA_TOPIC": "kmacs-topic-02",
            "VAST_KAFKA_DATABASE": "kafkatopics",
            "VAST_VMS_ADDRESS": "vms.example.com",
            "VAST_VMS_USER": "admin",
            "VAST_VMS_PASSWORD": "secret",
            "VAST_KAFKA_GROUP": "vast-iceberg-demo",
            "VAST_KAFKA_BROKER": "broker:9092",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(mod, "open_vms_client", return_value=client):
                with mock.patch("sys.stdout", io.StringIO()):
                    rc = mod.main([])
        self.assertEqual(rc, 0)
        self.assertEqual(client.deleted, [])
        self.assertEqual(client.created, [])

    def test_main_confirm_recreates_and_deletes_group(self):
        client = FakeVms(
            [
                {
                    "database_name": "kafkatopics",
                    "name": "kmacs-topic-02",
                    "topic_partitions": 1,
                    "retention_ms": 604800000,
                }
            ]
        )
        env = {
            "VAST_KAFKA_TOPIC": "kmacs-topic-02",
            "VAST_KAFKA_DATABASE": "kafkatopics",
            "VAST_VMS_ADDRESS": "vms.example.com",
            "VAST_VMS_USER": "admin",
            "VAST_VMS_PASSWORD": "secret",
            "VAST_KAFKA_GROUP": "vast-iceberg-demo",
            "VAST_KAFKA_BROKER": "broker:9092",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(mod, "open_vms_client", return_value=client):
                with mock.patch.object(mod, "delete_consumer_group", return_value="deleted") as group:
                    with mock.patch("sys.stdout", io.StringIO()):
                        rc = mod.main(["--confirm"])
        self.assertEqual(rc, 0)
        self.assertEqual(len(client.deleted), 1)
        self.assertEqual(len(client.created), 1)
        group.assert_called_once_with("broker:9092", "vast-iceberg-demo")

    def test_keep_group_skips_admin(self):
        client = FakeVms(
            [{"database_name": "kafkatopics", "name": "kmacs-topic-02", "topic_partitions": 1}]
        )
        env = {
            "VAST_KAFKA_TOPIC": "kmacs-topic-02",
            "VAST_KAFKA_DATABASE": "kafkatopics",
            "VAST_VMS_ADDRESS": "vms.example.com",
            "VAST_VMS_TOKEN": "tok",
            "VAST_KAFKA_GROUP": "vast-iceberg-demo",
            "VAST_KAFKA_BROKER": "broker:9092",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(mod, "open_vms_client", return_value=client):
                with mock.patch.object(mod, "delete_consumer_group") as group:
                    with mock.patch("sys.stdout", io.StringIO()):
                        rc = mod.main(["--confirm", "--keep-group"])
        self.assertEqual(rc, 0)
        group.assert_not_called()

    def test_open_vms_client_explains_missing_sdk(self):
        settings = mod.Settings(
            address="vms",
            user="admin",
            password="secret",
            token=None,
            tenant=None,
            cert_file=None,
            cert_server_name=None,
            database_name="kafkatopics",
            topic="kmacs-topic-02",
            schema_name=None,
            partitions=None,
            retention_ms=None,
            confirm=False,
            keep_group=False,
            group="",
            broker="",
            timeout=0,
        )
        with mock.patch.dict(sys.modules, {"vastpy": None}):
            with self.assertRaises(mod.ScriptError) as raised:
                mod.open_vms_client(settings)
        self.assertIn("pip install vastpy", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
