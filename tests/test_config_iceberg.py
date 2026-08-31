"""Tests for the optional 'iceberg' configuration section.

The load-bearing guarantee here is the first test case: a configuration file
with no 'iceberg' section, or with it disabled, must behave exactly as it did
before Iceberg support existed.

    python3 -m unittest discover -s tests -v
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import s3_event_consumer as consumer  # noqa: E402
from s3events import config as configmod  # noqa: E402

KAFKA_ONLY = {
    "kafka_config": {
        "bootstrap.servers": "10.0.0.10:9092",
        "group.id": "s3-event-demo",
        "auto.offset.reset": "earliest",
    },
    "topic": "s3-events",
}

ICEBERG = {
    "enabled": True,
    "namespace": "s3_events",
    "table": "object_events",
    "batch_size": 50,
    "flush_interval_seconds": 7,
    "catalog": {
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "somekey",
        "s3.secret-access-key": "somesecret",
        "s3.path-style-access": True,
    },
}


class ConfigFileTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "config.json"

    def write_json(self, document):
        self.path.write_text(json.dumps(document), encoding="utf-8")
        return self.path

    def load(self, document):
        return configmod.load_app_config(self.write_json(document))

    def with_iceberg(self, **overrides):
        # Deep copies, so a test that edits a nested value cannot leak into the
        # module-level fixtures and break whichever test runs next.
        iceberg = json.loads(json.dumps(ICEBERG))
        for key, value in overrides.items():
            if value is configmod:  # sentinel meaning "remove this key"
                iceberg.pop(key, None)
            else:
                iceberg[key] = value
        return {**json.loads(json.dumps(KAFKA_ONLY)), "iceberg": iceberg}

    def assertRejects(self, document, needle):
        with self.assertRaises(configmod.ConfigError) as ctx:
            self.load(document)
        self.assertIn(needle, str(ctx.exception))


class WithoutIcebergTestCase(ConfigFileTestCase):
    """Existing behaviour, unchanged."""

    def test_no_iceberg_section_is_valid_and_disabled(self):
        config = self.load(KAFKA_ONLY)
        self.assertIsNone(config.iceberg)
        self.assertFalse(config.iceberg_enabled)
        self.assertEqual(config.topic, "s3-events")
        self.assertEqual(config.kafka_config["group.id"], "s3-event-demo")

    def test_iceberg_disabled_is_the_same_as_absent(self):
        config = self.load(self.with_iceberg(enabled=False))
        self.assertIsNone(config.iceberg)
        self.assertFalse(config.iceberg_enabled)

    def test_disabled_section_is_not_validated(self):
        """A half-finished disabled section must not block startup."""
        document = {**KAFKA_ONLY, "iceberg": {"enabled": False, "catalog": {}}}
        self.assertIsNone(self.load(document).iceberg)

    def test_old_load_config_still_returns_a_pair(self):
        kafka_conf, topic = configmod.load_config(self.write_json(self.with_iceberg()))
        self.assertEqual(topic, "s3-events")
        self.assertEqual(kafka_conf["bootstrap.servers"], "10.0.0.10:9092")

    def test_old_load_config_is_reexported_from_the_entry_point(self):
        self.assertIs(consumer.load_config, configmod.load_config)
        self.assertIs(consumer.ConfigError, configmod.ConfigError)

    def test_kafka_validation_is_unchanged_when_iceberg_is_present(self):
        document = self.with_iceberg()
        document["kafka_config"]["bootstrap.servers"] = "<KAFKA_VIP_1>:<KAFKA_PORT>"
        self.assertRejects(document, "still contains placeholders")


class ValidIcebergTestCase(ConfigFileTestCase):
    def test_full_section_is_parsed(self):
        iceberg = self.load(self.with_iceberg()).iceberg
        self.assertEqual(iceberg.namespace, "s3_events")
        self.assertEqual(iceberg.table, "object_events")
        self.assertEqual(iceberg.table_identifier, "s3_events.object_events")
        self.assertEqual(iceberg.batch_size, 50)
        self.assertEqual(iceberg.flush_interval_seconds, 7.0)
        self.assertTrue(iceberg.create_if_missing)

    def test_enabled_defaults_to_true_when_the_section_is_present(self):
        document = self.with_iceberg(enabled=configmod)
        self.assertIsNotNone(self.load(document).iceberg)

    def test_defaults_are_applied(self):
        document = {
            **KAFKA_ONLY,
            "iceberg": {"catalog": {"type": "rest", "uri": "http://localhost:8181"}},
        }
        iceberg = self.load(document).iceberg
        self.assertEqual(iceberg.namespace, configmod.DEFAULT_NAMESPACE)
        self.assertEqual(iceberg.table, configmod.DEFAULT_TABLE)
        self.assertEqual(iceberg.batch_size, configmod.DEFAULT_BATCH_SIZE)
        self.assertEqual(
            iceberg.flush_interval_seconds, configmod.DEFAULT_FLUSH_INTERVAL_SECONDS
        )

    def test_catalog_properties_pass_straight_through(self):
        properties = self.load(self.with_iceberg()).iceberg.catalog_properties
        self.assertEqual(properties["type"], "rest")
        self.assertEqual(properties["uri"], "http://localhost:8181")
        self.assertEqual(properties["warehouse"], "s3://warehouse/")

    def test_json_booleans_become_pyiceberg_strings(self):
        properties = self.load(self.with_iceberg()).iceberg.catalog_properties
        self.assertEqual(properties["s3.path-style-access"], "true")

    def test_json_numbers_become_strings(self):
        document = self.with_iceberg()
        document["iceberg"]["catalog"]["s3.connect-timeout"] = 30
        properties = self.load(document).iceberg.catalog_properties
        self.assertEqual(properties["s3.connect-timeout"], "30")

    def test_unknown_catalog_properties_are_kept(self):
        document = self.with_iceberg()
        document["iceberg"]["catalog"]["some.future.pyiceberg.option"] = "value"
        properties = self.load(document).iceberg.catalog_properties
        self.assertEqual(properties["some.future.pyiceberg.option"], "value")

    def test_create_if_missing_can_be_turned_off(self):
        iceberg = self.load(self.with_iceberg(create_if_missing=False)).iceberg
        self.assertFalse(iceberg.create_if_missing)

    def test_float_flush_interval_is_accepted(self):
        iceberg = self.load(self.with_iceberg(flush_interval_seconds=0.5)).iceberg
        self.assertEqual(iceberg.flush_interval_seconds, 0.5)


class InvalidIcebergTestCase(ConfigFileTestCase):
    def test_section_must_be_an_object(self):
        self.assertRejects({**KAFKA_ONLY, "iceberg": "yes please"}, "must be a JSON object")

    def test_enabled_must_be_a_boolean(self):
        self.assertRejects(self.with_iceberg(enabled="yes"), "must be true or false")

    def test_catalog_is_required(self):
        self.assertRejects(self.with_iceberg(catalog=configmod), "catalog must be a non-empty")

    def test_catalog_must_not_be_empty(self):
        self.assertRejects(self.with_iceberg(catalog={}), "catalog must be a non-empty")

    def test_catalog_uri_is_required(self):
        self.assertRejects(self.with_iceberg(catalog={"type": "rest"}), "catalog.uri")

    def test_catalog_uri_placeholders_are_rejected(self):
        document = self.with_iceberg()
        document["iceberg"]["catalog"]["uri"] = "<ICEBERG_REST_CATALOG_URL>"
        self.assertRejects(document, "still contains placeholders")

    def test_credential_placeholders_are_rejected(self):
        document = self.with_iceberg()
        document["iceberg"]["catalog"]["s3.secret-access-key"] = "<VAST_S3_SECRET>"
        self.assertRejects(document, "still contains placeholders")

    def test_catalog_values_must_be_scalars(self):
        document = self.with_iceberg()
        document["iceberg"]["catalog"]["nested"] = {"not": "allowed"}
        self.assertRejects(document, "must be a string, number or boolean")

    def test_batch_size_must_be_positive(self):
        self.assertRejects(self.with_iceberg(batch_size=0), "greater than zero")
        self.assertRejects(self.with_iceberg(batch_size=-5), "greater than zero")

    def test_batch_size_must_be_a_whole_number(self):
        self.assertRejects(self.with_iceberg(batch_size=2.5), "whole number")

    def test_batch_size_must_be_a_number(self):
        self.assertRejects(self.with_iceberg(batch_size="lots"), "must be a number")

    def test_batch_size_boolean_is_not_a_number(self):
        self.assertRejects(self.with_iceberg(batch_size=True), "must be a number")

    def test_flush_interval_must_be_positive(self):
        self.assertRejects(self.with_iceberg(flush_interval_seconds=0), "greater than zero")

    def test_namespace_must_not_be_empty(self):
        self.assertRejects(self.with_iceberg(namespace="   "), "must be a non-empty string")

    def test_table_placeholders_are_rejected(self):
        self.assertRejects(self.with_iceberg(table="<TABLE_NAME>"), "still contains placeholders")

    def test_namespace_rejects_characters_that_would_break_the_identifier(self):
        for name in ("has space", "has.dot", "has/slash", "-leading-hyphen", "quote'd"):
            with self.subTest(name=name):
                self.assertRejects(self.with_iceberg(namespace=name), "letters, digits")

    def test_create_if_missing_must_be_a_boolean(self):
        self.assertRejects(self.with_iceberg(create_if_missing="sure"), "must be true or false")


class EnvReferenceTestCase(ConfigFileTestCase):
    """'env:NAME' keeps real credentials out of the configuration file."""

    def test_env_reference_is_resolved(self):
        with mock.patch.dict(os.environ, {"DEMO_SECRET": "s3cr3t"}):
            document = self.with_iceberg()
            document["iceberg"]["catalog"]["s3.secret-access-key"] = "env:DEMO_SECRET"
            properties = self.load(document).iceberg.catalog_properties
        self.assertEqual(properties["s3.secret-access-key"], "s3cr3t")

    def test_unset_env_reference_is_a_clear_error(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            document = self.with_iceberg()
            document["iceberg"]["catalog"]["s3.secret-access-key"] = "env:NOT_SET_ANYWHERE"
            self.assertRejects(document, "NOT_SET_ANYWHERE")

    def test_empty_env_reference_is_rejected(self):
        document = self.with_iceberg()
        document["iceberg"]["catalog"]["s3.secret-access-key"] = "env:"
        self.assertRejects(document, "missing an environment variable name")

    def test_ordinary_values_are_untouched(self):
        self.assertEqual(configmod.resolve_env_reference("plain", "where"), "plain")
        self.assertEqual(
            configmod.resolve_env_reference("environment:not-a-ref", "where"),
            "environment:not-a-ref",
        )


class SecretRedactionTestCase(ConfigFileTestCase):
    """Credentials must not be reachable through anything that gets logged."""

    def test_secret_names_are_recognised(self):
        for name in (
            "s3.secret-access-key",
            "s3.access-key-id",
            "client.secret",
            "token",
            "PASSWORD",
            "gcs.oauth2.token",
            "credential",
        ):
            with self.subTest(name=name):
                self.assertTrue(configmod.is_secret_name(name))

    def test_ordinary_names_are_not_secrets(self):
        for name in ("uri", "type", "warehouse", "s3.endpoint", "s3.region"):
            with self.subTest(name=name):
                self.assertFalse(configmod.is_secret_name(name))

    def test_safe_properties_hide_credentials_but_keep_the_rest(self):
        safe = self.load(self.with_iceberg()).iceberg.safe_properties()
        self.assertEqual(safe["s3.secret-access-key"], configmod.REDACTED)
        self.assertEqual(safe["s3.access-key-id"], configmod.REDACTED)
        self.assertEqual(safe["uri"], "http://localhost:8181")
        self.assertEqual(safe["s3.endpoint"], "http://localhost:9000")

    def test_no_secret_value_appears_in_the_rendered_safe_properties(self):
        rendered = repr(self.load(self.with_iceberg()).iceberg.safe_properties())
        self.assertNotIn("somesecret", rendered)
        self.assertNotIn("somekey", rendered)

    def test_startup_logging_does_not_leak_credentials(self):
        iceberg = self.load(self.with_iceberg()).iceberg
        with self.assertLogs(consumer.LOG, level="DEBUG") as logs:
            consumer.log_iceberg_settings(iceberg)
        joined = "\n".join(logs.output)
        self.assertNotIn("somesecret", joined)
        self.assertNotIn("somekey", joined)
        self.assertIn("s3_events.object_events", joined)


class ShippedConfigFileTestCase(unittest.TestCase):
    """The files committed to the repository must be what they claim to be."""

    ROOT = Path(__file__).resolve().parent.parent

    def test_example_config_iceberg_section_is_present_but_disabled(self):
        document = json.loads(
            (self.ROOT / configmod.EXAMPLE_CONFIG_FILENAME).read_text(encoding="utf-8")
        )
        self.assertIn("iceberg", document)
        self.assertFalse(document["iceberg"]["enabled"])

    def test_example_config_still_requires_editing(self):
        with self.assertRaises(configmod.ConfigError):
            configmod.load_app_config(self.ROOT / configmod.EXAMPLE_CONFIG_FILENAME)

    def test_example_config_holds_no_literal_credentials(self):
        catalog = json.loads(
            (self.ROOT / configmod.EXAMPLE_CONFIG_FILENAME).read_text(encoding="utf-8")
        )["iceberg"]["catalog"]
        for name, value in catalog.items():
            if configmod.is_secret_name(name):
                with self.subTest(name=name):
                    self.assertTrue(
                        value.startswith(configmod.ENV_REFERENCE_PREFIX)
                        or ("<" in value and ">" in value),
                        f"{name} must be a placeholder or an env: reference, got {value!r}",
                    )

    def test_local_demo_config_loads_and_enables_iceberg(self):
        config = configmod.load_app_config(self.ROOT / "s3_consumer_config.local-iceberg.json")
        self.assertTrue(config.iceberg_enabled)
        self.assertEqual(config.iceberg.table_identifier, "s3_events.object_events")
        self.assertEqual(config.iceberg.catalog_properties["uri"], "http://localhost:8181")

    def test_local_demo_config_only_ever_points_at_localhost(self):
        """It ships with credentials, so it must be unusable against anything real."""
        config = configmod.load_app_config(self.ROOT / "s3_consumer_config.local-iceberg.json")
        endpoints = [
            config.kafka_config["bootstrap.servers"],
            config.iceberg.catalog_properties["uri"],
            config.iceberg.catalog_properties["s3.endpoint"],
        ]
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                self.assertIn("localhost", endpoint)


if __name__ == "__main__":
    unittest.main()
