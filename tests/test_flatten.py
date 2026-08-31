"""Tests for flattening S3 event notifications into table rows.

The payload schema comes from VAST and varies by release, so the rule these
tests enforce is: whatever arrives, produce a row and never raise.

    python3 -m unittest discover -s tests -v
"""

import datetime as dt
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from s3events.flatten import EventRow, flatten_event, parse_timestamp  # noqa: E402

COMPLETE_EVENT = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "vast:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2026-08-31T09:03:41.123Z",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": "demo-data"},
                "object": {"key": "hello.txt", "size": 742, "eTag": "d41d8cd98f00b204"},
            },
        }
    ]
}

FIXED_TIME = dt.datetime(2026, 8, 31, 12, 0, 0, tzinfo=dt.timezone.utc)


def flatten(event, **kwargs):
    """Flatten with a fixed ingest time and the payload's own JSON as the raw text."""
    kwargs.setdefault("ingest_time", FIXED_TIME)
    return flatten_event(event, json.dumps(event), **kwargs)


class CompleteEventTestCase(unittest.TestCase):
    def setUp(self):
        self.rows = flatten(
            COMPLETE_EVENT, topic="s3-events", partition=2, offset=4711
        )

    def test_one_record_makes_one_row(self):
        self.assertEqual(len(self.rows), 1)
        self.assertIsInstance(self.rows[0], EventRow)

    def test_kafka_coordinates_are_carried(self):
        row = self.rows[0]
        self.assertEqual(row.kafka_topic, "s3-events")
        self.assertEqual(row.kafka_partition, 2)
        self.assertEqual(row.kafka_offset, 4711)
        self.assertEqual(row.record_index, 0)

    def test_s3_fields_are_flattened(self):
        row = self.rows[0]
        self.assertEqual(row.event_name, "ObjectCreated:Put")
        self.assertEqual(row.event_source, "vast:s3")
        self.assertEqual(row.bucket, "demo-data")
        self.assertEqual(row.object_key, "hello.txt")
        self.assertEqual(row.object_size, 742)
        self.assertEqual(row.object_etag, "d41d8cd98f00b204")

    def test_event_time_is_parsed_to_utc(self):
        self.assertEqual(
            self.rows[0].event_time,
            dt.datetime(2026, 8, 31, 9, 3, 41, 123000, tzinfo=dt.timezone.utc),
        )

    def test_ingest_time_is_used(self):
        self.assertEqual(self.rows[0].ingest_time, FIXED_TIME)

    def test_raw_event_round_trips(self):
        self.assertEqual(json.loads(self.rows[0].raw_event), COMPLETE_EVENT)

    def test_as_dict_matches_the_iceberg_column_names(self):
        expected = {
            "ingest_time", "kafka_topic", "kafka_partition", "kafka_offset",
            "record_index", "event_name", "event_time", "event_source",
            "bucket", "object_key", "object_size", "object_etag", "raw_event",
        }
        self.assertEqual(set(self.rows[0].as_dict()), expected)


class MissingOptionalFieldsTestCase(unittest.TestCase):
    """Nothing in an S3 event payload is guaranteed to be there."""

    def test_missing_size_and_etag_become_none(self):
        event = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {"bucket": {"name": "b"}, "object": {"key": "k"}},
                }
            ]
        }
        row = flatten(event)[0]
        self.assertIsNone(row.object_size)
        self.assertIsNone(row.object_etag)
        self.assertEqual(row.object_key, "k")
        self.assertEqual(row.bucket, "b")

    def test_missing_s3_section_leaves_object_fields_none(self):
        row = flatten({"Records": [{"eventName": "s3:TestEvent"}]})[0]
        self.assertEqual(row.event_name, "s3:TestEvent")
        self.assertIsNone(row.bucket)
        self.assertIsNone(row.object_key)
        self.assertIsNone(row.object_size)

    def test_missing_event_time_is_none(self):
        row = flatten({"Records": [{"eventName": "ObjectCreated:Put"}]})[0]
        self.assertIsNone(row.event_time)

    def test_unparseable_event_time_does_not_lose_the_row(self):
        event = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "eventTime": "yesterday afternoon",
                    "s3": {"bucket": {"name": "b"}, "object": {"key": "k"}},
                }
            ]
        }
        row = flatten(event)[0]
        self.assertIsNone(row.event_time)
        self.assertEqual(row.object_key, "k")

    def test_empty_strings_are_treated_as_absent(self):
        event = {"Records": [{"eventName": "   ", "s3": {"bucket": {"name": ""}}}]}
        row = flatten(event)[0]
        self.assertIsNone(row.event_name)
        self.assertIsNone(row.bucket)

    def test_no_kafka_coordinates_supplied(self):
        row = flatten(COMPLETE_EVENT)[0]
        self.assertIsNone(row.kafka_topic)
        self.assertIsNone(row.kafka_partition)
        self.assertIsNone(row.kafka_offset)


class MalformedStructureTestCase(unittest.TestCase):
    """Structurally wrong payloads still produce exactly one row, and never raise."""

    def assertOneEmptyRow(self, event):
        rows = flatten(event)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertIsNone(row.bucket)
        self.assertIsNone(row.object_key)
        self.assertEqual(row.ingest_time, FIXED_TIME)
        return row

    def test_records_is_not_a_list(self):
        row = self.assertOneEmptyRow({"Records": "nope"})
        self.assertIsNone(row.event_name)

    def test_records_entries_are_not_objects(self):
        rows = flatten({"Records": ["a string", 42, None]})
        self.assertEqual(len(rows), 3)
        self.assertEqual([r.record_index for r in rows], [0, 1, 2])
        self.assertTrue(all(r.object_key is None for r in rows))

    def test_empty_records_list_still_records_the_message(self):
        rows = flatten({"Records": []})
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0].raw_event), {"Records": []})

    def test_payload_is_a_bare_list(self):
        self.assertOneEmptyRow([1, 2, 3])

    def test_payload_is_a_bare_scalar(self):
        for value in ("just a string", 42, True, None):
            with self.subTest(value=value):
                rows = flatten_event(value, json.dumps(value), ingest_time=FIXED_TIME)
                self.assertEqual(len(rows), 1)
                self.assertIsNone(rows[0].object_key)

    def test_bucket_and_object_are_the_wrong_type(self):
        event = {"Records": [{"s3": {"bucket": "demo-data", "object": ["k"]}}]}
        self.assertOneEmptyRow(event)

    def test_s3_section_is_the_wrong_type(self):
        self.assertOneEmptyRow({"Records": [{"s3": "not an object"}]})

    def test_no_records_key_treats_the_payload_as_one_record(self):
        event = {
            "eventName": "ObjectCreated:Put",
            "s3": {"bucket": {"name": "b"}, "object": {"key": "k"}},
        }
        row = flatten(event)[0]
        self.assertEqual(row.event_name, "ObjectCreated:Put")
        self.assertEqual(row.object_key, "k")


class MultipleRecordsTestCase(unittest.TestCase):
    def test_each_record_becomes_a_row_with_its_index(self):
        event = {
            "Records": [
                {"s3": {"bucket": {"name": "b"}, "object": {"key": "one"}}},
                {"s3": {"bucket": {"name": "b"}, "object": {"key": "two"}}},
                {"s3": {"bucket": {"name": "b"}, "object": {"key": "three"}}},
            ]
        }
        rows = flatten(event, topic="t", partition=0, offset=9)
        self.assertEqual([r.object_key for r in rows], ["one", "two", "three"])
        self.assertEqual([r.record_index for r in rows], [0, 1, 2])
        # All rows from one message share the Kafka offset and the raw payload.
        self.assertEqual({r.kafka_offset for r in rows}, {9})
        self.assertEqual(len({r.raw_event for r in rows}), 1)

    def test_all_rows_share_one_ingest_time(self):
        event = {"Records": [{"s3": {}}, {"s3": {}}]}
        rows = flatten_event(event, "{}")
        self.assertEqual(rows[0].ingest_time, rows[1].ingest_time)


class FieldSpellingTestCase(unittest.TestCase):
    """Alternative spellings seen across S3 implementations and VAST releases."""

    def test_alternative_size_and_etag_spellings(self):
        for size_key in ("size", "objectSize", "Size", "contentLength"):
            for etag_key in ("eTag", "etag", "ETag"):
                with self.subTest(size=size_key, etag=etag_key):
                    event = {
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "b"},
                                    "object": {"key": "k", size_key: 5, etag_key: "abc"},
                                }
                            }
                        ]
                    }
                    row = flatten(event)[0]
                    self.assertEqual(row.object_size, 5)
                    self.assertEqual(row.object_etag, "abc")

    def test_size_sent_as_a_numeric_string(self):
        event = {"Records": [{"s3": {"object": {"key": "k", "size": " 4096 "}}}]}
        self.assertEqual(flatten(event)[0].object_size, 4096)

    def test_size_that_is_not_a_number_is_dropped(self):
        event = {"Records": [{"s3": {"object": {"key": "k", "size": "large"}}}]}
        self.assertIsNone(flatten(event)[0].object_size)

    def test_boolean_size_is_not_mistaken_for_an_integer(self):
        event = {"Records": [{"s3": {"object": {"key": "k", "size": True}}}]}
        self.assertIsNone(flatten(event)[0].object_size)

    def test_size_too_large_for_a_64_bit_column_is_dropped(self):
        event = {"Records": [{"s3": {"object": {"key": "k", "size": 2**70}}}]}
        self.assertIsNone(flatten(event)[0].object_size)

    def test_url_encoded_object_key_is_decoded(self):
        event = {"Records": [{"s3": {"object": {"key": "reports/q1+report%20final.pdf"}}}]}
        self.assertEqual(flatten(event)[0].object_key, "reports/q1 report final.pdf")


class TimestampTestCase(unittest.TestCase):
    def test_trailing_z_is_utc(self):
        self.assertEqual(
            parse_timestamp("2026-08-31T09:03:41Z"),
            dt.datetime(2026, 8, 31, 9, 3, 41, tzinfo=dt.timezone.utc),
        )

    def test_offset_is_normalised_to_utc(self):
        self.assertEqual(
            parse_timestamp("2026-08-31T11:03:41+02:00"),
            dt.datetime(2026, 8, 31, 9, 3, 41, tzinfo=dt.timezone.utc),
        )

    def test_naive_timestamp_is_assumed_utc(self):
        self.assertEqual(
            parse_timestamp("2026-08-31T09:03:41"),
            dt.datetime(2026, 8, 31, 9, 3, 41, tzinfo=dt.timezone.utc),
        )

    def test_nanosecond_precision_is_truncated_not_rejected(self):
        parsed = parse_timestamp("2026-08-31T09:03:41.123456789Z")
        self.assertEqual(parsed.microsecond, 123456)

    def test_single_digit_fraction_is_padded(self):
        self.assertEqual(parse_timestamp("2026-08-31T09:03:41.5Z").microsecond, 500000)

    def test_epoch_seconds(self):
        self.assertEqual(
            parse_timestamp(1767171821),
            dt.datetime.fromtimestamp(1767171821, tz=dt.timezone.utc),
        )

    def test_epoch_milliseconds(self):
        self.assertEqual(
            parse_timestamp(1767171821000),
            dt.datetime.fromtimestamp(1767171821, tz=dt.timezone.utc),
        )

    def test_unparseable_values_return_none(self):
        for value in (None, "", "   ", "yesterday", [], {}, True, "2026-13-45T99:99:99Z"):
            with self.subTest(value=value):
                self.assertIsNone(parse_timestamp(value))


if __name__ == "__main__":
    unittest.main()
