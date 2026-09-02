"""Tests for the demo dashboard helpers and synthetic cascade.

No broker, no catalog, no S3. The live collectors are not exercised here.

    python3 -m unittest tests.test_demo_watch -v
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("demo_watch", ROOT / "scripts" / "demo_watch.py")
assert SPEC is not None and SPEC.loader is not None
watch = importlib.util.module_from_spec(SPEC)
sys.modules["demo_watch"] = watch
SPEC.loader.exec_module(watch)


class ParseUriTests(unittest.TestCase):
    def test_plain_bucket(self):
        self.assertEqual(watch.parse_s3_uri("s3://kmacs-iceberg-bucket/"), ("kmacs-iceberg-bucket", ""))

    def test_nested_prefix(self):
        self.assertEqual(
            watch.parse_s3_uri("s3://bucket/warehouse/path/"),
            ("bucket", "warehouse/path/"),
        )

    def test_s3a(self):
        self.assertEqual(watch.parse_s3_uri("s3a://bucket/pre"), ("bucket", "pre"))

    def test_rejects_non_s3(self):
        with self.assertRaises(ValueError):
            watch.parse_s3_uri("https://example")

    def test_warehouse_data_prefix_matches_aws_ls_example(self):
        bucket, prefix = watch.warehouse_data_prefix(
            "s3://kmacs-iceberg-bucket/", "s3_events", "object_events"
        )
        self.assertEqual(bucket, "kmacs-iceberg-bucket")
        self.assertEqual(prefix, "s3_events/object_events/data/")


class JoinKeyTests(unittest.TestCase):
    def test_drops_empty_and_slashes(self):
        self.assertEqual(watch.join_key("s3_events/", "/object_events", "data"), "s3_events/object_events/data")
        self.assertEqual(watch.join_key("", "data"), "data")


class ScaleAndBarTests(unittest.TestCase):
    def test_nice_scale_leaves_room_to_grow(self):
        self.assertEqual(watch.nice_scale(184), 200)
        self.assertEqual(watch.nice_scale(0), 10)
        self.assertEqual(watch.nice_scale(200), 200)
        self.assertEqual(watch.nice_scale(201), 500)

    def test_shared_scale_keeps_iceberg_shorter_than_source(self):
        width = 20
        scale = watch.nice_scale(184)
        s3 = watch.bar_fill_count(184, scale, width)
        iceberg = watch.bar_fill_count(150, scale, width)
        self.assertEqual(scale, 200)
        self.assertGreater(s3, iceberg)
        self.assertLess(iceberg, width)

    def test_file_blocks_are_one_per_commit(self):
        bar = watch.file_blocks(6, 20, "#", "-")
        self.assertEqual(bar, "######--------------")
        self.assertEqual(bar.count("#"), 6)

    def test_empty_bar(self):
        self.assertEqual(watch.render_bar(None, 10, 8, "#", "-"), "--------")
        self.assertEqual(watch.render_bar(0, 10, 8, "#", "-"), "--------")


class SparklineTests(unittest.TestCase):
    def test_rising_series_ends_high(self):
        line = watch.sparkline([1, 2, 3, 4, 8], width=5, ticks="01234567")
        self.assertEqual(len(line), 5)
        self.assertEqual(line[-1], "7")
        self.assertEqual(line[0], "0")

    def test_pads_short_history(self):
        line = watch.sparkline([3.0], width=4, ticks="abc")
        self.assertEqual(len(line), 4)
        self.assertTrue(line.startswith("   "))

    def test_constant_uses_mid_tick(self):
        line = watch.sparkline([5, 5, 5], width=3, ticks="abcd")
        self.assertEqual(line, "ccc")

    def test_none_is_blank(self):
        line = watch.sparkline([None, None], width=2)
        self.assertEqual(line, "  ")


class FormatTests(unittest.TestCase):
    def test_int_and_missing(self):
        self.assertEqual(watch.format_int(184, 7), "    184")
        self.assertEqual(watch.format_int(1204, 7), "  1,204")
        self.assertEqual(watch.format_int(None, 7), "      -")

    def test_rate(self):
        self.assertIn("/s", watch.format_rate(12.4))
        self.assertIn("0.0/s", watch.format_rate(0.0))
        self.assertIn("--/s", watch.format_rate(None))

    def test_bytes(self):
        self.assertEqual(watch.format_bytes(48), "48B")
        self.assertEqual(watch.format_bytes(2048), "2.0KiB")

    def test_age_and_elapsed(self):
        self.assertEqual(watch.format_age(3), "3s ago")
        self.assertEqual(watch.format_elapsed(134), "02:14")

    def test_rows_per_file_is_batching_proof(self):
        self.assertEqual(watch.rows_per_file(50, 2), 25.0)
        self.assertIsNone(watch.rows_per_file(10, 0))

    def test_strip_ansi(self):
        self.assertEqual(watch.strip_ansi("\033[1m184\033[0m"), "184")


class PyicebergSummaryTests(unittest.TestCase):
    """Reproduce the live dashboard crash: dict(Summary) vs string-key lookup."""

    class FakeSummary:
        """Enough of PyIceberg's Summary to hit key.lower() on a tuple.

        The real class is a pydantic model *and* a Mapping. Pydantic's
        ``__iter__`` yields ``(field, value)`` pairs; Mapping.keys() walks
        that iterator; ``dict(summary)`` then looks up those pairs.
        """

        def __init__(self, extra: dict, operation: str = "append"):
            self.additional_properties = extra
            self.operation = type("Op", (), {"value": operation})()

        def __iter__(self):
            yield ("operation", self.operation)
            yield from self.additional_properties.items()

        def keys(self):
            return list(self)

        def __getitem__(self, key):
            if key.lower() == "operation":
                return self.operation
            return self.additional_properties.get(key)

        def get(self, key, default=None):
            try:
                value = self[key]
            except AttributeError:
                return default
            return default if value is None else value

    def test_dict_on_summary_is_the_live_crash(self):
        summary = self.FakeSummary({"total-records": "40", "added-records": "25"})
        with self.assertRaises(AttributeError) as caught:
            dict(summary)
        self.assertIn("lower", str(caught.exception))

    def test_snapshot_summary_map_reads_counts_without_dict(self):
        summary = self.FakeSummary(
            {"total-records": "40", "total-data-files": "2", "added-records": "25"},
            operation="append",
        )
        mapped = watch.snapshot_summary_map(summary)
        self.assertEqual(mapped["total-records"], "40")
        self.assertEqual(mapped["added-records"], "25")
        self.assertEqual(mapped["operation"], "append")

    def test_plain_dict_summary_still_works(self):
        mapped = watch.snapshot_summary_map({"total-records": 12, "added-records": 12})
        self.assertEqual(mapped["total-records"], 12)

    def test_iceberg_metrics_from_table_does_not_call_dict_on_summary(self):
        summary = self.FakeSummary(
            {"total-records": "40", "total-data-files": "2", "added-records": "15"}
        )

        class Snap:
            def __init__(self, payload):
                self.timestamp_ms = 1_725_000_000_000
                self.summary = payload

        class Table:
            def location(self):
                return "s3://kmacs-iceberg-bucket/s3_events/object_events"

            def snapshots(self):
                return [Snap(summary)]

            def current_snapshot(self):
                return Snap(summary)

        metrics = watch.iceberg_metrics_from_table(Table(), now=1_725_000_000_010)
        self.assertEqual(metrics["rows"], 40)
        self.assertEqual(metrics["data_files"], 2)
        self.assertEqual(metrics["last_added"], 15)
        self.assertEqual(metrics["snapshots"], 1)
        self.assertEqual(metrics["recent"][0][1], "append")
        self.assertEqual(metrics["recent"][0][2], 15)


class SimulatorTests(unittest.TestCase):
    def test_cascade_objects_lead_parquet_trails(self):
        sim = watch.IngestSimulator(batch_size=25, flush_interval=5.0, seed=1)
        for _ in range(20):
            sim.step(dt=5.0, puts=10)
        state = sim.state
        self.assertGreater(state.objects, 0)
        self.assertLessEqual(state.kafka, state.objects)
        self.assertLessEqual(state.rows, state.kafka)
        self.assertEqual(state.files, state.snaps)
        self.assertLess(state.files, state.rows)
        self.assertGreater(state.files, 0)
        # Demo batching: far fewer files than events.
        self.assertLess(state.files * 5, state.rows)

    def test_commit_size_is_batch_or_remainder(self):
        sim = watch.IngestSimulator(batch_size=25, flush_interval=100.0, seed=0)
        while sim.state.files == 0 and sim.state.objects < 200:
            sim.step(dt=0.1, puts=25)
        self.assertGreaterEqual(sim.state.rows, 25)
        self.assertEqual(sim.state.last_added, 25)
        self.assertEqual(sim.state.files, 1)

    def test_iceberg_trails_kafka_inside_a_flush_window(self):
        sim = watch.IngestSimulator(batch_size=25, flush_interval=5.0, seed=1)
        sim.step(dt=5.0, puts=40)
        self.assertGreater(sim.state.kafka, sim.state.rows)
        self.assertEqual(sim.state.last_added, 25)

    def test_as_sample_fills_dashboard_fields(self):
        sim = watch.IngestSimulator(batch_size=10, flush_interval=5.0, seed=1)
        sim.step(1.0, 12)
        sample = sim.as_sample(dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc))
        self.assertEqual(sample.source_objects, 12)
        self.assertIsNotNone(sample.parquet_s3)
        self.assertTrue(sample.source_latest)


class RenderTests(unittest.TestCase):
    def test_plain_line_has_fixed_columns(self):
        now = watch.Sample(
            t=10.0,
            wall=dt.datetime(2026, 9, 1, 23, 58, 10, tzinfo=dt.timezone.utc),
            source_objects=184,
            kafka_end=171,
            kafka_retained=171,
            kafka_lag=13,
            iceberg_rows=150,
            parquet_s3=6,
            iceberg_snaps=6,
        )
        prev = watch.Sample(
            t=5.0,
            wall=dt.datetime(2026, 9, 1, 23, 58, 5, tzinfo=dt.timezone.utc),
            source_objects=120,
            kafka_end=110,
            kafka_retained=110,
            iceberg_rows=100,
            parquet_s3=4,
            iceberg_snaps=4,
        )
        line = watch.render_plain(now, prev)
        self.assertTrue(line.startswith("23:58:10"))
        self.assertIn("184", line)
        self.assertIn("171", line)
        self.assertIn("150", line)
        self.assertIn("6", line)

    def test_tui_names_the_four_stages_and_has_no_ansi_without_color(self):
        style = watch.Style(enabled=False, ascii_mode=True)
        sim = watch.IngestSimulator(batch_size=25, flush_interval=5.0, seed=1)
        history = []
        wall = dt.datetime(2026, 9, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        for i in range(12):
            sim.step(5.0, 10)
            history.append(sim.as_sample(wall + dt.timedelta(seconds=5 * (i + 1))))
        meta = watch.ViewMeta(
            title="vast-iceberg-demo",
            source="s3://kmacs-data-bucket-02",
            topic="s3-events",
            table="s3_events.object_events",
            interval=5.0,
            elapsed=60.0,
            paused=False,
            synthetic=True,
            expect=None,
            batch_size=25,
        )
        frame = watch.render_tui(history, style, cols=100, rows=32, meta=meta)
        self.assertNotIn("\033[", frame)
        for label in ("SOURCE OBJECTS", "KAFKA EVENTS", "ICEBERG ROWS", "PARQUET FILES"):
            self.assertIn(label, frame)
        self.assertIn("kmacs-data-bucket-02", frame)
        self.assertIn("s3-events", frame)
        self.assertIn("rows/file", frame)
        self.assertIn("Parquet grows per Iceberg snapshot", frame)
        # Shared-scale teaching line is present.
        self.assertIn("share scale", frame)

    def test_tui_with_color_uses_ansi_but_not_on_every_character(self):
        style = watch.Style(enabled=True, ascii_mode=True, color256=True)
        sample = watch.Sample(
            t=1.0,
            wall=dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc),
            source_objects=10,
            kafka_retained=8,
            iceberg_rows=0,
            parquet_s3=0,
        )
        meta = watch.ViewMeta(
            title="vast-iceberg-demo",
            source="s3://demo-data",
            topic="s3-events",
            table="s3_events.object_events",
            interval=5.0,
            elapsed=1.0,
            paused=False,
            synthetic=False,
            expect=None,
            batch_size=25,
        )
        frame = watch.render_tui([sample], style, cols=90, rows=24, meta=meta)
        self.assertIn("\033[", frame)
        # Colour is for values and bars, not a different colour on every label.
        self.assertIn("SOURCE OBJECTS", watch.strip_ansi(frame))


class MainSyntheticTests(unittest.TestCase):
    def test_once_synthetic_prints_the_cascade(self):
        buf = io.StringIO()
        with mock.patch.dict("os.environ", {"VAST_KAFKA_GROUP": "vast-iceberg-demo"}):
            with mock.patch("sys.stdout", buf):
                rc = watch.main(
                [
                    "--synthetic",
                    "--once",
                    "--no-color",
                    "--ascii",
                    "--width",
                    "100",
                    "--synthetic-ticks",
                    "16",
                ]
            )
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        self.assertIn("SOURCE OBJECTS", out)
        self.assertIn("KAFKA EVENTS", out)
        self.assertIn("ICEBERG ROWS", out)
        self.assertIn("PARQUET FILES", out)
        self.assertNotIn("\033[", out)
        self.assertNotRegex(out, r"[+-]\d{4,}/s")

    def test_refuses_observer_group_equal_to_demo_group(self):
        buf = io.StringIO()
        with mock.patch.dict("os.environ", {"VAST_KAFKA_GROUP": "vast-iceberg-demo"}):
            with mock.patch("sys.stderr", buf):
                rc = watch.main(["--observer-group", "vast-iceberg-demo", "--synthetic", "--once"])
        self.assertEqual(rc, 2)
        self.assertIn("must not be the demo consumer group", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
