#!/usr/bin/env python3
"""Live terminal dashboard for the VAST S3 to Iceberg demo.

The story this screen is built to tell is a cascade, not four independent
counters. An object lands in the watched bucket, VAST publishes a Kafka event,
the consumer appends a row to Iceberg, and a Parquet file appears only when
Iceberg commits a snapshot (batch of 25 or 5s in the demo config). Watch the
trailing stages chase the source: that lag is the demo.

Observes only. It never joins the demo consumer group, never commits a Kafka
offset, and never writes to S3 or Iceberg.

    set -a; . ./docker/demo.env; set +a
    python3 scripts/demo_watch.py

Three terminals:

    1. python3 s3_event_consumer.py --config s3_consumer_config.json
    2. python3 scripts/demo_watch.py
    3. python3 scripts/demo_ingest.py --count 200 --interval 0.15

A TTY gets a redrawn full-screen view. A pipe, ``--plain``, or ``NO_COLOR``
gets a vmstat-style line every interval. ``--synthetic`` animates the same
layout without a cluster, so you can judge the screen before the customer
arrives.

Keys (TUI):  q quit   r session baseline   p pause   + / - interval
"""

from __future__ import annotations

import argparse
import datetime as dt
import heapq
import logging
import os
import random
import re
import select
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_CONFIG = "s3_consumer_config.json"
DEFAULT_INTERVAL = 5.0
DEFAULT_OBSERVER_GROUP = "vast-iceberg-demo-watch"
HISTORY = 72
LATEST_KEYS = 6
LATEST_SNAPS = 5
NICE_SCALES = (
    10, 20, 50, 100, 200, 500, 1000, 2000, 5000,
    10_000, 20_000, 50_000, 100_000, 200_000, 500_000, 1_000_000,
)

SPARK = "▁▂▃▄▅▆▇█"
SPARK_ASCII = " .:-=+*#"
BAR_FILL = "█"
BAR_EMPTY = "░"
BAR_FILL_ASCII = "#"
BAR_EMPTY_ASCII = "-"
ARROW = "→"
ARROW_ASCII = "->"

ANSI_RE = re.compile(r"\033\[[0-9;]*[A-Za-z]")
RESET = "\033[0m"
ENTER_ALT = "\033[?1049h\033[?25l"
LEAVE_ALT = "\033[?25h\033[?1049l"
SYNC_BEGIN = "\033[?2026h"
SYNC_END = "\033[?2026l"
CLEAR = "\033[H\033[J"

LOG = logging.getLogger("demo_watch")

# --------------------------------------------------------------------------- #
# Formatting
# --------------------------------------------------------------------------- #


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def visible_len(text: str) -> int:
    return len(strip_ansi(text))


def pad(text: str, width: int, align: str = "left") -> str:
    extra = max(0, width - visible_len(text))
    if align == "right":
        return (" " * extra) + text
    return text + (" " * extra)


def truncate(text: str, width: int, ascii_mode: bool = False) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width == 1:
        return text[-1]
    mark = "..." if ascii_mode else "…"
    keep = width - len(mark)
    if keep <= 0:
        return mark[:width]
    return mark + text[-keep:]


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, key-prefix) from an s3:// or s3a:// URI."""
    raw = (uri or "").strip()
    if raw.startswith("s3a://"):
        raw = "s3://" + raw[6:]
    if not raw.startswith("s3://"):
        raise ValueError(f"not an s3 URI: {uri!r}")
    rest = raw[5:]
    bucket, _, key = rest.partition("/")
    if not bucket:
        raise ValueError(f"s3 URI has no bucket: {uri!r}")
    return bucket, key.lstrip("/")


def join_key(*parts: str) -> str:
    """Join S3 key parts, dropping empty pieces and extra slashes."""
    chunks = []
    for part in parts:
        cleaned = (part or "").strip("/")
        if cleaned:
            chunks.append(cleaned)
    return "/".join(chunks)


def warehouse_data_prefix(warehouse: str, namespace: str, table: str) -> tuple[str, str]:
    """Bucket and prefix matching ``aws s3 ls s3://bucket/ns/table/data/``."""
    bucket, base = parse_s3_uri(warehouse)
    prefix = join_key(base, namespace, table, "data")
    return bucket, (prefix + "/") if prefix else ""


def warehouse_metadata_prefix(warehouse: str, namespace: str, table: str) -> tuple[str, str]:
    bucket, base = parse_s3_uri(warehouse)
    prefix = join_key(base, namespace, table, "metadata")
    return bucket, (prefix + "/") if prefix else ""


def nice_scale(value: float) -> int:
    """Smallest 'scope' stop at or above value, so a bar can still grow."""
    target = max(0.0, float(value))
    for stop in NICE_SCALES:
        if target <= stop:
            return stop
    return max(1, int(target * 1.05) + 1)


def sparkline(values: list[float | None], width: int, ticks: str = SPARK) -> str:
    """Fit the last ``width`` points into a one-line sparkline.

    Missing samples are blank. A constant series uses the mid tick so a live
    idle pipeline does not look like 'no data'.
    """
    if width <= 0:
        return ""
    series = list(values[-width:])
    if len(series) < width:
        series = [None] * (width - len(series)) + series
    present = [v for v in series if v is not None]
    if not present:
        return " " * width
    lo = min(present)
    hi = max(present)
    if hi <= lo:
        mid = ticks[len(ticks) // 2]
        return "".join(mid if v is not None else " " for v in series)
    last = len(ticks) - 1
    chars = []
    span = hi - lo
    for value in series:
        if value is None:
            chars.append(" ")
            continue
        idx = int(round((value - lo) / span * last))
        chars.append(ticks[max(0, min(last, idx))])
    return "".join(chars)


def bar_fill_count(value: float | None, scale: float, width: int) -> int:
    if width <= 0 or scale <= 0 or value is None or value <= 0:
        return 0
    filled = int(round(width * min(float(value), scale) / scale))
    return max(0, min(width, filled))


def render_bar(
    value: float | None,
    scale: float,
    width: int,
    filled_char: str = BAR_FILL,
    empty_char: str = BAR_EMPTY,
) -> str:
    n = bar_fill_count(value, scale, width)
    return (filled_char * n) + (empty_char * (width - n))


def file_blocks(
    count: int | None,
    width: int,
    filled_char: str = BAR_FILL,
    empty_char: str = BAR_EMPTY,
) -> str:
    """One block per Parquet file until the row is full, then a scaled bar.

    A percentage bar of '6 files' against itself is always full. Counting in
    blocks makes each Iceberg commit a visible tick.
    """
    if width <= 0:
        return ""
    if count is None or count <= 0:
        return empty_char * width
    if count <= width:
        return (filled_char * count) + (empty_char * (width - count))
    return render_bar(count, nice_scale(count), width, filled_char, empty_char)


def format_int(value: int | None, width: int = 7) -> str:
    if value is None:
        return "-".rjust(width)
    return f"{value:,}".rjust(width)


def format_rate(rate: float | None, width: int = 8) -> str:
    if rate is None:
        return "--/s".rjust(width)
    if abs(rate) < 0.05:
        return "0.0/s".rjust(width)
    if abs(rate) < 99.5:
        return f"{rate:+.1f}/s".rjust(width)
    return f"{rate:+.0f}/s".rjust(width)


def format_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    if n < 1024:
        return f"{n}B"
    for unit, size in (("KiB", 1024), ("MiB", 1024**2), ("GiB", 1024**3)):
        if n < size * 1024 or unit == "GiB":
            val = n / size
            if val < 10:
                return f"{val:.1f}{unit}"
            return f"{val:.0f}{unit}"
    return f"{n}B"


def format_age(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 0:
        seconds = 0.0
    if seconds < 60:
        return f"{seconds:.0f}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60):02d}s ago"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes:02d}m ago"


def format_elapsed(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def rate_between(earlier: int | None, later: int | None, dt_s: float) -> float | None:
    if earlier is None or later is None or dt_s <= 0:
        return None
    return (later - earlier) / dt_s


def series_rates(
    history: list[Any], getter: Callable[[Any], int | None]
) -> list[float | None]:
    rates: list[float | None] = []
    for prev, cur in zip(history, history[1:]):
        rates.append(rate_between(getter(prev), getter(cur), cur.t - prev.t))
    return rates


def rows_per_file(rows: int | None, files: int | None) -> float | None:
    if rows is None or files is None or files <= 0:
        return None
    return rows / files


# --------------------------------------------------------------------------- #
# Colour
# --------------------------------------------------------------------------- #


@dataclass
class Style:
    enabled: bool
    ascii_mode: bool
    color256: bool = False

    @property
    def fill(self) -> str:
        return BAR_FILL_ASCII if self.ascii_mode else BAR_FILL

    @property
    def empty(self) -> str:
        return BAR_EMPTY_ASCII if self.ascii_mode else BAR_EMPTY

    @property
    def ticks(self) -> str:
        return SPARK_ASCII if self.ascii_mode else SPARK

    @property
    def arrow(self) -> str:
        return ARROW_ASCII if self.ascii_mode else ARROW

    def _wrap(self, codes: str, text: str) -> str:
        if not self.enabled or not text:
            return text
        return f"{codes}{text}{RESET}"

    def bold(self, text: str) -> str:
        return self._wrap("\033[1m", text)

    def dim(self, text: str) -> str:
        return self._wrap("\033[2m", text)

    def reverse(self, text: str) -> str:
        return self._wrap("\033[7m", text)

    def label(self, text: str) -> str:
        return self._wrap("\033[38;5;245m" if self.color256 else "\033[90m", text)

    def value(self, text: str) -> str:
        return self._wrap("\033[1;38;5;255m" if self.color256 else "\033[1;37m", text)

    def up(self, text: str) -> str:
        return self._wrap("\033[1;38;5;114m" if self.color256 else "\033[1;32m", text)

    def lag(self, text: str) -> str:
        return self._wrap("\033[1;38;5;221m" if self.color256 else "\033[1;33m", text)

    def err(self, text: str) -> str:
        return self._wrap("\033[1;38;5;167m" if self.color256 else "\033[1;31m", text)

    def bar(self, text: str) -> str:
        return self._wrap("\033[38;5;74m" if self.color256 else "\033[36m", text)

    def bar_files(self, text: str) -> str:
        return self._wrap("\033[38;5;109m" if self.color256 else "\033[36m", text)

    def muted_bar(self, text: str) -> str:
        return self._wrap("\033[38;5;238m" if self.color256 else "\033[90m", text)

    def head(self, text: str) -> str:
        return self._wrap("\033[1;38;5;255m" if self.color256 else "\033[1m", text)

    def ok(self, text: str) -> str:
        return self._wrap("\033[38;5;114m" if self.color256 else "\033[32m", text)

    def rule(self, width: int) -> str:
        ch = "-" if self.ascii_mode else "─"
        return self.dim(ch * max(0, width))


def detect_style(no_color: bool, ascii_mode: bool) -> Style:
    enabled = True
    if no_color or os.environ.get("NO_COLOR"):
        enabled = False
    elif not sys.stdout.isatty():
        enabled = False
    elif os.environ.get("TERM", "") in ("", "dumb"):
        enabled = False
    term = os.environ.get("TERM", "")
    colorterm = os.environ.get("COLORTERM", "")
    color256 = ("256" in term) or ("truecolor" in colorterm) or ("24bit" in colorterm)
    if os.environ.get("LC_ALL", os.environ.get("LANG", "")).lower().endswith("ascii"):
        ascii_mode = True
    return Style(enabled=enabled, ascii_mode=ascii_mode, color256=color256)


# --------------------------------------------------------------------------- #
# Samples
# --------------------------------------------------------------------------- #


@dataclass
class Sample:
    t: float
    wall: dt.datetime
    source_objects: int | None = None
    source_bytes: int | None = None
    source_latest: list[tuple[dt.datetime, str, int]] = field(default_factory=list)
    kafka_end: int | None = None
    kafka_retained: int | None = None
    kafka_committed: int | None = None
    kafka_lag: int | None = None
    kafka_partitions: int = 0
    iceberg_rows: int | None = None
    iceberg_files: int | None = None
    iceberg_snaps: int | None = None
    last_added: int | None = None
    last_commit_age_s: float | None = None
    recent_snaps: list[tuple[dt.datetime, str, int | None]] = field(default_factory=list)
    parquet_s3: int | None = None
    metadata_s3: int | None = None
    latest_parquet: str | None = None
    table_location: str | None = None
    errors: dict[str, str] = field(default_factory=dict)
    poll_ms: float = 0.0


def kafka_events(sample: Sample) -> int | None:
    if sample.kafka_retained is not None:
        return sample.kafka_retained
    return sample.kafka_end


# --------------------------------------------------------------------------- #
# Synthetic ingest (no cluster)
# --------------------------------------------------------------------------- #


@dataclass
class SimState:
    objects: int = 0
    kafka: int = 0
    buffered: int = 0
    rows: int = 0
    files: int = 0
    snaps: int = 0
    since_flush: float = 0.0
    last_added: int = 0
    last_commit_age_s: float | None = None


class IngestSimulator:
    """A tiny model of the real pipeline, used by ``--synthetic``.

    Objects appear first. Kafka trails by a couple of events. Iceberg only
    advances when a batch fills or the flush interval elapses, and each commit
    produces one Parquet file. That is the cascade the dashboard is meant to
    make obvious.
    """

    def __init__(
        self,
        batch_size: int = 25,
        flush_interval: float = 5.0,
        seed: int = 1,
    ) -> None:
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.rng = random.Random(seed)
        self.state = SimState()
        self.clock = 0.0
        self.commits: list[tuple[float, int]] = []

    def step(self, dt: float, puts: int) -> SimState:
        state = self.state
        puts = max(0, int(puts))
        dt = max(0.0, float(dt))
        self.clock += dt
        state.since_flush += dt
        if state.last_commit_age_s is not None:
            state.last_commit_age_s += dt
        # Time passes first, so a leftover below batch_size is committed on the
        # flush interval *before* this interval's new objects arrive. That is
        # what makes Iceberg trail Kafka on the dashboard: the remainder of
        # the previous tick is what you see still in flight.
        if state.buffered > 0 and state.since_flush >= self.flush_interval:
            self._commit(state.buffered)
        state.objects += puts
        pending = state.objects - state.kafka
        if pending <= 0:
            delivered = 0
        elif pending <= 2:
            delivered = pending
        else:
            delivered = max(0, pending - self.rng.randint(0, 2))
        state.kafka += delivered
        state.buffered += delivered
        while state.buffered >= self.batch_size:
            self._commit(self.batch_size)
        return state

    def _commit(self, n: int) -> None:
        state = self.state
        state.buffered -= n
        state.rows += n
        state.files += 1
        state.snaps += 1
        state.last_added = n
        state.since_flush = 0.0
        state.last_commit_age_s = 0.0
        self.commits.append((self.clock, n))

    def as_sample(self, wall: dt.datetime, poll_ms: float = 0.0) -> Sample:
        state = self.state
        latest = []
        for i in range(min(LATEST_KEYS, state.objects)):
            idx = state.objects - i
            latest.append(
                (
                    wall - dt.timedelta(seconds=i),
                    f"demo/object-{idx:05d}.csv",
                    48,
                )
            )
        snaps = []
        for committed_at, added in self.commits[-LATEST_SNAPS:]:
            age = max(0.0, self.clock - committed_at)
            snaps.append((wall - dt.timedelta(seconds=age), "append", added))
        parquet = None
        if state.files:
            parquet = (
                f"s3_events/object_events/data/ingest_time_day="
                f"{wall.date().isoformat()}/00000-0-synthetic-{state.files:04d}.parquet"
            )
        return Sample(
            t=self.clock,
            wall=wall,
            source_objects=state.objects,
            source_bytes=state.objects * 48,
            source_latest=list(reversed(latest)),
            kafka_end=state.kafka,
            kafka_retained=state.kafka,
            kafka_committed=state.rows,
            kafka_lag=max(0, state.kafka - state.rows),
            kafka_partitions=1,
            iceberg_rows=state.rows,
            iceberg_files=state.files,
            iceberg_snaps=state.snaps,
            last_added=state.last_added or None,
            last_commit_age_s=state.last_commit_age_s,
            recent_snaps=snaps,
            parquet_s3=state.files,
            metadata_s3=state.snaps * 3 if state.snaps else 0,
            latest_parquet=parquet,
            table_location="s3://iceberg-warehouse/s3_events/object_events",
            poll_ms=poll_ms,
        )


# --------------------------------------------------------------------------- #
# Live collectors
# --------------------------------------------------------------------------- #


def _quiet_loggers() -> None:
    for name in (
        "pyiceberg",
        "botocore",
        "boto3",
        "urllib3",
        "s3fs",
        "fsspec",
        "aiobotocore",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def open_s3_client(endpoint: str, access_key: str, secret_key: str, region: str) -> Any:
    from botocore.config import Config

    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    try:
        import boto3
    except ImportError:
        from botocore.session import get_session

        return get_session().create_client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=cfg,
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=cfg,
    )


def observer_kafka_conf(kafka_config: dict[str, Any], observer_group: str) -> dict[str, Any]:
    """librdkafka settings that cannot steal work from the demo consumer.

    A unique group, auto-commit off, and no subscribe/poll. Watermark reads do
    not join a group; we still refuse the demo group.id so a mistake cannot
    trigger a rebalance in front of a customer.
    """
    conf = {
        key: value
        for key, value in kafka_config.items()
        if key not in ("error_cb", "logger")
    }
    conf["group.id"] = observer_group
    conf["enable.auto.commit"] = False
    conf["auto.offset.reset"] = "latest"
    return conf


def _list_prefix(
    client: Any,
    bucket: str,
    prefix: str,
    latest_n: int = 0,
    suffix: str | None = None,
) -> tuple[int, int, list[tuple[dt.datetime, str, int]], str | None]:
    paginator = client.get_paginator("list_objects_v2")
    count = 0
    total = 0
    latest: list[tuple[dt.datetime, str, int]] = []
    newest_key: str | None = None
    newest_time: dt.datetime | None = None
    kwargs: dict[str, Any] = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix
    for page in paginator.paginate(**kwargs):
        for item in page.get("Contents") or []:
            key = item["Key"]
            if key.endswith("/"):
                continue
            if suffix is not None and not key.endswith(suffix):
                continue
            count += 1
            size = int(item.get("Size") or 0)
            total += size
            modified = item.get("LastModified")
            if latest_n and modified is not None:
                entry = (modified, key, size)
                if len(latest) < latest_n:
                    heapq.heappush(latest, entry)
                else:
                    heapq.heappushpop(latest, entry)
            if modified is not None and (newest_time is None or modified > newest_time):
                newest_time = modified
                newest_key = key
    latest_sorted = sorted(latest) if latest else []
    return count, total, latest_sorted, newest_key


class LiveCluster:
    """Read-only handles to VAST S3, the Event Broker, and the Iceberg catalog."""

    def __init__(
        self,
        config: Any,
        source_bucket: str,
        source_prefix: str,
        observer_group: str,
        timeout: float,
    ) -> None:
        self._config = config
        self._source_bucket = source_bucket
        self._source_prefix = source_prefix
        self._observer_group = observer_group
        self._timeout = timeout
        self._s3 = None
        self._consumer = None
        self._admin = None
        self._catalog = None
        self._table = None
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="demo-watch")
        self._iceberg_ident = None
        self._warehouse = None
        self._namespace = os.environ.get("ICEBERG_NAMESPACE", "s3_events")
        self._table_name = os.environ.get("ICEBERG_TABLE", "object_events")
        if config.iceberg is not None:
            self._namespace = config.iceberg.namespace
            self._table_name = config.iceberg.table
            self._iceberg_ident = config.iceberg.table_identifier
            self._warehouse = config.iceberg.catalog_properties.get("warehouse")
        if not self._warehouse:
            self._warehouse = os.environ.get("ICEBERG_WAREHOUSE")

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def warehouse(self) -> str | None:
        return self._warehouse

    def open(self) -> None:
        _quiet_loggers()
        settings = _s3_settings(self._config)
        missing = [k for k, v in settings.items() if not v and k != "region"]
        if missing:
            raise RuntimeError(
                "missing S3 settings: "
                + ", ".join(missing)
                + ". Source docker/demo.env: set -a; . ./docker/demo.env; set +a"
            )
        self._s3 = open_s3_client(
            settings["endpoint"],
            settings["access_key"],
            settings["secret_key"],
            settings["region"],
        )
        from confluent_kafka import Consumer
        from confluent_kafka.admin import AdminClient

        kconf = observer_kafka_conf(self._config.kafka_config, self._observer_group)
        self._consumer = Consumer(kconf)
        self._admin = AdminClient(kconf)
        if self._config.iceberg is not None:
            try:
                from pyiceberg.catalog import load_catalog
            except ImportError as exc:
                LOG.warning("PyIceberg not installed; Iceberg metrics disabled (%s)", exc)
            else:
                self._catalog = load_catalog(
                    self._config.iceberg.catalog_name,
                    **self._config.iceberg.catalog_properties,
                )

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:  # noqa: BLE001 - best-effort shutdown
                pass
            self._consumer = None
        self._pool.shutdown(wait=False, cancel_futures=True)

    def sample(self) -> Sample:
        started = time.monotonic()
        wall = dt.datetime.now(dt.timezone.utc)
        futs = {
            "source": self._pool.submit(self._collect_source),
            "kafka": self._pool.submit(self._collect_kafka),
            "iceberg": self._pool.submit(self._collect_iceberg),
            "warehouse": self._pool.submit(self._collect_warehouse),
        }
        got: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for name, fut in futs.items():
            try:
                got[name] = fut.result(timeout=self._timeout)
            except Exception as exc:  # noqa: BLE001 - isolate a dead layer
                errors[name] = f"{type(exc).__name__}: {exc}"
                got[name] = None
        src = got.get("source") or {}
        kaf = got.get("kafka") or {}
        ice = got.get("iceberg") or {}
        wh = got.get("warehouse") or {}
        for layer, payload in (("source", src), ("kafka", kaf), ("iceberg", ice), ("warehouse", wh)):
            err = payload.get("error") if isinstance(payload, dict) else None
            if err:
                errors[layer] = err
        return Sample(
            t=time.monotonic(),
            wall=wall,
            source_objects=src.get("objects"),
            source_bytes=src.get("bytes"),
            source_latest=src.get("latest") or [],
            kafka_end=kaf.get("end"),
            kafka_retained=kaf.get("retained"),
            kafka_committed=kaf.get("committed"),
            kafka_lag=kaf.get("lag"),
            kafka_partitions=kaf.get("partitions") or 0,
            iceberg_rows=ice.get("rows"),
            iceberg_files=ice.get("data_files"),
            iceberg_snaps=ice.get("snapshots"),
            last_added=ice.get("last_added"),
            last_commit_age_s=ice.get("last_commit_age_s"),
            recent_snaps=ice.get("recent") or [],
            parquet_s3=wh.get("parquet"),
            metadata_s3=wh.get("metadata"),
            latest_parquet=wh.get("latest_parquet"),
            table_location=ice.get("location") or wh.get("location"),
            errors=errors,
            poll_ms=(time.monotonic() - started) * 1000.0,
        )

    def _collect_source(self) -> dict[str, Any]:
        count, total, latest, _ = _list_prefix(
            self._s3,
            self._source_bucket,
            self._source_prefix,
            latest_n=LATEST_KEYS,
        )
        return {"objects": count, "bytes": total, "latest": latest}

    def _collect_kafka(self) -> dict[str, Any]:
        from confluent_kafka import OFFSET_INVALID, TopicPartition

        topic = self._config.topic
        md = self._consumer.list_topics(topic, timeout=self._timeout)
        topic_md = md.topics.get(topic)
        if topic_md is None:
            return {"error": f"topic {topic!r} not in metadata"}
        if topic_md.error is not None:
            return {"error": str(topic_md.error)}
        high_sum = 0
        low_sum = 0
        partitions = list(topic_md.partitions)
        for pid in partitions:
            low, high = self._consumer.get_watermark_offsets(
                TopicPartition(topic, pid),
                timeout=self._timeout,
                cached=False,
            )
            low_sum += int(low)
            high_sum += int(high)
        committed_sum = None
        lag = None
        try:
            from confluent_kafka.admin import ConsumerGroupTopicPartitions

            group = self._config.kafka_config["group.id"]
            tps = [TopicPartition(topic, pid) for pid in partitions]
            request = ConsumerGroupTopicPartitions(group, tps)
            futures = self._admin.list_consumer_group_offsets([request])
            result = next(iter(futures.values())).result(timeout=self._timeout)
            offsets = []
            for tp in result.topic_partitions or []:
                off = tp.offset
                if off is None or off < 0 or off == OFFSET_INVALID:
                    continue
                offsets.append(int(off))
            if offsets:
                committed_sum = sum(offsets)
                lag = max(0, high_sum - committed_sum)
        except Exception:  # noqa: BLE001 - VAST may not implement this admin call
            committed_sum = None
            lag = None
        return {
            "end": high_sum,
            "retained": high_sum - low_sum,
            "committed": committed_sum,
            "lag": lag,
            "partitions": len(partitions),
        }

    def _collect_iceberg(self) -> dict[str, Any]:
        if self._catalog is None or self._iceberg_ident is None:
            return {"error": "Iceberg catalog not configured"}
        from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

        try:
            if self._table is None:
                self._table = self._catalog.load_table(self._iceberg_ident)
            else:
                try:
                    self._table.refresh()
                except Exception:  # noqa: BLE001 - reload if refresh is unavailable
                    self._table = self._catalog.load_table(self._iceberg_ident)
        except (NoSuchTableError, NoSuchNamespaceError):
            return {"error": f"table {self._iceberg_ident} does not exist yet"}
        table = self._table
        location = None
        try:
            location = table.location()
        except Exception:  # noqa: BLE001
            location = None
        snaps = list(table.snapshots())
        current = table.current_snapshot()
        summary = dict(current.summary or {}) if current is not None else {}
        rows = _intish(summary.get("total-records"))
        data_files = _intish(summary.get("total-data-files"))
        last_added = _intish(summary.get("added-records"))
        last_age = None
        if current is not None and getattr(current, "timestamp_ms", None):
            last_age = max(0.0, time.time() - (current.timestamp_ms / 1000.0))
        recent = []
        for snap in snaps[-LATEST_SNAPS:]:
            ts = dt.datetime.fromtimestamp(snap.timestamp_ms / 1000.0, tz=dt.timezone.utc)
            info = dict(snap.summary or {})
            op = info.get("operation") or getattr(snap, "operation", None) or "append"
            recent.append((ts, str(op), _intish(info.get("added-records"))))
        return {
            "rows": rows,
            "data_files": data_files,
            "snapshots": len(snaps),
            "last_added": last_added,
            "last_commit_age_s": last_age,
            "recent": recent,
            "location": location,
        }

    def _collect_warehouse(self) -> dict[str, Any]:
        warehouse = self._warehouse
        if not warehouse:
            return {"error": "ICEBERG_WAREHOUSE is not set"}
        # Built from the warehouse URI plus namespace/table, not from
        # table.location(): this collector runs in parallel with the Iceberg
        # one, so the Table handle may not be loaded yet on the first tick.
        bucket, data_prefix = warehouse_data_prefix(
            warehouse, self._namespace, self._table_name
        )
        _, meta_prefix = warehouse_metadata_prefix(
            warehouse, self._namespace, self._table_name
        )
        parquet, _, _, latest = _list_prefix(
            self._s3, bucket, data_prefix, latest_n=1, suffix=".parquet"
        )
        metadata, _, _, _ = _list_prefix(self._s3, bucket, meta_prefix)
        return {
            "parquet": parquet,
            "metadata": metadata,
            "latest_parquet": latest,
            "location": warehouse,
        }


def _intish(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _s3_settings(config: Any) -> dict[str, str]:
    props = config.iceberg.catalog_properties if config.iceberg is not None else {}
    return {
        "endpoint": os.environ.get("VAST_S3_ENDPOINT") or props.get("s3.endpoint") or "",
        "access_key": os.environ.get("VAST_S3_ACCESS_KEY") or props.get("s3.access-key-id") or "",
        "secret_key": os.environ.get("VAST_S3_SECRET_KEY") or props.get("s3.secret-access-key") or "",
        "region": os.environ.get("VAST_S3_REGION") or props.get("s3.region") or "us-east-1",
    }


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


PLAIN_HEADER = (
    "time       s3_obj   kafka    lag   rows  parquet  snaps    s3/s    k/s  row/s"
)


def _plain_cell(value: int | None, width: int) -> str:
    if value is None:
        return "-".rjust(width)
    return str(value).rjust(width)


def _plain_rate(value: float | None, width: int) -> str:
    if value is None:
        return "-".rjust(width)
    return f"{value:.1f}".rjust(width)


def render_plain(current: Sample, previous: Sample | None) -> str:
    dt_s = (current.t - previous.t) if previous is not None else 0.0
    s3_r = rate_between(
        previous.source_objects if previous else None, current.source_objects, dt_s
    )
    k_r = rate_between(
        kafka_events(previous) if previous else None, kafka_events(current), dt_s
    )
    row_r = rate_between(
        previous.iceberg_rows if previous else None, current.iceberg_rows, dt_s
    )
    wall = current.wall.strftime("%H:%M:%S")
    lag = current.kafka_lag
    if lag is None and kafka_events(current) is not None and current.iceberg_rows is not None:
        lag = max(0, kafka_events(current) - current.iceberg_rows)
    return (
        f"{wall}  {_plain_cell(current.source_objects, 6)}  "
        f"{_plain_cell(kafka_events(current), 6)}  {_plain_cell(lag, 5)}  "
        f"{_plain_cell(current.iceberg_rows, 5)}  {_plain_cell(current.parquet_s3, 7)}  "
        f"{_plain_cell(current.iceberg_snaps, 5)}  {_plain_rate(s3_r, 6)}  "
        f"{_plain_rate(k_r, 6)}  {_plain_rate(row_r, 5)}"
    )


@dataclass
class ViewMeta:
    title: str
    source: str
    topic: str
    table: str
    interval: float
    elapsed: float
    paused: bool
    synthetic: bool
    expect: int | None
    batch_size: int | None


def _event_scale(sample: Sample, expect: int | None) -> int:
    if expect is not None and expect > 0:
        return expect
    peak = 0.0
    for value in (sample.source_objects, kafka_events(sample), sample.iceberg_rows):
        if value is not None:
            peak = max(peak, float(value))
    return nice_scale(peak)


def _colored_bar(style: Style, filled: str, files: bool = False) -> str:
    n_fill = 0
    for ch in filled:
        if ch in (style.fill, BAR_FILL, BAR_FILL_ASCII, "#", "█"):
            n_fill += 1
        else:
            break
    body_fill = filled[:n_fill]
    body_empty = filled[n_fill:]
    paint = style.bar_files if files else style.bar
    return paint(body_fill) + style.muted_bar(body_empty)


def _meter_row(
    style: Style,
    label: str,
    value: int | None,
    bar: str,
    rate: float | None,
    spark: str,
    rising: bool,
    files: bool,
    cols: int,
) -> str:
    name = pad(style.label(label), 16)
    number = format_int(value, 7)
    if value is None:
        number = style.err(number)
    elif rising:
        number = style.up(number)
    else:
        number = style.value(number)
    rate_s = format_rate(rate, 8)
    if rate is not None and rate > 0.05:
        rate_s = style.up(rate_s)
    else:
        rate_s = style.dim(rate_s)
    spark_s = style.dim(spark) if not rising else style.ok(spark)
    bar_s = _colored_bar(style, bar, files=files)
    row = f"  {name} {number} {bar_s} {rate_s} {spark_s}"
    vis = visible_len(row)
    if vis < cols:
        return row + (" " * (cols - vis))
    return row


def _hop(a: int | None, b: int | None) -> int | None:
    if a is None or b is None:
        return None
    return max(0, a - b)


def render_tui(
    history: list[Sample],
    style: Style,
    cols: int,
    rows: int,
    meta: ViewMeta,
) -> str:
    cols = max(72, cols)
    current = history[-1]
    previous = history[-2] if len(history) >= 2 else None
    dt_s = (current.t - previous.t) if previous is not None else 0.0

    def r(getter: Callable[[Sample], int | None]) -> float | None:
        if previous is None:
            return None
        return rate_between(getter(previous), getter(current), dt_s)

    s3_rate = r(lambda s: s.source_objects)
    k_rate = r(kafka_events)
    row_rate = r(lambda s: s.iceberg_rows)
    file_rate = r(lambda s: s.parquet_s3)

    s3_up = bool(previous and current.source_objects is not None
                 and previous.source_objects is not None
                 and current.source_objects > previous.source_objects)
    k_up = bool(previous and kafka_events(current) is not None
                and kafka_events(previous) is not None
                and kafka_events(current) > kafka_events(previous))
    row_up = bool(previous and current.iceberg_rows is not None
                  and previous.iceberg_rows is not None
                  and current.iceberg_rows > previous.iceberg_rows)
    file_up = bool(previous and current.parquet_s3 is not None
                   and previous.parquet_s3 is not None
                   and current.parquet_s3 > previous.parquet_s3)

    spark_w = 14 if cols >= 96 else 10
    # label 2+16, value 1+7, bar gaps, rate 1+8, spark 1+spark_w
    bar_w = max(12, cols - (2 + 16 + 1 + 7 + 1 + 8 + 1 + spark_w + 3))
    scale = _event_scale(current, meta.expect)

    def spark_for(getter: Callable[[Sample], int | None]) -> str:
        return sparkline(series_rates(history, getter), spark_w, style.ticks)

    s3_bar = render_bar(current.source_objects, scale, bar_w, style.fill, style.empty)
    k_bar = render_bar(kafka_events(current), scale, bar_w, style.fill, style.empty)
    row_bar = render_bar(current.iceberg_rows, scale, bar_w, style.fill, style.empty)
    file_bar = file_blocks(current.parquet_s3, bar_w, style.fill, style.empty)

    lines: list[str] = []

    status = "SYNTHETIC" if meta.synthetic else "live"
    if meta.paused:
        status = "PAUSED"
    poll = f"poll {current.poll_ms:.0f}ms"
    poll_s = style.lag(poll) if current.poll_ms > meta.interval * 800 else style.dim(poll)
    right = (
        f"{status}  {format_elapsed(meta.elapsed)}  {poll_s}  "
        f"every {meta.interval:.1f}s"
    )
    left = style.head(meta.title)
    gap = max(1, cols - visible_len(left) - visible_len(right))
    lines.append(left + (" " * gap) + right)

    where = (
        f"{meta.source}  topic {meta.topic}  iceberg.{meta.table}"
    )
    lines.append(style.dim(truncate(where, cols, style.ascii_mode)))
    lines.append(style.rule(cols))

    a = style.dim(f" {style.arrow} ")
    flow = a.join(
        [
            style.bold(f"S3 {format_int(current.source_objects, 0).strip()}"),
            style.bold(f"Kafka {format_int(kafka_events(current), 0).strip()}"),
            style.bold(f"Iceberg {format_int(current.iceberg_rows, 0).strip()}"),
            style.bold(f"Parquet {format_int(current.parquet_s3, 0).strip()}"),
        ]
    )
    lines.append("  " + flow)
    lines.append(style.dim(f"  event bars share scale {scale:,} so trailing stages stay visible"))
    lines.append("")

    lines.append(
        _meter_row(
            style, "SOURCE OBJECTS", current.source_objects, s3_bar,
            s3_rate, spark_for(lambda s: s.source_objects), s3_up, False, cols,
        )
    )
    lines.append(
        _meter_row(
            style, "KAFKA EVENTS", kafka_events(current), k_bar,
            k_rate, spark_for(kafka_events), k_up, False, cols,
        )
    )
    lines.append(
        _meter_row(
            style, "ICEBERG ROWS", current.iceberg_rows, row_bar,
            row_rate, spark_for(lambda s: s.iceberg_rows), row_up, False, cols,
        )
    )
    lines.append(
        _meter_row(
            style, "PARQUET FILES", current.parquet_s3, file_bar,
            file_rate, spark_for(lambda s: s.parquet_s3), file_up, True, cols,
        )
    )
    lines.append("")

    kafka_behind_s3 = _hop(current.source_objects, kafka_events(current))
    table_behind_kafka = _hop(kafka_events(current), current.iceberg_rows)
    rpf = rows_per_file(current.iceberg_rows, current.parquet_s3)
    bits = []
    if kafka_behind_s3 is not None:
        bits.append(f"kafka {kafka_behind_s3:,} behind s3")
    if current.kafka_lag is not None:
        bits.append(f"consumer lag {current.kafka_lag:,}")
    if table_behind_kafka is not None:
        bits.append(f"iceberg {table_behind_kafka:,} behind kafka")
        batch = meta.batch_size
        if batch and 0 < table_behind_kafka < batch:
            bits.append(f"buffering next snapshot ({table_behind_kafka}/{batch})")
    if rpf is not None:
        bits.append(f"{rpf:.1f} rows/file")
    if current.last_added is not None:
        age = format_age(current.last_commit_age_s)
        bits.append(f"last commit +{current.last_added:,}  {age}")
    if current.iceberg_snaps is not None:
        bits.append(f"{current.iceberg_snaps:,} snapshots")
    if current.metadata_s3:
        bits.append(f"{current.metadata_s3:,} metadata objects")
    sep = "  |  " if style.ascii_mode else "  ·  "
    detail = sep.join(bits) if bits else "waiting for the first sample"
    if table_behind_kafka is not None and table_behind_kafka > 0:
        detail = style.lag(detail)
    lines.append("  " + style.dim("trailing  ") + detail)

    if current.errors:
        for layer, msg in current.errors.items():
            lines.append("  " + style.err(f"! {layer}: {truncate(msg, cols - 12, style.ascii_mode)}"))

    lines.append("")
    compact_terminal = 0 < rows < 22
    if not compact_terminal:
        left_h = style.label("commits (one snapshot per Iceberg flush)")
        right_h = style.label("latest source objects")
        if cols >= 100:
            left_w = min(46, cols // 2 - 2)
            lines.append("  " + pad(left_h, left_w) + "  " + right_h)
            snap_rows = _format_snaps(current.recent_snaps, left_w - 2, style)
            key_rows = _format_keys(current.source_latest, cols - left_w - 6, style)
            for i in range(max(len(snap_rows), len(key_rows), 1)):
                left_cell = pad(snap_rows[i] if i < len(snap_rows) else "", left_w)
                right_cell = key_rows[i] if i < len(key_rows) else ""
                lines.append("  " + left_cell + "  " + right_cell)
        else:
            lines.append("  " + left_h)
            for row in _format_snaps(current.recent_snaps, cols - 4, style):
                lines.append("  " + row)
            lines.append("  " + right_h)
            for row in _format_keys(current.source_latest, cols - 4, style):
                lines.append("  " + row)

    if current.latest_parquet:
        lines.append("")
        lines.append(
            "  "
            + style.dim("last parquet  ")
            + truncate(current.latest_parquet, cols - 18, style.ascii_mode)
        )

    hint = "q quit  r reset session  p pause  + / - interval"
    lesson = "Parquet grows per Iceberg snapshot, not per PUT"
    footer = hint + "   " + lesson
    footer_line = style.dim(truncate(footer, cols, style.ascii_mode))
    if rows > 8:
        while len(lines) < rows - 2:
            lines.append("")
        lines.append(footer_line)
        return "\n".join(lines[:rows]) + "\n"
    lines.append("")
    lines.append(footer_line)
    return "\n".join(lines) + "\n"


def _format_snaps(
    snaps: list[tuple[dt.datetime, str, int | None]], width: int, style: Style
) -> list[str]:
    if not snaps:
        return [style.dim("(none yet)")]
    rows = []
    for ts, op, added in snaps[-LATEST_SNAPS:]:
        when = ts.strftime("%H:%M:%S")
        extra = f"+{added:,}" if added is not None else ""
        rows.append(truncate(f"{when}  {op:<8} {extra:>6}", width, style.ascii_mode))
    return rows


def _format_keys(
    keys: list[tuple[dt.datetime, str, int]], width: int, style: Style
) -> list[str]:
    if not keys:
        return [style.dim("(none yet)")]
    items = list(keys)[-LATEST_KEYS:]
    items.reverse()
    rows = []
    for ts, key, size in items:
        when = ts.strftime("%H:%M:%S")
        name = key.rsplit("/", 1)[-1]
        rows.append(
            truncate(f"{when}  {name}  {format_bytes(size)}", width, style.ascii_mode)
        )
    return rows


# --------------------------------------------------------------------------- #
# Terminal loop
# --------------------------------------------------------------------------- #


def _stdin_key(timeout: float) -> str | None:
    if not sys.stdin.isatty():
        time.sleep(timeout)
        return None
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if not ready:
        return None
    ch = sys.stdin.read(1)
    if ch == "\x1b":
        extra, _, _ = select.select([sys.stdin], [], [], 0.01)
        while extra:
            sys.stdin.read(1)
            extra, _, _ = select.select([sys.stdin], [], [], 0.0)
        return None
    return ch


def _restore_terminal(old_term: Any | None, used_alt: bool) -> None:
    if used_alt:
        sys.stdout.write(LEAVE_ALT)
        sys.stdout.flush()
    if old_term is not None:
        import termios

        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_term)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Live dashboard of the VAST S3 to Kafka to Iceberg cascade. "
            "Read-only: never consumes the demo consumer group."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  set -a; . ./docker/demo.env; set +a\n"
            "  python3 scripts/demo_watch.py\n"
            "  python3 scripts/demo_watch.py --plain --interval 2\n"
            "  python3 scripts/demo_watch.py --synthetic --once\n"
        ),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        metavar="PATH",
        help=f"consumer config (default: {DEFAULT_CONFIG} in cwd or the repo root)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        metavar="SECONDS",
        help=f"refresh interval (default: {DEFAULT_INTERVAL:g})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="poll once, print one frame, exit (no alternate screen)",
    )
    parser.add_argument(
        "--plain",
        action="store_true",
        help="vmstat-style lines instead of the full-screen view",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="disable colour (also honours NO_COLOR)",
    )
    parser.add_argument(
        "--ascii",
        action="store_true",
        help="ASCII bars and arrows (no Unicode blocks)",
    )
    parser.add_argument(
        "--prefix",
        default="",
        metavar="KEY",
        help="only count source-bucket keys with this prefix (e.g. demo/)",
    )
    parser.add_argument(
        "--expect",
        type=int,
        default=None,
        metavar="N",
        help="lock event-bar scale to N (default: auto nice scale)",
    )
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="animate a local model of the pipeline; no cluster required",
    )
    parser.add_argument(
        "--synthetic-rate",
        type=float,
        default=40.0,
        metavar="N",
        help="objects per interval in --synthetic (default: 40)",
    )
    parser.add_argument(
        "--synthetic-ticks",
        type=int,
        default=None,
        metavar="N",
        help="with --synthetic --once, how many simulated intervals to run (default: 24)",
    )
    parser.add_argument(
        "--observer-group",
        default=DEFAULT_OBSERVER_GROUP,
        metavar="NAME",
        help=f"Kafka group for watermark reads (default: {DEFAULT_OBSERVER_GROUP})",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=None,
        help="override terminal width",
    )
    return parser.parse_args(argv)


def _find_config(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit
    cwd = Path.cwd() / DEFAULT_CONFIG
    if cwd.is_file():
        return cwd
    return REPO_ROOT / DEFAULT_CONFIG


def _batch_size_from_config(config: Any | None) -> int | None:
    if config is None or config.iceberg is None:
        return 25
    return config.iceberg.batch_size


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.interval <= 0:
        print("error: --interval must be greater than zero", file=sys.stderr)
        return 2
    if args.observer_group == os.environ.get("VAST_KAFKA_GROUP"):
        print(
            "error: --observer-group must not be the demo consumer group "
            f"({args.observer_group}); it would rebalance the running consumer.",
            file=sys.stderr,
        )
        return 2

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    style = detect_style(args.no_color, args.ascii)
    want_plain = args.plain or not sys.stdout.isatty()
    if args.once:
        want_plain = args.plain and not args.synthetic

    config = None
    cluster: LiveCluster | None = None
    simulator: IngestSimulator | None = None
    source_name = "s3://?"
    topic = os.environ.get("VAST_KAFKA_TOPIC", "s3-events")
    table = (
        f"{os.environ.get('ICEBERG_NAMESPACE', 's3_events')}."
        f"{os.environ.get('ICEBERG_TABLE', 'object_events')}"
    )

    if args.synthetic:
        batch = 25
        flush = 5.0
        simulator = IngestSimulator(batch_size=batch, flush_interval=flush, seed=1)
        source_name = "s3://demo-data (synthetic)"
        topic = "s3-events"
        table = "s3_events.object_events"
        meta_batch = batch
    else:
        from s3events.config import ConfigError, load_app_config

        config_path = _find_config(args.config)
        try:
            config = load_app_config(config_path)
        except ConfigError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        source_bucket = os.environ.get("VAST_SOURCE_BUCKET", "")
        if not source_bucket:
            print(
                "error: VAST_SOURCE_BUCKET is not set. "
                "run: set -a; . ./docker/demo.env; set +a",
                file=sys.stderr,
            )
            return 1
        if args.observer_group == config.kafka_config.get("group.id"):
            print(
                "error: observer group matches kafka_config group.id "
                f"({args.observer_group}); pick a different --observer-group.",
                file=sys.stderr,
            )
            return 1
        prefix = args.prefix
        source_name = f"s3://{source_bucket}"
        if prefix:
            source_name += "/" + prefix.lstrip("/")
        topic = config.topic
        if config.iceberg is not None:
            table = config.iceberg.table_identifier
        cluster = LiveCluster(
            config=config,
            source_bucket=source_bucket,
            source_prefix=prefix,
            observer_group=args.observer_group,
            timeout=max(2.0, args.interval - 0.4),
        )
        try:
            cluster.open()
        except Exception as exc:  # noqa: BLE001
            print(f"error: could not open collectors: {exc}", file=sys.stderr)
            return 1
        meta_batch = _batch_size_from_config(config)

    history: list[Sample] = []
    started = time.monotonic()
    interval = args.interval
    paused = False
    used_alt = False
    old_term = None

    def meta_now() -> ViewMeta:
        return ViewMeta(
            title="vast-iceberg-demo",
            source=source_name,
            topic=topic,
            table=table,
            interval=interval,
            elapsed=time.monotonic() - started,
            paused=paused,
            synthetic=bool(args.synthetic),
            expect=args.expect,
            batch_size=meta_batch,
        )

    def take_sample() -> Sample:
        if simulator is not None:
            t0 = time.monotonic()
            puts = max(0, int(round(args.synthetic_rate)))
            if args.synthetic_rate > 0 and puts == 0:
                puts = 1 if random.Random(int(t0)).random() < args.synthetic_rate else 0
            jitter = simulator.rng.randint(-2, 2) if puts else 0
            simulator.step(interval, max(0, puts + jitter))
            sample = simulator.as_sample(
                dt.datetime.now(dt.timezone.utc),
                poll_ms=(time.monotonic() - t0) * 1000.0,
            )
            return sample
        assert cluster is not None
        return cluster.sample()

    def frame(sample: Sample | None = None) -> str:
        current = sample or history[-1]
        cols, term_rows = shutil.get_terminal_size(fallback=(100, 32))
        if args.width:
            cols = args.width
        if want_plain and not args.synthetic:
            prev = history[-2] if len(history) >= 2 else None
            return render_plain(current, prev) + "\n"
        if args.once and args.plain:
            prev = history[-2] if len(history) >= 2 else None
            return render_plain(current, prev) + "\n"
        once_rows = 0 if args.once else term_rows
        return render_tui(history, style, cols, once_rows, meta_now())

    try:
        if args.once:
            ticks = 1
            if simulator is not None:
                ticks = args.synthetic_ticks if args.synthetic_ticks is not None else 24
            for _ in range(ticks):
                history.append(take_sample())
                history[:] = history[-HISTORY:]
            sys.stdout.write(frame())
            sys.stdout.flush()
            return 0

        if sys.stdin.isatty() and not want_plain:
            try:
                import termios
                import tty

                old_term = termios.tcgetattr(sys.stdin.fileno())
                tty.setcbreak(sys.stdin.fileno())
            except Exception:  # noqa: BLE001
                old_term = None
            sys.stdout.write(ENTER_ALT)
            sys.stdout.flush()
            used_alt = True

        printed_header = False
        while True:
            if not paused or not history:
                history.append(take_sample())
                history[:] = history[-HISTORY:]
            cols, term_rows = shutil.get_terminal_size(fallback=(100, 32))
            if args.width:
                cols = args.width
            if want_plain:
                if not printed_header:
                    sys.stdout.write(PLAIN_HEADER + "\n")
                    printed_header = True
                sys.stdout.write(render_plain(history[-1], history[-2] if len(history) > 1 else None) + "\n")
                sys.stdout.flush()
            else:
                body = render_tui(history, style, cols, term_rows, meta_now())
                sys.stdout.write(SYNC_BEGIN + CLEAR + body + SYNC_END)
                sys.stdout.flush()
            deadline = time.monotonic() + interval
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                key = _stdin_key(min(remaining, 0.25) if sys.stdin.isatty() else remaining)
                if key is None:
                    continue
                lowered = key.lower()
                if lowered in ("q", "\x03"):
                    return 0
                if lowered == "r":
                    keep = history[-1]
                    history.clear()
                    history.append(keep)
                    started = time.monotonic()
                    break
                if lowered == "p":
                    paused = not paused
                    break
                if key == "+":
                    interval = min(30.0, interval + 1.0)
                    break
                if key == "-":
                    interval = max(1.0, interval - 1.0)
                    break
    except KeyboardInterrupt:
        return 0
    finally:
        _restore_terminal(old_term, used_alt)
        if cluster is not None:
            cluster.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
