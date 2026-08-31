"""The sink interface and the dispatcher that fans events out to sinks.

A *sink* is somewhere a decoded event goes. The console sink prints it; the
Iceberg sink buffers it and writes it to a table. Adding another destination
means adding a sink, not editing the display path.

Two failure levels are distinguished, because they need opposite treatment:

:class:`SinkError`
    Something went wrong, but the consumer can carry on. Logged, and the other
    sinks are unaffected.
:class:`SinkFatalError`
    The sink cannot continue and the process must stop. Propagated out of the
    dispatcher, so the poll loop unwinds and the consumer exits non-zero without
    committing Kafka offsets. Raised, for example, when the Iceberg sink has
    exhausted its bounded retries: the records are still buffered and their
    offsets uncommitted, so stopping is what lets them be replayed rather than
    lost.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

from s3events.flatten import EventRow

LOG = logging.getLogger("s3_event_consumer")


class SinkError(Exception):
    """Raised when a sink cannot be started, or hits a recoverable problem."""


class SinkFatalError(SinkError):
    """Raised when a sink cannot continue and the consumer must stop.

    Deliberately propagates through the dispatcher rather than being logged and
    swallowed. The consumer treats it as a non-zero exit.
    """


@runtime_checkable
class EventSink(Protocol):
    """Somewhere decoded events are sent."""

    #: Name used in log messages.
    name: str

    def open(self) -> None:
        """Acquire whatever the sink needs, before the first event arrives.

        Raises:
            SinkError: when the sink cannot be started at all.
        """

    def handle(self, event: Any, rows: list[EventRow], raw_payload: str) -> None:
        """Accept one decoded Kafka message.

        Args:
            event: the decoded JSON payload, for sinks that want it whole.
            rows: the same payload flattened into table rows.
            raw_payload: the payload exactly as it arrived.
        """

    def tick(self) -> None:
        """Called on every poll-loop iteration, including idle ones.

        Lets a buffering sink flush on elapsed time rather than only on volume.
        """

    def close(self) -> None:
        """Flush anything pending and release resources.

        Raises:
            SinkError: when the sink could not shut down cleanly — for the
                Iceberg sink, when the final flush failed and records remain
                unwritten. The caller must treat this as a non-zero exit.
        """


class EventDispatcher:
    """Sends each decoded event to every sink.

    One misbehaving sink must not take down the others or the poll loop, so
    ``handle`` and ``tick`` log and continue — *except* for
    :class:`SinkFatalError`, which is the sink saying the process should stop.

    ``open`` never swallows: a sink that cannot start is a startup failure the
    caller needs to see.
    """

    def __init__(self, sinks: list[EventSink]) -> None:
        self._sinks = list(sinks)

    def __len__(self) -> int:
        return len(self._sinks)

    @property
    def sinks(self) -> list[EventSink]:
        return list(self._sinks)

    def open(self) -> None:
        """Open every sink, closing any already opened if one fails."""
        opened: list[EventSink] = []
        try:
            for sink in self._sinks:
                sink.open()
                opened.append(sink)
        except Exception:
            for sink in reversed(opened):
                self._safely(sink, "close", sink.close)
            raise

    def dispatch(self, event: Any, rows: list[EventRow], raw_payload: str) -> None:
        """Send one event to every sink.

        Raises:
            SinkFatalError: when a sink reports it cannot continue.
        """
        for sink in self._sinks:
            self._safely(sink, "handle", sink.handle, event, rows, raw_payload)

    def tick(self) -> None:
        """Nudge every sink.

        Raises:
            SinkFatalError: when a sink reports it cannot continue.
        """
        for sink in self._sinks:
            self._safely(sink, "tick", sink.tick)

    def close(self) -> bool:
        """Close every sink, in reverse order.

        Never raises: every sink gets its chance to shut down even if an earlier
        one failed.

        Returns:
            True when every sink shut down cleanly. False means at least one
            sink still holds unwritten records, and the caller must exit
            non-zero so nothing is silently acknowledged.
        """
        clean = True
        for sink in reversed(self._sinks):
            if not self._safely(sink, "close", sink.close, propagate_fatal=False):
                clean = False
        return clean

    @staticmethod
    def _safely(
        sink: EventSink,
        action: str,
        call: Any,
        *args: Any,
        propagate_fatal: bool = True,
    ) -> bool:
        """Run one sink call. Returns False when it failed."""
        try:
            call(*args)
        except SinkFatalError:
            if propagate_fatal:
                raise
            # During close, a fatal sink has already logged the detail; record
            # the unclean shutdown and let the remaining sinks close.
            return False
        except Exception as exc:  # noqa: BLE001 - one sink must not stop the rest
            LOG.error(
                "Sink '%s' failed during %s: %s: %s",
                getattr(sink, "name", type(sink).__name__),
                action,
                type(exc).__name__,
                exc,
            )
            return False
        return True
