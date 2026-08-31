"""Destinations for decoded Kafka events.

``s3events.sinks.iceberg`` is deliberately *not* imported here. Importing it is
cheap — it only pulls in PyIceberg when the sink is opened — but keeping it out
of this module's namespace makes the dependency boundary explicit: nothing in
the default path reaches for it.
"""

from s3events.sinks.base import EventDispatcher, EventSink, SinkError, SinkFatalError
from s3events.sinks.console import ConsoleSink, render_event, use_color

__all__ = [
    "ConsoleSink",
    "EventDispatcher",
    "EventSink",
    "SinkError",
    "SinkFatalError",
    "render_event",
    "use_color",
]
