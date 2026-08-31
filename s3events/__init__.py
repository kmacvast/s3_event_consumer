"""Internal package for the VAST S3 event notification demo consumer.

``s3_event_consumer.py`` remains the command-line entry point and re-exports the
names it always exposed. This package holds the pieces that entry point wires
together:

* :mod:`s3events.config`  — configuration loading and validation
* :mod:`s3events.flatten` — turning a decoded S3 event into flat table rows
* :mod:`s3events.sinks`   — where decoded events are sent

Nothing here imports PyIceberg at module scope. The Iceberg sink imports it the
first time it is opened, so a consumer running without an ``iceberg`` section —
including the standalone executable, which does not bundle PyIceberg — never
needs it installed.
"""

__all__ = ["config", "flatten", "sinks"]
