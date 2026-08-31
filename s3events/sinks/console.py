"""The console sink — the original, and still default, behaviour.

Event payloads go to stdout as pretty-printed JSON, optionally syntax
highlighted. Operational messages stay on stderr through ``logging``, so the
payload stream can be redirected on its own.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import JsonLexer

from s3events.flatten import EventRow


def use_color(no_color: bool) -> bool:
    """Colourise only on an interactive terminal, unless explicitly disabled."""
    if no_color or os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


def render_event(event: Any, color: bool = False) -> str:
    """Return the event as pretty-printed JSON, syntax-highlighted when wanted."""
    pretty = json.dumps(event, indent=2)
    if not color:
        return pretty
    return highlight(pretty, JsonLexer(), TerminalFormatter()).strip()


class ConsoleSink:
    """Prints each decoded event payload to stdout."""

    name = "console"

    def __init__(self, color: bool = False) -> None:
        self.color = color

    def open(self) -> None:
        return None

    def handle(self, event: Any, rows: list[EventRow], raw_payload: str) -> None:
        print(render_event(event, self.color), end="\n\n", flush=True)

    def tick(self) -> None:
        return None

    def close(self) -> None:
        return None
