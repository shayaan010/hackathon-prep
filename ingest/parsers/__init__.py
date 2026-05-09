"""
Source-specific HTML parsers.

Each parser is a deterministic (no-LLM) function that takes raw bytes/text
and returns a structured Pydantic document with provenance fields. Parsers
live next to the fetchers they consume from, but don't depend on them at
runtime.

Currently:
    - ca_leginfo: CA leginfo.legislature.ca.gov per-section HTML pages.
"""

from .types import (
    HistoryNote,
    StatuteSection,
    Subsection,
    ParseError,
)

__all__ = [
    "HistoryNote",
    "StatuteSection",
    "Subsection",
    "ParseError",
]
