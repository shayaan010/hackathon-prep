"""
Source-specific HTML parsers.

Each parser is a deterministic (no-LLM) function that takes raw bytes/text
and returns a structured Pydantic document with provenance fields. Parsers
live next to the fetchers they consume from, but don't depend on them at
runtime — they read from the filesystem.

Public API:
    StatuteSection, Subsection, HistoryNote, ParseError      (schemas)
    parse_section_html, parse_section_file                    (CA leginfo)
    parse_public_law_html, parse_public_law_file              (NY + TX public.law)
    run_parser, parse_one, ParseRecord                        (generic runner)
"""

from .ca_leginfo import parse_section_file, parse_section_html
from .public_law import parse_public_law_file, parse_public_law_html
from .runner import ParseRecord, parse_one, run_parser
from .types import (
    HistoryNote,
    ParseError,
    StatuteSection,
    Subsection,
)

__all__ = [
    # Types
    "HistoryNote",
    "StatuteSection",
    "Subsection",
    "ParseError",
    # Parser entry points
    "parse_section_html",
    "parse_section_file",
    "parse_public_law_html",
    "parse_public_law_file",
    # Runner
    "run_parser",
    "parse_one",
    "ParseRecord",
]
