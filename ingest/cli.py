"""
Shared CLI helpers for ingest fetchers and parser-runners.

This module exists so every fetcher and parser-runner takes the same
flags with the same defaults, and so a human or agent can predict the
CLI without reading each module.

Public API:
    add_fetcher_args(parser)          -- canonical fetcher CLI flags
    add_parser_runner_args(parser)    -- canonical parser-runner CLI flags
    section_sort_key(s)               -- canonical sort key for section IDs
    run_heartbeat(stats, total, ...)  -- periodic progress logger (asyncio task)

The fetcher / runner is responsible for reading the parsed args and
wiring them into the run.  This module only defines the surface.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Argparse helpers
# ---------------------------------------------------------------------------


def add_fetcher_args(
    parser: argparse.ArgumentParser,
    *,
    default_code: Optional[str] = None,
    default_out_dir: Optional[Path] = None,
    default_log_file: Optional[Path] = None,
    default_concurrency: int = 2,
    default_rate_interval: float = 0.4,
    default_heartbeat_every: float = 5.0,
    supports_sections_file: bool = True,
    supports_skip_known_missing: bool = True,
    supports_dedupe: bool = True,
) -> None:
    """Register the canonical fetcher CLI flags on ``parser``.

    Sources can opt out of a few flags via the keyword switches
    (e.g. ca_leginfo_toc has no notion of "sections", so it doesn't need
    --sections / --sections-file / --no-skip-missing / --dedupe).
    """
    parser.add_argument(
        "--code",
        default=default_code,
        help=("Law code to fetch"
              + (f" (default: {default_code})" if default_code else " (required)")),
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=default_out_dir,
        help=f"Output directory root (default: {default_out_dir})",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=default_concurrency,
        help=f"Parallel HTTP requests (default: {default_concurrency})",
    )
    parser.add_argument(
        "--rate-interval",
        type=float,
        default=default_rate_interval,
        help=(f"Min seconds between request starts, global across workers "
              f"(default: {default_rate_interval}, set 0 to disable)"),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even if HTML already on disk.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap number of sections processed (for smoke testing).",
    )
    parser.add_argument(
        "--sections",
        nargs="+",
        default=None,
        help="Explicit list of section numbers to fetch (skips discovery).",
    )
    if supports_sections_file:
        parser.add_argument(
            "--sections-file",
            type=Path,
            default=None,
            help="File with one section number per line.",
        )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=default_log_file,
        help=f"File to append detailed log lines to (default: {default_log_file})",
    )
    parser.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable file logging (stdout only).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log DEBUG-level messages to stdout.",
    )
    parser.add_argument(
        "--heartbeat-every",
        type=float,
        default=default_heartbeat_every,
        help=f"Seconds between progress heartbeat lines (default: {default_heartbeat_every})",
    )
    if supports_skip_known_missing:
        parser.add_argument(
            "--no-skip-missing",
            action="store_true",
            help="Re-attempt sections previously confirmed not-found.",
        )
    if supports_dedupe:
        parser.add_argument(
            "--dedupe",
            action="store_true",
            help="Compact the manifest (keep best record per section) and exit.",
        )


def add_parser_runner_args(
    parser: argparse.ArgumentParser,
    *,
    default_jurisdiction: Optional[str] = None,
    default_code: Optional[str] = None,
    default_in_dir: Optional[Path] = None,
    default_out_dir: Optional[Path] = None,
    default_log_file: Optional[Path] = None,
    jurisdiction_choices: Optional[list[str]] = None,
) -> None:
    """Register the canonical parser-runner CLI flags on ``parser``."""
    parser.add_argument(
        "-j", "--jurisdiction",
        default=default_jurisdiction,
        choices=jurisdiction_choices,
        help=(f"Jurisdiction (default: {default_jurisdiction})"
              if default_jurisdiction else "Jurisdiction (required)"),
    )
    parser.add_argument(
        "-c", "--code",
        default=default_code,
        help=(f"Law code (default: {default_code})"
              if default_code else "Law code (required)"),
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=default_in_dir,
        help=f"Input root containing {{CODE}}/*.html (default: {default_in_dir})",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=default_out_dir,
        help=f"Output root for {{CODE}}/*.json (default: {default_out_dir})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-parse even if cached output's content_sha256 matches.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap number of sections (for smoke testing).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=default_log_file,
        help=f"File to append detailed log lines to (default: {default_log_file})",
    )
    parser.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable file logging.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG-level logs to stdout.",
    )


# ---------------------------------------------------------------------------
# Section sort key (canonical impl, replaces 3 duplicates)
# ---------------------------------------------------------------------------


_SECTION_PATTERN = re.compile(
    r"^"
    r"(\d+)"                     # major
    r"(?:\.(\d+))?"              # optional .minor (CA '21451.5' or TX '545.001')
    r"(?:-([A-Za-z]+))?"         # optional -suffix (NY '1146-A')
    r"(?:\*(\d+))?"              # optional *N tail (NY star marker)
    r"$"
)


def section_sort_key(section: str) -> tuple:
    """Canonical sort key for statute section numbers across jurisdictions.

    Handles all observed forms:
      - CA integer:        '15', '21451'
      - CA decimal:        '21451.5'  -> (21451, 5, '', 0, ...)
      - NY suffix letter:  '100-A', '1146-A'
      - NY star marker:    '*1146'    -> sorted with '1146'
      - TX dotted:         '545.001', '5.001'

    Order: (major, minor, suffix, star_tail, star_prefix, original_string).
    """
    s = section.strip()
    if not s:
        return (10**9, 0, "", 0, 0, s)

    # Strip leading '*' but remember it
    star_prefix = 0
    s_clean = s
    if s_clean.startswith("*"):
        star_prefix = 1
        s_clean = s_clean[1:]

    m = _SECTION_PATTERN.match(s_clean)
    if not m:
        # Unparseable -> sort to end
        return (10**9, 0, "", 0, 0, s)

    major = int(m.group(1))
    minor = int(m.group(2) or 0)
    suffix = (m.group(3) or "").upper()
    star_tail = int(m.group(4)) if m.group(4) else 0
    return (major, minor, suffix, star_tail, star_prefix, s)


# ---------------------------------------------------------------------------
# Heartbeat task
# ---------------------------------------------------------------------------


async def run_heartbeat(
    stats: dict,
    total: int,
    *,
    every_s: float = 5.0,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Periodic progress logger; cancellable asyncio task.

    ``stats`` is a dict that the caller mutates as work progresses.
    Expected keys (all optional except ``completed``):
        completed, valid, not_found, retried, errors, bytes

    Usage:
        stats = {"completed": 0, "valid": 0, ...}
        hb = asyncio.create_task(run_heartbeat(stats, total))
        try:
            await asyncio.gather(*work)
        finally:
            hb.cancel()
            try: await hb
            except asyncio.CancelledError: pass
    """
    log = logger or logging.getLogger("ingest.heartbeat")
    last_completed = stats.get("completed", 0)
    last_t = asyncio.get_event_loop().time()
    while True:
        await asyncio.sleep(every_s)
        now = asyncio.get_event_loop().time()
        completed = stats.get("completed", 0)
        delta = completed - last_completed
        rate = delta / max(now - last_t, 0.001)
        remaining = total - completed
        eta = remaining / rate if rate > 0 else float("inf")
        eta_str = f"{eta/60:.1f}min" if eta != float("inf") else "n/a"
        bytes_mb = stats.get("bytes", 0) / (1 << 20)
        log.info(
            "progress %d/%d  valid=%d  not_found=%d  retried=%d  errors=%d  "
            "rate=%.1f/s eta=%s  %.1f MB",
            completed, total,
            stats.get("valid", 0), stats.get("not_found", 0),
            stats.get("retried", 0), stats.get("errors", 0),
            rate, eta_str, bytes_mb,
        )
        last_completed = completed
        last_t = now
