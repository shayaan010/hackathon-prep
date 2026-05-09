"""
Generic parser-runner driver.

Each per-source parser-runner (run_ca_leginfo.py, run_public_law.py)
does the same thing:

    For each *.html in raw_dir/<CODE>/,
        if parsed_dir/<CODE>/<section>.json exists with the same content_sha256,
            skip (cache hit);
        else,
            parse, write JSON, append to parse_manifest.jsonl.

This module lifts that loop.  Each runner just supplies a ``parse_fn``
that takes a ``Path`` and returns a ``StatuteSection``.

Public API:
    class ParseRecord       -- one row of parse_manifest.jsonl
    parse_one(...)          -- parse one file, return record (and write JSON)
    run_parser(...)         -- the loop
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from ..cli import section_sort_key
from ..manifest import ManifestWriter
from .types import ParseError, StatuteSection


@dataclasses.dataclass
class ParseRecord:
    section: str
    law_code: str
    jurisdiction: str
    ok: bool
    raw_path: str
    parsed_path: Optional[str]
    parsed_at: str
    content_sha256: str
    skipped_cached: bool
    error: Optional[str] = None


# ParseFn signature: (raw_path) -> StatuteSection
# Source-specific runners can use functools.partial to bind jurisdiction/law_code.
ParseFn = Callable[[Path], StatuteSection]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _load_existing_sha(parsed_path: Path) -> Optional[str]:
    if not parsed_path.exists():
        return None
    try:
        with parsed_path.open() as f:
            data = json.load(f)
        return data.get("content_sha256")
    except (OSError, json.JSONDecodeError):
        return None


def parse_one(
    raw_path: Path,
    parsed_dir: Path,
    parse_fn: ParseFn,
    *,
    jurisdiction: str,
    law_code: str,
    force: bool = False,
    log: Optional[logging.Logger] = None,
) -> ParseRecord:
    """Parse a single HTML file; cache-skip via SHA; write JSON; return record."""
    log = log or logging.getLogger("ingest.runner")
    section = raw_path.stem
    parsed_path = parsed_dir / law_code / f"{section}.json"

    raw_bytes = raw_path.read_bytes()
    sha = _sha256_bytes(raw_bytes)

    # Cache hit — same input bytes as last parse
    if not force:
        existing_sha = _load_existing_sha(parsed_path)
        if existing_sha == sha:
            log.debug("skip-cached section=%s", section)
            return ParseRecord(
                section=section,
                law_code=law_code,
                jurisdiction=jurisdiction,
                ok=True,
                raw_path=str(raw_path),
                parsed_path=str(parsed_path),
                parsed_at=_now_iso(),
                content_sha256=sha,
                skipped_cached=True,
            )

    try:
        rec = parse_fn(raw_path)
    except ParseError as e:
        log.warning("parse-error section=%s err=%s", section, e)
        return ParseRecord(
            section=section,
            law_code=law_code,
            jurisdiction=jurisdiction,
            ok=False,
            raw_path=str(raw_path),
            parsed_path=None,
            parsed_at=_now_iso(),
            content_sha256=sha,
            skipped_cached=False,
            error=f"ParseError: {e}",
        )
    except Exception as e:  # noqa: BLE001
        log.error(
            "unexpected-error section=%s type=%s err=%s",
            section, type(e).__name__, e,
        )
        return ParseRecord(
            section=section,
            law_code=law_code,
            jurisdiction=jurisdiction,
            ok=False,
            raw_path=str(raw_path),
            parsed_path=None,
            parsed_at=_now_iso(),
            content_sha256=sha,
            skipped_cached=False,
            error=f"{type(e).__name__}: {e}",
        )

    parsed_path.parent.mkdir(parents=True, exist_ok=True)
    with parsed_path.open("w", encoding="utf-8") as f:
        json.dump(rec.model_dump(), f, indent=2, ensure_ascii=False)
        f.write("\n")

    log.debug(
        "ok       section=%s subsections=%d history=%s",
        section, len(rec.subsections),
        rec.history.action if rec.history else None,
    )
    return ParseRecord(
        section=section,
        law_code=law_code,
        jurisdiction=jurisdiction,
        ok=True,
        raw_path=str(raw_path),
        parsed_path=str(parsed_path),
        parsed_at=_now_iso(),
        content_sha256=sha,
        skipped_cached=False,
    )


def run_parser(
    parse_fn: ParseFn,
    *,
    jurisdiction: str,
    law_code: str,
    raw_dir: Path,
    parsed_dir: Path,
    manifest_path: Optional[Path] = None,
    force: bool = False,
    limit: Optional[int] = None,
    log: Optional[logging.Logger] = None,
) -> dict:
    """Generic parser-runner.

    For each ``*.html`` under ``raw_dir/<law_code>/``:
        SHA-cache-check, parse, write JSON, append manifest record.

    Returns a summary dict ``{total, ok, fresh, cached, failed}``.
    """
    log = log or logging.getLogger("ingest.runner")

    code_in_dir = raw_dir / law_code
    if not code_in_dir.is_dir():
        raise SystemExit(f"input dir not found: {code_in_dir}")

    files = sorted(
        code_in_dir.glob("*.html"),
        key=lambda p: section_sort_key(p.stem),
    )
    if limit is not None:
        files = files[:limit]
    if not files:
        log.warning("no html files found under %s", code_in_dir)
        return {"total": 0, "ok": 0, "fresh": 0, "cached": 0, "failed": 0}

    parsed_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_path or (parsed_dir / "parse_manifest.jsonl")

    log.info(
        "start    jurisdiction=%s code=%s files=%d in=%s out=%s force=%s",
        jurisdiction, law_code, len(files), code_in_dir, parsed_dir, force,
    )

    cached = fresh = failed = 0
    failures: list[ParseRecord] = []
    t0 = time.time()

    with ManifestWriter(manifest_path) as mw:
        for path in files:
            rec = parse_one(
                path, parsed_dir, parse_fn,
                jurisdiction=jurisdiction, law_code=law_code,
                force=force, log=log,
            )
            mw.write(rec)
            if not rec.ok:
                failed += 1
                failures.append(rec)
            elif rec.skipped_cached:
                cached += 1
            else:
                fresh += 1

    elapsed = time.time() - t0
    total = len(files)
    log.info(
        "done     ok=%d (fresh=%d cached=%d) failed=%d elapsed=%.2fs (%.1f/s)",
        total - failed, fresh, cached, failed, elapsed,
        total / elapsed if elapsed > 0 else 0,
    )
    if failures:
        for rec in failures[:10]:
            log.warning("failed   section=%s err=%s", rec.section, rec.error)
        if len(failures) > 10:
            log.warning("... and %d more failures", len(failures) - 10)
    log.info("manifest %s", manifest_path)

    return {
        "total": total,
        "ok": total - failed,
        "fresh": fresh,
        "cached": cached,
        "failed": failed,
    }
