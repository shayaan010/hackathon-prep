"""
Batch driver: parse every HTML in data/raw/ca_leginfo_pages/{CODE}/ to JSON.

Default behavior:
    Input :  data/raw/ca_leginfo_pages/{CODE}/{section}.html
    Output:  data/parsed/ca_leginfo_pages/{CODE}/{section}.json
    Manifest: data/parsed/ca_leginfo_pages/parse_manifest.jsonl

Idempotent: a section is skipped if the existing parsed JSON's
content_sha256 matches the SHA of the current input HTML. Use --force
to re-parse everything.

Usage examples:
    # Parse every cached VEH page
    uv run python -m ingest.parsers.run_ca_leginfo

    # Smoke-test on the first 10
    uv run python -m ingest.parsers.run_ca_leginfo --limit 10 --force

    # Different law code
    uv run python -m ingest.parsers.run_ca_leginfo --code PEN

    # Custom paths
    uv run python -m ingest.parsers.run_ca_leginfo \\
        --in data/raw/ca_leginfo_pages \\
        --out data/parsed/ca_leginfo_pages
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .ca_leginfo import parse_section_file
from .types import ParseError


DEFAULT_IN_DIR = Path("data/raw/ca_leginfo_pages")
DEFAULT_OUT_DIR = Path("data/parsed/ca_leginfo_pages")
DEFAULT_CODE = "VEH"

logger = logging.getLogger("ca_leginfo_parser")


# ---------------------------------------------------------------------------
# Manifest record
# ---------------------------------------------------------------------------


@dataclass
class ParseRecord:
    section: str
    law_code: str
    ok: bool
    raw_path: str
    parsed_path: Optional[str]
    parsed_at: str
    content_sha256: str
    skipped_cached: bool
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _section_sort_key(name: str) -> tuple:
    """Sort section filenames like '21451' < '21451.5' < '21452'."""
    stem = Path(name).stem
    m = re.match(r"(\d+)(?:\.(\d+))?", stem)
    if not m:
        return (10**9, 0, stem)
    return (int(m.group(1)), int(m.group(2) or 0), stem)


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _load_existing_sha(parsed_path: Path) -> Optional[str]:
    """Read the previously-parsed content_sha256 if it exists, for caching."""
    if not parsed_path.exists():
        return None
    try:
        with parsed_path.open() as f:
            data = json.load(f)
        return data.get("content_sha256")
    except (OSError, json.JSONDecodeError):
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)


# ---------------------------------------------------------------------------
# Main batch routine
# ---------------------------------------------------------------------------


def parse_one(
    raw_path: Path,
    out_dir: Path,
    *,
    force: bool = False,
) -> ParseRecord:
    """Parse a single HTML file and write its JSON. Returns a manifest record."""
    section = raw_path.stem
    law_code = raw_path.parent.name
    parsed_path = out_dir / law_code / f"{section}.json"

    raw_bytes = raw_path.read_bytes()
    sha = _sha256_bytes(raw_bytes)

    # Cache hit: same input SHA as the last parse.
    if not force:
        existing_sha = _load_existing_sha(parsed_path)
        if existing_sha == sha:
            logger.debug("skip-cached section=%s", section)
            return ParseRecord(
                section=section,
                law_code=law_code,
                ok=True,
                raw_path=str(raw_path),
                parsed_path=str(parsed_path),
                parsed_at=_now_iso(),
                content_sha256=sha,
                skipped_cached=True,
            )

    try:
        rec = parse_section_file(raw_path)
    except ParseError as e:
        logger.warning("parse-error section=%s err=%s", section, e)
        return ParseRecord(
            section=section,
            law_code=law_code,
            ok=False,
            raw_path=str(raw_path),
            parsed_path=None,
            parsed_at=_now_iso(),
            content_sha256=sha,
            skipped_cached=False,
            error=f"ParseError: {e}",
        )
    except Exception as e:  # noqa: BLE001 — we want to record any failure
        logger.error(
            "unexpected-error section=%s type=%s err=%s",
            section, type(e).__name__, e,
        )
        return ParseRecord(
            section=section,
            law_code=law_code,
            ok=False,
            raw_path=str(raw_path),
            parsed_path=None,
            parsed_at=_now_iso(),
            content_sha256=sha,
            skipped_cached=False,
            error=f"{type(e).__name__}: {e}",
        )

    parsed_path.parent.mkdir(parents=True, exist_ok=True)
    with parsed_path.open("w") as f:
        json.dump(rec.model_dump(), f, indent=2, ensure_ascii=False)
        f.write("\n")

    logger.debug(
        "ok       section=%s subsections=%d history=%s",
        section, len(rec.subsections),
        rec.history.action if rec.history else None,
    )
    return ParseRecord(
        section=section,
        law_code=law_code,
        ok=True,
        raw_path=str(raw_path),
        parsed_path=str(parsed_path),
        parsed_at=_now_iso(),
        content_sha256=sha,
        skipped_cached=False,
    )


def run(
    *,
    in_dir: Path,
    out_dir: Path,
    code: str,
    force: bool = False,
    limit: Optional[int] = None,
    verbose: bool = False,
) -> list[ParseRecord]:
    _setup_logging(verbose)

    code_in_dir = in_dir / code
    if not code_in_dir.is_dir():
        raise SystemExit(f"input dir not found: {code_in_dir}")

    files = sorted(code_in_dir.glob("*.html"), key=lambda p: _section_sort_key(p.name))
    if limit is not None:
        files = files[:limit]
    if not files:
        logger.warning("no html files found under %s", code_in_dir)
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "parse_manifest.jsonl"

    logger.info(
        "start    code=%s files=%d in=%s out=%s force=%s",
        code, len(files), code_in_dir, out_dir, force,
    )

    records: list[ParseRecord] = []
    t0 = time.time()
    cached = 0
    fresh = 0
    failed = 0

    for path in files:
        rec = parse_one(path, out_dir, force=force)
        records.append(rec)
        if not rec.ok:
            failed += 1
        elif rec.skipped_cached:
            cached += 1
        else:
            fresh += 1

    # Append every run's records to the manifest (history of parses).
    with manifest_path.open("a") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec)) + "\n")

    elapsed = time.time() - t0
    logger.info(
        "done     ok=%d (fresh=%d cached=%d) failed=%d elapsed=%.2fs (%.1f/s)",
        len(records) - failed, fresh, cached, failed, elapsed,
        len(records) / elapsed if elapsed > 0 else 0,
    )
    if failed:
        for rec in records:
            if not rec.ok:
                logger.warning("failed   section=%s err=%s", rec.section, rec.error)
    logger.info("manifest %s", manifest_path)
    return records


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str]) -> dict:
    p = argparse.ArgumentParser(
        description="Parse leginfo per-section HTML pages into structured JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--code", default=DEFAULT_CODE,
        help="Law code subdirectory under --in (default: %(default)s).",
    )
    p.add_argument(
        "--in", dest="in_dir", type=Path, default=DEFAULT_IN_DIR,
        help="Input root containing {CODE}/*.html (default: %(default)s).",
    )
    p.add_argument(
        "--out", dest="out_dir", type=Path, default=DEFAULT_OUT_DIR,
        help="Output root where {CODE}/*.json lands (default: %(default)s).",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-parse even if cached output exists with matching content sha.",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help="Cap number of sections (smoke testing).",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Emit DEBUG-level per-section log lines.",
    )
    return vars(p.parse_args(argv))


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    run(
        in_dir=args["in_dir"],
        out_dir=args["out_dir"],
        code=args["code"],
        force=args["force"],
        limit=args["limit"],
        verbose=args["verbose"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
