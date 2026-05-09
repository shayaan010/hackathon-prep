"""
CACI (California Civil Jury Instructions) PDF fetcher.

The Judicial Council of California publishes the entire CACI corpus
as a single annual PDF on courts.ca.gov.  Each year is one file:

    https://www.courts.ca.gov/system/files/file/judicial_council_of_california_civil_jury_instructions_{YEAR}.pdf

The 2026 edition is ~11 MB, ~3,560 pages, ~1,400 instructions, with
embedded outline bookmarks (one per instruction) — see
``ingest/parsers/caci.py`` for the parse-and-index step.

This module is a thin fetcher: it downloads the editions you ask for
into ``data/raw/caci/{edition}/caci_{edition}.pdf`` and writes a
single-line manifest per fetch.  It deliberately does not use the
per-section fetcher framework in ``base.py``; that's built around
hundreds of small HTML files, not a single multi-MB PDF per edition.

Cache: a PDF is skipped if already on disk and ``%PDF-`` magic
matches.  Use ``--force`` to re-fetch.

Usage:
    uv run python -m ingest.sources.caci_pdfs                 # default: 2026 + 2025_supp
    uv run python -m ingest.sources.caci_pdfs --editions 2026
    uv run python -m ingest.sources.caci_pdfs --all
    uv run python -m ingest.sources.caci_pdfs --force
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .._http import default_client, get_with_retry, setup_logging
from ..manifest import ManifestWriter


SOURCE_SLUG = "caci_pdfs"
JURISDICTION = "CA"
PUBLISHER = "Judicial Council of California"

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"

logger = logging.getLogger(SOURCE_SLUG)


# Edition slug -> PDF URL.  The CA courts site has migrated paths a few
# times; these are the live URLs as of 2026-04 (verified by HEAD).
EDITIONS: dict[str, str] = {
    "2026": (
        "https://www.courts.ca.gov/system/files/file/"
        "judicial_council_of_california_civil_jury_instructions_2026.pdf"
    ),
    "2025": (
        "https://www.courts.ca.gov/system/files/file/"
        "judicial_council_of_california_civil_jury_instructions_2025.pdf"
    ),
    "2025_supp": (
        "https://www.courts.ca.gov/system/files/file/"
        "judicial_council_of_california_july_2025_supp.pdf"
    ),
    "2024": (
        "https://www.courts.ca.gov/system/files/2024-08/"
        "judicial_council_of_california_civil_jury_instructions_2024.pdf"
    ),
    "2024_supp": (
        "https://www.courts.ca.gov/system/files/2024-08/"
        "Judicial_Council_of_California_Civil_Jury_Instructions_May_2024_Supp.pdf"
    ),
}


def edition_pdf_path(out_root: Path, edition: str) -> Path:
    return out_root / edition / f"caci_{edition}.pdf"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_pdf(body: bytes) -> bool:
    return body[:5] == b"%PDF-"


async def fetch_one_edition(
    client,
    edition: str,
    *,
    out_root: Path,
    force: bool,
) -> dict:
    """Download one CACI edition.  Idempotent on disk."""
    url = EDITIONS[edition]
    dest = edition_pdf_path(out_root, edition)
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Cache short-circuit
    if dest.exists() and dest.stat().st_size > 0 and not force:
        body = dest.read_bytes()
        valid = _is_pdf(body)
        logger.info("cache-hit edition=%s bytes=%d valid=%s",
                    edition, len(body), valid)
        return {
            "source_slug": SOURCE_SLUG,
            "jurisdiction": JURISDICTION,
            "edition": edition,
            "url": url,
            "fetched_at": _now_iso(),
            "http_status": 200,
            "bytes": len(body),
            "content_sha256": hashlib.sha256(body).hexdigest(),
            "raw_path": str(dest),
            "valid": valid,
            "cached": True,
        }

    logger.info("fetch    edition=%s url=%s", edition, url)
    resp = await get_with_retry(client, url, label=f"caci-{edition}")
    body = resp.content
    valid = resp.status_code == 200 and _is_pdf(body)
    sha = hashlib.sha256(body).hexdigest() if body else ""

    raw_path: Optional[str] = None
    if valid:
        dest.write_bytes(body)
        raw_path = str(dest)
        logger.info("ok       edition=%s bytes=%d sha=%s",
                    edition, len(body), sha[:12])
    else:
        logger.warning("fail     edition=%s status=%d bytes=%d",
                       edition, resp.status_code, len(body))

    return {
        "source_slug": SOURCE_SLUG,
        "jurisdiction": JURISDICTION,
        "edition": edition,
        "url": url,
        "fetched_at": _now_iso(),
        "http_status": resp.status_code,
        "bytes": len(body),
        "content_sha256": sha,
        "raw_path": raw_path,
        "valid": valid,
        "cached": False,
    }


async def run(
    *,
    editions: list[str],
    out_dir: Path,
    force: bool = False,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"

    stats = {"completed": 0, "valid": 0, "errors": 0, "bytes": 0}
    async with default_client(concurrency=2, timeout=120.0) as client:
        with ManifestWriter(manifest_path) as mw:
            for ed in editions:
                if ed not in EDITIONS:
                    logger.error("unknown edition: %s (have %s)",
                                 ed, sorted(EDITIONS))
                    stats["errors"] += 1
                    continue
                rec = await fetch_one_edition(
                    client, ed, out_root=out_dir, force=force,
                )
                mw.write(rec)
                stats["completed"] += 1
                stats["bytes"] += rec["bytes"]
                if rec["valid"]:
                    stats["valid"] += 1
                else:
                    stats["errors"] += 1

    summary = {"editions": editions, **stats}
    logger.info("done     %s", summary)
    logger.info("manifest %s", manifest_path)
    return summary


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Fetch CACI annual PDF(s) from courts.ca.gov",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--editions", nargs="+", default=["2026"],
        help=f"Editions to fetch (default 2026). Known: {sorted(EDITIONS)}",
    )
    p.add_argument(
        "--all", action="store_true",
        help="Fetch all known editions.",
    )
    p.add_argument(
        "--out-dir", type=Path, default=DEFAULT_OUT_DIR,
        help=f"Output root (default {DEFAULT_OUT_DIR}).",
    )
    p.add_argument("--force", action="store_true",
                   help="Re-fetch even if a PDF is on disk.")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--log-file", type=Path, default=DEFAULT_LOG_FILE)
    p.add_argument("--no-log-file", action="store_true")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    log_path = None if args.no_log_file else args.log_file
    setup_logging(verbose=args.verbose, log_path=log_path)

    editions = sorted(EDITIONS) if args.all else args.editions
    asyncio.run(run(editions=editions, out_dir=args.out_dir, force=args.force))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
