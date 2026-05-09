"""
California leginfo per-section HTML fetcher.

Each statute section has a stable URL on leginfo.legislature.ca.gov:

    https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21453.&lawCode=VEH

Notes:
- The trailing dot after sectionNum is required.
- The server returns HTTP 200 even for non-existent sections.
- Real sections embed `op_statues = 'YYYY'` (chaptered year) in the body.
  Missing sections embed `op_statues = ''`. We use this to tell them apart.

Discovery:
- Default seed list: union of (a) base section numbers from the released eval CSV
  and (b) curated PI-relevant integer ranges (PI_RELEVANT_RANGES).
- ``--sections`` or ``--sections-file`` to override.
- ``--range LO HI`` for brute-force integer ranges.

Caching: a section is skipped if its HTML is already on disk.  Use ``--force``
to re-fetch.

Idempotency: run again as often as you like — cache hit = no network.
``--no-skip-missing`` re-attempts sections previously confirmed not-found.
``--dedupe`` compacts the manifest in place.

Output:
    data/raw/ca_leginfo_pages/
        {LAW_CODE}/{section}.html       # only valid sections kept
        manifest.jsonl                   # one line per attempted fetch (per-record)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
from pathlib import Path
from typing import Optional

from .._http import RateLimiter, default_client, setup_logging
from ..cli import add_fetcher_args
from ..manifest import ManifestWriter, dedupe, load_known_missing
from .base import FetcherConfig, register_source, run_fetcher


SOURCE_SLUG = "ca_leginfo_pages"
JURISDICTION = "CA"
DEFAULT_LAW_CODE = "VEH"

BASE_URL = "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml"
DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"
EVAL_CSV = Path("data/eval-ca-vehicle-code.csv")

logger = logging.getLogger(SOURCE_SLUG)

# Marker indicating a real section.  ``op_statues = '2022'`` for real sections,
# ``op_statues = ''`` for missing.
_VALID_SECTION_RE = re.compile(r"op_statues\s*=\s*'(\d{4})'")


# Curated PI-relevant CA Vehicle Code section ranges (used when no --sections,
# --sections-file, or --range is given).
PI_RELEVANT_RANGES: dict[str, list[tuple[int, int]]] = {
    "VEH": [
        (1, 700),         # General provisions, definitions
        (2800, 2820),     # Police/peace officer related
        (16000, 16080),   # Financial responsibility
        (20000, 20020),   # Division 10 — accidents/reports
        (21000, 23340),   # Division 11 — rules of the road
        (23100, 23250),   # Division 11.5 — DUI
        (24000, 28100),   # Divisions 12-13 — equipment, towing, loading
    ],
}


# ---------------------------------------------------------------------------
# Source config
# ---------------------------------------------------------------------------


def section_url(section: str, law_code: str) -> str:
    """leginfo expects a trailing dot after the section number."""
    return f"{BASE_URL}?sectionNum={section}.&lawCode={law_code}"


def _validity_check(section: str, body: bytes, text: str) -> tuple[bool, dict]:
    m = _VALID_SECTION_RE.search(text)
    if not m:
        return False, {}
    return True, {"op_statues": m.group(1)}


@register_source(SOURCE_SLUG)
def config_for(law_code: str = DEFAULT_LAW_CODE,
               out_root: Path = DEFAULT_OUT_DIR) -> FetcherConfig:
    return FetcherConfig(
        source_slug=SOURCE_SLUG,
        jurisdiction=JURISDICTION,
        law_code=law_code,
        code_name="Vehicle Code",
        url_builder=lambda s: section_url(s, law_code),
        validity_check=_validity_check,
        output_root=out_root,
        section_filename=lambda s: s.replace("/", "_") + ".html",
        default_concurrency=2,
        default_rate_interval=0.5,    # leginfo throttles aggressively
    )


# ---------------------------------------------------------------------------
# Section list builders
# ---------------------------------------------------------------------------


def _section_sort_key(s: str) -> tuple:
    m = re.match(r"(\d+)(?:\.(\d+))?", s)
    if not m:
        return (10**9, 0, s)
    return (int(m.group(1)), int(m.group(2) or 0), s)


def load_eval_sections(csv_path: Path = EVAL_CSV) -> list[str]:
    """Pull the unique base section numbers from the released eval CSV.

    Strips subsection markers: '21451(a)' -> '21451'; '21453(a)-(b)' -> '21453'.
    """
    if not csv_path.exists():
        return []
    sections: set[str] = set()
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = row.get("Section #", "").strip()
            if not raw:
                continue
            base = re.split(r"[\(\&\-]", raw, maxsplit=1)[0].strip()
            if base:
                sections.add(base)
    return sorted(sections, key=_section_sort_key)


def expand_ranges(ranges: list[tuple[int, int]]) -> list[str]:
    out: list[str] = []
    for lo, hi in ranges:
        out.extend(str(n) for n in range(lo, hi + 1))
    return out


def build_section_list(
    *,
    code: str,
    explicit_sections: Optional[list[str]] = None,
    sections_file: Optional[Path] = None,
    range_: Optional[tuple[int, int]] = None,
    eval_only: bool = False,
) -> list[str]:
    if explicit_sections:
        return list(explicit_sections)
    if sections_file:
        if not sections_file.exists():
            raise SystemExit(f"sections file not found at {sections_file}")
        with sections_file.open() as f:
            secs = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
        return sorted(set(secs), key=_section_sort_key)
    if range_:
        lo, hi = range_
        return [str(n) for n in range(lo, hi + 1)]
    if eval_only:
        return load_eval_sections(EVAL_CSV)
    # Default: union of eval CSV + curated PI ranges.
    all_secs: set[str] = set()
    if code == DEFAULT_LAW_CODE and EVAL_CSV.exists():
        all_secs.update(load_eval_sections(EVAL_CSV))
    ranges = PI_RELEVANT_RANGES.get(code)
    if ranges:
        all_secs.update(expand_ranges(ranges))
    return sorted(all_secs, key=_section_sort_key)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


async def run(
    *,
    law_code: str = DEFAULT_LAW_CODE,
    out_dir: Path = DEFAULT_OUT_DIR,
    concurrency: int = 2,
    rate_interval: float = 0.5,
    force: bool = False,
    sections: list[str],
    skip_known_missing: bool = True,
    heartbeat_every: float = 5.0,
    limit: Optional[int] = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"

    config = config_for(law_code=law_code, out_root=out_dir)

    if skip_known_missing and not force:
        missing = load_known_missing(
            manifest_path,
            jurisdiction=JURISDICTION,
            law_code=law_code,
        )
        if missing:
            before = len(sections)
            sections = [s for s in sections if s not in missing]
            logger.info(
                "skip-known-missing dropped=%d remaining=%d",
                before - len(sections), len(sections),
            )

    if limit is not None:
        sections = sections[:limit]

    logger.info(
        "start    code=%s sections=%d concurrency=%d out=%s",
        law_code, len(sections), concurrency, out_dir,
    )

    with ManifestWriter(manifest_path) as mw:
        stats = await run_fetcher(
            config, sections,
            force=force,
            concurrency=concurrency,
            rate_interval=rate_interval,
            manifest=mw,
            heartbeat_every=heartbeat_every,
            log=logger,
        )

    summary = {"code": law_code, "total": len(sections), **stats}
    logger.info("done     %s", summary)
    logger.info("manifest %s", manifest_path)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CA leginfo per-section HTML fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: eval seeds + curated PI-relevant ranges (VEH)
  uv run python -m ingest.sources.ca_leginfo_pages

  # Just the eval CSV's 41 sections
  uv run python -m ingest.sources.ca_leginfo_pages --eval-only

  # Explicit list
  uv run python -m ingest.sources.ca_leginfo_pages --sections 21453 21451

  # Brute-force range
  uv run python -m ingest.sources.ca_leginfo_pages --range 21000 22000

  # Different code
  uv run python -m ingest.sources.ca_leginfo_pages --code PEN --range 1 1000

  # Compact the manifest, keeping the best record per section
  uv run python -m ingest.sources.ca_leginfo_pages --dedupe
""",
    )
    add_fetcher_args(
        p,
        default_code=DEFAULT_LAW_CODE,
        default_out_dir=DEFAULT_OUT_DIR,
        default_log_file=DEFAULT_LOG_FILE,
        default_concurrency=2,
        default_rate_interval=0.5,
        supports_sections_file=True,
        supports_skip_known_missing=True,
        supports_dedupe=True,
    )
    # CA-pages-specific extras
    p.add_argument(
        "--eval-only",
        action="store_true",
        help="Only fetch sections from the eval CSV (data/eval-ca-vehicle-code.csv).",
    )
    p.add_argument(
        "--range",
        nargs=2,
        type=int,
        metavar=("LO", "HI"),
        help="Brute-force a custom integer section range (inclusive).",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])

    log_path = None if args.no_log_file else args.log_file
    setup_logging(SOURCE_SLUG, verbose=args.verbose, log_path=log_path)

    manifest_path = args.out_dir / "manifest.jsonl"

    if args.dedupe:
        before, after, qc = dedupe(
            manifest_path,
            key_fields=("law_code", "section"),
            sort_fn=lambda r: (r.get("law_code") or "",
                               _section_sort_key(r.get("section") or "")),
        )
        print(f"[dedupe] before={before} after={after}")
        print(f"[dedupe] quality: valid={qc.get('valid', 0)}  "
              f"not_found={qc.get('not_found', 0)}  "
              f"error={qc.get('error', 0)}  other={qc.get('other', 0)}")
        backup = manifest_path.with_suffix(manifest_path.suffix + ".bak")
        print(f"[dedupe] backup at {backup}")
        return 0

    sections = build_section_list(
        code=args.code,
        explicit_sections=args.sections,
        sections_file=getattr(args, "sections_file", None),
        range_=tuple(args.range) if args.range else None,
        eval_only=args.eval_only,
    )
    if not sections:
        print("No sections to fetch.")
        return 1

    asyncio.run(run(
        law_code=args.code,
        out_dir=args.out_dir,
        concurrency=args.concurrency,
        rate_interval=args.rate_interval,
        force=args.force,
        sections=sections,
        skip_known_missing=not args.no_skip_missing,
        heartbeat_every=args.heartbeat_every,
        limit=args.limit,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
