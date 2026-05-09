"""
Consolidate per-section parsed JSON files into a unified JSONL corpus.

Reads:
    data/parsed/{source_slug}/{LAW_CODE}/*.json

Writes:
    data/parsed/jsonl/{jurisdiction}/{LAW_CODE}.jsonl

Each JSONL line is a record matching this schema:

    {
      "id": "ca/veh/21453",
      "jurisdiction": "CA",
      "law_code": "VEH",
      "section": "21453",
      "code_name": "Vehicle Code",
      "section_name": null,
      "hierarchy": {
        "title":   {"ident": "1", "title": "...", "range": "..."},
        "division":{...},
        "chapter": {...},
        "article": {...},
        "part":    {...},
        "subtitle":{...},
        "subchapter":{...}
      },
      "text": "...plain-text body...",
      "markdown": "...markdown rendering...",
      "subsections": [{"label":"a", "text":"...", "depth_em":0.0}, ...],
      "history": {"raw": "...", "action":"Amended", "statutes_year":2022, ...},
      "source": {
        "url": "https://leginfo.legislature.ca.gov/...",
        "raw_path": "data/raw/ca_leginfo_pages/VEH/21453.html",
        "content_sha256": "...",
        "parsed_at": "2026-05-09T14:00:00+00:00",
        "metadata": {  # jurisdiction-specific extras
          "op_statues": "2022",
          "op_chapter": "957",
          "op_section": "3",
          "node_tree_path": "15.2.3"
        }
      }
    }

Usage:
    # All sources
    uv run python -m ingest.parsers.consolidate_jsonl

    # One source
    uv run python -m ingest.parsers.consolidate_jsonl --jurisdiction CA --code VEH

    # Custom output dir
    uv run python -m ingest.parsers.consolidate_jsonl --out data/parsed/jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional


DEFAULT_OUT_DIR = Path("data/parsed/jsonl")
DEFAULT_DATA_ROOT = Path("data/parsed")

logger = logging.getLogger("consolidate_jsonl")


@dataclass(frozen=True)
class SourceConfig:
    jurisdiction: str
    law_code: str
    parsed_dir: Path  # data/parsed/{source_slug}/{LAW_CODE}/

    @property
    def jsonl_filename(self) -> str:
        return f"{self.law_code}.jsonl"


# Map (jurisdiction) -> source dir slug, used to find parsed JSONs.
_KNOWN_SOURCES: list[SourceConfig] = [
    SourceConfig("CA", "VEH", DEFAULT_DATA_ROOT / "ca_leginfo_pages" / "VEH"),
    SourceConfig("NY", "VAT", DEFAULT_DATA_ROOT / "ny_public_law" / "VAT"),
    SourceConfig("TX", "TN", DEFAULT_DATA_ROOT / "tx_public_law" / "TN"),
]


# ---------------------------------------------------------------------------
# Schema mapping: per-section JSON -> unified JSONL record
# ---------------------------------------------------------------------------


# The metadata fields that belong in the source.metadata sub-dict (not the
# top-level fields). These are jurisdiction-specific extras that don't fit
# the unified schema cleanly.
_JURISDICTION_METADATA_FIELDS = {
    "op_statues",      # CA: stats year from JS metadata
    "op_chapter",      # CA: chapter from JS metadata
    "op_section",      # CA: section from JS metadata
    "node_tree_path",  # CA: leginfo internal node path
}

_HIERARCHY_FIELDS = {
    "title": ("title", "title_title"),
    "division": ("division", "division_title", "division_range"),
    "subtitle": ("subtitle", "subtitle_title"),
    "part": ("part", "part_title"),
    "chapter": ("chapter", "chapter_title", "chapter_range"),
    "subchapter": ("subchapter", "subchapter_title"),
    "article": ("article", "article_title", "article_range"),
}


def _section_id(jurisdiction: str, law_code: str, section: str) -> str:
    """Build a stable URI-style id for a section: 'ca/veh/21453'."""
    return f"{jurisdiction.lower()}/{law_code.lower()}/{section}"


def _hierarchy_subdict(rec: dict) -> dict[str, dict[str, Any]]:
    """Pull flat hierarchy fields from the parsed JSON into a nested dict."""
    out: dict[str, dict[str, Any]] = {}
    for level_key, fields in _HIERARCHY_FIELDS.items():
        ident_field = fields[0]
        ident = rec.get(ident_field)
        if not ident:
            continue
        node: dict[str, Any] = {"ident": ident}
        if len(fields) > 1:
            title = rec.get(fields[1])
            if title:
                node["title"] = title
        if len(fields) > 2:
            rng = rec.get(fields[2])
            if rng:
                node["range"] = rng
        out[level_key] = node
    return out


def _source_metadata(rec: dict) -> dict[str, Any]:
    """Pluck jurisdiction-specific extras into source.metadata."""
    out: dict[str, Any] = {}
    for key in _JURISDICTION_METADATA_FIELDS:
        val = rec.get(key)
        if val is not None and val != "":
            out[key] = val
    return out


def to_unified(rec: dict) -> dict[str, Any]:
    """Map a per-section parsed JSON to the unified JSONL schema."""
    jurisdiction = rec.get("jurisdiction") or "??"
    law_code = rec.get("law_code") or "??"
    section = rec.get("section_num") or rec.get("section") or "??"

    return {
        "id": _section_id(jurisdiction, law_code, section),
        "jurisdiction": jurisdiction,
        "law_code": law_code,
        "section": section,
        "code_name": rec.get("code_name"),
        "section_name": rec.get("section_name"),
        "hierarchy": _hierarchy_subdict(rec),
        "text": rec.get("text", ""),
        "markdown": rec.get("markdown", ""),
        "subsections": rec.get("subsections", []),
        "history": rec.get("history"),
        "source": {
            "url": rec.get("source_url"),
            "raw_path": rec.get("source_path"),
            "content_sha256": rec.get("content_sha256"),
            "parsed_at": rec.get("parsed_at"),
            "parser_version": rec.get("parser_version"),
            "metadata": _source_metadata(rec),
        },
    }


# ---------------------------------------------------------------------------
# Section sorting (natural order across CA, NY, TX section numbers)
# ---------------------------------------------------------------------------


def _section_sort_key(name: str) -> tuple:
    stem = Path(name).stem

    if "." in stem:
        parts = stem.split(".")
        try:
            return (0, int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, 0, "", stem)
        except ValueError:
            pass

    m = re.match(r"^(\d+)(?:\.(\d+))?(?:-([A-Za-z]+))?(?:\*(\d+))?$", stem)
    if m:
        major = int(m.group(1))
        minor = int(m.group(2)) if m.group(2) else 0
        suffix = (m.group(3) or "").upper()
        star = int(m.group(4)) if m.group(4) else 0
        return (1, major, minor, star, suffix, stem)

    return (9, 10**9, 0, 0, "", stem)


# ---------------------------------------------------------------------------
# Conversion driver
# ---------------------------------------------------------------------------


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


def consolidate_one(
    source: SourceConfig,
    out_dir: Path,
) -> tuple[int, int]:
    """Write one JSONL for a single (jurisdiction, law_code).

    Returns:
        (n_records, n_skipped) — n_skipped counts JSONs we couldn't read.
    """
    if not source.parsed_dir.is_dir():
        logger.warning("skip     %s/%s — no parsed dir at %s",
                       source.jurisdiction, source.law_code, source.parsed_dir)
        return 0, 0

    jsons = sorted(
        source.parsed_dir.glob("*.json"),
        key=lambda p: _section_sort_key(p.name),
    )
    if not jsons:
        logger.warning("skip     %s/%s — no JSON files in %s",
                       source.jurisdiction, source.law_code, source.parsed_dir)
        return 0, 0

    juris_out_dir = out_dir / source.jurisdiction.lower()
    juris_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = juris_out_dir / source.jsonl_filename

    n_records = 0
    n_skipped = 0

    with out_path.open("w") as f:
        for json_path in jsons:
            try:
                with json_path.open() as inp:
                    rec = json.load(inp)
            except (OSError, json.JSONDecodeError) as e:
                logger.warning("skip-bad %s err=%s", json_path, e)
                n_skipped += 1
                continue

            unified = to_unified(rec)
            f.write(json.dumps(unified, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
            n_records += 1

    bytes_written = out_path.stat().st_size
    logger.info(
        "ok       %s/%s -> %s (%d records, %d skipped, %.1f KB)",
        source.jurisdiction, source.law_code, out_path,
        n_records, n_skipped, bytes_written / 1024.0,
    )
    return n_records, n_skipped


def consolidate(
    *,
    sources: Iterable[SourceConfig],
    out_dir: Path,
) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    totals: dict[str, int] = {"records": 0, "skipped": 0, "files": 0}

    for source in sources:
        n_records, n_skipped = consolidate_one(source, out_dir)
        totals["records"] += n_records
        totals["skipped"] += n_skipped
        if n_records:
            totals["files"] += 1

    logger.info(
        "done     files=%d records=%d skipped=%d out=%s",
        totals["files"], totals["records"], totals["skipped"], out_dir,
    )
    return totals


def _parse_args(argv: list[str]) -> dict:
    p = argparse.ArgumentParser(
        description="Consolidate per-section parsed JSON files into unified JSONL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--jurisdiction", "-j", default=None,
        help="Filter to a single jurisdiction (CA/NY/TX). Default: all known.",
    )
    p.add_argument(
        "--code", "-c", default=None,
        help="Filter to a single law code. Default: all known for the jurisdiction.",
    )
    p.add_argument(
        "--out", dest="out_dir", type=Path, default=DEFAULT_OUT_DIR,
        help="Output root for JSONL files (default: %(default)s).",
    )
    p.add_argument("--verbose", action="store_true", help="DEBUG-level logs.")
    return vars(p.parse_args(argv))


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    _setup_logging(args["verbose"])

    sources = list(_KNOWN_SOURCES)
    if args["jurisdiction"]:
        j = args["jurisdiction"].upper()
        sources = [s for s in sources if s.jurisdiction == j]
    if args["code"]:
        c = args["code"].upper()
        sources = [s for s in sources if s.law_code == c]

    if not sources:
        raise SystemExit(
            f"no known sources match jurisdiction={args['jurisdiction']!r} "
            f"code={args['code']!r}"
        )

    consolidate(sources=sources, out_dir=args["out_dir"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())