"""
Consolidate per-section parsed JSON files into a unified JSONL corpus.

Reads:
    data/parsed/{source_slug}/{LAW_CODE}/*.json

Writes:
    data/parsed/jsonl/{jurisdiction}/{LAW_CODE}.jsonl

Sources are **auto-discovered**: any subdirectory of ``data/parsed/`` that
looks like ``{slug}/{CODE}/`` and contains valid StatuteSection JSON files
gets included.  Adding a new jurisdiction means dropping a new
``ingest/sources/<slug>.py`` + parser and re-running this — no edits here.

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
        ...
      },
      "text": "...",
      "markdown": "...",
      "subsections": [{"label":"a", "text":"...", "depth_em":0.0}, ...],
      "history": {"raw": "...", "action":"Amended", "statutes_year":2022, ...},
      "source": {
        "url": "...",
        "raw_path": "...",
        "content_sha256": "...",
        "parsed_at": "...",
        "metadata": {  # jurisdiction-specific extras
          "op_statues": "2022", ...
        }
      }
    }

Usage:
    # All sources (auto-discovered)
    uv run python -m ingest.parsers.consolidate_jsonl

    # Filter to one jurisdiction
    uv run python -m ingest.parsers.consolidate_jsonl --jurisdiction CA

    # Filter to a specific code
    uv run python -m ingest.parsers.consolidate_jsonl --jurisdiction CA --code VEH
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from .._http import setup_logging
from ..cli import section_sort_key


DEFAULT_OUT_DIR = Path("data/parsed/jsonl")
DEFAULT_DATA_ROOT = Path("data/parsed")
DEFAULT_LOG_FILE = Path("data/logs/consolidate_jsonl.log")

logger = logging.getLogger("consolidate_jsonl")


@dataclass(frozen=True)
class SourceConfig:
    jurisdiction: str
    law_code: str
    parsed_dir: Path  # data/parsed/{source_slug}/{LAW_CODE}/

    @property
    def jsonl_filename(self) -> str:
        return f"{self.law_code}.jsonl"


# ---------------------------------------------------------------------------
# Auto-discovery
# ---------------------------------------------------------------------------


def discover_sources(parsed_root: Path = DEFAULT_DATA_ROOT) -> list[SourceConfig]:
    """Walk ``data/parsed/<slug>/<CODE>/*.json`` and infer (jurisdiction, code).

    For each candidate ``CODE`` directory, reads the first parsed JSON and
    pulls ``jurisdiction`` and ``law_code`` from it (the JSONs are
    self-describing).  Skips directories that don't contain readable
    StatuteSection JSON.
    """
    if not parsed_root.is_dir():
        return []

    sources: list[SourceConfig] = []
    seen: set[tuple[str, str]] = set()

    for slug_dir in sorted(parsed_root.iterdir()):
        if not slug_dir.is_dir():
            continue
        # Skip the JSONL output dir
        if slug_dir.name == "jsonl":
            continue
        for code_dir in sorted(slug_dir.iterdir()):
            if not code_dir.is_dir():
                continue
            jsons = list(code_dir.glob("*.json"))
            if not jsons:
                continue
            # Sniff the first parseable JSON for jurisdiction + law_code
            jurisdiction: Optional[str] = None
            law_code: Optional[str] = None
            for j in jsons[:5]:
                try:
                    with j.open() as f:
                        rec = json.load(f)
                    jurisdiction = rec.get("jurisdiction")
                    law_code = rec.get("law_code")
                    if jurisdiction and law_code:
                        break
                except (OSError, json.JSONDecodeError):
                    continue
            if not jurisdiction or not law_code:
                logger.debug("auto-discover: skip %s (no parseable JSON)", code_dir)
                continue
            key = (jurisdiction, law_code)
            if key in seen:
                # Multiple sources for the same (jurisdiction, code) — skip later one
                logger.warning(
                    "auto-discover: duplicate (%s, %s) at %s — keeping first",
                    jurisdiction, law_code, code_dir,
                )
                continue
            seen.add(key)
            sources.append(SourceConfig(
                jurisdiction=jurisdiction,
                law_code=law_code,
                parsed_dir=code_dir,
            ))
            logger.debug(
                "auto-discover: %s/%s <- %s (%d JSONs)",
                jurisdiction, law_code, code_dir, len(jsons),
            )
    return sources


# ---------------------------------------------------------------------------
# Schema mapping: per-section JSON -> unified JSONL record
# ---------------------------------------------------------------------------


# Jurisdiction-specific extras that don't fit the unified schema cleanly.
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
    return f"{jurisdiction.lower()}/{law_code.lower()}/{section}"


def _hierarchy_subdict(rec: dict) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for level_key, fields in _HIERARCHY_FIELDS.items():
        ident_field = fields[0]
        ident = rec.get(ident_field)
        if not ident:
            continue
        node: dict[str, Any] = {"ident": ident}
        if len(fields) > 1:
            t = rec.get(fields[1])
            if t:
                node["title"] = t
        if len(fields) > 2:
            r = rec.get(fields[2])
            if r:
                node["range"] = r
        out[level_key] = node
    return out


def _source_metadata(rec: dict) -> dict[str, Any]:
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
# Conversion driver
# ---------------------------------------------------------------------------


def consolidate_one(
    source: SourceConfig,
    out_dir: Path,
) -> tuple[int, int]:
    """Write one JSONL for a single (jurisdiction, law_code).

    Returns ``(n_records, n_skipped)``.
    """
    if not source.parsed_dir.is_dir():
        logger.warning("skip     %s/%s — no parsed dir at %s",
                       source.jurisdiction, source.law_code, source.parsed_dir)
        return 0, 0

    jsons = sorted(
        source.parsed_dir.glob("*.json"),
        key=lambda p: section_sort_key(p.stem),
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

    with out_path.open("w", encoding="utf-8") as f:
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Consolidate per-section parsed JSON files into unified JSONL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--jurisdiction", "-j", default=None,
        help="Filter to one jurisdiction (e.g. CA, NY, TX). Default: all auto-discovered.",
    )
    p.add_argument(
        "--code", "-c", default=None,
        help="Filter to one law code. Default: all auto-discovered.",
    )
    p.add_argument(
        "--in-dir", type=Path, default=DEFAULT_DATA_ROOT,
        help=f"Parsed-data root to scan (default: {DEFAULT_DATA_ROOT})",
    )
    p.add_argument(
        "--out-dir", type=Path, default=DEFAULT_OUT_DIR,
        help=f"Output root for JSONL files (default: {DEFAULT_OUT_DIR})",
    )
    p.add_argument(
        "--log-file", type=Path, default=DEFAULT_LOG_FILE,
        help=f"File to append log lines to (default: {DEFAULT_LOG_FILE})",
    )
    p.add_argument("--no-log-file", action="store_true",
                   help="Disable file logging.")
    p.add_argument("--verbose", action="store_true",
                   help="DEBUG-level logs.")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])

    log_path = None if args.no_log_file else args.log_file
    setup_logging("consolidate_jsonl", verbose=args.verbose, log_path=log_path)

    sources = discover_sources(args.in_dir)
    if args.jurisdiction:
        j = args.jurisdiction.upper()
        sources = [s for s in sources if s.jurisdiction == j]
    if args.code:
        c = args.code.upper()
        sources = [s for s in sources if s.law_code == c]

    if not sources:
        raise SystemExit(
            f"no parsed sources found under {args.in_dir} matching "
            f"jurisdiction={args.jurisdiction!r} code={args.code!r}"
        )

    logger.info("discovered %d source(s):", len(sources))
    for s in sources:
        logger.info("  %s/%s <- %s", s.jurisdiction, s.law_code, s.parsed_dir)

    consolidate(sources=sources, out_dir=args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
