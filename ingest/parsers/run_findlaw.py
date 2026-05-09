"""
Batch runner for FindLaw statute section parser.

Usage:
    uv run python -m ingest.parsers.run_findlaw NY_VAT
    uv run python -m ingest.parsers.run_findlaw TX_TRANSP --force
"""
from __future__ import annotations

import hashlib
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

from .findlaw import parse_findlaw_html

logger = logging.getLogger("run_findlaw")

SOURCE_SLUG = "findlaw_pages"
DEFAULT_RAW_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_PARSED_DIR = Path("data/parsed") / SOURCE_SLUG
PARSER_VERSION = "1"

from ..sources.findlaw_sections import CODES, CodeConfig


def parse_one(
    raw_path: Path,
    parsed_dir: Path,
    config: CodeConfig,
    *,
    force: bool = False,
) -> dict:
    slug = config.slug
    out_path = parsed_dir / slug / (raw_path.stem + ".json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw_text = raw_path.read_text(encoding="utf-8", errors="ignore")
    sha = hashlib.sha256(raw_text.encode("utf-8", "ignore")).hexdigest()

    if out_path.exists() and not force:
        existing = json.loads(out_path.read_text())
        if existing.get("content_sha256") == sha:
            return {"status": "cached", "path": str(out_path)}

    try:
        section = parse_findlaw_html(
            raw_text,
            jurisdiction=config.jurisdiction,
            law_code=config.law_code,
            source_path=str(raw_path),
        )
    except Exception as e:
        logger.error("parse-error path=%s err=%s", raw_path, e)
        return {"status": "error", "path": str(raw_path), "error": str(e)}

    out_path.write_text(section.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return {"status": "ok", "path": str(out_path)}


def run(
    slug: str,
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    parsed_dir: Path = DEFAULT_PARSED_DIR,
    force: bool = False,
    verbose: bool = False,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    config = CODES.get(slug)
    if config is None:
        logger.error("Unknown code slug: %s (available: %s)", slug, list(CODES.keys()))
        raise SystemExit(1)

    code_raw_dir = raw_dir / slug
    if not code_raw_dir.exists():
        logger.error("Raw directory not found: %s", code_raw_dir)
        raise SystemExit(1)

    html_files = sorted(code_raw_dir.glob("*.html"))
    if not html_files:
        logger.error("No HTML files in %s", code_raw_dir)
        raise SystemExit(1)

    logger.info("start slug=%s files=%d in=%s out=%s", slug, len(html_files), code_raw_dir, parsed_dir / slug)

    manifest_path = parsed_dir / "parse_manifest_findlaw.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    fresh = 0
    cached = 0
    errors = 0
    t0 = time.time()

    for i, f in enumerate(html_files, 1):
        result = parse_one(f, parsed_dir, config, force=force)
        status = result["status"]
        if status == "ok":
            fresh += 1
        elif status == "cached":
            cached += 1
        elif status == "error":
            errors += 1

        with manifest_path.open("a") as mf:
            mf.write(json.dumps({
                "slug": slug,
                "file": f.name,
                "status": status,
                "path": result.get("path", ""),
                **({"error": result["error"]} if status == "error" else {}),
            }) + "\n")

    elapsed = time.time() - t0
    logger.info(
        "done ok=%d (fresh=%d cached=%d) failed=%d elapsed=%.1fs (%.1f/s)",
        fresh + cached, fresh, cached, errors, elapsed,
        (fresh + cached) / max(elapsed, 0.001),
    )
    logger.info("manifest %s", manifest_path)


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Parse FindLaw statute section HTML pages")
    p.add_argument("slug", choices=list(CODES.keys()), help="Code slug (e.g. NY_VAT, TX_TRANSP)")
    p.add_argument("--force", action="store_true", help="Re-parse even if output exists")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    run(args.slug, force=args.force, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())