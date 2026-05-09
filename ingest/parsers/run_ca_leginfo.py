"""
Batch driver: parse every HTML in data/raw/ca_leginfo_pages/{CODE}/ to JSON.

Default behavior:
    Input :  data/raw/ca_leginfo_pages/{CODE}/{section}.html
    Output:  data/parsed/ca_leginfo_pages/{CODE}/{section}.json
    Manifest: data/parsed/ca_leginfo_pages/parse_manifest.jsonl

Idempotent: a section is skipped if the existing parsed JSON's
``content_sha256`` matches the SHA of the current input HTML.  Use
``--force`` to re-parse everything.

Usage examples:
    # Parse every cached VEH page
    uv run python -m ingest.parsers.run_ca_leginfo

    # Smoke-test on the first 10
    uv run python -m ingest.parsers.run_ca_leginfo --limit 10 --force

    # Different law code
    uv run python -m ingest.parsers.run_ca_leginfo --code PEN

    # Custom paths
    uv run python -m ingest.parsers.run_ca_leginfo \\
        --in-dir data/raw/ca_leginfo_pages \\
        --out-dir data/parsed/ca_leginfo_pages
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from .._http import setup_logging
from ..cli import add_parser_runner_args
from .ca_leginfo import parse_section_file
from .runner import run_parser


DEFAULT_IN_DIR = Path("data/raw/ca_leginfo_pages")
DEFAULT_OUT_DIR = Path("data/parsed/ca_leginfo_pages")
DEFAULT_LOG_FILE = Path("data/logs/run_ca_leginfo.log")
DEFAULT_JURISDICTION = "CA"
DEFAULT_CODE = "VEH"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Parse CA leginfo per-section HTML pages into structured JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_parser_runner_args(
        p,
        default_jurisdiction=DEFAULT_JURISDICTION,
        default_code=DEFAULT_CODE,
        default_in_dir=DEFAULT_IN_DIR,
        default_out_dir=DEFAULT_OUT_DIR,
        default_log_file=DEFAULT_LOG_FILE,
        jurisdiction_choices=["CA"],
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])

    log_path = None if args.no_log_file else args.log_file
    log = setup_logging("run_ca_leginfo", verbose=args.verbose, log_path=log_path)

    run_parser(
        parse_fn=parse_section_file,
        jurisdiction=args.jurisdiction,
        law_code=args.code,
        raw_dir=args.in_dir,
        parsed_dir=args.out_dir,
        force=args.force,
        limit=args.limit,
        log=log,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
