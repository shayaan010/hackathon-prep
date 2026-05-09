"""
Batch driver: parse every HTML in data/raw/{ny|tx}_public_law/{CODE}/ to JSON.

Default behavior:
    Input :  data/raw/{ny|tx}_public_law/{CODE}/{section}.html
    Output:  data/parsed/{ny|tx}_public_law/{CODE}/{section}.json
    Manifest: data/parsed/{ny|tx}_public_law/parse_manifest.jsonl

Idempotent: a section is skipped if the existing parsed JSON's
``content_sha256`` matches the SHA of the current input HTML.  Use
``--force`` to re-parse everything.

Usage examples:
    # Parse every cached NY VAT page (uses NY default)
    uv run python -m ingest.parsers.run_public_law -j NY

    # Parse TX Transportation Code (uses TX default)
    uv run python -m ingest.parsers.run_public_law -j TX

    # Override code
    uv run python -m ingest.parsers.run_public_law -j NY -c VAT --limit 10 --force
"""
from __future__ import annotations

import argparse
import sys
from functools import partial
from pathlib import Path
from typing import Optional

from .._http import setup_logging
from ..cli import add_parser_runner_args
from .public_law import parse_public_law_file
from .runner import run_parser


SOURCE_SLUG_BY_JURISDICTION = {
    "NY": "ny_public_law",
    "TX": "tx_public_law",
    "FL": "fl_public_law",
    "OR": "or_public_law",
    "NV": "nv_public_law",
}
DEFAULT_CODE_BY_JURISDICTION = {
    "NY": "VAT",
    "TX": "TN",
    "FL": "TXX",
    "OR": "ORS59",
    "NV": "NRS43",
}
DEFAULT_DATA_ROOT = Path("data")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Parse public.law per-section HTML pages into structured JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_parser_runner_args(
        p,
        default_jurisdiction=None,           # required
        default_code=None,                   # filled in based on --jurisdiction
        default_in_dir=None,                 # filled in based on --jurisdiction
        default_out_dir=None,
        default_log_file=None,
        jurisdiction_choices=sorted(SOURCE_SLUG_BY_JURISDICTION),
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])

    if args.jurisdiction is None:
        print(
            "error: --jurisdiction is required ("
            + ", ".join(sorted(SOURCE_SLUG_BY_JURISDICTION))
            + ")",
            file=sys.stderr,
        )
        return 2

    source_slug = SOURCE_SLUG_BY_JURISDICTION[args.jurisdiction]

    # Apply per-jurisdiction defaults if user didn't override.
    code = args.code or DEFAULT_CODE_BY_JURISDICTION[args.jurisdiction]
    raw_dir = args.in_dir or (DEFAULT_DATA_ROOT / "raw" / source_slug)
    parsed_dir = args.out_dir or (DEFAULT_DATA_ROOT / "parsed" / source_slug)
    log_file = args.log_file or Path(f"data/logs/run_public_law_{args.jurisdiction.lower()}.log")

    log_path = None if args.no_log_file else log_file
    log = setup_logging(
        f"run_public_law_{args.jurisdiction.lower()}",
        verbose=args.verbose,
        log_path=log_path,
    )

    parse_fn = partial(
        parse_public_law_file,
        jurisdiction=args.jurisdiction,
        law_code=code,
    )

    run_parser(
        parse_fn=parse_fn,
        jurisdiction=args.jurisdiction,
        law_code=code,
        raw_dir=raw_dir,
        parsed_dir=parsed_dir,
        force=args.force,
        limit=args.limit,
        log=log,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
