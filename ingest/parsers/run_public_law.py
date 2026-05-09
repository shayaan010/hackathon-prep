"""
Batch driver: parse every HTML in data/raw/{ny|tx}_public_law/{CODE}/ to JSON.

Default behavior:
    Input :  data/raw/{source_slug}/{CODE}/{section}.html
    Output:  data/parsed/{source_slug}/{CODE}/{section}.json
    Manifest: data/parsed/{source_slug}/parse_manifest.jsonl

Idempotent: skipped if the existing parsed JSON's content_sha256 matches
the SHA of the current input HTML. Use --force to re-parse everything.

Usage examples:
    # Parse every cached NY VAT page
    uv run python -m ingest.parsers.run_public_law --jurisdiction NY --code VAT

    # Parse TX Transportation Code
    uv run python -m ingest.parsers.run_public_law --jurisdiction TX --code TN

    # Smoke-test on the first 10
    uv run python -m ingest.parsers.run_public_law --jurisdiction NY --code VAT --limit 10 --force
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

from .public_law import parse_public_law_file
from .types import ParseError


_SOURCE_SLUG_BY_JURISDICTION = {
    "NY": "ny_public_law",
    "TX": "tx_public_law",
}

DEFAULT_DATA_ROOT = Path("data")

logger = logging.getLogger("public_law_parser")


@dataclass
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


def _section_sort_key(name: str) -> tuple:
    """Sort section filenames in natural order:

    NY (integer + suffix letter):  '100' < '100-A' < '100-B' < '101'
    TX (decimal):                  '1.001' < '1.002' < '5.001' < '21.001'
    """
    stem = Path(name).stem

    if "." in stem:
        parts = stem.split(".")
        try:
            major = int(parts[0])
            minor = int(parts[1])
            return (0, major, minor, 0, "", stem)
        except ValueError:
            pass

    m = re.match(r"^(\d+)(?:-([A-Za-z]+))?(?:\*(\d+))?$", stem)
    if m:
        major = int(m.group(1))
        suffix = (m.group(2) or "").upper()
        star = int(m.group(3)) if m.group(3) else 0
        return (1, major, 0, star, suffix, stem)

    return (9, 10**9, 0, 0, "", stem)


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


def parse_one(
    raw_path: Path,
    out_dir: Path,
    *,
    jurisdiction: str,
    law_code: str,
    force: bool = False,
) -> ParseRecord:
    section = raw_path.stem
    parsed_path = out_dir / law_code / f"{section}.json"

    raw_bytes = raw_path.read_bytes()
    sha = _sha256_bytes(raw_bytes)

    if not force:
        existing_sha = _load_existing_sha(parsed_path)
        if existing_sha == sha:
            logger.debug("skip-cached section=%s", section)
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
        rec = parse_public_law_file(
            raw_path,
            jurisdiction=jurisdiction,
            law_code=law_code,
        )
    except ParseError as e:
        logger.warning("parse-error section=%s err=%s", section, e)
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
        logger.error(
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
        jurisdiction=jurisdiction,
        ok=True,
        raw_path=str(raw_path),
        parsed_path=str(parsed_path),
        parsed_at=_now_iso(),
        content_sha256=sha,
        skipped_cached=False,
    )


def run(
    *,
    jurisdiction: str,
    code: str,
    in_dir: Optional[Path] = None,
    out_dir: Optional[Path] = None,
    force: bool = False,
    limit: Optional[int] = None,
    verbose: bool = False,
) -> list[ParseRecord]:
    _setup_logging(verbose)

    source_slug = _SOURCE_SLUG_BY_JURISDICTION.get(jurisdiction)
    if source_slug is None:
        raise SystemExit(
            f"unknown jurisdiction {jurisdiction!r}; expected one of "
            f"{sorted(_SOURCE_SLUG_BY_JURISDICTION)}"
        )

    if in_dir is None:
        in_dir = DEFAULT_DATA_ROOT / "raw" / source_slug
    if out_dir is None:
        out_dir = DEFAULT_DATA_ROOT / "parsed" / source_slug

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
        "start    jurisdiction=%s code=%s files=%d in=%s out=%s force=%s",
        jurisdiction, code, len(files), code_in_dir, out_dir, force,
    )

    records: list[ParseRecord] = []
    t0 = time.time()
    cached = fresh = failed = 0

    for path in files:
        rec = parse_one(
            path,
            out_dir,
            jurisdiction=jurisdiction,
            law_code=code,
            force=force,
        )
        records.append(rec)
        if not rec.ok:
            failed += 1
        elif rec.skipped_cached:
            cached += 1
        else:
            fresh += 1

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


def _parse_args(argv: list[str]) -> dict:
    p = argparse.ArgumentParser(
        description="Parse public.law per-section HTML pages into structured JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--jurisdiction", "-j", required=True,
        choices=sorted(_SOURCE_SLUG_BY_JURISDICTION),
        help="Jurisdiction code: NY or TX.",
    )
    p.add_argument(
        "--code", "-c", required=True,
        help="Law code subdirectory (e.g. VAT for NY, TN for TX).",
    )
    p.add_argument(
        "--in", dest="in_dir", type=Path, default=None,
        help="Override input root (default: data/raw/{ny|tx}_public_law).",
    )
    p.add_argument(
        "--out", dest="out_dir", type=Path, default=None,
        help="Override output root (default: data/parsed/{ny|tx}_public_law).",
    )
    p.add_argument("--force", action="store_true", help="Re-parse everything.")
    p.add_argument("--limit", type=int, default=None, help="Cap number of sections.")
    p.add_argument("--verbose", action="store_true", help="DEBUG-level logs.")
    return vars(p.parse_args(argv))


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    run(
        jurisdiction=args["jurisdiction"],
        code=args["code"],
        in_dir=args["in_dir"],
        out_dir=args["out_dir"],
        force=args["force"],
        limit=args["limit"],
        verbose=args["verbose"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())