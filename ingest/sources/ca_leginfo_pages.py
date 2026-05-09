"""
California leginfo per-section HTML fetcher.

Each statute section has a stable URL on leginfo.legislature.ca.gov:

    https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21453.&lawCode=VEH

Notes:
- The trailing dot after sectionNum is required.
- The server returns HTTP 200 even for non-existent sections.
- Real sections embed `op_statues = 'YYYY'` (chaptered year) in the body.
  Missing sections embed `op_statues = ''`. We use this to skip the noise.

Strategy:
- Seed list = (a) all base section numbers from data/eval-ca-vehicle-code.csv
  + (b) optional brute-force range like 1-42000.
- High concurrency (default 20) — pages are ~160KB, simple GETs.
- Idempotent: skip if file already on disk.

Output:
    data/raw/ca_leginfo_pages/
        {LAW_CODE}/
            {section}.html         # only valid sections kept
        manifest.jsonl              # one line per attempted fetch
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import httpx


SOURCE_SLUG = "ca_leginfo_pages"
JURISDICTION = "CA"
BASE_URL = "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml"
DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
EVAL_CSV = Path("data/eval-ca-vehicle-code.csv")

DEFAULT_LAW_CODE = "VEH"
DEFAULT_CONCURRENCY = 20
DEFAULT_TIMEOUT_S = 30.0
USER_AGENT = (
    "OpenClawHackathonHarvester/0.1 "
    "(legal research; contact: hackathon@example.com)"
)

# Marker indicating a real section. The leginfo page sets `op_statues = '2022'`
# (or another year) for real sections, and `op_statues = ''` for missing ones.
_VALID_SECTION_RE = re.compile(r"op_statues\s*=\s*'(\d{4})'")


@dataclass
class FetchRecord:
    source_slug: str
    jurisdiction: str
    law_code: str
    section: str
    url: str
    fetched_at: str
    http_status: int
    bytes: int
    content_sha256: str
    raw_path: Optional[str]
    valid: bool
    op_statues: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Seed lists
# ---------------------------------------------------------------------------


def load_eval_sections(csv_path: Path = EVAL_CSV) -> list[str]:
    """Pull the unique base section numbers from the released eval CSV."""
    sections: set[str] = set()
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = row.get("Section #", "").strip()
            if not raw:
                continue
            # Strip subsection in parens: "21451(a)" -> "21451"; "21453(a)-(b)" -> "21453"
            base = re.split(r"[\(\&\-]", raw, maxsplit=1)[0].strip()
            if base:
                sections.add(base)
    return sorted(sections, key=_section_sort_key)


# Curated PI-relevant CA Vehicle Code section ranges.
# Source: divisions of CA Vehicle Code most relevant to personal-injury cases.
PI_RELEVANT_RANGES = {
    "VEH": [
        (1, 700),         # General provisions, definitions
        (2800, 2820),     # Police/peace officer related
        (16000, 16080),   # Financial responsibility
        (20000, 20020),   # Division 10 — accidents/reports (hit-and-run)
        (21000, 23340),   # Division 11 — rules of the road (the heart of PI)
        (23100, 23250),   # Division 11.5 — DUI
        (24000, 28100),   # Division 12-13 — equipment, towing, loading
    ],
}


def expand_ranges(ranges: list[tuple[int, int]]) -> list[str]:
    out: list[str] = []
    for lo, hi in ranges:
        out.extend(str(n) for n in range(lo, hi + 1))
    return out


def _section_sort_key(s: str) -> tuple:
    """Sort like ('21451' < '21451.5' < '21452')."""
    m = re.match(r"(\d+)(?:\.(\d+))?", s)
    if not m:
        return (10**9, 0, s)
    return (int(m.group(1)), int(m.group(2) or 0), s)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def section_url(section: str, law_code: str) -> str:
    # Leginfo expects a trailing dot after the section number.
    section_param = f"{section}."
    return f"{BASE_URL}?sectionNum={section_param}&lawCode={law_code}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def fetch_section(
    client: httpx.AsyncClient,
    section: str,
    law_code: str,
    out_dir: Path,
    *,
    force: bool = False,
) -> FetchRecord:
    code_dir = out_dir / law_code
    code_dir.mkdir(parents=True, exist_ok=True)
    safe_name = section.replace("/", "_") + ".html"
    dest = code_dir / safe_name
    url = section_url(section, law_code)

    # Idempotent skip if the HTML is already on disk.
    if dest.exists() and dest.stat().st_size > 0 and not force:
        text = dest.read_text(errors="ignore")
        m = _VALID_SECTION_RE.search(text)
        return FetchRecord(
            source_slug=SOURCE_SLUG,
            jurisdiction=JURISDICTION,
            law_code=law_code,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=200,
            bytes=dest.stat().st_size,
            content_sha256=hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest(),
            raw_path=str(dest),
            valid=bool(m),
            op_statues=m.group(1) if m else None,
        )

    try:
        resp = await client.get(url)
    except Exception as e:
        return FetchRecord(
            source_slug=SOURCE_SLUG,
            jurisdiction=JURISDICTION,
            law_code=law_code,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=0,
            bytes=0,
            content_sha256="",
            raw_path=None,
            valid=False,
            error=f"{type(e).__name__}: {e}",
        )

    body = resp.content
    text = resp.text
    m = _VALID_SECTION_RE.search(text)
    valid = bool(m)
    sha = hashlib.sha256(body).hexdigest()

    if valid:
        dest.write_bytes(body)
        raw_path: Optional[str] = str(dest)
    else:
        raw_path = None  # don't persist 160KB of "section doesn't exist" chrome

    return FetchRecord(
        source_slug=SOURCE_SLUG,
        jurisdiction=JURISDICTION,
        law_code=law_code,
        section=section,
        url=url,
        fetched_at=_now_iso(),
        http_status=resp.status_code,
        bytes=len(body),
        content_sha256=sha,
        raw_path=raw_path,
        valid=valid,
        op_statues=m.group(1) if m else None,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def run(
    sections: Iterable[str],
    law_code: str = DEFAULT_LAW_CODE,
    *,
    out_dir: Path = DEFAULT_OUT_DIR,
    concurrency: int = DEFAULT_CONCURRENCY,
    force: bool = False,
    progress_every: int = 100,
) -> list[FetchRecord]:
    sections = list(sections)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*;q=0.8"}
    timeout = httpx.Timeout(DEFAULT_TIMEOUT_S, connect=15.0)

    print(
        f"[start] {len(sections)} sections, code={law_code}, "
        f"concurrency={concurrency}",
        flush=True,
    )

    sem = asyncio.Semaphore(concurrency)
    completed = 0
    valid_count = 0
    bytes_total = 0
    lock = asyncio.Lock()

    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
        http2=False,
        limits=httpx.Limits(
            max_connections=concurrency * 2,
            max_keepalive_connections=concurrency,
        ),
    ) as client:

        async def _bounded(section: str) -> FetchRecord:
            nonlocal completed, valid_count, bytes_total
            async with sem:
                rec = await fetch_section(
                    client, section, law_code, out_dir, force=force
                )
            async with lock:
                completed += 1
                bytes_total += rec.bytes
                if rec.valid:
                    valid_count += 1
                if completed % progress_every == 0 or completed == len(sections):
                    print(
                        f"  ... {completed}/{len(sections)}  "
                        f"valid={valid_count}  bytes={bytes_total / (1<<20):.1f} MB",
                        flush=True,
                    )
            return rec

        records: list[FetchRecord] = await asyncio.gather(
            *[_bounded(s) for s in sections]
        )

    # Append to manifest (don't truncate — keep history of fetch runs).
    with manifest_path.open("a") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec)) + "\n")

    valid = [r for r in records if r.valid]
    invalid = [r for r in records if not r.valid and not r.error]
    failed = [r for r in records if r.error]
    print(
        f"\n[summary] {len(valid)} valid, {len(invalid)} not-found, "
        f"{len(failed)} failed; {bytes_total / (1<<20):.1f} MB"
    )
    if failed:
        for r in failed[:10]:
            print(f"  ✗ {r.section}: {r.error}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
    print(f"[manifest] {manifest_path}")
    return records


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str]) -> dict:
    import argparse

    p = argparse.ArgumentParser(
        description="CA leginfo per-section HTML fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Just the released eval set (41 sections — guaranteed coverage)
  uv run python -m ingest.sources.ca_leginfo_pages --eval-only

  # Eval set + the entire PI-relevant range for VEH
  uv run python -m ingest.sources.ca_leginfo_pages

  # Custom range (brute-force)
  uv run python -m ingest.sources.ca_leginfo_pages --range 21000 22000

  # Different law code (Penal Code)
  uv run python -m ingest.sources.ca_leginfo_pages --code PEN --range 1 1000
""",
    )
    p.add_argument(
        "--code",
        default=DEFAULT_LAW_CODE,
        help="Law code (VEH, PEN, HSC, ...). Default: %(default)s",
    )
    p.add_argument(
        "--eval-only",
        action="store_true",
        help="Only fetch sections from the eval CSV.",
    )
    p.add_argument(
        "--range",
        nargs=2,
        type=int,
        metavar=("LO", "HI"),
        help="Brute-force a custom integer section range (inclusive).",
    )
    p.add_argument(
        "--sections",
        nargs="+",
        help="Explicit list of section numbers to fetch.",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Output directory (default: %(default)s)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Parallel fetches (default: %(default)s)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even if HTML already on disk.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap number of sections (smoke testing).",
    )
    return vars(p.parse_args(argv))


def _build_section_list(args: dict) -> list[str]:
    code = args["code"]
    if args["sections"]:
        return list(args["sections"])
    if args["range"]:
        lo, hi = args["range"]
        return [str(n) for n in range(lo, hi + 1)]
    if args["eval_only"]:
        if not EVAL_CSV.exists():
            raise SystemExit(f"eval CSV not found at {EVAL_CSV}")
        return load_eval_sections(EVAL_CSV)

    # Default: eval seeds + curated PI-relevant ranges (deduped, sorted).
    all_secs: set[str] = set()
    if EVAL_CSV.exists() and code == DEFAULT_LAW_CODE:
        all_secs.update(load_eval_sections(EVAL_CSV))
    ranges = PI_RELEVANT_RANGES.get(code)
    if ranges:
        all_secs.update(expand_ranges(ranges))
    return sorted(all_secs, key=_section_sort_key)


async def _main_async(args: dict) -> int:
    sections = _build_section_list(args)
    if args["limit"] is not None:
        sections = sections[: args["limit"]]
    if not sections:
        print("No sections to fetch.")
        return 1

    await run(
        sections,
        law_code=args["code"],
        out_dir=args["out"],
        concurrency=args["concurrency"],
        force=args["force"],
    )
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
