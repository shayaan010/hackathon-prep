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
DEFAULT_CONCURRENCY = 2   # leginfo throttles aggressively; 2 is sustainable
DEFAULT_TIMEOUT_S = 30.0

# leginfo returns 403s less often when the request looks like a real browser.
# This is not deception — we identify ourselves in the URL contact param if asked.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 403 / rate-limit retry policy. Each attempt waits BACKOFF_BASE * 2**(attempt-1) sec
# plus a small jitter, capped at MAX_BACKOFF.
# leginfo's 403s are noisy/transient (per probing: ~30% baseline rate even sequential)
# so we use SHORT backoffs — 0.5s, 1s, 2s, 4s, 8s — to recover quickly.
MAX_RETRIES_403 = 6
BACKOFF_BASE = 0.5
MAX_BACKOFF = 30.0
RETRY_STATUSES = {403, 429, 502, 503, 504}

# Logger setup
import logging
logger = logging.getLogger("ca_leginfo_pages")

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
        logger.debug("cache hit  section=%s valid=%s", section, bool(m))
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

    # Retry loop for transient errors (403, 429, 5xx, network failures).
    last_status: int = 0
    last_error: Optional[str] = None
    body: bytes = b""
    text: str = ""
    resp: Optional[httpx.Response] = None

    for attempt in range(1, MAX_RETRIES_403 + 1):
        try:
            resp = await client.get(url)
            last_status = resp.status_code

            if resp.status_code in RETRY_STATUSES:
                # backoff: 0.5s, 1s, 2s, 4s, 8s, 16s with small jitter
                wait = min(
                    BACKOFF_BASE * (2 ** (attempt - 1)) + 0.1 * attempt,
                    MAX_BACKOFF,
                )
                logger.warning(
                    "retry    section=%s attempt=%d/%d status=%d backoff=%.1fs",
                    section, attempt, MAX_RETRIES_403,
                    resp.status_code, wait,
                )
                if attempt < MAX_RETRIES_403:
                    await asyncio.sleep(wait)
                    continue
                # exhausted retries — fall through with last response
                break

            # Success path (or any non-retryable status)
            body = resp.content
            text = resp.text
            break

        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError,
                httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
            last_error = f"{type(e).__name__}: {e}"
            wait = min(
                BACKOFF_BASE * (2 ** (attempt - 1)) + 0.1 * attempt,
                MAX_BACKOFF,
            )
            logger.warning(
                "retry    section=%s attempt=%d/%d error=%s backoff=%.1fs",
                section, attempt, MAX_RETRIES_403, type(e).__name__, wait,
            )
            if attempt < MAX_RETRIES_403:
                await asyncio.sleep(wait)
                continue
            # Exhausted retries on network error
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
                error=last_error,
            )

    # If we still have a retryable status after exhausting attempts, mark as error
    # so a future run will retry this section instead of caching it as "missing".
    if resp is not None and resp.status_code in RETRY_STATUSES:
        logger.error(
            "give-up  section=%s status=%d after %d retries",
            section, resp.status_code, MAX_RETRIES_403,
        )
        return FetchRecord(
            source_slug=SOURCE_SLUG,
            jurisdiction=JURISDICTION,
            law_code=law_code,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=resp.status_code,
            bytes=len(resp.content),
            content_sha256="",
            raw_path=None,
            valid=False,
            error=f"HTTP {resp.status_code} after {MAX_RETRIES_403} retries",
        )

    if resp is None:
        # Should not happen — but guard against it
        return FetchRecord(
            source_slug=SOURCE_SLUG,
            jurisdiction=JURISDICTION,
            law_code=law_code,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=last_status,
            bytes=0,
            content_sha256="",
            raw_path=None,
            valid=False,
            error=last_error or "no response",
        )

    m = _VALID_SECTION_RE.search(text)
    valid = bool(m)
    sha = hashlib.sha256(body).hexdigest()

    if valid:
        dest.write_bytes(body)
        raw_path: Optional[str] = str(dest)
        logger.debug(
            "ok       section=%s status=%d bytes=%d op_statues=%s",
            section, resp.status_code, len(body), m.group(1) if m else "",
        )
    else:
        raw_path = None
        logger.debug(
            "no-stmt  section=%s status=%d (section does not exist)",
            section, resp.status_code,
        )

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


def _record_quality(rec: dict) -> int:
    """Higher = more informative. Used to pick the best record per section.

    Priority:
      4: valid=True (we have the statute on disk)
      3: 200 not-found (server says it doesn't exist)
      2: any other 2xx/3xx with no error
      1: 4xx with no error
      0: errored / 5xx / connect failure
    """
    if rec.get("valid"):
        return 4
    if rec.get("error"):
        return 0
    status = rec.get("http_status") or 0
    if status == 200:
        return 3
    if 200 <= status < 400:
        return 2
    if 400 <= status < 500:
        return 1
    return 0


def dedupe_manifest(manifest_path: Path) -> tuple[int, int, dict[str, int]]:
    """Compact the manifest in place, keeping the best record per (law_code, section).

    Returns (records_before, records_after, quality_counts).
    Older duplicate lines are dropped. Records with no section/law_code are kept as-is.
    """
    if not manifest_path.exists():
        return 0, 0, {}

    lines = [l for l in manifest_path.read_text().splitlines() if l.strip()]
    before = len(lines)

    best: dict[tuple, dict] = {}
    keepers: list[dict] = []  # records without a section key (shouldn't exist, but safe)
    for line in lines:
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        key = (rec.get("law_code"), rec.get("section"))
        if not all(key):
            keepers.append(rec)
            continue
        prev = best.get(key)
        if prev is None or _record_quality(rec) > _record_quality(prev):
            best[key] = rec

    final = list(best.values()) + keepers

    quality_counts: dict[str, int] = {"valid": 0, "not_found": 0, "error": 0, "other": 0}
    for rec in final:
        q = _record_quality(rec)
        if q == 4:
            quality_counts["valid"] += 1
        elif q == 3:
            quality_counts["not_found"] += 1
        elif q == 0:
            quality_counts["error"] += 1
        else:
            quality_counts["other"] += 1

    # Sort for stable output: by law_code then section sort key
    final.sort(key=lambda r: (
        r.get("law_code") or "",
        _section_sort_key(r.get("section") or ""),
    ))

    backup = manifest_path.with_suffix(manifest_path.suffix + ".bak")
    manifest_path.replace(backup)
    with manifest_path.open("w") as f:
        for rec in final:
            f.write(json.dumps(rec) + "\n")

    return before, len(final), quality_counts


def load_known_missing(manifest_path: Path, law_code: str) -> set[str]:
    """Read the manifest and return sections previously confirmed not-found.

    A section is "known missing" if a prior fetch returned a valid 200 response
    but the body indicated no statute (op_statues = '' / regex didn't match).
    Sections with errors / 403 / 5xx are NOT considered known missing — those
    should be retried.
    """
    if not manifest_path.exists():
        return set()
    missing: set[str] = set()
    with manifest_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("law_code") != law_code:
                continue
            if rec.get("error"):
                continue
            if rec.get("http_status") == 200 and not rec.get("valid"):
                missing.add(rec["section"])
    return missing


def _setup_logging(log_path: Optional[Path], verbose: bool) -> None:
    """Configure root logger for the fetcher: stdout + optional file."""
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    # Clear any existing handlers (re-runs in same process).
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

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, mode="a")
        fh.setLevel(logging.DEBUG)  # file always gets debug
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.info("file-log path=%s", log_path)


async def _heartbeat(stats: dict, total: int, every_s: float = 5.0) -> None:
    """Periodically print rolling progress until cancelled."""
    last_completed = 0
    last_t = asyncio.get_event_loop().time()
    while True:
        await asyncio.sleep(every_s)
        now = asyncio.get_event_loop().time()
        delta = stats["completed"] - last_completed
        rate = delta / max(now - last_t, 0.001)
        remaining = total - stats["completed"]
        eta = remaining / rate if rate > 0 else float("inf")
        eta_str = f"{eta/60:.1f}min" if eta != float("inf") else "n/a"
        logger.info(
            "progress %d/%d  valid=%d  not_found=%d  retried=%d  errors=%d  "
            "rate=%.1f/s eta=%s  %.1f MB",
            stats["completed"], total,
            stats["valid"], stats["not_found"],
            stats["retried"], stats["errors"],
            rate, eta_str, stats["bytes"] / (1 << 20),
        )
        last_completed = stats["completed"]
        last_t = now


async def run(
    sections: Iterable[str],
    law_code: str = DEFAULT_LAW_CODE,
    *,
    out_dir: Path = DEFAULT_OUT_DIR,
    concurrency: int = DEFAULT_CONCURRENCY,
    force: bool = False,
    skip_known_missing: bool = True,
    log_path: Optional[Path] = None,
    verbose: bool = False,
    heartbeat_every_s: float = 5.0,
    request_interval_s: float = 0.0,
) -> list[FetchRecord]:
    sections = list(sections)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"

    _setup_logging(log_path, verbose)

    # Skip sections we've already confirmed don't exist (unless --force).
    if skip_known_missing and not force:
        missing = load_known_missing(manifest_path, law_code)
        if missing:
            before = len(sections)
            sections = [s for s in sections if s not in missing]
            logger.info(
                "skip-known-missing dropped=%d remaining=%d",
                before - len(sections), len(sections),
            )

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,*/*;q=0.8"}
    timeout = httpx.Timeout(DEFAULT_TIMEOUT_S, connect=15.0)

    logger.info(
        "start    code=%s sections=%d concurrency=%d out=%s",
        law_code, len(sections), concurrency, out_dir,
    )

    stats = {
        "completed": 0,
        "valid": 0,
        "not_found": 0,
        "retried": 0,
        "errors": 0,
        "bytes": 0,
    }
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(concurrency)
    rate_lock = asyncio.Lock()
    next_request_t: list[float] = [0.0]  # mutable closure cell

    async def _rate_limit():
        """Enforce a global minimum interval between request starts."""
        if request_interval_s <= 0:
            return
        async with rate_lock:
            now = asyncio.get_event_loop().time()
            wait = next_request_t[0] - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = asyncio.get_event_loop().time()
            next_request_t[0] = max(now, next_request_t[0]) + request_interval_s

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
            async with sem:
                await _rate_limit()
                rec = await fetch_section(
                    client, section, law_code, out_dir, force=force
                )
            async with lock:
                stats["completed"] += 1
                stats["bytes"] += rec.bytes
                if rec.valid:
                    stats["valid"] += 1
                elif rec.error:
                    stats["errors"] += 1
                elif rec.http_status == 200:
                    stats["not_found"] += 1
                else:
                    stats["retried"] += 1
            return rec

        # Heartbeat task for periodic progress
        hb_task = asyncio.create_task(
            _heartbeat(stats, len(sections), every_s=heartbeat_every_s)
        )

        try:
            records: list[FetchRecord] = await asyncio.gather(
                *[_bounded(s) for s in sections]
            )
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

    # Append to manifest. Dedupe afterwards for callers who want compaction.
    with manifest_path.open("a") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec)) + "\n")

    failed = [r for r in records if r.error]
    not_found = [r for r in records if not r.valid and not r.error and r.http_status == 200]
    valid = [r for r in records if r.valid]

    logger.info(
        "done     valid=%d not_found=%d errors=%d total_bytes=%.1fMB",
        len(valid), len(not_found), len(failed), stats["bytes"] / (1 << 20),
    )
    if failed:
        for r in failed[:10]:
            logger.warning("failed   section=%s status=%d err=%s",
                           r.section, r.http_status, r.error)
        if len(failed) > 10:
            logger.warning("... and %d more failures", len(failed) - 10)
    logger.info("manifest %s", manifest_path)
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
    p.add_argument(
        "--no-skip-missing",
        action="store_true",
        help="Re-attempt sections previously confirmed not-found.",
    )
    p.add_argument(
        "--dedupe",
        action="store_true",
        help="Compact the manifest (keep best record per section) and exit.",
    )
    p.add_argument(
        "--log-file",
        type=Path,
        default=Path("data/logs/ca_leginfo_pages.log"),
        help="File to append detailed log lines to (default: %(default)s)",
    )
    p.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable file logging.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Log DEBUG-level messages to stdout.",
    )
    p.add_argument(
        "--heartbeat-every",
        type=float,
        default=5.0,
        help="Seconds between progress heartbeat lines (default: %(default)s)",
    )
    p.add_argument(
        "--rate-interval",
        type=float,
        default=0.5,
        help="Minimum seconds between request starts (global, across all workers). "
        "Default %(default)s. Set 0 to disable.",
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
    out_dir: Path = args["out"]
    manifest_path = out_dir / "manifest.jsonl"

    if args["dedupe"]:
        before, after, qc = dedupe_manifest(manifest_path)
        print(f"[dedupe] before={before} after={after}")
        print(f"[dedupe] quality: valid={qc.get('valid', 0)}  "
              f"not_found={qc.get('not_found', 0)}  "
              f"error={qc.get('error', 0)}  other={qc.get('other', 0)}")
        backup = manifest_path.with_suffix(manifest_path.suffix + ".bak")
        print(f"[dedupe] backup at {backup}")
        return 0

    sections = _build_section_list(args)
    if args["limit"] is not None:
        sections = sections[: args["limit"]]
    if not sections:
        print("No sections to fetch.")
        return 1

    log_path = None if args["no_log_file"] else args["log_file"]

    await run(
        sections,
        law_code=args["code"],
        out_dir=out_dir,
        concurrency=args["concurrency"],
        force=args["force"],
        skip_known_missing=not args["no_skip_missing"],
        log_path=log_path,
        verbose=args["verbose"],
        heartbeat_every_s=args["heartbeat_every"],
        request_interval_s=args["rate_interval"],
    )
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
