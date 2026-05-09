"""
Texas Transportation Code fetcher (via texas.public.law).

Why public.law:
- statutes.capitol.texas.gov is an Angular SPA that renders content client-side;
  scraping it would require a headless browser.
- Justia/FindLaw are Cloudflare-protected with JS challenges.
- public.law serves clean static HTML, mirrors the official statutes, and cites
  its source on each page.

Discovery strategy:
1. Root index:    https://texas.public.law/statutes/tex._transp._code
                  -> 7 Titles, each with a Chapters range (e.g. "Chapters 501-1006").
2. Title page:    https://texas.public.law/statutes/tex._transp._code_title_<N>
                  -> list of Chapters.
3. Chapter page:  https://texas.public.law/statutes/tex._transp._code_chapter_<N>
                  -> list of Sections (decimal: e.g. "545.151"). All section URLs
                     are linked directly via <a href="...section_545.151">.
4. Section page:  https://texas.public.law/statutes/tex._transp._code_section_<X.Y>
                  -> single statute, HTTP 200 if exists, redirect/404 if not.

Output:
    data/raw/tx_public_law/
        TN/<chapter>.<section>.html  # one file per valid section
        toc/                         # cached title + chapter pages (debug/audit)
        TN_toc.jsonl                 # master section index
        manifest.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import httpx

from ingest._http import (
    RateLimiter,
    default_client,
    get_with_retry,
    setup_logging,
)


SOURCE_SLUG = "tx_public_law"
JURISDICTION = "TX"
LAW_CODE = "TN"   # Texas Transportation code abbreviation
BASE = "https://texas.public.law"
ROOT_URL = f"{BASE}/statutes/tex._transp._code"

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_CONCURRENCY = 2
DEFAULT_RATE_INTERVAL = 0.4

logger = logging.getLogger("tx_public_law")

_TAG_RE = re.compile(r"<[^>]+>")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class TxTitle:
    number: str        # "7"
    title: str         # "Vehicles and Traffic"
    chapter_range: str # "501-1006" or "Chapter 1"


@dataclass
class TxChapter:
    title_number: str  # "7"
    number: str        # "545"
    title: str         # "Operation and Movement of Vehicles"
    sections: list[str]   # ["545.001", "545.002", ...]


@dataclass
class FetchRecord:
    source_slug: str
    jurisdiction: str
    law_code: str
    section: str          # e.g. "545.151"
    url: str
    fetched_at: str
    http_status: int
    bytes: int
    content_sha256: str
    raw_path: Optional[str]
    valid: bool
    title_number: Optional[str] = None
    chapter: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _text_of(html: str) -> str:
    body = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL)
    inner = body.group(1) if body else html
    inner = re.sub(r"<script.*?</script>", " ", inner, flags=re.DOTALL)
    inner = re.sub(r"<style.*?</style>", " ", inner, flags=re.DOTALL)
    text = _TAG_RE.sub(" ", inner)
    text = text.replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# Root TOC: "Titles 1 General Provisions Chapter 1 2 General Provisions Relating to Carriers Chapters 5–20 ..."
_ROOT_TITLE_RE = re.compile(
    r"\b(\d{1,2})\s+([A-Z][^\d]{3,80}?)\s+Chapters?\s+([\d\-]+)",
)

# Chapter links appear in two URL forms:
#   tex._transp._code_chapter_545                                (canonical, used in our fetcher)
#   tex._transp._code_title_7_subtitle_c_chapter_545             (long, found on subtitle pages)
# We extract the chapter number from either form.
_CHAPTER_LINK_RE = re.compile(
    r"tex\._transp\._code(?:_title_\d+(?:_subtitle_[a-z])?)?_chapter_(\d+[a-z]?)",
)
_SUBTITLE_LINK_RE = re.compile(
    r"tex\._transp\._code_title_(\d+)_subtitle_([a-z])",
)

# Chapter page lists sections via section URL pattern:
#   /statutes/tex._transp._code_section_545.151
_SECTION_LINK_RE = re.compile(
    r"tex\._transp\._code_section_(\d+\.\d+(?:[a-z])?)",
)

# Chapter title is on its own page near the top: "Chapter 545 Operation and Movement of Vehicles"
_CHAPTER_TITLE_RE = re.compile(
    r"Chapter\s+\d+[a-z]?\s+(.+?)(?:\s+Statute|\s+Sections?|\s+Subchapter|\s+\d+\.)",
)


def parse_root(html: str) -> list[TxTitle]:
    text = _text_of(html)
    s = text.find("Titles")
    e = text.find("Stay Connected")
    if s >= 0 and e > s:
        text = text[s:e]
    titles: dict[str, TxTitle] = {}
    for m in _ROOT_TITLE_RE.finditer(text):
        num = m.group(1)
        if num in titles:
            continue
        titles[num] = TxTitle(
            number=num,
            title=m.group(2).strip(),
            chapter_range=m.group(3).strip(),
        )
    # Also handle "Chapter 1" (singular) for Title 1
    # already in our text; the regex matches Chapters/Chapter
    return list(titles.values())


def parse_title(html: str, title_number: str) -> tuple[list[str], list[str]]:
    """Return (chapters, subtitles) found on a title page.

    Some titles link directly to chapters, others link to subtitles which then
    link to chapters. Caller walks subtitles for chapter discovery if needed.
    """
    chapters: list[str] = []
    seen_ch: set[str] = set()
    for m in _CHAPTER_LINK_RE.finditer(html):
        ch = m.group(1)
        if ch in seen_ch:
            continue
        seen_ch.add(ch)
        chapters.append(ch)

    subtitles: list[str] = []
    seen_st: set[str] = set()
    for m in _SUBTITLE_LINK_RE.finditer(html):
        if m.group(1) != title_number:
            continue
        st = m.group(2)
        if st in seen_st:
            continue
        seen_st.add(st)
        subtitles.append(st)
    return chapters, subtitles


def parse_subtitle(html: str) -> list[str]:
    """Return chapter numbers linked from a subtitle page."""
    chapters: list[str] = []
    seen: set[str] = set()
    for m in _CHAPTER_LINK_RE.finditer(html):
        ch = m.group(1)
        if ch in seen:
            continue
        seen.add(ch)
        chapters.append(ch)
    return chapters


def subtitle_url(title_number: str, subtitle: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_title_{title_number}_subtitle_{subtitle}"


def parse_chapter(html: str) -> list[str]:
    """Return all section numbers (e.g. '545.151') linked from a chapter page."""
    sections: list[str] = []
    seen: set[str] = set()
    for m in _SECTION_LINK_RE.finditer(html):
        sec = m.group(1)
        if sec in seen:
            continue
        seen.add(sec)
        sections.append(sec)
    return sorted(sections, key=_section_sort_key)


def _section_sort_key(s: str) -> tuple:
    m = re.match(r"(\d+)\.(\d+)", s)
    if not m:
        return (10**9, 10**9, s)
    return (int(m.group(1)), int(m.group(2)), s)


# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------


def section_url(section: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_section_{section}"


def chapter_url(chapter: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_chapter_{chapter}"


def title_url(title_number: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_title_{title_number}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_filename(section: str) -> str:
    return section + ".html"


# ---------------------------------------------------------------------------
# Walker
# ---------------------------------------------------------------------------


async def discover_sections(
    client: httpx.AsyncClient,
    rate: RateLimiter,
    out_dir: Path,
) -> list[TxChapter]:
    toc_dir = out_dir / "toc"
    toc_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: root
    logger.info("toc      GET root")
    r = await get_with_retry(client, ROOT_URL, rate=rate, label="tx-root")
    r.raise_for_status()
    (toc_dir / "root.html").write_text(r.text)
    titles = parse_root(r.text)
    logger.info("toc      discovered %d titles", len(titles))
    for t in titles:
        logger.info("  title %-3s %s [Chapters %s]", t.number, t.title, t.chapter_range)

    # Step 2: each title -> chapters (directly or via subtitles)
    title_chapters: dict[str, list[str]] = {}
    title_subtitles: dict[str, list[str]] = {}

    async def _fetch_title(t: TxTitle):
        url = title_url(t.number)
        resp = await get_with_retry(client, url, rate=rate, label=f"tx-title-{t.number}")
        if resp.status_code >= 400:
            logger.warning("toc      title %s failed status=%d", t.number, resp.status_code)
            title_chapters[t.number] = []
            title_subtitles[t.number] = []
            return
        (toc_dir / f"title_{t.number}.html").write_text(resp.text)
        chs, sts = parse_title(resp.text, t.number)
        logger.info(
            "toc      title %-3s -> %d direct chapter links, %d subtitle links",
            t.number, len(chs), len(sts),
        )
        title_chapters[t.number] = chs
        title_subtitles[t.number] = sts

    await asyncio.gather(*[_fetch_title(t) for t in titles])

    # Step 2b: walk any subtitles to collect more chapters
    flat_subtitles: list[tuple[str, str]] = []
    for tnum, sts in title_subtitles.items():
        for st in sts:
            flat_subtitles.append((tnum, st))
    if flat_subtitles:
        logger.info("toc      walking %d subtitles for chapter discovery", len(flat_subtitles))

        async def _fetch_subtitle(tnum: str, st: str):
            url = subtitle_url(tnum, st)
            resp = await get_with_retry(client, url, rate=rate, label=f"tx-subt-{tnum}-{st}")
            if resp.status_code >= 400:
                logger.warning("toc      subtitle %s/%s failed status=%d",
                               tnum, st, resp.status_code)
                return
            (toc_dir / f"title_{tnum}_subtitle_{st}.html").write_text(resp.text)
            chs = parse_subtitle(resp.text)
            existing = title_chapters.setdefault(tnum, [])
            for ch in chs:
                if ch not in existing:
                    existing.append(ch)
            logger.info(
                "toc      subtitle %s/%s -> +%d chapter links (running total %d for title %s)",
                tnum, st, len(chs), len(existing), tnum,
            )

        await asyncio.gather(*[_fetch_subtitle(t, s) for t, s in flat_subtitles])

    # Build flat chapter list with parent title
    flat_chapters: list[tuple[str, str]] = []
    for tnum, chs in title_chapters.items():
        for ch in chs:
            flat_chapters.append((tnum, ch))
    logger.info("toc      total chapters across all titles: %d", len(flat_chapters))

    # Step 3: each chapter -> sections
    chapters_out: list[TxChapter] = []
    chap_lock = asyncio.Lock()

    async def _fetch_chapter(tnum: str, ch: str):
        url = chapter_url(ch)
        resp = await get_with_retry(client, url, rate=rate, label=f"tx-chap-{ch}")
        if resp.status_code >= 400:
            logger.warning("toc      chapter %s failed status=%d", ch, resp.status_code)
            return
        (toc_dir / f"chapter_{ch}.html").write_text(resp.text)
        sections = parse_chapter(resp.text)
        # Try to grab chapter title from rendered text
        text = _text_of(resp.text)
        m = _CHAPTER_TITLE_RE.search(text)
        chap_title = m.group(1).strip() if m else ""
        async with chap_lock:
            chapters_out.append(TxChapter(
                title_number=tnum,
                number=ch,
                title=chap_title,
                sections=sections,
            ))
        if len(chapters_out) % 25 == 0:
            logger.info(
                "toc      chapter walk: %d/%d done (last: ch%s -> %d secs)",
                len(chapters_out), len(flat_chapters), ch, len(sections),
            )

    await asyncio.gather(*[_fetch_chapter(t, c) for t, c in flat_chapters])

    total_sections = sum(len(c.sections) for c in chapters_out)
    logger.info(
        "toc      total: %d titles, %d chapters, %d sections (raw, before fetch)",
        len(titles), len(chapters_out), total_sections,
    )
    return chapters_out


# ---------------------------------------------------------------------------
# Section fetcher
# ---------------------------------------------------------------------------


async def fetch_section(
    client: httpx.AsyncClient,
    rate: RateLimiter,
    section: str,
    out_dir: Path,
    *,
    title_number: Optional[str],
    chapter: Optional[str],
    force: bool = False,
) -> FetchRecord:
    code_dir = out_dir / LAW_CODE
    code_dir.mkdir(parents=True, exist_ok=True)
    dest = code_dir / _safe_filename(section)
    url = section_url(section)

    # Cache short-circuit
    if dest.exists() and dest.stat().st_size > 1000 and not force:
        text = dest.read_text(errors="ignore")
        return FetchRecord(
            source_slug=SOURCE_SLUG, jurisdiction=JURISDICTION,
            law_code=LAW_CODE, section=section, url=url,
            fetched_at=_now_iso(), http_status=200,
            bytes=dest.stat().st_size,
            content_sha256=hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest(),
            raw_path=str(dest), valid=True,
            title_number=title_number, chapter=chapter,
        )

    resp = await get_with_retry(client, url, rate=rate, label=f"tx-sec-{section}")
    text = resp.text
    if resp.status_code == 200 and len(text) > 2000:
        dest.write_text(text)
        return FetchRecord(
            source_slug=SOURCE_SLUG, jurisdiction=JURISDICTION,
            law_code=LAW_CODE, section=section, url=url,
            fetched_at=_now_iso(), http_status=200,
            bytes=len(resp.content),
            content_sha256=hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest(),
            raw_path=str(dest), valid=True,
            title_number=title_number, chapter=chapter,
        )
    return FetchRecord(
        source_slug=SOURCE_SLUG, jurisdiction=JURISDICTION,
        law_code=LAW_CODE, section=section, url=url,
        fetched_at=_now_iso(), http_status=resp.status_code,
        bytes=len(resp.content),
        content_sha256="", raw_path=None, valid=False,
        title_number=title_number, chapter=chapter,
        error=None if resp.status_code in (404, 307, 301, 302) else f"HTTP {resp.status_code}",
    )


async def run(
    *,
    out_dir: Path = DEFAULT_OUT_DIR,
    concurrency: int = DEFAULT_CONCURRENCY,
    rate_interval: float = DEFAULT_RATE_INTERVAL,
    force: bool = False,
    sections_override: Optional[list[str]] = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"
    toc_index_path = out_dir / f"{LAW_CODE}_toc.jsonl"

    rate = RateLimiter(rate_interval)

    async with default_client(concurrency=concurrency) as client:
        # ---- Discovery ----
        if sections_override is None:
            chapters = await discover_sections(client, rate, out_dir)
            tasks: list[tuple[str, Optional[str], Optional[str]]] = []
            seen: set[str] = set()
            for ch in chapters:
                for sec in ch.sections:
                    if sec in seen:
                        continue
                    seen.add(sec)
                    tasks.append((sec, ch.title_number, ch.number))

            with toc_index_path.open("w") as f:
                for sec, tnum, ch in tasks:
                    f.write(json.dumps({
                        "code": LAW_CODE,
                        "section": sec,
                        "title_number": tnum,
                        "chapter": ch,
                        "section_url": section_url(sec),
                    }) + "\n")
            logger.info("toc      wrote %s (%d sections)", toc_index_path, len(tasks))
        else:
            tasks = [(s, None, None) for s in sections_override]

        # ---- Fetch ----
        sem = asyncio.Semaphore(concurrency)
        records: list[FetchRecord] = []
        stats = {"completed": 0, "valid": 0, "not_found": 0, "errors": 0, "bytes": 0}
        lock = asyncio.Lock()
        total = len(tasks)

        async def _worker(sec: str, tnum: Optional[str], ch: Optional[str]):
            async with sem:
                try:
                    rec = await fetch_section(
                        client, rate, sec, out_dir,
                        title_number=tnum, chapter=ch, force=force,
                    )
                except Exception as e:
                    rec = FetchRecord(
                        source_slug=SOURCE_SLUG, jurisdiction=JURISDICTION,
                        law_code=LAW_CODE, section=sec, url=section_url(sec),
                        fetched_at=_now_iso(), http_status=0, bytes=0,
                        content_sha256="", raw_path=None, valid=False,
                        title_number=tnum, chapter=ch, error=f"{type(e).__name__}: {e}",
                    )
            async with lock:
                records.append(rec)
                stats["completed"] += 1
                stats["bytes"] += rec.bytes
                if rec.valid:
                    stats["valid"] += 1
                elif rec.error:
                    stats["errors"] += 1
                else:
                    stats["not_found"] += 1
                if stats["completed"] % 50 == 0 or stats["completed"] == total:
                    logger.info(
                        "fetch    %d/%d  valid=%d  not_found=%d  errors=%d  %.1fMB",
                        stats["completed"], total, stats["valid"],
                        stats["not_found"], stats["errors"],
                        stats["bytes"] / (1 << 20),
                    )
            return rec

        await asyncio.gather(*[_worker(s, t, c) for s, t, c in tasks])

    with manifest_path.open("a") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec)) + "\n")

    summary = {
        "code": LAW_CODE,
        "total": total,
        **{k: stats[k] for k in ("valid", "not_found", "errors", "bytes")},
    }
    logger.info("done     %s", summary)
    logger.info("manifest %s", manifest_path)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> dict:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--rate-interval", type=float, default=DEFAULT_RATE_INTERVAL)
    p.add_argument("--force", action="store_true")
    p.add_argument("--sections", nargs="+",
                   help="Skip discovery and just fetch this explicit list of sections.")
    p.add_argument("--log-file", type=Path, default=Path("data/logs/tx_public_law.log"))
    p.add_argument("--no-log-file", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return vars(p.parse_args(argv))


async def _amain(args: dict) -> int:
    log_path = None if args["no_log_file"] else args["log_file"]
    setup_logging(args["verbose"], log_path)
    if log_path:
        logger.info("file-log path=%s", log_path)
    await run(
        out_dir=args["out_dir"],
        concurrency=args["concurrency"],
        rate_interval=args["rate_interval"],
        force=args["force"],
        sections_override=args.get("sections"),
    )
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    return asyncio.run(_amain(_parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
