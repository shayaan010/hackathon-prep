"""
New York Vehicle & Traffic Law fetcher (via newyork.public.law).

Why public.law:
- Official NY sources (nysenate.gov, justia, findlaw) are behind Cloudflare and
  return 403 to bot scrapers without a JS challenge solver.
- public.law is a non-profit mirror that returns clean HTML and respects scrapers.
- They cite their original source on each page (we capture this as provenance).

Discovery strategy:
1. Root index:   https://newyork.public.law/laws/n.y._vehicle_and_traffic_law
                  -> 12 Titles with text section ranges.
2. Title page:   https://newyork.public.law/laws/n.y._vehicle_and_traffic_law_title_<N>
                  -> list of Articles, each with section numbers (e.g. "1146-A").
3. Section page: https://newyork.public.law/laws/n.y._vehicle_and_traffic_law_section_<NUM>
                  -> single statute, HTTP 200 if exists, HTTP 404 if missing.

NY uses suffix-letter sections like "1146-A", "499-D", "1182-B" — we preserve them
as-is in canonical form. URLs are case-insensitive but we lowercase to dedupe.

Output:
    data/raw/ny_public_law/
        VAT/<section>.html          # one file per valid section
        toc/                        # cached title + article pages (debug/audit)
        VAT_toc.jsonl               # master section index (one record per section)
        manifest.jsonl              # one line per fetch attempt
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterable, Optional

import httpx

from ingest._http import (
    RateLimiter,
    default_client,
    get_with_retry,
    setup_logging,
)


SOURCE_SLUG = "ny_public_law"
JURISDICTION = "NY"
LAW_CODE = "VAT"
BASE = "https://newyork.public.law"
ROOT_URL = f"{BASE}/laws/n.y._vehicle_and_traffic_law"

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_CONCURRENCY = 2
DEFAULT_RATE_INTERVAL = 0.4  # public.law throttles when bursty; stay polite

logger = logging.getLogger("ny_public_law")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class TitleRef:
    number: str            # "7"
    title: str             # "Rules of the Road"
    range_text: str        # "1100–1289"


@dataclass
class ArticleRef:
    title_number: str      # parent title, e.g. "7"
    article: str           # "26"
    article_title: str     # "Right of Way"
    sections: list[str]    # ["1140", "1141", ..., "1146-A"]


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
    title_number: Optional[str] = None
    article: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")


def _text_of(html: str) -> str:
    """Strip scripts/styles/tags, decode entities, collapse whitespace."""
    body = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL)
    inner = body.group(1) if body else html
    inner = re.sub(r"<script.*?</script>", " ", inner, flags=re.DOTALL)
    inner = re.sub(r"<style.*?</style>", " ", inner, flags=re.DOTALL)
    text = _TAG_RE.sub(" ", inner)
    # Normalise unicode hyphen variants to ASCII hyphen
    text = text.replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# Root TOC: ".. Titles 1 Words ... Sections 100-161 2 Department ... Sections 200-263 ..."
# Title text can contain commas ("Snowmobiles, Motorboats and Limited Use Vehicles")
# and apostrophes ("Drivers' Licenses"), but no digits.
_ROOT_TITLE_RE = re.compile(
    r"\b(\d{1,3})\s+([A-Z][^\d]+?)\s+Sections?\s+([\d\-A-Z\u2013\u2014]+)",
)

# Article entry on title page: "23 Obedience to and Effect of Traffic Laws Sections 1100\u20131105"
_ARTICLE_ROW_RE = re.compile(
    r"\b(\d{1,3}(?:[\-\u2011][A-Z])?)\s+([A-Z][^\d]{3,80}?)\s+Sections?\s+(\d[\d\-A-Z\s,]*?)(?=\s+\d{1,3}(?:[\-\u2011][A-Z])?\s+[A-Z]|Stay Connected|Up to date|$)",
    re.IGNORECASE,
)

# Section line on article page: "1140 Vehicle approaching or entering intersection"
# - Section ID: digits with optional "-A" suffix
# - Followed by space + title (capitalized words)
_SECTION_LINE_RE = re.compile(
    r"(\d{2,4}(?:-[A-Z])?)\s+([A-Z][^\d]{3,200}?)(?=\s+\d{2,4}(?:-[A-Z])?\s+[A-Z]|Stay Connected|Up to date|$)",
)


def parse_root(html: str) -> list[TitleRef]:
    """Extract Title list from root index page."""
    text = _text_of(html)
    # Limit to the slice between "Titles" header and "Stay Connected"
    s = text.find("Titles")
    e = text.find("Stay Connected")
    if s >= 0 and e > s:
        text = text[s:e]
    titles: dict[str, TitleRef] = {}
    for m in _ROOT_TITLE_RE.finditer(text):
        num = m.group(1)
        if num in titles:
            continue
        titles[num] = TitleRef(
            number=num,
            title=m.group(2).strip(),
            range_text=m.group(3).strip(),
        )
    return list(titles.values())


def parse_title(html: str, title_number: str) -> list[ArticleRef]:
    """Extract Article entries (with their displayed section ranges) from a title page.

    Article ranges contain start–end like '1140–1146-A'. We expand to integer-aware
    candidates AND probe the suffix variants directly.
    """
    text = _text_of(html)
    s = text.find("Articles")
    e = text.find("Stay Connected")
    if s >= 0 and e > s:
        text = text[s:e]
    articles: list[ArticleRef] = []
    seen: set[str] = set()
    for m in _ARTICLE_ROW_RE.finditer(text):
        art = m.group(1).strip()
        if art in seen:
            continue
        seen.add(art)
        sections_text = m.group(3).strip()
        articles.append(ArticleRef(
            title_number=title_number,
            article=art,
            article_title=m.group(2).strip(),
            sections=[],  # filled later by article-page walk
        ))
    return articles


def parse_article(html: str, title_number: str, article: str) -> ArticleRef:
    """Extract exact section numbers (with suffixes) from an article page."""
    text = _text_of(html)
    # Slice to the section list area
    # Pattern: "Article N Title Sections 1140 ... 1146-A Approaching horses Stay Connected"
    s = text.find("Sections")
    e = text.find("Stay Connected")
    if s >= 0 and e > s:
        slice_text = text[s + len("Sections"):e]
    else:
        slice_text = text
    # Capture <number(-letter)?> followed by title
    sections: list[str] = []
    seen: set[str] = set()
    for m in _SECTION_LINE_RE.finditer(slice_text):
        sec = m.group(1)
        if sec in seen:
            continue
        seen.add(sec)
        sections.append(sec)
    return ArticleRef(
        title_number=title_number,
        article=article,
        article_title="",   # filled by caller
        sections=sections,
    )


def expand_range_text(range_text: str) -> list[str]:
    """Fallback: expand a range string like '1100-1289' to a list of int sections.

    Handles suffix-letter ranges crudely by including both the integer and letter-suffixed
    candidates from a small fixed set [A..F]. Used only if article-page walk yields nothing.
    """
    range_text = range_text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2011", "-")
    m = re.match(r"\s*(\d+)(?:-([A-Z]))?\s*-\s*(\d+)(?:-([A-Z]))?\s*", range_text)
    if not m:
        return []
    lo = int(m.group(1))
    hi = int(m.group(3))
    out: list[str] = []
    for n in range(lo, hi + 1):
        out.append(str(n))
        for letter in "ABCDEFGHIJKLMN":
            out.append(f"{n}-{letter}")
    return out


# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------


def section_url(section: str) -> str:
    return f"{BASE}/laws/n.y._vehicle_and_traffic_law_section_{section.lower()}"


def title_url(title_number: str) -> str:
    return f"{BASE}/laws/n.y._vehicle_and_traffic_law_title_{title_number}"


def article_url(article: str) -> str:
    return f"{BASE}/laws/n.y._vehicle_and_traffic_law_article_{article.lower()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_filename(section: str) -> str:
    return section.replace("/", "_").upper() + ".html"


# ---------------------------------------------------------------------------
# Walker
# ---------------------------------------------------------------------------


async def discover_sections(
    client: httpx.AsyncClient,
    rate: RateLimiter,
    out_dir: Path,
) -> list[ArticleRef]:
    """Walk root -> titles -> articles. Return all (article, sections) records.

    Pages are cached to {out_dir}/toc/ for audit/replay.
    """
    toc_dir = out_dir / "toc"
    toc_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: root
    logger.info("toc      GET root")
    r = await get_with_retry(client, ROOT_URL, rate=rate, label="ny-root")
    r.raise_for_status()
    (toc_dir / "root.html").write_text(r.text)
    titles = parse_root(r.text)
    logger.info("toc      discovered %d titles", len(titles))
    for t in titles:
        logger.info("  title %-3s %s [%s]", t.number, t.title, t.range_text)

    # Step 2: each title
    title_articles: dict[str, list[ArticleRef]] = {}

    async def _fetch_title(t: TitleRef):
        url = title_url(t.number)
        resp = await get_with_retry(client, url, rate=rate, label=f"ny-title-{t.number}")
        if resp.status_code >= 400:
            logger.warning("toc      title %s failed status=%d", t.number, resp.status_code)
            title_articles[t.number] = []
            return
        (toc_dir / f"title_{t.number}.html").write_text(resp.text)
        articles = parse_title(resp.text, t.number)
        logger.info("toc      title %-3s -> %d articles", t.number, len(articles))
        title_articles[t.number] = articles

    await asyncio.gather(*[_fetch_title(t) for t in titles])

    # Flatten + tag with title_number
    all_articles: list[ArticleRef] = []
    for tnum, articles in title_articles.items():
        all_articles.extend(articles)

    # Step 3: each article
    async def _fetch_article(ar: ArticleRef):
        url = article_url(ar.article)
        resp = await get_with_retry(client, url, rate=rate, label=f"ny-art-{ar.article}")
        if resp.status_code >= 400:
            logger.warning("toc      article %s failed status=%d", ar.article, resp.status_code)
            return
        (toc_dir / f"article_{ar.article}.html").write_text(resp.text)
        parsed = parse_article(resp.text, ar.title_number, ar.article)
        ar.sections = parsed.sections

    await asyncio.gather(*[_fetch_article(a) for a in all_articles])

    total_sections = sum(len(a.sections) for a in all_articles)
    logger.info(
        "toc      total: %d titles, %d articles, %d sections (raw, before fetch)",
        len(titles), len(all_articles), total_sections,
    )
    return all_articles


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
    article: Optional[str],
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
            source_slug=SOURCE_SLUG,
            jurisdiction=JURISDICTION,
            law_code=LAW_CODE,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=200,
            bytes=dest.stat().st_size,
            content_sha256=hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest(),
            raw_path=str(dest),
            valid=True,
            title_number=title_number,
            article=article,
        )

    resp = await get_with_retry(client, url, rate=rate, label=f"ny-sec-{section}")
    text = resp.text
    if resp.status_code == 200 and len(text) > 2000:
        dest.write_text(text)
        return FetchRecord(
            source_slug=SOURCE_SLUG,
            jurisdiction=JURISDICTION,
            law_code=LAW_CODE,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=200,
            bytes=len(resp.content),
            content_sha256=hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest(),
            raw_path=str(dest),
            valid=True,
            title_number=title_number,
            article=article,
        )
    # 404 or short body -> not found
    return FetchRecord(
        source_slug=SOURCE_SLUG,
        jurisdiction=JURISDICTION,
        law_code=LAW_CODE,
        section=section,
        url=url,
        fetched_at=_now_iso(),
        http_status=resp.status_code,
        bytes=len(resp.content),
        content_sha256="",
        raw_path=None,
        valid=False,
        title_number=title_number,
        article=article,
        error=None if resp.status_code == 404 else f"HTTP {resp.status_code}",
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
            articles = await discover_sections(client, rate, out_dir)
            # Build (section, title_number, article) tuples; dedupe sections
            seen: set[str] = set()
            tasks: list[tuple[str, Optional[str], Optional[str]]] = []
            for ar in articles:
                for sec in ar.sections:
                    if sec in seen:
                        continue
                    seen.add(sec)
                    tasks.append((sec, ar.title_number, ar.article))

            # Persist TOC index
            with toc_index_path.open("w") as f:
                for sec, tnum, art in tasks:
                    f.write(json.dumps({
                        "code": LAW_CODE,
                        "section": sec,
                        "title_number": tnum,
                        "article": art,
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

        async def _worker(sec: str, tnum: Optional[str], art: Optional[str]):
            async with sem:
                try:
                    rec = await fetch_section(
                        client, rate, sec, out_dir,
                        title_number=tnum, article=art, force=force,
                    )
                except Exception as e:
                    rec = FetchRecord(
                        source_slug=SOURCE_SLUG, jurisdiction=JURISDICTION,
                        law_code=LAW_CODE, section=sec, url=section_url(sec),
                        fetched_at=_now_iso(), http_status=0, bytes=0,
                        content_sha256="", raw_path=None, valid=False,
                        title_number=tnum, article=art, error=f"{type(e).__name__}: {e}",
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

        await asyncio.gather(*[_worker(s, t, a) for s, t, a in tasks])

    # Append all records to manifest
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
    p.add_argument("--log-file", type=Path, default=Path("data/logs/ny_public_law.log"))
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
