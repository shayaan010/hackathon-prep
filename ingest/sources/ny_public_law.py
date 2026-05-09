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
        manifest.jsonl              # one line per fetch attempt (per-record write)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Optional

import httpx

from .._http import RateLimiter, default_client, get_with_retry, setup_logging
from ..cli import add_fetcher_args
from ..manifest import ManifestWriter, dedupe, load_known_missing
from .base import FetcherConfig, fetch_one, register_source


SOURCE_SLUG = "ny_public_law"
JURISDICTION = "NY"
DEFAULT_LAW_CODE = "VAT"
BASE = "https://newyork.public.law"
ROOT_URL_TEMPLATE = f"{BASE}/laws/n.y._vehicle_and_traffic_law"

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"

logger = logging.getLogger(SOURCE_SLUG)


# ---------------------------------------------------------------------------
# Source config: URL builder + validity check
# ---------------------------------------------------------------------------


def section_url(section: str) -> str:
    return f"{BASE}/laws/n.y._vehicle_and_traffic_law_section_{section.lower()}"


def title_url(title_number: str) -> str:
    return f"{BASE}/laws/n.y._vehicle_and_traffic_law_title_{title_number}"


def article_url(article: str) -> str:
    return f"{BASE}/laws/n.y._vehicle_and_traffic_law_article_{article.lower()}"


def _safe_filename(section: str) -> str:
    return section.replace("/", "_").upper() + ".html"


def _validity_check(section: str, body: bytes, text: str) -> tuple[bool, dict]:
    """A real public.law section page is >2000 bytes and contains
    ``leaf-statute-body``.  Missing pages are 404 or short fallbacks."""
    if len(body) < 2000:
        return False, {}
    if "leaf-statute-body" not in text:
        return False, {}
    return True, {}


@register_source(SOURCE_SLUG)
def config_for(law_code: str = DEFAULT_LAW_CODE,
               out_root: Path = DEFAULT_OUT_DIR) -> FetcherConfig:
    return FetcherConfig(
        source_slug=SOURCE_SLUG,
        jurisdiction=JURISDICTION,
        law_code=law_code,
        code_name="Vehicle and Traffic Law",
        url_builder=section_url,
        validity_check=_validity_check,
        output_root=out_root,
        section_filename=_safe_filename,
        default_concurrency=2,
        default_rate_interval=0.4,
    )


# ---------------------------------------------------------------------------
# Discovery: TOC walker (root -> titles -> articles -> sections)
# ---------------------------------------------------------------------------


@dataclass
class TitleRef:
    number: str
    title: str
    range_text: str


@dataclass
class ArticleRef:
    title_number: str
    article: str
    article_title: str
    sections: list[str]


_TAG_RE = re.compile(r"<[^>]+>")
_ROOT_TITLE_RE = re.compile(
    r"\b(\d{1,3})\s+([A-Z][^\d]+?)\s+Sections?\s+([\d\-A-Z\u2013\u2014]+)",
)
_ARTICLE_ROW_RE = re.compile(
    r"\b(\d{1,3}(?:[\-\u2011][A-Z])?)\s+([A-Z][^\d]{3,80}?)\s+Sections?\s+(\d[\d\-A-Z\s,]*?)(?=\s+\d{1,3}(?:[\-\u2011][A-Z])?\s+[A-Z]|Stay Connected|Up to date|$)",
    re.IGNORECASE,
)
_SECTION_LINE_RE = re.compile(
    r"(\d{2,4}(?:-[A-Z])?)\s+([A-Z][^\d]{3,200}?)(?=\s+\d{2,4}(?:-[A-Z])?\s+[A-Z]|Stay Connected|Up to date|$)",
)


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


def parse_root(html: str) -> list[TitleRef]:
    text = _text_of(html)
    s = text.find("Titles")
    e = text.find("Stay Connected")
    if s >= 0 and e > s:
        text = text[s:e]
    titles: dict[str, TitleRef] = {}
    for m in _ROOT_TITLE_RE.finditer(text):
        num = m.group(1)
        if num in titles:
            continue
        titles[num] = TitleRef(num, m.group(2).strip(), m.group(3).strip())
    return list(titles.values())


def parse_title(html: str, title_number: str) -> list[ArticleRef]:
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
        articles.append(ArticleRef(title_number, art, m.group(2).strip(), []))
    return articles


def parse_article(html: str, title_number: str, article: str) -> ArticleRef:
    text = _text_of(html)
    s = text.find("Sections")
    e = text.find("Stay Connected")
    slice_text = text[s + len("Sections"):e] if (s >= 0 and e > s) else text
    sections: list[str] = []
    seen: set[str] = set()
    for m in _SECTION_LINE_RE.finditer(slice_text):
        sec = m.group(1)
        if sec in seen:
            continue
        seen.add(sec)
        sections.append(sec)
    return ArticleRef(title_number, article, "", sections)


async def discover_sections(
    client: httpx.AsyncClient,
    rate: RateLimiter,
    out_dir: Path,
) -> list[ArticleRef]:
    toc_dir = out_dir / "toc"
    toc_dir.mkdir(parents=True, exist_ok=True)

    logger.info("toc      GET root")
    r = await get_with_retry(client, ROOT_URL_TEMPLATE, rate=rate, label="ny-root")
    r.raise_for_status()
    (toc_dir / "root.html").write_text(r.text)
    titles = parse_root(r.text)
    logger.info("toc      discovered %d titles", len(titles))

    title_articles: dict[str, list[ArticleRef]] = {}

    async def _fetch_title(t: TitleRef) -> None:
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

    all_articles: list[ArticleRef] = []
    for tnum, articles in title_articles.items():
        all_articles.extend(articles)

    async def _fetch_article(ar: ArticleRef) -> None:
        url = article_url(ar.article)
        resp = await get_with_retry(client, url, rate=rate, label=f"ny-art-{ar.article}")
        if resp.status_code >= 400:
            logger.warning("toc      article %s failed status=%d", ar.article, resp.status_code)
            return
        (toc_dir / f"article_{ar.article}.html").write_text(resp.text)
        parsed = parse_article(resp.text, ar.title_number, ar.article)
        ar.sections = parsed.sections

    await asyncio.gather(*[_fetch_article(a) for a in all_articles])

    total = sum(len(a.sections) for a in all_articles)
    logger.info(
        "toc      total: %d titles, %d articles, %d sections",
        len(titles), len(all_articles), total,
    )
    return all_articles


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


async def run(
    *,
    law_code: str = DEFAULT_LAW_CODE,
    out_dir: Path = DEFAULT_OUT_DIR,
    concurrency: int = 2,
    rate_interval: float = 0.4,
    force: bool = False,
    sections_override: Optional[list[str]] = None,
    skip_known_missing: bool = True,
    heartbeat_every: float = 5.0,
    limit: Optional[int] = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"
    toc_index_path = out_dir / f"{law_code}_toc.jsonl"

    config = config_for(law_code=law_code, out_root=out_dir)
    rate = RateLimiter(rate_interval) if rate_interval > 0 else None

    async with default_client(concurrency=concurrency) as client:
        # ---- Discovery ----
        if sections_override is None:
            articles = await discover_sections(client, rate, out_dir)
            seen: set[str] = set()
            sections: list[str] = []
            article_by_section: dict[str, tuple[Optional[str], Optional[str]]] = {}
            for ar in articles:
                for sec in ar.sections:
                    if sec in seen:
                        continue
                    seen.add(sec)
                    sections.append(sec)
                    article_by_section[sec] = (ar.title_number, ar.article)
            with toc_index_path.open("w") as f:
                for sec in sections:
                    tnum, art = article_by_section[sec]
                    f.write(json.dumps({
                        "code": law_code,
                        "section": sec,
                        "title_number": tnum,
                        "article": art,
                        "section_url": section_url(sec),
                    }) + "\n")
            logger.info("toc      wrote %s (%d sections)", toc_index_path, len(sections))
        else:
            sections = sections_override
            article_by_section = {}

        if skip_known_missing and not force:
            missing = load_known_missing(
                manifest_path,
                jurisdiction=JURISDICTION,
                law_code=law_code,
            )
            if missing:
                before = len(sections)
                sections = [s for s in sections if s not in missing]
                logger.info(
                    "skip-known-missing dropped=%d remaining=%d",
                    before - len(sections), len(sections),
                )

        if limit is not None:
            sections = sections[:limit]

        logger.info(
            "start    code=%s sections=%d concurrency=%d out=%s",
            law_code, len(sections), concurrency, out_dir,
        )

        # ---- Fetch ----
        from .base import run_fetcher
        with ManifestWriter(manifest_path) as mw:
            stats = await run_fetcher(
                config, sections,
                force=force,
                concurrency=concurrency,
                rate_interval=rate_interval,
                manifest=mw,
                heartbeat_every=heartbeat_every,
                log=logger,
            )

    # Annotate manifest with article/title — re-read and merge
    # (only useful for sections discovered via TOC, otherwise harmless)
    if article_by_section:
        _annotate_manifest_with_articles(manifest_path, article_by_section)

    summary = {
        "code": law_code,
        "total": len(sections),
        **stats,
    }
    logger.info("done     %s", summary)
    logger.info("manifest %s", manifest_path)
    return summary


def _annotate_manifest_with_articles(
    manifest_path: Path,
    article_by_section: dict[str, tuple[Optional[str], Optional[str]]],
) -> None:
    """Best-effort: merge title_number/article into the latest record per section
    in the manifest.  Ensures the TOC info isn't lost.
    """
    if not manifest_path.exists():
        return
    lines = manifest_path.read_text().splitlines()
    out_lines: list[str] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            out_lines.append(line)
            continue
        sec = rec.get("section")
        if sec in article_by_section:
            tnum, art = article_by_section[sec]
            extra = rec.get("extra") or {}
            extra.setdefault("title_number", tnum)
            extra.setdefault("article", art)
            rec["extra"] = extra
        out_lines.append(json.dumps(rec, ensure_ascii=False, default=str))
    manifest_path.write_text("\n".join(out_lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="NY Vehicle & Traffic Law fetcher (newyork.public.law).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_fetcher_args(
        p,
        default_code=DEFAULT_LAW_CODE,
        default_out_dir=DEFAULT_OUT_DIR,
        default_log_file=DEFAULT_LOG_FILE,
        default_concurrency=2,
        default_rate_interval=0.4,
        supports_sections_file=True,
        supports_skip_known_missing=True,
        supports_dedupe=True,
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])

    log_path = None if args.no_log_file else args.log_file
    setup_logging(SOURCE_SLUG, verbose=args.verbose, log_path=log_path)

    manifest_path = args.out_dir / "manifest.jsonl"

    if args.dedupe:
        before, after, qc = dedupe(
            manifest_path,
            key_fields=("law_code", "section"),
        )
        print(f"[dedupe] before={before} after={after}")
        print(f"[dedupe] quality: valid={qc.get('valid', 0)}  "
              f"not_found={qc.get('not_found', 0)}  "
              f"error={qc.get('error', 0)}  other={qc.get('other', 0)}")
        return 0

    sections_override: Optional[list[str]] = None
    if args.sections:
        sections_override = list(args.sections)
    elif getattr(args, "sections_file", None):
        path: Path = args.sections_file
        if not path.exists():
            raise SystemExit(f"sections file not found at {path}")
        with path.open() as f:
            sections_override = [
                ln.strip() for ln in f if ln.strip() and not ln.startswith("#")
            ]

    asyncio.run(run(
        law_code=args.code,
        out_dir=args.out_dir,
        concurrency=args.concurrency,
        rate_interval=args.rate_interval,
        force=args.force,
        sections_override=sections_override,
        skip_known_missing=not args.no_skip_missing,
        heartbeat_every=args.heartbeat_every,
        limit=args.limit,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
