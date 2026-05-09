"""
Colorado Revised Statutes Title 42 (Vehicles and Traffic) fetcher
(via colorado.public.law).

Why public.law:
- leg.colorado.gov publishes only PDFs of the official statutes.
- public.law re-publishes them as clean per-section HTML.

Discovery strategy (NB: CO uses dash-separated section IDs, e.g. ``42-4-235``,
not dot-separated like FL/OR/NV/TX):

1. Title 42 page:    https://colorado.public.law/statutes/crs_title_42
                     -> Articles 1..21 (named-form URLs).  Each article also
                        has a number; we go straight to the numbered form.
2. Article page:     https://colorado.public.law/statutes/crs_title_42_article_4
                     -> list of Section URLs (``crs_42-4-235``).
3. Section page:     https://colorado.public.law/statutes/crs_42-4-235
                     -> single statute, HTTP 200 if exists.

We hardcode the PI-relevant *article numbers* rather than walking the named-form
layer — public.law's named-form pages just redirect to the numbered form.

Output:
    data/raw/co_public_law/
        CRS42/<title>-<article>-<section>.html
        toc/                  # cached title + article pages
        CRS42_toc.jsonl       # master section index
        manifest.jsonl
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
from .base import FetcherConfig, register_source, run_fetcher


SOURCE_SLUG = "co_public_law"
JURISDICTION = "CO"
DEFAULT_LAW_CODE = "CRS42"
BASE = "https://colorado.public.law"
ROOT_URL = f"{BASE}/statutes/crs_title_42"

# PI-relevant articles within Title 42:
#   2  Drivers' Licenses
#   4  Regulation of Vehicles and Traffic   <- the heart (rules of the road)
#   7  Motor Vehicle Financial Responsibility Law
PI_RELEVANT_ARTICLES = ["2", "4", "7"]

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"

logger = logging.getLogger(SOURCE_SLUG)

_TAG_RE = re.compile(r"<[^>]+>")
# Section IDs: title-article-section, e.g. "42-4-235", "42-4-109.5".
_SECTION_LINK_RE = re.compile(r"crs_(42-\d+-[\w.]+)")


# ---------------------------------------------------------------------------
# Source config
# ---------------------------------------------------------------------------


def section_url(section: str) -> str:
    return f"{BASE}/statutes/crs_{section}"


def article_url(article: str) -> str:
    return f"{BASE}/statutes/crs_title_42_article_{article}"


def title_url() -> str:
    return ROOT_URL


def _safe_filename(section: str) -> str:
    return section + ".html"


def _validity_check(section: str, body: bytes, text: str) -> tuple[bool, dict]:
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
        code_name="Colorado Revised Statutes Title 42",
        url_builder=section_url,
        validity_check=_validity_check,
        output_root=out_root,
        section_filename=_safe_filename,
        default_concurrency=2,
        default_rate_interval=0.4,
    )


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


@dataclass
class CoArticle:
    number: str
    sections: list[str]   # canonical "42-4-235" form


def _normalize_html(html: str) -> str:
    return html.replace("%5F", "_").replace("%5f", "_")


def _text_of(html: str) -> str:
    body = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL)
    inner = body.group(1) if body else html
    inner = re.sub(r"<script.*?</script>", " ", inner, flags=re.DOTALL)
    inner = re.sub(r"<style.*?</style>", " ", inner, flags=re.DOTALL)
    text = _TAG_RE.sub(" ", inner)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_article(html: str, article: str) -> list[str]:
    """Returns canonical section IDs (e.g. '42-4-235') from an article page."""
    html = _normalize_html(html)
    sections: list[str] = []
    seen: set[str] = set()
    prefix = f"42-{article}-"
    for m in _SECTION_LINK_RE.finditer(html):
        sec = m.group(1)
        if not sec.startswith(prefix):
            continue
        if sec not in seen:
            seen.add(sec)
            sections.append(sec)
    return sorted(sections, key=_section_sort_key)


def _section_sort_key(s: str) -> tuple:
    # CO sections look like "42-4-235" or "42-4-109.5"
    m = re.match(r"(\d+)-(\d+)-(\d+)(?:\.(\d+))?", s)
    if not m:
        return (10**9, 10**9, 10**9, 10**9, s)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)),
            int(m.group(4) or 0), s)


async def discover_sections(
    client: httpx.AsyncClient,
    rate: Optional[RateLimiter],
    out_dir: Path,
    *,
    articles_filter: Optional[list[str]] = None,
) -> list[CoArticle]:
    toc_dir = out_dir / "toc"
    toc_dir.mkdir(parents=True, exist_ok=True)

    # We don't really need to fetch the title page — we know the PI-relevant
    # articles by number — but cache it for audit/discovery.
    logger.info("toc      GET title 42")
    r = await get_with_retry(client, ROOT_URL, rate=rate, label="co-title-42")
    r.raise_for_status()
    (toc_dir / "title_42.html").write_text(r.text)

    articles = articles_filter or PI_RELEVANT_ARTICLES
    logger.info("toc      walking articles: %s", ",".join(articles))

    articles_out: list[CoArticle] = []
    art_lock = asyncio.Lock()

    async def _fetch_article(art: str):
        url = article_url(art)
        resp = await get_with_retry(client, url, rate=rate, label=f"co-art-{art}")
        if resp.status_code >= 400:
            logger.warning("toc      article %s failed status=%d", art, resp.status_code)
            return
        (toc_dir / f"article_{art}.html").write_text(resp.text)
        sections = parse_article(resp.text, art)
        async with art_lock:
            articles_out.append(CoArticle(art, sections))
        logger.info("toc      article %s: %d sections", art, len(sections))

    await asyncio.gather(*[_fetch_article(a) for a in articles])

    articles_out.sort(key=lambda a: int(a.number))
    total_sections = sum(len(a.sections) for a in articles_out)
    logger.info(
        "toc      total: %d articles, %d sections",
        len(articles_out), total_sections,
    )
    return articles_out


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
    articles_filter: Optional[list[str]] = None,
    skip_known_missing: bool = True,
    heartbeat_every: float = 5.0,
    limit: Optional[int] = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"
    toc_index_path = out_dir / f"{law_code}_toc.jsonl"

    config = config_for(law_code=law_code, out_root=out_dir)
    rate = RateLimiter(rate_interval) if rate_interval > 0 else None

    article_by_section: dict[str, str] = {}

    async with default_client(concurrency=concurrency) as client:
        if sections_override is None:
            articles = await discover_sections(
                client, rate, out_dir,
                articles_filter=articles_filter or PI_RELEVANT_ARTICLES,
            )
            seen: set[str] = set()
            sections: list[str] = []
            for art in articles:
                for sec in art.sections:
                    if sec in seen:
                        continue
                    seen.add(sec)
                    sections.append(sec)
                    article_by_section[sec] = art.number
            with toc_index_path.open("w") as f:
                for sec in sections:
                    f.write(json.dumps({
                        "code": law_code,
                        "section": sec,
                        "article": article_by_section.get(sec),
                        "section_url": section_url(sec),
                    }) + "\n")
            logger.info("toc      wrote %s (%d sections)", toc_index_path, len(sections))
        else:
            sections = sections_override

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
    article_by_section: dict[str, str],
) -> None:
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
            extra = rec.get("extra") or {}
            extra.setdefault("article", article_by_section[sec])
            rec["extra"] = extra
        out_lines.append(json.dumps(rec, ensure_ascii=False, default=str))
    manifest_path.write_text("\n".join(out_lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CO Title 42 Vehicles and Traffic fetcher (colorado.public.law).",
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
    p.add_argument(
        "--articles",
        nargs="+",
        default=None,
        help=(f"Filter to specific Title-42 articles "
              f"(default PI-relevant: {','.join(PI_RELEVANT_ARTICLES)})."),
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

    articles_filter: Optional[list[str]] = (
        list(args.articles) if args.articles else PI_RELEVANT_ARTICLES
    )

    asyncio.run(run(
        law_code=args.code,
        out_dir=args.out_dir,
        concurrency=args.concurrency,
        rate_interval=args.rate_interval,
        force=args.force,
        sections_override=sections_override,
        articles_filter=articles_filter,
        skip_known_missing=not args.no_skip_missing,
        heartbeat_every=args.heartbeat_every,
        limit=args.limit,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
