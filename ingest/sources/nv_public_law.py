"""
Nevada Revised Statutes Title 43 (Vehicles) fetcher (via nevada.public.law).

Why public.law:
- leg.state.nv.us serves the official NRS but as a per-chapter HTML mega-page
  with no clean per-section URLs.
- public.law mirrors NRS with one-section-per-URL and clean static HTML.

Discovery strategy (NB: NV adds a sub-chapter level vs. FL/OR/TX):
1. Title 43 page:    https://nevada.public.law/statutes/nrs_title_43
                     -> Chapters 480..490 (Public Safety; Vehicles; Watercraft).
                        PI-relevant: 484A,484B,484C,484E (traffic+DUI+crashes),
                                     485 (insurance/financial responsibility).
2. Chapter page:     https://nevada.public.law/statutes/nrs_chapter_484b
                     -> list of Sub-chapters (named slugs, e.g. "rules_of_the_road").
                        Sections are NOT listed at the chapter level for NV.
3. Sub-chapter page: https://nevada.public.law/statutes/nrs_chapter_484b_sub-chapter_<slug>
                     -> list of Sections (decimal: e.g. "484b.653").
4. Section page:     https://nevada.public.law/statutes/nrs_484b.653
                     -> single statute, HTTP 200 if exists.

Output:
    data/raw/nv_public_law/
        NRS43/<chapter>.<section>.html  # one file per valid section
        toc/                            # cached title + chapter + sub-chapter pages
        NRS43_toc.jsonl                 # master section index
        manifest.jsonl                  # per-record write
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


SOURCE_SLUG = "nv_public_law"
JURISDICTION = "NV"
DEFAULT_LAW_CODE = "NRS43"   # Title 43 Public Safety
BASE = "https://nevada.public.law"
ROOT_URL = f"{BASE}/statutes/nrs_title_43"

# PI-relevant chapters within Title 43 (lowercase to match URL convention):
#   484a Traffic Laws Generally
#   484b Rules of the Road                <- the heart
#   484c Driving Under the Influence
#   484e Crashes and Reports of Crashes
#   485  Motor Vehicle Insurance / Financial Responsibility
PI_RELEVANT_CHAPTERS = ["484a", "484b", "484c", "484e", "485"]

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"

logger = logging.getLogger(SOURCE_SLUG)

_TAG_RE = re.compile(r"<[^>]+>")
# Chapter IDs are mixed alpha/numeric (e.g. "484a", "485"), lowercase in URLs.
_CHAPTER_LINK_RE = re.compile(r"nrs_chapter_(\d+[a-z]?)(?!_sub-chapter)")
_SUBCHAPTER_LINK_RE = re.compile(
    r"nrs_chapter_(\d+[a-z]?)_sub-chapter_([a-z0-9_]+?)(?=[\"'\s>])"
)
_SECTION_LINK_RE = re.compile(r"nrs_(\d+[a-z]?\.\d+)")


# ---------------------------------------------------------------------------
# Source config
# ---------------------------------------------------------------------------


def section_url(section: str) -> str:
    return f"{BASE}/statutes/nrs_{section}"


def chapter_url(chapter: str) -> str:
    return f"{BASE}/statutes/nrs_chapter_{chapter}"


def subchapter_url(chapter: str, sub_slug: str) -> str:
    return f"{BASE}/statutes/nrs_chapter_{chapter}_sub-chapter_{sub_slug}"


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
        code_name="Nevada Revised Statutes Title 43",
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
class NvChapter:
    number: str
    sub_chapters: list[str]   # slugs
    sections: list[str]


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


def parse_title(html: str) -> list[str]:
    html = _normalize_html(html)
    chapters: list[str] = []
    seen: set[str] = set()
    for m in _CHAPTER_LINK_RE.finditer(html):
        ch = m.group(1).lower()
        if ch not in seen:
            seen.add(ch)
            chapters.append(ch)
    return chapters


def parse_chapter(html: str, chapter: str) -> list[str]:
    """Returns the list of sub-chapter slugs found on a chapter page."""
    html = _normalize_html(html)
    subs: list[str] = []
    seen: set[str] = set()
    for m in _SUBCHAPTER_LINK_RE.finditer(html):
        if m.group(1).lower() != chapter:
            continue
        slug = m.group(2)
        if slug not in seen:
            seen.add(slug)
            subs.append(slug)
    return subs


def parse_subchapter(html: str, chapter: str) -> list[str]:
    """Returns section IDs (e.g. '484b.653') found on a sub-chapter page."""
    html = _normalize_html(html)
    sections: list[str] = []
    seen: set[str] = set()
    chapter_l = chapter.lower()
    for m in _SECTION_LINK_RE.finditer(html):
        sec = m.group(1).lower()
        # Sections are <chapter>.<n>; verify the chapter prefix matches.
        if not sec.startswith(chapter_l + "."):
            continue
        if sec not in seen:
            seen.add(sec)
            sections.append(sec)
    return sorted(sections, key=_section_sort_key)


def _section_sort_key(s: str) -> tuple:
    # NV section IDs look like "484b.653" or "485.020"
    m = re.match(r"(\d+)([a-z]?)\.(\d+)", s)
    if not m:
        return (10**9, "", 10**9, s)
    return (int(m.group(1)), m.group(2), int(m.group(3)), s)


async def discover_sections(
    client: httpx.AsyncClient,
    rate: Optional[RateLimiter],
    out_dir: Path,
    *,
    chapters_filter: Optional[list[str]] = None,
) -> list[NvChapter]:
    toc_dir = out_dir / "toc"
    toc_dir.mkdir(parents=True, exist_ok=True)

    logger.info("toc      GET title 43")
    r = await get_with_retry(client, ROOT_URL, rate=rate, label="nv-title-43")
    r.raise_for_status()
    (toc_dir / "title_43.html").write_text(r.text)
    chapters_all = parse_title(r.text)
    logger.info("toc      title 43: %d chapters: %s",
                len(chapters_all), ",".join(chapters_all))

    if chapters_filter is not None:
        wanted = {c.lower() for c in chapters_filter}
        chapters = [c for c in chapters_all if c in wanted]
        skipped = [c for c in chapters_all if c not in wanted]
        logger.info("toc      filter PI-relevant=%s skipped=%s",
                    chapters, skipped)
    else:
        chapters = chapters_all

    chapters_out: list[NvChapter] = []
    chap_lock = asyncio.Lock()

    async def _fetch_chapter(ch: str):
        url = chapter_url(ch)
        resp = await get_with_retry(client, url, rate=rate, label=f"nv-chap-{ch}")
        if resp.status_code >= 400:
            logger.warning("toc      chapter %s failed status=%d", ch, resp.status_code)
            return
        (toc_dir / f"chapter_{ch}.html").write_text(resp.text)
        sub_slugs = parse_chapter(resp.text, ch)
        logger.info("toc      chapter %s: %d sub-chapters", ch, len(sub_slugs))

        # Walk each sub-chapter to harvest sections.
        sections: set[str] = set()
        sub_lock = asyncio.Lock()

        async def _fetch_sub(slug: str):
            url = subchapter_url(ch, slug)
            sub_resp = await get_with_retry(
                client, url, rate=rate, label=f"nv-sub-{ch}-{slug[:20]}",
            )
            if sub_resp.status_code >= 400:
                logger.warning(
                    "toc      sub %s/%s failed status=%d",
                    ch, slug, sub_resp.status_code,
                )
                return
            (toc_dir / f"chapter_{ch}_sub_{slug}.html").write_text(sub_resp.text)
            secs = parse_subchapter(sub_resp.text, ch)
            async with sub_lock:
                sections.update(secs)

        if sub_slugs:
            await asyncio.gather(*[_fetch_sub(s) for s in sub_slugs])
        else:
            # Some chapters may list sections directly (no sub-chapter layer).
            secs = parse_subchapter(resp.text, ch)
            sections.update(secs)

        ordered = sorted(sections, key=_section_sort_key)
        async with chap_lock:
            chapters_out.append(NvChapter(ch, sub_slugs, ordered))

    await asyncio.gather(*[_fetch_chapter(c) for c in chapters])

    chapters_out.sort(key=lambda c: _section_sort_key(c.number + ".0"))
    total_sections = sum(len(c.sections) for c in chapters_out)
    logger.info(
        "toc      total: %d chapters, %d sections",
        len(chapters_out), total_sections,
    )
    return chapters_out


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
    chapters_filter: Optional[list[str]] = None,
    skip_known_missing: bool = True,
    heartbeat_every: float = 5.0,
    limit: Optional[int] = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"
    toc_index_path = out_dir / f"{law_code}_toc.jsonl"

    config = config_for(law_code=law_code, out_root=out_dir)
    rate = RateLimiter(rate_interval) if rate_interval > 0 else None

    chapter_by_section: dict[str, str] = {}

    async with default_client(concurrency=concurrency) as client:
        if sections_override is None:
            chapters = await discover_sections(
                client, rate, out_dir,
                chapters_filter=chapters_filter or PI_RELEVANT_CHAPTERS,
            )
            seen: set[str] = set()
            sections: list[str] = []
            for ch in chapters:
                for sec in ch.sections:
                    if sec in seen:
                        continue
                    seen.add(sec)
                    sections.append(sec)
                    chapter_by_section[sec] = ch.number
            with toc_index_path.open("w") as f:
                for sec in sections:
                    f.write(json.dumps({
                        "code": law_code,
                        "section": sec,
                        "chapter": chapter_by_section.get(sec),
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

    if chapter_by_section:
        _annotate_manifest_with_chapters(manifest_path, chapter_by_section)

    summary = {
        "code": law_code,
        "total": len(sections),
        **stats,
    }
    logger.info("done     %s", summary)
    logger.info("manifest %s", manifest_path)
    return summary


def _annotate_manifest_with_chapters(
    manifest_path: Path,
    chapter_by_section: dict[str, str],
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
        if sec in chapter_by_section:
            extra = rec.get("extra") or {}
            extra.setdefault("chapter", chapter_by_section[sec])
            rec["extra"] = extra
        out_lines.append(json.dumps(rec, ensure_ascii=False, default=str))
    manifest_path.write_text("\n".join(out_lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="NV Title 43 fetcher (nevada.public.law).",
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
        "--chapters",
        nargs="+",
        default=None,
        help=(f"Filter to specific Title-43 chapters "
              f"(default PI-relevant: {','.join(PI_RELEVANT_CHAPTERS)}). "
              "Pass 'all' to fetch every chapter."),
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

    chapters_filter: Optional[list[str]] = None
    if args.chapters:
        if len(args.chapters) == 1 and args.chapters[0].lower() == "all":
            chapters_filter = None
        else:
            chapters_filter = [c.lower() for c in args.chapters]
    else:
        chapters_filter = PI_RELEVANT_CHAPTERS

    asyncio.run(run(
        law_code=args.code,
        out_dir=args.out_dir,
        concurrency=args.concurrency,
        rate_interval=args.rate_interval,
        force=args.force,
        sections_override=sections_override,
        chapters_filter=chapters_filter,
        skip_known_missing=not args.no_skip_missing,
        heartbeat_every=args.heartbeat_every,
        limit=args.limit,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
