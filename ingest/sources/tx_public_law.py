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
                  -> list of Chapters (or list of Subtitles).
3. Subtitle:      https://texas.public.law/statutes/tex._transp._code_title_<N>_subtitle_<X>
                  -> list of Chapters.
4. Chapter page:  https://texas.public.law/statutes/tex._transp._code_chapter_<N>
                  -> list of Sections (decimal: e.g. "545.151").
5. Section page:  https://texas.public.law/statutes/tex._transp._code_section_<X.Y>

Output:
    data/raw/tx_public_law/
        TN/<chapter>.<section>.html  # one file per valid section
        toc/                         # cached title + chapter pages (debug/audit)
        TN_toc.jsonl                 # master section index
        manifest.jsonl               # per-record write
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


SOURCE_SLUG = "tx_public_law"
JURISDICTION = "TX"
DEFAULT_LAW_CODE = "TN"
BASE = "https://texas.public.law"
ROOT_URL = f"{BASE}/statutes/tex._transp._code"

DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"

logger = logging.getLogger(SOURCE_SLUG)

_TAG_RE = re.compile(r"<[^>]+>")
_ROOT_TITLE_RE = re.compile(
    r"\b(\d{1,2})\s+([A-Z][^\d]{3,80}?)\s+Chapters?\s+([\d\-]+)",
)
_CHAPTER_LINK_RE = re.compile(
    r"tex\._transp\._code(?:_title_\d+(?:_subtitle_[a-z])?)?_chapter_(\d+[a-z]?)",
)
_SUBTITLE_LINK_RE = re.compile(
    r"tex\._transp\._code_title_(\d+)_subtitle_([a-z])",
)
_SECTION_LINK_RE = re.compile(
    r"tex\._transp\._code_section_(\d+\.\d+(?:[a-z])?)",
)
_CHAPTER_TITLE_RE = re.compile(
    r"Chapter\s+\d+[a-z]?\s+(.+?)(?:\s+Statute|\s+Sections?|\s+Subchapter|\s+\d+\.)",
)


# ---------------------------------------------------------------------------
# Source config
# ---------------------------------------------------------------------------


def section_url(section: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_section_{section}"


def chapter_url(chapter: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_chapter_{chapter}"


def title_url(title_number: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_title_{title_number}"


def subtitle_url(title_number: str, subtitle: str) -> str:
    return f"{BASE}/statutes/tex._transp._code_title_{title_number}_subtitle_{subtitle}"


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
        code_name="Transportation Code",
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
class TxTitle:
    number: str
    title: str
    chapter_range: str


@dataclass
class TxChapter:
    title_number: str
    number: str
    title: str
    sections: list[str]


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
        titles[num] = TxTitle(num, m.group(2).strip(), m.group(3).strip())
    return list(titles.values())


def parse_title(html: str, title_number: str) -> tuple[list[str], list[str]]:
    chapters: list[str] = []
    seen_ch: set[str] = set()
    for m in _CHAPTER_LINK_RE.finditer(html):
        ch = m.group(1)
        if ch not in seen_ch:
            seen_ch.add(ch)
            chapters.append(ch)
    subtitles: list[str] = []
    seen_st: set[str] = set()
    for m in _SUBTITLE_LINK_RE.finditer(html):
        if m.group(1) != title_number:
            continue
        st = m.group(2)
        if st not in seen_st:
            seen_st.add(st)
            subtitles.append(st)
    return chapters, subtitles


def parse_subtitle(html: str) -> list[str]:
    chapters: list[str] = []
    seen: set[str] = set()
    for m in _CHAPTER_LINK_RE.finditer(html):
        ch = m.group(1)
        if ch not in seen:
            seen.add(ch)
            chapters.append(ch)
    return chapters


def parse_chapter(html: str) -> list[str]:
    sections: list[str] = []
    seen: set[str] = set()
    for m in _SECTION_LINK_RE.finditer(html):
        sec = m.group(1)
        if sec not in seen:
            seen.add(sec)
            sections.append(sec)
    return sorted(sections, key=_section_sort_key)


def _section_sort_key(s: str) -> tuple:
    m = re.match(r"(\d+)\.(\d+)", s)
    if not m:
        return (10**9, 10**9, s)
    return (int(m.group(1)), int(m.group(2)), s)


async def discover_sections(
    client: httpx.AsyncClient,
    rate: Optional[RateLimiter],
    out_dir: Path,
) -> list[TxChapter]:
    toc_dir = out_dir / "toc"
    toc_dir.mkdir(parents=True, exist_ok=True)

    logger.info("toc      GET root")
    r = await get_with_retry(client, ROOT_URL, rate=rate, label="tx-root")
    r.raise_for_status()
    (toc_dir / "root.html").write_text(r.text)
    titles = parse_root(r.text)
    logger.info("toc      discovered %d titles", len(titles))

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
        title_chapters[t.number] = chs
        title_subtitles[t.number] = sts

    await asyncio.gather(*[_fetch_title(t) for t in titles])

    flat_subtitles: list[tuple[str, str]] = [
        (tnum, st) for tnum, sts in title_subtitles.items() for st in sts
    ]
    if flat_subtitles:
        async def _fetch_subtitle(tnum: str, st: str):
            url = subtitle_url(tnum, st)
            resp = await get_with_retry(client, url, rate=rate, label=f"tx-subt-{tnum}-{st}")
            if resp.status_code >= 400:
                return
            (toc_dir / f"title_{tnum}_subtitle_{st}.html").write_text(resp.text)
            chs = parse_subtitle(resp.text)
            existing = title_chapters.setdefault(tnum, [])
            for ch in chs:
                if ch not in existing:
                    existing.append(ch)
        await asyncio.gather(*[_fetch_subtitle(t, s) for t, s in flat_subtitles])

    flat_chapters: list[tuple[str, str]] = [
        (tnum, ch) for tnum, chs in title_chapters.items() for ch in chs
    ]
    logger.info("toc      total chapters: %d", len(flat_chapters))

    chapters_out: list[TxChapter] = []
    chap_lock = asyncio.Lock()

    async def _fetch_chapter(tnum: str, ch: str):
        url = chapter_url(ch)
        resp = await get_with_retry(client, url, rate=rate, label=f"tx-chap-{ch}")
        if resp.status_code >= 400:
            return
        (toc_dir / f"chapter_{ch}.html").write_text(resp.text)
        sections = parse_chapter(resp.text)
        text = _text_of(resp.text)
        m = _CHAPTER_TITLE_RE.search(text)
        chap_title = m.group(1).strip() if m else ""
        async with chap_lock:
            chapters_out.append(TxChapter(tnum, ch, chap_title, sections))

    await asyncio.gather(*[_fetch_chapter(t, c) for t, c in flat_chapters])

    total_sections = sum(len(c.sections) for c in chapters_out)
    logger.info(
        "toc      total: %d titles, %d chapters, %d sections",
        len(titles), len(chapters_out), total_sections,
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
            chapters = await discover_sections(client, rate, out_dir)
            seen: set[str] = set()
            sections: list[str] = []
            chapter_by_section: dict[str, tuple[Optional[str], Optional[str]]] = {}
            for ch in chapters:
                for sec in ch.sections:
                    if sec in seen:
                        continue
                    seen.add(sec)
                    sections.append(sec)
                    chapter_by_section[sec] = (ch.title_number, ch.number)
            with toc_index_path.open("w") as f:
                for sec in sections:
                    tnum, ch = chapter_by_section[sec]
                    f.write(json.dumps({
                        "code": law_code,
                        "section": sec,
                        "title_number": tnum,
                        "chapter": ch,
                        "section_url": section_url(sec),
                    }) + "\n")
            logger.info("toc      wrote %s (%d sections)", toc_index_path, len(sections))
        else:
            sections = sections_override
            chapter_by_section = {}

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
    chapter_by_section: dict[str, tuple[Optional[str], Optional[str]]],
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
            tnum, ch = chapter_by_section[sec]
            extra = rec.get("extra") or {}
            extra.setdefault("title_number", tnum)
            extra.setdefault("chapter", ch)
            rec["extra"] = extra
        out_lines.append(json.dumps(rec, ensure_ascii=False, default=str))
    manifest_path.write_text("\n".join(out_lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="TX Transportation Code fetcher (texas.public.law).",
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
