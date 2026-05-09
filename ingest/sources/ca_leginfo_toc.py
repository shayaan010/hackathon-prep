"""
California leginfo TOC walker — discovers the *exact* set of statute sections
in a code by walking the official Table of Contents tree, instead of guessing
section numbers via numeric ranges.

Tree structure on leginfo:

  codesTOCSelected.xhtml?tocCode=VEH                     (root: lists 34 divisions)
    -> codes_displayexpandedbranch.xhtml?...&division=N. (lists chapters/articles)
       -> codes_displayText.xhtml?...&chapter=N&article=N (renders all sections inline)

The displayText pages are the goldmine: each contains every section in that
chapter/article, with section numbers anchored as
    <a href="javascript:submitCodesValues('21350.', '15.2.1', '1974', '648', ...)">21350.</a>

Output:
    data/raw/ca_leginfo_toc/
        {LAW_CODE}_toc.jsonl     — one record per (division, chapter, article, section)
        chapters/                — saved displayText HTML (chapter-level corpus)
        debug/                   — saved division branch HTML (for diagnostics)

Usage:
    uv run python -m ingest.sources.ca_leginfo_toc --code VEH
    uv run python -m ingest.sources.ca_leginfo_toc --code VEH --diff data/raw/ca_leginfo_pages/manifest.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from dataclasses import dataclass, asdict
from html import unescape
from pathlib import Path
from typing import Optional

import httpx

from .._http import RateLimiter, default_client, get_with_retry, setup_logging


SOURCE_SLUG = "ca_leginfo_toc"
JURISDICTION = "CA"
BASE = "https://leginfo.legislature.ca.gov/faces"

DEFAULT_LAW_CODE = "VEH"
DEFAULT_OUT_DIR = Path("data/raw") / SOURCE_SLUG
DEFAULT_LOG_FILE = Path("data/logs") / f"{SOURCE_SLUG}.log"
DEFAULT_CONCURRENCY = 2
DEFAULT_RATE_INTERVAL = 0.4

logger = logging.getLogger(SOURCE_SLUG)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Division:
    code: str           # "VEH"
    division: str       # "11."
    title: str          # e.g. "RULES OF THE ROAD"
    section_range: str  # display-only: "21000 - 23336"
    nodetreepath: int


@dataclass
class ChapterArticle:
    """A leaf-ish node in the TOC: a (chapter [+article]) we can fetch displayText for."""
    code: str
    division: str
    title: str   # always empty for VEH at this level
    part: str
    chapter: str
    article: str

    def url(self) -> str:
        return (
            f"{BASE}/codes_displayText.xhtml"
            f"?lawCode={self.code}"
            f"&division={self.division}"
            f"&title={self.title}"
            f"&part={self.part}"
            f"&chapter={self.chapter}"
            f"&article={self.article}"
        )

    def slug(self) -> str:
        # Filesystem-safe identifier for caching
        parts = [
            f"div{self.division.rstrip('.')}",
            f"part{self.part.rstrip('.')}" if self.part else "",
            f"ch{self.chapter.rstrip('.')}" if self.chapter else "",
            f"art{self.article.rstrip('.')}" if self.article else "",
        ]
        return "_".join(p for p in parts if p)


@dataclass
class TocSection:
    code: str
    section: str         # canonical, no trailing dot — "21350" or "21351.3"
    division: str
    part: str
    chapter: str
    article: str
    source_url: str      # the displayText URL where we found it
    section_url: str     # the canonical codes_displaySection URL


# ---------------------------------------------------------------------------
# TOC parsing
# ---------------------------------------------------------------------------

# Division row in codesTOCSelected.xhtml looks like:
#   ...division=1.&...&nodetreepath=2 ... DIVISION 1 - WORDS AND PHRASES DEFINED [100 - 681]
_DIVISION_LINK_RE = re.compile(
    r'codes_displayexpandedbranch\.xhtml\?'
    r'tocCode=(?P<code>[A-Z]+)'
    r'&(?:amp;)?division=(?P<division>[\d.]+)'
    r'&(?:amp;)?title=(?P<title>[^&"\']*)'
    r'&(?:amp;)?part=(?P<part>[^&"\']*)'
    r'&(?:amp;)?chapter=(?P<chapter>[^&"\']*)'
    r'&(?:amp;)?article=(?P<article>[^&"\']*)'
    r'&(?:amp;)?nodetreepath=(?P<ntp>\d+)',
)

# Heading text inside the division anchor (already de-tagged):
#   "DIVISION 1. WORDS AND PHRASES DEFINED [100 - 681]"
# Note: the dot after the number, and brackets are produced by separate <span>s.
_DIVISION_HEADING_RE = re.compile(
    r'DIVISION\s+[\d.]+\.?\s*[-.]?\s*(.+?)\s*\[\s*([^\]]+?)\s*\]',
    re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")

# displayText link inside an expanded branch page
_DISPLAYTEXT_LINK_RE = re.compile(
    r'codes_displayText\.xhtml\?'
    r'lawCode=(?P<code>[A-Z]+)'
    r'&(?:amp;)?division=(?P<division>[\d.]*)'
    r'&(?:amp;)?title=(?P<title>[^&"\']*)'
    r'&(?:amp;)?part=(?P<part>[^&"\']*)'
    r'&(?:amp;)?chapter=(?P<chapter>[^&"\']*)'
    r'&(?:amp;)?article=(?P<article>[^&"\']*)',
)

# Section anchor inside displayText:
#   <a href="javascript:submitCodesValues('21350.', ...
_SECTION_ANCHOR_RE = re.compile(
    r"submitCodesValues\(\s*'([\d.]+?)\.?'",
)


def parse_divisions_from_root(html: str, code: str) -> list[Division]:
    """Parse the codesTOCSelected.xhtml root page to get all divisions."""
    # Decode entities first so the regex can see plain '&'.
    text = unescape(html)
    seen: dict[str, Division] = {}
    # Pair each link with the heading text that follows it.
    # leginfo renders: <a href=...>DIVISION N - TITLE [LO - HI]</a>
    # Strategy: scan all matches of the link regex, then grab the surrounding text
    # to extract the heading.
    for m in _DIVISION_LINK_RE.finditer(text):
        if m.group("code") != code:
            continue
        division = m.group("division")
        if division in seen:
            continue  # skip duplicates (page may render link twice)
        # The heading text is INSIDE the anchor: <a href=...>DIVISION 1. TITLE [LO - HI]</a>
        # Find the closing </a> and strip tags from the inner text.
        end_anchor = text.find("</a>", m.end())
        title = ""
        section_range = ""
        if end_anchor > 0:
            inner = _TAG_RE.sub(" ", text[m.end():end_anchor])
            inner = re.sub(r"\s+", " ", inner).strip()
            h = _DIVISION_HEADING_RE.search(inner)
            if h:
                title = h.group(1).strip()
                section_range = h.group(2).strip()
        seen[division] = Division(
            code=code,
            division=division,
            title=title,
            section_range=section_range,
            nodetreepath=int(m.group("ntp")),
        )
    return sorted(seen.values(), key=lambda d: d.nodetreepath)


def parse_displaytext_links(html: str, code: str) -> list[ChapterArticle]:
    """From a displayexpandedbranch page, extract all leaf displayText URLs."""
    text = unescape(html)
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[ChapterArticle] = []
    for m in _DISPLAYTEXT_LINK_RE.finditer(text):
        if m.group("code") != code:
            continue
        ca = ChapterArticle(
            code=code,
            division=m.group("division"),
            title=m.group("title"),
            part=m.group("part"),
            chapter=m.group("chapter"),
            article=m.group("article"),
        )
        key = (ca.division, ca.title, ca.part, ca.chapter, ca.article)
        if key in seen:
            continue
        seen.add(key)
        out.append(ca)
    return out


def parse_sections_from_displaytext(html: str) -> list[str]:
    """Extract all unique section numbers from a displayText chapter page."""
    text = unescape(html)
    nums = _SECTION_ANCHOR_RE.findall(text)
    # Strip trailing dots, dedupe, sort
    cleaned = sorted(
        {n.rstrip(".") for n in nums if n},
        key=_section_sort_key,
    )
    return cleaned


def _section_sort_key(s: str) -> tuple:
    m = re.match(r"(\d+)(?:\.(\d+))?", s)
    if not m:
        return (10**9, 0, s)
    return (int(m.group(1)), int(m.group(2) or 0), s)


def section_url(section: str, law_code: str) -> str:
    return f"{BASE}/codes_displaySection.xhtml?sectionNum={section}.&lawCode={law_code}"


# ---------------------------------------------------------------------------
# Walker
# ---------------------------------------------------------------------------


async def walk(
    law_code: str = DEFAULT_LAW_CODE,
    *,
    out_dir: Path = DEFAULT_OUT_DIR,
    concurrency: int = DEFAULT_CONCURRENCY,
    rate_interval: float = DEFAULT_RATE_INTERVAL,
    save_chapters: bool = True,
) -> tuple[list[Division], list[ChapterArticle], list[TocSection]]:
    """Full TOC walk. Returns (divisions, chapter_leaves, sections)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    chapters_dir = out_dir / "chapters"
    debug_dir = out_dir / "debug"
    if save_chapters:
        chapters_dir.mkdir(exist_ok=True)
    debug_dir.mkdir(exist_ok=True)

    rate = RateLimiter(rate_interval) if rate_interval > 0 else None
    sem = asyncio.Semaphore(concurrency)

    async with default_client(concurrency=concurrency) as client:

        # ---- 1. Root TOC: list of divisions ----
        root_url = f"{BASE}/codesTOCSelected.xhtml?tocCode={law_code}"
        logger.info("step-1   GET %s", root_url)
        resp = await get_with_retry(client, root_url, rate=rate, label="root-toc")
        (debug_dir / f"{law_code}_root.html").write_text(resp.text)
        divisions = parse_divisions_from_root(resp.text, law_code)
        logger.info("step-1   found %d divisions for %s", len(divisions), law_code)
        for d in divisions:
            logger.info("  div %-6s ntp=%-3d %s [%s]", d.division, d.nodetreepath, d.title, d.section_range)

        if not divisions:
            logger.error("no divisions parsed; aborting")
            return ([], [], [])

        # ---- 2. For each division: fetch expanded branch, extract chapter/article URLs ----
        # Some small divisions (e.g. VEH Div 1, 12.5) have NO chapters — leginfo redirects
        # codes_displayexpandedbranch -> codes_displayText for them. We detect this and
        # synthesize a single "whole-division" leaf so we still grab their sections.
        # Direct extraction at this step is also valuable as a backstop.
        direct_section_lists: dict[str, list[str]] = {}  # division -> sections (if redirected)

        async def _fetch_division(d: Division) -> tuple[Division, list[ChapterArticle]]:
            url = (
                f"{BASE}/codes_displayexpandedbranch.xhtml"
                f"?tocCode={d.code}"
                f"&division={d.division}"
                f"&title=&part=&chapter=&article="
                f"&nodetreepath={d.nodetreepath}"
            )
            async with sem:
                resp = await get_with_retry(client, url, rate=rate, label=f"div-{d.division}")
            (debug_dir / f"{law_code}_div{d.division.rstrip('.')}.html").write_text(resp.text)
            leaves = parse_displaytext_links(resp.text, law_code)
            if not leaves:
                # Likely redirected to displayText directly — extract sections from THIS page.
                final_url = str(resp.url)
                nums = parse_sections_from_displaytext(resp.text)
                if nums:
                    logger.info(
                        "step-2   div=%-6s redirected to displayText (%d sections inline) final_url=%s",
                        d.division, len(nums), final_url,
                    )
                    direct_section_lists[d.division] = nums
                    # Synthesize a leaf so the rest of the pipeline records it
                    leaves = [ChapterArticle(
                        code=law_code, division=d.division, title="",
                        part="", chapter="", article="",
                    )]
                    # Cache the displayText we just got, so step 3 doesn't re-fetch it.
                    if save_chapters:
                        slug = leaves[0].slug() or f"div{d.division.rstrip('.')}"
                        (chapters_dir / f"{slug}.html").write_text(resp.text)
                else:
                    logger.warning(
                        "step-2   div=%-6s -> 0 leaves AND 0 inline sections (final_url=%s)",
                        d.division, final_url,
                    )
            else:
                logger.info("step-2   div=%-6s -> %d displayText leaves", d.division, len(leaves))
            return d, leaves

        div_results = await asyncio.gather(*[_fetch_division(d) for d in divisions])
        all_leaves: list[ChapterArticle] = []
        for _, leaves in div_results:
            all_leaves.extend(leaves)
        logger.info("step-2   total leaves across all divisions: %d", len(all_leaves))

        # ---- 3. For each leaf, fetch displayText, extract section numbers ----
        sections: list[TocSection] = []
        sections_lock = asyncio.Lock()
        completed = 0
        total = len(all_leaves)

        async def _fetch_leaf(ca: ChapterArticle):
            nonlocal completed
            url = ca.url()
            slug = ca.slug()
            cache = chapters_dir / f"{slug}.html"
            if save_chapters and cache.exists() and cache.stat().st_size > 1000:
                html = cache.read_text(errors="ignore")
            else:
                async with sem:
                    resp = await get_with_retry(client, url, rate=rate, label=f"chap-{slug}")
                html = resp.text
                if save_chapters:
                    cache.write_text(html)
            nums = parse_sections_from_displaytext(html)
            new_secs = [
                TocSection(
                    code=law_code,
                    section=n,
                    division=ca.division,
                    part=ca.part,
                    chapter=ca.chapter,
                    article=ca.article,
                    source_url=url,
                    section_url=section_url(n, law_code),
                )
                for n in nums
            ]
            async with sections_lock:
                sections.extend(new_secs)
                completed += 1
                if completed % 10 == 0 or completed == total:
                    logger.info(
                        "step-3   %d/%d leaves done | sections so far=%d (slug=%s nums=%d)",
                        completed, total, len(sections), slug, len(nums),
                    )

        await asyncio.gather(*[_fetch_leaf(ca) for ca in all_leaves])

        # Dedupe sections (a section may appear in overlapping articles in rare cases)
        seen: set[str] = set()
        unique: list[TocSection] = []
        for s in sorted(sections, key=lambda x: _section_sort_key(x.section)):
            if s.section in seen:
                continue
            seen.add(s.section)
            unique.append(s)
        logger.info(
            "step-3   total section anchors=%d  unique sections=%d",
            len(sections), len(unique),
        )

        # ---- 4. Persist master section index ----
        index_path = out_dir / f"{law_code}_toc.jsonl"
        with index_path.open("w") as f:
            for s in unique:
                f.write(json.dumps(asdict(s)) + "\n")
        logger.info("wrote    %s (%d sections)", index_path, len(unique))

        return divisions, all_leaves, unique


# ---------------------------------------------------------------------------
# Diff vs existing manifest
# ---------------------------------------------------------------------------


def diff_against_manifest(
    sections: list[TocSection],
    manifest_path: Path,
) -> tuple[list[TocSection], list[str]]:
    """Return (sections_missing_from_manifest, valid_sections_in_manifest)."""
    valid_in_manifest: set[str] = set()
    if manifest_path.exists():
        with manifest_path.open() as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("valid"):
                    valid_in_manifest.add(rec.get("section", ""))
    missing = [s for s in sections if s.section not in valid_in_manifest]
    return missing, sorted(valid_in_manifest, key=_section_sort_key)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--code", default=DEFAULT_LAW_CODE,
                   help=f"Law code (default: {DEFAULT_LAW_CODE})")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                   help=f"Output directory root (default: {DEFAULT_OUT_DIR})")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                   help=f"Parallel HTTP requests (default: {DEFAULT_CONCURRENCY})")
    p.add_argument("--rate-interval", type=float, default=DEFAULT_RATE_INTERVAL,
                   help=f"Min seconds between request starts (default: {DEFAULT_RATE_INTERVAL})")
    p.add_argument("--no-save-chapters", action="store_true",
                   help="Don't save chapter-level HTML to disk")
    p.add_argument("--diff", type=Path,
                   help="Path to a per-section manifest.jsonl to diff against. "
                        "Outputs missing-sections list at out_dir/{code}_missing.txt")
    p.add_argument("--log-file", type=Path, default=DEFAULT_LOG_FILE,
                   help=f"File to append log lines to (default: {DEFAULT_LOG_FILE})")
    p.add_argument("--no-log-file", action="store_true",
                   help="Disable file logging.")
    p.add_argument("--verbose", action="store_true",
                   help="DEBUG-level logs to stdout.")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])

    log_path = None if args.no_log_file else args.log_file
    setup_logging(SOURCE_SLUG, verbose=args.verbose, log_path=log_path)

    divisions, leaves, sections = asyncio.run(walk(
        law_code=args.code,
        out_dir=args.out_dir,
        concurrency=args.concurrency,
        rate_interval=args.rate_interval,
        save_chapters=not args.no_save_chapters,
    ))

    summary = {
        "code": args.code,
        "divisions": len(divisions),
        "leaves": len(leaves),
        "sections_unique": len(sections),
    }
    logger.info("summary  %s", summary)

    if args.diff:
        missing, present = diff_against_manifest(sections, args.diff)
        out = args.out_dir / f"{args.code}_missing.txt"
        with out.open("w") as f:
            for s in missing:
                f.write(f"{s.section}\n")
        logger.info(
            "diff     manifest=%s  in-manifest=%d  expected=%d  missing=%d -> %s",
            args.diff, len(present), len(sections), len(missing), out,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
