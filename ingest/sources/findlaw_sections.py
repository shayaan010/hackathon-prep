"""
FindLaw statute section fetcher (NY VAT, TX Transportation Code, etc.).

Uses Playwright to fetch section HTML pages from codes.findlaw.com, which is
behind Cloudflare protection that blocks plain HTTP clients.

Discovery: section URLs come from pre-built seed lists (range expansion or
explicit lists).  The sitemap XML is also Cloudflare-protected, so we can't
use it for discovery either.  Instead, we:

  1. Expand known section-number ranges into candidate URLs
  2. Fetch each URL; 404-ish pages (missing sections) are detected by the
     absence of a ``div.codes-content`` element and discarded.

Each state+code combo is a "code config" that defines:
  - URL prefix and section slug pattern
  - Section number ranges
  - Section-number format (integer vs dotted)

Output:
    data/raw/findlaw_pages/
        {JURISDICTION}_{CODE}/
            {section_slug}.html
        manifest.jsonl
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger("findlaw_sections")

DEFAULT_OUT_DIR = Path("data/raw") / "findlaw_pages"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class CodeConfig:
    slug: str
    jurisdiction: str
    law_code: str
    code_name: str
    url_prefix: str
    section_pattern: str
    section_ranges: list[tuple[int, int]]
    dotted: bool = False

    def section_url(self, section: str) -> str:
        slug = section.replace(".", "-") if self.dotted else section
        return f"{self.url_prefix}{self.section_pattern.format(section=slug)}/"

    def section_filename(self, section: str) -> str:
        slug = section.replace(".", "-") if self.dotted else section
        return f"{slug}.html"

    def expand_ranges(self) -> list[str]:
        sections: list[str] = []
        for lo, hi in self.section_ranges:
            if self.dotted:
                for n in range(lo, hi + 1):
                    sections.append(str(n))
            else:
                for n in range(lo, hi + 1):
                    sections.append(str(n))
        return sections


CODES: dict[str, CodeConfig] = {
    "NY_VAT": CodeConfig(
        slug="NY_VAT",
        jurisdiction="NY",
        law_code="VAT",
        code_name="Vehicle and Traffic Law",
        url_prefix="https://codes.findlaw.com/ny/vehicle-and-traffic-law/",
        section_pattern="vat-sect-{section}",
        section_ranges=[
            (1, 2500),
        ],
        dotted=False,
    ),
    "TX_TRANSP": CodeConfig(
        slug="TX_TRANSP",
        jurisdiction="TX",
        law_code="TRANSP",
        code_name="Transportation Code",
        url_prefix="https://codes.findlaw.com/tx/transportation-code/",
        section_pattern="transp-sect-{section}",
        section_ranges=[
            (501, 600),
            (601, 700),
            (701, 800),
            (1001, 1120),
            (2001, 2100),
            (2201, 2300),
            (2401, 2500),
            (2501, 2600),
            (2601, 2700),
            (2901, 3000),
            (3001, 3100),
            (3201, 3300),
            (3401, 3500),
            (3601, 3700),
            (3701, 3800),
            (3901, 4000),
            (4001, 4100),
            (4101, 4200),
            (4201, 4300),
            (4401, 4500),
            (4501, 4600),
            (4601, 4700),
            (4701, 4800),
            (4801, 4900),
            (5001, 5100),
            (5201, 5300),
            (5301, 5400),
            (5401, 5500),
            (5501, 5600),
            (5601, 5700),
            (5701, 5800),
            (5801, 5900),
            (6001, 6150),
            (6201, 6350),
            (6301, 6410),
            (6601, 6720),
            (6801, 6950),
            (7001, 7110),
        ],
        dotted=True,
    ),
}

_VALID_CONTENT_MARKER = re.compile(r"div\.codes-content|class=\"codes-content\"", re.IGNORECASE)


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
    title: Optional[str] = None
    error: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch_sections_playwright(
    config: CodeConfig,
    sections: list[str],
    out_dir: Path = DEFAULT_OUT_DIR,
    *,
    force: bool = False,
    delay: float = 1.0,
    limit: Optional[int] = None,
    verbose: bool = False,
) -> list[FetchRecord]:
    from playwright.sync_api import sync_playwright

    code_dir = out_dir / config.slug
    code_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.jsonl"

    if limit:
        sections = sections[:limit]

    records: list[FetchRecord] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        # Visit the main code page first to solve Cloudflare challenge
        # and establish session cookies before fetching sections.
        logger.info("warming up browser session on %s", config.url_prefix.rstrip("/"))
        try:
            page.goto(config.url_prefix.rstrip("/") + "/", wait_until="domcontentloaded", timeout=30_000)
            time.sleep(3)
            warmup_html = page.content()
            if "moment" in warmup_html[:5000].lower() or "challenge-platform" in warmup_html:
                logger.info("Cloudflare challenge detected on warmup, waiting for resolution...")
                try:
                    page.wait_for_selector("div.codesBreadcrumb, div.codes-content", timeout=15_000)
                    logger.info("Cloudflare challenge resolved")
                except Exception:
                    logger.warning("Cloudflare challenge may not have resolved — pages may fail")
            else:
                logger.info("warmup page loaded (title=%s)", page.title()[:80])
        except Exception as e:
            logger.warning("warmup page load failed: %s (continuing anyway)", e)

        for i, section in enumerate(sections):
            url = config.section_url(section)
            dest = code_dir / config.section_filename(section)

            if dest.exists() and dest.stat().st_size > 0 and not force:
                text = dest.read_text(errors="ignore")
                sha = hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()
                valid = "codes-content" in text
                from selectolax.parser import HTMLParser
                title_text = None
                if valid:
                    tree = HTMLParser(text)
                    t = tree.css_first("div.title")
                    title_text = t.text(strip=True) if t else None
                records.append(FetchRecord(
                    source_slug="findlaw_sections",
                    jurisdiction=config.jurisdiction,
                    law_code=config.law_code,
                    section=section,
                    url=url,
                    fetched_at=_now_iso(),
                    http_status=200,
                    bytes=len(text.encode("utf-8")),
                    content_sha256=sha,
                    raw_path=str(dest),
                    valid=valid,
                    title=title_text,
                ))
                continue

            logger.info("fetch %s %s (%d/%d)", config.slug, section, i + 1, len(sections))

            try:
                response = page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                status = response.status if response else 0
                time.sleep(delay)

                html = page.content()
                sha = hashlib.sha256(html.encode("utf-8")).hexdigest()

                valid = "codes-content" in html and "subsection" in html
                if not valid and "404" in html[:2000].lower():
                    valid = False

                title_text = None
                if valid:
                    from selectolax.parser import HTMLParser
                    tree = HTMLParser(html)
                    t = tree.css_first("div.title")
                    title_text = t.text(strip=True) if t else None

                if valid:
                    dest.write_text(html, encoding="utf-8")
                    logger.info(
                        "  ok  section=%s status=%d bytes=%d title=%s",
                        section, status, len(html.encode("utf-8")),
                        (title_text or "")[:80],
                    )
                else:
                    logger.info("  skip section=%s (no statute content)", section)

                records.append(FetchRecord(
                    source_slug="findlaw_sections",
                    jurisdiction=config.jurisdiction,
                    law_code=config.law_code,
                    section=section,
                    url=url,
                    fetched_at=_now_iso(),
                    http_status=status,
                    bytes=len(html.encode("utf-8")),
                    content_sha256=sha,
                    raw_path=str(dest) if valid else None,
                    valid=valid,
                    title=title_text,
                ))

            except Exception as e:
                logger.error("  ERR section=%s %s", section, e)
                records.append(FetchRecord(
                    source_slug="findlaw_sections",
                    jurisdiction=config.jurisdiction,
                    law_code=config.law_code,
                    section=section,
                    url=url,
                    fetched_at=_now_iso(),
                    http_status=0,
                    bytes=0,
                    content_sha256="",
                    raw_path=None,
                    valid=False,
                    error=str(e),
                ))

        browser.close()

    with manifest_path.open("a") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec)) + "\n")

    valid_count = sum(1 for r in records if r.valid)
    logger.info(
        "done jurisdiction=%s code=%s total=%d valid=%d",
        config.jurisdiction, config.law_code, len(records), valid_count,
    )
    return records


def _setup_logging(log_path: Optional[Path], verbose: bool) -> None:
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
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, mode="a")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.info("file-log path=%s", log_path)


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="FindLaw statute section fetcher")
    p.add_argument("code", choices=list(CODES.keys()), help="Code to fetch (e.g. NY_VAT, TX_TRANSP)")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--force", action="store_true", help="Re-fetch even if HTML already on disk")
    p.add_argument("--delay", type=float, default=1.0, help="Seconds between page loads (default: 1.0)")
    p.add_argument("--limit", type=int, default=None, help="Cap number of sections to fetch")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--log-file", type=Path, default=Path("data/logs/findlaw_sections.log"))
    p.add_argument("--no-log-file", action="store_true")
    p.add_argument("--sections", nargs="+", help="Explicit section numbers to fetch")
    p.add_argument("--sections-file", type=Path, help="File with one section slug per line (default: auto-detect from data/)")
    p.add_argument("--range", nargs=2, type=int, metavar=("LO", "HI"), help="Integer range override")

    args = p.parse_args(argv if argv is not None else sys.argv[1:])
    config = CODES[args.code]

    _setup_logging(None if args.no_log_file else args.log_file, args.verbose)

    if args.sections:
        sections = args.sections
    elif args.sections_file:
        path: Path = args.sections_file
    else:
        default_sections_file = DEFAULT_OUT_DIR / f"{args.code}_sections.txt"
        if default_sections_file.exists():
            path = default_sections_file
            logger.info("auto-detected sections file: %s", path)
        else:
            path = None

    if not args.sections and path is not None:
        if not path.exists():
            raise SystemExit(f"sections file not found at {path}")
        with path.open() as f:
            secs = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
        sections = sorted(set(secs))
        logger.info("loaded %d sections from %s", len(sections), path)
    elif args.range:
        sections = [str(n) for n in range(args.range[0], args.range[1] + 1)]
    else:
        sections = config.expand_ranges()

    records = fetch_sections_playwright(
        config,
        sections,
        out_dir=args.out,
        force=args.force,
        delay=args.delay,
        limit=args.limit,
        verbose=args.verbose,
    )

    valid = sum(1 for r in records if r.valid)
    print(f"Fetched {len(records)} sections, {valid} valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())