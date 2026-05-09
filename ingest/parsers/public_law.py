"""
Parser for public.law HTML pages (works for both NY and TX).

Both sites share the same template with minor jurisdiction differences:

HTML structure:
  <body data-statute-number="404" data-parent-path="...">
  <h1 id="number_and_name">
    <span class="meta-name-and-number">N.Y. Vehicle & Traffic Law Section 404</span>
    <span id="name">Issuance of special number plates</span>
  </h1>
  <div id="leaf-statute-body">
    <section class="level-0 non-meta outline"><h2>1.</h2> ...</section>
    <section class="level-1 non-meta outline"><h3>(b)</h3> ...</section>
    <section class="level-2 non-meta outline"><h4>(A)</h4> ...</section>
    <section class="meta non-outline">Acts 1995, 74th Leg., ...</section>
  </div>

Breadcrumb (JSON-LD):
  NY: Laws > Vehicle & Traffic Law > Title 4 > Art. 14. Registration... > § 404
  TX: Statutes > Transp. Code > Title 3 > Chap. 21. Admin... > § 21.001

Source footer:
  <a id="footer-source-link" href="...">official source URL</a>

Cross-references:
  <a class="pragmatic" href="...">§ 401 (Registration of motor vehicles)</a>
  <a class="pedantic"  href="..." title="...">section four hundred one of this article</a>
  We keep pragmatic text, strip pedantic duplicates.

Public API:
    parse_public_law_html(html, *, jurisdiction, law_code, source_url, source_path) -> StatuteSection
    parse_public_law_file(path, *, jurisdiction, law_code, source_url) -> StatuteSection
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

from selectolax.parser import HTMLParser, Node

from .types import HistoryNote, ParseError, StatuteSection, Subsection


_BREADCRUMB_HIERARCHY_RE = re.compile(
    r"(?P<keyword>Title|Art\.|Chap\.|Subtitle|Part|Subchapter)\s+"
    r"(?P<ident>[\d]+[A-Za-z]?(?:\.\d+)?)"
    r"(?:\s*\.?\s+(?P<title>.*))?"
)

_WS_RE = re.compile(r"\s+")

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}

_KEYWORD_TO_KEY = {
    "Title": "TITLE",
    "Art.": "ARTICLE",
    "Chap.": "CHAPTER",
    "Subtitle": "SUBTITLE",
    "Part": "PART",
    "Subchapter": "SUBCHAPTER",
}


def parse_public_law_html(
    html: Union[str, bytes],
    *,
    jurisdiction: str = "NY",
    law_code: str = "VAT",
    source_url: Optional[str] = None,
    source_path: Optional[Path] = None,
) -> StatuteSection:
    if isinstance(html, bytes):
        raw_bytes = html
        html_text = html.decode("utf-8", errors="replace")
    else:
        raw_bytes = html.encode("utf-8", errors="replace")
        html_text = html

    sha = hashlib.sha256(raw_bytes).hexdigest()
    tree = HTMLParser(html_text)

    section_num = _extract_section_num(tree)
    section_name = _extract_section_name(tree)
    code_name = _extract_code_name(tree)
    hierarchy = _extract_breadcrumb_hierarchy(tree)
    canonical_url = _extract_canonical_url(tree) or source_url
    official_source_url = _extract_source_link(tree)
    subsections, history_raw = _extract_body(tree)
    last_modified = _extract_last_modified(tree)

    history = _parse_history(history_raw, jurisdiction) if history_raw else None
    # Fallback: synthesize a HistoryNote from the "Last modified" sidebar
    # if no inline meta history was found (common for NY pages).
    if history is None and last_modified is not None:
        history = HistoryNote(
            raw=f"Last modified: {last_modified}",
            action=None,
            statutes_year=None,
            chapter=None,
            section=None,
            bill_number=None,
            effective_date=_parse_date_str(last_modified),
            operative_date=None,
        )

    text = _render_text(subsections)
    markdown = _render_markdown(
        code_name=code_name,
        hierarchy=hierarchy,
        section_num=section_num,
        section_name=section_name,
        subsections=subsections,
        history=history,
    )

    parsed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    return StatuteSection(
        jurisdiction=jurisdiction,
        law_code=law_code,
        code_name=code_name,
        section_num=section_num,
        section_name=section_name,
        division=hierarchy.get("DIVISION", {}).get("ident"),
        division_title=hierarchy.get("DIVISION", {}).get("title"),
        division_range=hierarchy.get("DIVISION", {}).get("range"),
        chapter=hierarchy.get("CHAPTER", {}).get("ident"),
        chapter_title=hierarchy.get("CHAPTER", {}).get("title"),
        chapter_range=hierarchy.get("CHAPTER", {}).get("range"),
        article=hierarchy.get("ARTICLE", {}).get("ident"),
        article_title=hierarchy.get("ARTICLE", {}).get("title"),
        article_range=hierarchy.get("ARTICLE", {}).get("range"),
        part=hierarchy.get("PART", {}).get("ident"),
        part_title=hierarchy.get("PART", {}).get("title"),
        title=hierarchy.get("TITLE", {}).get("ident"),
        title_title=hierarchy.get("TITLE", {}).get("title"),
        text=text,
        markdown=markdown,
        subsections=subsections,
        history=history,
        source_url=canonical_url or official_source_url,
        source_path=str(source_path) if source_path else None,
        content_sha256=sha,
        parsed_at=parsed_at,
    )


def parse_public_law_file(
    path: Union[str, Path],
    *,
    jurisdiction: str = "NY",
    law_code: str = "VAT",
    source_url: Optional[str] = None,
) -> StatuteSection:
    path = Path(path)
    raw = path.read_bytes()
    return parse_public_law_html(
        raw,
        jurisdiction=jurisdiction,
        law_code=law_code,
        source_url=source_url,
        source_path=path,
    )


def _extract_section_num(tree: HTMLParser) -> str:
    body = tree.css_first("body")
    if body:
        num = body.attributes.get("data-statute-number", "")
        if num and num.strip():
            return num.strip()
    meta_tag = tree.css_first("span.meta-name-and-number")
    if meta_tag:
        text = _normalize_ws(meta_tag.text(strip=True))
        m = re.search(r"Section\s+([\d]+(?:\.[\d]+)?[A-Za-z]*(?:-[\d]+[A-Za-z]*)?)", text)
        if m:
            return m.group(1)
    h1 = tree.css_first("#number_and_name")
    if h1:
        text = _normalize_ws(h1.text(strip=True))
        m = re.search(r"Section\s+([\d]+(?:\.[\d]+)?[A-Za-z]*(?:-[\d]+[A-Za-z]*)?)", text)
        if m:
            return m.group(1)
    raise ParseError("Could not extract section number from public.law page")


def _extract_section_name(tree: HTMLParser) -> Optional[str]:
    name_span = tree.css_first("#name")
    if name_span:
        text = _normalize_ws(name_span.text(strip=True))
        return text if text else None
    return None


def _extract_code_name(tree: HTMLParser) -> Optional[str]:
    meta_tag = tree.css_first("span.meta-name-and-number")
    if meta_tag:
        text = _normalize_ws(meta_tag.text(separator=" ", strip=True))
        m = re.match(r"^(?:N\.Y\.|Tex\.)\s+(.+?)\s+Section\s+", text)
        if m:
            return m.group(1).strip()
    return None


def _extract_breadcrumb_hierarchy(tree: HTMLParser) -> dict[str, dict[str, str]]:
    hierarchy: dict[str, dict[str, str]] = {}
    ld_script = None
    for script in tree.css("script[type='application/ld+json']"):
        inner = script.text(strip=True)
        if "BreadcrumbList" in inner:
            ld_script = inner
            break

    if not ld_script:
        return hierarchy

    try:
        data = json.loads(ld_script)
    except json.JSONDecodeError:
        return hierarchy

    items = data.get("itemListElement", [])
    for item in items:
        name = item.get("name", "")
        if not name:
            continue
        name = name.strip()
        m = _BREADCRUMB_HIERARCHY_RE.match(name)
        if not m:
            continue

        kw = m.group("keyword")
        ident = m.group("ident")
        title = _normalize_ws(m.group("title")) if m.group("title") else None

        key = _KEYWORD_TO_KEY.get(kw)
        if key:
            hierarchy[key] = {"ident": ident, "title": title, "range": None}

    return hierarchy


def _extract_canonical_url(tree: HTMLParser) -> Optional[str]:
    link = tree.css_first('link[rel="canonical"]')
    if link:
        href = link.attributes.get("href", "")
        if href:
            return href
    return None


def _extract_source_link(tree: HTMLParser) -> Optional[str]:
    link = tree.css_first("#footer-source-link")
    if link:
        href = link.attributes.get("href", "")
        if href:
            return href
    return None


def _extract_last_modified(tree: HTMLParser) -> Optional[str]:
    """Find 'Last modified: <Date>' in the right sidebar card."""
    for p in tree.css("p.card-text"):
        strong = p.css_first("strong")
        if strong and "Last modified" in strong.text(strip=True):
            text = _normalize_ws(p.text(separator=" ", strip=True))
            m = re.search(r"([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4})", text)
            if m:
                return m.group(1)
    return None


def _get_class_set(node: Node) -> set[str]:
    raw = node.attributes.get("class", "") or ""
    return set(raw.split())


def _section_text_without_headers(section: Node) -> str:
    """Extract clean body text from a <section>:

    1. Re-parse the section HTML in isolation (so destructive ops don't
       affect the parent tree).
    2. Strip <h2>..<h6> heading elements (we capture those as labels).
    3. Strip <a class="pedantic"> cross-reference duplicates.
    4. Return whitespace-normalized text.
    """
    section_html = section.html
    if not section_html:
        return ""

    sub_tree = HTMLParser(section_html)
    for a in sub_tree.css("a.pedantic"):
        a.decompose()
    for h in sub_tree.css("h2, h3, h4, h5, h6"):
        h.decompose()

    body = sub_tree.body or sub_tree.root
    if body is None:
        return ""
    return _normalize_ws(body.text(separator=" ", strip=True))


def _section_label(section: Node) -> Optional[str]:
    for h_tag in section.css("h2, h3, h4, h5, h6"):
        h_text = _normalize_ws(h_tag.text(strip=True))
        m = re.match(r"^\(([^)]+)\)\s*$", h_text)
        if m:
            return m.group(1)
        m2 = re.match(r"^(\d+(?:\.\d+)*)\.?\s*$", h_text)
        if m2:
            return m2.group(1)
        m3 = re.match(r"^(\d+)\.$", h_text)
        if m3:
            return m3.group(1)
        break
    return None


def _extract_body(
    tree: HTMLParser,
) -> tuple[list[Subsection], Optional[str]]:
    body_div = tree.css_first("#leaf-statute-body")
    if body_div is None:
        raise ParseError("No #leaf-statute-body div found in public.law page")

    subsections: list[Subsection] = []
    history_raw: Optional[str] = None

    for section in body_div.css("section"):
        class_set = _get_class_set(section)
        is_meta = "meta" in class_set and "non-meta" not in class_set
        level_match = re.search(r"level-(\d+)", section.attributes.get("class", "") or "")
        level = int(level_match.group(1)) if level_match else 0

        if is_meta:
            text = _normalize_ws(section.text(strip=True))
            if text:
                history_raw = text
            continue

        text = _section_text_without_headers(section)
        if not text:
            continue

        label = _section_label(section)
        subsections.append(Subsection(label=label, text=text, depth_em=float(level)))

    if not subsections:
        all_text = _normalize_ws(body_div.text(separator="\n", strip=True))
        if all_text:
            source_idx = all_text.find("Source:")
            if source_idx > 0:
                all_text = all_text[:source_idx].strip()
            subsections.append(Subsection(label=None, text=all_text, depth_em=0.0))

    return subsections, history_raw


def _parse_history(raw: str, jurisdiction: str) -> HistoryNote:
    inner = raw.strip()

    action: Optional[str] = None
    m_action = re.match(
        r"^(Amended|Added|Enacted|Repealed|Renumbered)\s+(by\s+)?",
        inner,
        re.IGNORECASE,
    )
    if m_action:
        action = m_action.group(1).capitalize()

    statutes_year: Optional[int] = None
    chapter: Optional[str] = None
    section: Optional[str] = None
    bill_number: Optional[str] = None
    effective_date: Optional[str] = None
    operative_date: Optional[str] = None

    # TX format: "Acts 1995, 74th Leg., ch. 165, Sec. 1, eff. Sept. 1, 1995."
    #            "Amended by Acts 2003, 78th Leg., ch. 1325, § 19.014, eff. Jan. 1, 2004."
    m_tx = re.search(
        r"Acts\s+(\d{4})(?:,\s*\d+(?:st|nd|rd|th)?)?\s+Leg\.,\s*"
        r"(?:Ch\.|Chapter)\s*([\w.]+),\s*"
        r"(?:Sec\.|§)\s*([\w.]+)",
        inner,
        re.IGNORECASE,
    )
    if m_tx:
        try:
            statutes_year = int(m_tx.group(1))
        except (TypeError, ValueError):
            pass
        chapter = m_tx.group(2)
        section = m_tx.group(3)
        if action is None:
            action = "Enacted"
    else:
        m_year = re.search(r"(\d{4})", inner)
        if m_year:
            try:
                statutes_year = int(m_year.group(1))
            except ValueError:
                pass

    # Effective date: "eff. Sept. 1, 1995" or "Effective January 1, 2023"
    m_eff_short = re.search(
        r"(?:eff\.|Effective)\s+([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4})",
        inner,
    )
    if m_eff_short:
        effective_date = _parse_date_str(m_eff_short.group(1))

    m_bill = re.search(r"\(([A-Z]+\s+\d+[A-Z]*)\)", inner)
    if m_bill:
        bill_number = _normalize_ws(m_bill.group(1))

    return HistoryNote(
        raw=raw,
        action=action,
        statutes_year=statutes_year,
        chapter=chapter,
        section=section,
        bill_number=bill_number,
        effective_date=effective_date,
        operative_date=operative_date,
    )


def _render_text(subsections: list[Subsection]) -> str:
    lines: list[str] = []
    for s in subsections:
        if s.label:
            lines.append(f"({s.label}) {s.text}")
        else:
            lines.append(s.text)
    return "\n\n".join(lines)


def _render_markdown(
    *,
    code_name: Optional[str],
    hierarchy: dict[str, dict[str, str]],
    section_num: str,
    section_name: Optional[str],
    subsections: list[Subsection],
    history: Optional[HistoryNote],
) -> str:
    parts: list[str] = []

    if code_name:
        parts.append(f"# {code_name}")

    for level, key in [(2, "TITLE"), (3, "SUBTITLE"), (3, "CHAPTER"), (4, "ARTICLE"), (4, "SUBCHAPTER"), (4, "PART")]:
        info = hierarchy.get(key)
        if not info:
            continue
        line = f"{'#' * level} {key.title()} {info.get('ident', '')}"
        title = info.get("title")
        if title:
            line += f". {title}"
        parts.append(line.strip())

    heading = f"##### § {section_num}"
    if section_name:
        heading += f" — {section_name}"
    parts.append(heading)
    parts.append("")

    body_lines: list[str] = []
    for s in subsections:
        indent = "    " * int(s.depth_em) if s.depth_em else ""
        if s.label:
            body_lines.append(f"{indent}({s.label}) {s.text}")
        else:
            body_lines.append(f"{indent}{s.text}")
    parts.append("\n\n".join(body_lines))

    if history:
        parts.append("")
        parts.append(f"*{history.raw}*")

    return "\n".join(parts).strip() + "\n"


def _parse_date_str(s: str) -> Optional[str]:
    s = s.strip().rstrip(".")
    parts = s.replace(",", " ").split()
    if len(parts) < 3:
        return None

    month_name = parts[0].lower().rstrip(".")
    month = _MONTHS.get(month_name)
    if not month:
        return None

    try:
        day = int(parts[1])
        year = int(parts[2])
    except (ValueError, IndexError):
        return None

    return f"{year:04d}-{month:02d}-{day:02d}"


def _normalize_ws(s: Optional[str]) -> str:
    if not s:
        return ""
    return _WS_RE.sub(" ", s).strip()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage: python -m ingest.parsers.public_law <jurisdiction> <law_code> <path-to-html>")
        sys.exit(2)
    jurisdiction = sys.argv[1]
    law_code = sys.argv[2]
    path = Path(sys.argv[3])
    rec = parse_public_law_file(path, jurisdiction=jurisdiction, law_code=law_code)
    import json

    print(json.dumps(rec.model_dump(), indent=2, ensure_ascii=False))