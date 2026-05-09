"""
Parser for California leginfo per-section HTML pages.

Each page is ~160KB of JSF chrome wrapping a small, predictable content
block. The actual statute lives in:

    <div id="codeLawSectionNoHead">
        <h4>Vehicle Code - VEH</h4>
        <h4>DIVISION 11. RULES OF THE ROAD [21000 - 23336]</h4><i>...</i>
        <h4>CHAPTER 2. Traffic Signs, Signals, and Markings [21350 - 21468]</h4><i>...</i>
        <h5>ARTICLE 3. Offenses Relating to Traffic Devices [21450 - 21468]</h5><i>...</i>
        <font face="Times New Roman">
            <h6>21453.</h6>
            <p>(a) ...</p>
            <p>(b) ...</p>
            ...
            <i>(Amended by Stats. 2022, Ch. 957, Sec. 3.   (AB 2147)   Effective January 1, 2023.)</i>
        </font>
    </div>

Heading levels (<h4>/<h5>/<h6>) are NOT reliable indicators of hierarchy
— different sections use different levels for the same logical role.
We classify headings by their leading keyword instead
(DIVISION, CHAPTER, ARTICLE, PART, TITLE).

Authoritative metadata (lawCode, sectionNum, op_statues, op_chapter,
op_section, article) is also embedded in the page's printPopup()
JavaScript block; we sniff that with a regex as a cross-check.

Public API:
    parse_section_html(html: str | bytes, *, source_url=None, source_path=None) -> StatuteSection
    parse_section_file(path: Path, *, source_url=None) -> StatuteSection
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

from selectolax.parser import HTMLParser, Node

from .types import HistoryNote, ParseError, StatuteSection, Subsection


# ---------------------------------------------------------------------------
# Constants & regexes
# ---------------------------------------------------------------------------

LEGINFO_SECTION_URL_TEMPLATE = (
    "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml"
    "?sectionNum={section}.&lawCode={law_code}"
)

# JS-block metadata (cheap and authoritative).
_JS_META_PATTERNS = {
    "law_code": re.compile(r"var\s+lawCode\s*=\s*'([^']+)'"),
    "section_num": re.compile(r"var\s+sectionNum\s*=\s*'([^']+)'"),
    "op_statues": re.compile(r"var\s+op_statues\s*=\s*'([^']*)'"),
    "op_chapter": re.compile(r"var\s+op_chapter\s*=\s*'([^']*)'"),
    "op_section": re.compile(r"var\s+op_section\s*=\s*'([^']*)'"),
    "article_js": re.compile(r"var\s+article\s*=\s*'([^']*)'"),
}

# Pull the leginfo nodeTreePath from any of the JSF onclick payloads
# (e.g. "...'nodeTreePath':'15.2.3'..."). Useful for finding sibling sections.
_NODE_TREE_RE = re.compile(r"'nodeTreePath'\s*:\s*'([^']+)'")

# Identifies a hierarchy heading. Captures: keyword, ident, title, range.
# Examples:
#   "DIVISION 11. RULES OF THE ROAD [21000 - 23336]"
#   "CHAPTER 2. Traffic Signs, Signals, and Markings [21350 - 21468]"
#   "ARTICLE 3. Offenses Relating to Traffic Devices [21450 - 21468]"
#   "PART 2. ..."
#   "TITLE 1. ..."
_HIERARCHY_RE = re.compile(
    r"""^\s*
        (?P<keyword>DIVISION|CHAPTER|ARTICLE|PART|TITLE)
        \s+
        (?P<ident>[\d]+(?:\.\d+)?[A-Za-z]?)
        \.?\s+
        (?P<title>.+?)
        (?:\s*\[\s*(?P<range>[^\]]+?)\s*\])?
        \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Section number heading inside <h6>: "21453." -> "21453"
_SECTION_NUM_RE = re.compile(r"^\s*([\d]+(?:\.\d+)?[A-Za-z]?)\s*\.?\s*$")

# Subsection labels at the start of a paragraph:
#   (a)  -> a
#   (1)  -> 1
#   (A)  -> A
#   (i)  -> i (rare, usually inside subsections)
_SUBSECTION_LABEL_RE = re.compile(r"^\s*\(([A-Za-z]+|\d+)\)\s*")

# margin-left in a <p style="..."> indicates indentation depth.
_MARGIN_LEFT_RE = re.compile(r"margin-left\s*:\s*([\d.]+)\s*em")

# History note inside the trailing <i>...</i> looks like:
#   "(Amended by Stats. 2022, Ch. 957, Sec. 3.   (AB 2147)   Effective January 1, 2023.)"
#   "(Enacted by Stats. 1959, Ch. 3.)"
#   "(Added by Stats. 1999, Ch. 722, Sec. 5.   Effective January 1, 2000.)"
_HISTORY_OUTER_RE = re.compile(r"^\s*\((?P<inner>.+)\)\s*$", re.DOTALL)
_HISTORY_ACTION_RE = re.compile(
    r"^(?P<action>Enacted|Amended|Added|Repealed|Renumbered)\b",
    re.IGNORECASE,
)
_HISTORY_STATS_RE = re.compile(
    r"Stats\.\s*(?P<year>\d{4}),\s*Ch\.\s*(?P<chapter>[\w.]+?)"
    r"(?:,\s*Sec\.\s*(?P<section>[\w.]+))?\.",
)
_HISTORY_BILL_RE = re.compile(r"\(\s*([A-Z]+\s*\d+[A-Z]*)\s*\)")
_HISTORY_EFFECTIVE_RE = re.compile(
    r"Effective\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s*\d{4})",
)
_HISTORY_OPERATIVE_RE = re.compile(
    r"Operative\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s*\d{4})",
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

_WS_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def parse_section_html(
    html: Union[str, bytes],
    *,
    source_url: Optional[str] = None,
    source_path: Optional[Path] = None,
) -> StatuteSection:
    """Parse a leginfo per-section HTML page into a StatuteSection.

    Raises:
        ParseError: if the page does not contain a parseable statute body
            (e.g. it's the "section does not exist" template, or the layout
            has changed in an unrecognized way).
    """
    if isinstance(html, bytes):
        raw_bytes = html
        html_text = html.decode("utf-8", errors="replace")
    else:
        raw_bytes = html.encode("utf-8", errors="replace")
        html_text = html

    sha = hashlib.sha256(raw_bytes).hexdigest()

    # Cheap metadata sweep on the raw text (works even if HTML is malformed).
    js_meta = _extract_js_metadata(html_text)
    node_tree_path = _extract_node_tree_path(html_text)

    tree = HTMLParser(html_text)
    content = tree.css_first("#codeLawSectionNoHead")
    if content is None:
        raise ParseError(
            "No #codeLawSectionNoHead div found — page is likely the "
            "'section does not exist' template or the layout changed."
        )

    code_name, hierarchy = _parse_headers(content)
    section_num, paragraphs, history_raw = _parse_body(content)

    if not section_num:
        # Fall back to JS metadata if the <h6> wasn't where we expected.
        js_section = js_meta.get("section_num", "").rstrip(".")
        if js_section:
            section_num = js_section
        else:
            raise ParseError("Could not find section number heading (<h6>).")

    # Cross-check law code: prefer JS, fall back to the title text "Vehicle Code - VEH".
    law_code = js_meta.get("law_code")
    if not law_code and code_name and " - " in code_name:
        law_code = code_name.rsplit(" - ", 1)[-1].strip()
    if not law_code:
        # Last-resort: try the URL or the input file path.
        law_code = _infer_law_code_from_path(source_path) or "UNKNOWN"

    history = _parse_history(history_raw) if history_raw else None

    subsections = _build_subsections(paragraphs)
    text = _render_text(subsections)
    markdown = _render_markdown(
        code_name=code_name,
        hierarchy=hierarchy,
        section_num=section_num,
        subsections=subsections,
        history=history,
    )

    parsed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    return StatuteSection(
        jurisdiction="CA",
        law_code=law_code,
        code_name=_clean_code_name(code_name),
        section_num=section_num,
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
        node_tree_path=node_tree_path,
        text=text,
        markdown=markdown,
        subsections=subsections,
        history=history,
        op_statues=js_meta.get("op_statues") or None,
        op_chapter=js_meta.get("op_chapter") or None,
        op_section=js_meta.get("op_section") or None,
        source_url=source_url
        or LEGINFO_SECTION_URL_TEMPLATE.format(section=section_num, law_code=law_code),
        source_path=str(source_path) if source_path else None,
        content_sha256=sha,
        parsed_at=parsed_at,
    )


def parse_section_file(
    path: Union[str, Path],
    *,
    source_url: Optional[str] = None,
) -> StatuteSection:
    """Read a leginfo HTML file from disk and parse it."""
    path = Path(path)
    raw = path.read_bytes()
    return parse_section_html(raw, source_url=source_url, source_path=path)


# ---------------------------------------------------------------------------
# Metadata extraction (regex on raw HTML — independent of DOM parse)
# ---------------------------------------------------------------------------


def _extract_js_metadata(html_text: str) -> dict[str, str]:
    """Pull lawCode/sectionNum/op_statues/op_chapter/op_section/article from printPopup() JS.

    Returns a dict with whatever keys we found. Empty strings (e.g.
    `var op_section = '';`) are preserved as empty strings — callers
    should treat them as missing.
    """
    out: dict[str, str] = {}
    for key, pattern in _JS_META_PATTERNS.items():
        m = pattern.search(html_text)
        if m:
            value = m.group(1).strip()
            # Normalize trailing dots on section numbers / article ("21453." -> "21453").
            if key in ("section_num", "article_js"):
                value = value.rstrip(".")
            out[key if key != "article_js" else "article"] = value
    return out


def _extract_node_tree_path(html_text: str) -> Optional[str]:
    m = _NODE_TREE_RE.search(html_text)
    return m.group(1) if m else None


def _infer_law_code_from_path(path: Optional[Path]) -> Optional[str]:
    """If the file lives at .../{LAW_CODE}/{section}.html, return LAW_CODE."""
    if path is None:
        return None
    parent = path.parent.name
    if parent and parent.isupper() and parent.isalpha():
        return parent
    return None


# ---------------------------------------------------------------------------
# Header / hierarchy parsing
# ---------------------------------------------------------------------------


def _parse_headers(content: Node) -> tuple[Optional[str], dict[str, dict[str, str]]]:
    """Walk h4/h5 headings in the content div, classify by leading keyword.

    Returns:
        (code_name_text, {"DIVISION": {ident, title, range}, "CHAPTER": ..., "ARTICLE": ...})

    The first <h4> is treated as the code-name banner (e.g.
    "Vehicle Code - VEH") and is returned separately. Headings whose
    text doesn't start with DIVISION/CHAPTER/ARTICLE/PART/TITLE are
    ignored (this also handles the "General Provisions" lead-in
    heading, which has no logical hierarchy slot).
    """
    code_name: Optional[str] = None
    hierarchy: dict[str, dict[str, str]] = {}

    # Headings inside #codeLawSectionNoHead. We accept <h4>/<h5>/<h6>; the
    # section-number <h6> is filtered out below by structure (it lives inside
    # the <font> body wrapper).
    for h in content.css("h4, h5"):
        # Skip headings nested inside the body <font> wrapper (those are
        # the section number heading).
        if _has_ancestor_tag(h, "font"):
            continue

        text = _normalize_ws(h.text(strip=True))
        if not text:
            continue

        if code_name is None:
            # The first heading we see is the code banner.
            code_name = text
            continue

        m = _HIERARCHY_RE.match(text)
        if not m:
            # Non-keyword heading like "General Provisions" — skip; the
            # banner+section-num pair is enough to identify the section.
            continue

        keyword = m.group("keyword").upper()
        hierarchy[keyword] = {
            "ident": m.group("ident"),
            "title": _normalize_ws(m.group("title")),
            "range": _normalize_ws(m.group("range")) if m.group("range") else None,
        }

    return code_name, hierarchy


def _has_ancestor_tag(node: Node, tag_name: str) -> bool:
    parent = node.parent
    while parent is not None:
        if (parent.tag or "").lower() == tag_name:
            return True
        parent = parent.parent
    return False


def _clean_code_name(raw: Optional[str]) -> Optional[str]:
    """'Vehicle Code - VEH' -> 'Vehicle Code'."""
    if not raw:
        return raw
    # Strip the trailing " - ABBR" if present.
    parts = raw.rsplit(" - ", 1)
    if len(parts) == 2 and parts[1].isupper() and len(parts[1]) <= 6:
        return parts[0].strip()
    return raw.strip()


# ---------------------------------------------------------------------------
# Body parsing (section number + paragraphs + history note)
# ---------------------------------------------------------------------------


def _parse_body(
    content: Node,
) -> tuple[Optional[str], list[tuple[str, float]], Optional[str]]:
    """Extract the section number, list of (paragraph_text, depth_em), and
    raw history note string from inside the body <font> wrapper.

    The body wrapper is `<font face="Times New Roman">` containing:
        <h6>21453.</h6>
        <p>...</p>
        <p>...</p>
        ...
        <i>(Amended by ...)</i>

    If the <font> wrapper isn't there (rare — observed in some malformed
    pages) we fall back to scanning the whole content div.
    """
    body = content.css_first("font")
    scope: Node = body if body is not None else content

    # 1. Section number from the <h6> inside the body.
    section_num: Optional[str] = None
    h6 = scope.css_first("h6")
    if h6 is not None:
        m = _SECTION_NUM_RE.match(_normalize_ws(h6.text(strip=True)))
        if m:
            section_num = m.group(1)

    # 2. All paragraphs inside the body, in order, with their margin-left.
    paragraphs: list[tuple[str, float]] = []
    for p in scope.css("p"):
        # Defensive: skip <p> tags nested inside something we wouldn't expect
        # (none observed in practice but cheap to guard).
        text = _normalize_ws(p.text(strip=True))
        if not text:
            continue
        depth = _extract_margin_left_em(p.attributes.get("style", "") or "")
        paragraphs.append((text, depth))

    # 3. The history note: the last <i> under the body wrapper.
    #    (There are also <i> tags on Division/Chapter/Article enactment
    #    notes, but those live OUTSIDE the body <font>, so scoping to
    #    `scope` keeps them out.)
    history_raw: Optional[str] = None
    italics = scope.css("i")
    if italics:
        # Take the last italic with a non-empty body — that's the trailer.
        for i_node in reversed(italics):
            t = _normalize_ws(i_node.text(strip=True))
            if t:
                history_raw = t
                break

    return section_num, paragraphs, history_raw


def _extract_margin_left_em(style: str) -> float:
    if not style:
        return 0.0
    m = _MARGIN_LEFT_RE.search(style)
    if not m:
        return 0.0
    try:
        return float(m.group(1))
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Subsection labelling
# ---------------------------------------------------------------------------


def _build_subsections(paragraphs: list[tuple[str, float]]) -> list[Subsection]:
    """Detect leading (a)/(b)/(1)/(A) labels on each paragraph.

    We strip ONLY the first label, even if a paragraph starts with
    multiple (e.g. "(a)(1)..."), so the remaining label survives in the
    text. This keeps the flat list lossless.
    """
    out: list[Subsection] = []
    for text, depth in paragraphs:
        m = _SUBSECTION_LABEL_RE.match(text)
        if m:
            label = m.group(1)
            stripped = text[m.end():].lstrip()
            out.append(Subsection(label=label, text=stripped, depth_em=depth))
        else:
            out.append(Subsection(label=None, text=text, depth_em=depth))
    return out


# ---------------------------------------------------------------------------
# History parsing
# ---------------------------------------------------------------------------


def _parse_history(raw: str) -> HistoryNote:
    """Decompose the trailing italic note.

    The HTML looks like '(Amended by Stats. 2022, Ch. 957, Sec. 3.   (AB 2147)   Effective January 1, 2023.)'
    after .text() unwraps it. We strip the outer parens, then run
    targeted regexes.
    """
    inner = raw
    m_outer = _HISTORY_OUTER_RE.match(raw)
    if m_outer:
        inner = m_outer.group("inner").strip()

    action: Optional[str] = None
    m_action = _HISTORY_ACTION_RE.match(inner)
    if m_action:
        action = m_action.group("action").capitalize()

    statutes_year: Optional[int] = None
    chapter: Optional[str] = None
    section: Optional[str] = None
    m_stats = _HISTORY_STATS_RE.search(inner)
    if m_stats:
        try:
            statutes_year = int(m_stats.group("year"))
        except (TypeError, ValueError):
            pass
        chapter = m_stats.group("chapter")
        section = m_stats.group("section")

    bill_number: Optional[str] = None
    m_bill = _HISTORY_BILL_RE.search(inner)
    if m_bill:
        bill_number = _normalize_ws(m_bill.group(1))

    effective_date: Optional[str] = None
    m_eff = _HISTORY_EFFECTIVE_RE.search(inner)
    if m_eff:
        effective_date = _to_iso_date(m_eff.group("date"))

    operative_date: Optional[str] = None
    m_op = _HISTORY_OPERATIVE_RE.search(inner)
    if m_op:
        operative_date = _to_iso_date(m_op.group("date"))

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


def _to_iso_date(human: str) -> Optional[str]:
    """'January 1, 2023' -> '2023-01-01'. Returns None on parse failure."""
    if not human:
        return None
    parts = human.replace(",", " ").split()
    if len(parts) < 3:
        return None
    month_name = parts[0].lower()
    month = _MONTHS.get(month_name)
    if not month:
        return None
    try:
        day = int(parts[1])
        year = int(parts[2])
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render_text(subsections: list[Subsection]) -> str:
    """Produce a clean, embedding-friendly plain text body."""
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
    subsections: list[Subsection],
    history: Optional[HistoryNote],
) -> str:
    """Render the section as readable Markdown with hierarchy as headings.

    Used for LLM context, debugging, and human inspection.
    """
    parts: list[str] = []

    if code_name:
        parts.append(f"# {_clean_code_name(code_name)}")

    for level, keyword in [(2, "DIVISION"), (3, "CHAPTER"), (4, "ARTICLE")]:
        info = hierarchy.get(keyword)
        if not info:
            continue
        line = f"{'#' * level} {keyword.title()} {info.get('ident', '')}"
        title = info.get("title")
        if title:
            line += f". {title}"
        rng = info.get("range")
        if rng:
            line += f" [{rng}]"
        parts.append(line.strip())

    parts.append(f"##### § {section_num}")
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


# ---------------------------------------------------------------------------
# Misc utilities
# ---------------------------------------------------------------------------


def _normalize_ws(s: Optional[str]) -> str:
    if not s:
        return ""
    # Collapse runs of whitespace (incl. embedded newlines/tabs from the soft
    # line-breaks leginfo inserts inside paragraphs) into single spaces.
    return _WS_RE.sub(" ", s).strip()


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import json
    import sys

    if len(sys.argv) != 2:
        print("Usage: python -m ingest.parsers.ca_leginfo <path-to-html>")
        sys.exit(2)
    rec = parse_section_file(sys.argv[1])
    print(json.dumps(rec.model_dump(), indent=2, ensure_ascii=False))
