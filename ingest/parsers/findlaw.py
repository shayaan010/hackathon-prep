"""
Parser for FindLaw per-section HTML pages (NY, TX, etc.).

Each page has a consistent structure:

    <div class="title">
        New York Consolidated Laws, Vehicle and Traffic Law - VAT &sect; 1146. Drivers to exercise due care
    </div>
    <div class="codes-content">
        <div class="subsection"><p>(a) ...</p></div>
        <div class="subsection"><p>(b) ...</p></div>
        ...
    </div>

The title div gives us: jurisdiction, code name, law_code, section_num, section title.
Subsection divs give us the body text with labels.

Public API:
    parse_findlaw_html(html, jurisdiction, law_code, ...) -> StatuteSection
    parse_findlaw_file(path, ...) -> StatuteSection
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

from selectolax.parser import HTMLParser, Node

from .types import HistoryNote, ParseError, StatuteSection, Subsection


_SUBSECTION_LABEL_RE = re.compile(r"^\s*\(([A-Za-z]+|\d+)\)\s*")
_WS_RE = re.compile(r"\s+")


def _normalize_text(text: str) -> str:
    return _WS_RE.sub(" ", text).strip()


def _parse_title(title_text: str) -> dict:
    """Parse the title div text into components.

    Examples:
        "New York Consolidated Laws, Vehicle and Traffic Law - VAT § 1146. Drivers to exercise due care"
        "Texas Transportation Code - TRANSP § 545.001. Definitions"
    """
    result = {}
    m = re.match(r"(.+?)\s*[-–]\s*(\w+)\s*[§\xa7]+\s*([\d]+(?:\.\d+)?)\.?\s*(.*)", title_text.strip())
    if m:
        result["code_name"] = m.group(1).strip()
        result["law_code"] = m.group(2).strip()
        result["section_num"] = m.group(3).strip().rstrip(".")
        result["section_title"] = m.group(4).strip()
    else:
        m2 = re.match(r".*?(\w{2,})\s+[§§]\s*([\d.]+)", title_text.strip())
        if m2:
            result["law_code"] = m2.group(1).strip()
            result["section_num"] = m2.group(2).strip()
        result["code_name"] = title_text.strip()
    return result


def _extract_subsections(content: Node) -> list[Subsection]:
    """Extract subsections from the codes-content div.

    FindLaw nests <div class="subsection"> inside other subsection divs.
    We only parse *top-level* subsection divs (direct children of codes-content)
    and recursively walk their inner <p> and <div class="subsection"> children,
    assigning depth based on nesting level.
    """
    subs = []

    all_subsections = content.css("div.subsection")
    top_level = [
        node for node in all_subsections
        if node.parent and (node.parent.attributes.get("class", "") or "") == "codes-content section"
    ]
    if not top_level:
        top_level = all_subsections[:1] if all_subsections else []

    _SUBSECTION_LABEL_RE_LOCAL = _SUBSECTION_LABEL_RE

    def _walk(node: Node, depth: float):
        for child in node.iter():
            if child.tag == "p":
                text = child.text(strip=True, separator=" ")
                text = _normalize_text(text)
                if not text:
                    continue
                label = None
                m = _SUBSECTION_LABEL_RE_LOCAL.match(text)
                if m:
                    label = m.group(1)
                    text = _SUBSECTION_LABEL_RE_LOCAL.sub("", text, count=1).strip()
                subs.append(Subsection(label=label, text=text, depth_em=depth))
            elif child.tag == "div" and "subsection" in (child.attributes.get("class", "") or ""):
                _walk(child, depth + 1.0)

    for node in top_level:
        _walk(node, 0.0)

    return subs


def _extract_history(content: Node) -> Optional[HistoryNote]:
    """Try to find a history/source note after the subsections."""
    all_p = content.css("p")
    for p in all_p:
        text = p.text(strip=True, separator=" ")
        if not text:
            continue
        cls = (p.attributes.get("class", "") or "").lower()
        if "cite-this" in cls or "cookie" in cls:
            continue
        m = re.match(
            r"\(?\s*(Amended|Enacted|Added|Repealed|Effected)\s+"
            r"(?:by\s+)?(?:L\.?\s*\d{4}|Ch\.?\s*\d+|Stats\.?\s*\d+|Laws\s+\d{4})",
            text,
            re.IGNORECASE,
        )
        if m:
            raw = text.strip()
            if raw.startswith("(") and raw.endswith(")"):
                raw = raw[1:-1].strip()
            action = m.group(1)
            year_m = re.search(r"\b(19\d{2}|20\d{2})\b", raw)
            return HistoryNote(
                raw=raw,
                action=action,
                statutes_year=int(year_m.group(1)) if year_m else None,
            )
    return None


def _build_text(subsections: list[Subsection]) -> str:
    """Reconstruct full plain text from subsections."""
    parts = []
    for sub in subsections:
        if sub.label:
            parts.append(f"({sub.label}) {sub.text}")
        else:
            parts.append(sub.text)
    return "\n\n".join(parts)


def _build_markdown(
    code_name: str,
    section_num: str,
    section_title: str,
    subsections: list[Subsection],
) -> str:
    """Build markdown from parsed data."""
    lines = [f"# {code_name}", f"## § {section_num}. {section_title}", ""]
    for sub in subsections:
        indent = "    " * int(sub.depth_em)
        if sub.label:
            lines.append(f"{indent}({sub.label}) {sub.text}")
        else:
            lines.append(f"{indent}{sub.text}")
        lines.append("")
    return "\n".join(lines)


def parse_findlaw_html(
    html: Union[str, bytes],
    *,
    jurisdiction: str,
    law_code: str,
    source_url: Optional[str] = None,
    source_path: Optional[str] = None,
) -> StatuteSection:
    """Parse a FindLaw statute section HTML page.

    Args:
        html: Raw HTML string or bytes.
        jurisdiction: e.g. 'NY' or 'TX'.
        law_code: e.g. 'VAT' or 'TRANSP'.
        source_url: Original URL of the page.
        source_path: Local path to the HTML file.

    Returns:
        StatuteSection with parsed data.

    Raises:
        ParseError: If the page doesn't contain valid statute content.
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="ignore")

    tree = HTMLParser(html)

    content = tree.css_first("div.codes-content")
    if content is None:
        raise ParseError("No div.codes-content found — page is not a valid statute section")

    title_div = tree.css_first("div.title")
    if title_div is None:
        raise ParseError("No div.title found — page is not a valid statute section")

    title_text = title_div.text(strip=True)
    parsed_title = _parse_title(title_text)

    section_num = parsed_title.get("section_num", "")
    section_title = parsed_title.get("section_title", "")
    code_name = parsed_title.get("code_name", law_code)
    parsed_law_code = parsed_title.get("law_code", law_code)

    subsections = _extract_subsections(content)
    history = _extract_history(content)

    text = _build_text(subsections)
    markdown = _build_markdown(code_name, section_num, section_title, subsections)

    sha = hashlib.sha256(html.encode("utf-8", "ignore")).hexdigest()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    return StatuteSection(
        jurisdiction=jurisdiction,
        law_code=parsed_law_code,
        code_name=code_name,
        section_num=section_num,
        division=None,
        division_title=None,
        division_range=None,
        chapter=None,
        chapter_title=None,
        chapter_range=None,
        article=None,
        article_title=None,
        article_range=None,
        part=None,
        part_title=None,
        title=None,
        title_title=None,
        node_tree_path=None,
        text=text,
        markdown=markdown,
        subsections=[s.model_dump() for s in subsections],
        history=history.model_dump() if history else None,
        source_url=source_url,
        source_path=str(source_path) if source_path else None,
        content_sha256=sha,
        parsed_at=now,
        parser_version="1",
    )


def parse_findlaw_file(
    path: Path,
    *,
    jurisdiction: str,
    law_code: str,
    source_url: Optional[str] = None,
) -> StatuteSection:
    """Parse a FindLaw HTML file on disk."""
    html = path.read_text(encoding="utf-8", errors="ignore")
    return parse_findlaw_html(
        html,
        jurisdiction=jurisdiction,
        law_code=law_code,
        source_url=source_url,
        source_path=path,
    )