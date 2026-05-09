"""
Pydantic types for parsed statute documents.

These describe the *structural* output of a deterministic HTML parse —
not the targets of an LLM extraction. Schemas in `extract/schemas.py`
remain reserved for LLM-extracted, source-tracked facts.

Fields are intentionally Optional where the source HTML may not provide
them (some sections have no Division/Chapter/Article hierarchy at all).
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ParseError(Exception):
    """Raised when an HTML page cannot be parsed as a statute section.

    Common causes:
        - The `#codeLawSectionNoHead` content div is missing (page is the
          "section does not exist" template, or the layout changed).
        - The section number `<h6>` could not be found.
    """


class HistoryNote(BaseModel):
    """Parsed legislative history line (the trailing italic note).

    Examples of `raw`:
        "(Amended by Stats. 2022, Ch. 957, Sec. 3.   (AB 2147)   Effective January 1, 2023.)"
        "(Enacted by Stats. 1959, Ch. 3.)"
        "(Added by Stats. 1999, Ch. 722, Sec. 5.   Effective January 1, 2000.)"
    """

    raw: str = Field(
        description="The full italic history string as it appears in the HTML, parens trimmed."
    )
    action: Optional[str] = Field(
        None,
        description="Verb describing the action: 'Enacted', 'Amended', 'Added', 'Repealed', etc.",
    )
    statutes_year: Optional[int] = Field(
        None,
        description="Year of the chaptering statutes session, e.g. 2022.",
    )
    chapter: Optional[str] = Field(
        None,
        description="Stats. chapter number (string — chapters can have suffixes).",
    )
    section: Optional[str] = Field(
        None,
        description="Stats. section number within the chapter, e.g. '3' or '6.5'.",
    )
    bill_number: Optional[str] = Field(
        None,
        description="Originating bill identifier, e.g. 'AB 2147' or 'SB 814'.",
    )
    effective_date: Optional[str] = Field(
        None,
        description="Effective date as ISO YYYY-MM-DD if parseable, else None.",
    )
    operative_date: Optional[str] = Field(
        None,
        description="Operative date as ISO YYYY-MM-DD if parseable (often differs from effective).",
    )


class Subsection(BaseModel):
    """A single labelled paragraph within a statute section.

    The list is *flat*: nested subsections like (a)(1)(A) appear as
    separate entries with their leading label captured. The original
    indentation depth (margin-left in CSS) is preserved as `depth_em`
    for clients that want to reconstruct hierarchy themselves.
    """

    label: Optional[str] = Field(
        None,
        description=(
            "Leading subsection label as it appears, e.g. 'a', 'b', '1', 'A'. "
            "None for unlabeled lead-in paragraphs."
        ),
    )
    text: str = Field(
        description="Plain-text body with the leading label stripped, whitespace normalized."
    )
    depth_em: float = Field(
        0.0,
        description=(
            "CSS margin-left (in em) on the source <p>. 0 = top level, "
            "1 = (1)/(2) sub-level, 2.5 = (A)/(B), etc. Heuristic only."
        ),
    )


class StatuteSection(BaseModel):
    """A single CA statute section, fully parsed from its leginfo HTML page."""

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------
    jurisdiction: str = Field(
        "CA",
        description="ISO-style jurisdiction code; CA for California state code.",
    )
    law_code: str = Field(
        description="Code abbreviation, e.g. 'VEH', 'PEN', 'HSC'.",
    )
    code_name: Optional[str] = Field(
        None,
        description="Full code name as printed, e.g. 'Vehicle Code'.",
    )
    section_num: str = Field(
        description="Section number as it appears, e.g. '21453' or '21451.5'.",
    )
    section_name: Optional[str] = Field(
        None,
        description=(
            "Catchline / short title for the section, e.g. "
            "'Issuance of special number plates'. Empty for CA leginfo "
            "(which doesn't print catchlines), populated for NY/TX public.law."
        ),
    )

    # ------------------------------------------------------------------
    # Hierarchy (all optional — many sections have only a subset)
    # ------------------------------------------------------------------
    division: Optional[str] = Field(None, description="Division identifier, e.g. '11'.")
    division_title: Optional[str] = Field(
        None, description="Division title, e.g. 'RULES OF THE ROAD'."
    )
    division_range: Optional[str] = Field(
        None, description="Bracketed range, e.g. '21000 - 23336'."
    )

    chapter: Optional[str] = Field(None, description="Chapter identifier, e.g. '2'.")
    chapter_title: Optional[str] = Field(
        None, description="Chapter title, e.g. 'Traffic Signs, Signals, and Markings'."
    )
    chapter_range: Optional[str] = Field(None, description="Chapter section range.")

    article: Optional[str] = Field(None, description="Article identifier, e.g. '3'.")
    article_title: Optional[str] = Field(
        None, description="Article title, e.g. 'Offenses Relating to Traffic Devices'."
    )
    article_range: Optional[str] = Field(None, description="Article section range.")

    part: Optional[str] = Field(None, description="Part identifier, if present.")
    part_title: Optional[str] = Field(None, description="Part title, if present.")

    title: Optional[str] = Field(
        None, description="Title identifier (rare; some codes use it).",
    )
    title_title: Optional[str] = Field(None, description="Title heading text.")

    node_tree_path: Optional[str] = Field(
        None,
        description="leginfo's internal nodeTreePath, e.g. '15.2.3'. Useful for sibling lookups.",
    )

    # ------------------------------------------------------------------
    # Body
    # ------------------------------------------------------------------
    text: str = Field(
        description="Clean plain-text body (no HTML, normalized whitespace, paragraphs joined by '\\n\\n')."
    )
    markdown: str = Field(
        description="Markdown rendering: hierarchy as headings, paragraphs as text, history as italic footer."
    )
    subsections: list[Subsection] = Field(
        default_factory=list,
        description="Flat list of paragraphs with leading subsection labels detected.",
    )

    # ------------------------------------------------------------------
    # Provenance
    # ------------------------------------------------------------------
    history: Optional[HistoryNote] = Field(
        None,
        description="Parsed trailing italic note describing chaptering / amendment.",
    )
    op_statues: Optional[str] = Field(
        None,
        description="Raw 'op_statues' year from the page's printPopup() JS (e.g. '2022').",
    )
    op_chapter: Optional[str] = Field(
        None, description="Raw 'op_chapter' from the page's printPopup() JS."
    )
    op_section: Optional[str] = Field(
        None, description="Raw 'op_section' from the page's printPopup() JS."
    )
    source_url: Optional[str] = Field(
        None, description="Permalink to the leginfo page this was parsed from."
    )
    source_path: Optional[str] = Field(
        None, description="Local filesystem path to the raw HTML, if known."
    )
    content_sha256: str = Field(
        description="SHA256 hex digest of the raw input bytes — for cache invalidation."
    )
    parsed_at: str = Field(
        description="ISO-8601 UTC timestamp of when the parse was produced."
    )
    parser_version: str = Field(
        "1",
        description="Schema/parser version; bump when the parser meaningfully changes.",
    )
