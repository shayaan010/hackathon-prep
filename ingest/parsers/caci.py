"""
CACI parser: PDF -> per-instruction JSON + inverted indexes keyed by statute.

Input
-----
A single CACI annual PDF as fetched by ``ingest.sources.caci_pdfs``,
e.g. ``data/raw/caci_pdfs/2026/caci_2026.pdf``.

The PDF (Judicial Council of California Civil Jury Instructions) is a
~3,500-page document with an embedded outline tree.  Each leaf-outline
entry whose title matches ``NNNN. Title`` (optionally followed by a
parenthetical like ``(Veh. Code, § 22350)``) is one jury instruction.

Output
------
``data/parsed/caci/{edition}/instructions/CACI-NNNN.json``
    One file per instruction.  Schema below.

``data/parsed/caci/{edition}/index/by_statute.jsonl``
    Inverted index: one row per (statute_key, instruction_id) edge,
    with surrounding context.  ``statute_key`` is
    ``{JURISDICTION}/{CODE}/{SECTION}`` — e.g. ``CA/VEH/22350`` —
    which joins directly to the existing CA leginfo statute corpus.

``data/parsed/caci/{edition}/index/by_case.jsonl``
    One row per (case_key, instruction_id) edge.  ``case_key`` is the
    canonical citation form ``"{vol} {reporter} {page}"``.

``data/parsed/caci/{edition}/index/summary.json``
    Aggregate counts: total instructions, statutes cited, top statutes.

``data/parsed/caci/{edition}/parse_manifest.jsonl``
    One line per instruction processed.

Per-instruction JSON schema
---------------------------
{
  "id": "CACI-705",
  "edition": "2026",
  "number": "705",
  "title": "Turning",
  "title_full": "Turning (Veh. Code, § 22107)",
  "title_statute": {"jurisdiction":"CA","code":"VEH","section":"22107"},
  "series": "700",
  "series_title": "Series 700 Vehicles and Highway Safety",
  "page_start": 628,
  "page_end": 629,
  "instruction_text": "...",            # what is read to the jury
  "directions_for_use": "...",
  "sources_and_authority": [             # one item per bullet
    {
      "raw": "Turning and Changing Lanes. Vehicle Code section 22107.",
      "statutes": [{"jurisdiction":"CA","code":"VEH","section":"22107", ...}],
      "cases": []
    },
    ...
  ],
  "secondary_sources": "...",
  "all_statutes": [...],                # union over title + sources
  "all_cases": [...]
}
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pypdf


SOURCE_SLUG = "caci"
JURISDICTION = "CA"
DEFAULT_RAW_ROOT = Path("data/raw/caci_pdfs")
DEFAULT_OUT_ROOT = Path("data/parsed/caci")

logger = logging.getLogger(SOURCE_SLUG)


# ---------------------------------------------------------------------------
# Code abbreviations -> canonical 3- or 4-letter code names
# (matching the lawCode values used by leginfo for joins)
# ---------------------------------------------------------------------------

# Each entry: list of regex patterns -> canonical code key.
# Patterns are matched case-insensitively.  Longest prefix wins.
# We allow optional intra-word whitespace (PDF kerning artifact:
# 'V ehicle' for 'Vehicle') by inserting \s* between every two letters
# only for full-word forms; abbreviated forms are stable.
CA_CODES_RAW: list[tuple[list[str], str, str]] = [
    # (patterns, canonical, display)
    (["Code of Civil Procedure", r"Code\s*Civ\.\s*Proc\.", r"Code\s+Civ\.\s+Proc\."], "CCP",  "Code of Civil Procedure"),
    (["Civil",                   r"Civ\."],                                            "CIV",  "Civil"),
    (["Vehicle",                 r"Veh\."],                                            "VEH",  "Vehicle"),
    (["Penal",                   r"Pen\."],                                            "PEN",  "Penal"),
    (["Evidence",                r"Evid\."],                                           "EVID", "Evidence"),
    (["Government",              r"Gov\.",        r"Gov't"],                           "GOV",  "Government"),
    (["Health and Safety",       r"Health\s*&\s*Saf\.",  "Health & Saf."],             "HSC",  "Health and Safety"),
    (["Business and Professions", r"Bus\.\s*&\s*Prof\.", "Bus. & Prof."],              "BPC",  "Business and Professions"),
    (["Labor",                   r"Lab\."],                                            "LAB",  "Labor"),
    (["Probate",                 r"Prob\."],                                           "PROB", "Probate"),
    (["Insurance",               r"Ins\."],                                            "INS",  "Insurance"),
    (["Family",                  r"Fam\."],                                            "FAM",  "Family"),
    (["Welfare and Institutions", r"Welf\.\s*&\s*Inst\.", "Welf. & Inst."],            "WIC",  "Welfare and Institutions"),
    (["Education",               r"Educ\.",  r"Ed\."],                                 "EDC",  "Education"),
    (["Corporations",            r"Corp\."],                                           "CORP", "Corporations"),
    (["Commercial",              r"Com\."],                                            "COM",  "Commercial"),
    (["Public Resources",        r"Pub\.\s*Res\.",   "Pub. Res."],                     "PRC",  "Public Resources"),
    (["Public Utilities",        r"Pub\.\s*Util\.",  "Pub. Util."],                    "PUC",  "Public Utilities"),
    (["Streets and Highways",    r"Sts\.\s*&\s*High\.", "Sts. & High."],               "SHC",  "Streets and Highways"),
    (["Unemployment Insurance",  r"Unemp\.\s*Ins\."],                                  "UIC",  "Unemployment Insurance"),
    (["Revenue and Taxation",    r"Rev\.\s*&\s*Tax\.", "Rev. & Tax."],                 "RTC",  "Revenue and Taxation"),
    (["Fish and Game",                                                            ],   "FGC",  "Fish and Game"),
    (["Food and Agricultural", "Food and Agriculture"],                                "FAC",  "Food and Agricultural"),
    (["Harbors and Navigation"],                                                       "HNC",  "Harbors and Navigation"),
    (["Military and Veterans"],                                                        "MVC",  "Military and Veterans"),
    (["Public Contract",         r"Pub\.\s*Cont\.",  "Pub. Cont."],                    "PCC",  "Public Contract"),
    (["Water",                   r"Wat\."],                                            "WAT",  "Water"),
    (["Elections",               r"Elec\."],                                           "ELEC", "Elections"),
    (["Financial",               r"Fin\."],                                            "FIN",  "Financial"),
]


def _expand_kern(name: str) -> str:
    """Allow PDF kerning artifact: insert optional \\s* after each
    capital letter that begins a multi-letter word, so 'Vehicle' also
    matches 'V ehicle'."""
    # Only for word-form names (those starting with a capital letter,
    # not abbreviations with periods).
    if "\\." in name or name.endswith("."):
        return name
    # Insert \s* after the first letter of each capitalized word.
    def fix(m: re.Match) -> str:
        return m.group(1) + r"\s*" + m.group(2)
    return re.sub(r"\b([A-Z])([a-z])", fix, name)


def _build_code_pattern() -> tuple[re.Pattern[str], dict[str, tuple[str, str]]]:
    """Construct the master statute-citation regex and a lookup table
    from matched-prefix-text -> (canonical_code, display_name)."""
    alt_terms: list[str] = []
    canon_lookup: dict[str, tuple[str, str]] = {}
    for raw_patterns, canon, display in CA_CODES_RAW:
        for raw in raw_patterns:
            # If it looks like a regex (contains \), use as-is; else escape
            # plain text.
            if "\\" in raw:
                pat = raw
            else:
                pat = re.escape(raw)
            pat = _expand_kern(pat)
            alt_terms.append(pat)
            canon_lookup[raw.lower()] = (canon, display)
    # Sort by length descending so longer prefixes match first
    # (e.g. "Code of Civil Procedure" before "Civil").
    alt_terms.sort(key=len, reverse=True)
    alt = "|".join(f"(?:{a})" for a in alt_terms)
    # Match: <code-name> Code(,)? (section|sections|§|§§) <section-num>
    # The section can have subsections: 22350, 22350.5, 22350(a), 22350.5(a)(1)
    # And lists: "section 22350, 22351"
    pattern = re.compile(
        r"\b(?P<code_name>" + alt + r")\s+Code\s*,?\s*"
        r"(?:section|sections|§|§§)\s*"
        r"(?P<section>\d+(?:\.\d+)?(?:\s*\([a-zA-Z0-9]+\))*"
        r"(?:\s*,\s*(?:and\s+)?\d+(?:\.\d+)?(?:\s*\([a-zA-Z0-9]+\))*)*)",
        re.IGNORECASE,
    )
    return pattern, canon_lookup


STATUTE_RE, _CANON_LOOKUP = _build_code_pattern()

# Look up canonical for an arbitrary matched code name.  We compare
# against the lookup keys, ignoring whitespace and case.
def _canon_from_match(matched_text: str) -> Optional[tuple[str, str]]:
    norm = re.sub(r"\s+", " ", matched_text.strip().lower())
    if norm in _CANON_LOOKUP:
        return _CANON_LOOKUP[norm]
    # Strip kerning artifacts: collapse spaces inside words.
    collapsed = re.sub(r"(?<=[a-z])\s+(?=[a-z])", "", matched_text.strip().lower())
    if collapsed in _CANON_LOOKUP:
        return _CANON_LOOKUP[collapsed]
    # Try prefix match against patterns.  Slow but list is small.
    for raw_patterns, canon, display in CA_CODES_RAW:
        for raw in raw_patterns:
            test = re.escape(raw) if "\\" not in raw else raw
            test = _expand_kern(test)
            if re.fullmatch(test, matched_text.strip(), re.IGNORECASE):
                return (canon, display)
    return None


# Section-list parser: explodes "22350, 22351, and 22352(a)" into individuals.
_SECTION_PIECE_RE = re.compile(
    r"(?P<num>\d+(?:\.\d+)?)(?P<subs>(?:\s*\([a-zA-Z0-9]+\))*)"
)


def _split_sections(blob: str) -> list[tuple[str, list[str]]]:
    """('22350, 22351, and 22352(a)') -> [('22350',[]), ('22351',[]), ('22352',['a'])]."""
    out = []
    for m in _SECTION_PIECE_RE.finditer(blob):
        num = m.group("num")
        subs_text = m.group("subs") or ""
        subs = re.findall(r"\(([a-zA-Z0-9]+)\)", subs_text)
        out.append((num, subs))
    return out


# ---------------------------------------------------------------------------
# Case citation regex
# ---------------------------------------------------------------------------

# Reporters we care about (CA + Federal common ones).
# We anchor on (year) volume reporter page  which is a very strong signal.
# PDF extraction may insert spaces between Cal.|App. or other reporter parts,
# so allow optional whitespace between every component.
_REPORTER_ALT = (
    # California: Cal., Cal.App., Cal.2d, Cal.4th, Cal. App. 2d, Cal.App.5th, etc.
    r"Cal\.(?:\s*App\.)?(?:\s*\d(?:d|st|nd|rd|th))?(?:\s*Supp\.)?"
    # U.S. Supreme Court
    r"|U\.\s*S\."
    # Federal Reporter & supplements
    r"|F\.(?:\s*\d(?:d|st|nd|rd|th))?(?:\s*Supp\.(?:\s*\d(?:d|st|nd|rd|th))?)?"
    # State / regional reporters occasionally cited in CACI
    r"|N\.\s*Y\.(?:\s*\d(?:d|st|nd|rd|th))?"
    r"|N\.\s*W\.(?:\s*\d(?:d|st|nd|rd|th))?"
    r"|S\.\s*W\.(?:\s*\d(?:d|st|nd|rd|th))?"
    r"|N\.\s*E\.(?:\s*\d(?:d|st|nd|rd|th))?"
    r"|S\.\s*E\.(?:\s*\d(?:d|st|nd|rd|th))?"
    r"|P\.(?:\s*\d(?:d|st|nd|rd|th))?"
    r"|A\.(?:\s*\d(?:d|st|nd|rd|th))?"
    # Cal.Rptr. (treated as primary in some older cites)
    r"|Cal\.\s*Rptr\.(?:\s*\d(?:d|st|nd|rd|th))?"
)

CASE_RE = re.compile(
    # Party-A v. Party-B (year) vol Reporter page[, pin]
    # Allow line breaks inside party names (PDF wraps lines).
    r"(?P<name>[A-Z][\w'’.\-]+"
    r"(?:[\s\n]+(?:[A-Z][\w'’.\-]*|of|the|and|de|la|del|von)\.?)*"
    r"(?:,?[\s\n]+(?:Inc\.|LLC|Co\.|Corp\.|Ltd\.|L\.P\.|L\.L\.C\.|Co))?)"
    r"\s+v\.\s+"
    r"(?P<name2>[A-Z][\w'’.\-]+"
    r"(?:[\s\n]+(?:[A-Z][\w'’.\-]*|of|the|and|de|la|del|von)\.?)*"
    r"(?:,?[\s\n]+(?:Inc\.|LLC|Co\.|Corp\.|Ltd\.|L\.P\.|L\.L\.C\.|Co))?)"
    r"\s*\((?P<year>\d{4})\)\s+"
    r"(?P<vol>\d+)\s+"
    r"(?P<reporter>" + _REPORTER_ALT + r")"
    r"\s+(?P<page>\d+)"
    r"(?:,\s*(?P<pin>\d+(?:[\-–]\d+)?))?",
    re.UNICODE,
)


# ---------------------------------------------------------------------------
# PDF text post-processing
# ---------------------------------------------------------------------------

# Common PDF kerning fixes that simplify downstream regex.
_KERN_FIXES = [
    (re.compile(r"\bV\s+ehicle\b"),    "Vehicle"),
    (re.compile(r"\bC\s+ivil\b"),      "Civil"),
    (re.compile(r"\bP\s*\.\s*(\d)"),   r"P.\1"),
    (re.compile(r"\bCal\s*\.\s*"),     "Cal. "),
    (re.compile(r"§\s+(\d)"),          r"§\1"),
    (re.compile(r"\u2014|\u2013"),     "–"),  # normalize dashes
    (re.compile(r"[\u2018\u2019]"),    "'"),  # smart single quotes
    (re.compile(r"[\u201C\u201D]"),    '"'),  # smart double quotes
    (re.compile(r"\u00A0"),            " "),  # nbsp
]


def _normalize_pdf_text(s: str) -> str:
    for pat, repl in _KERN_FIXES:
        s = pat.sub(repl, s)
    # Collapse runs of whitespace except newlines (preserve line structure).
    s = re.sub(r"[ \t]+", " ", s)
    return s


# ---------------------------------------------------------------------------
# Outline walking
# ---------------------------------------------------------------------------

# CACI numbers can be: '700', '700A', 'VF-705', 'CACI No. ...'.
# We accept anything starting with one or more digits, optional letter suffix,
# then '.'  Followed by anything.  VF-* (verdict forms) we also collect.
_INSTRUCTION_TITLE_RE = re.compile(
    r"^\s*(?P<num>\d{1,4}[A-Z]?|VF-\d{1,4}[A-Z]?)\.\s+(?P<title>.+?)\s*$"
)
# Title parenthetical that contains a primary statute citation:
# '705. Turning (Veh. Code, § 22107)'.
_TITLE_PAREN_RE = re.compile(r"\(([^()]+?)\)\s*$")


def _collect_outline_leaves(items: Any, out: Optional[list] = None) -> list:
    if out is None:
        out = []
    for it in items:
        if isinstance(it, list):
            _collect_outline_leaves(it, out)
        else:
            out.append(it)
    return out


def _series_from_number(num: str) -> str:
    """'705' -> '700'. 'VF-705' -> 'VF-700'."""
    if num.startswith("VF-"):
        rest = num[3:]
    else:
        rest = num
    digits = re.match(r"^(\d+)", rest).group(1)
    n = int(digits)
    bucket = (n // 100) * 100
    prefix = "VF-" if num.startswith("VF-") else ""
    return f"{prefix}{bucket}"


# ---------------------------------------------------------------------------
# Per-instruction parser
# ---------------------------------------------------------------------------

# Section header markers (all on their own line in extracted text).
_HEADER_ALIASES = {
    "directions for use": "directions_for_use",
    "sources and authority": "sources_and_authority",
    "secondary sources": "secondary_sources",
}
_HEADER_RE = re.compile(
    r"^\s*(Directions for Use|Sources and Authority|Secondary Sources)\s*$",
    re.MULTILINE,
)
# Body / Directions delimiter: a "New <Date>" or "New ...; Revised <Date>" line.
_NEW_REVISED_RE = re.compile(
    r"^\s*(?:New|Revised|Renumbered).+?\d{4}.*?$",
    re.MULTILINE,
)


def _split_instruction_sections(text: str, title: str) -> dict[str, str]:
    """Split a single-instruction text blob into named sections.

    Keys: ``instruction_text``, ``directions_for_use``,
    ``sources_and_authority``, ``secondary_sources``.  Missing sections
    are empty strings.
    """
    # Strip the leading title line (it'll be the first line ~= title).
    # We do this defensively - the outline title can drift slightly from
    # the in-page title because of line wraps.
    t = text
    first_nl = t.find("\n")
    if first_nl > 0 and t[:first_nl].strip().startswith(title.split(".")[0]):
        t = t[first_nl + 1 :]

    # Find all header positions.
    headers: list[tuple[int, str]] = []
    for m in _HEADER_RE.finditer(t):
        canon = _HEADER_ALIASES[m.group(1).lower()]
        headers.append((m.start(), canon))

    # The pre-Directions chunk is the body (instruction_text + any "New ..." note).
    out = {
        "instruction_text": "",
        "directions_for_use": "",
        "sources_and_authority": "",
        "secondary_sources": "",
    }
    if not headers:
        out["instruction_text"] = t.strip()
        return out

    # Body is everything before the first header.
    first_header_pos = headers[0][0]
    body = t[:first_header_pos]
    # Strip the trailing 'New <date>' or 'Revised ...' line(s) into
    # implicit metadata; we keep the prose above.
    body = _NEW_REVISED_RE.sub("", body).strip()
    out["instruction_text"] = body

    # Subsequent slices.
    for i, (pos, canon) in enumerate(headers):
        end = headers[i + 1][0] if i + 1 < len(headers) else len(t)
        # Skip the header line itself.
        line_end = t.find("\n", pos)
        if line_end < 0 or line_end > end:
            line_end = pos
        out[canon] = t[line_end + 1 : end].strip()
    return out


# Bullets in Sources and Authority are prefixed with U+2022.  Some bullets
# wrap onto multiple lines.
_BULLET_RE = re.compile(r"(?:^|\n)\s*•\s*")


def _split_bullets(blob: str) -> list[str]:
    if not blob:
        return []
    parts = _BULLET_RE.split(blob)
    # Discard the pre-first-bullet header noise.
    parts = [p.strip() for p in parts if p.strip()]
    return parts


# ---------------------------------------------------------------------------
# Citation extractors
# ---------------------------------------------------------------------------


def extract_statutes(text: str) -> list[dict]:
    """Find every CA-code statute citation in ``text``.

    Returns a list of dicts:
        {jurisdiction, code, section, subsection (optional), display, raw, span}

    Section lists ('22350, 22351') are exploded so each yields its own
    record.
    """
    out: list[dict] = []
    for m in STATUTE_RE.finditer(text):
        prefix = m.group("code_name")
        canon = _canon_from_match(prefix)
        if canon is None:
            continue
        canon_code, display = canon
        sections_blob = m.group("section")
        for num, subs in _split_sections(sections_blob):
            base = {
                "jurisdiction": JURISDICTION,
                "code": canon_code,
                "section": num,
                "display": f"{display} Code section {num}",
                "raw": m.group(0),
                "span": [m.start(), m.end()],
            }
            if not subs:
                out.append(base)
            else:
                # Emit one record per subsection AND a base record so
                # both granularities are findable.
                out.append({**base})
                for sub in subs:
                    out.append({**base, "subsection": sub,
                                "display": f"{display} Code section {num}({sub})"})
    return out


def extract_cases(text: str) -> list[dict]:
    out: list[dict] = []
    for m in CASE_RE.finditer(text):
        # Collapse intra-name whitespace (PDF line wraps) so the case name
        # round-trips cleanly.
        name1 = re.sub(r"\s+", " ", m.group("name")).strip()
        name2 = re.sub(r"\s+", " ", m.group("name2")).strip()
        # Reporter: strip duplicate spaces and the kerning artifact between
        # 'Cal.' and 'App.' to canonicalize.
        reporter_raw = re.sub(r"\s+", " ", m.group("reporter")).strip()
        # Canonical reporter form: 'Cal. App.2d' -> 'Cal.App.2d',
        # 'Cal. 4th' -> 'Cal.4th'.
        reporter = re.sub(r"\.\s+", ".", reporter_raw)
        out.append({
            "name": f"{name1} v. {name2}",
            "year": int(m.group("year")),
            "volume": int(m.group("vol")),
            "reporter": reporter,
            "page": int(m.group("page")),
            "pin": m.group("pin"),
            "raw": re.sub(r"\s+", " ", m.group(0)).strip(),
            "span": [m.start(), m.end()],
        })
    return out


def parse_title(title_full: str) -> tuple[str, Optional[dict]]:
    """'Turning (Veh. Code, § 22107)' -> ('Turning', {VEH/22107}).

    Returns (clean_title, primary_statute_or_None).
    """
    m = _TITLE_PAREN_RE.search(title_full)
    if not m:
        return title_full.strip(), None
    inside = m.group(1)
    statutes = extract_statutes(inside)
    if not statutes:
        return title_full.strip(), None
    clean = title_full[: m.start()].strip().rstrip(",")
    primary = statutes[0]
    return clean, primary


# ---------------------------------------------------------------------------
# Main parse
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_pdf(
    pdf_path: Path,
    edition: str,
    out_root: Path,
    *,
    log: Optional[logging.Logger] = None,
    limit: Optional[int] = None,
) -> dict:
    """Drive the full parse: PDF -> per-instruction JSON files + indexes."""
    log = log or logger
    out_dir = out_root / edition
    instr_dir = out_dir / "instructions"
    index_dir = out_dir / "index"
    instr_dir.mkdir(parents=True, exist_ok=True)
    index_dir.mkdir(parents=True, exist_ok=True)

    log.info("opening %s", pdf_path)
    reader = pypdf.PdfReader(str(pdf_path))
    n_pages = len(reader.pages)
    log.info("pdf      pages=%d edition=%s", n_pages, edition)

    # Walk the outline: collect leaf-level instruction entries with page
    # ranges (start = this entry's page; end = next leaf's page).
    leaves = _collect_outline_leaves(reader.outline)

    # Build a map of series-bucket -> series_title from the parents.
    # The outline is a tree; we re-walk it to capture parent context.
    series_title_for: dict[str, str] = {}

    def _walk_with_parents(items: Any, parents: tuple[str, ...] = ()) -> None:
        for it in items:
            if isinstance(it, list):
                continue
            title = it.title or ""
            children_idx = items.index(it) + 1
            children = (items[children_idx]
                        if children_idx < len(items)
                        and isinstance(items[children_idx], list)
                        else [])
            if children:
                _walk_with_parents(children, parents + (title,))

    _walk_with_parents(reader.outline)

    # Simpler: parents are the "Series NNN ..." entries.  Pre-pass: find
    # all titles that look like series headers and record start pages.
    series_starts: list[tuple[int, str]] = []
    for it in leaves:
        title = (it.title or "").strip()
        m = re.match(r"^Series\s+(\d{2,4})\b", title)
        if m:
            page = reader.get_destination_page_number(it)
            series_starts.append((page, title))

    series_starts.sort()

    def _series_title_for_page(page: int) -> Optional[str]:
        cur = None
        for start, title in series_starts:
            if start <= page:
                cur = title
            else:
                break
        return cur

    instructions: list[dict] = []
    for i, it in enumerate(leaves):
        title_text = (it.title or "").strip()
        m = _INSTRUCTION_TITLE_RE.match(title_text)
        if not m:
            continue
        num = m.group("num")
        title_full = m.group("title")

        try:
            sp = reader.get_destination_page_number(it)
        except Exception:
            continue
        # Find next leaf's page for the upper bound.
        ep = n_pages
        for j in range(i + 1, len(leaves)):
            try:
                ep_j = reader.get_destination_page_number(leaves[j])
            except Exception:
                continue
            if ep_j > sp:
                ep = ep_j
                break

        instructions.append({
            "number": num,
            "title_full": title_full,
            "page_start": sp,
            "page_end": ep,
            "series_title": _series_title_for_page(sp),
        })

    log.info("outline  instructions=%d", len(instructions))
    if limit is not None:
        instructions = instructions[:limit]
        log.info("limit    truncated to %d", len(instructions))

    manifest_path = out_dir / "parse_manifest.jsonl"
    by_statute_path = index_dir / "by_statute.jsonl"
    by_case_path = index_dir / "by_case.jsonl"

    by_statute_counts: Counter[str] = Counter()
    by_code_counts: Counter[str] = Counter()
    instructions_with_statutes = 0

    with manifest_path.open("w", encoding="utf-8") as mf, \
         by_statute_path.open("w", encoding="utf-8") as sf, \
         by_case_path.open("w", encoding="utf-8") as cf:

        for ins in instructions:
            num = ins["number"]
            iid = f"CACI-{num}"
            text_chunks: list[str] = []
            for p in range(ins["page_start"], ins["page_end"]):
                if p < 0 or p >= n_pages:
                    continue
                try:
                    text_chunks.append(reader.pages[p].extract_text() or "")
                except Exception as e:
                    log.warning("extract-error iid=%s page=%d err=%s", iid, p, e)
            raw_text = "\n".join(text_chunks)
            text = _normalize_pdf_text(raw_text)

            # Title cleanup + primary-statute from title parenthetical.
            clean_title, title_statute = parse_title(ins["title_full"])

            # Section split.
            sections = _split_instruction_sections(text, title=ins["title_full"])

            # Bullet-level extraction in Sources and Authority.
            sources_bullets: list[dict] = []
            for bullet_text in _split_bullets(sections["sources_and_authority"]):
                sources_bullets.append({
                    "raw": bullet_text,
                    "statutes": extract_statutes(bullet_text),
                    "cases": extract_cases(bullet_text),
                })

            # Aggregates.
            all_statutes: list[dict] = []
            all_cases: list[dict] = []
            seen_stat_keys: set[str] = set()
            seen_case_keys: set[str] = set()

            def add_statute(s: dict) -> None:
                key = f"{s['jurisdiction']}/{s['code']}/{s['section']}"
                if "subsection" in s:
                    key += f"/{s['subsection']}"
                if key in seen_stat_keys:
                    return
                seen_stat_keys.add(key)
                all_statutes.append({**s, "statute_key": key})

            def add_case(c: dict) -> None:
                key = f"{c['volume']} {c['reporter']} {c['page']}"
                if key in seen_case_keys:
                    return
                seen_case_keys.add(key)
                all_cases.append({**c, "case_key": key})

            if title_statute is not None:
                title_statute_with_role = {**title_statute, "role": "title"}
                add_statute(title_statute_with_role)
            for b in sources_bullets:
                for s in b["statutes"]:
                    add_statute({**s, "role": "sources"})
                for c in b["cases"]:
                    add_case({**c, "role": "sources"})
            # Also extract from directions_for_use - these are real
            # references too.
            for s in extract_statutes(sections["directions_for_use"]):
                add_statute({**s, "role": "directions"})
            for c in extract_cases(sections["directions_for_use"]):
                add_case({**c, "role": "directions"})

            ins_record = {
                "id": iid,
                "edition": edition,
                "number": num,
                "title": clean_title,
                "title_full": ins["title_full"],
                "title_statute": title_statute,
                "series": _series_from_number(num),
                "series_title": ins["series_title"],
                "page_start": ins["page_start"] + 1,  # 1-indexed for humans
                "page_end": ins["page_end"],
                "instruction_text": sections["instruction_text"],
                "directions_for_use": sections["directions_for_use"],
                "sources_and_authority": sources_bullets,
                "secondary_sources": sections["secondary_sources"],
                "all_statutes": all_statutes,
                "all_cases": all_cases,
                "parsed_at": _now_iso(),
            }
            (instr_dir / f"{iid}.json").write_text(
                json.dumps(ins_record, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            # Manifest line.
            mf.write(json.dumps({
                "id": iid,
                "edition": edition,
                "number": num,
                "title": clean_title,
                "page_start": ins["page_start"] + 1,
                "page_end": ins["page_end"],
                "n_sources": len(sources_bullets),
                "n_statutes": len(all_statutes),
                "n_cases": len(all_cases),
                "has_title_statute": title_statute is not None,
            }, ensure_ascii=False) + "\n")
            mf.flush()

            if all_statutes:
                instructions_with_statutes += 1

            # Inverted indexes.
            for s in all_statutes:
                key = s["statute_key"]
                by_statute_counts[key] += 1
                by_code_counts[s["code"]] += 1
                # Find the bullet text where this citation appeared,
                # for context.  Falls back to title.
                context_text = ""
                if s.get("role") == "title":
                    context_text = ins["title_full"]
                else:
                    for b in sources_bullets:
                        if any(bs["section"] == s["section"]
                               and bs["code"] == s["code"]
                               for bs in b["statutes"]):
                            context_text = b["raw"]
                            break
                sf.write(json.dumps({
                    "statute_key": key,
                    "jurisdiction": s["jurisdiction"],
                    "code": s["code"],
                    "section": s["section"],
                    "subsection": s.get("subsection"),
                    "instruction_id": iid,
                    "instruction_number": num,
                    "instruction_title": clean_title,
                    "role": s.get("role", "sources"),
                    "context": context_text[:600] if context_text else None,
                }, ensure_ascii=False) + "\n")

            for c in all_cases:
                cf.write(json.dumps({
                    "case_key": c["case_key"],
                    "case_name": c["name"],
                    "year": c["year"],
                    "volume": c["volume"],
                    "reporter": c["reporter"],
                    "page": c["page"],
                    "pin": c.get("pin"),
                    "instruction_id": iid,
                    "instruction_number": num,
                    "instruction_title": clean_title,
                    "role": c.get("role", "sources"),
                }, ensure_ascii=False) + "\n")

    # Summary
    summary = {
        "edition": edition,
        "pdf_path": str(pdf_path),
        "n_instructions": len(instructions),
        "instructions_with_statutes": instructions_with_statutes,
        "unique_statute_keys": len(by_statute_counts),
        "total_statute_edges": sum(by_statute_counts.values()),
        "by_code_counts": dict(by_code_counts.most_common()),
        "top_statute_keys": [
            {"statute_key": k, "count": v}
            for k, v in by_statute_counts.most_common(50)
        ],
        "parsed_at": _now_iso(),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("done     %s", json.dumps({
        k: v for k, v in summary.items() if k != "top_statute_keys"
    }))
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Parse a CACI PDF into per-instruction JSON + statute index.",
    )
    p.add_argument("--edition", default="2026",
                   help="Edition to parse (default 2026).")
    p.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT,
                   help=f"Where caci_pdfs PDFs live (default {DEFAULT_RAW_ROOT}).")
    p.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT,
                   help=f"Output root (default {DEFAULT_OUT_ROOT}).")
    p.add_argument("--limit", type=int, default=None,
                   help="Parse only first N instructions (for debugging).")
    p.add_argument("--verbose", "-v", action="store_true")
    return p


def _setup_logging(verbose: bool) -> None:
    fmt = "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s"
    datefmt = "%H:%M:%S"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.DEBUG if verbose else logging.INFO)


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    _setup_logging(args.verbose)

    pdf_path = args.raw_root / args.edition / f"caci_{args.edition}.pdf"
    if not pdf_path.exists():
        logger.error("PDF not found at %s. Run: "
                     "uv run python -m ingest.sources.caci_pdfs --editions %s",
                     pdf_path, args.edition)
        return 2

    parse_pdf(pdf_path, args.edition, args.out_root, log=logger, limit=args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
