"""
Chat wrapper for the Lex Harvester statute assistant.

What's different vs. the prior single-shot /api/chat:

  * Tool-use retrieval. Claude is given a `search_statutes` tool and decides
    whether to invoke it. This skips retrieval for off-topic chatter and lets
    Claude refine its own query (e.g. "the running-a-red statute" -> search
    "circular red signal stop"). The agentic loop is bounded by
    `max_tool_iterations`.

  * Prompt caching. The system prompt + tool list are stable; mark the last
    system block `cache_control: ephemeral` so the prefix is reused across
    requests. (Note: caching only kicks in once the cached prefix exceeds the
    model's minimum — 4096 tokens on Opus 4.7. Below that it is a harmless no-op,
    forward-compatible for when we add more guidance.)

  * Stronger grounding. Explicit "no fabrication" rules in the system prompt,
    plus structured tool output (citation + section + verbatim excerpt) so
    Claude can cite cleanly.

  * Opus 4.7 with adaptive thinking. Adaptive is off by default on Opus 4.7;
    we turn it on so Claude thinks when needed (cross-reference / multi-step
    legal questions) and skips it for simple lookups. `effort: "high"` is the
    recommended minimum for intelligence-sensitive work.
"""
from __future__ import annotations

import os
import re
from contextlib import contextmanager
from typing import Any, Optional

from anthropic import Anthropic

from search.semantic import SemanticIndex
from store.db import Database


DEFAULT_MODEL = "claude-opus-4-7"

# Mirror of new_api/scripts/tag_statute_factors.py FACTOR_CATEGORIES — the 18
# strings the classifier may emit. Used to enum-constrain the chat tool's
# `factors` parameter so the model can't invent labels that won't match any
# row.
FACTOR_CATEGORIES: list[str] = [
    "Improper Turning",
    "Improper Passing",
    "Failure to Yield the Right-of-Way",
    "Improper Lane of Travel",
    "Improper Stopping",
    "DUI/DWI",
    "Fleeing the Scene of a Collision",
    "Failure to Maintain Lane",
    "Driving Too Fast For Conditions",
    "Using a Wireless Telephone/Texting While Driving",
    "Fleeing a Police Officer",
    "Failure to Obey Traffic Control Device",
    "Following Too Closely",
    "Failure to Yield at a Yield Sign",
    "Improper Starting",
    "Reckless Driving",
    "Failure to Use/Activate Horn",
    "Other",
]
DEFAULT_MAX_TOOL_ITERATIONS = 4
DEFAULT_MAX_TOKENS = 2048
DEFAULT_EFFORT = "high"

# Curated multi-state statute Postgres (lives in `new_api/docker-compose.yml`).
# Override with POSTGRES_DSN env var if you point at something else.
DEFAULT_STATUTES_DSN = "postgresql://postgres:postgres@localhost:5433/new_api"

# Mirror of api.main._PG_PUBLISHER_BY_JURISDICTION. Kept in sync manually since
# importing api.main here would create a circular import.
_PG_PUBLISHER_BY_JURISDICTION = {
    "CA": "California Legislative Information",
    "NY": "New York State Senate",
    "TX": "Texas Legislature Online",
    "CO": "Colorado General Assembly",
    "FL": "The Florida Senate",
    "NV": "Nevada Legislature",
    "OR": "Oregon State Legislature",
}


SYSTEM_PROMPT = """\
You are Lex Harvester, a focused legal-research assistant for personal-injury \
attorneys. Your domain is motor-vehicle statutes, contributing-factor analysis, \
and damages comparables.

The corpus has three kinds of content:
- **Statutes** — indexed motor-vehicle code sections (e.g. Cal. Veh. Code).
- **Verdicts & settlements** — real opinions ingested from CourtListener with a \
structured Verdict extraction (case name, jurisdiction, claim/injury type, dollar \
amount). Search results tagged `[VERDICT: <case_name> — <amount>]`.
- **Uploaded documents** — files the attorney added to the corpus, such as \
police reports, contracts, depositions, or briefs. Search results from these \
are tagged `[UPLOADED DOCUMENT: <filename>]`.

Tools available:
- `search_statutes`: Keyword-search the curated motor-vehicle statute database \
(CA, NY, TX, CO, FL, NV, OR). Returns short snippets with citation + section. \
Use this for any question about what a statute says or which statutes might \
apply to a fact pattern. Filter by `jurisdiction` (2-letter) when the attorney \
specifies a state.
- `get_statute`: Fetch the full text of a specific statute by its UUID. Use \
this when a `search_statutes` snippet isn't enough.
- `list_jurisdictions`: List the jurisdictions and codes available, with \
counts. Use when the attorney asks "what do you have?".
- `search_courtlistener`: Query CourtListener for real published opinions. \
Use this whenever the attorney asks for case law, prior opinions, similar cases, \
or "find me a case where...". Each hit comes back with a permanent CourtListener \
URL — ALWAYS include the URL when you cite a CourtListener result so the \
attorney can click through.

Skip these tools for casual chatter or follow-ups about material already in \
this conversation.

When the attorney attaches a file inline (paperclip in chat), the file's text \
is already in the user message. Treat it as facts, quote short phrases \
verbatim, then run `search_statutes` to surface applicable law.

Citation rules (NON-NEGOTIABLE):
1. Quote a short verbatim phrase — never paraphrase a citation or a fact from a file.
2. For statutes, always include the citation and section, e.g. **Cal. Veh. Code § 22350**.
3. For uploaded documents, attribute by filename, e.g. **(report.pdf)**.
4. For case law from `search_courtlistener`, include the case name, citation \
(if returned), and the CourtListener URL — never invent a CourtListener URL.
5. If a tool returns nothing relevant, say so plainly. Do NOT invent statutes, \
sections, quotes, cases, or URLs.
6. Distinguish "the corpus contains X" from "the law says X". You only have what's \
in the corpus or what the tools just returned.

Style:
- Short. 1-3 paragraphs of plain prose. Bullets only for lists of items.
- **Bold** for citations and key terms. No markdown headers.
- Address the user as "you" (the attorney), not "the user"."""


SEARCH_TOOL: dict[str, Any] = {
    "name": "search_statutes",
    "description": (
        "Search the curated motor-vehicle statute database covering "
        "California (CA), New York (NY), Texas (TX), Colorado (CO), Florida "
        "(FL), Nevada (NV), and Oregon (OR). Pass keywords in `query`, "
        "contributing-factor categories in `factors`, or both — at least "
        "one must be set. `query` AND-tokens across title / citation / "
        "full text; `factors` is any-of via array overlap. Use `factors` "
        "whenever the attorney describes conduct that maps to a known PI "
        "category (e.g. 'tailgating' → 'Following Too Closely', 'drunk' "
        "→ 'DUI/DWI', 'ran a red light' → 'Failure to Obey Traffic Control "
        "Device'). Returns id, citation, section, title, factors, and a "
        "500-char snippet. Follow up with `get_statute(id)` if needed."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Whitespace-separated keywords (AND-ed). Examples: "
                    "'reckless driving', 'cell phone driving', "
                    "'failure to yield crosswalk', '22350'. Optional if "
                    "`factors` is provided."
                ),
            },
            "jurisdiction": {
                "type": "string",
                "description": (
                    "Optional 2-letter filter: CA, NY, TX, CO, FL, NV, OR."
                ),
            },
            "code": {
                "type": "string",
                "description": (
                    "Optional code-name substring, e.g. 'Vehicle Code', "
                    "'Transportation Code'."
                ),
            },
            "factors": {
                "type": "array",
                "description": (
                    "Optional contributing-factor categories to filter on "
                    "(any-of). Use the EXACT strings from the enum."
                ),
                "items": {
                    "type": "string",
                    "enum": FACTOR_CATEGORIES,
                },
            },
            "top_k": {
                "type": "integer",
                "description": "How many results to return. Default 8; max 25.",
                "default": 8,
                "minimum": 1,
                "maximum": 25,
            },
        },
    },
}


GET_STATUTE_TOOL: dict[str, Any] = {
    "name": "get_statute",
    "description": (
        "Fetch the FULL text of a specific statute by its UUID. Use ids "
        "returned from `search_statutes` when the snippet isn't enough to "
        "answer the question."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Statute UUID."},
        },
        "required": ["id"],
    },
}


LIST_JURISDICTIONS_TOOL: dict[str, Any] = {
    "name": "list_jurisdictions",
    "description": (
        "List the jurisdictions and codes available in the statute database, "
        "with statute counts. Use this when the attorney asks 'what do you "
        "have?' or 'which states are covered?'."
    ),
    "input_schema": {"type": "object", "properties": {}},
}


COURTLISTENER_TOOL: dict[str, Any] = {
    "name": "search_courtlistener",
    "description": (
        "Search CourtListener for published court opinions matching a query. "
        "Use this whenever the attorney wants real case law — e.g. 'find a "
        "case where someone fled the police related to Veh. Code § 2800.1', "
        "or 'similar rear-end collision verdicts in California'. Each result "
        "comes with a permanent CourtListener URL that the attorney can open. "
        "Prefer this tool over `search_statutes` when the attorney asks for "
        "case law, opinions, or 'similar cases' — `search_statutes` only "
        "covers statutes and uploaded documents in the local corpus, not "
        "external case law."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Free-text query. Include factor or fact-pattern keywords "
                    "(e.g. 'fleeing police officer evade pursuit'). Avoid "
                    "stuffing in long boilerplate; CourtListener ranks by "
                    "relevance and over-long queries hurt recall."
                ),
            },
            "court": {
                "type": "string",
                "description": (
                    "Optional CourtListener court slug to restrict results: "
                    "'cal' (California), 'ny' (New York), 'tex' (Texas). "
                    "Omit for nationwide search."
                ),
                "default": "",
            },
            "top_k": {
                "type": "integer",
                "description": "How many opinions to return. Default 5; max 10.",
                "default": 5,
                "minimum": 1,
                "maximum": 10,
            },
        },
        "required": ["query"],
    },
}


class ChatWrapper:
    """Stateful-per-call wrapper around Claude with tool-use statute retrieval."""

    def __init__(
        self,
        index: SemanticIndex,
        db: Database,
        client: Optional[Anthropic] = None,
        model: str = DEFAULT_MODEL,
        max_tool_iterations: int = DEFAULT_MAX_TOOL_ITERATIONS,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        effort: str = DEFAULT_EFFORT,
        statutes_dsn: Optional[str] = None,
    ):
        self.index = index
        self.db = db
        self.client = client or Anthropic()
        self.model = model
        self.max_tool_iterations = max_tool_iterations
        self.max_tokens = max_tokens
        self.effort = effort
        self.statutes_dsn = (
            statutes_dsn
            or os.environ.get("POSTGRES_DSN")
            or os.environ.get("DATABASE_URL")
            or DEFAULT_STATUTES_DSN
        )

    # ---------- public entry point ----------

    def reply(
        self,
        message: str,
        history: Optional[list[dict[str, str]]] = None,
        attached_files: Optional[list[dict[str, str]]] = None,
        matter_name: Optional[str] = None,
        matter_caption: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Run one conversational turn and return Claude's reply.

        Returns a dict with the shape `{"text": str, "statutes": list[dict]}`.
        `statutes` is the deduped list of statute rows that the model actually
        retrieved this turn via `search_statutes` / `get_statute` (FE-shaped,
        same schema as /api/statutes items). Empty when the model didn't run
        a statute tool — the UI uses this to decide whether to render cards.

        Args:
            message: the user's latest message text.
            history: prior turns as [{"role": "user"|"assistant", "text": "..."}, ...].
            attached_files: list of {"filename": str, "text": str} the user uploaded
                with this turn. Their content is prepended as context to the user message.
            matter_name / matter_caption: optional case context (e.g. "Reyes v. Western
                Logistics", "Rear-end collision, I-880"). Passed as a soft hint.
        """
        # Per-turn accumulator for statute rows pulled by tool calls. We dedupe
        # by id; `get_statute` results override `search_statutes` (richer payload).
        self._statute_hits: list[dict] = []
        self._statute_hit_ids: set[str] = set()

        messages = self._format_history(history or [])
        messages.append(self._format_user_turn(message, attached_files, matter_name, matter_caption))

        for _ in range(self.max_tool_iterations):
            response = self._call_claude(messages)
            stop = response.stop_reason

            if stop == "end_turn":
                return self._make_reply(self._extract_text(response) or "(no reply)")

            if stop == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                tool_results = self._run_tools(response.content)
                messages.append({"role": "user", "content": tool_results})
                continue

            if stop == "refusal":
                return self._make_reply("I can't help with that request.")

            # max_tokens, pause_turn, or anything unexpected: surface what we have.
            return self._make_reply(self._extract_text(response) or "(reply truncated)")

        return self._make_reply(
            "I hit the tool-use iteration cap before reaching a final answer. "
            "Try a more specific question, or break it into smaller pieces."
        )

    def _make_reply(self, text: str) -> dict[str, Any]:
        return {"text": text, "statutes": list(self._statute_hits)}

    # ---------- Claude call ----------

    def _call_claude(self, messages: list[dict]) -> Any:
        return self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            thinking={"type": "adaptive"},
            output_config={"effort": self.effort},
            system=self._system_blocks(),
            tools=[SEARCH_TOOL, GET_STATUTE_TOOL, LIST_JURISDICTIONS_TOOL, COURTLISTENER_TOOL],
            messages=messages,
        )

    # ---------- prompt assembly ----------

    @staticmethod
    def _system_blocks() -> list[dict[str, Any]]:
        # cache_control on the last (only) system block also captures the tools
        # in the cached prefix, since `tools` renders before `system`.
        return [
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ]

    @staticmethod
    def _format_history(history: list[dict[str, str]]) -> list[dict]:
        out: list[dict] = []
        for m in history:
            role = m.get("role")
            text = m.get("text") or ""
            if role in ("user", "assistant") and text:
                out.append({"role": role, "content": text})
        return out

    @staticmethod
    def _format_user_turn(
        message: str,
        attached_files: Optional[list[dict[str, str]]],
        matter_name: Optional[str],
        matter_caption: Optional[str],
    ) -> dict[str, Any]:
        blocks: list[dict[str, Any]] = []

        if matter_name:
            caption = f" ({matter_caption})" if matter_caption else ""
            blocks.append({
                "type": "text",
                "text": f"[Active matter: {matter_name}{caption}]",
            })

        for af in attached_files or []:
            filename = af.get("filename") or "attached"
            text = (af.get("text") or "").strip()
            if not text:
                continue
            if len(text) > 12000:
                text = text[:12000] + "\n…[truncated]"
            blocks.append({
                "type": "text",
                "text": f"<attached_file name=\"{filename}\">\n{text}\n</attached_file>",
            })

        blocks.append({"type": "text", "text": message})
        return {"role": "user", "content": blocks}

    # ---------- tool execution ----------

    def _run_tools(self, content: list[Any]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for block in content:
            if getattr(block, "type", None) != "tool_use":
                continue
            name = block.name
            args = dict(block.input or {})
            try:
                if name == "search_statutes":
                    raw_factors = args.get("factors") or []
                    if not isinstance(raw_factors, list):
                        raw_factors = []
                    allowed = set(FACTOR_CATEGORIES)
                    factor_list = [
                        f for f in raw_factors
                        if isinstance(f, str) and f in allowed
                    ]
                    text = self._run_search(
                        args.get("query", ""),
                        int(args.get("top_k") or 8),
                        args.get("jurisdiction"),
                        args.get("code"),
                        factor_list or None,
                    )
                elif name == "get_statute":
                    text = self._run_get_statute(args.get("id", ""))
                elif name == "list_jurisdictions":
                    text = self._run_list_jurisdictions()
                elif name == "search_courtlistener":
                    text = self._run_courtlistener(
                        args.get("query", ""),
                        args.get("court", "") or "",
                        int(args.get("top_k") or 5),
                    )
                else:
                    text = f"Unknown tool: {name}"
                results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": text,
                })
            except Exception as e:
                results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": f"Tool error: {e}",
                    "is_error": True,
                })
        return results

    # ---------- Postgres helpers ----------

    @contextmanager
    def _pg_conn(self):
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(self.statutes_dsn, row_factory=dict_row)
        try:
            yield conn
        finally:
            conn.close()

    # Mirror of api.main._pg_row_to_statute. Duplicated here to avoid a circular
    # import (api.main imports ChatWrapper lazily). Keep the two in sync.
    @staticmethod
    def _pg_row_to_statute(row: dict) -> dict:
        citation = (row.get("canonical_citation") or "").strip()
        title = (row.get("title") or "").strip() or citation or "(untitled)"
        full = (row.get("complete_statute") or "").strip()

        summary = (row.get("plain_english_summary") or "").strip()
        if not summary:
            summary = full[:240]
            if len(full) > 240:
                summary += "…"

        jurisdiction_code = (row.get("jurisdiction_code") or "").strip()
        publisher = (
            (row.get("source_name") or "").strip()
            or _PG_PUBLISHER_BY_JURISDICTION.get(jurisdiction_code)
            or "Legislature"
        )

        updated_at = row.get("updated_at")
        last_verified = str(updated_at).split(" ")[0] if updated_at else ""

        return {
            "id": str(row.get("id")),
            "jurisdiction": jurisdiction_code,
            "jurisdictionLabel": (row.get("jurisdiction_name") or jurisdiction_code).strip(),
            "code": (row.get("code_name") or "").strip(),
            "section": (row.get("section_number") or "").strip(),
            "title": title,
            "summary": summary,
            "text": full,
            "factors": list(row.get("factors") or []),
            "related": [],
            "cases": [],
            "source": {
                "publisher": publisher,
                "url": (row.get("source_url") or "").strip(),
            },
            "lastVerified": last_verified,
        }

    def _record_statute_hit(self, row: dict, *, replace: bool = False) -> None:
        """Append a statute row (FE-shaped) to the per-turn accumulator.

        `replace=True` swaps any existing hit with the same id — used by
        `get_statute` so the richer full-text payload wins over a prior
        `search_statutes` snippet.
        """
        statute = self._pg_row_to_statute(row)
        sid = statute["id"]
        if sid in self._statute_hit_ids:
            if not replace:
                return
            for i, existing in enumerate(self._statute_hits):
                if existing["id"] == sid:
                    self._statute_hits[i] = statute
                    return
            return
        self._statute_hit_ids.add(sid)
        self._statute_hits.append(statute)

    @staticmethod
    def _run_courtlistener(query: str, court: str, top_k: int) -> str:
        """Hit the CourtListener API and format opinions with permanent URLs."""
        import asyncio
        import os

        from ingest.courtlistener import search_opinions

        query = (query or "").strip()
        if not query:
            return "No query was provided."

        top_k = max(1, min(int(top_k or 5), 10))
        court = (court or "").strip() or None

        if not os.environ.get("COURTLISTENER_TOKEN"):
            # The unauthenticated API still works but is heavily rate-limited;
            # surface the limitation honestly so Claude can pass it along.
            note = " (no COURTLISTENER_TOKEN configured — anonymous quota)"
        else:
            note = ""

        try:
            results = asyncio.run(
                search_opinions(query, court=court, page_size=top_k)
            )
        except Exception as e:
            return f"CourtListener search failed{note}: {e}"

        if not results:
            return f"No CourtListener opinions found for query {query!r}{note}."

        lines = [
            f"Found {len(results)} CourtListener opinion(s) for {query!r}"
            f"{(' in court=' + court) if court else ''}{note}:"
        ]
        for i, r in enumerate(results, 1):
            case_name = (r.get("caseName") or r.get("caseNameShort") or "").strip()
            court_name = (r.get("court") or "").strip()
            date_filed = (r.get("dateFiled") or "")[:10]
            citations = r.get("citation") or []
            citation_str = ", ".join(citations) if citations else ""

            absolute_url = r.get("absolute_url") or ""
            url = (
                f"https://www.courtlistener.com{absolute_url}"
                if absolute_url
                else f"https://www.courtlistener.com/opinion/{r.get('id')}/"
            )

            snippet = (r.get("snippet") or "").strip().replace("\n", " ")
            if len(snippet) > 400:
                snippet = snippet[:400] + "…"

            block = [f"[{i}] {case_name or '(no case name)'}"]
            details: list[str] = []
            if court_name:
                details.append(court_name)
            if date_filed:
                details.append(date_filed)
            if citation_str:
                details.append(citation_str)
            if details:
                block.append("    " + " · ".join(details))
            block.append(f"    URL: {url}")
            if snippet:
                block.append(f"    snippet: {snippet}")
            lines.append("\n".join(block))

        return "\n\n".join(lines)

    def _run_search(
        self,
        query: str,
        top_k: int,
        jurisdiction: Optional[str] = None,
        code: Optional[str] = None,
        factors: Optional[list[str]] = None,
    ) -> str:
        """Keyword + factor search over the curated Postgres statutes table.

        At least one of ``query`` / ``factors`` must be set. Keyword tokens
        AND-combine across title / citation / full text. Factors any-of via
        Postgres array overlap. Returns at most ``top_k`` hits formatted for
        Claude to cite.
        """
        query = (query or "").strip()
        top_k = max(1, min(int(top_k or 8), 25))

        where_parts: list[str] = []
        params: list[Any] = []

        tokens = [t for t in re.split(r"\s+", query) if t]
        for tok in tokens:
            like = f"%{tok}%"
            where_parts.append(
                "(title ILIKE %s OR canonical_citation ILIKE %s OR complete_statute ILIKE %s)"
            )
            params.extend([like, like, like])

        if jurisdiction:
            where_parts.append("jurisdiction_code = %s")
            params.append(jurisdiction.strip().upper())
        if code:
            where_parts.append("code_name ILIKE %s")
            params.append(f"%{code.strip()}%")
        if factors:
            cleaned = [f for f in (s.strip() for s in factors) if f]
            if cleaned:
                where_parts.append("factors && %s::text[]")
                params.append(cleaned)

        if not where_parts:
            return (
                "search_statutes requires at least one of `query` or `factors`."
            )
        where_sql = " AND ".join(where_parts)

        params.append(top_k)
        # Fetch the full row so we can both (a) format a snippet for Claude and
        # (b) record an FE-shaped statute hit for the UI to render as a card.
        sql = f"""
            SELECT
                id::text AS id,
                jurisdiction_code,
                jurisdiction_name,
                code_name,
                section_number,
                canonical_citation,
                title,
                complete_statute,
                plain_english_summary,
                source_url,
                source_name,
                factors,
                updated_at
            FROM statutes
            WHERE {where_sql}
            ORDER BY jurisdiction_code, section_number
            LIMIT %s
        """

        try:
            with self._pg_conn() as c:
                rows = c.execute(sql, params).fetchall()
        except Exception as e:
            return (
                f"Statute database is unreachable ({e}). "
                f"Verify Postgres is running on 5433 (new_api/docker-compose.yml)."
            )

        # Build a human-readable description of the filter set for the
        # "no results" / header lines so Claude knows what was searched.
        descr_parts: list[str] = []
        if query:
            descr_parts.append(f"query={query!r}")
        if factors:
            descr_parts.append(f"factors={factors}")
        if jurisdiction:
            descr_parts.append(f"jurisdiction={jurisdiction.upper()}")
        if code:
            descr_parts.append(f"code~={code!r}")
        descr = ", ".join(descr_parts) or "(no filters)"

        if not rows:
            return f"No statutes matched: {descr}"

        lines = [f"Found {len(rows)} statute(s) for {descr}:"]
        for i, r in enumerate(rows, 1):
            citation = (r.get("canonical_citation") or "").strip()
            title = (r.get("title") or "").strip()
            jc = r.get("jurisdiction_code") or ""
            cn = r.get("code_name") or ""
            sec = r.get("section_number") or ""
            url = r.get("source_url") or ""
            row_factors = list(r.get("factors") or [])
            full_text = (r.get("complete_statute") or "").strip()
            snippet = full_text[:500].replace("\n", " ")
            if len(full_text) > 500:
                snippet = snippet + "…"

            header = citation or f"{jc} {cn} § {sec}".strip()
            block = [f"[{i}] {header}"]
            block.append(f"    id: {r.get('id')}")
            if title:
                block.append(f"    title: {title}")
            if row_factors:
                block.append(f"    factors: {', '.join(row_factors)}")
            if url:
                block.append(f"    source: {url}")
            block.append(f"    excerpt: {snippet}")
            lines.append("\n".join(block))

            # Record this statute for the UI. `search_statutes` results don't
            # overwrite an existing hit (a prior `get_statute` would be richer).
            self._record_statute_hit(dict(r), replace=False)

        return "\n\n".join(lines)

    def _run_get_statute(self, statute_id: str) -> str:
        statute_id = (statute_id or "").strip()
        if not statute_id:
            return "No id was provided."

        sql = """
            SELECT
                id::text AS id,
                jurisdiction_code,
                jurisdiction_name,
                code_name,
                section_number,
                canonical_citation,
                title,
                complete_statute,
                plain_english_summary,
                source_url,
                source_name,
                factors,
                updated_at
            FROM statutes
            WHERE id::text = %s
        """
        try:
            with self._pg_conn() as c:
                row = c.execute(sql, [statute_id]).fetchone()
        except Exception as e:
            return f"Statute database is unreachable ({e})."

        if not row:
            return f"No statute found with id {statute_id!r}."

        # Record the full-text hit. `replace=True` so a prior search snippet
        # (if any) is replaced by this richer payload.
        self._record_statute_hit(dict(row), replace=True)

        body = (row.get("complete_statute") or "").strip()
        if len(body) > 6000:
            body = body[:6000] + "\n…[truncated]"

        return (
            f"{row.get('canonical_citation') or ''}\n"
            f"id: {row.get('id')}\n"
            f"title: {row.get('title') or ''}\n"
            f"jurisdiction: {row.get('jurisdiction_name')} ({row.get('jurisdiction_code')})\n"
            f"code: {row.get('code_name')} § {row.get('section_number')}\n"
            f"source: {row.get('source_url') or ''}\n\n"
            f"--- full text ---\n{body}"
        )

    def _run_list_jurisdictions(self) -> str:
        sql = """
            SELECT
                jurisdiction_code,
                jurisdiction_name,
                code_name,
                COUNT(*) AS n
            FROM statutes
            GROUP BY jurisdiction_code, jurisdiction_name, code_name
            ORDER BY jurisdiction_code, code_name
        """
        try:
            with self._pg_conn() as c:
                rows = c.execute(sql).fetchall()
        except Exception as e:
            return f"Statute database is unreachable ({e})."

        if not rows:
            return "No statutes loaded."

        lines = ["Available jurisdictions / codes:"]
        for r in rows:
            lines.append(
                f"  {r['jurisdiction_code']} ({r['jurisdiction_name']}) — "
                f"{r['code_name']}: {r['n']} statutes"
            )
        return "\n".join(lines)

    # ---------- response parsing ----------

    @staticmethod
    def _extract_text(response: Any) -> str:
        return "".join(
            getattr(b, "text", "")
            for b in response.content
            if getattr(b, "type", None) == "text"
        ).strip()
