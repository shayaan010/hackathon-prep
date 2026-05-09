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

from typing import Any, Optional

from anthropic import Anthropic

from search.semantic import SemanticIndex
from store.db import Database


DEFAULT_MODEL = "claude-opus-4-7"
DEFAULT_MAX_TOOL_ITERATIONS = 4
DEFAULT_MAX_TOKENS = 2048
DEFAULT_EFFORT = "high"


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
- `search_statutes`: Query both the statute corpus AND the attorney's uploaded \
documents by topic, factor, code section, or fact pattern. Use it whenever the \
answer depends on what a statute says, what's in an uploaded document, or which \
statutes might apply to facts described in an upload. Skip it only for casual \
chatter or follow-ups about material already in this conversation.

When uploaded-document hits come back:
- Treat the file content as facts. Quote short phrases verbatim.
- Then surface the applicable statutes (often a follow-up search is needed).

Citation rules (NON-NEGOTIABLE):
1. Quote a short verbatim phrase — never paraphrase a citation or a fact from a file.
2. For statutes, always include the citation and section, e.g. **Cal. Veh. Code § 22350**.
3. For uploaded documents, attribute by filename, e.g. **(report.pdf)**.
4. If `search_statutes` returns nothing relevant, say "I don't have anything on \
that in the corpus." Do NOT invent statutes, sections, quotes, or facts.
5. Distinguish "the corpus contains X" from "the law says X". You only have what's \
in the corpus.

When the user attaches a file inline (paperclip in chat):
- Treat that attachment as primary context, the same way you'd treat an uploaded \
document hit. Quote it verbatim. Run `search_statutes` to surface applicable law.

Style:
- Short. 1-3 paragraphs of plain prose. Bullets only for lists of items.
- **Bold** for citations and key terms. No markdown headers.
- Address the user as "you" (the attorney), not "the user"."""


SEARCH_TOOL: dict[str, Any] = {
    "name": "search_statutes",
    "description": (
        "Search the indexed corpus for excerpts matching a natural-language "
        "query. The corpus contains BOTH motor-vehicle statutes AND any "
        "documents the attorney has uploaded (police reports, contracts, "
        "depositions, briefs, etc.). Uploaded-document hits are tagged "
        "'[UPLOADED DOCUMENT: <filename>]' in the results. Use this for any "
        "question that depends on what a statute says, what's in an uploaded "
        "document, or which statutes apply to facts in an upload. Returns up "
        "to top_k excerpts."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Natural-language search query. Examples: 'reckless "
                    "driving on a wet road', 'cell phone use while driving', "
                    "'failure to yield at crosswalk', 'Cal. Veh. Code 22350'."
                ),
            },
            "top_k": {
                "type": "integer",
                "description": "How many excerpts to return. Default 5; max 12.",
                "default": 5,
                "minimum": 1,
                "maximum": 12,
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
    ):
        self.index = index
        self.db = db
        self.client = client or Anthropic()
        self.model = model
        self.max_tool_iterations = max_tool_iterations
        self.max_tokens = max_tokens
        self.effort = effort

    # ---------- public entry point ----------

    def reply(
        self,
        message: str,
        history: Optional[list[dict[str, str]]] = None,
        attached_files: Optional[list[dict[str, str]]] = None,
        matter_name: Optional[str] = None,
        matter_caption: Optional[str] = None,
    ) -> str:
        """
        Run one conversational turn and return Claude's reply.

        Args:
            message: the user's latest message text.
            history: prior turns as [{"role": "user"|"assistant", "text": "..."}, ...].
            attached_files: list of {"filename": str, "text": str} the user uploaded
                with this turn. Their content is prepended as context to the user message.
            matter_name / matter_caption: optional case context (e.g. "Reyes v. Western
                Logistics", "Rear-end collision, I-880"). Passed as a soft hint.
        """
        messages = self._format_history(history or [])
        messages.append(self._format_user_turn(message, attached_files, matter_name, matter_caption))

        for _ in range(self.max_tool_iterations):
            response = self._call_claude(messages)
            stop = response.stop_reason

            if stop == "end_turn":
                return self._extract_text(response) or "(no reply)"

            if stop == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                tool_results = self._run_tools(response.content)
                messages.append({"role": "user", "content": tool_results})
                continue

            if stop == "refusal":
                return "I can't help with that request."

            # max_tokens, pause_turn, or anything unexpected: surface what we have.
            return self._extract_text(response) or "(reply truncated)"

        return (
            "I hit the tool-use iteration cap before reaching a final answer. "
            "Try a more specific question, or break it into smaller pieces."
        )

    # ---------- Claude call ----------

    def _call_claude(self, messages: list[dict]) -> Any:
        return self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            thinking={"type": "adaptive"},
            output_config={"effort": self.effort},
            system=self._system_blocks(),
            tools=[SEARCH_TOOL],
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
            try:
                if name == "search_statutes":
                    text = self._run_search(
                        block.input.get("query", ""),
                        int(block.input.get("top_k", 5)),
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

    def _run_search(self, query: str, top_k: int) -> str:
        query = (query or "").strip()
        if not query:
            return "No query was provided."

        top_k = max(1, min(int(top_k or 5), 12))
        hits = self.index.search(query, top_k=top_k)
        if not hits:
            return f"No statute excerpts found for query: {query!r}"

        lines = [f"Found {len(hits)} excerpt(s) for query {query!r}:"]
        for i, h in enumerate(hits, 1):
            doc = self.db.get_document(h["doc_id"]) or {}
            meta = doc.get("metadata") or {}
            url = doc.get("source_url", "")
            is_upload = bool(meta.get("upload"))
            is_verdict = meta.get("kind") == "verdict"

            if is_upload:
                fn = (meta.get("filename") or url.replace("upload://", "") or "uploaded").strip()
                header = f"[UPLOADED DOCUMENT: {fn}]"
            elif is_verdict:
                # Pull the structured Verdict extraction so chat sees the dollar amount.
                exts = self.db.get_extractions_for_doc(h["doc_id"])
                v = next((e["data"] for e in exts if e.get("schema_name") == "Verdict"), None) or {}
                case = v.get("case_name") or meta.get("case_name") or "(case)"
                amt = v.get("total_amount_usd")
                amt_str = f"${amt:,.0f}" if isinstance(amt, (int, float)) and amt else "n/a"
                jur = v.get("jurisdiction") or ""
                tail = f" — {jur}" if jur else ""
                header = f"[VERDICT: {case} — {amt_str}{tail}]"
            else:
                cit = (meta.get("citation") or "").strip()
                sec = (meta.get("section") or "").strip()
                header = f"{cit} § {sec}".strip(" §") or url or "(unknown)"

            excerpt = (h.get("text") or "").strip().replace("\n", " ")
            if len(excerpt) > 800:
                excerpt = excerpt[:800] + "…"

            block = [f"[{i}] {header}"]
            if url and not is_upload:
                block.append(f"    source: {url}")
            block.append(f"    excerpt: {excerpt}")
            lines.append("\n".join(block))

        return "\n\n".join(lines)

    # ---------- response parsing ----------

    @staticmethod
    def _extract_text(response: Any) -> str:
        return "".join(
            getattr(b, "text", "")
            for b in response.content
            if getattr(b, "type", None) == "text"
        ).strip()
