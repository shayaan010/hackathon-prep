from __future__ import annotations

import json
import os
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .db import PostgresStore


# The 18 contributing-factor categories the classifier may emit. MUST stay in
# sync with new_api/scripts/tag_statute_factors.py FACTOR_CATEGORIES and the
# frontend's lib/statutes.ts. The chat tool exposes this as an enum so the
# model can't invent strings that won't match any row.
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


app = FastAPI(title="New API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_store: Optional[PostgresStore] = None


def get_store() -> PostgresStore:
    global _store
    if _store is None:
        _store = PostgresStore()
        _store.init_schema()
    return _store


@app.on_event("startup")
def _startup() -> None:
    get_store()


class CaseRef(BaseModel):
    name: str
    citation: str


class Source(BaseModel):
    publisher: str
    url: str


class Statute(BaseModel):
    id: str
    jurisdiction: str
    jurisdictionLabel: str
    code: str
    section: str
    title: str
    summary: str
    text: str
    factors: list[str] = Field(default_factory=list)
    related: list[str] = Field(default_factory=list)
    cases: list[CaseRef] = Field(default_factory=list)
    source: Source
    lastVerified: str


class StatuteList(BaseModel):
    items: list[Statute]
    total: int


class SearchHit(BaseModel):
    id: str
    doc_id: int
    chunk_idx: int
    score: float
    text: str
    char_start: int
    char_end: int
    source_url: str
    metadata: dict = Field(default_factory=dict)


class SearchRequest(BaseModel):
    query: str
    top_k: int = 10


class ChatMessage(BaseModel):
    role: str
    text: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = Field(default_factory=list)
    matter_name: Optional[str] = None
    matter_caption: Optional[str] = None


class ChatResponse(BaseModel):
    text: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/stats")
def stats():
    return get_store().stats()


@app.get("/api/statutes", response_model=StatuteList)
def list_statutes(
    q: Optional[str] = None,
    jurisdiction: Optional[str] = None,
    factors: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    """Server-side filtered statute search.

    All filters are AND-combined with each other. Within ``factors`` and
    ``jurisdiction`` (both CSV) the semantics are any-of so multi-chip UI
    works as expected.
    """
    factor_list: Optional[list[str]] = None
    if factors:
        factor_list = [f.strip() for f in factors.split(",") if f.strip()]

    rows, total = get_store().list_statutes(
        q=q,
        jurisdiction=jurisdiction,
        factors=factor_list,
        limit=limit,
    )
    items: list[dict] = []
    for row in rows:
        updated_at = row.get("updated_at")
        last_verified = str(updated_at).split(" ")[0] if updated_at else "1970-01-01"
        items.append(
            {
                "id": str(row["id"]),
                "jurisdiction": row["jurisdiction_code"],
                "jurisdictionLabel": row["jurisdiction_name"],
                "code": row["code_name"],
                "section": row["section_number"],
                "title": row.get("title") or row["canonical_citation"],
                "summary": row.get("plain_english_summary")
                or row.get("statute_language")
                or "",
                "text": row["complete_statute"],
                "factors": list(row.get("factors") or []),
                "related": [],
                "cases": [],
                "source": {
                    "publisher": row.get("source_name") or "Unknown",
                    "url": row["source_url"],
                },
                "lastVerified": last_verified,
            }
        )
    return {"items": items, "total": total}


@app.post("/api/search", response_model=list[SearchHit])
def search(req: SearchRequest):
    _ = req
    return []


# ----------------------------------------------------------------------
# Anthropic tool-use: let Claude query the statutes DB server-side.
# Tools are read-only and parameterized — the model never writes raw SQL.
# ----------------------------------------------------------------------

CHAT_TOOLS: list[dict[str, Any]] = [
    {
        "name": "search_statutes",
        "description": (
            "Search the statutes database. Pass keywords in `query`, contributing-factor "
            "categories in `factors`, or both — at least one is required. Use `factors` "
            "when the user describes conduct that maps to a known PI category "
            "(e.g. 'tailgating' → 'Following Too Closely'; 'drunk driving' → 'DUI/DWI'; "
            "'ran a red light' → 'Failure to Obey Traffic Control Device'). Use `query` "
            "for free-text or specific section numbers. Returns id, citation, title, "
            "factors, and a snippet — follow up with get_statute(id) for full text."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Keywords (whitespace-separated; AND-ed across title, citation, full text). Optional if `factors` is provided.",
                },
                "jurisdiction": {
                    "type": "string",
                    "description": "Optional 2-letter code: CA, NY, TX, CO, FL, NV, OR.",
                },
                "code": {
                    "type": "string",
                    "description": "Optional code-name substring, e.g. 'Vehicle Code'.",
                },
                "factors": {
                    "type": "array",
                    "description": "Optional contributing-factor filter (any-of). Only use values from the enum.",
                    "items": {
                        "type": "string",
                        "enum": FACTOR_CATEGORIES,
                    },
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (1-25). Default 10.",
                    "default": 10,
                },
            },
        },
    },
    {
        "name": "get_statute",
        "description": (
            "Fetch the full text of a specific statute by its UUID. Use ids returned "
            "from search_statutes when the snippet isn't enough to answer the user."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "Statute UUID."},
            },
            "required": ["id"],
        },
    },
    {
        "name": "list_jurisdictions",
        "description": (
            "List jurisdictions and codes available in the database, with a count of "
            "statutes per code. Useful when the user asks 'what do you have?'."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
]


def _run_chat_tool(name: str, args: dict[str, Any], store: PostgresStore) -> Any:
    if name == "search_statutes":
        limit = max(1, min(int(args.get("limit") or 10), 25))
        raw_factors = args.get("factors") or []
        if not isinstance(raw_factors, list):
            raw_factors = []
        # Drop anything outside the allow-list; the model is enum-constrained
        # in the schema but defensive filtering is cheap.
        allowed = set(FACTOR_CATEGORIES)
        factor_list = [f for f in raw_factors if isinstance(f, str) and f in allowed]
        return store.search_statutes(
            query=args.get("query"),
            jurisdiction=args.get("jurisdiction"),
            code=args.get("code"),
            factors=factor_list or None,
            limit=limit,
        )
    if name == "get_statute":
        return store.get_statute(args.get("id") or "")
    if name == "list_jurisdictions":
        return store.list_jurisdictions()
    return {"error": f"Unknown tool: {name}"}


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not set; chat is disabled.")

    from anthropic import Anthropic

    client = Anthropic()
    store = get_store()
    factor_list = ", ".join(f for f in FACTOR_CATEGORIES if f != "Other")
    system = (
        "You are a concise case assistant for a personal-injury attorney. "
        "You can call tools to search a Postgres database of statutes from "
        "CA, NY, TX, CO, FL, NV, and OR. ALWAYS use search_statutes when the user "
        "asks about a topic, section, or fact pattern — never fabricate citations. "
        "Use get_statute to read full text when a snippet isn't enough.\n\n"
        "Statutes are tagged with these contributing-factor categories: "
        f"{factor_list}. When the user describes conduct that maps to one of these "
        "(e.g. 'tailgating' → 'Following Too Closely', 'drunk' → 'DUI/DWI', "
        "'ran a red light' → 'Failure to Obey Traffic Control Device'), pass them "
        "via the `factors` parameter — it's faster and more precise than keyword search. "
        "Combine with `query` for fact-pattern specifics.\n\n"
        "Cite results inline (e.g., 'Cal. Veh. Code § 23103'). "
        "Answer in 1-3 short paragraphs, plain prose. Use **bold** for emphasis, "
        "bullets only when listing items."
    )

    messages: list[dict[str, Any]] = [
        {"role": m.role, "content": m.text}
        for m in req.history
        if m.role in ("user", "assistant")
    ]
    messages.append({"role": "user", "content": req.message})

    final_text = ""
    for _ in range(6):  # cap tool-use iterations
        resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            system=system,
            tools=CHAT_TOOLS,
            messages=messages,
        )

        if resp.stop_reason != "tool_use":
            final_text = "".join(
                b.text for b in resp.content if getattr(b, "type", None) == "text"
            ).strip()
            break

        # Carry the assistant turn (with its tool_use blocks) into the next call.
        messages.append({"role": "assistant", "content": resp.content})

        tool_results: list[dict[str, Any]] = []
        for block in resp.content:
            if getattr(block, "type", None) != "tool_use":
                continue
            try:
                result = _run_chat_tool(block.name, dict(block.input or {}), store)
                content = json.dumps(result, default=str)
            except Exception as e:  # surface tool errors to the model
                content = json.dumps({"error": str(e)})
            tool_results.append(
                {"type": "tool_result", "tool_use_id": block.id, "content": content}
            )
        messages.append({"role": "user", "content": tool_results})

    return ChatResponse(text=final_text or "(no reply)")
