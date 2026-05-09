"""
FastAPI bridge between the React frontend (case-compass / Lex Harvester) and
the Python pipeline (store + search + extract).

Routes:
  GET  /api/stats           - document/chunk/extraction counts
  GET  /api/statutes        - real corpus from SQLite (falls back to seed)
  POST /api/search          - hybrid (semantic + keyword) search,
                              with a section-number short-circuit
  POST /api/chat            - Claude-backed chat reply (with retrieval)

Run:
    uv run uvicorn api.main:app --reload --port 8000
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from store.db import Database
from search.semantic import SemanticIndex


DB_PATH = os.environ.get("HACKATHON_DB", "hackathon.db")

app = FastAPI(title="Lex Harvester API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev-only; tighten before prod
    allow_methods=["*"],
    allow_headers=["*"],
)


_db: Optional[Database] = None
_idx: Optional[SemanticIndex] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(DB_PATH)
        _db.init_schema()
    return _db


def get_index() -> SemanticIndex:
    global _idx
    if _idx is None:
        _idx = SemanticIndex(get_db())
    return _idx


# ---------- Schemas (mirror src/lib/statutes.ts on the frontend) ----------


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


class SearchHit(BaseModel):
    """A semantic-search hit, formatted to slot into the UI's result list."""
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
    role: str  # "user" | "assistant"
    text: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = Field(default_factory=list)
    matter_name: Optional[str] = None
    matter_caption: Optional[str] = None


class ChatResponse(BaseModel):
    text: str


# ---------- Statute sourcing (real DB, with seed fallback) ----------
# Lives server-side so the UI hits a real API, even before any docs are ingested.

_STATUTE_SEED_PATH = Path(__file__).parent / "statutes_seed.json"


def _load_seed_statutes() -> list[dict]:
    if _STATUTE_SEED_PATH.exists():
        return json.loads(_STATUTE_SEED_PATH.read_text(encoding="utf-8"))
    return []


_JURISDICTION_CODES = {
    "California": "CA",
    "New York": "NY",
    "Texas": "TX",
    "Florida": "FL",
    "Illinois": "IL",
    "Washington": "WA",
    "Arizona": "AZ",
    "Georgia": "GA",
}


def _doc_to_statute(doc: dict) -> dict:
    """Shape a documents-table row into the frontend's Statute type."""
    meta = doc.get("metadata") or {}
    state = (meta.get("state") or "").strip()
    section = (meta.get("section") or "").strip()
    citation_full = (meta.get("citation") or "").strip()
    statute_name = (meta.get("statute_name") or "").strip()
    gold = (meta.get("category_gold") or "").strip()

    jurisdiction_code = _JURISDICTION_CODES.get(state, (state[:2].upper() or "??"))
    code_name = "Vehicle Code" if "Veh" in citation_full else (citation_full or "Code")

    title = gold or statute_name or f"§ {section}"

    raw_text = doc.get("raw_text") or ""
    summary = raw_text.strip().split("\n")[0][:240]

    factors = [gold] if gold else []

    # Pull predicted-tag extraction(s) so the UI can show model output too.
    extractions = doc.get("_extractions") or []
    for ext in extractions:
        data = ext.get("data") or {}
        cat = data.get("primary_category")
        if cat and cat not in factors:
            factors.append(cat)

    section_slug = (
        section.replace("(", "-").replace(")", "").replace(".", "-").lower()
        or str(doc.get("id"))
    )
    stable_id = f"{jurisdiction_code.lower()}-vc-{section_slug}"

    return {
        "id": stable_id,
        "jurisdiction": jurisdiction_code,
        "jurisdictionLabel": state or jurisdiction_code,
        "code": code_name,
        "section": section,
        "title": title,
        "summary": summary,
        "text": raw_text,
        "factors": factors,
        "related": [],
        "cases": [],
        "source": {
            "publisher": "California Legislative Information"
            if "Cal" in citation_full
            else "Legislature",
            "url": doc.get("source_url", ""),
        },
        "lastVerified": (doc.get("fetched_at") or "")[:10],
    }


def _load_statutes_from_db() -> list[dict]:
    """Pull every document from SQLite and shape it into Statute objects."""
    db = get_db()
    with db.conn() as c:
        rows = c.execute(
            "SELECT id, source_url, raw_text, metadata, fetched_at "
            "FROM documents ORDER BY id"
        ).fetchall()

    if not rows:
        return []

    # Hydrate extractions in one query per doc (corpus is small; fine here).
    out = []
    for row in rows:
        d = dict(row)
        d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
        d["_extractions"] = db.get_extractions_for_doc(d["id"])
        out.append(_doc_to_statute(d))
    return out


# ---------- Routes ----------


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/stats")
def stats():
    return get_db().stats()


@app.get("/api/statutes", response_model=list[Statute])
def list_statutes():
    real = _load_statutes_from_db()
    return real if real else _load_seed_statutes()


_SECTION_QUERY_RE = re.compile(r"^\s*\d+(\.\d+)?(\([a-z0-9]\))?\s*$", re.I)


def _is_section_query(q: str) -> bool:
    return bool(_SECTION_QUERY_RE.match(q or ""))


def _section_lookup(db: Database, query: str) -> list[dict]:
    """Direct lookup of documents whose metadata.section matches a numeric query."""
    base = query.strip().split("(")[0].strip()
    with db.conn() as c:
        rows = c.execute("SELECT id, metadata FROM documents").fetchall()
    hits = []
    seen = set()
    for row in rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        section = (meta.get("section") or "").split("(")[0].strip()
        if section != base or row["id"] in seen:
            continue
        seen.add(row["id"])
        chunks = db.get_chunks_for_doc(row["id"])
        if not chunks:
            continue
        c0 = chunks[0]
        hits.append({
            "id": c0["id"],
            "doc_id": row["id"],
            "text": c0["text"],
            "score": 1.0,
            "char_start": c0["char_start"],
            "char_end": c0["char_end"],
            "chunk_idx": c0["chunk_idx"],
        })
    return hits


def _stem(w: str) -> str:
    """Cheap suffix stripping so 'driving' matches 'drive'."""
    w = w.lower()
    for suf in ("ing", "ed", "es", "s"):
        if w.endswith(suf) and len(w) > len(suf) + 2:
            return w[: -len(suf)]
    return w


_STOPWORDS = {"the", "a", "an", "of", "and", "or", "to", "for", "in", "on", "at", "is", "be"}


def _keyword_score(query: str, text: str) -> float:
    """Fraction of meaningful query words whose stem appears in text. Range [0, 1]."""
    words = [w.lower() for w in re.findall(r"\w{3,}", query)]
    words = [w for w in words if w not in _STOPWORDS]
    if not words:
        return 0.0
    t = text.lower()
    matches = sum(1 for w in words if _stem(w) in t)
    return matches / len(words)


@app.post("/api/search", response_model=list[SearchHit])
def search(req: SearchRequest):
    if not req.query.strip():
        return []

    db = get_db()

    # Section-number queries (e.g. "22107", "21451(a)") — short-circuit to a
    # direct lookup. Embeddings have no idea what those digits mean.
    if _is_section_query(req.query):
        direct = _section_lookup(db, req.query)
        if direct:
            return [
                {
                    "id": f"chunk-{h['id']}",
                    "doc_id": h["doc_id"],
                    "chunk_idx": h["chunk_idx"],
                    "score": h["score"],
                    "text": h["text"],
                    "char_start": h["char_start"],
                    "char_end": h["char_end"],
                    "source_url": (db.get_document(h["doc_id"]) or {}).get(
                        "source_url", ""
                    ),
                    "metadata": (db.get_document(h["doc_id"]) or {}).get("metadata")
                    or {},
                }
                for h in direct
            ]
        # If section not in corpus, fall through to semantic so the user still
        # gets *something*.

    # Hybrid: pull more candidates than requested, then rerank with a keyword
    # boost. With ~37 short docs, MiniLM scores cluster tightly and a literal-
    # keyword signal is what separates "DUI" from "Improper Passing" for the
    # query "driving under the influence".
    idx = get_index()
    semantic = idx.search(req.query, top_k=max(req.top_k * 3, 15))

    KW_WEIGHT = 0.6
    for h in semantic:
        h["_semantic"] = h["score"]
        kw = _keyword_score(req.query, h["text"])
        h["score"] = h["score"] + KW_WEIGHT * kw
    semantic.sort(key=lambda h: -h["score"])
    semantic = semantic[: req.top_k]

    out: list[dict] = []
    for h in semantic:
        doc = db.get_document(h["doc_id"]) or {}
        out.append({
            "id": f"chunk-{h['id']}",
            "doc_id": h["doc_id"],
            "chunk_idx": h["chunk_idx"],
            "score": h["score"],
            "text": h["text"],
            "char_start": h["char_start"],
            "char_end": h["char_end"],
            "source_url": doc.get("source_url", ""),
            "metadata": doc.get("metadata") or {},
        })
    return out


def _retrieve_context(query: str, top_k: int = 5) -> str:
    """Pull the top semantic-search hits and format them as grounding context."""
    if not query.strip():
        return ""
    try:
        idx = get_index()
        hits = idx.search(query, top_k=top_k)
    except Exception:
        return ""

    if not hits:
        return ""

    db = get_db()
    lines = ["RETRIEVED STATUTE EXCERPTS (use these to ground citations):", ""]
    for h in hits:
        doc = db.get_document(h["doc_id"]) or {}
        meta = doc.get("metadata") or {}
        cit = meta.get("citation", "").strip()
        sec = meta.get("section", "").strip()
        header = f"{cit} § {sec}".strip(" §") or doc.get("source_url", "")
        excerpt = (h.get("text") or "").strip().replace("\n", " ")
        if len(excerpt) > 600:
            excerpt = excerpt[:600] + "…"
        lines.append(f"[{header}]  {excerpt}")
    return "\n".join(lines)


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key or api_key.startswith("replace"):
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY not set; chat is disabled.",
        )

    from anthropic import Anthropic

    client = Anthropic()
    matter_blurb = ""
    if req.matter_name:
        matter_blurb = f"You are assisting with the matter: {req.matter_name}"
        if req.matter_caption:
            matter_blurb += f" ({req.matter_caption})"
        matter_blurb += "."

    retrieved = _retrieve_context(req.message, top_k=5)
    grounding = (
        f"\n\n{retrieved}\n\n"
        "When the answer can be supported by the excerpts above, cite the section "
        "(e.g. 'Cal. Veh. Code § 22350') and quote a short phrase. "
        "If the excerpts don't address the question, say so plainly — do NOT invent statutes."
        if retrieved
        else "\n\nNo statute excerpts were retrieved for this question. "
        "If the user is asking about a specific statute, say you don't have it on file."
    )

    system = (
        "You are a concise case assistant for a personal-injury attorney. "
        "Answer in 1-3 short paragraphs, plain prose. Use **bold** for emphasis, "
        "and bullets only when listing items. Do not fabricate citations or facts. "
        + matter_blurb
        + grounding
    )

    messages = [
        {"role": m.role, "content": m.text}
        for m in req.history
        if m.role in ("user", "assistant")
    ]
    messages.append({"role": "user", "content": req.message})

    resp = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=512,
        system=system,
        messages=messages,
    )

    text = "".join(
        block.text for block in resp.content if getattr(block, "type", None) == "text"
    ).strip()
    return ChatResponse(text=text or "(no reply)")
