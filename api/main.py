"""
FastAPI bridge between the React frontend (case-compass / Lex Harvester) and
the Python pipeline (store + search + extract).

Routes:
  GET  /api/stats           - document/chunk/extraction counts
  GET  /api/statutes        - mock statute list (matches the UI's Statute type)
  POST /api/search          - semantic search over indexed documents
  POST /api/chat            - Claude-backed chat reply for the case assistant

Run:
    uv run uvicorn api.main:app --reload --port 8000
"""
from __future__ import annotations

import os
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


# ---------- Mock statute seed (mirrors frontend src/lib/statutes.ts) ----------
# Lives server-side so the UI hits a real API, even before any docs are ingested.

_STATUTE_SEED_PATH = Path(__file__).parent / "statutes_seed.json"


def _load_statutes() -> list[dict]:
    import json
    if _STATUTE_SEED_PATH.exists():
        return json.loads(_STATUTE_SEED_PATH.read_text(encoding="utf-8"))
    return []


# ---------- Routes ----------


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/stats")
def stats():
    return get_db().stats()


@app.get("/api/statutes", response_model=list[Statute])
def list_statutes():
    return _load_statutes()


@app.post("/api/search", response_model=list[SearchHit])
def search(req: SearchRequest):
    if not req.query.strip():
        return []

    db = get_db()
    idx = get_index()
    hits = idx.search(req.query, top_k=req.top_k)

    out: list[dict] = []
    for h in hits:
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

    system = (
        "You are a concise case assistant for a personal-injury attorney. "
        "Answer in 1-3 short paragraphs, plain prose. Use **bold** for emphasis, "
        "and bullets only when listing items. Do not fabricate citations or facts. "
        + matter_blurb
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
