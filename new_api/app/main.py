from __future__ import annotations

import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .db import PostgresStore


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


@app.get("/api/statutes", response_model=list[Statute])
def list_statutes():
    rows = get_store().list_statutes()
    out: list[dict] = []
    for row in rows:
        updated_at = row.get("updated_at")
        last_verified = str(updated_at).split(" ")[0] if updated_at else "1970-01-01"
        out.append(
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
                "factors": [],
                "related": [],
                "cases": [],
                "source": {
                    "publisher": row.get("source_name") or "Unknown",
                    "url": row["source_url"],
                },
                "lastVerified": last_verified,
            }
        )
    return out


@app.post("/api/search", response_model=list[SearchHit])
def search(req: SearchRequest):
    _ = req
    return []


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not set; chat is disabled.")

    from anthropic import Anthropic

    client = Anthropic()
    system = (
        "You are a concise case assistant for a personal-injury attorney. "
        "Answer in 1-3 short paragraphs, plain prose. Use **bold** for emphasis, "
        "and bullets only when listing items. Do not fabricate citations or facts."
    )
    messages = [{"role": m.role, "content": m.text} for m in req.history if m.role in ("user", "assistant")]
    messages.append({"role": "user", "content": req.message})

    resp = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=512,
        system=system,
        messages=messages,
    )
    text = "".join(block.text for block in resp.content if getattr(block, "type", None) == "text").strip()
    return ChatResponse(text=text or "(no reply)")
