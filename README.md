<div align="center">

# ⚖️ LexHarvester

### Reusable building blocks for the EvenUp × OpenClaw hackathon — PI legal data.

[![Contributors][contributors-badge]](#)
[![License: MIT][license-badge]](#)
[![Python][python-badge]](#)
[![React][react-badge]](#)

[View Docs](#quick-reference) · [Report Bug](#) · [Request Feature](#)

</div>

---

<details>
<summary>📋 Table of Contents</summary>

1. [About The Project](#about-the-project)
   - [What's Here](#whats-here)
   - [Built With](#built-with)
2. [Getting Started](#getting-started)
3. [Statute Corpus](#statute-corpus-ingest)
4. [Quick Reference](#quick-reference)
5. [Key Design Decisions](#key-design-decisions)
6. [What's NOT Here](#whats-not-here-and-why)

</details>

---

## About The Project

> **No fabrication. Must trace to public sources.**  
> Every extraction schema requires a `source_quote` field. Hallucinated quotes are silently dropped.

This scaffold gives your team a running start on hackathon day — statute ingestion, structured LLM extraction, semantic search, a FastAPI backend, and a React UI, all wired together and ready to extend.

### What's Here

```
ingest/                    # Statute corpus pipeline
  sources/                 # Per-source fetchers (CA leginfo, NY/TX public.law)
  parsers/                 # HTML → StatuteSection JSON + JSONL consolidator
  cli.py                   # Shared argparse helpers
  manifest.py              # Append-per-record manifest writer + dedupe
  _http.py                 # Shared httpx wrappers (rate-limit, retry, logging)
  scrape.py                # Legacy generic fetch helpers
  parse_pdf.py             # PDF text extraction with vision OCR fallback
  courtlistener.py         # CourtListener API client

store/
  db.py                    # SQLite schema with source-tracking

extract/
  llm.py                   # Claude tool-use wrapper with strict schema validation
  schemas.py               # Pre-built Pydantic schemas for PI legal extractions

search/
  semantic.py              # Chunk → embed → store → query (local embeddings)

api/
  main.py                  # FastAPI: /api/stats, /api/statutes, /api/search, /api/chat
  statutes_seed.json       # Mock statute list

frontend/                  # case-compass React UI ("Lex Harvester")
  src/routes/              # TanStack Router file-based routes
  src/components/          # Page components + shadcn/ui primitives
  src/lib/api.ts           # Typed client for the FastAPI backend

demo/
  app.py                   # Legacy Streamlit UI

pipeline_example.py        # End-to-end test using all modules together
test_claude.py             # 30-second sanity check that ANTHROPIC_API_KEY works
```

### Built With

| Layer | Tech |
|-------|------|
| LLM | Anthropic Claude (tool use) |
| Backend | FastAPI + SQLite |
| Frontend | React + Vite + shadcn/ui + TanStack Router |
| Embeddings | `sentence-transformers/all-MiniLM-L6-v2` (local, free) |
| Package manager | `uv` (Python), `bun` (JS) |
| Optional DB | Postgres + pgvector via Docker |

---

## Getting Started

Add your API key, then verify setup:

```bash
nano .env                        # add ANTHROPIC_API_KEY
uv run python test_claude.py     # → "setup works"
```

Start the dev servers (separate terminals):

```bash
uv run uvicorn api.main:app --reload --port 8000   # API on :8000
cd frontend && bun install && bun run dev           # UI on :3000
```

---

## Statute Corpus (ingest/)

Covers **CA Vehicle Code**, **NY Vehicle & Traffic Law**, and **TX Transportation Code**. See `ingest/README.md` for the full pipeline and instructions for adding new jurisdictions.

The three-step pipeline (fetch → parse → consolidate) is idempotent — safe to re-run at any point. Output lands in `data/parsed/jsonl/{ca,ny,tx}/`.

---

## Quick Reference

All modules are importable by path — see the inline docstrings in each file for full usage. Key entry points:

| Module | What it does |
|--------|-------------|
| `extract.llm` | Claude tool-use wrapper — pass a Pydantic schema, get a validated result |
| `extract.schemas` | Pre-built schemas: `CaseMetadata`, `Verdict`, `DocketEntry`, `Attorney` |
| `search.semantic` | Chunk → embed → store → cosine query (local, no API cost) |
| `store.db` | SQLite with source-tracking; call `db.init_schema()` once |
| `ingest.scrape` | Generic `fetch` / `fetch_many` helpers |
| `ingest.courtlistener` | CourtListener opinion search + fetch |

---

## Key Design Decisions

<details>
<summary><strong>Why source quotes are mandatory</strong></summary>

The hackathon rule is "no fabrication, must trace to public sources." Every extraction schema inherits from `SourceTracked` which requires a `source_quote` field. The `extract.llm` module verifies that the quoted text actually appears in the source document before accepting the extraction. Hallucinated quotes are silently dropped.

</details>

<details>
<summary><strong>Why local embeddings</strong></summary>

`sentence-transformers/all-MiniLM-L6-v2` runs on your laptop with no API calls. Embedding 10k chunks costs nothing in API budget and works offline.

</details>

<details>
<summary><strong>Why SQLite</strong></summary>

At hackathon scale (~100k chunks max), SQLite + numpy is faster than spinning up Postgres + pgvector. One file. No services to run.

</details>

<details>
<summary><strong>Why tool use over JSON prompting</strong></summary>

Claude's tool use mode is dramatically more reliable for structured extraction. Pydantic validates the output; malformed responses are rejected.

</details>

<details>
<summary><strong>Why filesystem as the contract between fetchers and parsers</strong></summary>

Each layer in `ingest/` writes to disk and the next layer reads from disk. Fetchers don't import parsers; parsers don't import fetchers. This means you can re-run any layer independently, mix and match sources, and recover from a killed run without losing state.

</details>

---

## What's NOT Here (and Why)

| Missing | Reason |
|---------|--------|
| Agentic framework (LangGraph, CrewAI) | Costs more time than it saves in 6.5 hours. Build with raw `asyncio.gather` + Anthropic SDK if needed. |
| Playwright pre-installed | 80% of sources are JSON APIs or static HTML. If needed: `uv run playwright install chromium` (60 sec). |
| Vector database | SQLite + brute-force cosine is fine at this scale. |

---

<!-- Badge definitions -->
[contributors-badge]: https://img.shields.io/badge/contributors-team-blue
[license-badge]: https://img.shields.io/badge/License-MIT-green.svg
[python-badge]: https://img.shields.io/badge/Python-3.11+-blue?logo=python
[react-badge]: https://img.shields.io/badge/React-Vite-61DAFB?logo=react
