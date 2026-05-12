<div align="center">

# ⚖️ Hackathon Prep Scaffold

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
   - [Day-of Workflow](#day-of-workflow)
   - [Local Postgres (pgvector)](#local-postgres-pgvector)
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

### Day-of Workflow

> ⚠️ **First 30 min after eval drops — do NOT code.** Read tiers, pick one, sketch the data flow on paper, decide team roles.

**Morning setup — once you receive your API key:**

```bash
nano .env                                    # add real ANTHROPIC_API_KEY
uv run python test_claude.py                 # → "setup works"
uv run python pipeline_example.py           # full pipeline smoke test
```

**Start the dev servers** (separate terminals):

```bash
# Terminal 1 — Python API on :8000
uv run uvicorn api.main:app --reload --port 8000

# Terminal 2 — React UI on :3000
cd frontend && bun install && bun run dev
```

**Build phase tips:**

- **New jurisdiction** → drop a fetcher under `ingest/sources/<slug>.py`; `consolidate_jsonl` auto-discovers it.
- **Non-statute sources** → extend `ingest/scrape.py` or `ingest/courtlistener.py`.
- **Custom extraction** → modify `extract/schemas.py`.
- Storage / search / demo stay as-is.

> 🛑 **3:45 PM — stop coding, rehearse demo. Code freeze at 4 PM.**

---

### Local Postgres (pgvector)

```bash
# Start
docker compose -f docker-compose.postgres.yml up -d

# Seed statutes
export POSTGRES_DSN="postgresql://postgres:postgres@localhost:5432/hackathon"
uv run python scripts/load_released_set.py

# Run API (reads from Postgres when POSTGRES_DSN is set)
uv run uvicorn api.main:app --reload --port 8000

# Stop
docker compose -f docker-compose.postgres.yml down
```

---

## Statute Corpus (ingest/)

Covers **CA Vehicle Code**, **NY Vehicle & Traffic Law**, and **TX Transportation Code**. See `ingest/README.md` for the full pipeline, CLI reference, and instructions for adding new jurisdictions.

```bash
# 1. Fetch raw HTML (cached; idempotent re-runs)
uv run python -m ingest.sources.ca_leginfo_pages
uv run python -m ingest.sources.ny_public_law
uv run python -m ingest.sources.tx_public_law

# 2. Parse to structured JSON (SHA-cached)
uv run python -m ingest.parsers.run_ca_leginfo
uv run python -m ingest.parsers.run_public_law -j NY
uv run python -m ingest.parsers.run_public_law -j TX

# 3. Consolidate into unified JSONL (auto-discovers sources)
uv run python -m ingest.parsers.consolidate_jsonl
# → data/parsed/jsonl/{ca,ny,tx}/<CODE>.jsonl
```

---

## Quick Reference

```python
# Statutes
from ingest.parsers import StatuteSection, parse_section_html, parse_public_law_html
# Or read data/parsed/jsonl/<juris>/<CODE>.jsonl directly

# Scrape
from ingest.scrape import fetch, fetch_many, fetch_bytes
from ingest.courtlistener import search_opinions, fetch_opinion
from ingest.parse_pdf import extract_text, extract_full_text

# Store
from store.db import Database
db = Database("hackathon.db")
db.init_schema()

# Extract
from extract.llm import extract, extract_many
from extract.schemas import CaseMetadata, Verdict, DocketEntry, Attorney
result = extract(text, Verdict, "Extract the verdict.")

# Search
from search.semantic import SemanticIndex
idx = SemanticIndex(db)
idx.index_document(doc_id, text)
hits = idx.search("query text", top_k=10)
```

**Demo (React UI — preferred):**
```bash
# Terminal 1
uv run uvicorn api.main:app --reload --port 8000
# Terminal 2
cd frontend && bun install && bun run dev
```

**Demo (legacy Streamlit):**
```bash
uv run streamlit run demo/app.py
```

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
