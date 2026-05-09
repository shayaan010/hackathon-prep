# Hackathon Prep Scaffold

Reusable building blocks for the EvenUp x OpenClaw hackathon (PI legal data).

## What's here

```
ingest/                    # statute corpus pipeline (see ingest/README.md)
  sources/                 # per-source fetchers (CA leginfo, NY/TX public.law)
  parsers/                 # HTML -> StatuteSection JSON + JSONL consolidator
  cli.py                   # shared argparse helpers (uniform CLI across sources)
  manifest.py              # append-per-record manifest writer + dedupe
  _http.py                 # shared httpx wrappers (rate-limit, retry, logging)

  scrape.py                # legacy generic fetch helpers
  parse_pdf.py             # PDF text extraction with vision OCR fallback
  courtlistener.py         # CourtListener API client

store/
  db.py                    # SQLite schema with source-tracking for verifiability

extract/
  llm.py                   # Claude tool-use wrapper with strict schema validation
  schemas.py               # Pre-built Pydantic schemas for PI legal extractions

search/
  semantic.py              # Chunk -> embed -> store -> query (local embeddings)

api/
  main.py                  # FastAPI bridge: /api/stats, /api/statutes,
                           # /api/search, /api/chat — consumed by the React UI
  statutes_seed.json       # Mock statute list served by /api/statutes

frontend/                  # case-compass React UI ("Lex Harvester")
  src/routes/              # TanStack Router file-based routes (/, /organizer, /coverage)
  src/components/          # Page components + shadcn/ui primitives
  src/lib/api.ts           # Typed client for the FastAPI backend

demo/
  app.py                   # Legacy Streamlit UI (still works; replaced by frontend/)

data/
  raw/                     # statute fetcher output (HTML)
  parsed/                  # statute parser output (JSON + unified JSONL)
  logs/                    # per-fetcher / per-runner logs

pipeline_example.py        # End-to-end test using all modules together
test_claude.py             # 30-second sanity check that ANTHROPIC_API_KEY works
```

## Statute corpus (`ingest/`)

Per-jurisdiction statute fetchers + parsers. Currently covers CA Vehicle
Code, NY Vehicle & Traffic Law, and TX Transportation Code. See
[`ingest/README.md`](ingest/README.md) for the full pipeline, CLI reference,
and instructions for adding new jurisdictions.

```bash
# Fetch raw HTML (cached on disk; idempotent re-runs)
uv run python -m ingest.sources.ca_leginfo_pages
uv run python -m ingest.sources.ny_public_law
uv run python -m ingest.sources.tx_public_law

# Parse to structured JSON (SHA-cached)
uv run python -m ingest.parsers.run_ca_leginfo
uv run python -m ingest.parsers.run_public_law -j NY
uv run python -m ingest.parsers.run_public_law -j TX

# Consolidate into unified JSONL (auto-discovers sources)
uv run python -m ingest.parsers.consolidate_jsonl
# -> data/parsed/jsonl/{ca,ny,tx}/<CODE>.jsonl
```

## Day-of workflow

1. **Hackathon morning** — once you receive your API key:
   ```bash
   nano .env                                    # add real ANTHROPIC_API_KEY
   uv run python test_claude.py                 # → "setup works"
   uv run python pipeline_example.py            # full pipeline smoke test
   ```

   Then start the dev servers (in separate terminals):
   ```bash
   uv run uvicorn api.main:app --reload --port 8000   # Python API on :8000
   cd frontend && bun install && bun run dev          # React UI on :3000
   ```

2. **First 30 min after eval drops** — do NOT code. Read tiers, pick one,
   sketch the data flow on paper, decide team roles.

3. **Build phase**:
   - For statute-corpus extensions: drop a new fetcher under
     `ingest/sources/<slug>.py`. `consolidate_jsonl` will auto-discover it.
   - For non-statute sources: extend `ingest/scrape.py` or
     `ingest/courtlistener.py`.
   - Modify `extract/schemas.py` for the specific data to extract.
   - Storage / search / demo stay as-is.

4. **3:45pm** — stop coding, rehearse demo. Code freeze at 4pm.

## Local Postgres (pgvector)

This repo includes a local Postgres setup that auto-initializes your schema:
- `store/schema_postgres.sql` (tables + indexes)
- `docker-compose.postgres.yml` (local DB service)

Start local Postgres:
```bash
docker compose -f docker-compose.postgres.yml up -d
```

Set DSN and load statutes into `statutes`:
```bash
export POSTGRES_DSN="postgresql://postgres:postgres@localhost:5432/hackathon"
uv run python scripts/load_released_set.py
```

Run API (it will read statutes from Postgres when `POSTGRES_DSN` is set):
```bash
uv run uvicorn api.main:app --reload --port 8000
```

Stop local Postgres:
```bash
docker compose -f docker-compose.postgres.yml down
```

## Key design decisions

**Why source quotes are mandatory.** The hackathon rule is "no fabrication, must
trace to public sources." Every extraction schema inherits from `SourceTracked`
which requires a `source_quote` field. The `extract.llm` module verifies that
the quoted text actually appears in the source document before accepting the
extraction. Hallucinated quotes are silently dropped.

**Why local embeddings.** The semantic search uses
`sentence-transformers/all-MiniLM-L6-v2` which runs on your laptop with no API
calls. Embedding 10k chunks costs nothing in API budget and works offline.

**Why SQLite.** At hackathon scale (~100k chunks max), SQLite + numpy is faster
than spinning up Postgres + pgvector. One file. No services to run.

**Why tool use over JSON prompting.** Claude's tool use mode is dramatically
more reliable for structured extraction. Pydantic validates the output;
malformed responses are rejected.

**Why filesystem as the contract between fetchers and parsers.** Each layer
in `ingest/` writes to disk and the next layer reads from disk. Fetchers
don't import parsers; parsers don't import fetchers. This means you can
re-run any layer independently, mix and match sources, and recover from a
killed run without losing state.

## Quick reference

```python
# Statutes (the unified corpus)
from ingest.parsers import StatuteSection, parse_section_html, parse_public_law_html
# Or just read data/parsed/jsonl/<juris>/<CODE>.jsonl directly

# Generic scrape
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

# Demo (React UI — preferred)
# Terminal 1: uv run uvicorn api.main:app --reload --port 8000
# Terminal 2: cd frontend && bun install && bun run dev

# Demo (legacy Streamlit)
# uv run streamlit run demo/app.py
```

## What's NOT here (and why)

- **No agentic framework.** If you need agents, build them on the day with
  raw `asyncio.gather` and direct Anthropic SDK calls. LangGraph/CrewAI/etc
  will cost more time than they save in 6.5 hours.

- **No Playwright browser pre-installed.** 80% of likely sources are JSON APIs
  or static HTML. If you hit a JS-heavy site, install Chromium in 60 seconds:
  `uv run playwright install chromium`. (We tried Playwright for FindLaw —
  Cloudflare blocked us anyway. `public.law` works without a browser.)

- ~~**No frontend build system.**~~ — superseded. The repo now ships with
  `frontend/` (React + Vite + shadcn/ui via [case-compass]). Streamlit demo
  remains under `demo/` if you'd rather skip Bun.

  [case-compass]: https://github.com/FouzanAbdullah/case-compass

- **No vector database.** SQLite + brute-force cosine is fine at this scale.
