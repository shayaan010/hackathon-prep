# Hackathon Prep Scaffold

Reusable building blocks for the EvenUp x OpenClaw hackathon (PI legal data).

## What's here

```
ingest/
  scrape.py           # async HTTP fetching with retries & rate limiting
  parse_pdf.py        # PDF text extraction with vision OCR fallback
  courtlistener.py    # CourtListener API client (most likely data source)

store/
  db.py               # SQLite schema with source-tracking for verifiability

extract/
  llm.py              # Claude tool-use wrapper with strict schema validation
  schemas.py          # Pre-built Pydantic schemas for PI legal extractions

search/
  semantic.py         # Chunk → embed → store → query (local embeddings)

api/
  main.py             # FastAPI bridge: /api/stats, /api/statutes,
                      # /api/search, /api/chat — consumed by the React UI
  statutes_seed.json  # Mock statute list served by /api/statutes

frontend/             # case-compass React UI ("Lex Harvester")
  src/routes/         # TanStack Router file-based routes (/, /organizer, /coverage)
  src/components/     # Page components + shadcn/ui primitives
  src/lib/api.ts      # Typed client for the FastAPI backend

demo/
  app.py              # Legacy Streamlit UI (still works; replaced by frontend/)

pipeline_example.py   # End-to-end test using all modules together
test_claude.py        # 30-second sanity check that ANTHROPIC_API_KEY works
```

## Day-of workflow

1. **Hackathon morning** — once you receive your API key:
   ```bash
   # Replace placeholder with real key
   nano .env

   # Verify Claude responds
   uv run python test_claude.py     # → "setup works"

   # Verify pipeline end-to-end
   uv run python pipeline_example.py
   ```

   Then start the two dev servers (in separate terminals):
   ```bash
   # Terminal 1 — Python API on :8000
   uv run uvicorn api.main:app --reload --port 8000

   # Terminal 2 — React UI on :3000 (or whatever Vite picks)
   cd frontend
   bun install
   bun run dev
   ```
   The Vite dev server proxies `/api/*` to the FastAPI server, so you can
   hit the UI directly and ignore CORS.

2. **First 30 min after eval drops** — do NOT code:
   - Read all challenge tiers
   - Pick ONE tier
   - Sketch the data flow on paper
   - Decide team roles

3. **Build phase**:
   - Modify `ingest/` for the challenge's specific source
   - Modify `extract/schemas.py` for the specific data to extract
   - The rest (storage, search, demo) stays the same

4. **3:45pm** — stop coding, rehearse demo. Code freeze at 4pm.

## Key design decisions

**Why source quotes are mandatory.** The hackathon rule is "no fabrication, must
trace to public sources." Every extraction schema inherits from `SourceTracked`
which requires a `source_quote` field. The `extract.llm` module verifies that
the quoted text actually appears in the source document before accepting the
extraction. Hallucinated quotes are silently dropped.

**Why local embeddings.** The semantic search uses `sentence-transformers/all-MiniLM-L6-v2`
which runs on your laptop with no API calls. This means embedding 10k chunks
costs nothing in API budget and works offline.

**Why SQLite.** At hackathon scale (~100k chunks max), SQLite + numpy is faster
than spinning up Postgres + pgvector. One file. No services to run.

**Why tool use over JSON prompting.** Claude's tool use mode is dramatically
more reliable for structured extraction. Pydantic validates the output;
malformed responses are rejected.

## Quick reference

```python
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
  `uv run playwright install chromium`.

- ~~**No frontend build system.**~~ — superseded. The repo now ships with
  `frontend/` (React + Vite + shadcn/ui via [case-compass]). Streamlit demo
  remains under `demo/` if you'd rather skip Bun.

  [case-compass]: https://github.com/FouzanAbdullah/case-compass

- **No vector database.** SQLite + brute-force cosine is fine at this scale.
