# `ingest/` — statute corpus pipeline

Per-jurisdiction statute fetchers + parsers that produce a unified JSONL
corpus downstream code can consume.

Currently covers **CA Vehicle Code**, **NY Vehicle & Traffic Law**, and
**TX Transportation Code**. Adding a new jurisdiction is mostly drop-in
(see [Adding a jurisdiction](#adding-a-jurisdiction)).

---

## Quickstart

```bash
# 1. Discover + fetch raw HTML
uv run python -m ingest.sources.ca_leginfo_toc                # CA: walk TOC for full section list
uv run python -m ingest.sources.ca_leginfo_pages              # CA: fetch sections
uv run python -m ingest.sources.ny_public_law                 # NY: discover + fetch
uv run python -m ingest.sources.tx_public_law                 # TX: discover + fetch

# 2. Parse to structured JSON
uv run python -m ingest.parsers.run_ca_leginfo                # CA -> data/parsed/ca_leginfo_pages/
uv run python -m ingest.parsers.run_public_law -j NY          # NY -> data/parsed/ny_public_law/
uv run python -m ingest.parsers.run_public_law -j TX          # TX -> data/parsed/tx_public_law/

# 3. Consolidate into unified JSONL (downstream input)
uv run python -m ingest.parsers.consolidate_jsonl
# -> data/parsed/jsonl/{ca,ny,tx}/<CODE>.jsonl
```

Every step is **idempotent** — re-running with the same arguments will skip
work that's already on disk. Use `--force` to bust the cache.

---

## Architecture

The pipeline has four layers communicating via the **filesystem**:

```
       [discover]            [fetch]              [parse]              [consolidate]
            │                   │                    │                       │
            ▼                   ▼                    ▼                       ▼
    sources/<slug>.py    raw HTML on disk    parsers/<slug>.py    parsers/consolidate_jsonl.py
   (TOC walk OR seed)   data/raw/<slug>/       (HTML -> JSON)     data/parsed/jsonl/<juris>/<CODE>.jsonl
                                              data/parsed/<slug>/         ← downstream input
```

- **`ingest/sources/<slug>.py`** — fetchers. One per upstream source.
  Discovery + per-section HTTP fetch with retry + caching. Output: raw
  HTML files + a `manifest.jsonl`.
- **`ingest/parsers/<slug>.py`** (e.g. `ca_leginfo.py`, `public_law.py`) —
  pure parser libraries. Take HTML bytes, return a `StatuteSection` Pydantic
  model. No I/O, no network.
- **`ingest/parsers/run_*.py`** — runners. Drive the parser libraries over
  raw HTML directories, write per-section JSON + a parse manifest.
- **`ingest/parsers/consolidate_jsonl.py`** — flattens parsed JSON files into
  a unified JSONL corpus, **auto-discovering** sources from `data/parsed/`.

### Shared infrastructure

- **`ingest/_http.py`** — `RateLimiter`, `get_with_retry`, `default_client`,
  `setup_logging`. Used by all fetchers and runners.
- **`ingest/cli.py`** — `add_fetcher_args`, `add_parser_runner_args`,
  `section_sort_key`, `run_heartbeat`. Standardizes CLI flags so every
  fetcher takes the same arguments with the same defaults.
- **`ingest/manifest.py`** — `ManifestWriter` (per-record append, durable
  across kills), `dedupe`, `load_known_missing`.
- **`ingest/sources/base.py`** — `FetcherConfig`, `FetchRecord`, `run_fetcher`,
  `SOURCE_REGISTRY`, `register_source`. Every source registers itself; the
  generic concurrent fetch loop lives here.
- **`ingest/parsers/runner.py`** — `run_parser`, `parse_one`, `ParseRecord`.
  Generic SHA-cache loop used by `run_ca_leginfo` and `run_public_law`.
- **`ingest/parsers/types.py`** — `StatuteSection`, `Subsection`,
  `HistoryNote`, `ParseError`. The single source of truth for the
  parsed-document schema.

---

## Directory layout

```
data/
├── raw/                            ← fetcher output
│   ├── ca_leginfo_pages/
│   │   ├── VEH/<section>.html
│   │   └── manifest.jsonl
│   ├── ca_leginfo_toc/
│   │   ├── VEH_toc.jsonl           ← master section index for CA
│   │   ├── chapters/<slug>.html    ← cached chapter pages
│   │   └── debug/                  ← raw division-page HTML for diagnostics
│   ├── ny_public_law/
│   │   ├── VAT/<section>.html
│   │   ├── VAT_toc.jsonl
│   │   ├── toc/                    ← cached title + article pages
│   │   └── manifest.jsonl
│   └── tx_public_law/
│       ├── TN/<chapter>.<section>.html
│       ├── TN_toc.jsonl
│       ├── toc/                    ← cached title + chapter pages
│       └── manifest.jsonl
│
├── parsed/                         ← parser-runner output
│   ├── ca_leginfo_pages/
│   │   ├── VEH/<section>.json
│   │   └── parse_manifest.jsonl
│   ├── ny_public_law/
│   │   ├── VAT/<section>.json
│   │   └── parse_manifest.jsonl
│   ├── tx_public_law/
│   │   ├── TN/<section>.json
│   │   └── parse_manifest.jsonl
│   └── jsonl/                      ← consolidate_jsonl output (downstream input)
│       ├── ca/VEH.jsonl
│       ├── ny/VAT.jsonl
│       └── tx/TN.jsonl
│
└── logs/                           ← per-source / per-runner log files
    ├── ca_leginfo_pages.log
    ├── ca_leginfo_toc.log
    ├── ny_public_law.log
    ├── tx_public_law.log
    ├── run_ca_leginfo.log
    └── run_public_law_{ny,tx}.log
```

---

## Caching contract

| Layer | Cache key | How to bust |
|---|---|---|
| Fetcher | HTML file exists at `data/raw/<slug>/<CODE>/<section>.html` | `--force` |
| Fetcher (skip-known-missing) | section already recorded in `manifest.jsonl` as 200-but-empty | `--no-skip-missing` |
| Parser | JSON exists with matching `content_sha256` of raw HTML | `--force` |
| Consolidator | always overwrites the JSONL | n/a |

Re-running the whole pipeline on a fully-warm cache takes seconds.

---

## CLI reference

All four fetchers take the same flags (some sources expose extras —
`ca_leginfo_pages` has `--eval-only` and `--range`; `ca_leginfo_toc` has
`--diff`).

### Common fetcher flags

| Flag | Default | Notes |
|---|---|---|
| `--code CODE` | per-source (`VEH`/`VAT`/`TN`) | Law code to fetch |
| `--out-dir PATH` | `data/raw/<slug>` | Output root |
| `--concurrency N` | `2` | Parallel HTTP requests |
| `--rate-interval F` | `0.4` (CA pages: `0.5`) | Min seconds between request starts; `0` to disable |
| `--force` | off | Re-fetch even if cached |
| `--limit N` | none | Cap sections (smoke testing) |
| `--sections S [S …]` | none | Explicit list (skips discovery) |
| `--sections-file PATH` | none | Sections from a file, one per line |
| `--log-file PATH` | `data/logs/<slug>.log` | Append detailed log lines |
| `--no-log-file` | off | Disable file logging |
| `--verbose` | off | DEBUG-level stdout logs |
| `--heartbeat-every F` | `5.0` | Seconds between progress lines |
| `--no-skip-missing` | off | Re-attempt previously confirmed not-found |
| `--dedupe` | off | Compact manifest in place and exit |

### CA-leginfo-pages-only

| Flag | Notes |
|---|---|
| `--eval-only` | Only fetch sections from `data/eval-ca-vehicle-code.csv` |
| `--range LO HI` | Brute-force a custom integer range (inclusive) |

### CA-leginfo-toc-only

| Flag | Notes |
|---|---|
| `--no-save-chapters` | Don't cache chapter-level HTML |
| `--diff PATH` | Diff against a manifest; write `<CODE>_missing.txt` |

### Common parser-runner flags

| Flag | Default | Notes |
|---|---|---|
| `-j/--jurisdiction` | per-runner (`CA`/`NY`/`TX`) | |
| `-c/--code` | per-jurisdiction | |
| `--in-dir PATH` | `data/raw/<slug>` | |
| `--out-dir PATH` | `data/parsed/<slug>` | |
| `--force`, `--limit`, `--log-file`, `--no-log-file`, `--verbose` | as above | |

### Consolidator

| Flag | Default | Notes |
|---|---|---|
| `-j/--jurisdiction` | none (= all auto-discovered) | Filter |
| `-c/--code` | none (= all auto-discovered) | Filter |
| `--in-dir PATH` | `data/parsed` | Where to scan for sources |
| `--out-dir PATH` | `data/parsed/jsonl` | Where JSONL files land |

---

## Manifest schemas

### Raw fetch manifest (`data/raw/<slug>/manifest.jsonl`)

One JSON line per section attempted (per-record append — durable across kills):

```json
{
  "source_slug": "ca_leginfo_pages",
  "jurisdiction": "CA",
  "law_code": "VEH",
  "section": "21453",
  "url": "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21453.&lawCode=VEH",
  "fetched_at": "2026-05-09T14:00:00+00:00",
  "http_status": 200,
  "bytes": 162837,
  "content_sha256": "ab223a8...",
  "raw_path": "data/raw/ca_leginfo_pages/VEH/21453.html",
  "valid": true,
  "error": null,
  "extra": {"op_statues": "2022"}        // source-specific metadata
}
```

`extra` holds source-specific fields:
- CA: `op_statues` (chaptered year)
- NY: `title_number`, `article`
- TX: `title_number`, `chapter`

### Parse manifest (`data/parsed/<slug>/parse_manifest.jsonl`)

```json
{
  "section": "21453",
  "law_code": "VEH",
  "jurisdiction": "CA",
  "ok": true,
  "raw_path": "data/raw/ca_leginfo_pages/VEH/21453.html",
  "parsed_path": "data/parsed/ca_leginfo_pages/VEH/21453.json",
  "parsed_at": "2026-05-09T14:00:00+00:00",
  "content_sha256": "ab223a8...",
  "skipped_cached": false,
  "error": null
}
```

---

## Unified JSONL schema (downstream input)

`data/parsed/jsonl/<juris>/<CODE>.jsonl` is what downstream code consumes.
One section per line:

```json
{
  "id": "ca/veh/21453",
  "jurisdiction": "CA",
  "law_code": "VEH",
  "section": "21453",
  "code_name": "Vehicle Code",
  "section_name": null,
  "hierarchy": {
    "division": {"ident": "11", "title": "RULES OF THE ROAD", "range": "21000 - 23336"},
    "chapter":  {"ident": "2",  "title": "Traffic Signs, Signals, and Markings", "range": "21350 - 21468"},
    "article":  {"ident": "3",  "title": "Offenses Relating to Traffic Devices", "range": "21450 - 21468"}
  },
  "text": "(a) A driver facing a steady circular red signal alone shall stop ...",
  "markdown": "# Vehicle Code\n## Division 11. RULES OF THE ROAD ...",
  "subsections": [
    {"label": "a", "text": "A driver facing a steady circular red signal ...", "depth_em": 0.0},
    ...
  ],
  "history": {
    "raw": "(Amended by Stats. 2022, Ch. 957, Sec. 3. (AB 2147) Effective January 1, 2023.)",
    "action": "Amended",
    "statutes_year": 2022,
    "chapter": "957",
    "section": "3",
    "bill_number": "AB 2147",
    "effective_date": "2023-01-01",
    "operative_date": null
  },
  "source": {
    "url": "https://leginfo.legislature.ca.gov/...",
    "raw_path": "data/raw/ca_leginfo_pages/VEH/21453.html",
    "content_sha256": "ab223a8...",
    "parsed_at": "2026-05-09T14:00:00+00:00",
    "parser_version": "1",
    "metadata": {"op_statues": "2022", "op_chapter": "957", "op_section": "3", "node_tree_path": "15.2.3"}
  }
}
```

---

## Adding a jurisdiction

The minimum to add `<NEWSTATE>` (e.g. `OR`, `IL`):

1. **Drop a fetcher** at `ingest/sources/<slug>.py`. Use `ny_public_law.py`
   as a template if the upstream is HTML-per-section and supports
   public.law/justia-style discovery (root → titles → articles → sections).
   Use `ca_leginfo_pages.py` as a template if you have a known list of
   section numbers and want to brute-force a range.

   The fetcher must:
   - Define a `FetcherConfig` and register it via `@register_source("<slug>")`.
   - Use `add_fetcher_args` for its CLI.
   - Write to `data/raw/<slug>/<CODE>/*.html` and a `manifest.jsonl`.

2. **If the upstream HTML differs from existing parsers**, drop a parser
   library at `ingest/parsers/<slug_or_family>.py` and a runner at
   `ingest/parsers/run_<slug_or_family>.py`. Use `public_law.py` /
   `run_public_law.py` as templates. The parser must return a
   `StatuteSection` from `parsers/types.py`.

3. **That's it for `consolidate_jsonl`** — it auto-discovers any
   `data/parsed/<slug>/<CODE>/` directory containing valid
   `StatuteSection` JSONs. The new jurisdiction will appear in the next
   consolidation run.

---

## Source-specific notes

- **`leginfo.legislature.ca.gov`** is JSF-rendered but returns each section
  on a stable URL with a JS metadata block. Validity heuristic:
  ``op_statues = '<year>'`` is a real section; ``op_statues = ''`` is a
  missing one. `--rate-interval 0.5` (default for this source) is a polite
  starting point — leginfo throttles aggressively.
- **`newyork.public.law`** and **`texas.public.law`** are non-profit mirrors
  with clean static HTML. They return 404 for missing sections. Default
  `--rate-interval 0.4` is fine.
- **FindLaw / Justia** are Cloudflare-protected and reject HTTP clients
  without a JS challenge solver; they're not used here. We did experiment
  with Playwright, but `public.law` works without a browser, so we don't
  need it.
- **statutes.capitol.texas.gov** is an Angular SPA — content is rendered
  client-side, useless for plain HTTP scraping. That's why TX uses
  `texas.public.law`.

---

## Programmatic API

```python
from ingest.parsers import (
    StatuteSection,            # Pydantic schema
    parse_section_html,        # CA leginfo HTML -> StatuteSection
    parse_public_law_html,     # NY + TX public.law HTML -> StatuteSection
    run_parser,                # Generic batch parser-runner
)

from ingest.sources import (
    FetcherConfig,             # Per-source description
    FetchRecord,               # Manifest record
    run_fetcher,               # Generic concurrent fetcher
    SOURCE_REGISTRY,           # {slug: config_factory}
)

from ingest.cli import section_sort_key
from ingest.manifest import ManifestWriter, dedupe, load_known_missing
from ingest._http import RateLimiter, get_with_retry, default_client
```
