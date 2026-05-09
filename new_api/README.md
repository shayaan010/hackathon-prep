# new_api

Standalone FastAPI service with local Postgres (`pgvector`) via Docker Compose.

## Prerequisites

- Docker Desktop running

## Run

From project root:

```bash
cd new_api
docker compose up -d --build
```

## Verify

```bash
curl http://localhost:8001/api/health
```

Expected response:

```json
{"ok": true}
```

## Useful commands

View logs:

```bash
docker compose logs -f api
docker compose logs -f db
```

Stop services:

```bash
docker compose down
```

## Endpoints

- `GET /api/health`
- `GET /api/stats`
- `GET /api/statutes`
- `POST /api/search`
- `POST /api/chat`

Base URL:

`http://localhost:8001`

## Notes

- Postgres runs on host port `5433`.
- DB/schema are auto-created from `sql/schema.sql`.
- `POST /api/chat` needs `ANTHROPIC_API_KEY` in environment if you want chat enabled.

## Ingest VEH Statutes + pgvector Embeddings

This script reads JSON statutes from:
`data/parsed/ca_leginfo_pages/VEH`

It inserts into `new_api` Postgres as the source of truth and is idempotent:
- skips statutes already stored (`jurisdiction_code + code_name + section_number`)
- stores chunk embeddings in `documents.embedding` (pgvector)
- uses embedding model: `sentence-transformers/all-MiniLM-L6-v2`

Run from repo root (host-side), pointing at exposed Postgres:

```bash
cd new_api
POSTGRES_DSN=postgresql://postgres:postgres@localhost:5433/new_api \
python scripts/ingest_veh_statutes.py
```

Optional arguments:

```bash
python scripts/ingest_veh_statutes.py \
  --input-dir ../data/parsed/ca_leginfo_pages/VEH \
  --dsn postgresql://postgres:postgres@localhost:5433/new_api
```
