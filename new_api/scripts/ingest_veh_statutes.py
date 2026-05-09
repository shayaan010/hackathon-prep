from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
from fastembed import TextEmbedding
from psycopg.types.json import Json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.db import PostgresStore


MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _ensure_hf_auth_env() -> None:
    hf_token = os.environ.get("HF_TOKEN", "").strip()
    hub_token = os.environ.get("HUGGINGFACE_HUB_TOKEN", "").strip()

    if hf_token and not hub_token:
        os.environ["HUGGINGFACE_HUB_TOKEN"] = hf_token
    elif hub_token and not hf_token:
        os.environ["HF_TOKEN"] = hub_token


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _vector_dim(store: PostgresStore) -> int:
    with store.conn() as c:
        row = c.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod) AS col_type
            FROM pg_attribute a
            WHERE a.attrelid = 'documents'::regclass
              AND a.attname = 'embedding'
              AND NOT a.attisdropped
            """
        ).fetchone()
    col_type = (row or {}).get("col_type", "vector(384)")
    match = re.search(r"vector\((\d+)\)", str(col_type))
    return int(match.group(1)) if match else 384


def _fit_dim(vec: list[float], target_dim: int) -> list[float]:
    if len(vec) == target_dim:
        return vec
    if len(vec) > target_dim:
        return vec[:target_dim]
    return vec + [0.0] * (target_dim - len(vec))


def _l2_normalize(vec: list[float]) -> list[float]:
    arr = np.array(vec, dtype=np.float32)
    norm = float(np.linalg.norm(arr))
    if norm > 0:
        arr = arr / norm
    return arr.tolist()


def _build_embedder(cache_dir: Path) -> TextEmbedding:
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        return TextEmbedding(model_name=MODEL_NAME, cache_dir=str(cache_dir))
    except Exception as e:
        msg = str(e)
        # Recover from partial/corrupt model downloads in cache.
        if "NO_SUCHFILE" in msg or "File doesn't exist" in msg:
            shutil.rmtree(cache_dir, ignore_errors=True)
            cache_dir.mkdir(parents=True, exist_ok=True)
            return TextEmbedding(model_name=MODEL_NAME, cache_dir=str(cache_dir))
        raise


def _section_title(payload: dict[str, Any]) -> str | None:
    section_name = _norm(payload.get("section_name"))
    if section_name:
        return section_name
    if payload.get("title"):
        return _norm(payload["title"])
    chapter = _norm(payload.get("chapter_title"))
    article = _norm(payload.get("article_title"))
    if chapter and article:
        return f"{chapter} — {article}"
    return chapter or article or None


def _important_chunks(payload: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    chunks: list[tuple[str, dict[str, Any]]] = []

    section = _norm(payload.get("section_num"))
    header = f"Section {section}" if section else "Section"
    head_text = _norm(payload.get("text"))
    if head_text:
        intro = head_text[:1200]
        chunks.append(
            (
                f"{header}: {intro}",
                {"kind": "intro", "section_num": section},
            )
        )

    for sub in payload.get("subsections") or []:
        txt = _norm(sub.get("text"))
        if len(txt) < 30:
            continue
        label = _norm(sub.get("label"))
        depth = sub.get("depth_em")
        prefix = f"({label}) " if label else ""
        chunks.append(
            (
                f"{header} {prefix}{txt}",
                {
                    "kind": "subsection",
                    "label": label or None,
                    "depth_em": depth,
                    "section_num": section,
                },
            )
        )

    if not chunks and head_text:
        chunks.append((head_text[:1200], {"kind": "fallback", "section_num": section}))

    seen: set[str] = set()
    deduped: list[tuple[str, dict[str, Any]]] = []
    for text, meta in chunks:
        key = _norm(text).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append((text, meta))
    return deduped[:64]


def _statute_exists(store: PostgresStore, *, jurisdiction_code: str, code_name: str, section_number: str) -> bool:
    with store.conn() as c:
        row = c.execute(
            """
            SELECT 1
            FROM statutes
            WHERE jurisdiction_code = %s
              AND code_name = %s
              AND section_number = %s
            LIMIT 1
            """,
            (jurisdiction_code, code_name, section_number),
        ).fetchone()
    return bool(row)


def _insert_statute(store: PostgresStore, payload: dict[str, Any]) -> str:
    section = _norm(payload.get("section_num"))
    law_code = _norm(payload.get("law_code")) or "VEH"
    code_name = _norm(payload.get("code_name")) or "Vehicle Code"
    jurisdiction_code = _norm(payload.get("jurisdiction")) or "CA"
    jurisdiction_name = "California" if jurisdiction_code == "CA" else jurisdiction_code
    canonical_citation = f"Cal. Veh. Code § {section}" if section else "Cal. Veh. Code"

    with store.conn() as c:
        row = c.execute(
            """
            INSERT INTO statutes (
                jurisdiction_code,
                jurisdiction_name,
                code_name,
                law_code,
                section_number,
                canonical_citation,
                title,
                statute_language,
                complete_statute,
                plain_english_summary,
                source_url,
                source_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                jurisdiction_code,
                jurisdiction_name,
                code_name,
                law_code,
                section,
                canonical_citation,
                _section_title(payload),
                _norm(payload.get("text"))[:500] or None,
                _norm(payload.get("text")) or "",
                None,
                _norm(payload.get("source_url")) or "",
                "California Legislative Information",
            ),
        ).fetchone()
    return str(row["id"])


def _insert_embeddings(
    store: PostgresStore,
    statute_id: str,
    payload: dict[str, Any],
    model: TextEmbedding,
    target_dim: int,
    source_file: Path,
) -> int:
    chunks = _important_chunks(payload)
    if not chunks:
        return 0

    texts = [c[0] for c in chunks]
    embeddings = list(model.embed(texts))

    inserted = 0
    with store.conn() as c:
        for idx, ((chunk_text, chunk_meta), emb) in enumerate(zip(chunks, embeddings)):
            vec = _l2_normalize(emb.astype(np.float32).tolist())
            vec = _fit_dim(vec, target_dim)
            vec_literal = "[" + ",".join(f"{v:.8f}" for v in vec) + "]"
            metadata = {
                "source_file": str(source_file),
                "source_url": payload.get("source_url"),
                "content_sha256": payload.get("content_sha256"),
                "node_tree_path": payload.get("node_tree_path"),
                "parser_version": payload.get("parser_version"),
                "chunk_meta": chunk_meta,
            }
            c.execute(
                """
                INSERT INTO documents (
                    document_type,
                    statute_id,
                    chunk_index,
                    chunk_text,
                    embedding,
                    embedding_model,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
                ON CONFLICT (document_type, statute_id, chunk_index) DO NOTHING
                """,
                (
                    "statute",
                    statute_id,
                    idx,
                    chunk_text,
                    vec_literal,
                    MODEL_NAME,
                    Json(metadata),
                ),
            )
            inserted += 1
    return inserted


def ingest(input_dirs: list[Path], dsn: str | None = None) -> None:
    store = PostgresStore(dsn)
    store.init_schema()
    target_dim = _vector_dim(store)
    new_api_root = Path(__file__).resolve().parents[1]
    cache_dir = Path(os.environ.get("FASTEMBED_CACHE_DIR", new_api_root / ".cache" / "fastembed"))
    model = _build_embedder(cache_dir)

    files: list[Path] = []
    for base in input_dirs:
        if base.exists():
            files.extend(base.rglob("*.json"))
    files = sorted(files)
    if not files:
        joined = ", ".join(str(p) for p in input_dirs)
        print(f"No JSON files found under: {joined}")
        return

    total_files = len(files)
    print(f"Processing {total_files} JSON files...")

    created_statutes = 0
    skipped_existing = 0
    inserted_chunks = 0
    processed = 0

    for p in files:
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"SKIP {p.name}: invalid JSON ({e})")
            processed += 1
            continue

        jurisdiction_code = _norm(payload.get("jurisdiction")) or "CA"
        code_name = _norm(payload.get("code_name")) or "Vehicle Code"
        section_number = _norm(payload.get("section_num"))
        if not section_number:
            print(f"SKIP {p.name}: missing section_num")
            processed += 1
            continue

        if _statute_exists(
            store,
            jurisdiction_code=jurisdiction_code,
            code_name=code_name,
            section_number=section_number,
        ):
            skipped_existing += 1
            processed += 1
            if processed % 100 == 0 or processed == total_files:
                pct = (processed / total_files) * 100
                print(
                    f"\rProgress: {processed}/{total_files} ({pct:5.1f}%) | "
                    f"created={created_statutes} skipped={skipped_existing} "
                    f"chunks={inserted_chunks}",
                    end="",
                    flush=True,
                )
            continue

        statute_id = _insert_statute(store, payload)
        created_statutes += 1

        chunks = _insert_embeddings(
            store=store,
            statute_id=statute_id,
            payload=payload,
            model=model,
            target_dim=target_dim,
            source_file=p,
        )
        inserted_chunks += chunks
        processed += 1
        if processed % 100 == 0 or processed == total_files:
            pct = (processed / total_files) * 100
            print(
                f"\rProgress: {processed}/{total_files} ({pct:5.1f}%) | "
                f"created={created_statutes} skipped={skipped_existing} "
                f"chunks={inserted_chunks}",
                end="",
                flush=True,
            )

    print()

    print(
        "Done. "
        f"Created statutes: {created_statutes}, "
        f"Skipped existing: {skipped_existing}, "
        f"Inserted chunk embeddings: {inserted_chunks}, "
        f"Embedding dim in DB: {target_dim}"
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    new_api_root = Path(__file__).resolve().parents[1]
    _load_env_file(new_api_root / ".env")
    _ensure_hf_auth_env()

    host_defaults = [
        repo_root / "data" / "parsed" / "ca_leginfo_pages" / "VEH",
        repo_root / "data" / "parsed" / "ny_public_law",
        repo_root / "data" / "parsed" / "tx_public_law",
    ]
    container_defaults = [
        Path("/data/parsed/ca_leginfo_pages/VEH"),
        Path("/data/parsed/ny_public_law"),
        Path("/data/parsed/tx_public_law"),
    ]
    defaults = container_defaults if container_defaults[0].exists() else host_defaults

    parser = argparse.ArgumentParser(
        description="Ingest parsed statute JSON into new_api Postgres with MiniLM embeddings."
    )
    parser.add_argument(
        "--input-dir",
        action="append",
        type=Path,
        default=defaults,
        help=(
            "Directory containing statute JSON files. Repeat flag for multiple dirs. "
            f"Default: {', '.join(str(p) for p in defaults)}"
        ),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("POSTGRES_DSN")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:postgres@localhost:5433/new_api",
        help="Postgres DSN. Defaults to POSTGRES_DSN / DATABASE_URL / local docker DSN.",
    )

    args = parser.parse_args()
    ingest(args.input_dir, dsn=args.dsn)


if __name__ == "__main__":
    main()
