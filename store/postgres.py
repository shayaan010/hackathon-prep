"""
PostgreSQL storage for statute records and vectorized document chunks.

This module owns the Postgres schema for:
  - statutes
  - documents
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


class PostgresStatuteStore:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
        if not self.dsn:
            raise ValueError("Missing POSTGRES_DSN (or DATABASE_URL) for PostgresStatuteStore.")

    @contextmanager
    def conn(self):
        connection = psycopg.connect(self.dsn, row_factory=dict_row)
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self) -> None:
        """Create extensions, tables, and indexes. Safe to run repeatedly."""
        with self.conn() as c:
            c.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            c.execute("CREATE EXTENSION IF NOT EXISTS vector;")

            c.execute("""
                CREATE TABLE IF NOT EXISTS statutes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    jurisdiction_code TEXT NOT NULL,
                    jurisdiction_name TEXT NOT NULL,
                    code_name TEXT NOT NULL,
                    law_code TEXT,
                    section_number TEXT NOT NULL,
                    canonical_citation TEXT NOT NULL,
                    title TEXT,
                    statute_language TEXT,
                    complete_statute TEXT NOT NULL,
                    plain_english_summary TEXT,
                    source_url TEXT NOT NULL,
                    source_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (jurisdiction_code, code_name, section_number)
                );
            """)

            c.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    document_type TEXT NOT NULL,
                    statute_id UUID REFERENCES statutes(id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    embedding vector(1536),
                    embedding_model TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (document_type, statute_id, chunk_index)
                );
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_statutes_jurisdiction_section
                ON statutes (jurisdiction_code, section_number);
            """)
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_statute_id
                ON documents (statute_id);
            """)
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_embedding
                ON documents
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)

    def upsert_statute(
        self,
        *,
        jurisdiction_code: str,
        jurisdiction_name: str,
        code_name: str,
        law_code: Optional[str],
        section_number: str,
        canonical_citation: str,
        title: Optional[str],
        statute_language: Optional[str],
        complete_statute: str,
        plain_english_summary: Optional[str],
        source_url: str,
        source_name: Optional[str],
    ) -> str:
        with self.conn() as c:
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
                ON CONFLICT (jurisdiction_code, code_name, section_number)
                DO UPDATE SET
                    jurisdiction_name = EXCLUDED.jurisdiction_name,
                    law_code = EXCLUDED.law_code,
                    canonical_citation = EXCLUDED.canonical_citation,
                    title = EXCLUDED.title,
                    statute_language = EXCLUDED.statute_language,
                    complete_statute = EXCLUDED.complete_statute,
                    plain_english_summary = EXCLUDED.plain_english_summary,
                    source_url = EXCLUDED.source_url,
                    source_name = EXCLUDED.source_name,
                    updated_at = NOW()
                RETURNING id;
                """,
                (
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
                    source_name,
                ),
            ).fetchone()
        return str(row["id"])

    def insert_document_chunk(
        self,
        *,
        document_type: str,
        statute_id: str,
        chunk_index: int,
        chunk_text: str,
        embedding: Optional[list[float]] = None,
        embedding_model: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        embedding_literal: Optional[str] = None
        if embedding:
            embedding_literal = "[" + ",".join(f"{float(v):.8f}" for v in embedding) + "]"

        with self.conn() as c:
            row = c.execute(
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
                ON CONFLICT (document_type, statute_id, chunk_index)
                DO UPDATE SET
                    chunk_text = EXCLUDED.chunk_text,
                    embedding = EXCLUDED.embedding,
                    embedding_model = EXCLUDED.embedding_model,
                    metadata = EXCLUDED.metadata
                RETURNING id;
                """,
                (
                    document_type,
                    statute_id,
                    chunk_index,
                    chunk_text,
                    embedding_literal,
                    embedding_model,
                    Json(metadata or {}),
                ),
            ).fetchone()
        return str(row["id"])

    def list_statutes(self, jurisdiction_code: Optional[str] = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
                id,
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
                source_name,
                created_at,
                updated_at
            FROM statutes
        """
        params: tuple[Any, ...] = ()
        if jurisdiction_code:
            sql += " WHERE jurisdiction_code = %s"
            params = (jurisdiction_code,)
        sql += " ORDER BY jurisdiction_code, section_number"

        with self.conn() as c:
            rows = c.execute(sql, params).fetchall()
        return [self._normalize_row(row) for row in rows]

    @staticmethod
    def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        for key in ("created_at", "updated_at"):
            value = out.get(key)
            if isinstance(value, datetime):
                out[key] = value.isoformat()
        return out
