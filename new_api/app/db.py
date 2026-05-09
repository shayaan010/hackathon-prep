from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row


class PostgresStore:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
        if not self.dsn:
            raise ValueError("POSTGRES_DSN (or DATABASE_URL) is required.")
        self._schema_path = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

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
        sql = self._schema_path.read_text(encoding="utf-8")
        with self.conn() as c:
            c.execute(sql)

    def stats(self) -> dict[str, int]:
        with self.conn() as c:
            statutes = c.execute("SELECT COUNT(*) AS n FROM statutes").fetchone()["n"]
            documents = c.execute("SELECT COUNT(*) AS n FROM documents").fetchone()["n"]
        return {
            "statutes": int(statutes),
            "documents": int(documents),
        }

    def list_statutes(self) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute(
                """
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
                    updated_at
                FROM statutes
                ORDER BY jurisdiction_code, section_number
                """
            ).fetchall()
        return [dict(r) for r in rows]
