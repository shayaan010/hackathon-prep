from __future__ import annotations

import os
import re
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

    def list_statutes(
        self,
        *,
        q: Optional[str] = None,
        jurisdiction: Optional[str] = None,
        factors: Optional[list[str]] = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        """Search the statutes table with three optional filters.

        - ``q``: AND-tokenized ILIKE over title / canonical_citation / full text.
        - ``jurisdiction``: comma-separated 2-letter codes (e.g. "CA,NY").
        - ``factors``: any-of via Postgres array overlap (``factors && ARRAY[...]``).

        Returns ``(rows, total)`` where ``total`` is the unpaginated match count
        so the FE can render "50 of 1,234".
        """
        where: list[str] = []
        params: list[Any] = []

        if q and q.strip():
            tokens = [t for t in re.split(r"\s+", q.strip()) if t]
            for tok in tokens:
                like = f"%{tok}%"
                where.append(
                    "(title ILIKE %s OR canonical_citation ILIKE %s "
                    "OR complete_statute ILIKE %s)"
                )
                params.extend([like, like, like])

        if jurisdiction and jurisdiction.strip():
            codes = [
                c.strip().upper()
                for c in jurisdiction.split(",")
                if c.strip()
            ]
            if codes:
                where.append("jurisdiction_code = ANY(%s)")
                params.append(codes)

        if factors:
            cleaned = [f for f in (s.strip() for s in factors) if f]
            if cleaned:
                where.append("factors && %s::text[]")
                params.append(cleaned)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        with self.conn() as c:
            total_row = c.execute(
                f"SELECT COUNT(*) AS n FROM statutes{where_sql}", params
            ).fetchone()
            total = int(total_row["n"]) if total_row else 0

            rows = c.execute(
                f"""
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
                    factors,
                    updated_at
                FROM statutes
                {where_sql}
                ORDER BY jurisdiction_code, section_number
                LIMIT %s
                """,
                params + [int(limit)],
            ).fetchall()

        return [dict(r) for r in rows], total

    # ------------------------------------------------------------------
    # Tagging helpers (used by scripts/tag_statute_factors.py)
    # ------------------------------------------------------------------

    def list_untagged_statutes(
        self,
        *,
        jurisdiction: Optional[str] = None,
        limit: Optional[int] = None,
        retag: bool = False,
    ) -> list[dict[str, Any]]:
        """Statutes that still need factor tagging.

        ``retag=True`` ignores the ``factors`` column and returns everything
        (used by ``--retag``). Without it we skip rows that already have at
        least one factor — that's what makes the classifier resumable.
        """
        params: list[Any] = []
        where: list[str] = []
        if not retag:
            where.append("cardinality(factors) = 0")
        if jurisdiction:
            where.append("jurisdiction_code = %s")
            params.append(jurisdiction.upper())
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT %s"
            params.append(int(limit))

        sql = f"""
            SELECT
                id,
                jurisdiction_code,
                code_name,
                section_number,
                canonical_citation,
                title,
                complete_statute,
                factors
            FROM statutes
            {where_sql}
            ORDER BY jurisdiction_code, section_number
            {limit_sql}
        """
        with self.conn() as c:
            rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def update_statute_factors(self, statute_id: str, factors: list[str]) -> None:
        with self.conn() as c:
            c.execute(
                """
                UPDATE statutes
                SET factors = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (list(factors), statute_id),
            )

    # ------------------------------------------------------------------
    # Tool-facing helpers (used by /api/chat tool dispatch)
    # ------------------------------------------------------------------

    def search_statutes(
        self,
        query: Optional[str] = None,
        jurisdiction: Optional[str] = None,
        code: Optional[str] = None,
        factors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Keyword + factor search for the chat tool.

        At least one of ``query`` / ``factors`` should be set, otherwise the
        result is a meaningless top-N. Multi-word ``query`` is AND-ed across
        title / citation / full text. ``factors`` is any-of via array overlap.
        Returns short snippets — caller follows up with ``get_statute(id)``.
        """
        where_parts: list[str] = []
        params: list[Any] = []

        tokens = [t for t in re.split(r"\s+", (query or "").strip()) if t]
        for tok in tokens:
            like = f"%{tok}%"
            where_parts.append(
                "(title ILIKE %s OR canonical_citation ILIKE %s "
                "OR complete_statute ILIKE %s)"
            )
            params.extend([like, like, like])

        if jurisdiction:
            where_parts.append("jurisdiction_code = %s")
            params.append(jurisdiction.upper())
        if code:
            where_parts.append("code_name ILIKE %s")
            params.append(f"%{code}%")
        if factors:
            cleaned = [f for f in (s.strip() for s in factors) if f]
            if cleaned:
                where_parts.append("factors && %s::text[]")
                params.append(cleaned)

        if not where_parts:
            return []

        where_sql = " AND ".join(where_parts)
        params.append(int(limit))
        sql = f"""
            SELECT
                id,
                jurisdiction_code,
                jurisdiction_name,
                code_name,
                section_number,
                canonical_citation,
                title,
                LEFT(complete_statute, 500) AS snippet,
                factors,
                source_url
            FROM statutes
            WHERE {where_sql}
            ORDER BY jurisdiction_code, section_number
            LIMIT %s
        """
        with self.conn() as c:
            rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_statute(self, statute_id: str) -> Optional[dict[str, Any]]:
        if not statute_id:
            return None
        with self.conn() as c:
            row = c.execute(
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
                    complete_statute,
                    source_url,
                    source_name,
                    factors,
                    updated_at
                FROM statutes
                WHERE id = %s
                """,
                [statute_id],
            ).fetchone()
        return dict(row) if row else None

    def list_jurisdictions(self) -> list[dict[str, Any]]:
        with self.conn() as c:
            rows = c.execute(
                """
                SELECT
                    jurisdiction_code,
                    jurisdiction_name,
                    code_name,
                    COUNT(*) AS statute_count
                FROM statutes
                GROUP BY jurisdiction_code, jurisdiction_name, code_name
                ORDER BY jurisdiction_code, code_name
                """
            ).fetchall()
        return [dict(r) for r in rows]
