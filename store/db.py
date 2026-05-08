"""
SQLite storage layer with source tracking.

Three tables:
  - documents: raw fetched content (HTML, PDF text, JSON) with source URL
  - chunks: text chunks with embeddings for semantic search
  - extractions: structured data extracted by LLM, with source quote + char range

The source_quote and source_char_range columns on extractions are non-negotiable:
they're how you prove every claim in your demo traces back to real data, no fabrication.

Usage:
    from store.db import Database

    db = Database("hackathon.db")
    db.init_schema()

    doc_id = db.insert_document(
        source_url="https://courtlistener.com/opinion/123",
        raw_text="Full opinion text here...",
        metadata={"court": "CA Supreme Court", "date": "2023-04-15"},
    )

    db.insert_extraction(
        doc_id=doc_id,
        schema_name="CaseMetadata",
        data={"plaintiff": "Smith", "damages": 250000},
        source_quote="Plaintiff Smith was awarded $250,000 in damages",
        source_char_range=(1234, 1278),
    )
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union


def _now_iso() -> str:
    """UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, path: Union[str, Path] = "hackathon.db"):
        self.path = str(path)

    @contextmanager
    def conn(self):
        """Context manager for database connections."""
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self) -> None:
        """Create tables if they don't exist. Idempotent."""
        with self.conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_url TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    raw_text TEXT NOT NULL,
                    metadata TEXT,  -- JSON blob
                    UNIQUE(source_url)
                );

                CREATE INDEX IF NOT EXISTS idx_documents_source
                    ON documents(source_url);

                CREATE TABLE IF NOT EXISTS chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id INTEGER NOT NULL,
                    chunk_idx INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    embedding BLOB,  -- numpy array as bytes
                    char_start INTEGER NOT NULL,
                    char_end INTEGER NOT NULL,
                    FOREIGN KEY(doc_id) REFERENCES documents(id)
                );

                CREATE INDEX IF NOT EXISTS idx_chunks_doc
                    ON chunks(doc_id);

                CREATE TABLE IF NOT EXISTS extractions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id INTEGER NOT NULL,
                    schema_name TEXT NOT NULL,
                    data TEXT NOT NULL,  -- JSON blob of extracted fields
                    source_quote TEXT,   -- the verbatim text that supports the extraction
                    source_char_start INTEGER,
                    source_char_end INTEGER,
                    extracted_at TEXT NOT NULL,
                    FOREIGN KEY(doc_id) REFERENCES documents(id)
                );

                CREATE INDEX IF NOT EXISTS idx_extractions_doc
                    ON extractions(doc_id);

                CREATE INDEX IF NOT EXISTS idx_extractions_schema
                    ON extractions(schema_name);
            """)

    def insert_document(
        self,
        source_url: str,
        raw_text: str,
        metadata: Optional[dict] = None,
    ) -> int:
        """
        Insert a document. Returns the doc_id.

        If source_url already exists, updates the existing row and returns its id.
        """
        meta_json = json.dumps(metadata) if metadata else None
        now = _now_iso()

        with self.conn() as c:
            cursor = c.execute(
                """INSERT INTO documents (source_url, fetched_at, raw_text, metadata)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(source_url) DO UPDATE SET
                       fetched_at = excluded.fetched_at,
                       raw_text = excluded.raw_text,
                       metadata = excluded.metadata
                   RETURNING id""",
                (source_url, now, raw_text, meta_json),
            )
            row = cursor.fetchone()
            return row["id"]

    def get_document(self, doc_id: int) -> Optional[dict]:
        with self.conn() as c:
            row = c.execute(
                "SELECT * FROM documents WHERE id = ?", (doc_id,)
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
            return d

    def get_document_by_url(self, source_url: str) -> Optional[dict]:
        with self.conn() as c:
            row = c.execute(
                "SELECT * FROM documents WHERE source_url = ?", (source_url,)
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
            return d

    def insert_chunk(
        self,
        doc_id: int,
        chunk_idx: int,
        text: str,
        char_start: int,
        char_end: int,
        embedding: Optional[bytes] = None,
    ) -> int:
        with self.conn() as c:
            cursor = c.execute(
                """INSERT INTO chunks
                   (doc_id, chunk_idx, text, embedding, char_start, char_end)
                   VALUES (?, ?, ?, ?, ?, ?)
                   RETURNING id""",
                (doc_id, chunk_idx, text, embedding, char_start, char_end),
            )
            return cursor.fetchone()["id"]

    def get_chunks_for_doc(self, doc_id: int) -> list[dict]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM chunks WHERE doc_id = ? ORDER BY chunk_idx", (doc_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def all_chunks(self) -> list[dict]:
        """Return all chunks across all documents (for global semantic search)."""
        with self.conn() as c:
            rows = c.execute("SELECT * FROM chunks").fetchall()
            return [dict(r) for r in rows]

    def insert_extraction(
        self,
        doc_id: int,
        schema_name: str,
        data: dict,
        source_quote: Optional[str] = None,
        source_char_range: Optional[tuple[int, int]] = None,
    ) -> int:
        char_start, char_end = source_char_range or (None, None)
        now = _now_iso()
        with self.conn() as c:
            cursor = c.execute(
                """INSERT INTO extractions
                   (doc_id, schema_name, data, source_quote,
                    source_char_start, source_char_end, extracted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   RETURNING id""",
                (
                    doc_id,
                    schema_name,
                    json.dumps(data),
                    source_quote,
                    char_start,
                    char_end,
                    now,
                ),
            )
            return cursor.fetchone()["id"]

    def get_extractions_for_doc(self, doc_id: int) -> list[dict]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM extractions WHERE doc_id = ?", (doc_id,)
            ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                d["data"] = json.loads(d["data"])
                results.append(d)
            return results

    def get_extractions_by_schema(self, schema_name: str) -> list[dict]:
        with self.conn() as c:
            rows = c.execute(
                """SELECT e.*, d.source_url
                   FROM extractions e
                   JOIN documents d ON e.doc_id = d.id
                   WHERE e.schema_name = ?""",
                (schema_name,),
            ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                d["data"] = json.loads(d["data"])
                results.append(d)
            return results

    def stats(self) -> dict:
        """Quick counts for sanity-checking."""
        with self.conn() as c:
            return {
                "documents": c.execute("SELECT COUNT(*) FROM documents").fetchone()[0],
                "chunks": c.execute("SELECT COUNT(*) FROM chunks").fetchone()[0],
                "extractions": c.execute("SELECT COUNT(*) FROM extractions").fetchone()[0],
            }


# Quick smoke test
if __name__ == "__main__":
    db = Database("/tmp/test.db")
    db.init_schema()

    doc_id = db.insert_document(
        source_url="https://example.com/test",
        raw_text="The plaintiff was awarded $50,000 in damages.",
        metadata={"court": "Test Court"},
    )
    print(f"Inserted doc {doc_id}")

    db.insert_extraction(
        doc_id=doc_id,
        schema_name="DamageAward",
        data={"amount": 50000, "currency": "USD"},
        source_quote="The plaintiff was awarded $50,000 in damages.",
        source_char_range=(0, 47),
    )

    print("Stats:", db.stats())
    print("Doc:", db.get_document(doc_id))
    print("Extractions:", db.get_extractions_for_doc(doc_id))

    import os
    os.remove("/tmp/test.db")
