"""
Semantic search: chunk → embed → store → query.

Uses sentence-transformers for local embeddings (no API needed for this part).
The model `all-MiniLM-L6-v2` is small (~80MB), fast, and good enough for
hackathon-scale retrieval.

Storage is just SQLite with embedding BLOBs (numpy arrays serialized to bytes).
At hackathon scale (10k–100k chunks), brute-force cosine over all chunks is fine
and avoids the complexity of pgvector / Chroma / etc.

Usage:
    from search.semantic import SemanticIndex
    from store.db import Database

    db = Database("hackathon.db")
    db.init_schema()

    idx = SemanticIndex(db)

    # Index a document (chunks + embeds + stores)
    doc_id = db.insert_document(source_url="...", raw_text=long_text)
    idx.index_document(doc_id, long_text)

    # Query
    hits = idx.search("medical malpractice involving anesthesia", top_k=10)
    for hit in hits:
        print(hit["score"], hit["text"][:200])
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from store.db import Database


# Default model: small, fast, good for English
DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Chunking parameters - good defaults for legal/document text
DEFAULT_CHUNK_SIZE = 500   # characters, not tokens (rougher but simpler)
DEFAULT_CHUNK_OVERLAP = 100


def chunk_text(
    text: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overlap: int = DEFAULT_CHUNK_OVERLAP,
) -> list[tuple[str, int, int]]:
    """
    Split text into overlapping chunks.

    Tries to break on paragraph boundaries first, then sentence, then characters.
    Returns list of (chunk_text, char_start, char_end) tuples.
    """
    if len(text) <= chunk_size:
        return [(text, 0, len(text))]

    chunks = []
    pos = 0
    text_len = len(text)

    while pos < text_len:
        end = min(pos + chunk_size, text_len)

        if end < text_len:
            # Try to break on a paragraph
            para_break = text.rfind("\n\n", pos, end)
            if para_break > pos + chunk_size // 2:
                end = para_break + 2
            else:
                # Try sentence boundary
                sent_break = max(
                    text.rfind(". ", pos, end),
                    text.rfind("! ", pos, end),
                    text.rfind("? ", pos, end),
                )
                if sent_break > pos + chunk_size // 2:
                    end = sent_break + 2

        chunk = text[pos:end].strip()
        if chunk:
            chunks.append((chunk, pos, end))

        if end >= text_len:
            break

        # Overlap: step back a bit for next chunk
        pos = end - overlap

    return chunks


class SemanticIndex:
    """
    Wraps a sentence-transformers model + a Database for semantic search.

    The model is loaded lazily on first use to keep import fast.
    """

    def __init__(
        self,
        db: "Database",
        model_name: str = DEFAULT_MODEL,
    ):
        self.db = db
        self.model_name = model_name
        self._model = None

    @property
    def model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def embed(self, texts: list[str]) -> np.ndarray:
        """Embed a batch of texts. Returns (n, dim) array of L2-normalized vectors."""
        embeddings = self.model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,  # so cosine == dot product
            show_progress_bar=False,
        )
        return embeddings.astype(np.float32)

    def index_document(
        self,
        doc_id: int,
        text: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        overlap: int = DEFAULT_CHUNK_OVERLAP,
    ) -> int:
        """
        Chunk, embed, and store all chunks for a document.

        Returns the number of chunks created.
        """
        chunks = chunk_text(text, chunk_size=chunk_size, overlap=overlap)
        if not chunks:
            return 0

        chunk_texts = [c[0] for c in chunks]
        embeddings = self.embed(chunk_texts)

        for idx, ((chunk, start, end), emb) in enumerate(zip(chunks, embeddings)):
            self.db.insert_chunk(
                doc_id=doc_id,
                chunk_idx=idx,
                text=chunk,
                char_start=start,
                char_end=end,
                embedding=emb.tobytes(),
            )

        return len(chunks)

    def search(
        self,
        query: str,
        top_k: int = 10,
        doc_ids: Optional[list[int]] = None,
    ) -> list[dict]:
        """
        Find the top_k chunks most similar to the query.

        If doc_ids is provided, search only within those documents.
        Returns list of dicts with: id, doc_id, text, score, char_start, char_end.
        """
        query_emb = self.embed([query])[0]  # shape: (dim,)

        # Pull all relevant chunks from DB
        if doc_ids is not None:
            all_chunks = []
            for did in doc_ids:
                all_chunks.extend(self.db.get_chunks_for_doc(did))
        else:
            all_chunks = self.db.all_chunks()

        if not all_chunks:
            return []

        # Decode embeddings and stack
        valid = [c for c in all_chunks if c.get("embedding")]
        if not valid:
            return []

        emb_matrix = np.stack([
            np.frombuffer(c["embedding"], dtype=np.float32) for c in valid
        ])

        # Cosine similarity (already normalized, so just dot product)
        scores = emb_matrix @ query_emb

        # Top-k indices
        k = min(top_k, len(scores))
        top_idx = np.argpartition(-scores, k - 1)[:k]
        top_idx = top_idx[np.argsort(-scores[top_idx])]

        results = []
        for i in top_idx:
            chunk = valid[i]
            results.append({
                "id": chunk["id"],
                "doc_id": chunk["doc_id"],
                "text": chunk["text"],
                "score": float(scores[i]),
                "char_start": chunk["char_start"],
                "char_end": chunk["char_end"],
                "chunk_idx": chunk["chunk_idx"],
            })

        return results


# Quick smoke test
if __name__ == "__main__":
    from store.db import Database
    import os
    import tempfile

    tmpdir = tempfile.mkdtemp()
    dbpath = os.path.join(tmpdir, "test.db")

    db = Database(dbpath)
    db.init_schema()

    sample = """
    The plaintiff alleged severe spinal injuries resulting from a slip-and-fall
    at the defendant's grocery store on January 12, 2023. Medical bills exceeded
    $87,000 within the first six months. The defendant argued contributory negligence.

    In an unrelated matter, the court ruled on a product liability case involving
    a defective lawn mower. The plaintiff suffered lacerations requiring 32 stitches.
    Settlement was reached for $45,000.

    A third case concerned medical malpractice during anesthesia administration.
    The patient suffered hypoxic brain injury. The jury awarded $2.3 million.
    """

    doc_id = db.insert_document(source_url="test://doc1", raw_text=sample)
    idx = SemanticIndex(db)
    n = idx.index_document(doc_id, sample, chunk_size=200, overlap=40)
    print(f"Indexed {n} chunks")

    for query in ["slip and fall", "anesthesia injury", "lawn mower defect"]:
        print(f"\nQuery: {query!r}")
        hits = idx.search(query, top_k=2)
        for h in hits:
            print(f"  [{h['score']:.3f}] {h['text'][:120]}...")

    import shutil
    shutil.rmtree(tmpdir)
