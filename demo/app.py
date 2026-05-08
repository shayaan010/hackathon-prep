"""
Minimal Streamlit demo app.

Shows a query box, runs semantic search over indexed documents, and displays
results with extracted fields and source links. The source-quote highlighting
is the credibility play for judges - they can click through to the original.

Run with:
    uv run streamlit run demo/app.py

Make sure you've ingested data first (see top of this file for a quick seed).
"""
import json
import os
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from store.db import Database
from search.semantic import SemanticIndex


DB_PATH = os.environ.get("HACKATHON_DB", "hackathon.db")


@st.cache_resource
def get_db():
    db = Database(DB_PATH)
    db.init_schema()
    return db


@st.cache_resource
def get_index(_db):
    return SemanticIndex(_db)


def main():
    st.set_page_config(page_title="PI Legal Research", layout="wide")
    st.title("PI Legal Research Stack")

    db = get_db()
    idx = get_index(db)

    stats = db.stats()

    with st.sidebar:
        st.header("Index stats")
        st.metric("Documents", stats["documents"])
        st.metric("Chunks", stats["chunks"])
        st.metric("Extractions", stats["extractions"])

        if stats["documents"] == 0:
            st.warning(
                "No documents indexed yet. Run an ingestion script to populate the DB."
            )

    query = st.text_input(
        "Search the corpus",
        placeholder="e.g. medical malpractice spinal injury California",
    )
    top_k = st.slider("Results", min_value=1, max_value=20, value=5)

    if not query:
        st.info("Enter a query above to search.")
        return

    with st.spinner("Searching..."):
        hits = idx.search(query, top_k=top_k)

    if not hits:
        st.warning("No results found.")
        return

    st.subheader(f"{len(hits)} results")

    for hit in hits:
        doc = db.get_document(hit["doc_id"])
        score = hit["score"]
        source_url = doc["source_url"] if doc else "(unknown)"
        metadata = doc["metadata"] if doc else {}

        with st.container(border=True):
            cols = st.columns([3, 1])
            with cols[0]:
                st.markdown(f"**Score:** `{score:.3f}`")
                st.markdown(f"**Source:** [{source_url}]({source_url})")
                if metadata:
                    with st.expander("Metadata"):
                        st.json(metadata)
            with cols[1]:
                st.caption(f"Chunk #{hit['chunk_idx']}")
                st.caption(f"chars {hit['char_start']}–{hit['char_end']}")

            st.markdown("**Matched text:**")
            st.markdown(f"> {hit['text']}")

            # Show any extractions tied to this document
            extractions = db.get_extractions_for_doc(hit["doc_id"])
            if extractions:
                with st.expander(f"Extracted data ({len(extractions)})"):
                    for ext in extractions:
                        st.markdown(f"**{ext['schema_name']}**")
                        st.json(ext["data"])
                        if ext["source_quote"]:
                            st.caption(f"Source quote: _{ext['source_quote']}_")


if __name__ == "__main__":
    main()
