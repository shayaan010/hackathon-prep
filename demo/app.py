import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from store.db import Database
from search.semantic import SemanticIndex

DB_PATH = os.environ.get("HACKATHON_DB", "hackathon.db")

CATEGORIES = [
    "Improper Turning", "Improper Passing", "Failure to Yield the Right-of-Way",
    "Improper Lane of Travel", "Improper Stopping", "DUI/DWI",
    "Fleeing the Scene of a Collision", "Failure to Maintain Lane",
    "Driving Too Fast For Conditions", "Using a Wireless Telephone/Texting While Driving",
    "Fleeing a Police Officer", "Failure to Obey Traffic Control Device",
    "Following Too Closely", "Failure to Yield at a Yield Sign",
    "Improper Starting", "Reckless Driving", "Failure to Use/Activate Horn"
]

SUGGESTED_QUERIES = [
    "Improper turning",
    "DUI / DWI",
    "Reckless driving",
    "Failure to yield",
    "Following too closely",
]

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Global background: atmospheric gradient ─────────────────── */
.stApp {
    background:
        radial-gradient(1200px 600px at 80% -10%, rgba(56, 189, 248, 0.18), transparent 60%),
        radial-gradient(900px 700px at 10% 110%, rgba(59, 130, 246, 0.22), transparent 55%),
        linear-gradient(180deg, #050912 0%, #0a1428 45%, #0f1d3a 100%);
    background-attachment: fixed;
    color: #e2e8f0;
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
}

/* hide default streamlit chrome */
#MainMenu, footer { visibility: hidden; }
header[data-testid="stHeader"] { background: transparent; }

.main .block-container {
    padding-top: 3rem;
    padding-bottom: 4rem;
    max-width: 1100px;
}

h1, h2, h3, h4, h5, h6, p, span, label, div {
    color: #e2e8f0;
}
h1, h2, h3 {
    font-family: 'Inter', sans-serif;
    letter-spacing: -0.02em;
    font-weight: 600;
}

/* ── Hero ─────────────────────────────────────────────────────── */
.hero {
    text-align: center;
    margin: 1.5rem auto 2rem auto;
}
.hero .greeting {
    font-size: 2.6rem;
    font-weight: 600;
    color: #f8fafc;
    line-height: 1.15;
    letter-spacing: -0.02em;
    margin-bottom: 0.4rem;
}
.hero .greeting .accent {
    background: linear-gradient(90deg, #93c5fd, #67e8f9);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.hero .sub {
    font-size: 1.05rem;
    color: #94a3b8;
    font-weight: 400;
}

/* ── Search input as pill ─────────────────────────────────────── */
div[data-testid="stTextInput"] > div > div {
    background: rgba(15, 23, 42, 0.55) !important;
    border: 1px solid rgba(148, 163, 184, 0.18) !important;
    border-radius: 999px !important;
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    box-shadow: 0 8px 32px rgba(2, 6, 23, 0.4);
    transition: border-color 0.2s, box-shadow 0.2s;
}
div[data-testid="stTextInput"] > div > div:focus-within {
    border-color: rgba(96, 165, 250, 0.5) !important;
    box-shadow: 0 8px 32px rgba(2, 6, 23, 0.5), 0 0 0 4px rgba(96, 165, 250, 0.08);
}
div[data-testid="stTextInput"] input {
    background: transparent !important;
    color: #e2e8f0 !important;
    font-size: 1rem !important;
    padding: 0.95rem 1.4rem !important;
    border: none !important;
}
div[data-testid="stTextInput"] input::placeholder {
    color: #64748b !important;
}

/* number input — make it match */
div[data-testid="stNumberInput"] > div > div {
    background: rgba(15, 23, 42, 0.55) !important;
    border: 1px solid rgba(148, 163, 184, 0.18) !important;
    border-radius: 999px !important;
    backdrop-filter: blur(14px);
}
div[data-testid="stNumberInput"] input {
    background: transparent !important;
    color: #e2e8f0 !important;
    border: none !important;
    text-align: center;
}

/* selectbox */
div[data-testid="stSelectbox"] > div > div {
    background: rgba(15, 23, 42, 0.55) !important;
    border: 1px solid rgba(148, 163, 184, 0.18) !important;
    border-radius: 12px !important;
    color: #e2e8f0 !important;
}

/* ── Buttons (default) — pill outline ─────────────────────────── */
.stButton > button {
    background: rgba(15, 23, 42, 0.55);
    color: #e2e8f0;
    border: 1px solid rgba(148, 163, 184, 0.22);
    border-radius: 999px;
    padding: 0.5rem 1.2rem;
    font-weight: 500;
    font-size: 0.9rem;
    backdrop-filter: blur(10px);
    transition: all 0.18s ease;
}
.stButton > button:hover {
    background: rgba(30, 41, 59, 0.7);
    border-color: rgba(148, 163, 184, 0.45);
    color: #f8fafc;
    transform: translateY(-1px);
}
.stButton > button:active {
    transform: translateY(0);
}

/* ── Glass cards (st.container(border=True)) ──────────────────── */
div[data-testid="stVerticalBlockBorderWrapper"] {
    background: rgba(15, 23, 42, 0.45) !important;
    border: 1px solid rgba(148, 163, 184, 0.14) !important;
    border-radius: 18px !important;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    padding: 1.2rem 1.4rem !important;
    box-shadow: 0 8px 30px rgba(2, 6, 23, 0.35);
    transition: border-color 0.2s, transform 0.2s;
}
div[data-testid="stVerticalBlockBorderWrapper"]:hover {
    border-color: rgba(148, 163, 184, 0.28) !important;
}

/* ── Sidebar — dark glass ─────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: rgba(7, 11, 22, 0.85) !important;
    border-right: 1px solid rgba(148, 163, 184, 0.12);
    backdrop-filter: blur(20px);
}
[data-testid="stSidebar"] * {
    color: #cbd5e1;
}
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: #f1f5f9;
    font-size: 0.72rem !important;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-weight: 600;
    color: #94a3b8 !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.7rem !important;
    font-weight: 600;
    color: #f8fafc !important;
}
[data-testid="stMetricLabel"] {
    color: #64748b !important;
    font-size: 0.72rem !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* ── Tabs ─────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0.5rem;
    background: transparent;
    border-bottom: 1px solid rgba(148, 163, 184, 0.12);
    margin-bottom: 1.5rem;
}
.stTabs [data-baseweb="tab"] {
    padding: 0.6rem 1.2rem;
    color: #94a3b8;
    background: transparent !important;
    font-weight: 500;
    border: none;
    border-radius: 0;
}
.stTabs [aria-selected="true"] {
    color: #f8fafc !important;
    border-bottom: 2px solid #60a5fa !important;
}

/* ── Pills (tags / scores / chips) ────────────────────────────── */
.tag-pill {
    display: inline-block;
    padding: 3px 12px;
    background: rgba(59, 130, 246, 0.12);
    color: #93c5fd;
    border: 1px solid rgba(96, 165, 250, 0.25);
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 500;
    margin-right: 6px;
}
.score-pill {
    display: inline-block;
    padding: 3px 12px;
    background: rgba(16, 185, 129, 0.12);
    color: #6ee7b7;
    border: 1px solid rgba(52, 211, 153, 0.28);
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
}
.meta-line {
    color: #64748b;
    font-size: 0.82rem;
    font-variant-numeric: tabular-nums;
}
.source-link a {
    color: #94a3b8;
    text-decoration: none;
    border-bottom: 1px dotted rgba(148, 163, 184, 0.4);
    font-size: 0.85rem;
}
.source-link a:hover {
    color: #cbd5e1;
    border-bottom-color: #cbd5e1;
}
.quote-block {
    border-left: 2px solid rgba(96, 165, 250, 0.5);
    padding: 0.5rem 1rem;
    color: #cbd5e1;
    font-style: italic;
    background: rgba(15, 23, 42, 0.4);
    margin: 0.6rem 0;
    border-radius: 0 6px 6px 0;
}

/* blockquote (matched text) */
blockquote {
    border-left: 2px solid rgba(148, 163, 184, 0.3) !important;
    background: rgba(15, 23, 42, 0.3);
    padding: 0.6rem 1rem !important;
    border-radius: 0 6px 6px 0;
    color: #cbd5e1 !important;
}

/* expanders */
div[data-testid="stExpander"] {
    background: transparent;
    border: 1px solid rgba(148, 163, 184, 0.14);
    border-radius: 12px;
}
div[data-testid="stExpander"] summary { color: #cbd5e1; }

/* alerts/info/warning */
div[data-baseweb="notification"] {
    background: rgba(15, 23, 42, 0.6) !important;
    border-radius: 12px;
}

.section-label {
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #64748b;
    font-size: 0.72rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
}

/* st.caption */
.stCaption, [data-testid="stCaptionContainer"] {
    color: #64748b !important;
}

/* ── Home button (top-left nav) ──────────────────────────────── */
div[data-testid="stHorizontalBlock"]:has(button[kind="secondary"][data-testid*="home_btn"]) {
    margin-bottom: -0.5rem;
}
.stButton > button[kind="secondary"]:has(+ *),
button[data-testid="baseButton-secondary"][kind="secondary"][aria-describedby],
[data-testid*="home_btn"] button {
    background: transparent !important;
    border: 1px solid rgba(148, 163, 184, 0.18) !important;
    color: #94a3b8 !important;
    font-size: 0.82rem !important;
    padding: 0.35rem 0.9rem !important;
    box-shadow: none !important;
}
[data-testid*="home_btn"] button:hover {
    background: rgba(15, 23, 42, 0.6) !important;
    color: #e2e8f0 !important;
    border-color: rgba(148, 163, 184, 0.35) !important;
}
</style>
"""


@st.cache_resource
def get_db():
    db = Database(DB_PATH)
    db.init_schema()
    with db.conn() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("""CREATE TABLE IF NOT EXISTS collection_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER REFERENCES collections(id),
            doc_id INTEGER,
            note TEXT DEFAULT '',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    return db


@st.cache_resource
def get_index(_db):
    return SemanticIndex(_db)


def get_collections(db):
    with db.conn() as c:
        return c.execute("SELECT id, name FROM collections ORDER BY created_at DESC").fetchall()


def create_collection(db, name):
    with db.conn() as c:
        c.execute("INSERT OR IGNORE INTO collections (name) VALUES (?)", (name,))


def add_to_collection(db, collection_id, doc_id, note=""):
    with db.conn() as c:
        existing = c.execute(
            "SELECT id FROM collection_items WHERE collection_id=? AND doc_id=?",
            (collection_id, doc_id)
        ).fetchone()
        if not existing:
            c.execute(
                "INSERT INTO collection_items (collection_id, doc_id, note) VALUES (?,?,?)",
                (collection_id, doc_id, note)
            )
            return True
        return False


def get_collection_items(db, collection_id):
    with db.conn() as c:
        return c.execute("""
            SELECT ci.doc_id, ci.note, ci.added_at
            FROM collection_items ci
            WHERE ci.collection_id = ?
            ORDER BY ci.added_at DESC
        """, (collection_id,)).fetchall()


_SECTION_QUERY_RE = re.compile(r"^\s*\d+(\.\d+)?(\([a-z0-9]\))?\s*$", re.I)


def is_section_query(q: str) -> bool:
    """True if the query looks like a statute section number (e.g. '22107', '21451(a)', '21451.5')."""
    return bool(_SECTION_QUERY_RE.match(q or ""))


def section_number_lookup(db, query: str) -> list[dict]:
    """Find documents whose metadata.section matches the query, return synthetic hits."""
    base = query.strip().split("(")[0].strip()
    with db.conn() as c:
        rows = c.execute("SELECT id, metadata FROM documents").fetchall()

    hits = []
    seen = set()
    for row in rows:
        meta_raw = row["metadata"]
        if not meta_raw:
            continue
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
        except (json.JSONDecodeError, TypeError):
            continue
        section = (meta.get("section") or "").split("(")[0].strip()
        if section != base or row["id"] in seen:
            continue
        seen.add(row["id"])
        chunks = db.get_chunks_for_doc(row["id"])
        if not chunks:
            continue
        c0 = chunks[0]
        hits.append({
            "id": c0["id"],
            "doc_id": row["id"],
            "text": c0["text"],
            "score": 1.0,
            "char_start": c0["char_start"],
            "char_end": c0["char_end"],
            "chunk_idx": c0["chunk_idx"],
            "is_direct_match": True,
        })
    return hits


def trim_chunk_for_display(text: str, started_mid: bool, ended_mid: bool) -> str:
    """Drop partial words at chunk edges and add ellipsis so display starts/ends cleanly."""
    if not text:
        return text
    text = text.strip()
    if started_mid:
        for i, ch in enumerate(text[:30]):
            if ch.isspace():
                text = text[i + 1:]
                break
        text = "… " + text.lstrip()
    if ended_mid and text and text[-1] not in '.!?"\')]':
        last_space = text.rfind(" ")
        if last_space > len(text) - 30:
            text = text[:last_space].rstrip() + " …"
    return text


def get_coverage(db):
    with db.conn() as c:
        rows = c.execute("SELECT data FROM extractions").fetchall()
    counts = {cat: 0 for cat in CATEGORIES}
    for row in rows:
        try:
            data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            cat = data.get("primary_category", "")
            if cat in counts:
                counts[cat] += 1
        except Exception:
            pass
    return counts


def render_hero():
    st.markdown(
        """
        <div class="hero">
            <div class="greeting">
                Welcome to your <span class="accent">Legal Research</span> workspace.
            </div>
            <div class="sub">
                Search the personal-injury statute corpus and assemble case files.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_suggestion_chips():
    st.markdown(
        '<div style="text-align:center; margin: 0.5rem 0 0.25rem 0;">'
        '<span class="section-label">Try a query</span></div>',
        unsafe_allow_html=True,
    )
    cols = st.columns(len(SUGGESTED_QUERIES))
    for i, q in enumerate(SUGGESTED_QUERIES):
        with cols[i]:
            if st.button(q, key=f"chip_{i}", use_container_width=True):
                st.session_state["_pending_query"] = q
                st.rerun()


def render_landing_cards(db, stats):
    coverage = get_coverage(db)
    collections = get_collections(db)
    covered = sum(1 for v in coverage.values() if v > 0)
    top_cats = sorted(coverage.items(), key=lambda x: -x[1])[:3]
    top_cats = [(k, v) for k, v in top_cats if v > 0]

    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown('<div class="section-label">Corpus</div>', unsafe_allow_html=True)
            st.markdown(
                f"<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>"
                f"{stats['documents']:,}</div>"
                f"<div class='meta-line'>documents · {stats['chunks']:,} chunks · "
                f"{stats['extractions']:,} extractions</div>",
                unsafe_allow_html=True,
            )
    with c2:
        with st.container(border=True):
            st.markdown('<div class="section-label">Coverage</div>', unsafe_allow_html=True)
            st.markdown(
                f"<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>"
                f"{covered}<span style='color:#64748b; font-size:1rem;'> / 17</span></div>"
                f"<div class='meta-line'>contributing-factor categories tagged</div>",
                unsafe_allow_html=True,
            )
    with c3:
        with st.container(border=True):
            st.markdown('<div class="section-label">Case files</div>', unsafe_allow_html=True)
            if collections:
                names = ", ".join(c["name"] for c in collections[:3])
                st.markdown(
                    f"<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>"
                    f"{len(collections)}</div>"
                    f"<div class='meta-line'>{names}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>0</div>"
                    "<div class='meta-line'>create one in the Case Files tab</div>",
                    unsafe_allow_html=True,
                )


def render_search_panel():
    cols = st.columns([5, 1])
    with cols[0]:
        query = st.text_input(
            "Query",
            placeholder="Ask about the corpus — e.g. improper turning while driving",
            label_visibility="collapsed",
            key="query",
        )
    with cols[1]:
        top_k = st.number_input(
            "Results", min_value=1, max_value=20, value=5, step=1,
            label_visibility="collapsed",
        )
    return query, top_k


def render_result(hit, doc, extractions, collections, db):
    score = hit["score"]
    source_url = doc["source_url"] if doc else "(unknown)"
    metadata = doc["metadata"] if doc else {}

    tag = None
    reasoning = ""
    source_quote = ""
    for ext in extractions:
        data = ext["data"]
        tag = data.get("primary_category", "")
        reasoning = data.get("reasoning", "")
        source_quote = ext.get("source_quote", "") or data.get("source_quote", "")
        break

    doc_text_len = len(doc.get("raw_text") or "") if doc else 0
    started_mid = hit["char_start"] > 0
    ended_mid = doc_text_len == 0 or hit["char_end"] < doc_text_len
    display_text = trim_chunk_for_display(hit["text"], started_mid, ended_mid)

    is_direct = hit.get("is_direct_match", False)

    with st.container(border=True):
        header_left, header_right = st.columns([4, 1])
        with header_left:
            if is_direct:
                pills = '<span class="score-pill">Exact match</span>'
            else:
                pills = f'<span class="score-pill">{score:.3f}</span>'
            if tag:
                pills += f' <span class="tag-pill">{tag}</span>'
            st.markdown(pills, unsafe_allow_html=True)

            citation = metadata.get("citation") if metadata else None
            section = metadata.get("section") if metadata else None
            if citation:
                title_line = f"**{citation}**"
                if section:
                    title_line += f" &nbsp;·&nbsp; §{section}"
                st.markdown(title_line, unsafe_allow_html=True)
            st.markdown(
                f'<div class="source-link meta-line">'
                f'<a href="{source_url}" target="_blank">{source_url}</a></div>',
                unsafe_allow_html=True,
            )
        with header_right:
            st.markdown(
                f'<div class="meta-line" style="text-align:right;">'
                f'Chunk #{hit["chunk_idx"]}<br>'
                f'chars {hit["char_start"]}–{hit["char_end"]}</div>',
                unsafe_allow_html=True,
            )

        st.markdown(f"> {display_text}")

        if source_quote:
            st.markdown(
                f'<div class="quote-block">{source_quote}</div>',
                unsafe_allow_html=True,
            )

        bottom_left, bottom_right = st.columns([3, 2])
        with bottom_left:
            if reasoning:
                with st.expander("Tag rationale"):
                    st.write(reasoning)
            if metadata:
                with st.expander("Document metadata"):
                    st.json(metadata)
        with bottom_right:
            if collections:
                with st.popover("Save to case file", use_container_width=True):
                    st.markdown(
                        '<div class="section-label" style="margin-bottom:0.4rem;">'
                        'Choose a case file</div>',
                        unsafe_allow_html=True,
                    )
                    for col in collections:
                        if st.button(
                            col["name"],
                            key=f"savecf_{hit['doc_id']}_{hit['chunk_idx']}_{col['id']}",
                            use_container_width=True,
                        ):
                            added = add_to_collection(db, col["id"], hit["doc_id"])
                            st.toast(
                                f"Saved to {col['name']}." if added
                                else f"Already in {col['name']}."
                            )
            else:
                st.caption("Create a case file to save results.")


def main():
    st.set_page_config(
        page_title="PI Legal Research",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    db = get_db()
    idx = get_index(db)
    stats = db.stats()

    with st.sidebar:
        st.markdown("## Corpus")
        st.metric("Documents", f"{stats['documents']:,}")
        st.metric("Chunks", f"{stats['chunks']:,}")
        st.metric("Extractions", f"{stats['extractions']:,}")
        st.markdown("---")
        st.markdown("## About")
        st.caption(
            "Semantic research surface for personal-injury statutes. "
            "Search, tag by contributing factor, and assemble case files."
        )

    if "_pending_query" in st.session_state:
        st.session_state["query"] = st.session_state.pop("_pending_query")

    nav_cols = st.columns([1, 9])
    with nav_cols[0]:
        if st.button("← Home", key="home_btn"):
            st.session_state["_pending_query"] = ""
            st.rerun()

    tab1, tab2 = st.tabs(["Search", "Case Files"])

    # ── TAB 1: SEARCH ──────────────────────────────────────────────
    with tab1:
        query_in_state = st.session_state.get("query", "")

        if not query_in_state:
            render_hero()

        query, top_k = render_search_panel()

        if not query:
            render_suggestion_chips()
            st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)
            render_landing_cards(db, stats)
        else:
            with st.spinner("Searching corpus..."):
                if is_section_query(query):
                    direct = section_number_lookup(db, query)
                    hits = direct if direct else idx.search(query, top_k=top_k)
                else:
                    hits = idx.search(query, top_k=top_k)

            collections = get_collections(db)
            if not hits:
                st.markdown(
                    '<div style="text-align:center; padding:3rem 1rem; color:#64748b;">'
                    'No results found for that query.</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="meta-line" style="margin: 1rem 0;">'
                    f'{len(hits)} result{"s" if len(hits) != 1 else ""}</div>',
                    unsafe_allow_html=True,
                )
                for hit in hits:
                    doc = db.get_document(hit["doc_id"])
                    extractions = db.get_extractions_for_doc(hit["doc_id"])
                    render_result(hit, doc, extractions, collections, db)

    # ── TAB 2: CASE FILES ──────────────────────────────────────────
    with tab2:
        st.markdown('<div class="section-label">Case files</div>', unsafe_allow_html=True)

        with st.container(border=True):
            cf_a, cf_b = st.columns([4, 1])
            with cf_a:
                new_name = st.text_input(
                    "New case file",
                    placeholder="New case file name — e.g. Smith v. Acme 2026",
                    label_visibility="collapsed",
                    key="new_case_file",
                )
            with cf_b:
                if st.button("Create", use_container_width=True, key="create_cf"):
                    if new_name.strip():
                        create_collection(db, new_name.strip())
                        st.toast(f"Created: {new_name}")
                        st.rerun()

        st.markdown('<div style="height: 1rem;"></div>', unsafe_allow_html=True)

        collections = get_collections(db)
        if collections:
            col_names = [c["name"] for c in collections]
            selected_name = st.selectbox("Open case file", col_names)
            selected_id = next(c["id"] for c in collections if c["name"] == selected_name)

            items = get_collection_items(db, selected_id)
            if items:
                st.markdown(
                    f'<div class="meta-line" style="margin: 0.5rem 0 1rem 0;">'
                    f'{len(items)} statute{"s" if len(items) != 1 else ""} saved</div>',
                    unsafe_allow_html=True,
                )
                for item in items:
                    doc = db.get_document(item["doc_id"])
                    if not doc:
                        continue
                    source_url = doc["source_url"]
                    metadata = doc["metadata"] or {}
                    citation = metadata.get("citation", source_url)
                    section = metadata.get("section", "")

                    extractions = db.get_extractions_for_doc(item["doc_id"])
                    tag = ""
                    for ext in extractions:
                        tag = ext["data"].get("primary_category", "")
                        break

                    with st.container(border=True):
                        if tag:
                            st.markdown(f'<span class="tag-pill">{tag}</span>', unsafe_allow_html=True)
                        title_line = f"**{citation}**"
                        if section:
                            title_line += f" &nbsp;·&nbsp; §{section}"
                        st.markdown(title_line, unsafe_allow_html=True)
                        st.markdown(
                            f'<div class="source-link meta-line">'
                            f'<a href="{source_url}" target="_blank">{source_url}</a></div>',
                            unsafe_allow_html=True,
                        )
                        if item["note"]:
                            st.markdown(
                                f'<div class="quote-block">{item["note"]}</div>',
                                unsafe_allow_html=True,
                            )
            else:
                st.markdown(
                    '<div style="text-align:center; padding:2.5rem 1rem; color:#64748b;">'
                    'No statutes saved yet. Search and save results from the Search tab.</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div style="text-align:center; padding:2.5rem 1rem; color:#64748b;">'
                'No case files yet. Create one above to begin organizing research.</div>',
                unsafe_allow_html=True,
            )

        st.markdown('<div style="height: 2rem;"></div>', unsafe_allow_html=True)
        st.markdown('<div class="section-label">Coverage analysis</div>', unsafe_allow_html=True)
        st.caption("Distribution of statutes across the 17 contributing-factor categories.")

        coverage = get_coverage(db)
        if any(v > 0 for v in coverage.values()):
            import pandas as pd
            df = pd.DataFrame(list(coverage.items()), columns=["Category", "Statutes Tagged"])
            df = df.sort_values("Statutes Tagged", ascending=True)

            with st.container(border=True):
                st.bar_chart(df.set_index("Category"), height=420, color="#60a5fa")

            thin = df[df["Statutes Tagged"] <= 1]["Category"].tolist()
            stat_cols = st.columns(3)
            with stat_cols[0]:
                with st.container(border=True):
                    st.markdown('<div class="section-label">Categories covered</div>', unsafe_allow_html=True)
                    st.markdown(
                        f"<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>"
                        f"{(df['Statutes Tagged'] > 0).sum()}<span style='color:#64748b; font-size:1rem;'> / 17</span></div>",
                        unsafe_allow_html=True,
                    )
            with stat_cols[1]:
                with st.container(border=True):
                    st.markdown('<div class="section-label">Thin coverage</div>', unsafe_allow_html=True)
                    st.markdown(
                        f"<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>"
                        f"{len(thin)}</div>"
                        f"<div class='meta-line'>categories with ≤1 statute</div>",
                        unsafe_allow_html=True,
                    )
            with stat_cols[2]:
                with st.container(border=True):
                    st.markdown('<div class="section-label">Total tagged</div>', unsafe_allow_html=True)
                    st.markdown(
                        f"<div style='font-size:1.8rem; font-weight:600; color:#f8fafc;'>"
                        f"{int(df['Statutes Tagged'].sum())}</div>",
                        unsafe_allow_html=True,
                    )

            if thin:
                with st.expander(f"Thin-coverage categories ({len(thin)})"):
                    for cat in thin:
                        st.markdown(f"- {cat}")
        else:
            st.markdown(
                '<div style="text-align:center; padding:2.5rem 1rem; color:#64748b;">'
                'No tagging data available yet.</div>',
                unsafe_allow_html=True,
            )


if __name__ == "__main__":
    main()
