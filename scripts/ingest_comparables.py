"""
Ingest CourtListener PI opinions and extract damages comparables.

End-to-end:
  1. Search CourtListener for PI opinions across CA/NY/TX.
  2. Pull full opinion text for each hit.
  3. Store as documents (metadata: {kind: "opinion", court, jurisdiction}).
  4. Run Verdict extract_many over each opinion to surface damages awards.
  5. Persist Verdict extractions tagged with the document id.

Run:
    uv run python scripts/ingest_comparables.py --per-jurisdiction 10
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Make the project root importable when run as `python scripts/...`
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

from store.db import Database
from search.semantic import SemanticIndex
from extract.llm import extract_many, ExtractionError
from extract.schemas import Verdict
from ingest.courtlistener import search_opinions, fetch_opinion


load_dotenv()


JURISDICTIONS = [
    # CourtListener `court` slugs cover the broad state systems.
    {"code": "CA", "label": "California", "court_filter": "cal"},
    {"code": "NY", "label": "New York", "court_filter": "ny"},
    {"code": "TX", "label": "Texas", "court_filter": "tex"},
]

# Free-text query that biases toward PI opinions with damages discussion.
SEARCH_QUERY = (
    'personal injury negligence damages awarded plaintiff '
    '"motor vehicle" OR collision OR "rear-end" OR "hit and run"'
)


async def fetch_opinion_text(result: dict) -> tuple[str, dict]:
    """
    Returns (text, metadata) for a CourtListener search result.
    Falls back to whatever text is on the search result if fetch fails.
    """
    op_id = result.get("id") or result.get("opinion_id")
    text = ""
    op = {}
    if op_id:
        try:
            op = await fetch_opinion(int(op_id))
        except Exception as e:
            print(f"    fetch_opinion({op_id}) failed: {e}")
        text = (
            (op.get("plain_text") or "").strip()
            or (op.get("html_lawbox") or "").strip()
            or (op.get("html") or "").strip()
        )
    if not text:
        text = (result.get("snippet") or "").strip()

    meta = {
        "kind": "opinion",
        "court": result.get("court") or op.get("cluster", {}).get("docket", {}).get("court", ""),
        "case_name": result.get("caseName") or result.get("caseNameShort") or "",
        "date_filed": result.get("dateFiled") or "",
        "citation": ", ".join(result.get("citation", []) or []) or "",
        "courtlistener_id": op_id,
    }
    return text, meta


async def harvest_one_jurisdiction(
    db: Database,
    idx: SemanticIndex,
    jurisdiction: dict,
    per_jurisdiction: int,
) -> list[int]:
    """Returns list of doc_ids inserted/updated for this jurisdiction."""
    print(f"\n=== {jurisdiction['label']} ({jurisdiction['code']}) ===")
    results = await search_opinions(
        SEARCH_QUERY,
        court=jurisdiction["court_filter"],
        page_size=per_jurisdiction,
    )
    print(f"  found {len(results)} opinion hit(s)")

    doc_ids: list[int] = []
    for i, r in enumerate(results, 1):
        case_name = r.get("caseName", "(no name)")[:80]
        print(f"  [{i}/{len(results)}] {case_name}")

        text, meta = await fetch_opinion_text(r)
        if len(text) < 500:
            print(f"      skipped: text too short ({len(text)} chars)")
            continue

        meta["jurisdiction"] = jurisdiction["code"]
        meta["jurisdictionLabel"] = jurisdiction["label"]

        source_url = (
            f"https://www.courtlistener.com{r.get('absolute_url', '')}"
            if r.get("absolute_url")
            else f"https://www.courtlistener.com/opinion/{meta['courtlistener_id']}/"
        )

        doc_id = db.insert_document(
            source_url=source_url,
            raw_text=text,
            metadata=meta,
        )
        doc_ids.append(doc_id)

        # Index for semantic search (skip if already indexed).
        if not db.get_chunks_for_doc(doc_id):
            n = idx.index_document(doc_id, text)
            print(f"      indexed {n} chunk(s)")

    return doc_ids


def extract_verdicts_for_doc(db: Database, doc_id: int) -> int:
    """Run Verdict extract_many on a doc; persist each as an extraction. Returns count."""
    doc = db.get_document(doc_id)
    if not doc:
        return 0

    text = (doc.get("raw_text") or "").strip()
    if len(text) < 500:
        return 0

    # Cap text we send to Claude — opinions can be long.
    if len(text) > 30_000:
        text = text[:30_000] + "\n[...truncated]"

    try:
        verdicts = extract_many(
            text,
            Verdict,
            instructions=(
                "Extract every monetary verdict, settlement, or damages award "
                "discussed in this opinion. For each, include the verbatim "
                "sentence(s) that state the amount. If no specific dollar "
                "amounts are awarded, return none."
            ),
        )
    except ExtractionError as e:
        print(f"      extract failed for doc {doc_id}: {e}")
        return 0

    saved = 0
    for v in verdicts:
        try:
            db.insert_extraction(
                doc_id=doc_id,
                schema_name="Verdict",
                data=v.model_dump(),
                source_quote=v.source_quote,
            )
            saved += 1
        except Exception as e:
            print(f"      insert failed: {e}")

    return saved


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-jurisdiction", type=int, default=8)
    parser.add_argument("--skip-extract", action="store_true",
                        help="Just ingest; don't run Claude verdict extraction.")
    args = parser.parse_args()

    if not os.getenv("COURTLISTENER_TOKEN"):
        print("WARNING: COURTLISTENER_TOKEN not set — using anonymous rate-limited access")
    if not os.getenv("ANTHROPIC_API_KEY") and not args.skip_extract:
        print("ERROR: ANTHROPIC_API_KEY not set; pass --skip-extract or add the key")
        return

    db = Database()
    db.init_schema()
    idx = SemanticIndex(db)

    started = time.time()
    all_doc_ids: list[int] = []
    for j in JURISDICTIONS:
        doc_ids = await harvest_one_jurisdiction(db, idx, j, args.per_jurisdiction)
        all_doc_ids.extend(doc_ids)

    print(f"\n--- ingest done in {time.time() - started:.1f}s; {len(all_doc_ids)} docs ---")

    if args.skip_extract:
        return

    print("\n=== Verdict extraction ===")
    extract_started = time.time()
    total_verdicts = 0
    for i, doc_id in enumerate(all_doc_ids, 1):
        n = extract_verdicts_for_doc(db, doc_id)
        total_verdicts += n
        print(f"  [{i}/{len(all_doc_ids)}] doc {doc_id}: {n} verdict(s)")

    print(
        f"\n--- extracted {total_verdicts} verdict(s) "
        f"in {time.time() - extract_started:.1f}s ---"
    )
    print(f"--- total: {time.time() - started:.1f}s ---")


if __name__ == "__main__":
    asyncio.run(main())
