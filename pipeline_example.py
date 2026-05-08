"""
End-to-end pipeline example.

This is the "Hello World" that proves all your modules wire together:
  1. Fetch a document (via scrape.py)
  2. Store it in the DB (via store/db.py)
  3. Chunk + embed it for search (via search/semantic.py)
  4. Extract structured data via Claude (via extract/llm.py + extract/schemas.py)
  5. Query and display (via demo/app.py)

On hackathon day, you'll replace step 1 with whatever real source the challenge
points at, and possibly swap the schema in step 4. Everything else stays.

Run:
    uv run python pipeline_example.py
"""
import asyncio
import os

from ingest.scrape import fetch
from store.db import Database
from search.semantic import SemanticIndex
from extract.llm import extract, NotFound, ExtractionError, find_quote_offset
from extract.schemas import Verdict, CaseMetadata


# A small public-domain sample document for testing without hitting any real sources.
# Replace with a real fetch on the day.
SAMPLE_DOCUMENT = """
Smith v. Acme Manufacturing Corp.
Superior Court of California, San Francisco County
Case No. CGC-22-601234
Filed: March 15, 2022
Judge: Hon. Maria Rodriguez

OPINION

Plaintiff Jane Smith brought this product liability action against defendant
Acme Manufacturing Corp. following injuries sustained on June 3, 2021, when
a kitchen blender manufactured by defendant exploded during normal use.
Plaintiff suffered second-degree burns to her face and hands, and a laceration
to her right cheek requiring 18 stitches. Medical bills totaled $42,300.

After a five-day trial, the jury returned a verdict in favor of plaintiff
on October 14, 2023. The jury awarded $185,000 in compensatory damages
and $50,000 in punitive damages, for a total verdict of $235,000.

Counsel for plaintiff: Sarah Chen, Chen & Associates LLP, CA Bar No. 234567.
Counsel for defendant: Robert Walsh, Walsh Defense Group, CA Bar No. 198432.
"""


async def run():
    print("=" * 60)
    print("PIPELINE EXAMPLE")
    print("=" * 60)

    # ---- 1. Set up DB ----
    db = Database("hackathon.db")
    db.init_schema()
    print(f"\n[1] Database ready at hackathon.db")

    # ---- 2. Ingest a document ----
    # In real use: text = await fetch("https://courtlistener.com/...")
    text = SAMPLE_DOCUMENT.strip()
    doc_id = db.insert_document(
        source_url="example://sample-opinion-1",
        raw_text=text,
        metadata={"jurisdiction": "California", "type": "opinion"},
    )
    print(f"[2] Inserted document {doc_id} ({len(text)} chars)")

    # ---- 3. Index for semantic search ----
    idx = SemanticIndex(db)
    n_chunks = idx.index_document(doc_id, text, chunk_size=300, overlap=60)
    print(f"[3] Indexed {n_chunks} chunks for semantic search")

    # ---- 4. Extract structured data with Claude ----
    has_key = os.getenv("ANTHROPIC_API_KEY") and not os.getenv(
        "ANTHROPIC_API_KEY", ""
    ).startswith("replace")

    if not has_key:
        print("\n[4] SKIPPED: ANTHROPIC_API_KEY not set.")
        print("    On hackathon day with a real key, this step extracts:")
        print("      - CaseMetadata (case_name, court, judge, claim_types, ...)")
        print("      - Verdict (plaintiff, amounts, dates)")
        print("    Each extraction includes a source_quote that's verified against")
        print("    the source text (rejected if fabricated).")
    else:
        print("\n[4] Extracting structured data via Claude...")

        try:
            metadata = extract(
                text,
                CaseMetadata,
                instructions="Extract the case metadata.",
            )
            print(f"  CaseMetadata: {metadata.case_name} ({metadata.court})")
            offset = find_quote_offset(metadata.source_quote, text)
            db.insert_extraction(
                doc_id=doc_id,
                schema_name="CaseMetadata",
                data=metadata.model_dump(),
                source_quote=metadata.source_quote,
                source_char_range=offset,
            )
        except (NotFound, ExtractionError) as e:
            print(f"  CaseMetadata extraction failed: {e}")

        try:
            verdict = extract(
                text,
                Verdict,
                instructions="Extract the verdict or settlement information.",
            )
            print(f"  Verdict: ${verdict.total_amount_usd:,.0f} for {verdict.plaintiff}")
            offset = find_quote_offset(verdict.source_quote, text)
            db.insert_extraction(
                doc_id=doc_id,
                schema_name="Verdict",
                data=verdict.model_dump(),
                source_quote=verdict.source_quote,
                source_char_range=offset,
            )
        except (NotFound, ExtractionError) as e:
            print(f"  Verdict extraction failed: {e}")

    # ---- 5. Query the index ----
    print("\n[5] Semantic search test:")
    queries = [
        "burns from defective product",
        "punitive damages award",
        "plaintiff's attorney",
    ]
    for q in queries:
        hits = idx.search(q, top_k=1)
        if hits:
            top = hits[0]
            preview = top["text"][:100].replace("\n", " ")
            print(f"  {q!r} → [{top['score']:.3f}] {preview}...")

    # ---- 6. Stats ----
    print(f"\n[6] Final stats: {db.stats()}")
    print("\nDone. Run `uv run streamlit run demo/app.py` to query in the browser.")


if __name__ == "__main__":
    asyncio.run(run())
