"""
Pull personal-injury opinions from CourtListener, extract verdict facts with
Claude, and store them as Verdict extractions for the /comparables UI.

No hand-curated data: every record in the DB after this runs traces back to a
real CourtListener opinion (`source_url` + `source_quote`).

The path is:
    CourtListener search → fetch opinion text → store as document
    → extract.llm.extract(..., Verdict, verify_source_quote=True)
    → db.insert_extraction()
    → SemanticIndex.index_document()  (so chat retrieval finds these too)

Opinions where the schema can't be filled (no monetary verdict in the text)
raise NotFound and are skipped — that's how we keep the comparables table real.

Run:
    uv run python scripts/ingest_verdicts.py --limit 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# .env carries ANTHROPIC_API_KEY and COURTLISTENER_TOKEN; load before SDK imports.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    )
except ImportError:
    pass

from extract.llm import extract, ExtractionError, NotFound
from extract.schemas import Verdict
from ingest.courtlistener import search_opinions, fetch_opinion, fetch_cluster
from search.semantic import SemanticIndex
from store.db import Database


SCHEMA_NAME = "Verdict"

# Opinions tend to be long. Trim aggressively — the verdict statement is almost
# always in the first few pages, and Claude's tool-use cost scales with input.
MAX_INPUT_CHARS = 25_000

# Queries chosen to bias toward opinions that discuss damages awards explicitly.
# Each runs separately and results are de-duplicated by opinion ID.
DEFAULT_QUERIES = [
    '"jury awarded" damages plaintiff personal injury',
    '"awarded the plaintiff" damages negligence',
    '"verdict in favor of the plaintiff" damages',
    '"compensatory damages" "punitive damages" personal injury',
    'settlement "personal injury" damages plaintiff',
]


EXTRACTION_INSTRUCTIONS = (
    "Extract the monetary verdict, settlement, or judgment described in this "
    "opinion. Fill `case_name`, `jurisdiction` (the state), `court`, "
    "`plaintiff`, `defendant`, `award_type` (verdict / settlement / judgment / "
    "dismissal), `claim_type` (e.g. motor vehicle negligence, premises "
    "liability, product liability, medical malpractice, wrongful death), "
    "`injury_type`, and the dollar amounts (`total_amount_usd`, "
    "`compensatory_amount_usd`, `punitive_amount_usd`). The `source_quote` "
    "MUST be the exact sentence from the opinion that states the dollar "
    "amount of the award — no paraphrasing. If the opinion does not state a "
    "dollar verdict or settlement amount, call `not_found`."
)


def _opinion_text(opinion: dict) -> str:
    """Pick the best available text representation of the opinion."""
    for key in ("plain_text", "html_with_citations", "html", "html_lawbox", "html_columbia"):
        val = opinion.get(key)
        if val and isinstance(val, str) and len(val) > 200:
            return val
    return ""


def _strip_html(s: str) -> str:
    import re
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


async def _gather_candidates(
    queries: list[str], per_query: int, sleep_s: float
) -> list[dict]:
    """Run all search queries, dedupe by cluster ID, return candidate hits."""
    seen: set[int] = set()
    out: list[dict] = []
    for q in queries:
        for attempt in range(3):
            try:
                hits = await search_opinions(q, page_size=per_query)
                break
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    wait = 5 * (attempt + 1)
                    print(f"  · rate limited on {q!r}, sleeping {wait}s")
                    await asyncio.sleep(wait)
                    continue
                print(f"  ! search failed for {q!r}: {e}")
                hits = []
                break
        for h in hits:
            cid = h.get("cluster_id")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            out.append(h)
        await asyncio.sleep(sleep_s)
    return out


def _opinion_id_from_url(url: str) -> int | None:
    """sub_opinions entries are full URLs like .../api/rest/v4/opinions/4692367/"""
    try:
        return int(str(url).rstrip("/").rsplit("/", 1)[-1])
    except (ValueError, AttributeError):
        return None


async def _hydrate_opinion(hit: dict) -> tuple[str, str, dict]:
    """
    Return (source_url, full_text, metadata) for one search hit.

    CourtListener search returns cluster-level summaries; the opinion text
    lives on a separate /opinions/{id} endpoint reachable via the cluster's
    sub_opinions list. We pick the first non-empty plain_text.
    """
    cluster_id = hit.get("cluster_id")
    case_name = (hit.get("caseName") or hit.get("case_name") or "").strip()
    court = (hit.get("court_id") or hit.get("court") or "").strip()
    date_filed = hit.get("dateFiled") or hit.get("date_filed") or ""
    abs_url = hit.get("absolute_url") or ""
    source_url = (
        f"https://www.courtlistener.com{abs_url}"
        if abs_url and not abs_url.startswith("http")
        else abs_url or f"https://www.courtlistener.com/opinion/{cluster_id}/"
    )
    metadata = {
        "kind": "verdict",
        "source": "courtlistener",
        "cluster_id": cluster_id,
        "case_name": case_name,
        "court": court,
        "date_filed": date_filed,
    }

    if not cluster_id:
        return source_url, "", metadata

    cluster = None
    for attempt in range(3):
        try:
            cluster = await fetch_cluster(cluster_id)
            break
        except Exception as e:
            if "429" in str(e) and attempt < 2:
                wait = 5 * (attempt + 1)
                print(f"  · cluster {cluster_id} rate limited, sleeping {wait}s")
                await asyncio.sleep(wait)
                continue
            metadata["error"] = f"cluster fetch: {e}"
            return source_url, "", metadata
    if cluster is None:
        return source_url, "", metadata

    sub_opinions = cluster.get("sub_opinions") or []
    text = ""
    op_id_used = None
    for op_url in sub_opinions:
        op_id = _opinion_id_from_url(op_url)
        if op_id is None:
            continue
        try:
            op = await fetch_opinion(op_id)
        except Exception:
            continue
        candidate = _opinion_text(op)
        if candidate:
            text = candidate
            op_id_used = op_id
            break

    if op_id_used is not None:
        metadata["opinion_id"] = op_id_used

    if "<" in text[:200]:
        text = _strip_html(text)

    return source_url, text, metadata


def _ingest_one(
    db: Database,
    idx: SemanticIndex,
    source_url: str,
    text: str,
    metadata: dict,
    extract_model: str,
) -> tuple[str, dict | None]:
    """
    Insert document, run extraction, store + index.

    Returns ("skip"|"miss"|"ok"|"error", extraction_data_or_none).
    """
    if not text or len(text) < 500:
        return "skip", None

    # Skip if we've already extracted a Verdict for this URL (idempotent re-runs).
    existing = db.get_document_by_url(source_url)
    if existing:
        prior = db.get_extractions_for_doc(existing["id"])
        if any(p.get("schema_name") == SCHEMA_NAME for p in prior):
            return "skip", None

    truncated = text[:MAX_INPUT_CHARS]

    try:
        verdict = extract(
            text=truncated,
            schema=Verdict,
            instructions=EXTRACTION_INSTRUCTIONS,
            model=extract_model,
            verify_source_quote=True,
        )
    except NotFound:
        return "miss", None
    except ExtractionError as e:
        print(f"    extraction error: {e}")
        return "error", None

    data = verdict.model_dump(exclude_none=True)

    # Don't keep verdicts with no usable amount — those won't sort meaningfully.
    if not any(
        data.get(k) for k in ("total_amount_usd", "compensatory_amount_usd", "punitive_amount_usd")
    ):
        return "miss", None

    doc_id = db.insert_document(source_url=source_url, raw_text=text, metadata=metadata)
    db.insert_extraction(
        doc_id=doc_id,
        schema_name=SCHEMA_NAME,
        data=data,
        source_quote=verdict.source_quote,
    )
    try:
        idx.index_document(doc_id, text)
    except Exception as e:
        # Indexing failure shouldn't kill the whole run — extraction still landed.
        print(f"    index warning: {e}")

    return "ok", data


async def _run(args: argparse.Namespace) -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit("ANTHROPIC_API_KEY is not set; can't run extraction.")

    db = Database()
    db.init_schema()
    idx = SemanticIndex(db)

    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] "
          f"Searching CourtListener with {len(DEFAULT_QUERIES)} queries…")
    candidates = await _gather_candidates(
        DEFAULT_QUERIES, per_query=args.per_query, sleep_s=args.sleep
    )
    print(f"  found {len(candidates)} unique candidates")

    if args.limit and args.limit < len(candidates):
        candidates = candidates[: args.limit]
        print(f"  (limiting to {args.limit})")

    counts = {"ok": 0, "miss": 0, "error": 0, "skip": 0}
    for i, hit in enumerate(candidates, 1):
        case_name = (hit.get("caseName") or hit.get("case_name") or "?").strip()[:80]
        print(f"\n[{i}/{len(candidates)}] {case_name}")
        source_url, text, metadata = await _hydrate_opinion(hit)
        if not text:
            print(f"  - no text available; {metadata.get('error', '')}")
            counts["skip"] += 1
            continue

        status, data = _ingest_one(db, idx, source_url, text, metadata, args.model)
        counts[status] += 1
        if status == "ok" and data:
            amt = (
                data.get("total_amount_usd")
                or (data.get("compensatory_amount_usd") or 0)
                + (data.get("punitive_amount_usd") or 0)
            )
            print(
                f"  + {data.get('case_name', case_name)} "
                f"({data.get('jurisdiction', '?')}) "
                f"— ${amt:,.0f} {data.get('award_type', '')}"
            )
        elif status == "miss":
            print("  - no monetary verdict in this opinion")
        elif status == "skip":
            print("  - already extracted")

        await asyncio.sleep(args.sleep)

    print("\n" + "=" * 60)
    print(json.dumps({**counts, "stats": db.stats()}, indent=2))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=25, help="Max opinions to attempt (default 25)")
    p.add_argument(
        "--per-query",
        type=int,
        default=10,
        help="Opinions to fetch per CourtListener query (default 10)",
    )
    p.add_argument(
        "--model",
        default="claude-sonnet-4-6",
        help="Model for extraction (default claude-sonnet-4-6 — cheaper than Opus, plenty for structured extract)",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Seconds to wait between CourtListener calls (default 2.0)",
    )
    args = p.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
