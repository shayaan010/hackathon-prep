"""
CourtListener API integration.

CourtListener (courtlistener.com) is the most likely-to-be-relevant data source
for a personal injury legal hackathon. It has:
  - Free API with generous rate limits
  - Massive corpus of opinions, dockets, and oral arguments
  - Clean JSON responses

API docs: https://www.courtlistener.com/help/api/rest/

Get a free API token at https://www.courtlistener.com/profile/api/ for higher
rate limits. Set it as COURTLISTENER_TOKEN in your .env.

Usage:
    from ingest.courtlistener import search_opinions, fetch_opinion

    results = await search_opinions("personal injury negligence", page_size=20)
    for r in results:
        opinion = await fetch_opinion(r["id"])
        print(opinion["plain_text"][:500])
"""
import os
import sys
from typing import Optional

# Allow both `python -m ingest.courtlistener` (relative) and direct execution
try:
    from .scrape import fetch_json
except ImportError:
    # Running as a script - add project root to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ingest.scrape import fetch_json


BASE = "https://www.courtlistener.com/api/rest/v4"


def _auth_headers() -> dict:
    """Returns auth headers if a CourtListener token is in the environment."""
    token = os.getenv("COURTLISTENER_TOKEN")
    if token:
        return {"Authorization": f"Token {token}"}
    return {}


async def search_opinions(
    query: str,
    court: Optional[str] = None,
    page_size: int = 20,
    page: int = 1,
) -> list[dict]:
    """
    Search the opinions endpoint.
    """
    from urllib.parse import urlencode

    params = {
        "q": query,
        "type": "o",
        "page_size": page_size,
        "page": page,
    }
    if court:
        params["court"] = court

    url = f"{BASE}/search/?{urlencode(params)}"
    data = await fetch_json(url, headers=_auth_headers())
    return data.get("results", [])


async def fetch_opinion(opinion_id: int) -> dict:
    """Fetch a single opinion's full data including text."""
    url = f"{BASE}/opinions/{opinion_id}/"
    return await fetch_json(url, headers=_auth_headers())


async def fetch_cluster(cluster_id: int) -> dict:
    """Fetch an opinion cluster (groups majority/dissent/concurrence)."""
    url = f"{BASE}/clusters/{cluster_id}/"
    return await fetch_json(url, headers=_auth_headers())


async def search_dockets(
    query: str,
    court: Optional[str] = None,
    page_size: int = 20,
    page: int = 1,
) -> list[dict]:
    """Search the dockets endpoint (case-level metadata + filings)."""
    from urllib.parse import urlencode

    params = {
        "q": query,
        "type": "r",
        "page_size": page_size,
        "page": page,
    }
    if court:
        params["court"] = court

    url = f"{BASE}/search/?{urlencode(params)}"
    data = await fetch_json(url, headers=_auth_headers())
    return data.get("results", [])


async def fetch_docket(docket_id: int) -> dict:
    """Fetch full docket data."""
    url = f"{BASE}/dockets/{docket_id}/"
    return await fetch_json(url, headers=_auth_headers())


async def list_docket_entries(docket_id: int) -> list[dict]:
    """List all entries on a docket."""
    url = f"{BASE}/docket-entries/?docket={docket_id}"
    data = await fetch_json(url, headers=_auth_headers())
    return data.get("results", [])


# Quick smoke test
if __name__ == "__main__":
    import asyncio

    async def main():
        print("Searching for 'personal injury' opinions...")
        results = await search_opinions("personal injury negligence", page_size=5)
        for r in results:
            case_name = r.get("caseName", "(no case name)")
            court = r.get("court", "(no court)")
            date = r.get("dateFiled", "(no date)")
            print(f"  - {case_name} | {court} | {date}")

    asyncio.run(main())
