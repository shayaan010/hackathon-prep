"""
Async HTTP scraping with retries, rate limiting, and polite headers.

Use this for fetching HTML pages, JSON APIs, or any HTTP resource at scale.
For JS-heavy pages that don't render with httpx, use playwright (separate module).

Usage:
    from ingest.scrape import fetch, fetch_many

    # Single fetch
    text = await fetch("https://www.courtlistener.com/api/rest/v4/opinions/?q=injury")

    # Parallel fetch with rate limiting
    urls = ["https://...", "https://...", ...]
    results = await fetch_many(urls, concurrency=5)
"""
import asyncio
from typing import Optional
import httpx


# Polite default headers - identifies you to servers
DEFAULT_HEADERS = {
    "User-Agent": "HackathonBot/1.0 (research project; contact: your-email@example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class FetchError(Exception):
    """Raised when a fetch fails after all retries."""
    pass


async def fetch(
    url: str,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    follow_redirects: bool = True,
) -> str:
    """
    Fetch a URL with retries and exponential backoff.

    Returns the response body as text. Raises FetchError if all retries fail.
    """
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
    last_error = None

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=follow_redirects,
        headers=merged_headers,
    ) as client:
        for attempt in range(max_retries):
            try:
                response = await client.get(url)
                response.raise_for_status()
                return response.text
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                last_error = e
                # Don't retry on 4xx (except 429 rate limit)
                if isinstance(e, httpx.HTTPStatusError):
                    status = e.response.status_code
                    if 400 <= status < 500 and status != 429:
                        raise FetchError(f"HTTP {status} for {url}: {e}") from e

                if attempt < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    delay = retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

    raise FetchError(f"Failed to fetch {url} after {max_retries} attempts: {last_error}")


async def fetch_json(url: str, **kwargs) -> dict:
    """Fetch and parse JSON from a URL. Convenience wrapper around fetch."""
    import json
    text = await fetch(url, **kwargs)
    return json.loads(text)


async def fetch_many(
    urls: list[str],
    concurrency: int = 5,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
) -> list[tuple[str, Optional[str], Optional[str]]]:
    """
    Fetch many URLs in parallel with bounded concurrency.

    Returns list of (url, content, error) tuples. Error is None on success;
    content is None on failure. This lets you keep going even if some fail.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def _fetch_one(url: str):
        async with semaphore:
            try:
                content = await fetch(url, headers=headers, timeout=timeout)
                return (url, content, None)
            except Exception as e:
                return (url, None, str(e))

    tasks = [_fetch_one(url) for url in urls]
    return await asyncio.gather(*tasks)


async def fetch_bytes(
    url: str,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
) -> bytes:
    """
    Fetch raw bytes (for PDFs, images, etc.).

    Use this instead of fetch() when downloading binary files.
    """
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
    last_error = None

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers=merged_headers,
    ) as client:
        for attempt in range(max_retries):
            try:
                response = await client.get(url)
                response.raise_for_status()
                return response.content
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.0 * (2 ** attempt))

    raise FetchError(f"Failed to fetch bytes from {url}: {last_error}")


# Quick smoke test
if __name__ == "__main__":
    async def main():
        # Test against a stable, polite endpoint
        text = await fetch("https://httpbin.org/html")
        print(f"Fetched {len(text)} chars")

        # Test parallel
        urls = ["https://httpbin.org/get"] * 3
        results = await fetch_many(urls, concurrency=2)
        for url, content, error in results:
            status = "OK" if content else f"ERR: {error}"
            print(f"{url}: {status}")

    asyncio.run(main())
