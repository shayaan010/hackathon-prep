"""
Shared HTTP utilities for ingest source fetchers.

Provides:
- USER_AGENT (browser-like, used everywhere to reduce bot challenges)
- RateLimiter (global min-interval between request starts)
- get_with_retry (httpx GET with exponential backoff for 403/429/5xx + network errors)
- a default httpx limits config

Why:
- leginfo, public.law, justia, and similar sites all throttle aggressive scrapers.
- Per-fetch retry policy is identical across sources, no need to re-implement it.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_S = 30.0
MAX_RETRIES = 6
BACKOFF_BASE = 0.5
MAX_BACKOFF = 30.0
RETRY_STATUSES = {403, 429, 500, 502, 503, 504}
# 404 is treated as a clean negative response (e.g. "section does not exist").
# Most sources return that for missing pages; callers can interpret it.


class RateLimiter:
    """Global rate limiter: min seconds between request starts (across all workers)."""
    def __init__(self, interval: float):
        self.interval = interval
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self):
        if self.interval <= 0:
            return
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = asyncio.get_event_loop().time()
            self._next = max(now, self._next) + self.interval


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    rate: Optional[RateLimiter] = None,
    label: str = "",
    accept_404: bool = True,
) -> httpx.Response:
    """GET with rate limit + retry. Returns the final response.

    accept_404 (default True): treat 404 as a non-retryable success-ish response
    (caller decides what to do). Set False to treat 404 as a hard error.
    """
    last_exc: Optional[Exception] = None
    last_resp: Optional[httpx.Response] = None
    for attempt in range(1, MAX_RETRIES + 1):
        if rate is not None:
            await rate.wait()
        try:
            resp = await client.get(url)
            last_resp = resp
            if resp.status_code in RETRY_STATUSES:
                wait = min(BACKOFF_BASE * (2 ** (attempt - 1)) + 0.1 * attempt, MAX_BACKOFF)
                logger.warning(
                    "retry    %s attempt=%d/%d status=%d backoff=%.1fs",
                    label or url, attempt, MAX_RETRIES, resp.status_code, wait,
                )
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(wait)
                    continue
                # Exhausted — return last resp so caller can decide
                return resp
            if resp.status_code == 404 and not accept_404:
                # Caller wants 404 to be retried — treat as if it's transient
                wait = min(BACKOFF_BASE * (2 ** (attempt - 1)) + 0.1 * attempt, MAX_BACKOFF)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(wait)
                    continue
                return resp
            return resp
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError,
                httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
            last_exc = e
            wait = min(BACKOFF_BASE * (2 ** (attempt - 1)) + 0.1 * attempt, MAX_BACKOFF)
            logger.warning(
                "retry    %s attempt=%d/%d error=%s backoff=%.1fs",
                label or url, attempt, MAX_RETRIES, type(e).__name__, wait,
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(wait)
                continue
    if last_resp is not None:
        return last_resp
    raise RuntimeError(f"exhausted retries for {url}: {last_exc}")


def default_client(*, concurrency: int = 4, timeout: float = DEFAULT_TIMEOUT_S) -> httpx.AsyncClient:
    """Return a pre-configured httpx.AsyncClient with our standard headers + limits."""
    return httpx.AsyncClient(
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=timeout,
        follow_redirects=True,
        http2=False,
        limits=httpx.Limits(
            max_connections=concurrency * 2,
            max_keepalive_connections=concurrency,
        ),
    )


def setup_logging(
    name: Optional[str] = None,
    *,
    verbose: bool = False,
    log_path=None,
) -> logging.Logger:
    """Configure a named logger with stdout + optional file handler.

    If ``name`` is None, configures the root logger (legacy behavior).
    Returns the configured logger so callers can do
    ``log = setup_logging("ny_public_law", verbose=True, log_path=...)``.

    Idempotent: existing handlers on the named logger are removed first.
    """
    fmt = "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    target = logging.getLogger(name) if name else logging.getLogger()
    # Clear existing handlers so re-runs in the same process don't double-log.
    for h in list(target.handlers):
        target.removeHandler(h)

    import sys
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    stream.setLevel(logging.DEBUG if verbose else logging.INFO)
    target.addHandler(stream)

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fileh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        fileh.setFormatter(formatter)
        fileh.setLevel(logging.DEBUG)
        target.addHandler(fileh)

    target.setLevel(logging.DEBUG if verbose else logging.INFO)
    # Don't double-log via root if a name was given.
    if name:
        target.propagate = False
    if log_path:
        target.info("file-log path=%s", log_path)
    return target
