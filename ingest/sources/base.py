"""
Generic fetcher driver: shared FetchRecord + concurrent fetch loop.

The fetchers in this package all do the same thing in slightly
different shapes:

    1. For each section in some list,
    2. Build a URL,
    3. Cache-skip if the file is already on disk,
    4. GET (with retry + rate limit),
    5. Validate the body (per-source heuristic),
    6. Write to disk if valid; record the outcome in the manifest.

This module lifts steps 1, 3, 4, 6 into ``run_fetcher``.  Each source
plugs in its URL builder, its validity check, and any extra fields it
wants on the manifest record.

Public API:
    class FetcherConfig         -- per-source description
    class FetchRecord           -- shared manifest record
    async fetch_one(...)        -- fetch one section, return record
    async run_fetcher(...)      -- concurrent loop over many sections
    SOURCE_REGISTRY             -- dict mapping source_slug -> FetcherConfig factory
    register_source(slug)       -- decorator to register a config factory
"""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import httpx
from pydantic import BaseModel, Field

from .._http import (
    RateLimiter,
    default_client,
    get_with_retry,
)
from ..manifest import ManifestWriter


# ---------------------------------------------------------------------------
# Records & configs
# ---------------------------------------------------------------------------


class FetchRecord(BaseModel):
    """One row of a fetcher's manifest.jsonl.

    Common fields all sources share.  Source-specific extras live in
    ``extra`` (e.g. CA's ``op_statues``, NY's ``article``, TX's ``chapter``).
    """
    source_slug: str
    jurisdiction: str
    law_code: str
    section: str
    url: str
    fetched_at: str
    http_status: int
    bytes: int
    content_sha256: str
    raw_path: Optional[str] = None
    valid: bool = False
    error: Optional[str] = None
    extra: dict = Field(default_factory=dict)


# A validity check returns (is_valid, extra_metadata_dict).
# - is_valid: did the body actually contain a real statute?
# - extra: per-source metadata harvested from the body (e.g. op_statues year).
ValidityResult = tuple[bool, dict]
ValidityCheck = Callable[[str, bytes, str], ValidityResult]
# args:    section, body_bytes, body_text -> (valid, extra)


@dataclasses.dataclass
class FetcherConfig:
    """Per-source fetcher description.

    Each ``ingest/sources/<slug>.py`` constructs one of these per
    ``(jurisdiction, law_code)`` pair and either:
      - Registers it via ``@register_source(slug)`` for auto-discovery, or
      - Passes it directly to ``run_fetcher(config, sections, ...)``.
    """
    source_slug: str
    jurisdiction: str
    law_code: str
    code_name: str
    url_builder: Callable[[str], str]
    validity_check: ValidityCheck
    output_root: Path                          # where raw HTML goes
    section_filename: Callable[[str], str] = (
        lambda section: section.replace("/", "_") + ".html"
    )
    default_concurrency: int = 2
    default_rate_interval: float = 0.4
    extra_record_fields: Callable[[str, dict], dict] = (
        lambda section, extra: {}
    )  # maps (section, parsed_extras) -> additional fields to merge into FetchRecord.extra


# ---------------------------------------------------------------------------
# Per-section fetch
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()


def _section_dir(config: FetcherConfig) -> Path:
    return config.output_root / config.law_code


async def fetch_one(
    client: httpx.AsyncClient,
    config: FetcherConfig,
    section: str,
    *,
    rate: Optional[RateLimiter] = None,
    force: bool = False,
    log: Optional[logging.Logger] = None,
) -> FetchRecord:
    """Fetch one section: cache-check, GET with retry, validity-check, write."""
    log = log or logging.getLogger(config.source_slug)
    code_dir = _section_dir(config)
    code_dir.mkdir(parents=True, exist_ok=True)
    fname = config.section_filename(section)
    dest = code_dir / fname
    url = config.url_builder(section)

    # Cache short-circuit
    if dest.exists() and dest.stat().st_size > 0 and not force:
        text = dest.read_text(errors="ignore")
        body = text.encode("utf-8", "ignore")
        valid, extra = config.validity_check(section, body, text)
        log.debug("cache-hit section=%s valid=%s", section, valid)
        return FetchRecord(
            source_slug=config.source_slug,
            jurisdiction=config.jurisdiction,
            law_code=config.law_code,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=200,
            bytes=dest.stat().st_size,
            content_sha256=_sha256_text(text),
            raw_path=str(dest),
            valid=valid,
            extra={**extra, **config.extra_record_fields(section, extra)},
        )

    # Network fetch (with retry)
    try:
        resp = await get_with_retry(client, url, rate=rate, label=f"{config.source_slug}-{section}")
    except Exception as e:  # noqa: BLE001
        log.warning("fetch-error section=%s err=%s", section, e)
        return FetchRecord(
            source_slug=config.source_slug,
            jurisdiction=config.jurisdiction,
            law_code=config.law_code,
            section=section,
            url=url,
            fetched_at=_now_iso(),
            http_status=0,
            bytes=0,
            content_sha256="",
            raw_path=None,
            valid=False,
            error=f"{type(e).__name__}: {e}",
        )

    body = resp.content
    text = resp.text
    sha = hashlib.sha256(body).hexdigest()

    valid, extra = config.validity_check(section, body, text)

    raw_path: Optional[str] = None
    if valid:
        dest.write_bytes(body)
        raw_path = str(dest)
        log.debug("ok section=%s status=%d bytes=%d", section, resp.status_code, len(body))
    else:
        log.debug("not-found section=%s status=%d", section, resp.status_code)

    error: Optional[str] = None
    if resp.status_code >= 500 or resp.status_code in (403, 429):
        # We exhausted retries and still got a retryable status — record as error
        # so the next run will retry instead of caching as "not found".
        error = f"HTTP {resp.status_code} after retries"

    return FetchRecord(
        source_slug=config.source_slug,
        jurisdiction=config.jurisdiction,
        law_code=config.law_code,
        section=section,
        url=url,
        fetched_at=_now_iso(),
        http_status=resp.status_code,
        bytes=len(body),
        content_sha256=sha,
        raw_path=raw_path,
        valid=valid,
        error=error,
        extra={**extra, **config.extra_record_fields(section, extra)},
    )


# ---------------------------------------------------------------------------
# Concurrent run
# ---------------------------------------------------------------------------


async def run_fetcher(
    config: FetcherConfig,
    sections: list[str],
    *,
    force: bool = False,
    concurrency: Optional[int] = None,
    rate_interval: Optional[float] = None,
    skip_known_missing: bool = True,
    manifest: Optional[ManifestWriter] = None,
    heartbeat_every: float = 5.0,
    log: Optional[logging.Logger] = None,
) -> dict:
    """Generic concurrent fetch loop.

    Pulls together: rate limit, concurrency semaphore, heartbeat task,
    cache short-circuit (per fetch_one), per-record manifest writes,
    error capture.

    Returns a stats dict ``{completed, valid, not_found, errors, bytes}``.

    The caller is responsible for:
      - constructing the section list (discovery or explicit)
      - filtering known-missing sections (via ``manifest.load_known_missing``)
      - opening / closing the ``ManifestWriter`` (we just write to it)
      - logging start / done summary
    """
    log = log or logging.getLogger(config.source_slug)
    concurrency = concurrency or config.default_concurrency
    rate_interval = rate_interval if rate_interval is not None else config.default_rate_interval

    rate = RateLimiter(rate_interval) if rate_interval > 0 else None

    stats = {
        "completed": 0,
        "valid": 0,
        "not_found": 0,
        "retried": 0,
        "errors": 0,
        "bytes": 0,
    }
    sem = asyncio.Semaphore(concurrency)
    lock = asyncio.Lock()
    total = len(sections)

    async with default_client(concurrency=concurrency) as client:

        async def _worker(section: str) -> None:
            async with sem:
                rec = await fetch_one(
                    client, config, section,
                    rate=rate, force=force, log=log,
                )
            async with lock:
                stats["completed"] += 1
                stats["bytes"] += rec.bytes
                if rec.valid:
                    stats["valid"] += 1
                elif rec.error:
                    stats["errors"] += 1
                elif rec.http_status == 200:
                    stats["not_found"] += 1
                else:
                    stats["retried"] += 1
            if manifest is not None:
                manifest.write(rec)

        # Heartbeat
        from ..cli import run_heartbeat
        hb_task = asyncio.create_task(
            run_heartbeat(stats, total, every_s=heartbeat_every, logger=log)
        )

        try:
            await asyncio.gather(*[_worker(s) for s in sections])
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

    return stats


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


# Maps source_slug -> a callable that returns a FetcherConfig given (law_code).
# Each source registers itself via @register_source("...").
SOURCE_REGISTRY: dict[str, Callable[[str], FetcherConfig]] = {}


def register_source(slug: str) -> Callable[[Callable], Callable]:
    """Decorator: register a FetcherConfig factory under ``slug``.

    Usage:
        @register_source("ny_public_law")
        def config_for(law_code: str) -> FetcherConfig:
            ...
    """
    def _wrap(fn: Callable[[str], FetcherConfig]) -> Callable[[str], FetcherConfig]:
        SOURCE_REGISTRY[slug] = fn
        return fn
    return _wrap
