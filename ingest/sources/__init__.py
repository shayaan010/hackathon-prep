"""Per-source bulk fetchers.

Each module under this package is responsible for one upstream data source.
They produce raw bytes on disk + a manifest.jsonl describing what was fetched.

The fetcher's contract is: bytes on disk + manifest. Parsing happens later.

Public API:
    FetcherConfig, FetchRecord     (shared config + record)
    fetch_one, run_fetcher         (generic concurrent fetcher)
    SOURCE_REGISTRY                (slug -> config factory; populated lazily)
    register_source                (decorator)
    load_all_sources()             (import every source module to populate registry)
"""
import importlib

from .base import (
    FetchRecord,
    FetcherConfig,
    SOURCE_REGISTRY,
    fetch_one,
    register_source,
    run_fetcher,
)


def load_all_sources() -> dict:
    """Import every known source module so SOURCE_REGISTRY is populated.

    Use this if you want to iterate all sources programmatically.  Running a
    single fetcher as ``python -m ingest.sources.<slug>`` doesn't need this —
    the source's own __main__ block triggers its own registration.
    """
    for slug in (
        "ca_leginfo_pages",
        "ny_public_law",
        "tx_public_law",
        "fl_public_law",
        "or_public_law",
        "nv_public_law",
    ):
        importlib.import_module(f"ingest.sources.{slug}")
    return dict(SOURCE_REGISTRY)


__all__ = [
    "FetchRecord",
    "FetcherConfig",
    "SOURCE_REGISTRY",
    "fetch_one",
    "register_source",
    "run_fetcher",
    "load_all_sources",
]
