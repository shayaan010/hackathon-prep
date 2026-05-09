"""
Append-per-record manifest writer + utilities.

Why this exists:
    The original fetchers buffered their FetchRecords in memory and wrote
    the manifest to disk at end-of-run.  If a run was killed (kill, OOM,
    network blowup, terminal disconnect), the manifest was lost — we
    observed this with the TX run leaving 3,000+ HTML files but no
    ``manifest.jsonl``.  ``ManifestWriter`` flushes every record so
    progress is durable.

Public API:
    ManifestWriter(path)            -- context manager + .write(record)
    dedupe(path, key_fields, ...)   -- compact in-place keeping best record
    load_known_missing(path, ...)   -- read manifest; return confirmed-not-found sections
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import threading
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Union

from pydantic import BaseModel


def _record_to_dict(rec: Any) -> dict:
    """Best-effort: convert a Pydantic model / dataclass / dict to a plain dict."""
    if isinstance(rec, BaseModel):
        return rec.model_dump()
    if dataclasses.is_dataclass(rec) and not isinstance(rec, type):
        return dataclasses.asdict(rec)
    if isinstance(rec, dict):
        return rec
    raise TypeError(f"unsupported manifest record type: {type(rec).__name__}")


class ManifestWriter:
    """Thread-safe / async-safe append-per-record JSONL writer.

    Usage (sync):
        with ManifestWriter(path) as mw:
            mw.write(rec)

    Usage (async, multiple workers):
        mw = ManifestWriter(path)
        try:
            await asyncio.gather(*[worker(mw) for _ in ...])
        finally:
            mw.close()

    Each write is a single ``f.write(line) + flush()`` under a lock, so
    if the process is killed mid-run the manifest contains every record
    that was returned to ``write()`` before the kill.
    """

    def __init__(self, path: Path, *, mode: str = "a") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open(mode, encoding="utf-8")
        self._sync_lock = threading.Lock()

    def write(self, record: Any) -> None:
        """Atomically append one JSON line for ``record``."""
        d = _record_to_dict(record)
        line = json.dumps(d, ensure_ascii=False, default=str) + "\n"
        with self._sync_lock:
            self._fh.write(line)
            self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.flush()
        finally:
            self._fh.close()

    def __enter__(self) -> "ManifestWriter":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------


def _default_quality_fn(rec: dict) -> int:
    """Higher = more informative.  Used to break ties when deduping.

    Priority:
      4: valid=True (we have the statute on disk)
      3: 200 not-found (server says it doesn't exist)
      2: any other 2xx/3xx with no error
      1: 4xx with no error
      0: errored / 5xx / connect failure
    """
    if rec.get("valid"):
        return 4
    if rec.get("error"):
        return 0
    status = rec.get("http_status") or 0
    if status == 200:
        return 3
    if 200 <= status < 400:
        return 2
    if 400 <= status < 500:
        return 1
    return 0


def dedupe(
    path: Path,
    *,
    key_fields: tuple[str, ...] = ("law_code", "section"),
    quality_fn: Callable[[dict], int] = _default_quality_fn,
    sort_fn: Optional[Callable[[dict], Any]] = None,
) -> tuple[int, int, dict[str, int]]:
    """Compact a manifest in place keeping the best record per key.

    Older duplicate lines are dropped.  Records missing any key field
    are kept as-is (defensive — shouldn't happen in well-formed manifests).

    Returns ``(records_before, records_after, quality_counts)`` where
    ``quality_counts`` is a dict with keys ``valid, not_found, error, other``.

    A ``.bak`` file is written next to ``path`` before overwriting, so a
    bad dedupe is always recoverable.
    """
    path = Path(path)
    if not path.exists():
        return 0, 0, {}

    lines = [l for l in path.read_text().splitlines() if l.strip()]
    before = len(lines)

    best: dict[tuple, dict] = {}
    keepers: list[dict] = []  # records missing key fields
    for line in lines:
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        key = tuple(rec.get(k) for k in key_fields)
        if any(v is None for v in key):
            keepers.append(rec)
            continue
        prev = best.get(key)
        if prev is None or quality_fn(rec) > quality_fn(prev):
            best[key] = rec

    final = list(best.values()) + keepers

    quality_counts: dict[str, int] = {"valid": 0, "not_found": 0, "error": 0, "other": 0}
    for rec in final:
        q = quality_fn(rec)
        if q == 4:
            quality_counts["valid"] += 1
        elif q == 3:
            quality_counts["not_found"] += 1
        elif q == 0:
            quality_counts["error"] += 1
        else:
            quality_counts["other"] += 1

    if sort_fn is not None:
        final.sort(key=sort_fn)

    backup = path.with_suffix(path.suffix + ".bak")
    path.replace(backup)
    with path.open("w", encoding="utf-8") as f:
        for rec in final:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    return before, len(final), quality_counts


# ---------------------------------------------------------------------------
# Known-missing
# ---------------------------------------------------------------------------


def load_known_missing(
    path: Path,
    *,
    jurisdiction: Optional[str] = None,
    law_code: Optional[str] = None,
) -> set[str]:
    """Read manifest; return sections previously confirmed not-found.

    A section is "known missing" if a prior fetch returned a 200 response
    but the body was flagged ``valid=False`` (e.g. CA leginfo's
    ``op_statues=''``, public.law's small-body 404 fallback, etc.).
    Sections with errors / 4xx / 5xx are NOT considered known missing —
    those should be retried.

    If ``jurisdiction`` or ``law_code`` is provided, only matching records
    are considered.
    """
    path = Path(path)
    if not path.exists():
        return set()
    missing: set[str] = set()
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if jurisdiction is not None and rec.get("jurisdiction") != jurisdiction:
                continue
            if law_code is not None and rec.get("law_code") != law_code:
                continue
            if rec.get("error"):
                continue
            if rec.get("http_status") == 200 and not rec.get("valid"):
                section = rec.get("section")
                if section:
                    missing.add(section)
    return missing


# ---------------------------------------------------------------------------
# Iteration helper (used by consolidate_jsonl etc.)
# ---------------------------------------------------------------------------


def iter_records(path: Path) -> Iterable[dict]:
    """Yield each JSON record from a manifest, skipping malformed lines."""
    path = Path(path)
    if not path.exists():
        return
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
