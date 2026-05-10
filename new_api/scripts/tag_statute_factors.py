"""Tag every statute in Postgres with up to 3 PI contributing-factor labels.

Uses the Anthropic API via ``AsyncAnthropic`` with a bounded semaphore so we
can push thousands of rows through in ~a minute (target: 8k rows/min ≈ 133
rows/s — set ``--concurrency`` accordingly to your account's rate limit).

Idempotent: rows with ``cardinality(factors) > 0`` are skipped unless
``--retag`` is passed. Resumable: kill at any time, re-run, it picks up.

Usage from repo root:

    POSTGRES_DSN=postgresql://postgres:postgres@localhost:5433/new_api \\
    ANTHROPIC_API_KEY=... \\
    uv run python new_api/scripts/tag_statute_factors.py \\
        --concurrency 100 --limit 50

Drop ``--limit`` to process every untagged statute. Add ``--jurisdiction CA``
to scope to one state at a time.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.db import PostgresStore


# Keep this list IN SYNC with frontend/src/lib/statutes.ts FACTOR_CATEGORIES.
# These are the only strings the classifier may emit; anything else gets
# dropped during validation. "Other" is the explicit fallback for statutes
# that don't directly govern any of the 17 PI-relevant conduct categories
# (definitions, registration, licensing, administrative, etc.) — it lets us
# distinguish "considered and not relevant" from "not yet tagged".
OTHER_FACTOR = "Other"
FACTOR_CATEGORIES: list[str] = [
    "Improper Turning",
    "Improper Passing",
    "Failure to Yield the Right-of-Way",
    "Improper Lane of Travel",
    "Improper Stopping",
    "DUI/DWI",
    "Fleeing the Scene of a Collision",
    "Failure to Maintain Lane",
    "Driving Too Fast For Conditions",
    "Using a Wireless Telephone/Texting While Driving",
    "Fleeing a Police Officer",
    "Failure to Obey Traffic Control Device",
    "Following Too Closely",
    "Failure to Yield at a Yield Sign",
    "Improper Starting",
    "Reckless Driving",
    "Failure to Use/Activate Horn",
    OTHER_FACTOR,
]
_FACTOR_SET = set(FACTOR_CATEGORIES)


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


# Build the prompt once — only the per-statute body changes.
_FACTOR_LIST_BLOCK = "\n".join(f"- {c}" for c in FACTOR_CATEGORIES)


def _build_prompt(citation: str, title: str | None, text: str, max_factors: int) -> str:
    title_line = f"Title: {title}\n" if title else ""
    body = (text or "")[:2000]
    return f"""You are a legal expert tagging vehicle/traffic code statutes for personal injury attorneys.

Statute: {citation}
{title_line}Text: {body}

From this exact list of contributing-factor categories, return up to {max_factors} that the statute DIRECTLY governs. Only include a specific category if the statute clearly addresses that conduct.

If NONE of the specific categories above apply (for example: definitions, registration, licensing, fees, administrative procedures, equipment specs that don't relate to driver conduct), return exactly ["{OTHER_FACTOR}"]. Do not combine "{OTHER_FACTOR}" with any other category — it is a mutually-exclusive fallback.

Allowed categories (use these EXACT strings, case-sensitive):
{_FACTOR_LIST_BLOCK}

Respond with JSON only — no prose, no code fences:
{{"factors": ["<exact category name>", ...]}}"""


def _extract_json(text: str) -> dict[str, Any]:
    """Pull the first {...} object out of a model response.

    Tolerates code fences and stray prose so a slightly chatty model doesn't
    nuke the whole tagging run.
    """
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"no JSON object in response: {text[:200]!r}")
    return json.loads(text[start : end + 1])


def _validate_factors(raw: Any, max_factors: int) -> list[str]:
    """Coerce, de-duplicate, allow-list filter, clamp, and apply Other rules.

    "Other" is mutually exclusive with the specific categories. If the model
    returns it alongside real ones, drop it. If it returns nothing valid at
    all, fall back to ["Other"] so we never persist an empty array.
    """
    out: list[str] = []
    if isinstance(raw, list):
        seen: set[str] = set()
        for item in raw:
            if not isinstance(item, str):
                continue
            f = item.strip()
            if f in _FACTOR_SET and f not in seen:
                seen.add(f)
                out.append(f)
                if len(out) >= max_factors:
                    break

    specific = [f for f in out if f != OTHER_FACTOR]
    if specific:
        return specific[:max_factors]
    return [OTHER_FACTOR]


async def _classify_one(
    client: Any,
    sem: asyncio.Semaphore,
    *,
    model: str,
    row: dict[str, Any],
    max_factors: int,
    retries: int,
) -> tuple[str, list[str], str | None]:
    """Returns (statute_id, factors, error_message_or_none)."""
    citation = row.get("canonical_citation") or row.get("section_number") or "(unknown)"
    title = row.get("title")
    text = row.get("complete_statute") or ""
    prompt = _build_prompt(citation, title, text, max_factors)

    attempt = 0
    backoff = 1.0
    async with sem:
        while True:
            attempt += 1
            try:
                resp = await client.messages.create(
                    model=model,
                    max_tokens=200,
                    messages=[{"role": "user", "content": prompt}],
                )
                content = resp.content[0].text if resp.content else ""
                data = _extract_json(content)
                factors = _validate_factors(data.get("factors"), max_factors)
                return str(row["id"]), factors, None
            except Exception as e:
                # 429 / overloaded / transient: exponential backoff and retry.
                msg = str(e)
                transient = any(
                    s in msg.lower()
                    for s in ("rate limit", "overloaded", "429", "529", "timeout", "timed out")
                )
                if attempt > retries or not transient:
                    return str(row["id"]), [], f"{type(e).__name__}: {msg[:200]}"
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


async def _run(args: argparse.Namespace) -> int:
    from anthropic import AsyncAnthropic

    store = PostgresStore(args.dsn)
    store.init_schema()

    rows = store.list_untagged_statutes(
        jurisdiction=args.jurisdiction,
        limit=args.limit,
        retag=args.retag,
    )
    if not rows:
        scope = f" for {args.jurisdiction}" if args.jurisdiction else ""
        print(f"Nothing to tag{scope} (everything already has factors). Use --retag to redo.")
        return 0

    total = len(rows)
    print(
        f"Tagging {total} statutes via {args.model} "
        f"with concurrency={args.concurrency}, max_factors={args.max_factors}, "
        f"dry_run={args.dry_run}"
    )

    client = AsyncAnthropic()
    sem = asyncio.Semaphore(args.concurrency)
    tasks = [
        _classify_one(
            client,
            sem,
            model=args.model,
            row=r,
            max_factors=args.max_factors,
            retries=args.retries,
        )
        for r in rows
    ]

    started = time.time()
    done = 0
    tagged_specific = 0  # at least one of the 17 specific categories
    tagged_other = 0  # only "Other" — validator guarantees this when nothing else fits
    errors = 0
    err_samples: list[tuple[str, str]] = []

    for coro in asyncio.as_completed(tasks):
        statute_id, factors, err = await coro
        done += 1
        if err is not None:
            errors += 1
            if len(err_samples) < 5:
                err_samples.append((statute_id, err))
        else:
            if factors == [OTHER_FACTOR]:
                tagged_other += 1
            else:
                tagged_specific += 1
            if not args.dry_run:
                store.update_statute_factors(statute_id, factors)

        if done % 100 == 0 or done == total:
            elapsed = time.time() - started
            rate = done / elapsed if elapsed > 0 else 0.0
            eta = (total - done) / rate if rate > 0 else 0.0
            print(
                f"\r  {done}/{total}  specific={tagged_specific}  other={tagged_other}  "
                f"errors={errors}  {rate:5.1f} rows/s  eta={eta:5.0f}s",
                end="",
                flush=True,
            )

    print()
    elapsed = time.time() - started
    print(
        f"Done in {elapsed:.1f}s  ({total / elapsed if elapsed else 0:.1f} rows/s).  "
        f"specific={tagged_specific}  other={tagged_other}  errors={errors}"
    )
    if err_samples:
        print("First few errors:")
        for sid, e in err_samples:
            print(f"  {sid}: {e}")
    return 0 if errors == 0 else 1


def _parse_args() -> argparse.Namespace:
    new_api_root = Path(__file__).resolve().parents[1]
    _load_env_file(new_api_root / ".env")
    repo_root = Path(__file__).resolve().parents[2]
    _load_env_file(repo_root / ".env")

    parser = argparse.ArgumentParser(
        description="Tag statutes.factors[] using the Anthropic API."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("POSTGRES_DSN")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:postgres@localhost:5433/new_api",
        help="Postgres DSN. Defaults to POSTGRES_DSN / DATABASE_URL / local docker DSN.",
    )
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-6",
        help="Anthropic model id (default: claude-sonnet-4-6).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Max concurrent in-flight requests (default: 100).",
    )
    parser.add_argument(
        "--max-factors",
        type=int,
        default=3,
        help="Max factors per statute (default: 3).",
    )
    parser.add_argument(
        "--jurisdiction",
        default=None,
        help="Tag a single 2-letter jurisdiction code (e.g. CA). Default: all.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N statutes. Useful for sanity checks.",
    )
    parser.add_argument(
        "--retag",
        action="store_true",
        help="Re-tag statutes that already have factors.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print predictions without writing.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Per-row retry budget for transient errors (default: 4).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set. Put it in new_api/.env or export it.",
            file=sys.stderr,
        )
        sys.exit(2)
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
