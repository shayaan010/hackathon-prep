from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.db import PostgresStore


# Per-jurisdiction metadata used to fill missing fields and build citations.
# Keyed by (jurisdiction_code, law_code).
JURISDICTION_META: dict[tuple[str, str], dict[str, str]] = {
    ("CA", "VEH"): {
        "jurisdiction_name": "California",
        "code_name": "Vehicle Code",
        "citation_prefix": "Cal. Veh. Code §",
        "source_name": "California Legislative Information",
    },
    ("NY", "VAT"): {
        "jurisdiction_name": "New York",
        "code_name": "Vehicle & Traffic Law",
        "citation_prefix": "N.Y. Veh. & Traf. Law §",
        "source_name": "New York Public Law",
    },
    ("TX", "TN"): {
        "jurisdiction_name": "Texas",
        "code_name": "Transportation Code",
        "citation_prefix": "Tex. Transp. Code §",
        "source_name": "Texas Public Law",
    },
    ("CO", "CRS42"): {
        "jurisdiction_name": "Colorado",
        "code_name": "Colorado Revised Statutes Title 42 (Vehicles and Traffic)",
        "citation_prefix": "Colo. Rev. Stat. §",
        "source_name": "Colorado Public Law",
    },
    ("FL", "TXX"): {
        "jurisdiction_name": "Florida",
        "code_name": "Florida Statutes Title XXIII (Motor Vehicles)",
        "citation_prefix": "Fla. Stat. §",
        "source_name": "Florida Public Law",
    },
    ("NV", "NRS43"): {
        "jurisdiction_name": "Nevada",
        "code_name": "Nevada Revised Statutes Title 43 (Public Safety; Vehicles; Watercraft)",
        "citation_prefix": "Nev. Rev. Stat. §",
        "source_name": "Nevada Public Law",
    },
    ("OR", "ORS59"): {
        "jurisdiction_name": "Oregon",
        "code_name": "Oregon Revised Statutes Volume 17 (Vehicle Code)",
        "citation_prefix": "Or. Rev. Stat. §",
        "source_name": "Oregon Public Law",
    },
}


def _meta_for(jurisdiction_code: str, law_code: str) -> dict[str, str]:
    return JURISDICTION_META.get((jurisdiction_code, law_code), {})


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


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _section_title(payload: dict[str, Any]) -> str | None:
    section_name = _norm(payload.get("section_name"))
    if section_name:
        return section_name
    if payload.get("title"):
        return _norm(payload["title"])
    chapter = _norm(payload.get("chapter_title"))
    article = _norm(payload.get("article_title"))
    if chapter and article:
        return f"{chapter} — {article}"
    return chapter or article or None


BATCH_SIZE = 1000

INSERT_SQL = """
    INSERT INTO statutes (
        jurisdiction_code,
        jurisdiction_name,
        code_name,
        law_code,
        section_number,
        canonical_citation,
        title,
        statute_language,
        complete_statute,
        plain_english_summary,
        source_url,
        source_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (jurisdiction_code, code_name, section_number) DO NOTHING
"""


def _row_from_payload(payload: dict[str, Any]) -> tuple | None:
    section = _norm(payload.get("section_num"))
    if not section:
        return None

    jurisdiction_code = _norm(payload.get("jurisdiction")) or "CA"
    law_code = _norm(payload.get("law_code")) or "VEH"
    meta = _meta_for(jurisdiction_code, law_code)

    code_name = (
        _norm(payload.get("code_name"))
        or meta.get("code_name")
        or law_code
    )
    jurisdiction_name = meta.get("jurisdiction_name") or jurisdiction_code
    citation_prefix = meta.get("citation_prefix") or f"{jurisdiction_code} {law_code} §"
    canonical_citation = (
        f"{citation_prefix} {section}" if section else citation_prefix.rstrip(" §")
    )
    source_name = meta.get("source_name") or f"{jurisdiction_name} statutes"
    text = _norm(payload.get("text"))

    return (
        jurisdiction_code,
        jurisdiction_name,
        code_name,
        law_code,
        section,
        canonical_citation,
        _section_title(payload),
        text[:500] or None,
        text or "",
        None,
        _norm(payload.get("source_url")) or "",
        source_name,
    )


def _flush_batch(store: PostgresStore, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with store.conn() as c:
        cur = c.cursor()
        cur.executemany(INSERT_SQL, rows)
        return cur.rowcount or 0


def ingest(input_dirs: list[Path], dsn: str | None = None) -> None:
    store = PostgresStore(dsn)
    store.init_schema()

    files: list[Path] = []
    for base in input_dirs:
        if base.exists():
            files.extend(base.rglob("*.json"))
    files = sorted(files)
    if not files:
        joined = ", ".join(str(p) for p in input_dirs)
        print(f"No JSON files found under: {joined}")
        return

    total_files = len(files)
    print(f"Processing {total_files} JSON files in batches of {BATCH_SIZE}...")

    inserted = 0
    skipped_invalid = 0
    batch: list[tuple] = []

    for idx, p in enumerate(files, start=1):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"SKIP {p.name}: invalid JSON ({e})")
            skipped_invalid += 1
            continue

        row = _row_from_payload(payload)
        if row is None:
            print(f"SKIP {p.name}: missing section_num")
            skipped_invalid += 1
            continue

        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            n = _flush_batch(store, batch)
            inserted += n
            print(
                f"Batch flushed: files={idx}/{total_files} "
                f"submitted={len(batch)} inserted={n} total_inserted={inserted}",
                flush=True,
            )
            batch.clear()

    if batch:
        n = _flush_batch(store, batch)
        inserted += n
        print(
            f"Final batch flushed: submitted={len(batch)} "
            f"inserted={n} total_inserted={inserted}",
            flush=True,
        )
        batch.clear()

    print(
        "Done. "
        f"Inserted statutes: {inserted}, "
        f"Skipped invalid: {skipped_invalid}, "
        f"Files seen: {total_files}"
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    new_api_root = Path(__file__).resolve().parents[1]
    _load_env_file(new_api_root / ".env")

    host_defaults = [
        repo_root / "data" / "parsed" / "ca_leginfo_pages" / "VEH",
        repo_root / "data" / "parsed" / "ny_public_law",
        repo_root / "data" / "parsed" / "tx_public_law",
        repo_root / "data" / "parsed" / "co_public_law",
        repo_root / "data" / "parsed" / "fl_public_law",
        repo_root / "data" / "parsed" / "nv_public_law",
        repo_root / "data" / "parsed" / "or_public_law",
    ]
    container_defaults = [
        Path("/data/parsed/ca_leginfo_pages/VEH"),
        Path("/data/parsed/ny_public_law"),
        Path("/data/parsed/tx_public_law"),
        Path("/data/parsed/co_public_law"),
        Path("/data/parsed/fl_public_law"),
        Path("/data/parsed/nv_public_law"),
        Path("/data/parsed/or_public_law"),
    ]
    defaults = container_defaults if container_defaults[0].exists() else host_defaults

    parser = argparse.ArgumentParser(
        description="Seed parsed statute JSON into new_api Postgres (no embeddings)."
    )
    parser.add_argument(
        "--input-dir",
        action="append",
        type=Path,
        default=defaults,
        help=(
            "Directory containing statute JSON files. Repeat flag for multiple dirs. "
            f"Default: {', '.join(str(p) for p in defaults)}"
        ),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("POSTGRES_DSN")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:postgres@localhost:5433/new_api",
        help="Postgres DSN. Defaults to POSTGRES_DSN / DATABASE_URL / local docker DSN.",
    )

    args = parser.parse_args()
    ingest(args.input_dir, dsn=args.dsn)


if __name__ == "__main__":
    main()
