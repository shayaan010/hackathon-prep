"""
Quick CLI for looking up CACI instructions by statute or case key.

This is a convenience utility on top of the indexes produced by
``ingest.parsers.caci``.  It loads the by_statute.jsonl + by_case.jsonl
indexes lazily into memory and prints matching instructions.

Examples:
    # all CACIs that cite Vehicle Code 22350 (basic speed law)
    uv run python -m ingest.parsers.caci_lookup --statute "CA/VEH/22350"

    # the Civ. Code 1714 general-negligence framework
    uv run python -m ingest.parsers.caci_lookup --statute "CA/CIV/1714"

    # accept the human form too: "Cal. Veh. Code § 22350"
    uv run python -m ingest.parsers.caci_lookup --statute "Cal. Veh. Code § 22350"

    # show full text of an instruction
    uv run python -m ingest.parsers.caci_lookup --instruction CACI-706

    # eval-CSV coverage report
    uv run python -m ingest.parsers.caci_lookup --eval-coverage
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

from .caci import (
    DEFAULT_OUT_ROOT,
    CA_CODES_RAW,
    extract_statutes,
)


def _load_by_statute(out_root: Path, edition: str) -> dict[str, list[dict]]:
    path = out_root / edition / "index" / "by_statute.jsonl"
    idx: dict[str, list[dict]] = defaultdict(list)
    with path.open() as f:
        for line in f:
            rec = json.loads(line)
            idx[rec["statute_key"]].append(rec)
    return idx


def _load_by_case(out_root: Path, edition: str) -> dict[str, list[dict]]:
    path = out_root / edition / "index" / "by_case.jsonl"
    idx: dict[str, list[dict]] = defaultdict(list)
    with path.open() as f:
        for line in f:
            rec = json.loads(line)
            idx[rec["case_key"]].append(rec)
    return idx


def normalize_statute_query(q: str) -> Optional[str]:
    """Accept either a canonical key 'CA/VEH/22350' or a human form
    'Cal. Veh. Code § 22350' / 'Vehicle Code section 22350'.

    Returns the canonical key (without subsection) or None.
    """
    q = q.strip()
    if re.match(r"^[A-Z]{2}/[A-Z]+/[\d.]+(?:/[A-Za-z0-9]+)?$", q):
        return q
    found = extract_statutes(q)
    if found:
        s = found[0]
        key = f"{s['jurisdiction']}/{s['code']}/{s['section']}"
        if "subsection" in s:
            key += f"/{s['subsection']}"
        return key
    return None


def cmd_lookup_statute(args: argparse.Namespace) -> int:
    key = normalize_statute_query(args.statute)
    if not key:
        print(f"Could not parse statute query: {args.statute!r}", file=sys.stderr)
        print("Try canonical form 'CA/VEH/22350' or human form "
              "'Cal. Veh. Code § 22350'.", file=sys.stderr)
        return 2

    idx = _load_by_statute(args.out_root, args.edition)

    base_hits = idx.get(key, [])
    sub_hits: list[dict] = []
    if "/" in key.lstrip("CA/"):  # subsection-precise query: also include base
        parts = key.split("/")
        if len(parts) == 4:  # CA/CODE/SECTION/SUB
            base_key = "/".join(parts[:3])
            base_hits = idx.get(base_key, []) + base_hits
    else:
        # Section-level query: also include all subsection variants
        sub_hits = sum((v for k, v in idx.items() if k.startswith(key + "/")), [])

    all_hits = base_hits + sub_hits
    if not all_hits:
        print(f"No CACI instructions cite {key}.")
        return 1

    seen = set()
    print(f"\n{key} cited by:\n")
    for h in all_hits:
        iid = h["instruction_id"]
        if iid in seen:
            continue
        seen.add(iid)
        sub_indicator = (f"(via subsection {h.get('subsection')})"
                         if h.get("subsection") else "")
        role = h.get("role")
        print(f"  {iid:12s}  {h['instruction_title']}  "
              f"[role={role}]  {sub_indicator}")
        ctx = h.get("context")
        if ctx and args.verbose:
            print(f"      context: {ctx[:300]!r}")
    print()
    return 0


def cmd_show_instruction(args: argparse.Namespace) -> int:
    iid = args.instruction
    if not iid.startswith("CACI-"):
        iid = f"CACI-{iid}"
    path = args.out_root / args.edition / "instructions" / f"{iid}.json"
    if not path.exists():
        print(f"Instruction {iid} not found at {path}", file=sys.stderr)
        return 2
    d = json.loads(path.read_text())
    print(f"\n=== {d['id']}: {d['title']}")
    print(f"    full title: {d['title_full']}")
    print(f"    series:     {d['series_title']}")
    print(f"    pages:      {d['page_start']}-{d['page_end']}")
    if d.get("title_statute"):
        ts = d["title_statute"]
        print(f"    primary:    {ts['display']}")
    print(f"\n--- Instruction text (read to jury) ---")
    print(d["instruction_text"][:2000])
    if d["directions_for_use"] and args.verbose:
        print(f"\n--- Directions for Use ---")
        print(d["directions_for_use"][:2000])
    print(f"\n--- Sources and Authority ({len(d['sources_and_authority'])} bullets) ---")
    for i, b in enumerate(d["sources_and_authority"]):
        print(f"\n[{i}] {b['raw'][:400]}")
        for s in b["statutes"]:
            print(f"     STATUTE: {s.get('display', s)}")
        for c in b["cases"]:
            print(f"     CASE: {c['name']} ({c['year']}) {c['volume']} {c['reporter']} {c['page']}")
    print(f"\n--- All citations summary ---")
    print(f"  statutes: {len(d['all_statutes'])}")
    for s in d["all_statutes"]:
        print(f"    - {s.get('statute_key', s)}")
    print(f"  cases: {len(d['all_cases'])}")
    return 0


def cmd_eval_coverage(args: argparse.Namespace) -> int:
    eval_csv = Path(args.eval_csv)
    if not eval_csv.exists():
        print(f"Eval CSV not found at {eval_csv}", file=sys.stderr)
        return 2

    idx = _load_by_statute(args.out_root, args.edition)

    rows: list[tuple[str, str, Optional[str], str]] = []
    with eval_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            cite = row.get("Statute", "")
            m = re.search(
                r"Cal\.\s+Veh\.\s+Code\s+§\s+(\d+(?:\.\d+)?)\s*(?:\(([^)]+)\))?",
                cite,
            )
            if not m:
                continue
            rows.append((
                cite, m.group(1), m.group(2),
                row.get("Contributing Factor", ""),
            ))

    print(f"Eval CSV: {len(rows)} statute citations from {eval_csv}\n")

    seen_sec: set[str] = set()
    unique_secs: list[tuple[str, str]] = []
    for _, sec, _, factor in rows:
        if sec in seen_sec:
            continue
        seen_sec.add(sec)
        unique_secs.append((sec, factor))

    hit_secs = miss_secs = 0
    print("section          factor                                       CACI hits")
    print("-" * 90)
    for sec, factor in unique_secs:
        base_key = f"CA/VEH/{sec}"
        sub_keys = [k for k in idx if k.startswith(base_key + "/")]
        all_hits = idx.get(base_key, []) + sum((idx[k] for k in sub_keys), [])
        if all_hits:
            hit_secs += 1
            unique = sorted({h["instruction_id"] for h in all_hits})
            sample = ", ".join(unique[:3])
            if len(unique) > 3:
                sample += f" (+{len(unique)-3} more)"
            print(f"  ✓ §{sec:<10s} {factor:42s}  {sample}")
        else:
            miss_secs += 1
            print(f"  ✗ §{sec:<10s} {factor:42s}  no CACI")

    print("-" * 90)
    print(f"\nUnique sections covered: {hit_secs}/{len(unique_secs)}")
    print(f"Eval rows covered:       "
          f"{sum(1 for _, sec, _, _ in rows if any(idx.get(f'CA/VEH/{sec}') or [k for k in idx if k.startswith(f'CA/VEH/{sec}/')]))}"
          f"/{len(rows)}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Look up CACI instructions by statute or instruction ID.",
    )
    p.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    p.add_argument("--edition", default="2026")
    p.add_argument("--verbose", "-v", action="store_true")

    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--statute", help="Statute key or human form, e.g. CA/VEH/22350.")
    grp.add_argument("--instruction", help="Show full text of an instruction, e.g. CACI-706 or 706.")
    grp.add_argument("--eval-coverage", action="store_true",
                     help="Print eval-CSV coverage report.")
    p.add_argument("--eval-csv", default="data/eval-ca-vehicle-code.csv",
                   help="Path to eval CSV (for --eval-coverage).")

    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    if args.statute:
        return cmd_lookup_statute(args)
    if args.instruction:
        return cmd_show_instruction(args)
    if args.eval_coverage:
        return cmd_eval_coverage(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
