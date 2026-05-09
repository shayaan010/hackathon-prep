import csv
import os
import sys
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from store.postgres import PostgresStatuteStore


def section_to_url(section: str) -> str:
    """Strip subsection markers to build leginfo URL."""
    # "21451(a)" -> "21451", "21453(a)-(b)" -> "21453"
    base = section.split("(")[0].strip()
    return f"https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum={base}&lawCode=VEH"


def _law_code_from_url(url: str) -> str | None:
    try:
        query = parse_qs(urlparse(url).query)
        vals = query.get("lawCode") or []
        return vals[0] if vals else None
    except Exception:
        return None


def load_csv(path: str, pg_dsn: str | None = None):
    store = PostgresStatuteStore(pg_dsn)
    store.init_schema()

    upserted = 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            section = row["Section #"].strip()
            source_url = (row.get("Source URL") or "").strip() or section_to_url(section)

            store.upsert_statute(
                jurisdiction_code=(row.get("Jurisdiction") or "CA").strip(),
                jurisdiction_name=(row.get("State") or "California").strip(),
                code_name=(row.get("Code") or row.get("Statute") or "Vehicle Code").strip(),
                law_code=(_law_code_from_url(source_url) or "").strip() or None,
                section_number=section,
                canonical_citation=(
                    row.get("Canonical Citation")
                    or row.get("Statute")
                    or f"Cal. Veh. Code § {section}"
                ).strip(),
                title=(row.get("Topic / Working Title") or row.get("Statute") or "").strip() or None,
                statute_language=(row.get("Statute Language") or "").strip() or None,
                complete_statute=(row.get("Complete Statute") or "").strip(),
                plain_english_summary=(row.get("Injury Relevance Reason") or "").strip() or None,
                source_url=source_url,
                source_name="California Legislative Information",
            )
            upserted += 1

    print(f"Done. Upserted {upserted} statutes into Postgres.")


if __name__ == "__main__":
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    preferred = os.path.join(repo_root, "data", "california_vehicle_injury_statuses.csv")
    fallback = os.path.join(repo_root, "data", "california_vehicle_injury_statutes.csv")

    csv_path = preferred if os.path.exists(preferred) else fallback
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            "Could not find california_vehicle_injury_statuses.csv or "
            "california_vehicle_injury_statutes.csv in ./data"
        )

    print(f"Loading from: {csv_path}")
    load_csv(csv_path, pg_dsn=os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL"))
