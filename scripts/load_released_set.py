import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from store.db import Database


def section_to_url(section: str) -> str:
    """Strip subsection markers to build leginfo URL."""
    # "21451(a)" -> "21451", "21453(a)-(b)" -> "21453"
    base = section.split("(")[0].strip()
    return f"https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum={base}&lawCode=VEH"


def load_csv(path: str, db_path: str = "hackathon.db"):
    db = Database(db_path)
    db.init_schema()

    loaded = 0
    skipped = 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            section = row["Section #"].strip()
            source_url = section_to_url(section)

            if db.get_document_by_url(source_url):
                skipped += 1
                continue

            metadata = {
                "state": row["State"].strip(),
                "citation": row["Universal Citation"].strip(),
                "section": section,
                "statute_name": row["Statute"].strip(),
                "category_gold": row["Contributing Factor"].strip(),
            }

            db.insert_document(
                source_url=source_url,
                raw_text=row["Complete Statute"].strip(),
                metadata=metadata,
            )
            loaded += 1

    print(f"Done. Loaded: {loaded}, Skipped (already exists): {skipped}")


if __name__ == "__main__":
    csv_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "eval-ca-vehicle-code.csv",
    )
    print(f"Loading from: {csv_path}")
    load_csv(csv_path)
