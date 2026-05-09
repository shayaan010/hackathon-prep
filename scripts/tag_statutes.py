import sys, os, json, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from anthropic import Anthropic
from store.db import Database


def extract_json(text: str) -> dict:
    """Pull the first {...} object out of a model response, tolerating fences/prose."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"no JSON object in response: {text[:200]!r}")
    return json.loads(text[start : end + 1])

CATEGORIES = [
    "Improper Turning", "Improper Passing", "Failure to Yield the Right-of-Way",
    "Improper Lane of Travel", "Improper Stopping", "DUI/DWI",
    "Fleeing the Scene of a Collision", "Failure to Maintain Lane",
    "Driving Too Fast For Conditions", "Using a Wireless Telephone/Texting While Driving",
    "Fleeing a Police Officer", "Failure to Obey Traffic Control Device",
    "Following Too Closely", "Failure to Yield at a Yield Sign",
    "Improper Starting", "Reckless Driving", "Failure to Use/Activate Horn"
]

client = Anthropic()
db = Database()

def tag_document(doc_id, content, citation):
    prompt = f"""You are a legal expert tagging vehicle code statutes for personal injury attorneys.

Statute: {citation}
Text: {content[:2000]}

Choose the SINGLE best contributing factor category from this exact list:
{chr(10).join(f'- {c}' for c in CATEGORIES)}

Respond with JSON only, no other text:
{{
  "primary_category": "<exact category name from list>",
  "reasoning": "<one sentence why>",
  "source_quote": "<exact short phrase from the statute text that justifies this tag>"
}}"""

    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    
    text = response.content[0].text
    result = extract_json(text)
    
    # Verify source_quote appears in content
    if result.get("source_quote", "") not in content:
        result["source_quote"] = ""
    
    return result

with db.conn() as c:
    docs = c.execute("SELECT id, raw_text, metadata FROM documents").fetchall()
print(f"Tagging {len(docs)} documents...")

tagged = 0
for doc in docs:
    doc_id, raw_text, metadata_str = doc["id"], doc["raw_text"], doc["metadata"]
    metadata = json.loads(metadata_str) if metadata_str else {}
    citation = metadata.get("citation", str(doc_id))

    with db.conn() as c:
        existing = c.execute(
            "SELECT id FROM extractions WHERE doc_id = ? AND schema_name = ?",
            (doc_id, "ContributingFactor"),
        ).fetchone()
    if existing:
        print(f"  Skipping {citation} (already tagged)")
        continue

    try:
        result = tag_document(doc_id, raw_text or "", citation)
        db.insert_extraction(
            doc_id=doc_id,
            schema_name="ContributingFactor",
            data={
                "primary_category": result["primary_category"],
                "reasoning": result.get("reasoning", ""),
            },
            source_quote=result.get("source_quote") or None,
        )
        print(f"  ✓ {citation} → {result['primary_category']}")
        tagged += 1
    except Exception as e:
        print(f"  ✗ {citation}: {e}")

print(f"\nDone. Tagged {tagged} new documents.")
