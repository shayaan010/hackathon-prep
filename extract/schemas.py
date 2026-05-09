"""
Pre-built Pydantic schemas for common personal-injury legal extractions.

These are starting points. On the day, you'll likely fork or compose them
based on the specific challenge. Every schema has a `source_quote` field so
extraction.py can verify against the source.

Usage:
    from extract.schemas import CaseMetadata
    from extract.llm import extract

    metadata = extract(opinion_text, CaseMetadata, "Extract case metadata.")
"""
from typing import Optional
from pydantic import BaseModel, Field


class SourceTracked(BaseModel):
    """Base class enforcing source quotes for verifiability."""
    source_quote: str = Field(
        description=(
            "The verbatim sentence(s) from the source text that support this extraction. "
            "Must appear in the source character-for-character. Required for every extraction."
        )
    )


class CaseMetadata(SourceTracked):
    """High-level metadata for a court case or opinion."""
    case_name: Optional[str] = Field(
        None,
        description="Full case caption, e.g. 'Smith v. Acme Corp.'"
    )
    court: Optional[str] = Field(
        None,
        description="Court name, e.g. 'Superior Court of California, San Francisco County'"
    )
    judge: Optional[str] = Field(None, description="Presiding judge's full name")
    filed_date: Optional[str] = Field(
        None,
        description="Date filed in YYYY-MM-DD format if determinable"
    )
    docket_number: Optional[str] = Field(None, description="Court docket or case number")
    claim_types: list[str] = Field(
        default_factory=list,
        description="Legal claim types, e.g. ['negligence', 'product liability']"
    )
    injury_types: list[str] = Field(
        default_factory=list,
        description="Types of injury alleged, e.g. ['traumatic brain injury', 'spinal cord injury']"
    )


class Verdict(SourceTracked):
    """A single verdict, settlement, or damages award.

    Designed for the comparables table — every field a paralegal would sort
    or filter by must be its own column, not buried in case metadata.
    """
    case_name: Optional[str] = Field(
        None,
        description="Case caption, e.g. 'Reyes v. Western Logistics'"
    )
    citation: Optional[str] = Field(
        None,
        description="Reporter or jury-verdict-reporter citation if shown"
    )
    jurisdiction: Optional[str] = Field(
        None,
        description="State or jurisdiction (e.g. 'California', 'Texas', 'New York')"
    )
    court: Optional[str] = Field(
        None,
        description="Court name, e.g. 'Alameda County Superior Court'"
    )
    plaintiff: Optional[str] = Field(None, description="Plaintiff's name as it appears")
    defendant: Optional[str] = Field(None, description="Primary defendant's name")
    award_type: Optional[str] = Field(
        None,
        description="One of: verdict, settlement, judgment, dismissal"
    )
    claim_type: Optional[str] = Field(
        None,
        description=(
            "Primary claim type, e.g. 'motor vehicle negligence', 'product liability', "
            "'premises liability', 'medical malpractice'"
        ),
    )
    injury_type: Optional[str] = Field(
        None,
        description=(
            "Primary injury, e.g. 'traumatic brain injury', 'soft tissue', "
            "'spinal cord injury', 'wrongful death', 'fracture'"
        ),
    )
    total_amount_usd: Optional[float] = Field(
        None,
        description="Total monetary award in USD. Use 0 if dismissed/no award."
    )
    compensatory_amount_usd: Optional[float] = Field(
        None, description="Compensatory portion in USD"
    )
    punitive_amount_usd: Optional[float] = Field(
        None, description="Punitive portion in USD"
    )
    decision_date: Optional[str] = Field(
        None, description="Date of decision in YYYY-MM-DD format"
    )


class DocketEntry(SourceTracked):
    """A single entry on a court docket."""
    entry_date: Optional[str] = Field(None, description="Date of entry in YYYY-MM-DD")
    entry_number: Optional[int] = Field(None, description="Sequential entry number")
    description: str = Field(description="What the entry says")
    filing_party: Optional[str] = Field(
        None, description="Who filed it (plaintiff, defendant, court, etc.)"
    )
    document_url: Optional[str] = Field(
        None, description="Link to the filed document if present"
    )


class Attorney(SourceTracked):
    """An attorney appearing in a case."""
    name: str = Field(description="Attorney's full name")
    firm: Optional[str] = Field(None, description="Law firm name")
    bar_number: Optional[str] = Field(None, description="State bar number if listed")
    role: Optional[str] = Field(
        None,
        description="Role in case: 'plaintiff', 'defendant', 'co-counsel', etc."
    )
    represents: Optional[str] = Field(
        None, description="The party they represent"
    )


class Citation(SourceTracked):
    """A legal citation extracted from a document."""
    full_text: str = Field(description="The citation as it appears in the source")
    case_name: Optional[str] = Field(None, description="Parsed case name")
    reporter: Optional[str] = Field(
        None, description="Reporter abbreviation, e.g. 'F.3d', 'Cal.App.4th'"
    )
    volume: Optional[int] = Field(None)
    page: Optional[int] = Field(None)
    year: Optional[int] = Field(None)


class Injury(SourceTracked):
    """A specific injury or medical condition mentioned."""
    description: str = Field(description="Description of the injury as stated in source")
    body_part: Optional[str] = Field(None, description="Affected body part if specified")
    severity: Optional[str] = Field(
        None,
        description="Severity if stated: 'minor', 'moderate', 'severe', 'fatal', etc."
    )
    permanent: Optional[bool] = Field(
        None, description="True if described as permanent or ongoing"
    )


class MedicalExpense(SourceTracked):
    """Medical bills or expenses mentioned in a case."""
    amount_usd: float = Field(description="Amount in USD")
    description: str = Field(description="What the expense covered")
    provider: Optional[str] = Field(None, description="Hospital, doctor, or facility")
    date_or_period: Optional[str] = Field(
        None, description="When incurred (date or range)"
    )


class ProductDefect(SourceTracked):
    """A product defect or recall mention (relevant for product-liability PI cases)."""
    product_name: str = Field(description="Product name as stated")
    manufacturer: Optional[str] = Field(None)
    defect_description: str = Field(description="Nature of the defect")
    recall_date: Optional[str] = Field(None, description="YYYY-MM-DD if recalled")
    recall_number: Optional[str] = Field(None, description="Official recall ID if any")


# Quick reference list of all schemas for easy iteration
ALL_SCHEMAS = [
    CaseMetadata,
    Verdict,
    DocketEntry,
    Attorney,
    Citation,
    Injury,
    MedicalExpense,
    ProductDefect,
]


if __name__ == "__main__":
    # Print all schemas as JSON Schema for inspection
    import json
    for schema in ALL_SCHEMAS:
        print(f"=== {schema.__name__} ===")
        print(json.dumps(schema.model_json_schema(), indent=2))
        print()
