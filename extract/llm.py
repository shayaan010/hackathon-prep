"""
Structured extraction with Claude via tool use.

Pass in (text, Pydantic model, instructions); get back a validated instance.

Why tool use instead of free-form JSON prompting:
  - Claude's tool use is dramatically more reliable for structured output
  - Pydantic validates the response automatically
  - The schema's Field descriptions become part of Claude's prompt
  - Strict mode rejects malformed responses

The hackathon rule is "no fabrication". Every extracted instance includes a
source_quote field that Claude must populate with the verbatim text supporting
the extraction. We then verify that quote actually appears in the source text.

Usage:
    from pydantic import BaseModel, Field
    from extract.llm import extract

    class Verdict(BaseModel):
        plaintiff: str = Field(description="Name of the plaintiff")
        amount_usd: int = Field(description="Award amount in USD")
        source_quote: str = Field(
            description="Verbatim sentence from the source supporting this extraction"
        )

    result = extract(
        text="...full opinion text...",
        schema=Verdict,
        instructions="Extract the verdict information."
    )
    print(result.amount_usd)  # 250000
"""
import json
import os
from typing import Optional, Type, TypeVar

from pydantic import BaseModel, ValidationError


T = TypeVar("T", bound=BaseModel)


DEFAULT_MODEL = "claude-sonnet-4-5"

SYSTEM_PROMPT = """You extract structured information from documents.

Critical rules:
1. Only extract information explicitly stated in the source text.
2. For every extraction, populate the `source_quote` field with the verbatim sentence(s) from the source that support your extraction. The quote must appear in the source character-for-character.
3. If the source does not contain the requested information, do NOT guess. Use the `not_found` tool instead.
4. Do not paraphrase, summarize, or infer. If you cannot quote the source for a field, treat it as missing."""


class ExtractionError(Exception):
    """Raised when extraction fails (model error, validation error, no source quote, etc.)."""
    pass


class NotFound(Exception):
    """Raised when the model determines the requested info isn't in the source."""
    pass


def extract(
    text: str,
    schema: Type[T],
    instructions: str = "",
    model: str = DEFAULT_MODEL,
    verify_source_quote: bool = True,
) -> T:
    """
    Extract a single instance of `schema` from `text`.

    Args:
        text: source document text
        schema: Pydantic model class describing what to extract
        instructions: extra task-specific guidance (e.g. "extract the verdict")
        model: Claude model name
        verify_source_quote: if True, raise if source_quote isn't found verbatim in text

    Returns:
        validated instance of `schema`

    Raises:
        NotFound: if Claude says the info isn't in the source
        ExtractionError: if the extraction fails or the source quote is fabricated
    """
    from anthropic import Anthropic

    client = Anthropic()
    schema_dict = schema.model_json_schema()

    tools = [
        {
            "name": "record_extraction",
            "description": "Record the extracted structured information from the source text.",
            "input_schema": schema_dict,
        },
        {
            "name": "not_found",
            "description": "Use this if the source text does not contain the requested information. Do NOT guess or fabricate.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Brief explanation of what's missing.",
                    }
                },
                "required": ["reason"],
            },
        },
    ]

    user_message = f"""Source text:
<source>
{text}
</source>

Task: {instructions or 'Extract the structured information described by the tool schema.'}

Use the `record_extraction` tool to return the structured data, or `not_found` if the information isn't in the source."""

    response = client.messages.create(
        model=model,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        tools=tools,
        tool_choice={"type": "any"},  # force a tool call
        messages=[{"role": "user", "content": user_message}],
    )

    # Find the tool use block
    tool_use = None
    for block in response.content:
        if block.type == "tool_use":
            tool_use = block
            break

    if not tool_use:
        raise ExtractionError(
            f"Model did not call a tool. Response: {response.content}"
        )

    if tool_use.name == "not_found":
        reason = tool_use.input.get("reason", "no reason given")
        raise NotFound(reason)

    if tool_use.name != "record_extraction":
        raise ExtractionError(f"Unexpected tool call: {tool_use.name}")

    # Validate against schema
    try:
        result = schema.model_validate(tool_use.input)
    except ValidationError as e:
        raise ExtractionError(f"Schema validation failed: {e}") from e

    # Verify source quote actually appears in the text (anti-hallucination check)
    if verify_source_quote and hasattr(result, "source_quote"):
        quote = getattr(result, "source_quote")
        if quote and not _quote_in_text(quote, text):
            raise ExtractionError(
                f"Source quote not found verbatim in text. "
                f"Quote: {quote!r}"
            )

    return result


def extract_many(
    text: str,
    schema: Type[T],
    instructions: str = "",
    model: str = DEFAULT_MODEL,
    verify_source_quote: bool = True,
    max_results: int = 20,
) -> list[T]:
    """
    Extract multiple instances of `schema` from `text`.

    For when the source contains, say, multiple verdicts or multiple docket entries.
    """
    from anthropic import Anthropic

    client = Anthropic()
    schema_dict = schema.model_json_schema()

    # Wrap schema in an array
    array_schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": schema_dict,
                "maxItems": max_results,
            }
        },
        "required": ["items"],
    }

    tools = [
        {
            "name": "record_extractions",
            "description": "Record all extracted instances from the source text.",
            "input_schema": array_schema,
        },
        {
            "name": "not_found",
            "description": "Use this if the source contains zero matching items.",
            "input_schema": {
                "type": "object",
                "properties": {"reason": {"type": "string"}},
                "required": ["reason"],
            },
        },
    ]

    user_message = f"""Source text:
<source>
{text}
</source>

Task: {instructions or 'Extract all matching instances described by the tool schema.'}

Use `record_extractions` with an array of items, or `not_found` if there are none."""

    response = client.messages.create(
        model=model,
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        tools=tools,
        tool_choice={"type": "any"},
        messages=[{"role": "user", "content": user_message}],
    )

    tool_use = None
    for block in response.content:
        if block.type == "tool_use":
            tool_use = block
            break

    if not tool_use:
        raise ExtractionError("Model did not call a tool")

    if tool_use.name == "not_found":
        return []

    if tool_use.name != "record_extractions":
        raise ExtractionError(f"Unexpected tool call: {tool_use.name}")

    raw_items = tool_use.input.get("items", [])
    results = []

    for raw in raw_items:
        try:
            instance = schema.model_validate(raw)
        except ValidationError:
            continue  # skip bad items rather than failing the whole batch

        if verify_source_quote and hasattr(instance, "source_quote"):
            quote = getattr(instance, "source_quote")
            if quote and not _quote_in_text(quote, text):
                continue  # silently drop fabricated entries

        results.append(instance)

    return results


def _quote_in_text(quote: str, text: str) -> bool:
    """
    Check if a quote appears in the source text, with some tolerance for
    whitespace differences (newlines vs spaces, multiple spaces, etc.).
    """
    # Normalize whitespace in both strings
    norm_quote = " ".join(quote.split())
    norm_text = " ".join(text.split())
    return norm_quote in norm_text


def find_quote_offset(quote: str, text: str) -> Optional[tuple[int, int]]:
    """
    Find the (start, end) character offsets where `quote` appears in `text`.

    Returns None if not found. Uses normalized whitespace matching.
    """
    norm_quote = " ".join(quote.split())
    # Walk the text looking for a normalized match
    norm_text = " ".join(text.split())
    if norm_quote not in norm_text:
        return None

    # Try direct match first (fast path)
    idx = text.find(quote)
    if idx >= 0:
        return (idx, idx + len(quote))

    # Fall back: try matching with flexible whitespace
    # Build a regex with \s+ between words
    import re
    words = norm_quote.split()
    pattern = r"\s+".join(re.escape(w) for w in words)
    match = re.search(pattern, text)
    if match:
        return match.span()
    return None


# Quick smoke test (requires ANTHROPIC_API_KEY)
if __name__ == "__main__":
    from pydantic import BaseModel, Field

    class Verdict(BaseModel):
        plaintiff: str = Field(description="Name of the plaintiff")
        amount_usd: int = Field(description="Award amount in USD")
        source_quote: str = Field(
            description="Verbatim sentence from the source supporting this extraction"
        )

    sample = "On April 5, 2023, the jury awarded plaintiff Jane Smith $250,000 in compensatory damages."

    if not os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY", "").startswith("replace"):
        print("ANTHROPIC_API_KEY not set (or still placeholder). Skipping live test.")
    else:
        result = extract(sample, Verdict, instructions="Extract the verdict.")
        print(result)
