"""
PDF text extraction with fallback for scanned documents.

Strategy:
  1. Try pdfplumber for text extraction (works for born-digital PDFs)
  2. If pages return empty/junk, fall back to Claude vision (for scanned PDFs)

Returns text annotated with page numbers so you can cite back to source.

Usage:
    from ingest.parse_pdf import extract_text

    pages = extract_text("/path/to/document.pdf")
    for page_num, text in pages:
        print(f"--- Page {page_num} ---\n{text}")
"""
import base64
from pathlib import Path
from typing import Union

import pdfplumber


# Threshold: if a page has fewer chars than this, consider it scanned
SCANNED_PAGE_THRESHOLD = 50


def extract_text(
    pdf_path: Union[str, Path],
    use_vision_fallback: bool = False,
) -> list[tuple[int, str]]:
    """
    Extract text from a PDF, returning (page_number, text) tuples.

    Page numbers are 1-indexed for human readability.

    If use_vision_fallback=True and a page appears to be scanned (very little
    text extracted), the function will use Claude vision to OCR it. Requires
    ANTHROPIC_API_KEY to be set.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    pages = []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            text = text.strip()

            if len(text) < SCANNED_PAGE_THRESHOLD and use_vision_fallback:
                # Page looks scanned - use Claude vision
                text = _ocr_page_with_claude(pdf_path, i - 1)

            pages.append((i, text))

    return pages


def extract_tables(pdf_path: Union[str, Path]) -> list[tuple[int, list[list[list[str]]]]]:
    """
    Extract tables from a PDF.

    Returns (page_number, tables) tuples. Each table is a list of rows;
    each row is a list of cell strings.
    """
    pdf_path = Path(pdf_path)
    pages_with_tables = []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if tables:
                pages_with_tables.append((i, tables))

    return pages_with_tables


def _ocr_page_with_claude(pdf_path: Path, page_index: int) -> str:
    """
    OCR a single PDF page using Claude vision.

    Renders the page as PNG, sends to Claude, returns extracted text.
    Page index is 0-indexed here (internal).
    """
    # Lazy imports so the module loads even without anthropic installed
    from anthropic import Anthropic
    import pdfplumber

    # Render page to PNG bytes
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_index]
        # Convert to image
        img = page.to_image(resolution=200)
        img_bytes = img.original.tobytes() if hasattr(img.original, 'tobytes') else None

        # Easier path: save to a temp BytesIO as PNG
        from io import BytesIO
        buf = BytesIO()
        img.save(buf, format="PNG")
        png_bytes = buf.getvalue()

    b64 = base64.standard_b64encode(png_bytes).decode("utf-8")

    client = Anthropic()
    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": b64,
                    },
                },
                {
                    "type": "text",
                    "text": (
                        "Extract all text from this document page exactly as it appears. "
                        "Preserve paragraph breaks. Do not summarize, paraphrase, or add commentary. "
                        "If you cannot read part of the page, write [unreadable] in that spot."
                    ),
                },
            ],
        }],
    )

    return response.content[0].text.strip()


def extract_full_text(pdf_path: Union[str, Path], **kwargs) -> str:
    """
    Convenience: extract all text as a single string with page markers.

    Useful when you want to dump a whole PDF into an LLM prompt.
    """
    pages = extract_text(pdf_path, **kwargs)
    return "\n\n".join(f"[Page {n}]\n{text}" for n, text in pages if text)


# Quick smoke test
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        pages = extract_text(sys.argv[1])
        for page_num, text in pages[:3]:
            print(f"--- Page {page_num} ({len(text)} chars) ---")
            print(text[:500])
            print()
    else:
        print("Usage: python parse_pdf.py <path-to-pdf>")
