"""Per-source bulk fetchers.

Each module under this package is responsible for one upstream data source.
They produce raw bytes on disk + a manifest.jsonl describing what was fetched.

The fetcher's contract is: bytes on disk + manifest. Parsing happens later.
"""
