<div align="center">

<img width="212" height="79" alt="image" src="https://github.com/user-attachments/assets/dadff85a-3b57-4d83-adba-96a3d845b892" />


### A research workbench for personal injury attorneys — built at the EvenUp × OpenClaw Hackathon.


</div>

---

<details>
<summary>📋 Table of Contents</summary>

1. [About The Project](#about-the-project)
   - [Features](#features)
   - [Built With](#built-with)
2. [Getting Started](#getting-started)
3. [Legal Data Coverage](#legal-data-coverage)
4. [Key Design Decisions](#key-design-decisions)
5. [License](#license)

</details>

---

## About The Project

**Lex Harvester** is a research workbench for personal injury attorneys. It helps lawyers surface relevant statutes, pull comparable verdicts, and spot gaps in their research — with every result traceable back to a verified public source.

In law, getting something wrong has real consequences. That principle drove every decision we made: if a source couldn't be verified, the answer wasn't used.

### Features

- **Statute search** — query across CA, NY, and TX vehicle and traffic law to find relevant statutes instantly
- **Damages comparables** — surface verdicts from similar cases, each linked to a verified source
- **Research gap detection** — identify areas of a case that lack supporting precedent or statute
- **Source-verified outputs** — every result traces back to a real public document; unverifiable answers are discarded

### Built With

| Layer | Tech |
|-------|------|
| LLM | Anthropic Claude |
| Backend | FastAPI + SQLite |
| Frontend | React + Vite + shadcn/ui |
| Search | Local semantic embeddings (no API cost) |

---

## Getting Started

Add your API key, then verify setup:

```bash
nano .env                        # add ANTHROPIC_API_KEY
uv run python test_claude.py     # → "setup works"
```

Start the dev servers (separate terminals):

```bash
uv run uvicorn api.main:app --reload --port 8000   # API on :8000
cd frontend && bun install && bun run dev           # UI on :3000
```

---

## Legal Data Coverage

Lex Harvester currently covers:

- **California** — Vehicle Code
- **New York** — Vehicle & Traffic Law
- **Texas** — Transportation Code

---

## Key Design Decisions

<details>
<summary><strong>Why source quotes are mandatory</strong></summary>

Every result Lex Harvester returns must trace back to a real source document. The extraction pipeline verifies that quoted text actually appears in the source before accepting it — hallucinated quotes are silently dropped and never shown to the user. If we couldn't verify it, we didn't use it.

</details>

<details>
<summary><strong>Why structured extraction over freeform AI responses</strong></summary>

Freeform LLM responses are hard to validate. By using Claude's tool use mode with strict Pydantic schemas, every output is machine-validated before it reaches the UI — malformed or incomplete responses are rejected outright.

</details>

---


<!-- Badge definitions -->
[contributors-badge]: https://img.shields.io/badge/contributors-5-blue
[license-badge]: https://img.shields.io/badge/License-MIT-green.svg
[python-badge]: https://img.shields.io/badge/Python-3.11+-blue?logo=python
[react-badge]: https://img.shields.io/badge/React-Vite-61DAFB?logo=react
