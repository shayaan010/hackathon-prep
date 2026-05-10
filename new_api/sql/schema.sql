CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS statutes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    jurisdiction_code TEXT NOT NULL,
    jurisdiction_name TEXT NOT NULL,

    code_name TEXT NOT NULL,
    law_code TEXT,
    section_number TEXT NOT NULL,

    canonical_citation TEXT NOT NULL,
    title TEXT,

    statute_language TEXT,
    complete_statute TEXT NOT NULL,
    plain_english_summary TEXT,

    source_url TEXT NOT NULL,
    source_name TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (jurisdiction_code, code_name, section_number)
);

-- Contributing-factor tags (multi-label). Populated by
-- scripts/tag_statute_factors.py via the Anthropic API. Empty array (not
-- NULL) for untagged rows so the API never has to coalesce nulls.
ALTER TABLE statutes
    ADD COLUMN IF NOT EXISTS factors TEXT[] NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    document_type TEXT NOT NULL,
    statute_id UUID REFERENCES statutes(id) ON DELETE CASCADE,

    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,

    embedding vector(1536),
    embedding_model TEXT,

    metadata JSONB DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (document_type, statute_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_statutes_jurisdiction_section
ON statutes (jurisdiction_code, section_number);

CREATE INDEX IF NOT EXISTS idx_statutes_factors
ON statutes USING GIN (factors);

CREATE INDEX IF NOT EXISTS idx_documents_statute_id
ON documents (statute_id);

CREATE INDEX IF NOT EXISTS idx_documents_embedding
ON documents
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
