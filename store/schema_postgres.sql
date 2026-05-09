CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS statutes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    jurisdiction_code TEXT NOT NULL,       -- CA, TX, FL
    jurisdiction_name TEXT NOT NULL,       -- California, Texas, Florida

    code_name TEXT NOT NULL,               -- California Vehicle Code
    law_code TEXT,                         -- VEH
    section_number TEXT NOT NULL,          -- 23152

    canonical_citation TEXT NOT NULL,      -- Cal. Veh. Code § 23152
    title TEXT,

    statute_language TEXT,                 -- short relevant excerpt
    complete_statute TEXT NOT NULL,        -- full official statute text
    plain_english_summary TEXT,

    source_url TEXT NOT NULL,
    source_name TEXT,                      -- California Legislative Information

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (jurisdiction_code, code_name, section_number)
);

CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    document_type TEXT NOT NULL,           -- statute, case, regulation, article
    statute_id UUID REFERENCES statutes(id) ON DELETE CASCADE,

    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,

    embedding vector(1536),                -- depends on embedding model
    embedding_model TEXT,

    metadata JSONB DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (document_type, statute_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_statutes_jurisdiction_section
ON statutes (jurisdiction_code, section_number);

CREATE INDEX IF NOT EXISTS idx_documents_statute_id
ON documents (statute_id);

CREATE INDEX IF NOT EXISTS idx_documents_embedding
ON documents
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
