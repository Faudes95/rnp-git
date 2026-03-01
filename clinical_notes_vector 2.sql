CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS clinical_notes (
    id BIGSERIAL PRIMARY KEY,
    patient_id TEXT,
    note_text TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    embedding_vec vector(384)
);

CREATE INDEX IF NOT EXISTS idx_notes_vector 
ON clinical_notes USING ivfflat (embedding_vec vector_cosine_ops);
