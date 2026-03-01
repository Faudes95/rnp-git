CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE clinical_notes 
ADD COLUMN IF NOT EXISTS embedding_vec vector(384);

CREATE INDEX IF NOT EXISTS idx_notes_vector 
ON clinical_notes USING ivfflat (embedding_vec vector_cosine_ops);
