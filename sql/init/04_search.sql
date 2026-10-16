-- Phase 5: semantic product search. Idempotent (make db-migrate).

CREATE EXTENSION IF NOT EXISTS vector;

-- One embedding per product, from its name, category, brand and pack size.
-- source_text is kept so the indexer can skip products whose text hasn't
-- changed, and so a search result can be explained.
CREATE TABLE IF NOT EXISTS product_embeddings (
    product_id   TEXT PRIMARY KEY REFERENCES products (product_id),
    model        TEXT NOT NULL,
    source_text  TEXT NOT NULL,
    embedding    vector(384) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Approximate nearest-neighbour index on cosine distance. With 200 products an
-- exact scan is just as fast; the index is here so the design holds at catalogue
-- scale (tens of thousands of products).
CREATE INDEX IF NOT EXISTS product_embeddings_hnsw
    ON product_embeddings USING hnsw (embedding vector_cosine_ops);

GRANT SELECT ON product_embeddings TO retail_reader;

-- Lexical side of hybrid search: stemmed full-text over product names
-- ("milks" matches "Milk"), with a GIN index for the same reason as above.
ALTER TABLE products ADD COLUMN IF NOT EXISTS name_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', name)) STORED;
CREATE INDEX IF NOT EXISTS products_name_tsv ON products USING gin (name_tsv);
