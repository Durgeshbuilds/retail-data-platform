"""(Re)build product embeddings in Postgres. Only changed products are re-embedded.

    python -m retail.search.index
"""

import logging

import psycopg
from pgvector.psycopg import register_vector

from retail import config
from retail.search.embedder import Embedder, product_text

log = logging.getLogger(__name__)


def index_products(conn: psycopg.Connection, embedder: Embedder) -> int:
    """Embed products whose text or model changed since they were last indexed."""
    register_vector(conn)
    rows = conn.execute(
        """
        SELECT p.product_id, p.name, p.category, p.brand, p.unit, e.source_text, e.model
        FROM products p LEFT JOIN product_embeddings e USING (product_id)
        ORDER BY p.product_id
        """
    ).fetchall()
    stale = [
        (pid, text)
        for pid, name, category, brand, unit, old_text, old_model in rows
        if (text := product_text(name, category, brand, unit)) != old_text
        or old_model != embedder.model_name
    ]
    if not stale:
        return 0
    vectors = embedder.passages([text for _, text in stale])
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO product_embeddings (product_id, model, source_text, embedding)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE
            SET model = EXCLUDED.model, source_text = EXCLUDED.source_text,
                embedding = EXCLUDED.embedding, updated_at = now()
            """,
            [(pid, embedder.model_name, text, vec) for (pid, text), vec in zip(stale, vectors,
                                                                                strict=True)],
        )
    return len(stale)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    embedder = Embedder()
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        changed = index_products(conn, embedder)
        total = conn.execute("SELECT count(*) FROM product_embeddings").fetchone()[0]
    log.info("embedded %d products (%d indexed in total, model %s)",
             changed, total, embedder.model_name)


if __name__ == "__main__":
    main()
