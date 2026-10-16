"""Resolve a free-text subject ("milks", "amul", "cold drinks") to products.

Hybrid retrieval, most precise signal first:

1. category  the text names a category ("dairy")           -> every product in it
2. brand     the text names a brand ("amul")               -> every product of it
3. name      stemmed full-text match on product names      -> the matches, kept to
             the category closest in meaning to the text, so "milk" keeps toned
             and soya milk but sets Cadbury Dairy Milk aside as *excluded*
4. semantic  nothing matched literally ("cold drinks")      -> nearest products by
             embedding, a fuzzy match the caller should show for confirmation

Every result says how it matched, and excluded candidates are returned too, so
the user can see (and override) what was counted.
"""

from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import psycopg
from pgvector.psycopg import register_vector

from retail.search.embedder import Embedder

MatchType = Literal["category", "brand", "name", "semantic", "none"]

# Semantic fallback: keep hits within this cosine distance of the best one. bge
# scores are compressed (unrelated products still score ~0.65), so an absolute
# cut-off doesn't work; a margin from the top does.
SEMANTIC_MARGIN = 0.03
SEMANTIC_MAX = 8


@dataclass(frozen=True)
class ProductRef:
    product_id: str
    name: str
    unit: str
    category: str
    brand: str
    score: float | None = None


@dataclass(frozen=True)
class Resolution:
    query: str
    match_type: MatchType
    selected: list[ProductRef]
    excluded: list[ProductRef] = field(default_factory=list)
    note: str = ""


PRODUCT_COLUMNS = "product_id, name, unit, category, brand"


class ProductResolver:
    def __init__(self, dsn: str, embedder: Embedder) -> None:
        self.dsn = dsn
        self.embedder = embedder
        with psycopg.connect(dsn) as conn:
            self.categories = [r[0] for r in conn.execute(
                "SELECT DISTINCT category FROM products ORDER BY 1")]
        self._category_vectors = embedder.passages(self.categories)

    def _products(self, conn, where: str, params: dict) -> list[ProductRef]:
        rows = conn.execute(
            f"SELECT {PRODUCT_COLUMNS} FROM products WHERE {where} ORDER BY name, unit", params
        ).fetchall()
        return [ProductRef(*r) for r in rows]

    def closest_category(self, query_vector: np.ndarray, among: set[str]) -> str:
        scores = self._category_vectors @ query_vector
        return max(among, key=lambda c: scores[self.categories.index(c)])

    def resolve(self, query: str) -> Resolution:
        with psycopg.connect(self.dsn) as conn:
            register_vector(conn)
            q = {"q": query}

            hits = self._products(
                conn, "to_tsvector('english', category) @@ plainto_tsquery('english', %(q)s)", q)
            if hits:
                return Resolution(query, "category", hits,
                                  note=f"All products in {hits[0].category}.")

            hits = self._products(conn, "lower(brand) = lower(%(q)s)", q)
            if hits:
                return Resolution(query, "brand", hits, note=f"All {hits[0].brand} products.")

            vector = self.embedder.query(query)
            hits = self._products(conn, "name_tsv @@ plainto_tsquery('english', %(q)s)", q)
            if hits:
                categories = {h.category for h in hits}
                if len(categories) == 1:
                    return Resolution(query, "name", hits)
                keep = self.closest_category(vector, categories)
                return Resolution(
                    query, "name",
                    [h for h in hits if h.category == keep],
                    [h for h in hits if h.category != keep],
                    note=f"Kept {keep}; matches in other categories are listed as excluded.",
                )

            rows = conn.execute(
                f"""
                SELECT {", ".join("p." + c for c in PRODUCT_COLUMNS.split(", "))},
                       1 - (e.embedding <=> %(v)s) AS score
                FROM product_embeddings e JOIN products p USING (product_id)
                ORDER BY e.embedding <=> %(v)s LIMIT %(k)s
                """,
                {"v": vector, "k": SEMANTIC_MAX * 2},
            ).fetchall()
        if not rows:
            return Resolution(query, "none", [], note="Product embeddings are missing: "
                                                      "run `make search-index`.")
        best = rows[0][-1]
        near = [ProductRef(*r) for r in rows if r[-1] >= best - SEMANTIC_MARGIN][:SEMANTIC_MAX]
        return Resolution(query, "semantic", near,
                          note="No exact match; these are the closest products by meaning. "
                               "Check them before trusting the number.")
