"""Sentence embeddings with fastembed (ONNX on CPU, ~130 MB model, no PyTorch)."""

from collections.abc import Sequence

import numpy as np
from fastembed import TextEmbedding

from retail.config import DATA_DIR

MODEL_NAME = "BAAI/bge-small-en-v1.5"
DIMENSIONS = 384


def product_text(name: str, category: str, brand: str, unit: str) -> str:
    """The text a product is embedded from. Changing it re-embeds every product."""
    return f"{name}. {category}. {brand}. {unit}"


class Embedder:
    def __init__(self, model_name: str = MODEL_NAME) -> None:
        self.model_name = model_name
        # Cache under data/ rather than the default temp dir, so the download survives.
        self._model = TextEmbedding(model_name, cache_dir=str(DATA_DIR / "models"))

    def passages(self, texts: Sequence[str]) -> np.ndarray:
        """Embeddings for documents (product descriptions), unit length."""
        return np.array(list(self._model.passage_embed(list(texts))), dtype=np.float32)

    def query(self, text: str) -> np.ndarray:
        """Embedding for a search query (bge adds its retrieval instruction), unit length."""
        return np.array(next(iter(self._model.query_embed([text]))), dtype=np.float32)
