"""Local sentence-transformers embedder satisfying Graphiti's EmbedderClient ABC.

MiroFish's LLM proxy (a CommandCode-style reasoning-model proxy) is not
guaranteed to expose an `/embeddings` endpoint, so Graphiti's built-in
`OpenAIEmbedder` cannot be assumed to work against it. The backend already
depends on `sentence-transformers`/`torch` for other features, so this runs
embeddings locally instead, with zero external calls and zero marginal cost.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from graphiti_core.embedder.client import EmbedderClient, EmbedderConfig

# all-MiniLM-L6-v2 is small, fast on CPU, and produces 384-dimensional
# embeddings. `EMBEDDING_DIM` must be set to this value (see utils/zep.py,
# which sets it via `os.environ.setdefault` before graphiti_core is
# imported) so Graphiti's zero-vector search fallback stays the same length
# as the vectors this embedder actually produces.
DEFAULT_MODEL_NAME = "all-MiniLM-L6-v2"
DEFAULT_EMBEDDING_DIM = 384


class SentenceTransformersEmbedder(EmbedderClient):
    """Runs `SentenceTransformer.encode()` in a worker thread per call."""

    def __init__(self, model_name: str = DEFAULT_MODEL_NAME):
        # Imported lazily so importing this module doesn't force a torch
        # import (and model download) for callers that never construct one
        # (e.g. simple unit tests that monkeypatch the embedder entirely).
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name
        self._model = SentenceTransformer(model_name)
        dim = self._model.get_sentence_embedding_dimension()
        self.config = EmbedderConfig(embedding_dim=dim or DEFAULT_EMBEDDING_DIM)

    def _encode_sync(self, texts: list[str]) -> list[list[float]]:
        vectors = self._model.encode(texts, normalize_embeddings=True)
        return [vector.tolist() for vector in vectors]

    async def create(
        self, input_data: str | list[str] | Iterable[int] | Iterable[Iterable[int]]
    ) -> list[float]:
        if isinstance(input_data, str):
            texts = [input_data]
        elif isinstance(input_data, list) and input_data and isinstance(input_data[0], str):
            texts = list(input_data)
        else:
            # Graphiti's ABC also allows token-id iterables in principle, but
            # every current Graphiti call site passes text. Fail loudly
            # rather than silently mis-embedding.
            raise TypeError(
                "SentenceTransformersEmbedder only supports str or list[str] input"
            )

        vectors = await asyncio.to_thread(self._encode_sync, texts)
        return vectors[0]

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        return await asyncio.to_thread(self._encode_sync, input_data_list)
