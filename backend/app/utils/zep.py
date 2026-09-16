"""Shared Graphiti client, sync/async bridge, and retry policy.

MiroFish previously used Zep Cloud (a managed SaaS) as its "graph memory"
backend. Zep Cloud is itself a hosted wrapper around Graphiti, the
open-source temporal knowledge graph engine published by the same team.
This module now talks directly to a self-hosted Graphiti instance backed by
a local Neo4j database, which removes Zep Cloud's metered credits entirely.

The module (and its `zep_`-prefixed neighbors: `zep_paging.py`,
`zep_lifecycle.py`, `zep_tools.py`, `zep_entity_reader.py`,
`zep_graph_memory_updater.py`) keep their historical names to minimize
churn across call sites; only the implementation changed.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from functools import lru_cache
from typing import Any, Callable, TypeVar

# Graphiti reads EMBEDDING_DIM from the environment at import time
# (graphiti_core/embedder/client.py) and uses it as the length of a
# zero-vector fallback in search.py. It must match the real dimensionality
# of whatever embedder we configure below (SentenceTransformersEmbedder /
# all-MiniLM-L6-v2 -> 384), or Neo4j's vector.similarity.cosine() comparisons
# between the fallback vector and real embeddings will be length-mismatched.
# Setting it here (before graphiti_core is imported anywhere in the process)
# is the only way to control it, since it is a module-level constant.
os.environ.setdefault("EMBEDDING_DIM", "384")

from graphiti_core import Graphiti  # noqa: E402
from graphiti_core.cross_encoder.openai_reranker_client import (  # noqa: E402
    OpenAIRerankerClient,
)
from graphiti_core.llm_client.config import LLMConfig  # noqa: E402

from ..config import Config  # noqa: E402
from .graphiti_llm_client import MiroFishGraphitiLLMClient  # noqa: E402
from .logger import get_logger  # noqa: E402
from .sentence_transformers_embedder import SentenceTransformersEmbedder  # noqa: E402

logger = get_logger("mirofish.zep")

T = TypeVar("T")

# Graphiti/Neo4j run entirely locally with no metered credits, so unlike the
# old Zep Cloud integration there is no HTTP rate limit to respect here.
GRAPHITI_QUERY_TIMEOUT_SECONDS = 60.0
# Retained for ZepGraphMemoryUpdater's worker-drain deadline (queue flush +
# thread join). Graphiti's add_episode already blocks until ingestion is
# fully complete, so this no longer bounds a separate async "processing"
# poll like it did for Zep Cloud -- it only bounds how long MiroFish waits
# for its own in-process worker thread to finish flushing.
ZEP_INGESTION_WAIT_TIMEOUT_SECONDS = 600
MAX_ZEP_SEARCH_QUERY_CHARS = 400
MAX_ZEP_SEARCH_RESULTS = 50


def normalize_zep_search_query(query: Any) -> str:
    """Return a non-empty query within the graph search endpoint's limit."""

    if not isinstance(query, str):
        raise ValueError("Graph search query must be a string")
    normalized = query.strip()
    if not normalized:
        raise ValueError("Graph search query must not be empty")
    return normalized[:MAX_ZEP_SEARCH_QUERY_CHARS]


def normalize_zep_search_limit(limit: Any) -> int:
    """Clamp a search result limit to MiroFish's search contract."""

    try:
        normalized = int(limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("Graph search limit must be an integer") from exc
    if normalized < 1:
        raise ValueError("Graph search limit must be at least 1")
    return min(normalized, MAX_ZEP_SEARCH_RESULTS)


class _BackgroundEventLoop:
    """A single event loop, owned by one dedicated daemon thread, that lives
    for the life of the process.

    Graphiti's public API is fully async; MiroFish's Flask routes and
    services are synchronous. The straightforward bridge -- `asyncio.run(...)`
    per call site -- was tried first, but it does not work correctly here:
    `get_zep_client()` returns a *process-shared* `Graphiti` instance (so
    every request reuses the same Neo4j driver / connection pool instead of
    reconnecting per call), and the neo4j async driver binds its connections
    to whichever event loop was running when they were opened. `asyncio.run`
    creates and destroys a brand-new loop on every call, so the second
    `run_async(...)` call from a different `asyncio.run()` loop fails with
    "Task ... got Future ... attached to a different loop" the moment it
    touches a connection opened during an earlier call. This was confirmed
    against a real local Neo4j instance, not just reasoned about.

    Routing every coroutine through one long-lived loop (via
    `run_coroutine_threadsafe`, safe to call concurrently from Flask's
    threaded dev server) keeps the shared client's connections on the loop
    that opened them, which is what actually works.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="mirofish-graphiti-loop",
            daemon=True,
        )
        self._thread.start()

    def run(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()


_background_loop: _BackgroundEventLoop | None = None
_background_loop_lock = threading.Lock()


def _get_background_loop() -> _BackgroundEventLoop:
    global _background_loop
    if _background_loop is None:
        with _background_loop_lock:
            if _background_loop is None:
                _background_loop = _BackgroundEventLoop()
    return _background_loop


def run_async(coro):
    """Run a coroutine to completion from MiroFish's synchronous Flask code.

    See `_BackgroundEventLoop` for why this is not a plain `asyncio.run(...)`
    per call.
    """

    return _get_background_loop().run(coro)


@lru_cache(maxsize=1)
def _cached_graphiti_client(uri: str, user: str, password: str) -> Graphiti:
    llm_config = LLMConfig(
        api_key=Config.LLM_API_KEY,
        base_url=Config.LLM_BASE_URL,
        model=Config.LLM_MODEL_NAME,
    )
    llm_client = MiroFishGraphitiLLMClient(
        config=llm_config,
        reasoning_effort=Config.LLM_REASONING_EFFORT,
        # This proxy (CommandCode, GLM/DeepSeek-family models) does not
        # reliably enforce OpenAI's native `json_schema` response_format --
        # it lets the model freelance field names (e.g. `entities` instead
        # of the required `extracted_entities`), which fails Graphiti's own
        # Pydantic validation. `json_object` mode injects the schema into
        # the prompt instead of relying on the API to constrain output.
        structured_output_mode="json_object",
    )
    embedder = SentenceTransformersEmbedder()
    # Best-effort reuse of the same OpenAI-compatible proxy for reranking.
    # OpenAIRerankerClient scores candidate passages with a boolean
    # classifier prompt + logprobs (graphiti_core/cross_encoder/
    # openai_reranker_client.py); if the configured proxy doesn't return
    # usable logprobs for this model it degrades search ranking quality but
    # does not crash the search (Graphiti falls back to the unscored order
    # on a reranker exception in the search pipeline).
    cross_encoder = OpenAIRerankerClient(config=llm_config)

    client = Graphiti(
        uri=uri,
        user=user,
        password=password,
        llm_client=llm_client,
        embedder=embedder,
        cross_encoder=cross_encoder,
    )
    return client


_indices_lock = threading.Lock()
_indices_built = False


def get_zep_client() -> Graphiti:
    """Return a process-shared Graphiti client connected to local Neo4j."""

    client = _cached_graphiti_client(
        Config.NEO4J_URI, Config.NEO4J_USER, Config.NEO4J_PASSWORD
    )

    global _indices_built
    if not _indices_built:
        with _indices_lock:
            if not _indices_built:
                # Idempotent (Neo4j `CREATE INDEX IF NOT EXISTS` semantics);
                # cheap to call once per process before first use. Retried
                # like any other read/connect below in case Neo4j is still
                # starting up (e.g. right after `docker compose up`).
                call_zep_read_with_retry(
                    lambda: run_async(client.build_indices_and_constraints()),
                    operation_name="build Neo4j indices and constraints",
                )
                _indices_built = True

    return client


def clear_zep_client_cache() -> None:
    """Clear cached clients. Intended for tests and controlled reconfiguration."""

    global _indices_built
    _cached_graphiti_client.cache_clear()
    _indices_built = False


def snapshot_current_client() -> Graphiti | None:
    """Return whatever Graphiti client is currently cached, without
    constructing a new one, and without disturbing the cache.

    Used by `settings_store.apply_and_propagate` for hot-swapping
    credentials. **Must be called before mutating `Config.NEO4J_*`**: the
    cache key is `(Config.NEO4J_URI, Config.NEO4J_USER,
    Config.NEO4J_PASSWORD)`, so calling this after the credentials change
    would look the client up under the *new* key, miss the cache (since the
    old entry is keyed by the old credentials), and return None -- leaving
    the real old client cached under its original key forever, its Neo4j
    driver connection pool never closed. Calling it beforehand, while
    `Config` still holds the credentials the cached entry was built with,
    is guaranteed to be a cache hit (see the `cache_info().currsize` guard
    below), not a fresh construction.

    Returns None if nothing has been cached yet (`get_zep_client()` was
    never called in this process).
    """

    if _cached_graphiti_client.cache_info().currsize == 0:
        return None
    # maxsize=1 and currsize>0 means exactly one entry exists, and the only
    # code path that ever populates it (`get_zep_client`) always uses these
    # same three Config attributes as the call args -- so this call cannot
    # be anything but a cache hit.
    return _cached_graphiti_client(
        Config.NEO4J_URI, Config.NEO4J_USER, Config.NEO4J_PASSWORD
    )


def close_client(client: Graphiti | None) -> bool:
    """Best-effort close of a Graphiti client evicted from the cache.

    Never raises: a credential swap must succeed even if tearing down the
    abandoned client's Neo4j driver pool fails. Returns False (and logs)
    on failure so the caller can surface a non-fatal warning.
    """

    if client is None:
        return True
    try:
        run_async(client.close())
        return True
    except Exception:
        logger.exception("Failed to close an evicted Graphiti client")
        return False


def is_retryable_zep_error(error: BaseException) -> bool:
    """Return whether a failed *read* is safe and useful to retry.

    Local Neo4j has no metered rate limit (no more 429/Retry-After
    handling), but it can still be temporarily unreachable (e.g. the
    container is still starting) or drop a connection mid-query.
    """

    from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired

    if isinstance(error, (ServiceUnavailable, SessionExpired)):
        return True
    if isinstance(error, (ConnectionError, TimeoutError, OSError)):
        return True
    if isinstance(error, Neo4jError):
        # Transient classification per Neo4j's own status code convention.
        code = getattr(error, "code", "") or ""
        return ".TransientError." in code
    return False


def call_zep_read_with_retry(
    operation: Callable[[], T],
    *,
    operation_name: str,
    max_attempts: int = 3,
    initial_delay: float = 2.0,
    max_delay: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Retry a safe read only for transient Neo4j connectivity errors."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as error:
            if attempt == max_attempts or not is_retryable_zep_error(error):
                raise

            delay = min(initial_delay * (2 ** (attempt - 1)), max_delay)
            logger.warning(
                "Graphiti %s attempt %s/%s failed (%s); retrying in %.1fs",
                operation_name,
                attempt,
                max_attempts,
                type(error).__name__,
                delay,
            )
            sleep(delay)

    raise AssertionError("unreachable")
