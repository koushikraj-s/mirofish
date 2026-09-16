"""Complete Graphiti node/edge listing for one group_id (Neo4j-backed).

Zep Cloud's node/edge "get all" endpoints were opaque-cursor-paginated HTTP
calls. Graphiti runs the equivalent query directly against Neo4j via
`EntityNode.get_by_group_ids` / `EntityEdge.get_by_group_ids`
(graphiti_core/nodes.py, graphiti_core/edges.py), which already supports
`uuid`-cursor pagination server-side (`ORDER BY uuid DESC` + `uuid < cursor`).
This module just drives that pagination to exhaustion, the same contract
`fetch_all_nodes`/`fetch_all_edges` provided before.
"""

from __future__ import annotations

from typing import Any

from graphiti_core import Graphiti
from graphiti_core.edges import EntityEdge
from graphiti_core.errors import GroupsEdgesNotFoundError, GroupsNodesNotFoundError
from graphiti_core.nodes import EntityNode, EpisodicNode

from .logger import get_logger
from .zep import call_zep_read_with_retry, run_async

logger = get_logger("mirofish.zep_paging")

_DEFAULT_PAGE_SIZE = 100
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_DELAY = 2.0


def _fetch_all(
    getter,
    driver,
    graph_id: str,
    *,
    not_found_error: type[Exception],
    item_name: str,
    page_size: int,
    max_items: int | None,
    max_retries: int,
    retry_delay: float,
) -> list[Any]:
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be between 1 and 1000")
    if max_items is not None and max_items < 1:
        raise ValueError("max_items must be at least 1 when provided")

    all_items: list[Any] = []
    uuid_cursor: str | None = None
    page_number = 0

    while True:
        page_number += 1

        def _get_page(cursor=uuid_cursor):
            return run_async(
                getter(driver, [graph_id], limit=page_size, uuid_cursor=cursor)
            )

        try:
            batch = call_zep_read_with_retry(
                _get_page,
                operation_name=f"fetch {item_name} page {page_number} (graph={graph_id})",
                max_attempts=max_retries,
                initial_delay=retry_delay,
            )
        except not_found_error:
            # An empty group is valid data, not a pagination error.
            break

        if not batch:
            break
        all_items.extend(batch)

        if max_items is not None and len(all_items) >= max_items:
            if len(all_items) > max_items:
                all_items = all_items[:max_items]
            logger.warning(
                "Graphiti %s listing reached explicit max_items=%s for graph %s",
                item_name,
                max_items,
                graph_id,
            )
            break

        if len(batch) < page_size:
            # Short page: no more results.
            break
        uuid_cursor = batch[-1].uuid

    return all_items


def fetch_all_nodes(
    client: Graphiti,
    graph_id: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_items: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
) -> list[EntityNode]:
    """Fetch every entity node in `graph_id` (Graphiti group_id) unless capped."""

    return _fetch_all(
        EntityNode.get_by_group_ids,
        client.driver,
        graph_id,
        not_found_error=GroupsNodesNotFoundError,
        item_name="nodes",
        page_size=page_size,
        max_items=max_items,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )


def fetch_all_edges(
    client: Graphiti,
    graph_id: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
    max_items: int | None = None,
) -> list[EntityEdge]:
    """Fetch every entity edge in `graph_id` (Graphiti group_id) unless capped."""

    return _fetch_all(
        EntityEdge.get_by_group_ids,
        client.driver,
        graph_id,
        not_found_error=GroupsEdgesNotFoundError,
        item_name="edges",
        page_size=page_size,
        max_items=max_items,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )


def fetch_all_episodes(
    client: Graphiti,
    graph_id: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_items: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_delay: float = _DEFAULT_RETRY_DELAY,
) -> list[EpisodicNode]:
    """Fetch every episode in `graph_id` (Graphiti group_id) unless capped.

    Unlike nodes/edges, Graphiti raises no dedicated "group has no episodes"
    error -- an empty group just returns an empty page -- so there is no
    `not_found_error` to catch specially; `GroupsNodesNotFoundError` is
    passed only for structural symmetry with `_fetch_all` and is never
    actually raised by the episode query.
    """

    return _fetch_all(
        EpisodicNode.get_by_group_ids,
        client.driver,
        graph_id,
        not_found_error=GroupsNodesNotFoundError,
        item_name="episodes",
        page_size=page_size,
        max_items=max_items,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )
