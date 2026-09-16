from types import SimpleNamespace

import pytest

from app.utils import zep_paging


def _client():
    return SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))


def _edge(uuid):
    return SimpleNamespace(uuid=uuid)


def test_edge_cap_stops_pagination_at_requested_limit(monkeypatch):
    pages = [
        [_edge("e1"), _edge("e2")],
        [_edge("e3"), _edge("e4")],
    ]
    calls = []

    async def fake_get_by_group_ids(driver, group_ids, limit=None, uuid_cursor=None):
        calls.append({"group_ids": group_ids, "limit": limit, "uuid_cursor": uuid_cursor})
        return pages[len(calls) - 1]

    monkeypatch.setattr(
        zep_paging.EntityEdge, "get_by_group_ids", staticmethod(fake_get_by_group_ids)
    )

    result = zep_paging.fetch_all_edges(_client(), "graph", page_size=2, max_items=3)

    assert [edge.uuid for edge in result] == ["e1", "e2", "e3"]
    assert len(calls) == 2
    assert calls[1]["uuid_cursor"] == "e2"


def test_existing_positional_retry_arguments_keep_their_meaning(monkeypatch):
    observed = {}

    async def fake_get_by_group_ids(driver, group_ids, limit=None, uuid_cursor=None):
        observed["limit"] = limit
        return []

    monkeypatch.setattr(
        zep_paging.EntityEdge, "get_by_group_ids", staticmethod(fake_get_by_group_ids)
    )

    # fetch_all_edges(client, graph_id, page_size, max_retries, retry_delay, max_items)
    zep_paging.fetch_all_edges(_client(), "graph", 25, 7, 0.25)

    assert observed["limit"] == 25


def test_pagination_uses_the_last_returned_uuid_as_cursor():
    calls = []

    async def fake_get_by_group_ids(driver, group_ids, limit=None, uuid_cursor=None):
        calls.append((tuple(group_ids), limit, uuid_cursor))
        if uuid_cursor is None:
            return [_edge("e1"), _edge("e2")]
        if uuid_cursor == "e2":
            return [_edge("e3")]
        return []

    import app.utils.zep_paging as mod

    mod.EntityEdge.get_by_group_ids = staticmethod(fake_get_by_group_ids)

    result = mod.fetch_all_edges(_client(), "graph", page_size=2)

    assert [edge.uuid for edge in result] == ["e1", "e2", "e3"]
    assert calls == [
        (("graph",), 2, None),
        (("graph",), 2, "e2"),
    ]


def test_pagination_stops_on_short_page_without_another_request():
    calls = []

    async def fake_get_by_group_ids(driver, group_ids, limit=None, uuid_cursor=None):
        calls.append(uuid_cursor)
        return [_edge("only-one")]

    import app.utils.zep_paging as mod

    mod.EntityEdge.get_by_group_ids = staticmethod(fake_get_by_group_ids)

    result = mod.fetch_all_edges(_client(), "graph", page_size=5)

    assert [edge.uuid for edge in result] == ["only-one"]
    assert calls == [None]


def test_empty_group_returns_empty_list_without_error():
    from graphiti_core.errors import GroupsEdgesNotFoundError

    async def fake_get_by_group_ids(driver, group_ids, limit=None, uuid_cursor=None):
        raise GroupsEdgesNotFoundError(group_ids)

    import app.utils.zep_paging as mod

    mod.EntityEdge.get_by_group_ids = staticmethod(fake_get_by_group_ids)

    result = mod.fetch_all_edges(_client(), "graph")

    assert result == []


def test_invalid_page_size_raises():
    with pytest.raises(ValueError):
        zep_paging.fetch_all_edges(_client(), "graph", page_size=0)
