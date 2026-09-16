"""Behavioral contracts that survived the Zep Cloud -> Graphiti migration.

The old test_zep_cloud_contracts.py mostly locked down Zep Cloud's Batch API
wire format and its async-ingestion polling semantics (batch create/add/
process, operation_id reconciliation after an ambiguous reply, `processed`
polling). None of that exists anymore: Graphiti's `add_episode` is a single
call that blocks until the episode is fully written to Neo4j, and there is
no separate "graph create" or "batch" API to reconcile after a lost
response. Those tests are intentionally not carried forward. What remains
here are the genuinely portable behaviors: search query/limit normalization,
full-graph edge direction filtering, and "a real backend error must not be
swallowed as empty/missing data".
"""

from types import SimpleNamespace

import pytest
from graphiti_core.errors import NodeNotFoundError

from app.services.oasis_profile_generator import OasisProfileGenerator
from app.services.zep_entity_reader import EntityNode, ZepEntityReader
from app.services.zep_tools import ZepToolsService


def _permanent_error():
    from neo4j.exceptions import ClientError

    error = ClientError("permission denied")
    error._neo4j_code = "Neo.ClientError.Security.Unauthorized"
    return error


def test_report_search_caps_the_query_sent_to_graph(monkeypatch):
    calls = []

    async def fake_search_(self, *, query, config, group_ids):
        calls.append({"query": query, "group_ids": group_ids})
        return SimpleNamespace(edges=[], nodes=[])

    import app.services.zep_tools as mod

    service = object.__new__(ZepToolsService)
    service.client = SimpleNamespace(search_=lambda **kw: fake_search_(service, **kw))

    original_query = "q" * 401
    result = service.search_graph("graph-id", original_query)

    assert calls[0]["query"] == original_query[:400]
    assert calls[0]["group_ids"] == ["graph-id"]
    assert result.query == original_query


def test_profile_context_search_caps_both_queries_sent_to_graph(monkeypatch):
    calls = []

    async def fake_search_(*, query, config, group_ids):
        calls.append(query)
        return SimpleNamespace(edges=[], nodes=[])

    generator = object.__new__(OasisProfileGenerator)
    generator.graph_client = SimpleNamespace(search_=fake_search_)
    generator.graph_id = "graph-id"

    entity = EntityNode(
        uuid="node-id",
        name="n" * 500,
        labels=["Entity", "Person"],
        summary="",
        attributes={},
    )
    generator._search_zep_for_entity(entity)

    assert len(calls) == 2
    assert all(0 < len(query) <= 400 for query in calls)


def test_entity_context_includes_incoming_edges_from_the_full_graph(monkeypatch):
    incoming = {
        "uuid": "edge-in",
        "name": "WORKS_AT",
        "fact": "Alice works at Acme",
        "source_node_uuid": "alice",
        "target_node_uuid": "acme",
        "attributes": {},
    }
    outgoing = {
        "uuid": "edge-out",
        "name": "BUILDS",
        "fact": "Acme builds Product",
        "source_node_uuid": "acme",
        "target_node_uuid": "product",
        "attributes": {},
    }
    unrelated = {
        "uuid": "edge-unrelated",
        "name": "LOCATED_IN",
        "fact": "OtherCo is located in Paris",
        "source_node_uuid": "other-company",
        "target_node_uuid": "paris",
        "attributes": {},
    }

    async def fake_get_by_uuid(driver, uuid):
        return SimpleNamespace(
            uuid="acme",
            name="Acme",
            labels=["Company"],
            summary="",
            attributes={},
        )

    import app.services.zep_entity_reader as mod

    monkeypatch.setattr(
        mod.GraphitiEntityNode, "get_by_uuid", staticmethod(fake_get_by_uuid)
    )

    reader = object.__new__(ZepEntityReader)
    reader.client = SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))
    reader.get_all_edges = lambda _graph_id: [incoming, outgoing, unrelated]
    reader.get_all_nodes = lambda _graph_id: [
        {"uuid": "alice", "name": "Alice", "labels": ["Person"], "summary": ""},
        {"uuid": "acme", "name": "Acme", "labels": ["Company"], "summary": ""},
        {"uuid": "product", "name": "Product", "labels": ["Product"], "summary": ""},
    ]

    entity = reader.get_entity_with_context("graph-id", "acme")

    assert entity is not None
    assert len(entity.related_edges) == 2
    assert {edge["edge_name"] for edge in entity.related_edges} == {
        "WORKS_AT",
        "BUILDS",
    }
    assert {edge["direction"] for edge in entity.related_edges} == {
        "incoming",
        "outgoing",
    }
    assert {node["name"] for node in entity.related_nodes} == {"Alice", "Product"}


def test_entity_reader_does_not_turn_auth_failure_into_missing_entity(monkeypatch):
    async def unauthorized(driver, uuid):
        raise _permanent_error()

    import app.services.zep_entity_reader as mod

    monkeypatch.setattr(
        mod.GraphitiEntityNode, "get_by_uuid", staticmethod(unauthorized)
    )

    reader = object.__new__(ZepEntityReader)
    reader.client = SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))

    with pytest.raises(Exception) as error:
        reader.get_entity_with_context("graph-id", "node-id")

    assert not isinstance(error.value, NodeNotFoundError)


def test_entity_reader_does_not_turn_edge_failure_into_empty_data(monkeypatch):
    async def forbidden(driver, node_uuid):
        raise _permanent_error()

    import app.services.zep_entity_reader as mod

    monkeypatch.setattr(
        mod.GraphitiEntityEdge, "get_by_node_uuid", staticmethod(forbidden)
    )

    reader = object.__new__(ZepEntityReader)
    reader.client = SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))

    with pytest.raises(Exception):
        reader.get_node_edges("node-id")


def test_report_tools_do_not_turn_graph_read_failures_into_empty_data(monkeypatch):
    async def unauthorized(driver, uuid):
        raise _permanent_error()

    import app.services.zep_tools as mod

    monkeypatch.setattr(
        mod.GraphitiEntityNode, "get_by_uuid", staticmethod(unauthorized)
    )

    service = object.__new__(ZepToolsService)
    service.client = SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))

    with pytest.raises(Exception):
        service.get_node_detail("node-id")

    service.get_all_edges = lambda _graph_id: (_ for _ in ()).throw(
        _permanent_error()
    )
    with pytest.raises(Exception):
        service.get_node_edges("graph-id", "node-id")
