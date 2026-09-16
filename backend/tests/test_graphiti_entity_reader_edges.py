from types import SimpleNamespace

from app.services.zep_entity_reader import ZepEntityReader


def test_get_node_edges_uses_graphiti_get_by_node_uuid(monkeypatch):
    calls = []

    async def fake_get_by_node_uuid(driver, node_uuid):
        calls.append((driver, node_uuid))
        return [
            SimpleNamespace(
                uuid="edge-1",
                name="KNOWS",
                fact="Alice knows Bob",
                source_node_uuid="node-1",
                target_node_uuid="node-2",
                attributes={"since": "2024"},
            )
        ]

    import app.services.zep_entity_reader as mod

    monkeypatch.setattr(
        mod.GraphitiEntityEdge, "get_by_node_uuid", staticmethod(fake_get_by_node_uuid)
    )

    reader = object.__new__(ZepEntityReader)
    reader.client = SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))

    assert reader.get_node_edges("node-1") == [{
        "uuid": "edge-1",
        "name": "KNOWS",
        "fact": "Alice knows Bob",
        "source_node_uuid": "node-1",
        "target_node_uuid": "node-2",
        "attributes": {"since": "2024"},
    }]
    assert calls == [(reader.client.driver, "node-1")]


def test_get_node_edges_with_graph_id_filters_full_graph_both_directions(monkeypatch):
    reader = object.__new__(ZepEntityReader)
    reader.client = SimpleNamespace(driver=SimpleNamespace(name="fake-driver"))

    all_edges = [
        {
            "uuid": "e1",
            "name": "KNOWS",
            "fact": "a knows b",
            "source_node_uuid": "node-1",
            "target_node_uuid": "node-2",
            "attributes": {},
        },
        {
            "uuid": "e2",
            "name": "WORKS_FOR",
            "fact": "c works for node-1",
            "source_node_uuid": "node-3",
            "target_node_uuid": "node-1",
            "attributes": {},
        },
        {
            "uuid": "e3",
            "name": "UNRELATED",
            "fact": "d unrelated to e",
            "source_node_uuid": "node-4",
            "target_node_uuid": "node-5",
            "attributes": {},
        },
    ]

    import app.services.zep_entity_reader as mod

    monkeypatch.setattr(mod.ZepEntityReader, "get_all_edges", lambda self, graph_id: all_edges)

    result = reader.get_node_edges("node-1", graph_id="graph-1")

    assert {edge["uuid"] for edge in result} == {"e1", "e2"}
