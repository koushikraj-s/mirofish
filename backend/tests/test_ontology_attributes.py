from app.services.ontology_generator import OntologyGenerator
from app.services.graph_builder import GraphBuilderService
from app.utils.ontology import (
    MAX_ONTOLOGY_ATTRIBUTES,
    MAX_ONTOLOGY_SOURCE_TARGETS,
    normalize_ontology_attribute,
    normalize_ontology_attributes,
)


def test_normalize_string_attribute():
    assert normalize_ontology_attribute("role") == {
        "name": "role",
        "type": "text",
        "description": "role",
    }


def test_preserve_valid_dictionary_attribute():
    original = {"name": "role", "type": "text", "description": "Public role"}
    assert normalize_ontology_attribute(original) == original
    assert normalize_ontology_attribute(original) is not original


def test_reject_unusable_attribute_shapes():
    for value in (None, 7, [], {}, {"name": None}, {"name": ""}, "   "):
        assert normalize_ontology_attribute(value) is None


def test_attribute_list_is_non_empty_and_capped():
    assert normalize_ontology_attributes(None) == [{
        "name": "details",
        "type": "text",
        "description": "Additional details about this ontology type.",
    }]

    attributes = [None] + [f"field_{index}" for index in range(12)]
    normalized = normalize_ontology_attributes(attributes)

    assert len(normalized) == MAX_ONTOLOGY_ATTRIBUTES
    assert [attribute["name"] for attribute in normalized] == [
        f"field_{index}" for index in range(MAX_ONTOLOGY_ATTRIBUTES)
    ]


def test_generator_normalizes_entity_and_edge_attributes():
    result = OntologyGenerator(llm_client=object())._validate_and_process({
        "entity_types": [{"name": "speaker", "attributes": ["role", None]}],
        "edge_types": [{"name": "quotes", "attributes": ["source_url", {}]}],
    })

    assert result["entity_types"][0]["attributes"] == [{
        "name": "role",
        "type": "text",
        "description": "role",
    }]
    assert result["edge_types"][0]["attributes"] == [{
        "name": "source_url",
        "type": "text",
        "description": "source_url",
    }]


def test_generator_adds_a_property_to_empty_custom_types():
    result = OntologyGenerator(llm_client=object())._validate_and_process({
        "entity_types": [{"name": "speaker", "attributes": []}],
        "edge_types": [{"name": "quotes", "attributes": []}],
    })

    assert result["entity_types"][0]["attributes"][0]["name"] == "details"
    assert result["edge_types"][0]["attributes"][0]["name"] == "details"


def test_build_pydantic_types_renames_reserved_attribute_names():
    entity_types, _edge_types, _edge_type_map = OntologyGenerator.build_pydantic_types({
        "entity_types": [{
            "name": "Speaker",
            "attributes": ["role", None, {"name": "summary"}],
        }],
        "edge_types": [],
    })

    speaker = entity_types["Speaker"]
    assert set(speaker.model_fields.keys()) == {"role", "entity_summary"}


def test_graph_builder_set_ontology_builds_runtime_pydantic_types_and_edge_map():
    builder = object.__new__(GraphBuilderService)
    import threading
    builder._graph_ontologies = {}
    builder._ontology_lock = threading.Lock()

    builder.set_ontology("graph-id", {
        "entity_types": [{
            "name": "Speaker",
            "attributes": ["graph_id"] + [
                f"field_{index}" for index in range(10)
            ],
        }],
        "edge_types": [{
            "name": "MENTIONS",
            "attributes": [],
            "source_targets": [{"source": "Speaker", "target": "Speaker"}],
        }],
    })

    entity_types, edge_types, edge_type_map = builder._graph_ontologies["graph-id"]

    speaker = entity_types["Speaker"]
    assert len(speaker.model_fields) == MAX_ONTOLOGY_ATTRIBUTES
    assert "entity_graph_id" in speaker.model_fields
    assert speaker.model_fields["entity_graph_id"].description == "graph_id"

    mentions = edge_types["MENTIONS"]
    assert list(mentions.model_fields.keys()) == ["details"]
    assert mentions.model_fields["details"].description == (
        "Additional details about this ontology type."
    )
    assert edge_type_map[("Speaker", "Speaker")] == ["MENTIONS"]


def test_graph_builder_passes_an_empty_entity_mapping_for_edge_only_ontology():
    builder = object.__new__(GraphBuilderService)
    import threading
    builder._graph_ontologies = {}
    builder._ontology_lock = threading.Lock()

    builder.set_ontology("graph-id", {
        "entity_types": [],
        "edge_types": [{
            "name": "RELATED_TO",
            "attributes": ["reason"],
            "source_targets": [{"source": "Entity", "target": "Entity"}],
        }],
    })

    entity_types, _edge_types, _edge_type_map = builder._graph_ontologies["graph-id"]
    assert entity_types == {}


def test_graph_builder_deduplicates_and_caps_edge_source_targets():
    builder = object.__new__(GraphBuilderService)
    import threading
    builder._graph_ontologies = {}
    builder._ontology_lock = threading.Lock()

    source_targets = [
        {"source": f"Source{index}", "target": f"Target{index}"}
        for index in range(MAX_ONTOLOGY_SOURCE_TARGETS + 2)
    ]
    source_targets.insert(1, dict(source_targets[0]))

    builder.set_ontology("graph-id", {
        "entity_types": [],
        "edge_types": [{
            "name": "RELATED_TO",
            "attributes": ["reason"],
            "source_targets": source_targets,
        }],
    })

    _entity_types, _edge_types, edge_type_map = builder._graph_ontologies["graph-id"]
    keys = [key for key in edge_type_map if edge_type_map[key] == ["RELATED_TO"]]
    assert len(keys) == MAX_ONTOLOGY_SOURCE_TARGETS
    assert set(keys) == {
        (f"Source{index}", f"Target{index}")
        for index in range(MAX_ONTOLOGY_SOURCE_TARGETS)
    }


def test_generator_ignores_invalid_entries_and_normalizes_edge_names():
    source_targets = [
        {"source": "speaker", "target": "news outlet"},
        {"source": "speaker", "target": "news outlet"},
        None,
    ] + [
        {"source": "speaker", "target": "news outlet" if index == 0 else "Person"}
        for index in range(12)
    ]

    result = OntologyGenerator(llm_client=object())._validate_and_process({
        "entity_types": ["speaker", None, 7, {"name": "news outlet"}],
        "edge_types": [
            "unusable edge",
            None,
            {"name": "worksFor", "source_targets": source_targets},
            {"name": "works-for", "source_targets": []},
        ],
    })

    assert [entity["name"] for entity in result["entity_types"][:2]] == [
        "Speaker",
        "NewsOutlet",
    ]
    assert [edge["name"] for edge in result["edge_types"]] == ["WORKS_FOR"]
    assert result["edge_types"][0]["source_targets"] == [
        {"source": "Speaker", "target": "NewsOutlet"},
        {"source": "Speaker", "target": "Person"},
    ]


def test_generator_caps_after_discarding_invalid_edge_endpoints():
    invalid_first = [
        {"source": f"Removed{index}", "target": "AlsoRemoved"}
        for index in range(MAX_ONTOLOGY_SOURCE_TARGETS)
    ]
    result = OntologyGenerator(llm_client=object())._validate_and_process({
        "entity_types": [{"name": "person"}, {"name": "organization"}],
        "edge_types": [{
            "name": "works_for",
            "source_targets": invalid_first + [
                {"source": "person", "target": "organization"}
            ],
        }],
    })

    assert result["edge_types"][0]["source_targets"] == [
        {"source": "Person", "target": "Organization"}
    ]
