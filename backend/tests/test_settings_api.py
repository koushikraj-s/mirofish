"""Tests for the `/api/settings` blueprint (`app.api.settings`)."""

import logging
import os

import pytest

import app.api.settings as settings_api
from app import create_app
from app.config import Config
from app.services import settings_store
from app.utils.logger import get_logger


@pytest.fixture(autouse=True)
def isolated_store_and_config(tmp_path, monkeypatch):
    monkeypatch.setattr(Config, "UPLOAD_FOLDER", str(tmp_path))
    original = {key: getattr(Config, key) for key in settings_store.EDITABLE_KEYS}
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(Config, key, value)


@pytest.fixture
def client():
    app = create_app()
    app.config.update(TESTING=True)
    return app.test_client()


# --------------------------------------------------------------------------
# GET /api/settings
# --------------------------------------------------------------------------


def test_get_settings_never_leaks_raw_secret(client, monkeypatch):
    monkeypatch.setattr(Config, "LLM_API_KEY", "sk-real-secret-value-should-not-leak")

    response = client.get('/api/settings')

    assert response.status_code == 200
    raw_body = response.get_data(as_text=True)
    assert "sk-real-secret-value-should-not-leak" not in raw_body

    data = response.get_json()["data"]
    assert data["LLM_API_KEY"]["set"] is True
    assert data["LLM_API_KEY"]["value"] != "sk-real-secret-value-should-not-leak"
    assert set(data.keys()) == set(settings_store.EDITABLE_KEYS)
    assert "SECRET_KEY" not in data


def test_get_settings_reports_non_secret_values_verbatim(client, monkeypatch):
    monkeypatch.setattr(Config, "LLM_BASE_URL", "https://example.test/v1")
    response = client.get('/api/settings')
    data = response.get_json()["data"]
    # Base URL/model/URI/user are not secrets -- the UI needs the real
    # value to let a user meaningfully edit it.
    assert data["LLM_BASE_URL"]["value"] == "https://example.test/v1"


# --------------------------------------------------------------------------
# PUT /api/settings
# --------------------------------------------------------------------------


def test_put_settings_hot_swaps_and_never_echoes_raw_value(client):
    response = client.put('/api/settings', json={"LLM_API_KEY": "sk-newly-submitted-value-999"})

    assert response.status_code == 200
    raw_body = response.get_data(as_text=True)
    assert "sk-newly-submitted-value-999" not in raw_body

    payload = response.get_json()
    assert payload["success"] is True
    assert payload["data"]["LLM_API_KEY"]["source"] == "override"
    assert payload["warnings"] == []
    assert Config.LLM_API_KEY == "sk-newly-submitted-value-999"

    path = settings_store._store_path()
    assert os.path.exists(path)


def test_put_settings_rejects_non_dict_body(client):
    response = client.put('/api/settings', json=["not", "a", "dict"])
    assert response.status_code == 400
    assert response.get_json()["success"] is False


def test_put_settings_rejects_non_string_values(client):
    response = client.put('/api/settings', json={"LLM_API_KEY": 12345})
    assert response.status_code == 400
    assert response.get_json()["success"] is False


def test_put_settings_ignores_non_editable_keys_with_warning(client):
    original_secret_key = Config.SECRET_KEY

    response = client.put('/api/settings', json={
        "SECRET_KEY": "attempted-override",
        "LLM_MODEL_NAME": "new-model-name",
    })

    assert response.status_code == 200
    payload = response.get_json()
    assert Config.SECRET_KEY == original_secret_key
    assert any("SECRET_KEY" in warning for warning in payload["warnings"])
    assert payload["data"]["LLM_MODEL_NAME"]["value"] == "new-model-name"


def test_put_settings_clearing_a_key_reverts_to_previous_default(client):
    original = Config.LLM_MODEL_NAME

    client.put('/api/settings', json={"LLM_MODEL_NAME": "temporary-override"})
    assert Config.LLM_MODEL_NAME == "temporary-override"

    response = client.put('/api/settings', json={"LLM_MODEL_NAME": None})
    assert response.status_code == 200
    assert Config.LLM_MODEL_NAME == original


# --------------------------------------------------------------------------
# POST /api/settings/test
# --------------------------------------------------------------------------


def test_llm_probe_reports_failure_as_ok_false_with_http_200(client, monkeypatch):
    class FakeAuthError(Exception):
        status_code = 401

    class FakeLLMClient:
        def __init__(self, **kwargs):
            pass

        def chat(self, *args, **kwargs):
            raise FakeAuthError("boom")

    monkeypatch.setattr(settings_api, "LLMClient", FakeLLMClient)
    monkeypatch.setattr(Config, "LLM_API_KEY", "sk-existing-key")

    response = client.post('/api/settings/test', json={})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["success"] is True
    assert payload["data"]["ok"] is False
    assert payload["data"]["category"] == "auth"
    assert "latency_ms" in payload["data"]
    # Never leak the (fake) key or any internal exception text.
    assert "sk-existing-key" not in response.get_data(as_text=True)


def test_llm_probe_succeeds(client, monkeypatch):
    class FakeLLMClient:
        def __init__(self, **kwargs):
            pass

        def chat(self, *args, **kwargs):
            return "pong"

    monkeypatch.setattr(settings_api, "LLMClient", FakeLLMClient)
    monkeypatch.setattr(Config, "LLM_API_KEY", "sk-existing-key")

    response = client.post('/api/settings/test', json={})
    payload = response.get_json()
    assert payload["data"]["ok"] is True
    assert isinstance(payload["data"]["latency_ms"], int)


def test_llm_probe_falls_back_to_saved_values_for_omitted_fields(client, monkeypatch):
    seen_kwargs = {}

    class RecordingLLMClient:
        def __init__(self, **kwargs):
            seen_kwargs.update(kwargs)

        def chat(self, *args, **kwargs):
            return "pong"

    monkeypatch.setattr(settings_api, "LLMClient", RecordingLLMClient)
    monkeypatch.setattr(Config, "LLM_API_KEY", "sk-saved-key")
    monkeypatch.setattr(Config, "LLM_BASE_URL", "https://saved.example/v1")
    monkeypatch.setattr(Config, "LLM_MODEL_NAME", "saved-model")

    response = client.post('/api/settings/test', json={"llm_model_name": "candidate-model"})

    assert response.status_code == 200
    assert seen_kwargs["api_key"] == "sk-saved-key"
    assert seen_kwargs["base_url"] == "https://saved.example/v1"
    assert seen_kwargs["model"] == "candidate-model"


def test_llm_probe_no_key_configured(client, monkeypatch):
    monkeypatch.setattr(Config, "LLM_API_KEY", "")
    response = client.post('/api/settings/test', json={})
    payload = response.get_json()
    assert payload["success"] is True
    assert payload["data"]["ok"] is False
    assert payload["data"]["category"] == "auth"


# --------------------------------------------------------------------------
# POST /api/settings/test-neo4j
# --------------------------------------------------------------------------


def test_neo4j_probe_reports_failure_as_ok_false_with_http_200(client, monkeypatch):
    from neo4j.exceptions import ServiceUnavailable
    import neo4j

    class FakeDriver:
        def verify_connectivity(self):
            raise ServiceUnavailable("simulated down")

        def close(self):
            pass

    class FakeGraphDatabase:
        @staticmethod
        def driver(uri, auth=None, **kwargs):
            return FakeDriver()

    monkeypatch.setattr(neo4j, "GraphDatabase", FakeGraphDatabase)

    response = client.post('/api/settings/test-neo4j', json={"neo4j_uri": "bolt://fake:7687"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["success"] is True
    assert payload["data"]["ok"] is False
    assert payload["data"]["category"] == "network"


def test_neo4j_probe_succeeds_and_closes_driver(client, monkeypatch):
    import neo4j

    closed = []

    class FakeDriver:
        def verify_connectivity(self):
            return None

        def close(self):
            closed.append(True)

    class FakeGraphDatabase:
        @staticmethod
        def driver(uri, auth=None, **kwargs):
            return FakeDriver()

    monkeypatch.setattr(neo4j, "GraphDatabase", FakeGraphDatabase)

    response = client.post('/api/settings/test-neo4j', json={"neo4j_uri": "bolt://fake:7687"})
    payload = response.get_json()
    assert payload["data"]["ok"] is True
    assert closed == [True]


def test_neo4j_probe_no_uri_configured(client, monkeypatch):
    monkeypatch.setattr(Config, "NEO4J_URI", "")
    response = client.post('/api/settings/test-neo4j', json={})
    payload = response.get_json()
    assert payload["success"] is True
    assert payload["data"]["ok"] is False


# --------------------------------------------------------------------------
# log_request redaction (app/__init__.py)
# --------------------------------------------------------------------------


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def test_log_request_redacts_sensitive_keys(client):
    request_logger = get_logger('mirofish.request')
    handler = _ListHandler()
    handler.setLevel(logging.DEBUG)
    previous_level = request_logger.level
    request_logger.setLevel(logging.DEBUG)
    request_logger.addHandler(handler)
    try:
        client.put(
            '/api/settings',
            json={
                "LLM_API_KEY": "sk-must-not-appear-in-logs-abc",
                "NEO4J_PASSWORD": "also-must-not-appear-hunter2",
            },
        )
    finally:
        request_logger.removeHandler(handler)
        request_logger.setLevel(previous_level)

    joined = "\n".join(handler.messages)
    assert "sk-must-not-appear-in-logs-abc" not in joined
    assert "also-must-not-appear-hunter2" not in joined
    assert "REDACTED" in joined


def test_log_request_leaves_non_sensitive_keys_untouched(client):
    request_logger = get_logger('mirofish.request')
    handler = _ListHandler()
    handler.setLevel(logging.DEBUG)
    previous_level = request_logger.level
    request_logger.setLevel(logging.DEBUG)
    request_logger.addHandler(handler)
    try:
        client.put('/api/settings', json={"LLM_MODEL_NAME": "totally-fine-to-log"})
    finally:
        request_logger.removeHandler(handler)
        request_logger.setLevel(previous_level)

    joined = "\n".join(handler.messages)
    assert "totally-fine-to-log" in joined
