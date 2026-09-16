from pathlib import Path
from types import SimpleNamespace

import pytest
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

from app.utils import zep


def test_permanent_errors_fail_without_retry():
    calls = []

    def operation():
        calls.append(True)
        raise ValueError("bad query")

    with pytest.raises(ValueError):
        zep.call_zep_read_with_retry(
            operation,
            operation_name="permanent failure",
            sleep=lambda _seconds: None,
        )

    assert len(calls) == 1


def _neo4j_error(cls, code, message="test"):
    error = cls(message)
    error._neo4j_code = code
    return error


def test_transient_neo4j_error_is_retried():
    calls = []
    sleeps = []

    def operation():
        calls.append(True)
        if len(calls) == 1:
            raise _neo4j_error(
                TransientError, "Neo.TransientError.Transaction.DeadlockDetected"
            )
        return "ok"

    result = zep.call_zep_read_with_retry(
        operation,
        operation_name="transient read",
        initial_delay=2.0,
        sleep=sleeps.append,
    )

    assert result == "ok"
    assert len(calls) == 2
    assert sleeps == [2.0]


def test_service_unavailable_is_retryable():
    assert zep.is_retryable_zep_error(ServiceUnavailable("down"))
    assert zep.is_retryable_zep_error(SessionExpired("expired"))
    assert zep.is_retryable_zep_error(ConnectionError("refused"))
    assert zep.is_retryable_zep_error(TimeoutError("timed out"))


def test_permanent_neo4j_error_is_not_retryable():
    from neo4j.exceptions import ClientError

    assert not zep.is_retryable_zep_error(
        _neo4j_error(ClientError, "Neo.ClientError.Statement.SyntaxError", "bad cypher")
    )
    assert not zep.is_retryable_zep_error(ValueError("unrelated"))


def test_graphiti_client_is_shared_and_process_wide(monkeypatch):
    created = []

    class FakeGraphiti:
        def __init__(self, **kwargs):
            created.append(kwargs)
            self.kwargs = kwargs

        async def build_indices_and_constraints(self, delete_existing: bool = False):
            return None

    monkeypatch.setattr(zep, "Graphiti", FakeGraphiti)
    monkeypatch.setattr(zep, "MiroFishGraphitiLLMClient", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr(zep, "SentenceTransformersEmbedder", lambda: SimpleNamespace())
    monkeypatch.setattr(zep, "OpenAIRerankerClient", lambda **kwargs: SimpleNamespace())
    zep.clear_zep_client_cache()

    first = zep.get_zep_client()
    second = zep.get_zep_client()

    assert first is second
    assert len(created) == 1
    zep.clear_zep_client_cache()


def test_neo4j_config_has_local_dev_defaults():
    from app.config import Config

    assert Config.NEO4J_URI
    assert Config.NEO4J_USER
    assert Config.NEO4J_PASSWORD


def test_zep_cloud_env_vars_are_gone_from_env_example():
    env_example = Path(__file__).resolve().parents[2] / ".env.example"
    contents = env_example.read_text(encoding="utf-8")

    assert "ZEP_API_KEY" not in contents
    assert "NEO4J_URI" in contents
    assert "NEO4J_PASSWORD" in contents
