"""Tests for `app.services.settings_store` and the atomic JSON I/O it
relies on (`app.utils.atomic_io`).

Every test isolates the credentials store under `tmp_path` (via
`Config.UPLOAD_FOLDER`) and restores `Config`'s seven editable attributes
afterward, since `Config` is process-global mutable state and
`apply_to_config()`/`apply_and_propagate()` really do `setattr` it.
"""

import json
import os
import stat
import threading
from types import SimpleNamespace

import pytest

from app.config import Config
from app.services import settings_store
from app.utils import zep as zep_module
from app.utils.atomic_io import atomic_write_json, read_json_tolerant


@pytest.fixture(autouse=True)
def isolated_store_and_config(tmp_path, monkeypatch):
    monkeypatch.setattr(Config, "UPLOAD_FOLDER", str(tmp_path))
    original = {key: getattr(Config, key) for key in settings_store.EDITABLE_KEYS}
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(Config, key, value)


# --------------------------------------------------------------------------
# atomic_io
# --------------------------------------------------------------------------


def test_atomic_write_survives_a_simulated_partial_write(tmp_path, monkeypatch):
    path = str(tmp_path / "creds.json")
    atomic_write_json(path, {"a": 1})
    assert read_json_tolerant(path) == {"a": 1}

    def boom(*args, **kwargs):
        raise OSError("disk full (simulated)")

    # Fail partway through the write (after the temp file is created and
    # opened, before the atomic rename). The previously-committed file at
    # `path` must be untouched, and no leftover `.tmp` file should remain.
    monkeypatch.setattr(os, "fsync", boom)
    with pytest.raises(OSError):
        atomic_write_json(path, {"a": 2})
    monkeypatch.undo()

    assert read_json_tolerant(path) == {"a": 1}
    assert not os.path.exists(path + ".tmp")


def test_read_json_tolerant_degrades_on_truncated_file(tmp_path):
    path = str(tmp_path / "creds.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write('{"a": 1, "b": [1, 2,')  # deliberately truncated / invalid

    assert read_json_tolerant(path, default="fallback") == "fallback"


def test_read_json_tolerant_degrades_on_missing_file(tmp_path):
    path = str(tmp_path / "does-not-exist.json")
    assert read_json_tolerant(path, default={"x": 1}) == {"x": 1}
    assert read_json_tolerant(path) is None


def test_atomic_write_sets_requested_mode(tmp_path):
    path = str(tmp_path / "creds.json")
    atomic_write_json(path, {"a": 1}, mode=0o600)
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600


# --------------------------------------------------------------------------
# settings_store: persistence + layering
# --------------------------------------------------------------------------


def test_load_overrides_is_partial():
    settings_store.save_overrides({"LLM_API_KEY": "sk-only-this-one"})
    assert settings_store.load_overrides() == {"LLM_API_KEY": "sk-only-this-one"}


def test_save_overrides_merges_instead_of_replacing():
    settings_store.save_overrides({"LLM_API_KEY": "sk-first"})
    settings_store.save_overrides({"NEO4J_PASSWORD": "second-value"})
    overrides = settings_store.load_overrides()
    assert overrides == {"LLM_API_KEY": "sk-first", "NEO4J_PASSWORD": "second-value"}


def test_save_overrides_empty_string_clears_a_key():
    settings_store.save_overrides({"LLM_API_KEY": "sk-abc"})
    assert "LLM_API_KEY" in settings_store.load_overrides()

    settings_store.save_overrides({"LLM_API_KEY": ""})
    assert "LLM_API_KEY" not in settings_store.load_overrides()


def test_save_overrides_none_clears_a_key():
    settings_store.save_overrides({"LLM_API_KEY": "sk-abc"})
    settings_store.save_overrides({"LLM_API_KEY": None})
    assert "LLM_API_KEY" not in settings_store.load_overrides()


def test_save_overrides_ignores_non_editable_keys():
    settings_store.save_overrides({"SECRET_KEY": "not-editable", "LLM_API_KEY": "sk-abc"})
    overrides = settings_store.load_overrides()
    assert overrides == {"LLM_API_KEY": "sk-abc"}


def test_store_file_and_dir_permissions():
    settings_store.save_overrides({"LLM_API_KEY": "sk-abc"})
    path = settings_store._store_path()
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode) == 0o700


def test_apply_to_config_layers_override_over_default():
    original = Config.LLM_MODEL_NAME
    settings_store.save_overrides({"LLM_MODEL_NAME": "override-model"})
    settings_store.apply_to_config()

    assert Config.LLM_MODEL_NAME == "override-model"
    # Every other key falls through untouched.
    assert Config.NEO4J_URI == settings_store._ORIGINAL_DEFAULTS["NEO4J_URI"]
    assert original != "override-model"


def test_apply_to_config_reverts_a_cleared_override():
    """Regression test: setattr-ing an override and then simply not
    setattr-ing it again after it's cleared leaves Config permanently
    stuck on the stale value. apply_to_config() must actively restore the
    original default, not just skip absent keys."""

    original = Config.LLM_MODEL_NAME
    settings_store.save_overrides({"LLM_MODEL_NAME": "temporary-override"})
    settings_store.apply_to_config()
    assert Config.LLM_MODEL_NAME == "temporary-override"

    settings_store.save_overrides({"LLM_MODEL_NAME": None})
    settings_store.apply_to_config()
    assert Config.LLM_MODEL_NAME == original


def test_config_validate_passes_with_only_override_credentials(monkeypatch):
    """A user who configures everything via the settings UI and leaves
    `.env` empty must not fail `Config.validate()` (which `run.py` uses to
    decide whether to `sys.exit(1)` at boot)."""

    monkeypatch.setattr(Config, "LLM_API_KEY", "")
    monkeypatch.setattr(Config, "NEO4J_URI", "")

    settings_store.save_overrides({
        "LLM_API_KEY": "sk-from-ui-only",
        "NEO4J_URI": "bolt://ui-configured-host:7687",
    })
    settings_store.apply_to_config()

    assert Config.validate() == []


def test_concurrent_save_overrides_do_not_lose_updates():
    """Serializes concurrent writers via the module-level lock; if the
    read-merge-write cycle weren't serialized (or the write weren't
    atomic), some of these seven concurrent single-key writes would be
    lost to a race."""

    def worker(key, value):
        settings_store.save_overrides({key: value})

    threads = [
        threading.Thread(target=worker, args=(key, f"value-for-{key}"))
        for key in settings_store.EDITABLE_KEYS
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    overrides = settings_store.load_overrides()
    for key in settings_store.EDITABLE_KEYS:
        assert overrides[key] == f"value-for-{key}"


# --------------------------------------------------------------------------
# settings_store: masking + source reporting
# --------------------------------------------------------------------------


def test_describe_settings_never_leaks_a_raw_secret():
    settings_store.save_overrides({
        "LLM_API_KEY": "sk-super-secret-value-123456",
        "NEO4J_PASSWORD": "hunter2-hunter2-hunter2",
    })
    settings_store.apply_to_config()

    described = settings_store.describe_settings()
    blob = json.dumps(described)
    assert "sk-super-secret-value-123456" not in blob
    assert "hunter2-hunter2-hunter2" not in blob

    assert described["LLM_API_KEY"]["set"] is True
    assert described["LLM_API_KEY"]["source"] == "override"
    assert described["NEO4J_PASSWORD"]["value"] != "hunter2-hunter2-hunter2"


def test_describe_settings_reports_unset_key(monkeypatch):
    monkeypatch.setattr(Config, "LLM_REASONING_EFFORT", "")
    described = settings_store.describe_settings()
    assert described["LLM_REASONING_EFFORT"]["set"] is False
    assert described["LLM_REASONING_EFFORT"]["value"] is None


def test_describe_sources_env_vs_default_vs_override(monkeypatch):
    monkeypatch.setitem(settings_store._ORIGINAL_ENV_PRESENT, "LLM_MODEL_NAME", True)
    monkeypatch.setitem(settings_store._ORIGINAL_ENV_PRESENT, "LLM_REASONING_EFFORT", False)

    sources = settings_store.describe_sources()
    assert sources["LLM_MODEL_NAME"] == "env"
    assert sources["LLM_REASONING_EFFORT"] == "default"

    settings_store.save_overrides({"LLM_MODEL_NAME": "custom-model"})
    sources = settings_store.describe_sources()
    assert sources["LLM_MODEL_NAME"] == "override"


# --------------------------------------------------------------------------
# settings_store: propagation ordering + Graphiti client hot-swap
# --------------------------------------------------------------------------


def _install_fake_graphiti(monkeypatch, *, close=None):
    created = []

    class FakeGraphiti:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            created.append(self)

        async def build_indices_and_constraints(self, delete_existing: bool = False):
            return None

        async def close(self):
            if close is not None:
                await close(self)

    monkeypatch.setattr(zep_module, "Graphiti", FakeGraphiti)
    monkeypatch.setattr(zep_module, "MiroFishGraphitiLLMClient", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr(zep_module, "SentenceTransformersEmbedder", lambda: SimpleNamespace())
    monkeypatch.setattr(zep_module, "OpenAIRerankerClient", lambda **kwargs: SimpleNamespace())
    zep_module.clear_zep_client_cache()
    return created


def test_apply_and_propagate_closes_old_client_not_a_new_one(monkeypatch):
    closed = []

    async def record_close(client):
        closed.append(client)

    created = _install_fake_graphiti(monkeypatch, close=record_close)

    old_client = zep_module.get_zep_client()
    assert created == [old_client]

    result = settings_store.apply_and_propagate({"NEO4J_PASSWORD": "brand-new-password"})

    assert closed == [old_client]
    assert result["warnings"] == []

    new_client = zep_module.get_zep_client()
    assert new_client is not old_client
    assert created == [old_client, new_client]
    assert new_client not in closed

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_evicts_cache_for_llm_only_change(monkeypatch):
    """Critical subtlety: the client cache key is (uri, user, password)
    only. An LLM-only change must still evict + close the cached client,
    because that client's LLM client was baked in at construction time --
    otherwise a swapped-in LLM key would never actually take effect for
    graph memory."""

    closed = []

    async def record_close(client):
        closed.append(client)

    created = _install_fake_graphiti(monkeypatch, close=record_close)

    old_client = zep_module.get_zep_client()
    settings_store.apply_and_propagate({"LLM_API_KEY": "sk-new-llm-key"})

    assert closed == [old_client]

    new_client = zep_module.get_zep_client()
    assert new_client is not old_client
    assert len(created) == 2

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_warns_but_does_not_raise_on_close_failure(monkeypatch):
    async def failing_close(client):
        raise RuntimeError("driver pool teardown failed (simulated)")

    _install_fake_graphiti(monkeypatch, close=failing_close)
    zep_module.get_zep_client()

    result = settings_store.apply_and_propagate({"LLM_API_KEY": "sk-another-new-key"})

    assert any("failed to close" in warning.lower() for warning in result["warnings"])
    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_step_order(monkeypatch):
    """Directly proves the mandated ordering: persist, THEN snapshot the
    old client (before Config is mutated), THEN apply to Config, THEN
    evict + close the snapshotted old client."""

    calls = []

    real_save_overrides = settings_store.save_overrides

    def spy_save_overrides(patch):
        calls.append("persist")
        return real_save_overrides(patch)

    monkeypatch.setattr(settings_store, "save_overrides", spy_save_overrides)

    real_apply_to_config = settings_store.apply_to_config

    def spy_apply_to_config():
        calls.append("apply_to_config")
        return real_apply_to_config()

    monkeypatch.setattr(settings_store, "apply_to_config", spy_apply_to_config)

    async def record_close(client):
        calls.append("close_old_client")

    _install_fake_graphiti(monkeypatch, close=record_close)
    zep_module.get_zep_client()

    real_snapshot = zep_module.snapshot_current_client

    def spy_snapshot():
        calls.append("snapshot_old_client")
        return real_snapshot()

    monkeypatch.setattr(zep_module, "snapshot_current_client", spy_snapshot)

    real_clear = zep_module.clear_zep_client_cache

    def spy_clear():
        calls.append("clear_cache")
        return real_clear()

    monkeypatch.setattr(zep_module, "clear_zep_client_cache", spy_clear)

    settings_store.apply_and_propagate({"NEO4J_URI": "bolt://otherhost:7687"})

    assert calls.index("persist") < calls.index("snapshot_old_client")
    assert calls.index("snapshot_old_client") < calls.index("apply_to_config")
    assert calls.index("apply_to_config") < calls.index("clear_cache")
    assert calls.index("clear_cache") <= calls.index("close_old_client")

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_ignores_unknown_keys_with_warning():
    result = settings_store.apply_and_propagate({
        "SECRET_KEY": "should-not-apply",
        "LLM_MODEL_NAME": "accepted-model",
    })
    assert any("SECRET_KEY" in warning for warning in result["warnings"])
    assert Config.LLM_MODEL_NAME == "accepted-model"


def test_snapshot_current_client_returns_none_when_cache_empty(monkeypatch):
    zep_module.clear_zep_client_cache()
    assert zep_module.snapshot_current_client() is None


def test_close_client_handles_none():
    assert zep_module.close_client(None) is True
