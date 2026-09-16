"""Mid-run LLM credential reload.

Covers:

1. `app.services.simulation_ipc.write_credentials_reload_broadcast` --
   the writer-side helper Flask uses to bump
   `<sim_dir>/credentials_reload.json` (version strictly increasing,
   file never deleted).
2. `app.services.settings_store.apply_and_propagate`'s 6th (additive)
   step: it broadcasts only when an LLM credential actually changed, it
   runs after the existing load-bearing 5-step sequence, and a broadcast
   failure never fails the credential swap itself (fail-soft).
3. The reader/apply side duplicated identically in all three simulation
   scripts (`run_twitter_simulation.py`, `run_reddit_simulation.py`,
   `run_parallel_simulation.py`): tolerant parsing of the broadcast file,
   the mtime-gated cheap poll, stale/equal version rejection, and the
   hasattr-guarded, fail-soft rebuild of camel-ai's `OpenAIModel` private
   client attributes.
4. A canary against camel-ai's actual internals: if a camel-ai upgrade
   ever renames/removes `_api_key`/`_url`/`_client`/`_async_client`, this
   must fail loudly here at test time instead of silently mid-simulation.
"""

import json
import os
import sys
import time
from types import SimpleNamespace

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BACKEND_DIR = os.path.abspath(os.path.join(_TESTS_DIR, ".."))
_SCRIPTS_DIR = os.path.join(_BACKEND_DIR, "scripts")

if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import run_parallel_simulation as parallel_script  # noqa: E402
import run_reddit_simulation as reddit_script  # noqa: E402
import run_twitter_simulation as twitter_script  # noqa: E402

from app.config import Config  # noqa: E402
from app.services import settings_store  # noqa: E402
from app.services import simulation_ipc  # noqa: E402
from app.services import simulation_runner as runner_module  # noqa: E402

ALL_SCRIPTS = [twitter_script, reddit_script, parallel_script]
ALL_SCRIPT_IDS = ["twitter", "reddit", "parallel"]


@pytest.fixture(autouse=True)
def isolated_store_and_config(tmp_path, monkeypatch):
    """Same isolation contract as test_settings_store.py: never let these
    tests touch the real `backend/uploads/config/credentials.json` or
    leave `Config`'s editable attributes mutated for later tests."""
    monkeypatch.setattr(Config, "UPLOAD_FOLDER", str(tmp_path))
    original = {key: getattr(Config, key) for key in settings_store.EDITABLE_KEYS}
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(Config, key, value)


# ==========================================================================
# Fake camel-ai model double (for script-level reload logic tests)
# ==========================================================================


class _FakeOpenAIClient:
    """Stands in for camel-ai's real `openai.OpenAI`/`AsyncOpenAI` client
    objects. `_apply_credentials_reload` only ever does
    `type(model._client)(timeout=..., max_retries=..., base_url=..., api_key=...)`
    so any class with that constructor shape is a faithful double."""

    def __init__(self, *, timeout=None, max_retries=3, base_url=None, api_key=None):
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_url = base_url
        self.api_key = api_key


class _FakeAsyncOpenAIClient(_FakeOpenAIClient):
    pass


class _ExplodingClient:
    """A client class whose constructor always fails, to test the
    fail-soft path when rebuilding the client itself raises."""

    def __init__(self, **kwargs):
        raise RuntimeError("client construction failed (simulated)")


def _make_fake_model(api_key="old-exhausted-key", base_url="https://old.example.com/v1"):
    model = SimpleNamespace()
    model._api_key = api_key
    model._url = base_url
    model._timeout = 180.0
    model._max_retries = 3
    model._client = _FakeOpenAIClient(
        timeout=model._timeout, max_retries=model._max_retries, base_url=base_url, api_key=api_key
    )
    model._async_client = _FakeAsyncOpenAIClient(
        timeout=model._timeout, max_retries=model._max_retries, base_url=base_url, api_key=api_key
    )
    return model


# ==========================================================================
# simulation_ipc.write_credentials_reload_broadcast (writer side)
# ==========================================================================


def test_write_credentials_reload_broadcast_creates_file_with_version_1(tmp_path):
    sim_dir = str(tmp_path / "sim-1")
    version = simulation_ipc.write_credentials_reload_broadcast(
        sim_dir, llm_api_key="sk-first", llm_base_url="https://api.example.com", llm_model_name="gpt-4o-mini"
    )
    assert version == 1

    path = os.path.join(sim_dir, simulation_ipc.CREDENTIALS_RELOAD_FILENAME)
    data = json.loads(open(path, encoding="utf-8").read())
    assert data["version"] == 1
    assert data["llm_api_key"] == "sk-first"
    assert data["llm_base_url"] == "https://api.example.com"
    assert data["llm_model_name"] == "gpt-4o-mini"
    assert "updated_at" in data


def test_write_credentials_reload_broadcast_increments_version(tmp_path):
    sim_dir = str(tmp_path / "sim-1")
    v1 = simulation_ipc.write_credentials_reload_broadcast(sim_dir, llm_api_key="sk-a", llm_base_url=None)
    v2 = simulation_ipc.write_credentials_reload_broadcast(sim_dir, llm_api_key="sk-b", llm_base_url=None)
    v3 = simulation_ipc.write_credentials_reload_broadcast(sim_dir, llm_api_key="sk-c", llm_base_url=None)
    assert (v1, v2, v3) == (1, 2, 3)


def test_write_credentials_reload_broadcast_never_deletes_file(tmp_path):
    sim_dir = str(tmp_path / "sim-1")
    simulation_ipc.write_credentials_reload_broadcast(sim_dir, llm_api_key="sk-a", llm_base_url=None)
    path = os.path.join(sim_dir, simulation_ipc.CREDENTIALS_RELOAD_FILENAME)
    assert os.path.exists(path)

    simulation_ipc.write_credentials_reload_broadcast(sim_dir, llm_api_key="sk-b", llm_base_url=None)
    assert os.path.exists(path)  # rewritten in place, never removed

    data = json.loads(open(path, encoding="utf-8").read())
    assert data["llm_api_key"] == "sk-b"
    assert data["version"] == 2


def test_write_credentials_reload_broadcast_sets_restrictive_permissions(tmp_path):
    import stat

    sim_dir = str(tmp_path / "sim-1")
    simulation_ipc.write_credentials_reload_broadcast(sim_dir, llm_api_key="sk-a", llm_base_url=None)
    path = os.path.join(sim_dir, simulation_ipc.CREDENTIALS_RELOAD_FILENAME)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


# ==========================================================================
# settings_store.apply_and_propagate: step 6 (credential reload broadcast)
# ==========================================================================


class _FakeGraphitiClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        return None

    async def close(self):
        return None


def _install_fake_graphiti(monkeypatch):
    from app.utils import zep as zep_module

    monkeypatch.setattr(zep_module, "Graphiti", _FakeGraphitiClient)
    monkeypatch.setattr(zep_module, "MiroFishGraphitiLLMClient", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr(zep_module, "SentenceTransformersEmbedder", lambda: SimpleNamespace())
    monkeypatch.setattr(zep_module, "OpenAIRerankerClient", lambda **kwargs: SimpleNamespace())
    zep_module.clear_zep_client_cache()
    return zep_module


def test_apply_and_propagate_broadcasts_to_every_running_simulation(monkeypatch, tmp_path):
    zep_module = _install_fake_graphiti(monkeypatch)

    run_state_dir = tmp_path / "uploads" / "simulations"
    run_state_dir.mkdir(parents=True)
    monkeypatch.setattr(runner_module.SimulationRunner, "RUN_STATE_DIR", str(run_state_dir))
    monkeypatch.setattr(
        runner_module.SimulationRunner,
        "get_running_simulations",
        classmethod(lambda cls: ["sim-live-1", "sim-live-2"]),
    )

    result = settings_store.apply_and_propagate({"LLM_API_KEY": "sk-fresh-key"})
    assert result["warnings"] == []

    for sim_id in ("sim-live-1", "sim-live-2"):
        broadcast_path = run_state_dir / sim_id / "credentials_reload.json"
        assert broadcast_path.exists()
        data = json.loads(broadcast_path.read_text(encoding="utf-8"))
        assert data["version"] == 1
        assert data["llm_api_key"] == "sk-fresh-key"

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_does_not_query_running_sims_for_non_llm_keys(monkeypatch):
    zep_module = _install_fake_graphiti(monkeypatch)

    queried = []
    monkeypatch.setattr(
        runner_module.SimulationRunner,
        "get_running_simulations",
        classmethod(lambda cls: queried.append(True) or []),
    )

    result = settings_store.apply_and_propagate({"NEO4J_PASSWORD": "brand-new-password"})
    assert result["warnings"] == []
    assert queried == []  # LLM credentials untouched -> broadcast is a pure no-op

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_broadcast_triggers_on_base_url_change_too(monkeypatch, tmp_path):
    zep_module = _install_fake_graphiti(monkeypatch)

    run_state_dir = tmp_path / "sims"
    monkeypatch.setattr(runner_module.SimulationRunner, "RUN_STATE_DIR", str(run_state_dir))
    monkeypatch.setattr(
        runner_module.SimulationRunner,
        "get_running_simulations",
        classmethod(lambda cls: ["sim-live"]),
    )

    settings_store.apply_and_propagate({"LLM_BASE_URL": "https://new-provider.example.com/v1"})

    broadcast_path = run_state_dir / "sim-live" / "credentials_reload.json"
    assert broadcast_path.exists()
    data = json.loads(broadcast_path.read_text(encoding="utf-8"))
    assert data["llm_base_url"] == "https://new-provider.example.com/v1"

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_broadcast_is_fail_soft(monkeypatch, tmp_path):
    """A broadcast failure (e.g. a disk error writing one simulation's
    reload file) must never undo or fail the credential swap that already
    committed in steps 1-5 -- it only ever adds a warning."""
    zep_module = _install_fake_graphiti(monkeypatch)

    monkeypatch.setattr(runner_module.SimulationRunner, "RUN_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        runner_module.SimulationRunner,
        "get_running_simulations",
        classmethod(lambda cls: ["sim-broken"]),
    )

    def boom(*args, **kwargs):
        raise OSError("disk full (simulated)")

    monkeypatch.setattr(simulation_ipc, "write_credentials_reload_broadcast", boom)

    result = settings_store.apply_and_propagate({"LLM_API_KEY": "sk-another-key"})

    assert Config.LLM_API_KEY == "sk-another-key"  # the swap itself still fully succeeded
    assert any("credential reload" in w.lower() for w in result["warnings"])

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_broadcast_is_fail_soft_when_runner_enumeration_fails(monkeypatch):
    zep_module = _install_fake_graphiti(monkeypatch)

    def boom(cls):
        raise RuntimeError("process table corrupted (simulated)")

    monkeypatch.setattr(runner_module.SimulationRunner, "get_running_simulations", classmethod(boom))

    result = settings_store.apply_and_propagate({"LLM_API_KEY": "sk-yet-another-key"})

    assert Config.LLM_API_KEY == "sk-yet-another-key"
    assert any("credential reload" in w.lower() for w in result["warnings"])

    zep_module.clear_zep_client_cache()


def test_apply_and_propagate_broadcast_runs_after_existing_five_steps(monkeypatch):
    """Proves the 6th step is additive at the end, never reordering the
    load-bearing persist -> snapshot -> apply_to_config -> mirror-env ->
    evict/close sequence."""
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

    zep_module = _install_fake_graphiti(monkeypatch)

    real_clear = zep_module.clear_zep_client_cache

    def spy_clear():
        calls.append("clear_cache")
        return real_clear()

    monkeypatch.setattr(zep_module, "clear_zep_client_cache", spy_clear)

    monkeypatch.setattr(
        runner_module.SimulationRunner, "get_running_simulations", classmethod(lambda cls: [])
    )

    real_broadcast = settings_store._broadcast_credentials_reload

    def spy_broadcast(patch, warnings):
        calls.append("broadcast")
        return real_broadcast(patch, warnings)

    monkeypatch.setattr(settings_store, "_broadcast_credentials_reload", spy_broadcast)

    settings_store.apply_and_propagate({"LLM_API_KEY": "sk-order-check"})

    assert calls == ["persist", "apply_to_config", "clear_cache", "broadcast"]

    zep_module.clear_zep_client_cache()


# ==========================================================================
# Script-level reader/apply logic (duplicated identically in all 3 scripts)
# ==========================================================================


@pytest.mark.parametrize("script", ALL_SCRIPTS, ids=ALL_SCRIPT_IDS)
class TestApplyCredentialsReload:
    def test_rebuilds_client_and_updates_attrs(self, script):
        model = _make_fake_model(api_key="old-exhausted-key", base_url="https://old.example.com/v1")
        old_client, old_async_client = model._client, model._async_client

        applied = script._apply_credentials_reload(
            model,
            {"version": 2, "llm_api_key": "sk-topped-up", "llm_base_url": "https://new.example.com/v1"},
            platform="twitter",
        )

        assert applied is True
        assert model._api_key == "sk-topped-up"
        assert model._url == "https://new.example.com/v1"
        assert model._client is not old_client
        assert model._async_client is not old_async_client
        assert model._client.api_key == "sk-topped-up"
        assert model._client.base_url == "https://new.example.com/v1"
        assert model._async_client.api_key == "sk-topped-up"

    def test_missing_api_key_skips(self, script):
        model = _make_fake_model()
        old_client = model._client

        applied = script._apply_credentials_reload(
            model, {"version": 2, "llm_api_key": None, "llm_base_url": "https://x"}, platform="twitter"
        )

        assert applied is False
        assert model._client is old_client
        assert model._api_key == "old-exhausted-key"

    def test_missing_attrs_fails_soft_camel_upgrade_canary(self, script):
        """Simulates a future camel-ai release that renamed away these
        private attributes. Must degrade to a no-op warning, never raise,
        so an optional reload never takes down a live simulation."""
        bare_model = SimpleNamespace()  # no _client/_async_client/_api_key/_url at all

        applied = script._apply_credentials_reload(
            bare_model, {"version": 2, "llm_api_key": "sk-new", "llm_base_url": None}, platform="twitter"
        )

        assert applied is False

    def test_client_construction_error_fails_soft(self, script):
        model = _make_fake_model(api_key="old-key")
        model._client = object.__new__(_ExplodingClient)
        model._async_client = object.__new__(_ExplodingClient)

        applied = script._apply_credentials_reload(
            model, {"version": 2, "llm_api_key": "sk-new", "llm_base_url": None}, platform="twitter"
        )

        assert applied is False
        assert model._api_key == "old-key"  # left untouched on failure


@pytest.mark.parametrize("script", ALL_SCRIPTS, ids=ALL_SCRIPT_IDS)
class TestPollAndApplyCredentialsReload:
    def test_no_broadcast_file_is_a_noop(self, script, tmp_path):
        model = _make_fake_model()
        last_mtime, last_version = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, None, 0, platform="twitter"
        )
        assert last_mtime is None
        assert last_version == 0
        assert model._api_key == "old-exhausted-key"

    def test_applies_new_version_and_tracks_state(self, script, tmp_path):
        path = tmp_path / script.CREDENTIALS_RELOAD_FILENAME
        path.write_text(
            json.dumps({"version": 1, "llm_api_key": "sk-new", "llm_base_url": "https://x/v1"}),
            encoding="utf-8",
        )
        model = _make_fake_model()

        last_mtime, last_version = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, None, 0, platform="twitter"
        )

        assert last_version == 1
        assert last_mtime == os.path.getmtime(str(path))
        assert model._api_key == "sk-new"

    def test_stale_or_equal_version_is_not_reapplied(self, script, tmp_path):
        path = tmp_path / script.CREDENTIALS_RELOAD_FILENAME
        path.write_text(
            json.dumps({"version": 1, "llm_api_key": "sk-first", "llm_base_url": None}), encoding="utf-8"
        )
        model = _make_fake_model()

        mtime1, version1 = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, None, 0, platform="twitter"
        )
        assert version1 == 1
        assert model._api_key == "sk-first"

        # File rewritten (new mtime) but with the *same* version number --
        # must not be re-applied even though mtime changed.
        time.sleep(0.01)
        path.write_text(
            json.dumps({"version": 1, "llm_api_key": "sk-should-not-apply", "llm_base_url": None}),
            encoding="utf-8",
        )
        mtime2, version2 = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, mtime1, version1, platform="twitter"
        )
        assert version2 == 1
        assert model._api_key == "sk-first"  # unchanged

        # An unchanged file (same mtime) is also skipped without even being
        # re-read.
        mtime3, version3 = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, mtime2, version2, platform="twitter"
        )
        assert (mtime3, version3) == (mtime2, version2)

    def test_malformed_json_is_ignored_with_a_warning_not_a_crash(self, script, tmp_path, capsys):
        path = tmp_path / script.CREDENTIALS_RELOAD_FILENAME
        path.write_text("{not valid json at all", encoding="utf-8")
        model = _make_fake_model()

        last_mtime, last_version = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, None, 0, platform="twitter"
        )

        assert last_version == 0
        assert model._api_key == "old-exhausted-key"
        assert "警告" in capsys.readouterr().out

    def test_missing_version_field_is_ignored(self, script, tmp_path):
        path = tmp_path / script.CREDENTIALS_RELOAD_FILENAME
        path.write_text(json.dumps({"llm_api_key": "sk-no-version"}), encoding="utf-8")
        model = _make_fake_model()

        _, last_version = script._poll_and_apply_credentials_reload(
            str(tmp_path), model, None, 0, platform="twitter"
        )

        assert last_version == 0
        assert model._api_key == "old-exhausted-key"

    def test_both_platforms_independently_apply_the_same_broadcast(self, script, tmp_path):
        """The fan-out bug this whole design exists to avoid: unlike
        ipc_commands/ (single-consumer, delete-on-read), both twitter and
        reddit must be able to read and apply the same broadcast
        independently, and the file must survive both reads."""
        path = tmp_path / script.CREDENTIALS_RELOAD_FILENAME
        path.write_text(
            json.dumps({"version": 5, "llm_api_key": "sk-shared-new-key", "llm_base_url": None}),
            encoding="utf-8",
        )

        twitter_model = _make_fake_model()
        reddit_model = _make_fake_model()

        _, t_version = script._poll_and_apply_credentials_reload(
            str(tmp_path), twitter_model, None, 0, platform="twitter"
        )
        _, r_version = script._poll_and_apply_credentials_reload(
            str(tmp_path), reddit_model, None, 0, platform="reddit"
        )

        assert t_version == 5
        assert r_version == 5
        assert twitter_model._api_key == "sk-shared-new-key"
        assert reddit_model._api_key == "sk-shared-new-key"
        assert path.exists()  # never consumed/deleted


# ==========================================================================
# camel-ai contract canary
# ==========================================================================


def test_camel_openai_model_exposes_the_attributes_reload_depends_on(monkeypatch):
    """The mid-run reload rebuilds camel-ai's `OpenAIModel` by reaching
    into its private `_api_key`/`_url`/`_client`/`_async_client` (and
    reuses `_timeout`/`_max_retries`) -- none of that is public camel-ai
    API and a future camel-ai release could rename or remove it. If it
    ever does, this must fail loudly HERE, at test time, rather than
    silently mid-simulation where `_apply_credentials_reload`'s hasattr
    guard would just log a warning and keep serving the old (possibly
    exhausted) credentials for the rest of the run.
    """
    monkeypatch.setenv("OPENAI_API_KEY", "sk-canary-test-key")
    from camel.models import ModelFactory
    from camel.types import ModelPlatformType

    model = ModelFactory.create(model_platform=ModelPlatformType.OPENAI, model_type="gpt-4o-mini")

    for attr in ("_api_key", "_url", "_client", "_async_client", "_timeout", "_max_retries"):
        assert hasattr(model, attr), (
            f"camel-ai's OpenAIModel no longer exposes `{attr}`. The mid-run LLM "
            f"credential reload in run_twitter_simulation.py/run_reddit_simulation.py/"
            f"run_parallel_simulation.py's _apply_credentials_reload depends on this "
            f"exact attribute and must be updated to match camel-ai's new internals."
        )


def test_apply_credentials_reload_against_a_real_camel_openai_model(monkeypatch):
    """End-to-end against the real camel-ai class (no fakes on that side):
    proves `_apply_credentials_reload` actually swaps the baked-in
    OpenAI/AsyncOpenAI client objects for freshly constructed ones with
    the new key/url, not just bookkeeping attributes that nothing reads."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-old-exhausted-key")
    from camel.models import ModelFactory
    from camel.types import ModelPlatformType

    model = ModelFactory.create(model_platform=ModelPlatformType.OPENAI, model_type="gpt-4o-mini")
    old_client, old_async_client = model._client, model._async_client
    assert model._api_key == "sk-old-exhausted-key"

    applied = twitter_script._apply_credentials_reload(
        model,
        {
            "version": 2,
            "llm_api_key": "sk-new-topped-up-key",
            "llm_base_url": "https://proxy.example.com/v1",
        },
        platform="twitter",
    )

    assert applied is True
    assert model._api_key == "sk-new-topped-up-key"
    assert model._url == "https://proxy.example.com/v1"
    assert model._client is not old_client
    assert model._async_client is not old_async_client
    assert model._client.api_key == "sk-new-topped-up-key"
    assert str(model._client.base_url).rstrip("/") == "https://proxy.example.com/v1"
    assert model._async_client.api_key == "sk-new-topped-up-key"
