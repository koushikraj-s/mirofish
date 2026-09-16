"""Runtime-configurable, hot-swappable LLM/Neo4j credentials.

Background: `Config`'s seven `LLM_*`/`NEO4J_*` attributes are plain class
attributes evaluated once, from `.env`, when `app.config` is first imported
(see `app/config.py`). If the configured LLM key runs out of credits mid
simulation, there was previously no way to swap in a working key without
restarting the whole backend -- which discards any queued work.

This module is the single place that:
  * persists a partial-override patch on top of `.env`/class defaults
    (`backend/uploads/config/credentials.json`, versioned and gitignored),
  * applies it onto the live `Config` class via `setattr` (every consumer
    that matters already re-reads `Config` fresh per call -- see
    `LLMClient.__init__` and `openai_chat_compat`'s per-call
    `reasoning_effort` read), and
  * propagates a change to the one place that *is* cached across calls:
    the process-wide Graphiti client in `app.utils.zep`.

Layering (lowest to highest priority): class literal default < `.env` /
process environment < this store's persisted override. A partial override
file only overrides the keys it contains; every absent key falls through
to whatever `.env`/the class default already produced.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ..config import Config
from ..utils.atomic_io import atomic_write_json, read_json_tolerant
from ..utils.logger import get_logger

logger = get_logger("mirofish.settings_store")

SCHEMA_VERSION = 1

# The only keys this store is allowed to read, persist, or setattr onto
# Config. SECRET_KEY is deliberately excluded (Flask reads it once for
# session/cookie signing; rotating it live only invalidates existing
# signatures, it does not "hot swap" anything) -- see api/settings.py.
EDITABLE_KEYS: Tuple[str, ...] = (
    "LLM_API_KEY",
    "LLM_BASE_URL",
    "LLM_MODEL_NAME",
    "LLM_REASONING_EFFORT",
    "NEO4J_URI",
    "NEO4J_USER",
    "NEO4J_PASSWORD",
)

# Values that must never be echoed back raw in an API response.
SECRET_KEYS = frozenset({"LLM_API_KEY", "NEO4J_PASSWORD"})

# Reentrant because `apply_and_propagate` (the write entry point) holds this
# lock for its whole ordered sequence and calls `save_overrides` internally;
# a plain `threading.Lock` would deadlock on that self-call. A plain Lock
# would also work if save_overrides never locked on its own, but
# save_overrides must also be independently safe to call directly (as the
# tests do), so it takes the lock itself -- hence RLock.
_lock = threading.RLock()


def _store_path() -> str:
    return os.path.join(Config.UPLOAD_FOLDER, "config", "credentials.json")


def _store_dir() -> str:
    return os.path.dirname(_store_path())


# --- Original (pre-override) state, snapshotted once at import time -------
#
# This module is imported for the first time from `app/config.py`'s bottom
# deferred import (see `_apply_persisted_overrides` there), *before*
# `apply_to_config()` has ever run. At that exact moment, `Config`'s seven
# attributes hold exactly what `.env`/the process environment (or the class
# body's hardcoded literal) produced -- no override has been layered on top
# yet. Snapshotting it here, once, is what lets `describe_sources()` later
# report "env" vs "default" correctly even after `apply_and_propagate` has
# mirrored an override into `os.environ` for subprocess inheritance (which
# would otherwise make every overridden key look like "env" if source were
# computed by re-reading `os.environ` live).
_ORIGINAL_ENV_PRESENT: Dict[str, bool] = {
    key: bool(os.environ.get(key)) for key in EDITABLE_KEYS
}

# The actual pre-override values themselves (not just whether they came
# from the environment), snapshotted at the same safe moment as
# `_ORIGINAL_ENV_PRESENT` above. `apply_to_config()` needs these to
# correctly *revert* a key when its override is cleared -- `setattr`
# already happened once for the override, so simply skipping a
# no-longer-overridden key would leave `Config` stuck on the stale
# override value forever instead of falling back to `.env`/the class
# default (confirmed with a real clear-after-set round trip; skipping
# absent keys is NOT sufficient).
_ORIGINAL_DEFAULTS: Dict[str, Any] = {
    key: getattr(Config, key, None) for key in EDITABLE_KEYS
}


def load_overrides() -> Dict[str, Any]:
    """Return the persisted override patch: only EDITABLE_KEYS with a
    saved, non-empty value. Missing/corrupt/partial files degrade to "no
    overrides" rather than raising (see `read_json_tolerant`)."""

    document = read_json_tolerant(_store_path(), default=None)
    if not isinstance(document, dict):
        return {}
    return {
        key: value
        for key, value in document.items()
        if key in EDITABLE_KEYS and isinstance(value, str) and value != ""
    }


def save_overrides(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge *patch* into the persisted overrides and atomically write them.

    A key mapped to None or an empty/whitespace-only string *clears* that
    override (falls back to `.env`/default) instead of persisting an empty
    credential. Keys outside EDITABLE_KEYS are silently ignored -- callers
    that need to warn about that (the API layer) filter before calling
    this.

    Returns the full merged override dict (not just the patch) so callers
    can see the final state without a second read.
    """

    with _lock:
        merged = load_overrides()
        for key, value in patch.items():
            if key not in EDITABLE_KEYS:
                continue
            if value is None or (isinstance(value, str) and value.strip() == ""):
                merged.pop(key, None)
            elif isinstance(value, str):
                merged[key] = value

        directory = _store_dir()
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o700)

        document = {
            "version": SCHEMA_VERSION,
            "updated_at": time.time(),
            **merged,
        }
        atomic_write_json(_store_path(), document, mode=0o600)
        return merged


def apply_to_config() -> None:
    """setattr every one of the seven editable keys onto `Config`: the
    persisted override if one exists, otherwise the original `.env`/class
    default it had before any override was ever applied.

    Called once at import time (via `config.py`'s deferred bottom-of-file
    call, *before* `run.py`'s `Config.validate()` -- see that module) and
    again after every successful `apply_and_propagate`. Deliberately
    idempotent and unconditional for every key (not just the ones in the
    current patch): `Config`'s attributes are mutable class state, so a
    key that previously had an override which was since *cleared* must be
    actively reset back to `_ORIGINAL_DEFAULTS`, not merely left alone --
    leaving it alone would strand `Config` on the stale override value
    forever.
    """

    overrides = load_overrides()
    for key in EDITABLE_KEYS:
        setattr(Config, key, overrides.get(key, _ORIGINAL_DEFAULTS.get(key)))


def describe_sources() -> Dict[str, str]:
    """Per-key layering: "override" | "env" | "default"."""

    overrides = load_overrides()
    sources: Dict[str, str] = {}
    for key in EDITABLE_KEYS:
        if key in overrides:
            sources[key] = "override"
        elif _ORIGINAL_ENV_PRESENT.get(key):
            sources[key] = "env"
        else:
            sources[key] = "default"
    return sources


def _mask(value: Any) -> Optional[str]:
    """`sk-...ab12`-style mask: never enough of the value to reconstruct
    it, always something to visually confirm which key is configured."""

    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    if len(text) <= 4:
        return "*" * len(text)
    return f"{text[:2]}...{text[-4:]}"


def describe_settings() -> Dict[str, Dict[str, Any]]:
    """A safe-to-return snapshot of all seven keys: current effective
    value (masked for LLM_API_KEY/NEO4J_PASSWORD, verbatim for the rest --
    a base URL, model name, reasoning effort, URI, or username is not a
    secret and the UI needs the real value to let the user edit it), a
    `set` boolean, and the override/env/default `source`.

    Never includes a raw secret value under any key.
    """

    sources = describe_sources()
    result: Dict[str, Dict[str, Any]] = {}
    for key in EDITABLE_KEYS:
        value = getattr(Config, key, None)
        is_set = bool(value)
        result[key] = {
            "set": is_set,
            "source": sources[key],
            "value": (_mask(value) if key in SECRET_KEYS else value) if is_set else None,
        }
    return result


# LLM-credential keys whose change actually matters to an already-running
# simulation subprocess. NEO4J_* is irrelevant to it (Neo4j/Graphiti only
# ever runs inside this Flask process -- the `run_*_simulation.py` scripts
# never import `zep`/graphiti_core at all) and LLM_REASONING_EFFORT is only
# read per-call by `openai_chat_compat`, which those scripts never use --
# they construct a camel-ai `OpenAIModel` directly via `ModelFactory.create`.
_LLM_RELOAD_TRIGGER_KEYS = frozenset({"LLM_API_KEY", "LLM_BASE_URL"})


def _broadcast_credentials_reload(filtered_patch: Dict[str, Any], warnings: List[str]) -> None:
    """Step 6 of `apply_and_propagate`, appended after its load-bearing
    5-step sequence: if the LLM credentials a running simulation subprocess
    baked into its camel-ai model at startup just changed, bump
    `<sim_dir>/credentials_reload.json` for every simulation that currently
    has a live subprocess (`SimulationRunner.get_running_simulations()`), so
    its round loop can rebuild the model's OpenAI client in place on its
    next round instead of continuing to fail against the exhausted/old key
    for the rest of the run.

    Best-effort by design and never load-bearing for the ordering above: a
    failure here must never undo or fail the credential swap that already
    committed in steps 1-5. Every failure is logged and folded into
    `warnings` instead of raised.
    """

    if not (_LLM_RELOAD_TRIGGER_KEYS & filtered_patch.keys()):
        return

    try:
        # Deferred for the same reason as the `zep` import in
        # `apply_and_propagate` below: `simulation_runner.py` itself
        # imports `..utils.zep` (graphiti_core/sentence_transformers,
        # heavy), and this module is imported from `config.py`'s own
        # bootstrap -- keep that path free of it until a real request
        # actually needs it.
        from .simulation_runner import SimulationRunner
        from . import simulation_ipc
    except Exception as e:  # pragma: no cover - import machinery failure
        logger.warning("Could not load simulation runner for credential reload broadcast: %s", e)
        warnings.append("Failed to broadcast credential reload to running simulations; see server logs.")
        return

    try:
        running_ids = SimulationRunner.get_running_simulations()
    except Exception as e:
        logger.warning("Could not enumerate running simulations for credential reload broadcast: %s", e)
        warnings.append("Failed to broadcast credential reload to running simulations; see server logs.")
        return

    if not running_ids:
        logger.info("LLM credential change applied; no live simulation subprocess to broadcast to.")
        return

    broadcasted: List[str] = []
    for simulation_id in running_ids:
        sim_dir = os.path.join(SimulationRunner.RUN_STATE_DIR, simulation_id)
        try:
            version = simulation_ipc.write_credentials_reload_broadcast(
                sim_dir,
                llm_api_key=Config.LLM_API_KEY,
                llm_base_url=Config.LLM_BASE_URL,
                llm_model_name=Config.LLM_MODEL_NAME,
            )
            broadcasted.append(f"{simulation_id}(v{version})")
        except Exception as e:
            logger.warning(
                "Failed to broadcast credential reload to simulation %s: %s", simulation_id, e
            )
            warnings.append(
                f"Failed to broadcast credential reload to simulation {simulation_id}; see server logs."
            )

    if broadcasted:
        logger.info(
            "Broadcast LLM credential reload to running simulation(s): %s",
            ", ".join(broadcasted),
        )


def apply_and_propagate(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Persist *patch* and make it take effect for the running process.

    Steps run in exactly this order -- the ordering is load-bearing:

      1. Persist the merged overrides atomically. If the process dies
         before finishing propagation, the new credentials are still
         recoverable on the next boot instead of being lost.
      2. Snapshot whichever Graphiti client is *currently* cached, using
         the CURRENT (pre-mutation) `Config.NEO4J_*` values as the lookup
         key. This must happen before step 3: the client cache's key is
         `(uri, user, password)`, so looking it up *after* Config is
         mutated would query the NEW key, miss the cache (a maxsize=1 LRU
         has no entry under the new key yet), and silently strand the real
         old client cached forever under its original key -- leaking its
         Neo4j driver connection pool for the rest of the process
         lifetime. See `zep.snapshot_current_client` for the cache-hit
         guarantee this relies on.
      3. `setattr` the new values onto `Config`. Every fresh-per-call
         consumer (`LLMClient.__init__`, the per-call
         `Config.LLM_REASONING_EFFORT` read in `openai_chat_compat`) picks
         this up on its very next call with no code change needed.
      4. Mirror the changed keys into `os.environ`. `simulation_runner.py`
         spawns subprocesses via `os.environ.copy()`; only an `os.environ`
         write (not a `Config` attribute write) is visible to a process
         spawned *after* this point.
      5. Evict the Graphiti client cache and close the OLD client
         snapshotted in step 2 (not a fresh lookup -- that would now find
         nothing, since eviction already happened, or worse, a
         freshly-constructed new client under the new key). This must run
         even for an LLM-only change with unchanged Neo4j credentials: the
         cached Graphiti client's LLM client was baked in at construction
         time from `Config.LLM_*` (`zep.py`'s `_cached_graphiti_client`
         body), and the cache key is Neo4j-credentials-only, so an
         LLM-only change would otherwise never evict by key and the graph
         memory pipeline would keep silently using the stale/exhausted LLM
         key forever. Close failures are logged and turned into a
         `warnings` entry, never raised -- a credential swap must succeed
         even if the abandoned client's teardown has trouble.
      6. Broadcast the new LLM credentials to every simulation subprocess
         that is currently running (see `_broadcast_credentials_reload`).
         This step is additive and never load-bearing for steps 1-5 above:
         it runs last, after the swap has already fully committed, and any
         failure here only ever adds a `warnings` entry -- it can never
         undo or fail the credential swap itself.

    Returns `{"settings": describe_settings(), "warnings": [...]}`.
    """

    # Deferred rather than a module-level import: `zep.py` eagerly imports
    # graphiti_core/sentence_transformers (heavy). This module is imported
    # from `config.py`'s own bootstrap (see `_ORIGINAL_ENV_PRESENT` comment
    # above); `apply_to_config()` -- the only thing that bootstrap path
    # calls -- never needs `zep`, only `apply_and_propagate` (a real
    # request) does. Keeping the import here instead of at module scope
    # avoids pulling the ML stack into `app.config`'s own import purely to
    # satisfy a code path that boot never exercises.
    from ..utils import zep

    warnings: List[str] = []
    filtered_patch: Dict[str, Any] = {}
    for key, value in patch.items():
        if key not in EDITABLE_KEYS:
            warnings.append(f"Ignored unknown or non-editable key: {key!r}")
            continue
        if value is not None and not isinstance(value, str):
            warnings.append(f"Ignored non-string value for {key}")
            continue
        filtered_patch[key] = value

    if not filtered_patch:
        return {"settings": describe_settings(), "warnings": warnings}

    with _lock:
        # Step 1
        merged = save_overrides(filtered_patch)

        # Step 2 -- BEFORE step 3 mutates Config. See docstring above.
        old_client = zep.snapshot_current_client()

        # Step 3
        apply_to_config()

        # Step 4
        for key in filtered_patch:
            value = merged.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

        # Step 5
        zep.clear_zep_client_cache()
        if old_client is not None and not zep.close_client(old_client):
            warnings.append(
                "The previous Graphiti/Neo4j client failed to close cleanly; "
                "see server logs. The new credentials are active regardless."
            )

        # Step 6 -- additive, see docstring above. Deliberately still
        # inside the lock so a burst of rapid successive credential changes
        # broadcasts strictly-increasing versions in the same order they
        # were applied.
        _broadcast_credentials_reload(filtered_patch, warnings)

    logger.info(
        "Applied runtime credential override for: %s",
        ", ".join(sorted(filtered_patch.keys())),
    )
    return {"settings": describe_settings(), "warnings": warnings}
