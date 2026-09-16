"""Crash-durability + idempotent-replay tests for the graph-ingestion
pipeline: `ZepGraphMemoryUpdater`'s write-ahead journaling in
`_send_batch_activities`, and `ZepGraphMemoryManager.resume_ingestion`'s
reconciliation + tail-drain.

Unlike `test_graphiti_graph_memory_updater.py` (which exercises the
in-memory batching/networking behavior with synthetic AgentActivity
objects), these tests write *real* actions.jsonl files under a temp
directory (matching `action_logger.PlatformActionLogger`'s exact on-disk
shape) and drive activities through `add_activity_from_dict`, so the
byte-range journaling actually engages.
"""

import hashlib
import json
import os
import re
import socket
from types import SimpleNamespace

import pytest

from app.config import Config
from app.services import zep_graph_memory_updater as updater_module
from app.services.graph_ingestion_journal import (
    GraphIngestionJournal,
    actions_log_path,
    load_cursor,
)
from app.services.zep_graph_memory_updater import (
    ZepGraphMemoryManager,
    ZepGraphMemoryUpdater,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _action_entry(
    round_num, agent_id, agent_name, action_type, action_args=None,
    success=True, timestamp="2026-07-22T12:00:00+08:00",
):
    """Matches action_logger.PlatformActionLogger.log_action's exact entry
    shape (round, timestamp, agent_id, agent_name, action_type,
    action_args, result, success)."""

    return {
        "round": round_num,
        "timestamp": timestamp,
        "agent_id": agent_id,
        "agent_name": agent_name,
        "action_type": action_type,
        "action_args": action_args or {},
        "result": None,
        "success": success,
    }


def _write_action_entries(sim_dir, platform, entries):
    path = actions_log_path(sim_dir, platform)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return path


def _episode_result(uuid="episode-uuid"):
    return SimpleNamespace(episode=SimpleNamespace(uuid=uuid))


def _updater(monkeypatch, tmp_path, add_episode, *, simulation_id="sim-journal", graph_id="graph-1"):
    monkeypatch.setattr(Config, "OASIS_SIMULATION_DATA_DIR", str(tmp_path))
    client = SimpleNamespace(add_episode=add_episode)
    monkeypatch.setattr(updater_module, "get_zep_client", lambda: client)
    updater = ZepGraphMemoryUpdater(graph_id, simulation_id=simulation_id)
    updater.SEND_INTERVAL = 0
    # Accept activities without spawning the background worker thread --
    # these tests drain the queue and call _send_batch_activities
    # themselves, deterministically, instead of racing a live worker.
    updater._running = True
    return updater


def _feed(updater, platform, entries):
    """Write *entries* to the real actions.jsonl and drive them through
    add_activity_from_dict (the live-tailer entry point that engages byte
    range scanning), then drain the resulting AgentActivity objects
    straight off the queue -- without starting the worker thread -- so the
    caller can pass them to `_send_batch_activities` deterministically."""

    _write_action_entries(updater.sim_dir, platform, entries)
    for entry in entries:
        updater.add_activity_from_dict(entry, platform)
    activities = []
    while not updater._activity_queue.empty():
        activities.append(updater._activity_queue.get_nowait())
    return activities


def _patch_fresh_client(monkeypatch, fresh_client, episodes):
    monkeypatch.setattr(
        updater_module,
        "_cached_graphiti_client",
        SimpleNamespace(__wrapped__=lambda *a, **k: fresh_client),
    )
    monkeypatch.setattr(
        updater_module, "fetch_all_episodes", lambda client, graph_id: episodes
    )
    closed = []
    monkeypatch.setattr(updater_module, "close_client", lambda c: closed.append(c) or True)
    return closed


# ---------------------------------------------------------------------------
# Deterministic episode naming
# ---------------------------------------------------------------------------

def test_episode_name_is_deterministic_seq_based_not_timestamp(monkeypatch, tmp_path):
    writes = []

    async def add_episode(**kwargs):
        writes.append(kwargs)
        return _episode_result()

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-name")
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    assert len(writes) == 1
    name = writes[0]["name"]
    assert name == "sim-name_twitter_seq000001_r1-1"
    # No millisecond-epoch suffix anywhere -- that was the old, non-replay-
    # detectable scheme this replaces.
    assert not re.search(r"_\d{10,}(?:_|$)", name)


def test_episode_name_seq_increments_across_batches_and_persists(monkeypatch, tmp_path):
    writes = []

    async def add_episode(**kwargs):
        writes.append(kwargs)
        return _episode_result()

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-seq")
    for batch_index in range(2):
        entries = [
            _action_entry(
                batch_index + 1, i, f"Agent{i}", "CREATE_POST", {"content": f"b{batch_index}-{i}"}
            )
            for i in range(5)
        ]
        activities = _feed(updater, "twitter", entries)
        updater._send_batch_activities(activities, "twitter")

    assert [w["name"] for w in writes] == [
        "sim-seq_twitter_seq000001_r1-1",
        "sim-seq_twitter_seq000002_r2-2",
    ]
    cursor = load_cursor(updater.sim_dir, "twitter")
    assert cursor["next_seq"] == 3


# ---------------------------------------------------------------------------
# Write-ahead ordering
# ---------------------------------------------------------------------------

def test_intent_and_cursor_are_durable_before_add_episode_runs(monkeypatch, tmp_path):
    observed = {}
    updater = None

    async def add_episode(**kwargs):
        # By the time add_episode is invoked, steps 1-2 (INTENT fsync'd +
        # cursor advanced) must already be visible on disk -- this is the
        # entire write-ahead guarantee.
        journal = GraphIngestionJournal(updater.sim_dir)
        observed["pending_seqs"] = [p["seq"] for p in journal.pending_intents()]
        observed["cursor"] = load_cursor(updater.sim_dir, "twitter")
        return _episode_result()

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-order")
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    assert observed["pending_seqs"] == [1]
    assert observed["cursor"] == {
        "batched_offset": activities[-1].journal_end,
        "next_seq": 2,
    }

    # After a successful send, the dangling INTENT is resolved by COMMITTED.
    journal = GraphIngestionJournal(updater.sim_dir)
    assert journal.pending_intents() == []


def test_intent_records_the_sha256_of_the_exact_episode_text(monkeypatch, tmp_path):
    async def add_episode(**kwargs):
        return _episode_result()

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-sha")
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    combined_text = "\n".join(a.to_episode_text() for a in activities)
    expected_digest = hashlib.sha256(combined_text.encode("utf-8")).hexdigest()

    journal = GraphIngestionJournal(updater.sim_dir)
    records = journal.read_records()
    intent = next(r for r in records if r["type"] == "INTENT")
    assert intent["sha256"] == expected_digest


# ---------------------------------------------------------------------------
# Fail-closed behavior: a dangling INTENT is the durable record of ambiguity
# ---------------------------------------------------------------------------

def test_failed_send_leaves_a_dangling_intent_with_no_committed(monkeypatch, tmp_path):
    async def add_episode(**kwargs):
        raise RuntimeError("LLM provider out of credits")

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-fail")
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    assert len(updater._failed_batches) == 1

    journal = GraphIngestionJournal(updater.sim_dir)
    pending = journal.pending_intents()
    assert len(pending) == 1
    assert pending[0]["seq"] == 1

    # The cursor was still advanced (step 2 happens before the attempt in
    # step 3 that can fail) -- a retry must never re-scan these bytes as if
    # nothing had happened to them.
    cursor = load_cursor(updater.sim_dir, "twitter")
    assert cursor["batched_offset"] == activities[-1].journal_end
    assert cursor["next_seq"] == 2


def test_activities_with_no_real_backing_file_are_sent_without_journaling(monkeypatch, tmp_path):
    """Synthetic activities (not sourced from add_activity_from_dict / a
    real actions.jsonl -- e.g. constructed directly, as in
    test_graphiti_graph_memory_updater.py) must keep working exactly as
    before: sent normally, just with no INTENT/COMMITTED trail."""

    from app.services.zep_graph_memory_updater import AgentActivity

    writes = []

    async def add_episode(**kwargs):
        writes.append(kwargs)
        return _episode_result()

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-synthetic")
    activity = AgentActivity(
        platform="twitter", agent_id=1, agent_name="Agent",
        action_type="CREATE_POST", action_args={"content": "hi"},
        round_num=1, timestamp="2026-07-22T12:00:00+08:00",
    )
    assert activity.journal_start is None and activity.journal_end is None

    updater._send_batch_activities([activity], "twitter")

    assert len(writes) == 1
    journal = GraphIngestionJournal(updater.sim_dir)
    assert journal.read_records() == []


# ---------------------------------------------------------------------------
# MAX_EPISODE_CHARS split: one flush -> multiple episodes -> multiple seqs
# ---------------------------------------------------------------------------

def test_split_payload_produces_two_intents_with_ordered_non_overlapping_ranges(monkeypatch, tmp_path):
    async def add_episode(**kwargs):
        raise RuntimeError("boom")

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-split", graph_id="graph-split")
    small = _action_entry(1, 1, "Agent1", "CREATE_POST", {"content": "short post"})
    big = _action_entry(1, 2, "Agent2", "CREATE_POST", {"content": "x" * 20_000})
    activities = _feed(updater, "twitter", [small, big])
    assert len(activities) == 2

    processed = updater._send_batch_activities(activities, "twitter")
    assert processed == 2
    assert len(updater._failed_batches) == 2  # both payloads failed independently

    journal = GraphIngestionJournal(updater.sim_dir)
    pending = journal.pending_intents()
    assert [p["seq"] for p in pending] == [1, 2]
    assert pending[0]["start"] == activities[0].journal_start
    assert pending[0]["end"] == activities[0].journal_end
    assert pending[1]["start"] == activities[1].journal_start
    assert pending[1]["end"] == activities[1].journal_end
    assert pending[0]["end"] <= pending[1]["start"]
    assert "seq000001" in pending[0]["episode_name"]
    assert "seq000002" in pending[1]["episode_name"]


def test_split_payload_intents_reconcile_independently(monkeypatch, tmp_path):
    async def add_episode(**kwargs):
        raise RuntimeError("boom")

    updater = _updater(monkeypatch, tmp_path, add_episode, simulation_id="sim-split-2", graph_id="graph-split-2")
    small = _action_entry(1, 1, "Agent1", "CREATE_POST", {"content": "short post"})
    big = _action_entry(1, 2, "Agent2", "CREATE_POST", {"content": "x" * 20_000})
    activities = _feed(updater, "twitter", [small, big])
    updater._send_batch_activities(activities, "twitter")

    journal = GraphIngestionJournal(updater.sim_dir)
    pending = journal.pending_intents()
    assert len(pending) == 2

    # Seq 1's episode really did land; seq 2's never did.
    fake_episode = SimpleNamespace(name=pending[0]["episode_name"], uuid="landed-1")
    add_episode_calls = []

    async def fresh_add_episode(**kwargs):
        add_episode_calls.append(kwargs)
        return _episode_result("resent-2")

    fresh_client = SimpleNamespace(add_episode=fresh_add_episode)
    _patch_fresh_client(monkeypatch, fresh_client, [fake_episode])

    result = ZepGraphMemoryManager.resume_ingestion("sim-split-2", "graph-split-2")

    assert result["pending_before"] == 2
    assert result["already_committed"] == 1
    assert result["resent"] == 1
    assert result["still_failed"] == 0
    assert len(add_episode_calls) == 1
    assert add_episode_calls[0]["name"] == pending[1]["episode_name"]
    # The big activity's text must be rebuilt with the exact same
    # truncation rule as the original send.
    assert len(add_episode_calls[0]["episode_body"]) <= ZepGraphMemoryUpdater.MAX_EPISODE_CHARS
    assert add_episode_calls[0]["episode_body"].endswith("[truncated by MiroFish]")

    assert journal.pending_intents() == []


# ---------------------------------------------------------------------------
# resume_ingestion: reconciliation of dangling INTENTs
# ---------------------------------------------------------------------------

def test_resume_ingestion_synthesizes_committed_when_episode_already_landed(monkeypatch, tmp_path):
    async def crashing_add_episode(**kwargs):
        raise RuntimeError("boom")

    updater = _updater(
        monkeypatch, tmp_path, crashing_add_episode,
        simulation_id="sim-resume-1", graph_id="graph-1",
    )
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    journal = GraphIngestionJournal(updater.sim_dir)
    pending_before = journal.pending_intents()
    assert len(pending_before) == 1
    episode_name = pending_before[0]["episode_name"]

    fake_episode = SimpleNamespace(name=episode_name, uuid="uuid-that-really-landed")
    add_episode_calls = []

    async def fresh_add_episode(**kwargs):
        add_episode_calls.append(kwargs)
        return _episode_result("should-not-be-used")

    fresh_client = SimpleNamespace(add_episode=fresh_add_episode)
    closed = _patch_fresh_client(monkeypatch, fresh_client, [fake_episode])

    result = ZepGraphMemoryManager.resume_ingestion("sim-resume-1", "graph-1")

    assert result["pending_before"] == 1
    assert result["already_committed"] == 1
    assert result["resent"] == 0
    assert result["still_failed"] == 0
    assert add_episode_calls == []  # must NOT resend a batch that really landed

    assert journal.pending_intents() == []
    committed = [r for r in journal.read_records() if r["type"] == "COMMITTED"]
    assert committed[0]["episode_uuid"] == "uuid-that-really-landed"
    assert closed == [fresh_client]


def test_resume_ingestion_resends_when_episode_never_landed(monkeypatch, tmp_path):
    async def crashing_add_episode(**kwargs):
        raise RuntimeError("boom")

    updater = _updater(
        monkeypatch, tmp_path, crashing_add_episode,
        simulation_id="sim-resume-2", graph_id="graph-2",
    )
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    journal = GraphIngestionJournal(updater.sim_dir)
    pending_before = journal.pending_intents()
    episode_name = pending_before[0]["episode_name"]
    expected_text = "\n".join(a.to_episode_text() for a in activities)

    add_episode_calls = []

    async def fresh_add_episode(**kwargs):
        add_episode_calls.append(kwargs)
        return _episode_result("resent-uuid")

    fresh_client = SimpleNamespace(add_episode=fresh_add_episode)
    _patch_fresh_client(monkeypatch, fresh_client, [])  # nothing landed in Neo4j

    result = ZepGraphMemoryManager.resume_ingestion("sim-resume-2", "graph-2")

    assert result["already_committed"] == 0
    assert result["resent"] == 1
    assert result["still_failed"] == 0
    assert len(add_episode_calls) == 1
    assert add_episode_calls[0]["name"] == episode_name
    assert add_episode_calls[0]["episode_body"] == expected_text

    committed = [r for r in journal.read_records() if r["type"] == "COMMITTED"]
    assert len(committed) == 1
    assert committed[0]["episode_uuid"] == "resent-uuid"
    assert journal.pending_intents() == []


def test_resume_ingestion_refuses_to_resend_on_sha256_mismatch(monkeypatch, tmp_path):
    async def crashing_add_episode(**kwargs):
        raise RuntimeError("boom")

    updater = _updater(
        monkeypatch, tmp_path, crashing_add_episode,
        simulation_id="sim-resume-3", graph_id="graph-3",
    )
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)
    updater._send_batch_activities(activities, "twitter")

    # Simulate actions.jsonl having changed since the crash (e.g. a
    # force-restart truncated/rewrote it) by tampering with the on-disk
    # content in place, preserving byte length so the recorded range still
    # reads back the same number of bytes.
    path = actions_log_path(updater.sim_dir, "twitter")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    tampered = content.replace("post 0", "HACKED")
    assert tampered != content
    with open(path, "w", encoding="utf-8") as f:
        f.write(tampered)

    add_episode_calls = []

    async def fresh_add_episode(**kwargs):
        add_episode_calls.append(kwargs)
        return _episode_result("should-not-happen")

    fresh_client = SimpleNamespace(add_episode=fresh_add_episode)
    _patch_fresh_client(monkeypatch, fresh_client, [])  # nothing landed

    result = ZepGraphMemoryManager.resume_ingestion("sim-resume-3", "graph-3")

    assert result["resent"] == 0
    assert result["already_committed"] == 0
    assert result["still_failed"] == 1
    assert add_episode_calls == []  # refused, never sent the tampered text

    journal = GraphIngestionJournal(updater.sim_dir)
    # Still dangling -- resume_ingestion must not fabricate a COMMITTED for
    # a batch it refused to resolve.
    assert len(journal.pending_intents()) == 1


# ---------------------------------------------------------------------------
# resume_ingestion: resume tailing for never-batched activities
# ---------------------------------------------------------------------------

def test_resume_ingestion_drains_activities_logged_but_never_batched(monkeypatch, tmp_path):
    monkeypatch.setattr(Config, "OASIS_SIMULATION_DATA_DIR", str(tmp_path))
    simulation_id = "sim-tail"
    sim_dir = os.path.join(str(tmp_path), simulation_id)
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"post {i}"})
        for i in range(2)  # fewer than BATCH_SIZE=5 -- never batched live
    ]
    _write_action_entries(sim_dir, "twitter", entries)

    add_episode_calls = []

    async def fresh_add_episode(**kwargs):
        add_episode_calls.append(kwargs)
        return _episode_result("tail-uuid")

    fresh_client = SimpleNamespace(add_episode=fresh_add_episode)
    _patch_fresh_client(monkeypatch, fresh_client, [])

    result = ZepGraphMemoryManager.resume_ingestion(simulation_id, "graph-tail")

    assert result["pending_before"] == 0
    assert result["tail_items_sent"] == 2
    assert len(add_episode_calls) == 1
    assert add_episode_calls[0]["name"] == f"{simulation_id}_twitter_seq000001_r1-1"

    journal = GraphIngestionJournal(sim_dir)
    committed = [r for r in journal.read_records() if r["type"] == "COMMITTED"]
    assert len(committed) == 1

    cursor = load_cursor(sim_dir, "twitter")
    assert cursor["next_seq"] == 2


def test_resume_ingestion_on_a_simulation_with_no_ingestion_activity_is_a_noop(monkeypatch, tmp_path):
    monkeypatch.setattr(Config, "OASIS_SIMULATION_DATA_DIR", str(tmp_path))

    def boom():
        raise AssertionError("resume_ingestion must not use the process-wide cached client")

    monkeypatch.setattr(updater_module, "get_zep_client", boom)

    fresh_client = SimpleNamespace(add_episode=lambda **kw: None)
    closed = _patch_fresh_client(monkeypatch, fresh_client, [])

    result = ZepGraphMemoryManager.resume_ingestion("sim-empty", "graph-empty")

    assert result == {
        "pending_before": 0,
        "resent": 0,
        "already_committed": 0,
        "still_failed": 0,
        "tail_items_sent": 0,
    }
    assert closed == [fresh_client]


# ---------------------------------------------------------------------------
# Real Neo4j: verify the genuine failure path (zero-credit LLM provider)
# ---------------------------------------------------------------------------

def _neo4j_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 7687), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _neo4j_reachable(), reason="local Neo4j (bolt://localhost:7687) is not running")
def test_real_neo4j_add_episode_failure_leaves_a_clean_dangling_intent(monkeypatch, tmp_path):
    """End-to-end against the actual local Neo4j container: no mocking of
    add_episode at all. The configured LLM provider is documented to
    currently have zero credits, so entity extraction is expected to fail
    with a real error -- this proves the dangling-INTENT / fail-closed
    behavior holds against the real client, not just a mock standing in for
    it. If credits have since been restored (add_episode actually
    succeeds), the alternate branch below still asserts the complementary
    invariant: a clean commit with no dangling INTENT.
    """

    monkeypatch.setattr(Config, "OASIS_SIMULATION_DATA_DIR", str(tmp_path))
    simulation_id = "sim-real-neo4j-journal-test"
    graph_id = "graph-real-neo4j-journal-test"

    updater = ZepGraphMemoryUpdater(graph_id, simulation_id=simulation_id)
    updater._running = True
    entries = [
        _action_entry(1, i, f"Agent{i}", "CREATE_POST", {"content": f"integration test post {i}"})
        for i in range(5)
    ]
    activities = _feed(updater, "twitter", entries)

    try:
        updater._send_batch_activities(activities, "twitter")

        journal = GraphIngestionJournal(updater.sim_dir)
        pending = journal.pending_intents()

        if updater._failed_batches:
            # Expected: the zero-credit provider caused a real failure.
            assert len(pending) == 1
            assert len(updater._failed_batches) == 1
            cursor = load_cursor(updater.sim_dir, "twitter")
            assert cursor["batched_offset"] == activities[-1].journal_end
        else:
            # Credits were available after all -- the batch genuinely
            # committed, so there must be no dangling ambiguity left.
            assert pending == []
    finally:
        try:
            ZepGraphMemoryManager.clear_simulation_episodes(graph_id, simulation_id)
        except Exception:
            pass
