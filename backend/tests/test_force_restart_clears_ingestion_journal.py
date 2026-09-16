"""A force-restart must wipe the graph-ingestion journal along with actions.jsonl.

This is a regression guard for a silent-data-loss bug, not a style preference.
`cleanup_simulation_logs` truncates `<sim>/<platform>/actions.jsonl` back to
zero bytes when a run is force-restarted. The ingestion journal's cursor stores
a byte offset *into that same file* plus the next episode sequence number. If
the cursor outlives the file it points into, the restarted run's very first
batch is compared against a `batched_offset` from the abandoned run, every new
batch is treated as "already ingested", and the graph silently stops receiving
the new run's activity -- with no error anywhere.
"""

import json
import os

from app.services.simulation_runner import SimulationRunner


def _make_sim_dir(tmp_path, simulation_id):
    sim_dir = tmp_path / simulation_id
    (sim_dir / "twitter").mkdir(parents=True)
    (sim_dir / "reddit").mkdir(parents=True)
    (sim_dir / "graph_ingestion").mkdir(parents=True)

    (sim_dir / "twitter" / "actions.jsonl").write_text(
        json.dumps({"round": 1, "agent_id": 1, "action_type": "CREATE_POST"}) + "\n",
        encoding="utf-8",
    )
    (sim_dir / "run_state.json").write_text("{}", encoding="utf-8")
    # A cursor deep into the (about to be deleted) actions.jsonl.
    (sim_dir / "graph_ingestion" / "cursor_twitter.json").write_text(
        json.dumps({"batched_offset": 99999, "next_seq": 42}), encoding="utf-8"
    )
    (sim_dir / "graph_ingestion" / "journal.jsonl").write_text(
        json.dumps({"type": "INTENT", "seq": 41, "platform": "twitter"}) + "\n",
        encoding="utf-8",
    )
    return sim_dir


def test_cleanup_removes_graph_ingestion_directory(tmp_path, monkeypatch):
    simulation_id = "sim_cleanup_journal"
    sim_dir = _make_sim_dir(tmp_path, simulation_id)
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))

    result = SimulationRunner.cleanup_simulation_logs(simulation_id)

    assert result.get("success") is not False, result
    assert not (sim_dir / "graph_ingestion").exists(), (
        "graph_ingestion/ survived a force-restart; a stale cursor would make "
        "the restarted run's batches be skipped as already-ingested"
    )


def test_cursor_never_outlives_the_actions_log_it_indexes(tmp_path, monkeypatch):
    """The specific invariant: no cursor may reference a deleted actions.jsonl."""
    simulation_id = "sim_cursor_invariant"
    sim_dir = _make_sim_dir(tmp_path, simulation_id)
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))

    SimulationRunner.cleanup_simulation_logs(simulation_id)

    actions_log = sim_dir / "twitter" / "actions.jsonl"
    cursor = sim_dir / "graph_ingestion" / "cursor_twitter.json"
    assert not actions_log.exists(), "precondition: cleanup should remove actions.jsonl"
    assert not cursor.exists(), (
        "a cursor pointing at byte 99999 of a file that no longer exists is the "
        "silent-skip failure mode this guards"
    )
