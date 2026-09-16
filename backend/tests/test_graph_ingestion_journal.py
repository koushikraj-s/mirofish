"""Unit tests for the write-ahead graph-ingestion journal
(`app/services/graph_ingestion_journal.py`).

Covers the pure file-I/O primitives in isolation, before
`zep_graph_memory_updater.py` layers episode-construction/replay logic on
top of them: journal append/read, INTENT<->COMMITTED matching, cursor
persistence + monotonicity, and the actions.jsonl line scanner (including
malformed-line tolerance).
"""

import json
import os
import stat

import pytest

from app.services.graph_ingestion_journal import (
    GraphIngestionJournal,
    actions_log_path,
    cursor_path,
    iter_action_lines,
    is_activity_line,
    journal_path,
    load_cursor,
    save_cursor,
)


# ---------------------------------------------------------------------------
# GraphIngestionJournal: append + read
# ---------------------------------------------------------------------------

def test_append_intent_then_committed_round_trips_through_read_records(tmp_path):
    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(
        seq=1, platform="twitter", episode_name="sim_twitter_seq000001_r1-1",
        start=0, end=120, sha256="abc123",
    )
    journal.append_committed(
        seq=1, platform="twitter", episode_name="sim_twitter_seq000001_r1-1",
        episode_uuid="episode-uuid-1",
    )

    records = journal.read_records()
    assert len(records) == 2
    assert records[0]["type"] == "INTENT"
    assert records[0]["seq"] == 1
    assert records[0]["start"] == 0
    assert records[0]["end"] == 120
    assert records[0]["sha256"] == "abc123"
    assert records[1]["type"] == "COMMITTED"
    assert records[1]["episode_uuid"] == "episode-uuid-1"


def test_append_creates_journal_file_fsync_safe_permissions(tmp_path):
    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(
        seq=1, platform="twitter", episode_name="ep", start=0, end=10, sha256="x",
    )
    path = journal_path(str(tmp_path))
    assert os.path.exists(path)
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600


def test_read_records_on_missing_journal_returns_empty_list(tmp_path):
    journal = GraphIngestionJournal(str(tmp_path))
    assert journal.read_records() == []
    assert journal.pending_intents() == []


def test_read_records_tolerates_a_trailing_malformed_line(tmp_path):
    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(
        seq=1, platform="twitter", episode_name="ep", start=0, end=10, sha256="x",
    )
    with open(journal.path, "a", encoding="utf-8") as f:
        f.write("{not valid json\n")

    records = journal.read_records()
    assert len(records) == 1
    assert records[0]["type"] == "INTENT"


# ---------------------------------------------------------------------------
# pending_intents(): INTENT<->COMMITTED matching, keyed by (platform, seq)
# ---------------------------------------------------------------------------

def test_pending_intents_excludes_committed_ones(tmp_path):
    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(seq=1, platform="twitter", episode_name="a", start=0, end=10, sha256="x")
    journal.append_intent(seq=2, platform="twitter", episode_name="b", start=10, end=20, sha256="y")
    journal.append_committed(seq=1, platform="twitter", episode_name="a", episode_uuid="u1")

    pending = journal.pending_intents()
    assert [p["seq"] for p in pending] == [2]
    assert pending[0]["episode_name"] == "b"


def test_pending_intents_are_returned_in_seq_order_regardless_of_write_order(tmp_path):
    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(seq=3, platform="twitter", episode_name="c", start=20, end=30, sha256="z")
    journal.append_intent(seq=1, platform="twitter", episode_name="a", start=0, end=10, sha256="x")
    journal.append_intent(seq=2, platform="twitter", episode_name="b", start=10, end=20, sha256="y")

    pending = journal.pending_intents()
    assert [p["seq"] for p in pending] == [1, 2, 3]


def test_pending_intents_disambiguates_same_seq_across_platforms(tmp_path):
    """seq is only unique *within* a platform -- twitter seq=1 and reddit
    seq=1 legitimately coexist in the one shared journal.jsonl. Committing
    one must never be mistaken for committing the other."""

    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(seq=1, platform="twitter", episode_name="tw", start=0, end=10, sha256="x")
    journal.append_intent(seq=1, platform="reddit", episode_name="rd", start=0, end=15, sha256="y")
    journal.append_committed(seq=1, platform="twitter", episode_name="tw", episode_uuid="u1")

    pending = journal.pending_intents()
    assert len(pending) == 1
    assert pending[0]["platform"] == "reddit"
    assert pending[0]["episode_name"] == "rd"


def test_pending_intents_a_seq_overwritten_by_a_later_intent_with_same_key_uses_latest(tmp_path):
    # Defensive: should never happen in practice (seq is reserved once per
    # send), but the last record for a given (platform, seq) key wins.
    journal = GraphIngestionJournal(str(tmp_path))
    journal.append_intent(seq=1, platform="twitter", episode_name="a-old", start=0, end=5, sha256="x")
    journal.append_intent(seq=1, platform="twitter", episode_name="a-new", start=0, end=8, sha256="w")

    pending = journal.pending_intents()
    assert len(pending) == 1
    assert pending[0]["episode_name"] == "a-new"


# ---------------------------------------------------------------------------
# Cursor: load/save, atomicity, monotonicity expectations
# ---------------------------------------------------------------------------

def test_load_cursor_defaults_when_missing(tmp_path):
    cursor = load_cursor(str(tmp_path), "twitter")
    assert cursor == {"batched_offset": 0, "next_seq": 1}


def test_save_then_load_cursor_round_trips(tmp_path):
    save_cursor(str(tmp_path), "twitter", batched_offset=500, next_seq=4)
    cursor = load_cursor(str(tmp_path), "twitter")
    assert cursor == {"batched_offset": 500, "next_seq": 4}


def test_cursor_is_written_with_owner_only_permissions(tmp_path):
    save_cursor(str(tmp_path), "twitter", batched_offset=10, next_seq=2)
    mode = stat.S_IMODE(os.stat(cursor_path(str(tmp_path), "twitter")).st_mode)
    assert mode == 0o600


def test_cursors_are_independent_per_platform(tmp_path):
    save_cursor(str(tmp_path), "twitter", batched_offset=100, next_seq=3)
    save_cursor(str(tmp_path), "reddit", batched_offset=50, next_seq=2)

    assert load_cursor(str(tmp_path), "twitter") == {"batched_offset": 100, "next_seq": 3}
    assert load_cursor(str(tmp_path), "reddit") == {"batched_offset": 50, "next_seq": 2}


def test_load_cursor_tolerates_a_corrupt_file(tmp_path):
    path = cursor_path(str(tmp_path), "twitter")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("{not json")

    assert load_cursor(str(tmp_path), "twitter") == {"batched_offset": 0, "next_seq": 1}


def test_load_cursor_rejects_negative_or_non_int_fields(tmp_path):
    path = cursor_path(str(tmp_path), "twitter")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"batched_offset": -5, "next_seq": "oops"}, f)

    assert load_cursor(str(tmp_path), "twitter") == {"batched_offset": 0, "next_seq": 1}


def test_save_cursor_rejects_invalid_values(tmp_path):
    with pytest.raises(ValueError):
        save_cursor(str(tmp_path), "twitter", batched_offset=-1, next_seq=1)
    with pytest.raises(ValueError):
        save_cursor(str(tmp_path), "twitter", batched_offset=0, next_seq=0)


def test_repeated_cursor_saves_can_only_move_offset_and_seq_forward_when_caller_does_so(tmp_path):
    """The module itself does not enforce monotonicity (a caller could pass
    any value), but the normal call sequence in
    ZepGraphMemoryUpdater._send_batch_activities always advances both
    fields together; this test locks in that a straightforward sequence of
    saves reflects strictly increasing offsets/seqs end to end."""

    save_cursor(str(tmp_path), "twitter", batched_offset=0, next_seq=1)
    save_cursor(str(tmp_path), "twitter", batched_offset=120, next_seq=2)
    save_cursor(str(tmp_path), "twitter", batched_offset=250, next_seq=3)

    cursor = load_cursor(str(tmp_path), "twitter")
    assert cursor["batched_offset"] == 250
    assert cursor["next_seq"] == 3


# ---------------------------------------------------------------------------
# iter_action_lines: the byte-accurate scanner over actions.jsonl
# ---------------------------------------------------------------------------

def _write_actions_log(path: str, entries) -> list:
    """Write one JSON object per line (matching action_logger.py's exact
    `json.dumps(entry, ensure_ascii=False) + '\\n'` shape) and return the
    list of (start, end) byte offsets each line actually occupies, computed
    independently by re-reading the file, for cross-checking against
    `iter_action_lines`'s own offsets."""

    os.makedirs(os.path.dirname(path), exist_ok=True)
    offsets = []
    with open(path, "wb") as f:
        pos = 0
        for entry in entries:
            line = (json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8")
            offsets.append((pos, pos + len(line)))
            f.write(line)
            pos += len(line)
    return offsets


def test_iter_action_lines_offsets_match_real_file_positions(tmp_path):
    path = str(tmp_path / "twitter" / "actions.jsonl")
    entries = [
        {"round": 0, "agent_id": 1, "action_type": "CREATE_POST", "success": True},
        {"round": 0, "event_type": "round_end", "round_num": 0},
        {"round": 1, "agent_id": 2, "action_type": "LIKE_POST", "success": True},
    ]
    expected_offsets = _write_actions_log(path, entries)

    results = list(iter_action_lines(path))
    assert [(s, e) for s, e, _ in results] == expected_offsets
    assert [parsed["action_type"] if "action_type" in parsed else parsed.get("event_type")
            for _, _, parsed in results] == ["CREATE_POST", "round_end", "LIKE_POST"]

    # A fresh open()+seek() to a returned start offset must land exactly on
    # that line's first byte -- this is the guarantee resume_ingestion's
    # byte-range rebuild depends on.
    last_start, last_end, _ = results[-1]
    with open(path, "rb") as f:
        f.seek(last_start)
        raw = f.read(last_end - last_start)
    assert json.loads(raw) == entries[-1]


def test_iter_action_lines_resumes_from_a_mid_file_offset(tmp_path):
    path = str(tmp_path / "twitter" / "actions.jsonl")
    entries = [{"round": i, "agent_id": i, "action_type": "CREATE_POST"} for i in range(4)]
    offsets = _write_actions_log(path, entries)

    resumed = list(iter_action_lines(path, start_offset=offsets[2][0]))
    assert len(resumed) == 2
    assert resumed[0][2]["round"] == 2
    assert resumed[1][2]["round"] == 3


def test_iter_action_lines_skips_blank_lines_without_yielding_them(tmp_path):
    path = str(tmp_path / "twitter" / "actions.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"round": 0, "action_type": "CREATE_POST"}) + "\n")
        f.write("\n")
        f.write(json.dumps({"round": 1, "action_type": "LIKE_POST"}) + "\n")

    results = list(iter_action_lines(path))
    assert len(results) == 2
    assert results[0][2]["action_type"] == "CREATE_POST"
    assert results[1][2]["action_type"] == "LIKE_POST"


def test_iter_action_lines_yields_none_for_a_malformed_line_but_keeps_going(tmp_path):
    path = str(tmp_path / "twitter" / "actions.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"round": 0, "action_type": "CREATE_POST"}) + "\n")
        f.write("{this is not json\n")
        f.write(json.dumps({"round": 1, "action_type": "LIKE_POST"}) + "\n")

    results = list(iter_action_lines(path))
    assert len(results) == 3
    assert results[0][2]["action_type"] == "CREATE_POST"
    assert results[1][2] is None
    assert results[2][2]["action_type"] == "LIKE_POST"


def test_iter_action_lines_on_missing_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(iter_action_lines(str(tmp_path / "does-not-exist" / "actions.jsonl")))


def test_iter_action_lines_on_empty_file_yields_nothing(tmp_path):
    path = str(tmp_path / "twitter" / "actions.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").close()

    assert list(iter_action_lines(path)) == []


def test_is_activity_line():
    assert is_activity_line({"action_type": "CREATE_POST"}) is True
    assert is_activity_line({"event_type": "round_end"}) is False
    assert is_activity_line(None) is False


def test_actions_log_path_and_journal_paths_match_expected_layout(tmp_path):
    sim_dir = str(tmp_path)
    assert actions_log_path(sim_dir, "twitter") == os.path.join(sim_dir, "twitter", "actions.jsonl")
    assert journal_path(sim_dir) == os.path.join(sim_dir, "graph_ingestion", "journal.jsonl")
    assert cursor_path(sim_dir, "reddit") == os.path.join(sim_dir, "graph_ingestion", "cursor_reddit.json")
