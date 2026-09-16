"""Write-ahead journal for MiroFish's graph-ingestion pipeline.

Why this exists
----------------
`ZepGraphMemoryUpdater` (`zep_graph_memory_updater.py`) used to keep all
pending work in RAM only (`Queue` + per-platform buffers). A hard kill (OOM,
`kill -9`, laptop sleep, or the LLM provider running out of credits mid-drain)
lost every batch that had not yet finished `add_episode`, with no way to
finish the drain later -- even after the underlying problem (credentials,
crash) was fixed.

This module gives the ingestion pipeline a durable, replay-safe record of
"what has been sent, and what is still ambiguous" so a separate, explicit
recovery pass (`ZepGraphMemoryManager.resume_ingestion`) can pick up exactly
where a crashed run left off, without ever double-ingesting a batch that
actually made it into Neo4j.

On-disk layout (per simulation)
--------------------------------
    <sim_dir>/graph_ingestion/
        journal.jsonl            # append-only, fsync'd per append
        cursor_twitter.json      # atomic; {"batched_offset": int, "next_seq": int}
        cursor_reddit.json       # atomic; same shape, one per platform

`journal.jsonl` holds two record types, one JSON object per line:

    INTENT    {"type": "INTENT", "seq": int, "platform": str,
               "episode_name": str, "start": int, "end": int,
               "sha256": str, "written_at": iso8601}

        Written (and fsync'd) *before* the corresponding `add_episode` call
        is attempted. `start`/`end` are the exact half-open byte range
        `[start, end)` of that platform's `actions.jsonl` that produced the
        episode text (inclusive of any interleaved round_start/round_end/
        simulation_end event lines or filtered-out entries -- the range
        marks "everything up to `end` has already been accounted for by
        some batch", not "every byte in here is episode text").
        `sha256` is the SHA-256 of the exact episode text (`episode_body`)
        that was (or was about to be) sent for this seq.

    COMMITTED {"type": "COMMITTED", "seq": int, "platform": str,
               "episode_name": str, "episode_uuid": str, "written_at": iso8601}

        Written only *after* `add_episode` returns successfully (whether
        that happens on the first attempt or during a later
        `resume_ingestion` replay). An INTENT with no matching COMMITTED
        (matched on `(platform, seq)`, since seq is only unique per
        platform) is the durable record of an *ambiguous* batch: the
        process could have died at any point between the INTENT fsync and
        Neo4j actually finishing the write, so whether it landed or not is
        unknown until `resume_ingestion` checks.

`cursor_{platform}.json` holds `batched_offset` (the byte offset in that
platform's `actions.jsonl` up to which every line has already been folded
into some batch -- sent, pending, or explicitly skipped) and `next_seq` (the
next per-platform sequence number to hand out). It is advanced atomically
*after* the INTENT append/fsync and *before* `add_episode` is attempted, so
a crash between INTENT-write and `add_episode` still leaves the cursor
correctly past that batch's bytes -- re-deriving that same range again on
resume would be wrong; `resume_ingestion` uses the INTENT's own recorded
range instead.

Everything here is dependency-free file I/O (using
`app.utils.atomic_io.atomic_write_json` for the cursors, exactly like
`settings_store.py` does for credentials) plus a small line-scanner over
`actions.jsonl`. It has no knowledge of Graphiti, episode text construction,
or the AgentActivity model -- that logic stays in
`zep_graph_memory_updater.py`, which is the only caller.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..utils.atomic_io import atomic_write_json, read_json_tolerant
from ..utils.logger import get_logger

logger = get_logger("mirofish.graph_ingestion_journal")

JOURNAL_DIR_NAME = "graph_ingestion"
JOURNAL_FILE_NAME = "journal.jsonl"

_DEFAULT_CURSOR = {"batched_offset": 0, "next_seq": 1}


def journal_dir(sim_dir: str) -> str:
    return os.path.join(sim_dir, JOURNAL_DIR_NAME)


def journal_path(sim_dir: str) -> str:
    return os.path.join(journal_dir(sim_dir), JOURNAL_FILE_NAME)


def cursor_path(sim_dir: str, platform: str) -> str:
    return os.path.join(journal_dir(sim_dir), f"cursor_{platform}.json")


def actions_log_path(sim_dir: str, platform: str) -> str:
    return os.path.join(sim_dir, platform, "actions.jsonl")


def load_cursor(sim_dir: str, platform: str) -> Dict[str, int]:
    """Read `cursor_{platform}.json`, tolerating a missing/corrupt file.

    A missing or unparseable cursor is treated as "nothing batched yet"
    (offset 0, seq 1) -- the same fresh-start semantics as a brand-new
    simulation, since a partially-written cursor can never be trusted more
    than that (atomic_write_json guarantees no *torn* write is ever
    observed, but a genuinely absent file is a normal, expected state for a
    simulation that has not ingested anything yet).
    """

    data = read_json_tolerant(cursor_path(sim_dir, platform), default=None)
    if not isinstance(data, dict):
        return dict(_DEFAULT_CURSOR)

    batched_offset = data.get("batched_offset", 0)
    next_seq = data.get("next_seq", 1)
    if not isinstance(batched_offset, int) or batched_offset < 0:
        batched_offset = 0
    if not isinstance(next_seq, int) or next_seq < 1:
        next_seq = 1
    return {"batched_offset": batched_offset, "next_seq": next_seq}


def save_cursor(sim_dir: str, platform: str, *, batched_offset: int, next_seq: int) -> None:
    """Atomically persist the cursor. See `atomic_io.atomic_write_json` --
    this can never leave a torn/partial cursor file on disk, even under a
    `kill -9` mid-write."""

    if batched_offset < 0:
        raise ValueError("batched_offset must not be negative")
    if next_seq < 1:
        raise ValueError("next_seq must be at least 1")
    atomic_write_json(
        cursor_path(sim_dir, platform),
        {"batched_offset": batched_offset, "next_seq": next_seq},
        mode=0o600,
    )


def iter_action_lines(
    path: str, start_offset: int = 0
) -> Iterator[Tuple[int, int, Optional[Dict[str, Any]]]]:
    """Yield `(line_start, line_end, parsed)` for every raw line in *path*
    starting at byte offset *start_offset*, in file order.

    `parsed` is `None` for a line that fails `json.loads` -- mirrors
    `SimulationRunner._read_action_log`'s tolerant `except
    json.JSONDecodeError: pass` handling of malformed lines. Callers should
    still advance past these (they are yielded, not silently swallowed)
    rather than getting stuck.

    Reads in binary mode so the returned offsets are exact byte offsets
    (usable directly with a later `seek()`, including from a fresh `open()`
    in a different process), independent of any text-mode newline/encoding
    translation. `action_logger.PlatformActionLogger` always appends
    `'\\n'`-terminated UTF-8 JSON lines, one `write()` call each.
    """

    with open(path, "rb") as f:
        f.seek(start_offset)
        pos = start_offset
        for raw_line in f:
            line_start = pos
            pos += len(raw_line)
            line_end = pos
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                yield (line_start, line_end, None)
                continue
            yield (line_start, line_end, parsed)


def is_activity_line(parsed: Optional[Dict[str, Any]]) -> bool:
    """True for a line that `SimulationRunner._read_action_log` would feed
    into `add_activity_from_dict` -- i.e. everything except
    round_start/round_end/simulation_end event entries and unparseable
    lines. Does NOT filter `success: false` or `DO_NOTHING`; those are
    filtered further downstream (see `zep_graph_memory_updater.py`), same as
    the live tailer does.
    """

    return isinstance(parsed, dict) and "event_type" not in parsed


class GraphIngestionJournal:
    """Append-only write-ahead journal for one simulation's graph ingestion.

    Not internally locked: callers must serialize their own writes (in
    practice, `ZepGraphMemoryUpdater` only ever appends from its single
    worker thread / a caller that has already joined it, and
    `resume_ingestion` refuses to run while a live updater exists for the
    same simulation -- see `app/api/simulation.py`).
    """

    def __init__(self, sim_dir: str):
        self.sim_dir = sim_dir
        self.dir = journal_dir(sim_dir)
        self.path = journal_path(sim_dir)

    def append_intent(
        self,
        *,
        seq: int,
        platform: str,
        episode_name: str,
        start: int,
        end: int,
        sha256: str,
    ) -> None:
        self._append(
            {
                "type": "INTENT",
                "seq": seq,
                "platform": platform,
                "episode_name": episode_name,
                "start": start,
                "end": end,
                "sha256": sha256,
                "written_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def append_committed(
        self,
        *,
        seq: int,
        platform: str,
        episode_name: str,
        episode_uuid: str,
    ) -> None:
        self._append(
            {
                "type": "COMMITTED",
                "seq": seq,
                "platform": platform,
                "episode_name": episode_name,
                "episode_uuid": episode_uuid,
                "written_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def _append(self, record: Dict[str, Any]) -> None:
        os.makedirs(self.dir, exist_ok=True)
        line = (json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        # O_APPEND + a single write() of one line keeps the append atomic
        # with respect to any other reader (POSIX guarantees a write() of
        # this size, well under PIPE_BUF, is never interleaved with another
        # writer's -- moot here since there is only ever one writer, but the
        # fsync below is what actually matters: it forces the line onto
        # disk before this call returns, so a `kill -9` immediately after
        # can never lose it.
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)

    def read_records(self) -> List[Dict[str, Any]]:
        """Return every well-formed record, in file order. A record that
        fails to parse (possible only if the process died mid-`write()` of
        the record's own bytes, before the fsync in `_append` could have
        returned -- i.e. not durably committed in the first place) is
        skipped rather than raised, so a journal with exactly one such
        trailing partial line is still fully readable.
        """

        if not os.path.exists(self.path):
            return []
        records: List[Dict[str, Any]] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(
                        "Skipping unparseable trailing journal line in %s", self.path
                    )
                    continue
        return records

    def pending_intents(self) -> List[Dict[str, Any]]:
        """Every INTENT record with no matching COMMITTED, oldest first.

        Matched on `(platform, seq)`: `seq` is only unique *within* a
        platform (each platform has its own cursor/sequence), so two
        different platforms can legitimately share the same seq number in
        the one shared `journal.jsonl`.
        """

        records = self.read_records()
        committed_keys = {
            (r.get("platform"), r.get("seq"))
            for r in records
            if r.get("type") == "COMMITTED"
        }
        intents_by_key: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
        for r in records:
            if r.get("type") != "INTENT":
                continue
            intents_by_key[(r.get("platform"), r.get("seq"))] = r

        pending = [
            intents_by_key[key]
            for key in intents_by_key
            if key not in committed_keys
        ]
        pending.sort(key=lambda r: (r.get("platform") or "", r.get("seq") or 0))
        return pending
