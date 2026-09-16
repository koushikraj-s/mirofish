"""
崩溃安全与启动自检相关测试

覆盖：
1. run_state.json / state.json 的原子写入与容错读取（模拟kill -9导致的
   中途截断）。
2. process_started_epoch + cmdline 双重校验的PID重用防护
   （SimulationRunner._is_same_process）。
3. 无Popen句柄的孤儿进程终止（SimulationRunner._terminate_orphaned_pid）。
4. 启动自检 SimulationRunner.reconcile_on_boot() 对存活孤儿/已死孤儿/
   已是终态的运行状态的调解行为。
5. 停滞检测信号 stall_detected/stalled_since。
6. report.py 在FAILED运行下仍可生成报告，并携带覆盖度caveat。
"""

import json
import os
import signal
import sys
import time
from types import SimpleNamespace

import psutil
import pytest
from flask import Flask

from app.api import report as report_api
from app.models.project import ProjectStatus
from app.services import simulation_manager as manager_module
from app.services.simulation_manager import SimulationManager, SimulationState
from app.services.simulation_runner import (
    RunnerStatus,
    SimulationRunner,
    SimulationRunState,
)


def _json_result(result):
    if isinstance(result, tuple):
        response, status = result
    else:
        response, status = result, result.status_code
    return response.get_json(), status


def _spawn_marker_process(marker: str):
    """启动一个带有start_new_session=True（拥有独立进程组，避免误杀测试
    进程本身）、长时间存活、且cmdline中包含marker字符串的子进程，
    用于模拟"仍然存活的模拟子进程"。返回(process, create_time_epoch)。

    在真实的后端重启场景中，孤儿进程被 init/launchd 收养后，它退出时会被
    自动回收（reap），不会长期停留在zombie状态。测试进程本身不是init，如果
    不主动reap，一个被SIGTERM/SIGKILL终止的子进程会以zombie状态停留在
    进程表中，导致psutil.pid_exists一直返回True，也会导致对其进程组的
    后续signal投递在某些平台上失败（Operation not permitted）。这里用一个
    后台守护线程持续调用process.wait()来模拟"外部收割者"，使
    psutil.pid_exists在进程真正退出后能够如实反映为False，贴近生产环境下
    孤儿进程被init收养后的真实生命周期。
    """
    import subprocess
    import threading

    process = subprocess.Popen(
        [sys.executable, "-c", f"import time; time.sleep(30)  # {marker}"],
        start_new_session=True,
    )
    reaper = threading.Thread(target=process.wait, daemon=True)
    reaper.start()
    # 给psutil一点时间确认该进程确实已经在系统进程表中可见
    deadline = time.time() + 2
    while time.time() < deadline and not psutil.pid_exists(process.pid):
        time.sleep(0.02)
    create_time = psutil.Process(process.pid).create_time()
    return process, create_time


def _spawn_and_reap_dead_pid() -> int:
    """启动并立刻等待一个进程退出，返回一个几乎可以确定"当前已不存在"的pid。"""
    process = __import__("subprocess").Popen(
        [sys.executable, "-c", "pass"], start_new_session=True
    )
    process.wait(timeout=5)
    return process.pid


# ============== 1. 原子写入 + 容错读取 ==============


def test_save_run_state_writes_atomically_and_load_tolerates_truncation(tmp_path):
    simulation_id = "sim-crash-1"
    SimulationRunner.RUN_STATE_DIR = str(tmp_path)
    try:
        state = SimulationRunState(
            simulation_id=simulation_id,
            runner_status=RunnerStatus.RUNNING,
            current_round=5,
            total_rounds=10,
        )
        SimulationRunner._save_run_state(state)

        state_file = tmp_path / simulation_id / "run_state.json"
        assert state_file.exists()
        # atomic_write_json must never leave its temp file behind on success.
        assert not (tmp_path / simulation_id / "run_state.json.tmp").exists()

        # Full file round-trips cleanly.
        SimulationRunner._run_states.pop(simulation_id, None)
        reloaded = SimulationRunner._load_run_state(simulation_id)
        assert reloaded is not None
        assert reloaded.current_round == 5

        # Simulate a kill -9 mid-write: truncate the file to half its bytes.
        original_bytes = state_file.read_bytes()
        state_file.write_bytes(original_bytes[: len(original_bytes) // 2])

        SimulationRunner._run_states.pop(simulation_id, None)
        degraded = SimulationRunner._load_run_state(simulation_id)
        assert degraded is None  # tolerates corruption instead of raising

        # And get_run_state (the public accessor) must not raise either.
        SimulationRunner._run_states.pop(simulation_id, None)
        assert SimulationRunner.get_run_state(simulation_id) is None
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)


def test_save_run_state_leaves_previous_file_untouched_on_serialization_failure(
    tmp_path,
):
    """A failure while building the new content must never corrupt or
    truncate whatever was already durably on disk (and must not leave a
    stray .tmp file behind).
    """
    simulation_id = "sim-crash-2"
    SimulationRunner.RUN_STATE_DIR = str(tmp_path)
    try:
        good_state = SimulationRunState(
            simulation_id=simulation_id,
            runner_status=RunnerStatus.RUNNING,
            current_round=3,
        )
        SimulationRunner._save_run_state(good_state)
        state_file = tmp_path / simulation_id / "run_state.json"
        original_content = state_file.read_text(encoding="utf-8")

        # An object json.dump cannot serialize (e.g. a raw socket-like
        # object) forces atomic_write_json's write to fail mid-stream.
        broken_state = SimulationRunState(
            simulation_id=simulation_id,
            runner_status=RunnerStatus.RUNNING,
        )
        broken_state.error = object()  # not JSON-serializable

        with pytest.raises(TypeError):
            SimulationRunner._save_run_state(broken_state)

        assert state_file.read_text(encoding="utf-8") == original_content
        assert not (tmp_path / simulation_id / "run_state.json.tmp").exists()
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)


def test_load_simulation_state_tolerates_truncated_state_json(tmp_path, monkeypatch):
    """_load_simulation_state previously had no try/except around json.load
    at all and would raise on a truncated state.json -- unlike
    SimulationRunner._load_run_state. This is the fix for that asymmetry.
    """
    monkeypatch.setattr(manager_module.SimulationManager, "SIMULATION_DATA_DIR", str(tmp_path))
    manager = SimulationManager()
    simulation_id = "sim-crash-3"

    state = SimulationState(
        simulation_id=simulation_id,
        project_id="proj-1",
        graph_id="graph-1",
    )
    manager._save_simulation_state(state)

    state_file = tmp_path / simulation_id / "state.json"
    assert state_file.exists()
    assert not (tmp_path / simulation_id / "state.json.tmp").exists()

    original_bytes = state_file.read_bytes()
    state_file.write_bytes(original_bytes[: len(original_bytes) // 2])

    fresh_manager = SimulationManager()  # bypass fresh_manager's in-memory cache
    fresh_manager._simulations.clear()
    result = fresh_manager._load_simulation_state(simulation_id)
    assert result is None  # degrades instead of raising


# ============== 2. PID重用防护 ==============


def test_is_same_process_true_for_matching_pid_epoch_and_cmdline():
    simulation_id = "sim-pid-match"
    process, create_time = _spawn_marker_process(simulation_id)
    try:
        assert SimulationRunner._is_same_process(
            process.pid, create_time, simulation_id
        ) is True
    finally:
        # A background reaper thread (started in _spawn_marker_process)
        # already owns wait()ing on this process -- calling wait() again
        # here would race it. Just make sure it is asked to exit.
        try:
            process.terminate()
        except Exception:
            pass


def test_is_same_process_false_when_create_time_mismatches():
    simulation_id = "sim-pid-epoch-mismatch"
    process, create_time = _spawn_marker_process(simulation_id)
    try:
        assert SimulationRunner._is_same_process(
            process.pid, create_time - 500, simulation_id
        ) is False
    finally:
        # A background reaper thread (started in _spawn_marker_process)
        # already owns wait()ing on this process -- calling wait() again
        # here would race it. Just make sure it is asked to exit.
        try:
            process.terminate()
        except Exception:
            pass


def test_is_same_process_false_when_cmdline_does_not_match_simulation_id():
    simulation_id = "sim-pid-cmdline-mismatch"
    process, create_time = _spawn_marker_process(simulation_id)
    try:
        assert SimulationRunner._is_same_process(
            process.pid, create_time, "totally-different-simulation-id"
        ) is False
    finally:
        # A background reaper thread (started in _spawn_marker_process)
        # already owns wait()ing on this process -- calling wait() again
        # here would race it. Just make sure it is asked to exit.
        try:
            process.terminate()
        except Exception:
            pass


def test_is_same_process_false_for_dead_pid():
    dead_pid = _spawn_and_reap_dead_pid()
    assert SimulationRunner._is_same_process(dead_pid, None, "whatever") is False


def test_is_same_process_false_for_falsy_pid():
    assert SimulationRunner._is_same_process(None, None, "sim-x") is False
    assert SimulationRunner._is_same_process(0, None, "sim-x") is False


# ============== 3. 孤儿进程终止（无Popen句柄）==============


def test_terminate_orphaned_pid_actually_kills_the_process():
    simulation_id = "sim-terminate-orphan"
    process, _create_time = _spawn_marker_process(simulation_id)
    try:
        assert psutil.pid_exists(process.pid) is True
        SimulationRunner._terminate_orphaned_pid(
            process.pid, simulation_id, timeout=5
        )
        assert psutil.pid_exists(process.pid) is False
    finally:
        pass  # the background reaper thread from _spawn_marker_process reaps it


def test_terminate_orphaned_pid_is_a_noop_for_an_already_dead_pid():
    dead_pid = _spawn_and_reap_dead_pid()
    # Must not raise even though there is nothing left to terminate.
    SimulationRunner._terminate_orphaned_pid(dead_pid, "sim-already-dead", timeout=2)


# ============== 4. 启动自检 reconcile_on_boot() ==============


def _write_run_state(sim_dir, **overrides):
    os.makedirs(sim_dir, exist_ok=True)
    data = {
        "simulation_id": os.path.basename(sim_dir),
        "runner_status": "running",
        "current_round": 1,
        "total_rounds": 10,
        "process_pid": None,
        "process_started_epoch": None,
        "graph_memory_enabled": False,
        "graph_ingestion_complete": True,
        "manual_stop_requested": False,
    }
    data.update(overrides)
    with open(os.path.join(sim_dir, "run_state.json"), "w", encoding="utf-8") as f:
        json.dump(data, f)


def test_reconcile_on_boot_marks_dead_orphan_as_failed(tmp_path, monkeypatch):
    simulation_id = "sim_reconcile_dead"
    sim_dir = tmp_path / simulation_id
    dead_pid = _spawn_and_reap_dead_pid()
    _write_run_state(
        str(sim_dir),
        simulation_id=simulation_id,
        runner_status="running",
        process_pid=dead_pid,
        manual_stop_requested=False,
    )
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        SimulationRunner, "_sync_simulation_status", classmethod(lambda *_a, **_k: None)
    )

    try:
        result = SimulationRunner.reconcile_on_boot()
        assert result["scanned"] == 1
        assert result["reconciled"] == 1
        assert result["skipped_terminal"] == 0
        assert result["errors"] == []

        with open(sim_dir / "run_state.json", encoding="utf-8") as f:
            final = json.load(f)
        assert final["runner_status"] == "failed"
        assert final["error"]
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)
        SimulationRunner._manual_stop_requests.discard(simulation_id)


def test_reconcile_on_boot_terminates_live_orphan_and_marks_stopped_when_manual_stop_was_requested(
    tmp_path, monkeypatch
):
    simulation_id = "sim_reconcile_alive"
    sim_dir = tmp_path / simulation_id
    process, create_time = _spawn_marker_process(simulation_id)
    _write_run_state(
        str(sim_dir),
        simulation_id=simulation_id,
        runner_status="stopping",
        process_pid=process.pid,
        process_started_epoch=create_time,
        manual_stop_requested=True,  # crash happened mid-stop
    )
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        SimulationRunner, "_sync_simulation_status", classmethod(lambda *_a, **_k: None)
    )

    try:
        assert psutil.pid_exists(process.pid) is True
        result = SimulationRunner.reconcile_on_boot()
        assert result["reconciled"] == 1
        assert psutil.pid_exists(process.pid) is False  # orphan was terminated

        with open(sim_dir / "run_state.json", encoding="utf-8") as f:
            final = json.load(f)
        assert final["runner_status"] == "stopped"
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)
        SimulationRunner._manual_stop_requests.discard(simulation_id)
        # the background reaper thread from _spawn_marker_process reaps it


def test_reconcile_on_boot_honestly_marks_ingestion_incomplete_for_enabled_but_unrecoverable_updater(
    tmp_path, monkeypatch
):
    """The exact bug being fixed: after a restart, cls._graph_memory_enabled
    is always empty, so the pre-fix code silently concluded 'no graph
    memory was ever enabled' no matter what state.graph_memory_enabled said.
    """
    simulation_id = "sim_reconcile_graph_memory"
    sim_dir = tmp_path / simulation_id
    dead_pid = _spawn_and_reap_dead_pid()
    _write_run_state(
        str(sim_dir),
        simulation_id=simulation_id,
        runner_status="running",
        process_pid=dead_pid,
        graph_memory_enabled=True,
        graph_ingestion_complete=False,
        manual_stop_requested=False,
    )
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        SimulationRunner, "_sync_simulation_status", classmethod(lambda *_a, **_k: None)
    )
    assert simulation_id not in SimulationRunner._graph_memory_enabled

    try:
        result = SimulationRunner.reconcile_on_boot()
        assert result["reconciled"] == 1

        with open(sim_dir / "run_state.json", encoding="utf-8") as f:
            final = json.load(f)
        # No live updater survives a restart -- honest incomplete, not a
        # silently assumed success.
        assert final["graph_ingestion_complete"] is False
        assert final["runner_status"] == "failed"
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)
        SimulationRunner._manual_stop_requests.discard(simulation_id)


def test_reconcile_on_boot_skips_already_terminal_runs(tmp_path, monkeypatch):
    simulation_id = "sim_reconcile_terminal"
    sim_dir = tmp_path / simulation_id
    _write_run_state(
        str(sim_dir),
        simulation_id=simulation_id,
        runner_status="completed",
        process_pid=999999999,  # must never be inspected/touched
    )
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))

    try:
        result = SimulationRunner.reconcile_on_boot()
        assert result["scanned"] == 1
        assert result["reconciled"] == 0
        assert result["skipped_terminal"] == 1

        with open(sim_dir / "run_state.json", encoding="utf-8") as f:
            final = json.load(f)
        assert final["runner_status"] == "completed"  # untouched
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)


def test_reconcile_on_boot_handles_empty_and_missing_directories(tmp_path, monkeypatch):
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "does-not-exist"))
    result = SimulationRunner.reconcile_on_boot()
    assert result == {"scanned": 0, "reconciled": 0, "skipped_terminal": 0, "errors": []}


# ============== 5. 停滞检测 ==============


def test_stall_detected_only_while_running_and_past_threshold():
    from datetime import datetime, timedelta

    stale = (datetime.now() - timedelta(seconds=99999)).isoformat()
    recent = (datetime.now() - timedelta(seconds=5)).isoformat()

    running_stale = SimulationRunState(
        simulation_id="s1", runner_status=RunnerStatus.RUNNING,
        last_round_advance_at=stale,
    )
    assert running_stale.to_dict()["stall_detected"] is True
    assert running_stale.to_dict()["stalled_since"] == stale

    running_recent = SimulationRunState(
        simulation_id="s2", runner_status=RunnerStatus.RUNNING,
        last_round_advance_at=recent,
    )
    assert running_recent.to_dict()["stall_detected"] is False

    running_never_advanced = SimulationRunState(
        simulation_id="s3", runner_status=RunnerStatus.RUNNING,
    )
    assert running_never_advanced.to_dict()["stall_detected"] is False

    completed_stale = SimulationRunState(
        simulation_id="s4", runner_status=RunnerStatus.COMPLETED,
        last_round_advance_at=stale,
    )
    # Never flag a finished run as "stalled" regardless of how old the
    # timestamp is.
    assert completed_stale.to_dict()["stall_detected"] is False


def test_round_end_event_advances_last_round_advance_at(tmp_path, monkeypatch):
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))
    log_dir = tmp_path / "sim-advance" / "twitter"
    log_dir.mkdir(parents=True)
    log_path = log_dir / "actions.jsonl"
    log_path.write_text(
        json.dumps({"event_type": "round_end", "round": 1, "simulated_hours": 1}) + "\n",
        encoding="utf-8",
    )
    state = SimulationRunState(simulation_id="sim-advance", runner_status=RunnerStatus.RUNNING)
    assert state.last_round_advance_at is None

    SimulationRunner._read_action_log(str(log_path), 0, state, "twitter")

    assert state.last_round_advance_at is not None
    assert state.current_round == 1


# ============== 6. report.py：FAILED运行仍可生成报告 ==============


def test_get_report_includes_coverage_caveat_for_failed_run(monkeypatch):
    report = SimpleNamespace(
        report_id="report-1",
        simulation_id="sim-1",
        graph_id="graph-1",
        simulation_requirement="req",
        status=report_api.ReportStatus.COMPLETED,
        outline=None,
        markdown_content="# report",
        created_at="t0",
        completed_at="t1",
        error=None,
        to_dict=lambda: {
            "report_id": "report-1",
            "simulation_id": "sim-1",
            "status": "completed",
        },
    )
    run_state = SimpleNamespace(
        runner_status=RunnerStatus.FAILED,
        current_round=72,
        total_rounds=72,
        twitter_current_round=72,
        reddit_current_round=72,
        twitter_actions_count=1,
        reddit_actions_count=1,
        graph_memory_enabled=True,
        graph_ingestion_complete=False,
        error="graph drain failed",
    )
    monkeypatch.setattr(
        report_api.ReportManager, "get_report", classmethod(lambda _cls, _id: report)
    )
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(lambda _cls, _id: run_state),
    )

    app = Flask(__name__)
    with app.test_request_context("/api/report/report-1"):
        body, status = _json_result(report_api.get_report("report-1"))

    assert status == 200
    assert body["data"]["graph_possibly_incomplete"] is True
    assert body["data"]["coverage"]["graph_ingestion_complete"] is False


def test_get_report_by_simulation_reports_clean_coverage_for_completed_run(
    monkeypatch,
):
    report = SimpleNamespace(
        report_id="report-2",
        simulation_id="sim-2",
        to_dict=lambda: {"report_id": "report-2", "simulation_id": "sim-2"},
    )
    run_state = SimpleNamespace(
        runner_status=RunnerStatus.COMPLETED,
        current_round=10,
        total_rounds=10,
        twitter_current_round=10,
        reddit_current_round=10,
        twitter_actions_count=3,
        reddit_actions_count=3,
        graph_memory_enabled=True,
        graph_ingestion_complete=True,
        error=None,
    )
    monkeypatch.setattr(
        report_api.ReportManager,
        "get_report_by_simulation",
        classmethod(lambda _cls, _id: report),
    )
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(lambda _cls, _id: run_state),
    )

    app = Flask(__name__)
    with app.test_request_context("/api/report/by-simulation/sim-2"):
        body, status = _json_result(
            report_api.get_report_by_simulation("sim-2")
        )

    assert status == 200
    assert body["data"]["graph_possibly_incomplete"] is False


def test_build_report_coverage_degrades_gracefully_for_minimal_run_state(monkeypatch):
    """A run_state stand-in that only sets runner_status (as several existing
    tests do) must never crash coverage computation.
    """
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(
            lambda _cls, _id: SimpleNamespace(runner_status=RunnerStatus.COMPLETED)
        ),
    )
    result = report_api._build_report_coverage("sim-x")
    assert result["graph_possibly_incomplete"] is False
    assert result["coverage"]["rounds_completed"] is None


def test_build_report_coverage_handles_missing_run_state(monkeypatch):
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(lambda _cls, _id: None),
    )
    result = report_api._build_report_coverage("sim-missing")
    assert result == {"graph_possibly_incomplete": True, "coverage": None}
