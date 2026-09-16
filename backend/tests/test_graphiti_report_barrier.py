from types import SimpleNamespace

import pytest
from flask import Flask

from app.api import graph as graph_api
from app.api import report as report_api
from app.api import simulation as simulation_api
from app.models.project import ProjectStatus
from app.services.simulation_manager import SimulationStatus
from app.services.simulation_runner import RunnerStatus
from app.utils.zep_lifecycle import (
    get_graph_readers,
    unregister_graph_reader,
)


def _json_result(result):
    if isinstance(result, tuple):
        response, status = result
    else:
        response, status = result, result.status_code
    return response.get_json(), status


def test_report_generation_waits_for_graph_ingestion(monkeypatch):
    simulation = SimpleNamespace(project_id="proj-1", graph_id="graph-1")
    monkeypatch.setattr(
        report_api,
        "SimulationManager",
        lambda: SimpleNamespace(
            get_simulation=lambda _simulation_id: simulation
        ),
    )
    monkeypatch.setattr(
        report_api.ReportManager,
        "get_report_by_simulation",
        classmethod(lambda _cls, _simulation_id: None),
    )
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(
            lambda _cls, _simulation_id: SimpleNamespace(
                runner_status=RunnerStatus.STOPPING
            )
        ),
    )
    monkeypatch.setattr(
        report_api.ZepGraphMemoryManager,
        "get_updater",
        classmethod(lambda _cls, _simulation_id: object()),
    )

    app = Flask(__name__)
    with app.test_request_context(
        "/api/report/generate",
        method="POST",
        json={"simulation_id": "sim-1"},
    ):
        body, status = _json_result(report_api.generate_report())

    assert status == 409
    assert body["ingestion_pending"] is True


def test_active_rerun_does_not_return_a_stale_completed_report(monkeypatch):
    simulation = SimpleNamespace(project_id="proj-1", graph_id="graph-1")
    monkeypatch.setattr(
        report_api,
        "SimulationManager",
        lambda: SimpleNamespace(
            get_simulation=lambda _simulation_id: simulation
        ),
    )
    monkeypatch.setattr(
        report_api.ReportManager,
        "get_report_by_simulation",
        classmethod(
            lambda _cls, _simulation_id: SimpleNamespace(
                report_id="old-report",
                status=report_api.ReportStatus.COMPLETED,
            )
        ),
    )
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(
            lambda _cls, _simulation_id: SimpleNamespace(
                runner_status=RunnerStatus.STOPPING
            )
        ),
    )
    monkeypatch.setattr(
        report_api.ZepGraphMemoryManager,
        "get_updater",
        classmethod(lambda _cls, _simulation_id: object()),
    )

    app = Flask(__name__)
    with app.test_request_context(
        "/api/report/generate",
        method="POST",
        json={"simulation_id": "sim-1"},
    ):
        body, status = _json_result(report_api.generate_report())

    assert status == 409
    assert body["ingestion_pending"] is True


def test_failed_ingestion_can_generate_a_caveated_report_after_restart(monkeypatch):
    """A FAILED run (e.g. all rounds finished but the graph drain failed
    after a boot-time reconciliation, or an LLM-credit outage) must still be
    reportable -- the raw action-log data is real -- but the report has to
    say so rather than look identical to a clean COMPLETED run.
    """
    simulation = SimpleNamespace(project_id="proj-1", graph_id="graph-1")
    project = SimpleNamespace(
        project_id="proj-1",
        graph_id="graph-1",
        status=ProjectStatus.GRAPH_COMPLETED,
        simulation_requirement="mock requirement",
    )
    run_state = SimpleNamespace(
        runner_status=RunnerStatus.FAILED,
        current_round=72,
        total_rounds=72,
        twitter_current_round=72,
        reddit_current_round=72,
        twitter_actions_count=10,
        reddit_actions_count=5,
        graph_memory_enabled=True,
        graph_ingestion_complete=False,
        error="43 graph activity batch(es) failed",
    )

    class Tasks:
        def create_task(self, **_kwargs):
            return "task-1"

        def update_task(self, *_args, **_kwargs):
            pass

    class ParkedThread:
        def __init__(self, *, target, daemon):
            self.target = target

        def start(self):
            pass

    monkeypatch.setattr(
        report_api,
        "SimulationManager",
        lambda: SimpleNamespace(
            get_simulation=lambda _simulation_id: simulation
        ),
    )
    monkeypatch.setattr(
        report_api.ProjectManager,
        "get_project",
        classmethod(lambda _cls, _project_id: project),
    )
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(lambda _cls, _simulation_id: run_state),
    )
    monkeypatch.setattr(
        report_api.ZepGraphMemoryManager,
        "get_updater",
        classmethod(lambda _cls, _simulation_id: None),
    )
    monkeypatch.setattr(
        report_api.ReportManager,
        "get_report_by_simulation",
        classmethod(lambda _cls, _simulation_id: None),
    )
    monkeypatch.setattr(report_api, "TaskManager", Tasks)
    monkeypatch.setattr(report_api.threading, "Thread", ParkedThread)

    app = Flask(__name__)
    try:
        with app.test_request_context(
            "/api/report/generate",
            method="POST",
            json={"simulation_id": "sim-1"},
        ):
            body, status = _json_result(report_api.generate_report())

        assert status == 200
        assert body["data"]["graph_possibly_incomplete"] is True
        coverage = body["data"]["coverage"]
        assert coverage["runner_status"] == "failed"
        assert coverage["rounds_completed"] == 72
        assert coverage["total_rounds"] == 72
        assert coverage["graph_ingestion_complete"] is False
    finally:
        report_id = body.get("data", {}).get("report_id") if status == 200 else None
        if report_id:
            unregister_graph_reader("graph-1", report_id)


def test_report_reader_lease_blocks_graph_start_and_delete(monkeypatch):
    simulation = SimpleNamespace(
        simulation_id="sim-1",
        project_id="proj-1",
        graph_id="graph-1",
        status=SimulationStatus.READY,
    )
    project = SimpleNamespace(
        project_id="proj-1",
        graph_id="graph-1",
        status=ProjectStatus.GRAPH_COMPLETED,
        simulation_requirement="mock requirement",
    )
    run_state = SimpleNamespace(runner_status=RunnerStatus.COMPLETED)
    worker_targets = []
    runner_calls = []

    class Tasks:
        def create_task(self, **_kwargs):
            return "task-1"

        def update_task(self, *_args, **_kwargs):
            pass

        def complete_task(self, *_args, **_kwargs):
            pass

        def fail_task(self, *_args, **_kwargs):
            pass

    class ParkedThread:
        def __init__(self, *, target, daemon):
            assert daemon is True
            self.target = target

        def start(self):
            worker_targets.append(self.target)

    class Agent:
        def __init__(self, **_kwargs):
            pass

        def generate_report(self, *, progress_callback, report_id):
            progress_callback("mock", 100, "done")
            return SimpleNamespace(
                report_id=report_id,
                status=report_api.ReportStatus.COMPLETED,
                error=None,
            )

    monkeypatch.setattr(
        report_api,
        "SimulationManager",
        lambda: SimpleNamespace(
            get_simulation=lambda _simulation_id: simulation
        ),
    )
    monkeypatch.setattr(
        simulation_api,
        "SimulationManager",
        lambda: SimpleNamespace(
            get_simulation=lambda _simulation_id: simulation
        ),
    )
    monkeypatch.setattr(
        report_api.ProjectManager,
        "get_project",
        classmethod(lambda _cls, _project_id: project),
    )
    monkeypatch.setattr(
        report_api.SimulationRunner,
        "get_run_state",
        classmethod(lambda _cls, _simulation_id: run_state),
    )
    monkeypatch.setattr(
        report_api.ZepGraphMemoryManager,
        "get_updater",
        classmethod(lambda _cls, _simulation_id: None),
    )
    monkeypatch.setattr(
        report_api.ReportManager,
        "get_report_by_simulation",
        classmethod(lambda _cls, _simulation_id: None),
    )
    monkeypatch.setattr(
        report_api.ReportManager,
        "save_report",
        classmethod(lambda _cls, _report: None),
    )
    monkeypatch.setattr(report_api, "TaskManager", Tasks)
    monkeypatch.setattr(report_api, "ReportAgent", Agent)
    monkeypatch.setattr(report_api.threading, "Thread", ParkedThread)
    monkeypatch.setattr(
        simulation_api.SimulationRunner,
        "start_simulation",
        classmethod(
            lambda _cls, **_kwargs: runner_calls.append(True)
        ),
    )
    monkeypatch.setattr(
        graph_api.ZepGraphMemoryManager,
        "get_simulation_ids_for_graph",
        classmethod(lambda _cls, _graph_id: []),
    )
    monkeypatch.setattr(
        graph_api,
        "SimulationManager",
        lambda: SimpleNamespace(list_simulations=lambda: []),
    )

    app = Flask(__name__)
    report_id = None
    try:
        with app.test_request_context(
            "/api/report/generate",
            method="POST",
            json={"simulation_id": "sim-1"},
        ):
            body, status = _json_result(report_api.generate_report())
        assert status == 200
        report_id = body["data"]["report_id"]
        assert get_graph_readers("graph-1") == [report_id]
        assert len(worker_targets) == 1

        with app.test_request_context(
            "/api/simulation/start",
            method="POST",
            json={
                "simulation_id": "sim-1",
                "enable_graph_memory_update": True,
            },
        ):
            start_body, start_status = _json_result(
                simulation_api.start_simulation()
            )
        assert start_status == 409
        assert start_body["active_reports"] == [report_id]
        assert runner_calls == []

        with pytest.raises(graph_api.GraphInUseError, match=f"report:{report_id}"):
            graph_api._delete_cloud_graph_if_present("graph-1")

        # Let the parked background report finish; its finally block must
        # release the lease even if report generation fails.
        worker_targets[0]()
        assert get_graph_readers("graph-1") == []
    finally:
        if report_id:
            unregister_graph_reader("graph-1", report_id)
