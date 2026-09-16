"""
Round级断点续跑相关测试

覆盖：
1. 三个脚本（run_parallel_simulation.py / run_twitter_simulation.py /
   run_reddit_simulation.py）各自实现的 round_checkpoint.json 读写
   （write_round_checkpoint / load_resume_state / _db_schema_is_complete /
   clear_checkpoint_platform_section）在结构和行为上保持一致。
2. 并行模式下 Twitter / Reddit 两个协程共享同一份 checkpoint 文件时，
   并发写入不会互相覆盖（_checkpoint_lock 的存在意义）。
3. get_active_agents_for_round 从未播种的全局 random 改为按
   "{simulation_id}:{platform}:{round_num}" 播种的 random.Random 之后，
   同一个round可复现，不同round/平台之间仍然独立随机。
4. 后端 SimulationRunner.start_simulation 在 resume=True 时会把
   --resume 追加到子进程命令行；SimulationRunner.resume_simulation 在
   没有有效 round_checkpoint.json 时拒绝续跑（不静默从round 0重来）。
5. /api/simulation/resume 路由：force 与 resume 互斥、缺少
   simulation_id、无效platform、simulation不存在、正常委托给
   SimulationRunner.resume_simulation 并透传 resume=True。
"""

import asyncio
import json
import os
import sqlite3
import sys

import pytest
from flask import Flask

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BACKEND_DIR = os.path.abspath(os.path.join(_TESTS_DIR, ".."))
_SCRIPTS_DIR = os.path.join(_BACKEND_DIR, "scripts")

if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import run_parallel_simulation as parallel_script  # noqa: E402
import run_twitter_simulation as twitter_script  # noqa: E402
import run_reddit_simulation as reddit_script  # noqa: E402

from app.api import simulation as simulation_api  # noqa: E402
from app.services import simulation_runner as runner_module  # noqa: E402
from app.services.simulation_runner import (  # noqa: E402
    SimulationRunner,
    SimulationRunState,
    RunnerStatus,
)
from app.services.simulation_manager import SimulationManager  # noqa: E402


CHECKPOINT_MODULES = [parallel_script, twitter_script, reddit_script]


# ---------------------------------------------------------------------------
# 1. checkpoint 读写：三个脚本的实现互相一致
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_checkpoint_schema_constants_match(module):
    assert module.CHECKPOINT_FILENAME == "round_checkpoint.json"
    assert module.CHECKPOINT_SCHEMA_VERSION == 1


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_load_resume_state_missing_file_refuses(tmp_path, module):
    """找不到checkpoint时必须明确 found=False，而不是假装"从0开始"是安全的
    （调用方看到 found=False 会拒绝续跑）。"""
    state = module.load_resume_state(str(tmp_path), "twitter")
    assert state["found"] is False
    assert state["start_round"] == 0
    assert state["last_rowid"] == 0


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_load_resume_state_corrupt_json_refuses(tmp_path, module):
    """半截JSON（模拟kill -9砍在写入中途, 若没有原子写入保护会发生的情况）
    必须被宽容地当成"没有checkpoint"处理，不能让子进程崩溃。"""
    checkpoint_path = tmp_path / module.CHECKPOINT_FILENAME
    checkpoint_path.write_text('{"schema_version": 1, "twitter": {', encoding="utf-8")
    state = module.load_resume_state(str(tmp_path), "twitter")
    assert state["found"] is False


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_load_resume_state_wrong_schema_version_refuses(tmp_path, module):
    checkpoint_path = tmp_path / module.CHECKPOINT_FILENAME
    checkpoint_path.write_text(
        json.dumps({"schema_version": 999, "twitter": {"next_round_index": 5}}),
        encoding="utf-8",
    )
    state = module.load_resume_state(str(tmp_path), "twitter")
    assert state["found"] is False


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_write_then_load_round_trip(tmp_path, module):
    from datetime import datetime

    original_start = datetime(2026, 1, 1, 8, 0, 0)

    async def _run():
        await module.write_round_checkpoint(
            str(tmp_path), "twitter",
            next_round_index=7, last_rowid=123, total_actions=42,
            original_start_time=original_start,
        )

    asyncio.run(_run())

    state = module.load_resume_state(str(tmp_path), "twitter")
    assert state["found"] is True
    assert state["start_round"] == 7
    assert state["last_rowid"] == 123
    assert state["total_actions"] == 42
    assert state["original_start_time"] == original_start

    # 另一个平台的section不应受影响/不存在
    other = module.load_resume_state(str(tmp_path), "reddit")
    assert other["found"] is False


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_original_start_time_never_drifts_across_resumes(tmp_path, module):
    """连续多次写入（模拟多次resume）必须保留最初的 start_time 不变——
    这是Reddit时钟连续性修复的锚点，如果每次resume都用"现在"重新盖写
    start_time，同一个start_round在不同次resume后会映射到不同的时钟
    起点，制造出漂移。"""
    from datetime import datetime, timedelta

    original_start = datetime(2026, 1, 1, 8, 0, 0)
    later_start = datetime(2026, 1, 2, 20, 0, 0)  # 假装是很久之后才resume

    async def _run():
        await module.write_round_checkpoint(
            str(tmp_path), "twitter",
            next_round_index=1, last_rowid=10, total_actions=1,
            original_start_time=original_start,
        )
        # 第二次写入传入一个不同的 original_start_time，函数必须忽略它，
        # 保留文件里已经存在的那个值。
        await module.write_round_checkpoint(
            str(tmp_path), "twitter",
            next_round_index=2, last_rowid=20, total_actions=2,
            original_start_time=later_start,
        )

    asyncio.run(_run())

    state = module.load_resume_state(str(tmp_path), "twitter")
    assert state["original_start_time"] == original_start
    assert state["start_round"] == 2  # 但每平台section本身要正常更新


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_concurrent_writes_from_both_platforms_do_not_clobber(tmp_path, module):
    """并行模式下 Twitter / Reddit 协程共享同一份 checkpoint 文件。用
    asyncio.gather并发触发两个平台各写入20次，验证 _checkpoint_lock 序列
    化了"读-改-写"，两个平台的最终section都反映各自最后一次写入，谁都没
    有丢失更新。"""
    from datetime import datetime

    original_start = datetime(2026, 1, 1, 0, 0, 0)

    async def _writer(platform, rounds):
        for i in range(rounds):
            await module.write_round_checkpoint(
                str(tmp_path), platform,
                next_round_index=i, last_rowid=i * 10, total_actions=i,
                original_start_time=original_start,
            )
            # 人为让出控制权，加大交叉写入的概率
            await asyncio.sleep(0)

    async def _run():
        await asyncio.gather(
            _writer("twitter", 20),
            _writer("reddit", 20),
        )

    asyncio.run(_run())

    twitter_state = module.load_resume_state(str(tmp_path), "twitter")
    reddit_state = module.load_resume_state(str(tmp_path), "reddit")

    assert twitter_state["found"] is True
    assert twitter_state["start_round"] == 19
    assert reddit_state["found"] is True
    assert reddit_state["start_round"] == 19


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_clear_checkpoint_platform_section_removes_only_that_platform(tmp_path, module):
    from datetime import datetime

    original_start = datetime(2026, 1, 1, 0, 0, 0)

    async def _run():
        await module.write_round_checkpoint(
            str(tmp_path), "twitter", next_round_index=5, last_rowid=50,
            total_actions=5, original_start_time=original_start,
        )
        await module.write_round_checkpoint(
            str(tmp_path), "reddit", next_round_index=3, last_rowid=30,
            total_actions=3, original_start_time=original_start,
        )
        await module.clear_checkpoint_platform_section(str(tmp_path), "twitter")

    asyncio.run(_run())

    assert module.load_resume_state(str(tmp_path), "twitter")["found"] is False
    reddit_state = module.load_resume_state(str(tmp_path), "reddit")
    assert reddit_state["found"] is True
    assert reddit_state["start_round"] == 3


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_clear_checkpoint_platform_section_deletes_file_when_last_section(tmp_path, module):
    """清空最后一个平台的section之后，整份checkpoint文件应该被删除，
    而不是留下一个只有schema_version/start_time、没有任何平台数据的
    空壳文件（避免下次resume误判"找到了记录"）。"""
    from datetime import datetime

    async def _run():
        await module.write_round_checkpoint(
            str(tmp_path), "twitter", next_round_index=5, last_rowid=50,
            total_actions=5, original_start_time=datetime(2026, 1, 1),
        )
        await module.clear_checkpoint_platform_section(str(tmp_path), "twitter")

    asyncio.run(_run())

    assert not os.path.exists(os.path.join(str(tmp_path), module.CHECKPOINT_FILENAME))


# ---------------------------------------------------------------------------
# 2. sqlite schema 完整性防护
# ---------------------------------------------------------------------------

def _make_complete_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for table in [
        "user", "post", "follow", "mute", "like", "dislike", "report",
        "trace", "rec", "comment", "comment_like", "comment_dislike", "product",
    ]:
        cursor.execute(f"CREATE TABLE {table} (id INTEGER)")
    conn.commit()
    conn.close()


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_db_schema_is_complete_true_for_full_schema(tmp_path, module):
    db_path = str(tmp_path / "sim.db")
    _make_complete_db(db_path)
    assert module._db_schema_is_complete(db_path) is True


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_db_schema_is_complete_false_for_missing_table(tmp_path, module):
    """针对"被在自己第一轮结束前杀死的数据库"的防护：create_db()对每张
    表都用不带 IF NOT EXISTS 的 CREATE TABLE 且共享同一个try/except，
    第一条语句失败会导致后面所有表的创建被跳过——résumé前必须能识破
    一个残缺的schema。"""
    db_path = str(tmp_path / "sim.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE user (id INTEGER)")  # 只建一张表，模拟残缺schema
    conn.commit()
    conn.close()
    assert module._db_schema_is_complete(db_path) is False


@pytest.mark.parametrize("module", CHECKPOINT_MODULES, ids=["parallel", "twitter", "reddit"])
def test_db_schema_is_complete_false_for_missing_file(tmp_path, module):
    assert module._db_schema_is_complete(str(tmp_path / "does_not_exist.db")) is False


# ---------------------------------------------------------------------------
# 3. 确定性RNG：get_active_agents_for_round
# ---------------------------------------------------------------------------

class _FakeGraph:
    def get_agent(self, agent_id):
        return f"agent_{agent_id}"


class _FakeEnv:
    agent_graph = _FakeGraph()


def _rng_test_config():
    return {
        "time_config": {"agents_per_hour_min": 5, "agents_per_hour_max": 20},
        "agent_configs": [
            {"agent_id": i, "active_hours": list(range(24)), "activity_level": 0.6}
            for i in range(30)
        ],
    }


def test_parallel_script_active_agents_deterministic_per_round():
    config = _rng_test_config()
    env = _FakeEnv()

    first = parallel_script.get_active_agents_for_round(env, config, 10, 5, "sim_x", "twitter")
    second = parallel_script.get_active_agents_for_round(env, config, 10, 5, "sim_x", "twitter")
    assert [a for a, _ in first] == [a for a, _ in second]

    different_round = parallel_script.get_active_agents_for_round(env, config, 10, 6, "sim_x", "twitter")
    assert [a for a, _ in first] != [a for a, _ in different_round]

    different_platform = parallel_script.get_active_agents_for_round(env, config, 10, 5, "sim_x", "reddit")
    assert [a for a, _ in first] != [a for a, _ in different_platform]

    different_sim = parallel_script.get_active_agents_for_round(env, config, 10, 5, "sim_y", "twitter")
    assert [a for a, _ in first] != [a for a, _ in different_sim]


@pytest.mark.parametrize(
    "runner_cls,platform_tag",
    [
        (twitter_script.TwitterSimulationRunner, "twitter"),
        (reddit_script.RedditSimulationRunner, "reddit"),
    ],
)
def test_single_platform_runner_active_agents_deterministic_per_round(tmp_path, runner_cls, platform_tag):
    config = _rng_test_config()
    config["simulation_id"] = "sim_x"
    config_path = tmp_path / "simulation_config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    runner = runner_cls(config_path=str(config_path), wait_for_commands=False)
    env = _FakeEnv()

    first = runner._get_active_agents_for_round(env, 10, 5)
    second = runner._get_active_agents_for_round(env, 10, 5)
    assert [a for a, _ in first] == [a for a, _ in second]

    different_round = runner._get_active_agents_for_round(env, 10, 6)
    assert [a for a, _ in first] != [a for a, _ in different_round]


# ---------------------------------------------------------------------------
# 4. 后端 SimulationRunner: --resume 命令行 + resume_simulation 校验
# ---------------------------------------------------------------------------

def test_start_simulation_appends_resume_flag_when_resuming(monkeypatch, tmp_path):
    simulation_id = "sim-resume-cmd"
    sim_dir = tmp_path / "runs" / simulation_id
    scripts_dir = tmp_path / "scripts"
    sim_dir.mkdir(parents=True)
    scripts_dir.mkdir()
    (sim_dir / "simulation_config.json").write_text(
        json.dumps({"time_config": {"total_simulation_hours": 1, "minutes_per_round": 60}}),
        encoding="utf-8",
    )
    (scripts_dir / "run_twitter_simulation.py").write_text("pass\n", encoding="utf-8")

    captured_cmds = []

    class Process:
        pid = 4242

        def poll(self):
            return None

    def fake_popen(cmd, **kwargs):
        captured_cmds.append(cmd)
        return Process()

    class FakeThread:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            pass

    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(SimulationRunner, "SCRIPTS_DIR", str(scripts_dir))
    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(runner_module.threading, "Thread", FakeThread)
    monkeypatch.setattr(
        SimulationRunner, "_sync_simulation_status",
        classmethod(lambda _cls, *_a, **_kw: None),
    )

    try:
        SimulationRunner.start_simulation(
            simulation_id, platform="twitter", enable_graph_memory_update=False,
            resume=True,
        )
        assert len(captured_cmds) == 1
        assert "--resume" in captured_cmds[0]
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)
        SimulationRunner._processes.pop(simulation_id, None)
        SimulationRunner._action_queues.pop(simulation_id, None)
        SimulationRunner._stdout_files.pop(simulation_id, None)
        SimulationRunner._stderr_files.pop(simulation_id, None)
        SimulationRunner._graph_memory_enabled.pop(simulation_id, None)


def test_start_simulation_omits_resume_flag_by_default(monkeypatch, tmp_path):
    simulation_id = "sim-no-resume-cmd"
    sim_dir = tmp_path / "runs" / simulation_id
    scripts_dir = tmp_path / "scripts"
    sim_dir.mkdir(parents=True)
    scripts_dir.mkdir()
    (sim_dir / "simulation_config.json").write_text(
        json.dumps({"time_config": {"total_simulation_hours": 1, "minutes_per_round": 60}}),
        encoding="utf-8",
    )
    (scripts_dir / "run_twitter_simulation.py").write_text("pass\n", encoding="utf-8")

    captured_cmds = []

    class Process:
        pid = 4243

        def poll(self):
            return None

    def fake_popen(cmd, **kwargs):
        captured_cmds.append(cmd)
        return Process()

    class FakeThread:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            pass

    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(SimulationRunner, "SCRIPTS_DIR", str(scripts_dir))
    monkeypatch.setattr(runner_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(runner_module.threading, "Thread", FakeThread)
    monkeypatch.setattr(
        SimulationRunner, "_sync_simulation_status",
        classmethod(lambda _cls, *_a, **_kw: None),
    )

    try:
        SimulationRunner.start_simulation(
            simulation_id, platform="twitter", enable_graph_memory_update=False,
        )
        assert len(captured_cmds) == 1
        assert "--resume" not in captured_cmds[0]
    finally:
        SimulationRunner._run_states.pop(simulation_id, None)
        SimulationRunner._processes.pop(simulation_id, None)
        SimulationRunner._action_queues.pop(simulation_id, None)
        SimulationRunner._stdout_files.pop(simulation_id, None)
        SimulationRunner._stderr_files.pop(simulation_id, None)
        SimulationRunner._graph_memory_enabled.pop(simulation_id, None)


def test_resume_simulation_refuses_when_sim_dir_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "runs"))
    with pytest.raises(ValueError, match="模拟目录不存在"):
        SimulationRunner.resume_simulation("sim-does-not-exist")


def test_resume_simulation_refuses_when_no_checkpoint(monkeypatch, tmp_path):
    simulation_id = "sim-no-checkpoint"
    sim_dir = tmp_path / "runs" / simulation_id
    sim_dir.mkdir(parents=True)
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "runs"))

    with pytest.raises(ValueError, match="round_checkpoint"):
        SimulationRunner.resume_simulation(simulation_id)


def test_resume_simulation_refuses_when_checkpoint_has_no_platform_sections(monkeypatch, tmp_path):
    simulation_id = "sim-empty-checkpoint"
    sim_dir = tmp_path / "runs" / simulation_id
    sim_dir.mkdir(parents=True)
    (sim_dir / "round_checkpoint.json").write_text(
        json.dumps({"schema_version": 1, "start_time": "2026-01-01T00:00:00"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "runs"))

    with pytest.raises(ValueError, match="round_checkpoint"):
        SimulationRunner.resume_simulation(simulation_id)


def test_resume_simulation_delegates_to_start_simulation_with_resume_true(monkeypatch, tmp_path):
    simulation_id = "sim-valid-checkpoint"
    sim_dir = tmp_path / "runs" / simulation_id
    sim_dir.mkdir(parents=True)
    (sim_dir / "round_checkpoint.json").write_text(
        json.dumps({
            "schema_version": 1,
            "start_time": "2026-01-01T00:00:00",
            "twitter": {
                "next_round_index": 6, "last_rowid": 100,
                "total_actions_logged": 20, "updated_at": "2026-01-01T01:00:00",
            },
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path / "runs"))

    captured_kwargs = {}
    fake_state = SimulationRunState(simulation_id=simulation_id, runner_status=RunnerStatus.RUNNING)

    def fake_start_simulation(cls, **kwargs):
        captured_kwargs.update(kwargs)
        return fake_state

    monkeypatch.setattr(
        SimulationRunner, "start_simulation", classmethod(fake_start_simulation)
    )

    result = SimulationRunner.resume_simulation(simulation_id, platform="twitter", max_rounds=10)

    assert result is fake_state
    assert captured_kwargs["simulation_id"] == simulation_id
    assert captured_kwargs["platform"] == "twitter"
    assert captured_kwargs["max_rounds"] == 10
    assert captured_kwargs["resume"] is True


# ---------------------------------------------------------------------------
# 5. /api/simulation/resume 路由
# ---------------------------------------------------------------------------

_flask_app = Flask(__name__)


def _post_resume(payload):
    with _flask_app.test_request_context(
        "/api/simulation/resume", method="POST", json=payload,
    ):
        result = simulation_api.resume_simulation()
    if isinstance(result, tuple):
        response, status = result
    else:
        response, status = result, result.status_code
    return response.get_json(), status


def test_resume_route_requires_simulation_id():
    body, status = _post_resume({})
    assert status == 400
    assert body["success"] is False


def test_resume_route_rejects_force_true():
    body, status = _post_resume({"simulation_id": "sim-1", "force": True})
    assert status == 400
    assert body["success"] is False
    assert "force" in body["error"].lower()


def test_resume_route_rejects_invalid_platform(monkeypatch):
    body, status = _post_resume({"simulation_id": "sim-1", "platform": "bogus"})
    assert status == 400


def test_resume_route_404_when_simulation_not_found(monkeypatch):
    monkeypatch.setattr(SimulationManager, "get_simulation", lambda self, sid: None)
    body, status = _post_resume({"simulation_id": "sim-does-not-exist"})
    assert status == 404
    assert body["success"] is False


def test_resume_route_surfaces_value_error_as_400(monkeypatch):
    class _FakeState:
        project_id = "proj-1"

    monkeypatch.setattr(SimulationManager, "get_simulation", lambda self, sid: _FakeState())
    monkeypatch.setattr(
        SimulationRunner, "resume_simulation",
        classmethod(lambda _cls, **_kw: (_ for _ in ()).throw(ValueError("no checkpoint found"))),
    )
    body, status = _post_resume({"simulation_id": "sim-1"})
    assert status == 400
    assert "no checkpoint found" in body["error"]


def test_resume_route_success_forwards_resume_semantics(monkeypatch):
    class _FakeState:
        project_id = "proj-1"

    fake_run_state = SimulationRunState(simulation_id="sim-1", runner_status=RunnerStatus.RUNNING)

    captured = {}

    def fake_resume(cls, **kwargs):
        captured.update(kwargs)
        return fake_run_state

    monkeypatch.setattr(SimulationManager, "get_simulation", lambda self, sid: _FakeState())
    monkeypatch.setattr(SimulationRunner, "resume_simulation", classmethod(fake_resume))

    body, status = _post_resume({"simulation_id": "sim-1", "platform": "parallel"})

    assert status == 200
    assert body["success"] is True
    assert body["data"]["resumed"] is True
    assert captured["simulation_id"] == "sim-1"
    assert captured["platform"] == "parallel"
