"""
针对 run_twitter_simulation.py / run_reddit_simulation.py 的动作日志回归测试

背景：这两个单平台脚本此前完全没有接入 action_logger，导致 actions.jsonl
从未被写入，后端监控（simulation_runner._read_action_log）读不到任何内容，
UI 动作流为空，图谱记忆摄取管线也因此永远不会被触发。

本测试验证：
1. 两个脚本都正确导入并使用 PlatformActionLogger（回归防护，防止再次漏掉）。
2. 两个脚本内部重新实现的 fetch_new_actions_from_db 能从 SQLite trace 表中
   正确还原动作记录（字段形状与 run_parallel_simulation.py 中的实现一致）。
3. 两个脚本产生的 actions.jsonl 内容能被后端真实使用的解析逻辑
   （SimulationRunner._read_action_log）正确解析：round_start/round_end/
   simulation_end 事件被正确识别，逐条动作被计入 state，且 simulation_end
   事件能正确置位 twitter_completed / reddit_completed。
"""

import json
import os
import sqlite3
import sys

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BACKEND_DIR = os.path.abspath(os.path.join(_TESTS_DIR, ".."))
_SCRIPTS_DIR = os.path.join(_BACKEND_DIR, "scripts")

# run_twitter_simulation.py / run_reddit_simulation.py 是裸脚本（无 __init__.py），
# 需要手动把 scripts 目录加入 sys.path 才能 import，这与脚本自身启动时的做法一致。
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import run_twitter_simulation as twitter_script  # noqa: E402
import run_reddit_simulation as reddit_script  # noqa: E402
from action_logger import PlatformActionLogger  # noqa: E402

from app.services.simulation_runner import (  # noqa: E402
    RunnerStatus,
    SimulationRunState,
    SimulationRunner,
)


# ---------------------------------------------------------------------------
# 1. 静态接线检查：两个脚本必须导入并使用 PlatformActionLogger
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("module", [twitter_script, reddit_script])
def test_script_imports_platform_action_logger(module):
    assert module.PlatformActionLogger is PlatformActionLogger


@pytest.mark.parametrize(
    "runner_cls",
    [twitter_script.TwitterSimulationRunner, reddit_script.RedditSimulationRunner],
)
def test_runner_declares_action_logger_slot(tmp_path, runner_cls):
    """Runner.__init__ 必须准备好 action_logger / agent_names 状态"""
    config_path = tmp_path / "simulation_config.json"
    config_path.write_text(json.dumps({"simulation_id": "sim_test", "agent_configs": []}))
    runner = runner_cls(config_path=str(config_path), wait_for_commands=False)
    assert runner.action_logger is None
    assert runner.agent_names == {}


@pytest.mark.parametrize("module", [twitter_script, reddit_script])
def test_round_loop_checks_shutdown_event(module):
    """第二个已验证的 bug：round 循环必须能响应 _shutdown_event 提前退出"""
    import inspect

    if module is twitter_script:
        source = inspect.getsource(module.TwitterSimulationRunner.run)
    else:
        source = inspect.getsource(module.RedditSimulationRunner.run)

    assert "_shutdown_event" in source
    assert "_shutdown_event.is_set()" in source


# ---------------------------------------------------------------------------
# 2. fetch_new_actions_from_db 的行为一致性（两个脚本本地重新实现的版本）
# ---------------------------------------------------------------------------

def _make_trace_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE trace (
            user_id INTEGER,
            created_at TEXT,
            action TEXT,
            info TEXT
        )
        """
    )
    cursor.execute(
        "INSERT INTO trace (user_id, created_at, action, info) VALUES (?, ?, ?, ?)",
        (1, "2026-01-01T00:00:00", "create_post", json.dumps({"content": "hello world"})),
    )
    cursor.execute(
        "INSERT INTO trace (user_id, created_at, action, info) VALUES (?, ?, ?, ?)",
        (2, "2026-01-01T00:00:01", "refresh", json.dumps({})),
    )
    conn.commit()
    conn.close()


@pytest.mark.parametrize("module", [twitter_script, reddit_script])
def test_fetch_new_actions_from_db_shape(tmp_path, module):
    db_path = str(tmp_path / "sim.db")
    _make_trace_db(db_path)

    agent_names = {1: "founders", 2: "candidates"}
    actions, new_last_rowid = module.fetch_new_actions_from_db(db_path, 0, agent_names)

    # 'refresh' 属于 FILTERED_ACTIONS，应被过滤掉，只剩 create_post
    assert len(actions) == 1
    assert new_last_rowid == 2  # rowid 推进到最后一行，即便该行被过滤

    action = actions[0]
    assert action["agent_id"] == 1
    assert action["agent_name"] == "founders"
    assert action["action_type"] == "CREATE_POST"
    assert action["action_args"]["content"] == "hello world"


# ---------------------------------------------------------------------------
# 3. 端到端 schema 兼容性：用后端真实解析逻辑读取脚本产生的 actions.jsonl
# ---------------------------------------------------------------------------

def _write_platform_log(simulation_dir: str, platform: str) -> str:
    """模拟脚本在一次含 2 轮的模拟中会写出的事件序列（与 run_parallel_simulation.py
    的调用顺序完全一致：simulation_start -> round 0(初始事件) -> round 1..N -> simulation_end）"""
    logger = PlatformActionLogger(platform, simulation_dir)

    config = {
        "time_config": {"total_simulation_hours": 1},
        "agent_configs": [{"agent_id": 0}, {"agent_id": 1}],
    }
    logger.log_simulation_start(config)

    # round 0：初始事件
    logger.log_round_start(0, 0)
    logger.log_action(
        round_num=0,
        agent_id=0,
        agent_name="founders",
        action_type="CREATE_POST",
        action_args={"content": "initial post"},
    )
    logger.log_round_end(0, 1)

    # round 1：主循环第一轮
    logger.log_round_start(1, 0)
    logger.log_action(
        round_num=1,
        agent_id=1,
        agent_name="candidates",
        action_type="LIKE_POST",
        action_args={"post_id": 1},
    )
    logger.log_round_end(1, 1)

    # round 2：无活跃 agent 的空轮次
    logger.log_round_start(2, 1)
    logger.log_round_end(2, 0)

    logger.log_simulation_end(2, 2)

    return logger.log_path


@pytest.mark.parametrize("platform", ["twitter", "reddit"])
def test_produced_log_is_valid_jsonl(tmp_path, platform):
    log_path = _write_platform_log(str(tmp_path), platform)

    assert os.path.exists(log_path)
    with open(log_path, "r", encoding="utf-8") as f:
        lines = [line for line in f if line.strip()]

    assert len(lines) == 10  # start + 3*(round_start+round_end) + 2 actions + end
    for line in lines:
        json.loads(line)  # must not raise


@pytest.mark.parametrize("platform", ["twitter", "reddit"])
def test_backend_parser_reads_produced_log_correctly(tmp_path, platform):
    """核心回归测试：确认后端真实使用的 _read_action_log 能正确解析
    单平台脚本产生的 actions.jsonl，覆盖此前该文件根本不存在导致的
    UI 动作流为空 / 图谱记忆管线从不触发的问题。"""
    log_path = _write_platform_log(str(tmp_path), platform)

    state = SimulationRunState(
        simulation_id="sim_test_single_platform",
        runner_status=RunnerStatus.RUNNING,
    )

    new_position = SimulationRunner._read_action_log(log_path, 0, state, platform)

    assert new_position > 0

    # 两条真实动作（round 0 的 CREATE_POST + round 1 的 LIKE_POST）都要被记录
    assert len(state.recent_actions) == 2
    action_types = {a.action_type for a in state.recent_actions}
    assert action_types == {"CREATE_POST", "LIKE_POST"}

    if platform == "twitter":
        assert state.twitter_actions_count == 2
        assert state.reddit_actions_count == 0
        assert state.twitter_completed is True
        assert state.twitter_running is False
    else:
        assert state.reddit_actions_count == 2
        assert state.twitter_actions_count == 0
        assert state.reddit_completed is True
        assert state.reddit_running is False

    # round_end 事件必须把 current_round 推进到最新一轮（round 2）
    assert state.current_round == 2


@pytest.mark.parametrize("platform", ["twitter", "reddit"])
def test_check_all_platforms_completed_sees_single_platform_log(tmp_path, platform, monkeypatch):
    """回归第二个后端消费点：_check_all_platforms_completed 通过检测
    actions.jsonl 是否存在来判断平台是否启用；此前单平台脚本从不写这个
    文件，平台永远不会被视为"已启用"，因而也永远不会被视为"已完成"。"""
    sim_dir = tmp_path / "sim_test_all_platforms"
    sim_dir.mkdir()
    platform_dir = sim_dir / platform
    platform_dir.mkdir()

    _write_platform_log(str(sim_dir), platform)

    monkeypatch.setattr(SimulationRunner, "RUN_STATE_DIR", str(tmp_path))

    state = SimulationRunState(
        simulation_id="sim_test_all_platforms",
        runner_status=RunnerStatus.RUNNING,
    )
    if platform == "twitter":
        state.twitter_completed = True
    else:
        state.reddit_completed = True

    assert SimulationRunner._check_all_platforms_completed(state) is True
