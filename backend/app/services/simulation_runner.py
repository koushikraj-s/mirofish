"""
OASIS模拟运行器
在后台运行模拟并记录每个Agent的动作，支持实时状态监控
"""

import os
import sys
import json
import time
import asyncio
import threading
import subprocess
import signal
import atexit
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from queue import Queue

import psutil

from ..config import Config
from ..utils.logger import get_logger
from ..utils.locale import get_locale, set_locale
from ..utils.atomic_io import atomic_write_json, read_json_tolerant
from ..utils.zep import (
    GRAPHITI_QUERY_TIMEOUT_SECONDS,
    ZEP_INGESTION_WAIT_TIMEOUT_SECONDS,
)
from .zep_graph_memory_updater import ZepGraphMemoryManager
from .simulation_ipc import SimulationIPCClient, CommandType, IPCResponse

logger = get_logger('mirofish.simulation_runner')

# 标记是否已注册清理函数
_cleanup_registered = False

# 平台检测
IS_WINDOWS = sys.platform == 'win32'

# 停滞检测阈值（秒）：监控线程认为一次轮次推进"停滞"之前允许的最长间隔。
# 单轮可能因为LLM调用缓慢而合理地耗时数分钟，因此默认值刻意设置得很宽松，
# 仅用于在前端展示提示，绝不用于自动终止模拟。可通过环境变量覆盖。
STALL_THRESHOLD_SECONDS = int(
    os.environ.get("MIROFISH_STALL_THRESHOLD_SECONDS", "1200")
)


class RunnerStatus(str, Enum):
    """运行器状态"""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"


class SimulationStopPending(TimeoutError):
    """The monitor still owns a bounded graph-ingestion finalization."""


@dataclass
class AgentAction:
    """Agent动作记录"""
    round_num: int
    timestamp: str
    platform: str  # twitter / reddit
    agent_id: int
    agent_name: str
    action_type: str  # CREATE_POST, LIKE_POST, etc.
    action_args: Dict[str, Any] = field(default_factory=dict)
    result: Optional[str] = None
    success: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "round_num": self.round_num,
            "timestamp": self.timestamp,
            "platform": self.platform,
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "action_type": self.action_type,
            "action_args": self.action_args,
            "result": self.result,
            "success": self.success,
        }


@dataclass
class RoundSummary:
    """每轮摘要"""
    round_num: int
    start_time: str
    end_time: Optional[str] = None
    simulated_hour: int = 0
    twitter_actions: int = 0
    reddit_actions: int = 0
    active_agents: List[int] = field(default_factory=list)
    actions: List[AgentAction] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "round_num": self.round_num,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "simulated_hour": self.simulated_hour,
            "twitter_actions": self.twitter_actions,
            "reddit_actions": self.reddit_actions,
            "active_agents": self.active_agents,
            "actions_count": len(self.actions),
            "actions": [a.to_dict() for a in self.actions],
        }


@dataclass
class SimulationRunState:
    """模拟运行状态（实时）"""
    simulation_id: str
    runner_status: RunnerStatus = RunnerStatus.IDLE
    
    # 进度信息
    current_round: int = 0
    total_rounds: int = 0
    simulated_hours: int = 0
    total_simulation_hours: int = 0
    
    # 各平台独立轮次和模拟时间（用于双平台并行显示）
    twitter_current_round: int = 0
    reddit_current_round: int = 0
    twitter_simulated_hours: int = 0
    reddit_simulated_hours: int = 0
    
    # 平台状态
    twitter_running: bool = False
    reddit_running: bool = False
    twitter_actions_count: int = 0
    reddit_actions_count: int = 0
    
    # 平台完成状态（通过检测 actions.jsonl 中的 simulation_end 事件）
    twitter_completed: bool = False
    reddit_completed: bool = False
    
    # 每轮摘要
    rounds: List[RoundSummary] = field(default_factory=list)
    
    # 最近动作（用于前端实时展示）
    recent_actions: List[AgentAction] = field(default_factory=list)
    max_recent_actions: int = 50
    
    # 时间戳
    started_at: Optional[str] = None
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    
    # 错误信息
    error: Optional[str] = None

    # 进程ID（用于停止）
    process_pid: Optional[int] = None

    # 进程启动时间（epoch秒，Popen后立即通过psutil捕获）。
    # 与process_pid配合，用于后端重启后安全判断该pid是否仍是"我们的"进程
    # （而不是操作系统回收同一pid后启动的无关进程）。
    process_started_epoch: Optional[float] = None

    # 该次运行是否启用了图谱记忆更新（持久化版本的_graph_memory_enabled字典，
    # 用于后端重启后in-memory字典清空时仍能判断是否需要处理图谱写入收尾）。
    graph_memory_enabled: bool = False

    # 图谱记忆写入是否已确认完整完成。启用图谱更新时默认为False，
    # 仅在updater.stop()成功排空后置为True；未启用图谱更新则始终视为True
    # （没有需要完成的写入）。后端重启后若无法重新确认，诚实地保持/置为False。
    graph_ingestion_complete: bool = True

    # 是否曾经有人（用户或启动自检）对该次运行发起过停止请求，用于在
    # 重启后判定终态应为STOPPED还是FAILED（持久化版本的_manual_stop_requests）。
    manual_stop_requested: bool = False

    # 最近一次真实轮次推进（current_round或*_current_round增加）的时间戳。
    # 是唯一可靠的"仍在前进"信号，用于停滞检测；updated_at每次监控tick都会
    # 刷新，不能作为进度信号。
    last_round_advance_at: Optional[str] = None

    # 监控线程读取各平台actions.jsonl的文件读取位置（原为函数局部变量），
    # 持久化后可在诊断/未来的轮次级恢复逻辑中使用。
    twitter_log_position: int = 0
    reddit_log_position: int = 0

    def add_action(self, action: AgentAction):
        """添加动作到最近动作列表"""
        self.recent_actions.insert(0, action)
        if len(self.recent_actions) > self.max_recent_actions:
            self.recent_actions = self.recent_actions[:self.max_recent_actions]
        
        if action.platform == "twitter":
            self.twitter_actions_count += 1
        else:
            self.reddit_actions_count += 1
        
        self.updated_at = datetime.now().isoformat()
    
    def _compute_stall(self) -> tuple[bool, Optional[str]]:
        """Compute (stall_detected, stalled_since) from last_round_advance_at.

        Only meaningful while the run is actively RUNNING. updated_at is
        refreshed every ~2s monitor tick regardless of progress, so it is
        deliberately NOT used here -- only a genuine round advance moves
        last_round_advance_at, which is what makes this a real "are we
        stuck" signal rather than a "is the monitor thread alive" signal.
        A run that has not advanced a single round yet (last_round_advance_at
        is None) is never reported stalled.
        """
        if self.runner_status != RunnerStatus.RUNNING or not self.last_round_advance_at:
            return False, None
        try:
            last_advance = datetime.fromisoformat(self.last_round_advance_at)
        except (TypeError, ValueError):
            return False, None
        elapsed = (datetime.now() - last_advance).total_seconds()
        if elapsed >= STALL_THRESHOLD_SECONDS:
            return True, self.last_round_advance_at
        return False, None

    def to_dict(self) -> Dict[str, Any]:
        stall_detected, stalled_since = self._compute_stall()
        return {
            "simulation_id": self.simulation_id,
            "runner_status": self.runner_status.value,
            "current_round": self.current_round,
            "total_rounds": self.total_rounds,
            "simulated_hours": self.simulated_hours,
            "total_simulation_hours": self.total_simulation_hours,
            "progress_percent": round(self.current_round / max(self.total_rounds, 1) * 100, 1),
            # 各平台独立轮次和时间
            "twitter_current_round": self.twitter_current_round,
            "reddit_current_round": self.reddit_current_round,
            "twitter_simulated_hours": self.twitter_simulated_hours,
            "reddit_simulated_hours": self.reddit_simulated_hours,
            "twitter_running": self.twitter_running,
            "reddit_running": self.reddit_running,
            "twitter_completed": self.twitter_completed,
            "reddit_completed": self.reddit_completed,
            "twitter_actions_count": self.twitter_actions_count,
            "reddit_actions_count": self.reddit_actions_count,
            "total_actions_count": self.twitter_actions_count + self.reddit_actions_count,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "process_pid": self.process_pid,
            "process_started_epoch": self.process_started_epoch,
            "graph_memory_enabled": self.graph_memory_enabled,
            "graph_ingestion_complete": self.graph_ingestion_complete,
            "manual_stop_requested": self.manual_stop_requested,
            "last_round_advance_at": self.last_round_advance_at,
            "twitter_log_position": self.twitter_log_position,
            "reddit_log_position": self.reddit_log_position,
            # 停滞检测：仅用于前端提示，从不触发自动终止
            "stall_detected": stall_detected,
            "stalled_since": stalled_since,
        }
    
    def to_detail_dict(self) -> Dict[str, Any]:
        """包含最近动作的详细信息"""
        result = self.to_dict()
        result["recent_actions"] = [a.to_dict() for a in self.recent_actions]
        result["rounds_count"] = len(self.rounds)
        return result


class SimulationRunner:
    """
    模拟运行器
    
    负责：
    1. 在后台进程中运行OASIS模拟
    2. 解析运行日志，记录每个Agent的动作
    3. 提供实时状态查询接口
    4. 支持暂停/停止/恢复操作
    """
    
    # 运行状态存储目录
    RUN_STATE_DIR = os.path.join(
        os.path.dirname(__file__),
        '../../uploads/simulations'
    )
    
    # 脚本目录
    SCRIPTS_DIR = os.path.join(
        os.path.dirname(__file__),
        '../../scripts'
    )
    
    # 内存中的运行状态
    _run_states: Dict[str, SimulationRunState] = {}
    _processes: Dict[str, subprocess.Popen] = {}
    _action_queues: Dict[str, Queue] = {}
    _monitor_threads: Dict[str, threading.Thread] = {}
    _stdout_files: Dict[str, Any] = {}  # 存储 stdout 文件句柄
    _stderr_files: Dict[str, Any] = {}  # 存储 stderr 文件句柄
    
    # 图谱记忆更新配置
    _graph_memory_enabled: Dict[str, bool] = {}  # simulation_id -> enabled
    _finalization_locks: Dict[str, threading.Lock] = {}
    _finalization_locks_guard = threading.Lock()
    _manual_stop_requests: set[str] = set()

    @classmethod
    def _finalization_lock(cls, simulation_id: str) -> threading.Lock:
        with cls._finalization_locks_guard:
            return cls._finalization_locks.setdefault(
                simulation_id, threading.Lock()
            )

    @classmethod
    def _sync_simulation_status(
        cls,
        simulation_id: str,
        runner_status: RunnerStatus,
        error: str | None = None,
    ) -> None:
        """Keep persisted simulation metadata aligned with run_state.json."""

        from .simulation_manager import SimulationManager, SimulationStatus

        status_map = {
            RunnerStatus.RUNNING: SimulationStatus.RUNNING,
            RunnerStatus.STOPPING: SimulationStatus.STOPPING,
            RunnerStatus.STOPPED: SimulationStatus.STOPPED,
            RunnerStatus.COMPLETED: SimulationStatus.COMPLETED,
            RunnerStatus.FAILED: SimulationStatus.FAILED,
        }
        status = status_map.get(runner_status)
        if status is None:
            return
        try:
            manager = SimulationManager()
            simulation = manager.get_simulation(simulation_id)
            if simulation is None:
                return
            simulation.status = status
            simulation.error = error
            manager._save_simulation_state(simulation)
        except Exception as sync_error:
            # state.json is a secondary projection. Never let a projection
            # failure skip the authoritative run-state finalization or graph
            # ingestion drain.
            logger.error(
                "同步模拟状态失败: simulation_id=%s, status=%s, error=%s",
                simulation_id,
                runner_status.value,
                sync_error,
            )
    
    @classmethod
    def get_run_state(cls, simulation_id: str) -> Optional[SimulationRunState]:
        """获取运行状态"""
        if simulation_id in cls._run_states:
            return cls._run_states[simulation_id]
        
        # 尝试从文件加载
        state = cls._load_run_state(simulation_id)
        if state:
            cls._run_states[simulation_id] = state
        return state
    
    @classmethod
    def _load_run_state(cls, simulation_id: str) -> Optional[SimulationRunState]:
        """从文件加载运行状态

        使用 read_json_tolerant 容忍缺失/空/被kill -9中途截断的
        run_state.json：宁可返回None（视为"无运行状态"）也不能让一次
        意外的部分写入使整个后端在读取状态时抛出异常。
        """
        state_file = os.path.join(cls.RUN_STATE_DIR, simulation_id, "run_state.json")
        data = read_json_tolerant(state_file, default=None)
        if data is None:
            return None

        try:
            state = SimulationRunState(
                simulation_id=simulation_id,
                runner_status=RunnerStatus(data.get("runner_status", "idle")),
                current_round=data.get("current_round", 0),
                total_rounds=data.get("total_rounds", 0),
                simulated_hours=data.get("simulated_hours", 0),
                total_simulation_hours=data.get("total_simulation_hours", 0),
                # 各平台独立轮次和时间
                twitter_current_round=data.get("twitter_current_round", 0),
                reddit_current_round=data.get("reddit_current_round", 0),
                twitter_simulated_hours=data.get("twitter_simulated_hours", 0),
                reddit_simulated_hours=data.get("reddit_simulated_hours", 0),
                twitter_running=data.get("twitter_running", False),
                reddit_running=data.get("reddit_running", False),
                twitter_completed=data.get("twitter_completed", False),
                reddit_completed=data.get("reddit_completed", False),
                twitter_actions_count=data.get("twitter_actions_count", 0),
                reddit_actions_count=data.get("reddit_actions_count", 0),
                started_at=data.get("started_at"),
                updated_at=data.get("updated_at", datetime.now().isoformat()),
                completed_at=data.get("completed_at"),
                error=data.get("error"),
                process_pid=data.get("process_pid"),
                process_started_epoch=data.get("process_started_epoch"),
                graph_memory_enabled=data.get("graph_memory_enabled", False),
                graph_ingestion_complete=data.get("graph_ingestion_complete", True),
                manual_stop_requested=data.get("manual_stop_requested", False),
                last_round_advance_at=data.get("last_round_advance_at"),
                twitter_log_position=data.get("twitter_log_position", 0),
                reddit_log_position=data.get("reddit_log_position", 0),
            )
            
            # 加载最近动作
            actions_data = data.get("recent_actions", [])
            for a in actions_data:
                state.recent_actions.append(AgentAction(
                    round_num=a.get("round_num", 0),
                    timestamp=a.get("timestamp", ""),
                    platform=a.get("platform", ""),
                    agent_id=a.get("agent_id", 0),
                    agent_name=a.get("agent_name", ""),
                    action_type=a.get("action_type", ""),
                    action_args=a.get("action_args", {}),
                    result=a.get("result"),
                    success=a.get("success", True),
                ))
            
            return state
        except Exception as e:
            logger.error(f"加载运行状态失败: {str(e)}")
            return None
    
    @classmethod
    def _save_run_state(cls, state: SimulationRunState):
        """保存运行状态到文件

        使用atomic_write_json而不是直接open(...).write(...)：监控线程每
        ~2秒调用一次本方法，一次kill -9恰好落在写入中途就会把run_state.json
        截断成半个JSON文档（已在生产环境中真实发生过）。atomic_write_json
        写临时文件+fsync+os.replace，任何时刻打开该路径要么看到完整旧文件，
        要么看到完整新文件，绝不会看到半截内容。
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, state.simulation_id)
        state_file = os.path.join(sim_dir, "run_state.json")

        data = state.to_detail_dict()

        # run_state.json is routinely inspected/edited by operators and is
        # not a credentials file like settings_store.py's target, so keep it
        # at normal file permissions rather than atomic_write_json's default
        # 0600.
        atomic_write_json(state_file, data, mode=0o644)

        cls._run_states[state.simulation_id] = state
    
    @classmethod
    def start_simulation(
        cls,
        simulation_id: str,
        platform: str = "parallel",  # twitter / reddit / parallel
        max_rounds: int = None,  # 最大模拟轮数（可选，用于截断过长的模拟）
        enable_graph_memory_update: bool = False,  # 是否将活动更新到图谱
        graph_id: str = None,  # 图谱ID（启用图谱更新时必需）
        resume: bool = False,  # 是否从 round_checkpoint.json 记录的断点续跑
    ) -> SimulationRunState:
        """
        启动模拟
        
        Args:
            simulation_id: 模拟ID
            platform: 运行平台 (twitter/reddit/parallel)
            max_rounds: 最大模拟轮数（可选，用于截断过长的模拟）
            enable_graph_memory_update: 是否将Agent活动动态更新到图谱
            graph_id: 图谱ID（启用图谱更新时必需）
            resume: 是否从 round_checkpoint.json 记录的断点续跑——只是给
                子进程命令行追加 --resume；真正的续跑校验（checkpoint是否
                存在、数据库schema是否完整等）全部在子进程脚本内部完成，
                本方法自己不做也不能做任何清理动作。调用方（见下方
                resume_simulation）必须保证这次调用之前从未对该
                simulation_id 调用过会清空运行产物的 cleanup_simulation_
                logs / clear_simulation_episodes——resume与强制重启互斥。

        Returns:
            SimulationRunState
        """
        # 加载模拟配置
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        config_path = os.path.join(sim_dir, "simulation_config.json")
        
        if not os.path.exists(config_path):
            raise ValueError(f"模拟配置不存在，请先调用 /prepare 接口")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 初始化运行状态
        time_config = config.get("time_config", {})
        total_hours = time_config.get("total_simulation_hours", 72)
        minutes_per_round = time_config.get("minutes_per_round", 30)
        total_rounds = int(total_hours * 60 / minutes_per_round)
        
        # 如果指定了最大轮数，则截断
        if max_rounds is not None and max_rounds > 0:
            original_rounds = total_rounds
            total_rounds = min(total_rounds, max_rounds)
            if total_rounds < original_rounds:
                logger.info(f"轮数已截断: {original_rounds} -> {total_rounds} (max_rounds={max_rounds})")
        
        state = SimulationRunState(
            simulation_id=simulation_id,
            runner_status=RunnerStatus.STARTING,
            total_rounds=total_rounds,
            total_simulation_hours=total_hours,
            started_at=datetime.now().isoformat(),
        )
        
        # Atomically claim this simulation ID. The expensive updater/process
        # startup happens after releasing the lock, while the persisted
        # STARTING state makes every concurrent start fail closed.
        with cls._finalization_lock(simulation_id):
            existing = cls.get_run_state(simulation_id)
            active_statuses = {
                RunnerStatus.STARTING,
                RunnerStatus.RUNNING,
                RunnerStatus.PAUSED,
                RunnerStatus.STOPPING,
            }
            if (
                existing and existing.runner_status in active_statuses
            ) or ZepGraphMemoryManager.get_updater(simulation_id) is not None:
                raise ValueError(f"模拟已在运行或结束处理中: {simulation_id}")
            cls._save_run_state(state)
        
        # 如果启用图谱记忆更新，创建更新器
        if enable_graph_memory_update:
            if not graph_id:
                raise ValueError("启用图谱记忆更新时必须提供 graph_id")
            
            try:
                ZepGraphMemoryManager.create_updater(simulation_id, graph_id)
                cls._graph_memory_enabled[simulation_id] = True
                logger.info(f"已启用图谱记忆更新: simulation_id={simulation_id}, graph_id={graph_id}")
            except Exception as e:
                logger.error(f"创建图谱记忆更新器失败: {e}")
                cls._graph_memory_enabled[simulation_id] = False
                state.runner_status = RunnerStatus.FAILED
                state.error = f"图谱更新器初始化失败: {e}"
                with cls._finalization_lock(simulation_id):
                    cls._save_run_state(state)
                    cls._sync_simulation_status(
                        simulation_id,
                        RunnerStatus.FAILED,
                        state.error,
                    )
                raise RuntimeError(state.error) from e
        else:
            cls._graph_memory_enabled[simulation_id] = False

        # Persist the graph-memory flag onto the durable state itself (not
        # just the in-memory dict) so a restart can still tell whether this
        # run owed a graph drain, and mark ingestion as not-yet-complete
        # while it is actually enabled -- stop_updater() flips it to True
        # only after a confirmed successful drain.
        state.graph_memory_enabled = cls._graph_memory_enabled.get(simulation_id, False)
        state.graph_ingestion_complete = not state.graph_memory_enabled

        # 确定运行哪个脚本（脚本位于 backend/scripts/ 目录）
        if platform == "twitter":
            script_name = "run_twitter_simulation.py"
            state.twitter_running = True
        elif platform == "reddit":
            script_name = "run_reddit_simulation.py"
            state.reddit_running = True
        else:
            script_name = "run_parallel_simulation.py"
            state.twitter_running = True
            state.reddit_running = True
        
        script_path = os.path.join(cls.SCRIPTS_DIR, script_name)
        
        if not os.path.exists(script_path):
            cleanup_error = None
            if cls._graph_memory_enabled.get(simulation_id, False):
                try:
                    ZepGraphMemoryManager.stop_updater(simulation_id)
                    cls._graph_memory_enabled.pop(simulation_id, None)
                except Exception as error:
                    cleanup_error = error
            state.runner_status = RunnerStatus.FAILED
            state.twitter_running = False
            state.reddit_running = False
            state.error = f"脚本不存在: {script_path}"
            if cleanup_error is not None:
                state.error += f"; 图谱写入清理失败: {cleanup_error}"
            with cls._finalization_lock(simulation_id):
                cls._save_run_state(state)
                cls._sync_simulation_status(
                    simulation_id,
                    RunnerStatus.FAILED,
                    state.error,
                )
            raise ValueError(state.error)
        
        # 创建动作队列
        action_queue = Queue()
        cls._action_queues[simulation_id] = action_queue

        process = None
        main_log_file = None

        # 启动模拟进程
        try:
            # 构建运行命令，使用完整路径
            # 新的日志结构：
            #   twitter/actions.jsonl - Twitter 动作日志
            #   reddit/actions.jsonl  - Reddit 动作日志
            #   simulation.log        - 主进程日志
            
            cmd = [
                sys.executable,  # Python解释器
                script_path,
                "--config", config_path,  # 使用完整配置文件路径
            ]
            
            # 如果指定了最大轮数，添加到命令行参数
            if max_rounds is not None and max_rounds > 0:
                cmd.extend(["--max-rounds", str(max_rounds)])

            # 断点续跑：--resume 让子进程从 round_checkpoint.json 记录的
            # round继续，跳过数据库删除和初始事件播种（详见
            # backend/scripts 下三个 run_*.py 脚本里对应的 --resume 实现）
            if resume:
                cmd.append("--resume")

            # 创建主日志文件，避免 stdout/stderr 管道缓冲区满导致进程阻塞
            main_log_path = os.path.join(sim_dir, "simulation.log")
            main_log_file = open(main_log_path, 'w', encoding='utf-8')
            
            # 设置子进程环境变量，确保 Windows 上使用 UTF-8 编码
            # 这可以修复第三方库（如 OASIS）读取文件时未指定编码的问题
            env = os.environ.copy()
            env['PYTHONUTF8'] = '1'  # Python 3.7+ 支持，让所有 open() 默认使用 UTF-8
            env['PYTHONIOENCODING'] = 'utf-8'  # 确保 stdout/stderr 使用 UTF-8
            
            # 设置工作目录为模拟目录（数据库等文件会生成在此）
            # 使用 start_new_session=True 创建新的进程组，确保可以通过 os.killpg 终止所有子进程
            process = subprocess.Popen(
                cmd,
                cwd=sim_dir,
                stdout=main_log_file,
                stderr=subprocess.STDOUT,  # stderr 也写入同一个文件
                text=True,
                encoding='utf-8',  # 显式指定编码
                bufsize=1,
                env=env,  # 传递带有 UTF-8 设置的环境变量
                start_new_session=True,  # 创建新进程组，确保服务器关闭时能终止所有相关进程
            )

            # Capture the OS-reported process start time right after spawn so
            # a later boot-time reconciliation can tell "this pid is still
            # our subprocess" apart from "the OS recycled this pid for an
            # unrelated process after a backend restart".
            try:
                process_started_epoch = psutil.Process(process.pid).create_time()
            except Exception as e:
                logger.warning(f"无法捕获进程启动时间: simulation_id={simulation_id}, error={e}")
                process_started_epoch = None

            # Capture locale before spawning monitor thread
            current_locale = get_locale()

            monitor_thread = threading.Thread(
                target=cls._monitor_simulation,
                args=(simulation_id, current_locale),
                daemon=True
            )

            # Atomically publish every resource needed by stop/finalization.
            # The monitor is registered before start; if it exits immediately,
            # it waits on the same lock until RUNNING is fully visible.
            with cls._finalization_lock(simulation_id):
                cls._stdout_files[simulation_id] = main_log_file
                cls._stderr_files[simulation_id] = None
                state.process_pid = process.pid
                state.process_started_epoch = process_started_epoch
                state.runner_status = RunnerStatus.RUNNING
                cls._processes[simulation_id] = process
                cls._monitor_threads[simulation_id] = monitor_thread
                cls._save_run_state(state)
                cls._sync_simulation_status(
                    simulation_id,
                    RunnerStatus.RUNNING,
                )
                monitor_thread.start()
            
            logger.info(f"模拟启动成功: {simulation_id}, pid={process.pid}, platform={platform}")
            
        except Exception as e:
            cleanup_errors = []
            if process is not None and process.poll() is None:
                try:
                    cls._terminate_process(process, simulation_id)
                except Exception as error:
                    cleanup_errors.append(f"子进程终止失败: {error}")
            cls._processes.pop(simulation_id, None)
            cls._monitor_threads.pop(simulation_id, None)
            cls._action_queues.pop(simulation_id, None)
            cls._stdout_files.pop(simulation_id, None)
            cls._stderr_files.pop(simulation_id, None)
            if main_log_file is not None:
                try:
                    main_log_file.close()
                except Exception as error:
                    cleanup_errors.append(f"日志关闭失败: {error}")
            if cls._graph_memory_enabled.get(simulation_id, False):
                try:
                    ZepGraphMemoryManager.stop_updater(simulation_id)
                    cls._graph_memory_enabled.pop(simulation_id, None)
                except Exception as error:
                    cleanup_errors.append(f"图谱写入清理失败: {error}")
            state.runner_status = RunnerStatus.FAILED
            state.twitter_running = False
            state.reddit_running = False
            state.error = str(e)
            if cleanup_errors:
                state.error += "; " + "; ".join(cleanup_errors)
            with cls._finalization_lock(simulation_id):
                cls._save_run_state(state)
                cls._sync_simulation_status(
                    simulation_id,
                    RunnerStatus.FAILED,
                    state.error,
                )
            raise

        return state

    @classmethod
    def resume_simulation(
        cls,
        simulation_id: str,
        platform: str = "parallel",  # twitter / reddit / parallel
        max_rounds: int = None,  # 最大模拟轮数（应与被中断的那次运行保持一致）
        enable_graph_memory_update: bool = False,
        graph_id: str = None,
    ) -> SimulationRunState:
        """
        从上一次中断的round断点续跑模拟（真正的round级断点续跑）。

        与 start_simulation 的关键区别、以及为什么"续跑"与"强制重新开始"
        必须互斥：
        - 本方法绝不调用、也不能间接触发 cleanup_simulation_logs 或
          ZepGraphMemoryManager.clear_simulation_episodes——那两个操作会
          删除 sqlite 数据库、actions.jsonl 和图谱摄取journal，而这些正
          是续跑所需要保留的状态。调用方（API路由）必须保证走的是这条
          独立的resume路径，而不是给 /start 传 force=true。
        - 只在命令行上给子进程追加 --resume（由 start_simulation 里的
          cmd构建逻辑处理），真正的续跑决策（要不要跳过数据库删除/初始
          事件播种、从哪个round开始、last_rowid恢复到多少）全部由子进程
          自己读取 <sim_dir>/round_checkpoint.json 决定——参见
          backend/scripts/run_parallel_simulation.py /
          run_twitter_simulation.py / run_reddit_simulation.py。
        - 在派生子进程之前先确认 round_checkpoint.json 存在，找不到就
          直接拒绝（抛 ValueError）：没有checkpoint说明这次运行从未完整
          跑完一轮，没有安全的续跑点——"拒绝续跑"远好过"悄悄从round 0
          重新开始并让调用方误以为衔接上了"。

        Args:
            simulation_id: 模拟ID
            platform: 运行平台 (twitter/reddit/parallel)，应与被中断的那
                次运行使用的平台一致——否则子进程会找不到对应平台的
                checkpoint记录而拒绝启动（见上）
            max_rounds: 最大模拟轮数（可选，应与原运行保持一致；轮数定义
                变化可能让checkpoint里的 next_round_index 语义与新一轮的
                total_rounds 对不上）
            enable_graph_memory_update / graph_id: 同 start_simulation

        Returns:
            SimulationRunState

        Raises:
            ValueError: 找不到该模拟目录，或该目录下没有有效的
                round_checkpoint.json 可供续跑
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        if not os.path.exists(sim_dir):
            raise ValueError(f"模拟目录不存在: {simulation_id}")

        checkpoint_path = os.path.join(sim_dir, "round_checkpoint.json")
        checkpoint_data = read_json_tolerant(checkpoint_path, default=None)
        if not isinstance(checkpoint_data, dict) or not (
            checkpoint_data.get("twitter") or checkpoint_data.get("reddit")
        ):
            raise ValueError(
                "未找到可用的 round_checkpoint.json，没有可以续跑的断点。"
                "请使用 /start（可选 force=true）重新开始这次模拟。"
            )

        logger.info(
            f"续跑模拟: simulation_id={simulation_id}, platform={platform}, "
            f"checkpoint={checkpoint_path}"
        )

        return cls.start_simulation(
            simulation_id=simulation_id,
            platform=platform,
            max_rounds=max_rounds,
            enable_graph_memory_update=enable_graph_memory_update,
            graph_id=graph_id,
            resume=True,
        )

    @classmethod
    def _monitor_simulation(cls, simulation_id: str, locale: str = 'zh'):
        """监控模拟进程，解析动作日志"""
        set_locale(locale)
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        
        # 新的日志结构：分平台的动作日志
        twitter_actions_log = os.path.join(sim_dir, "twitter", "actions.jsonl")
        reddit_actions_log = os.path.join(sim_dir, "reddit", "actions.jsonl")
        
        process = cls._processes.get(simulation_id)
        state = cls.get_run_state(simulation_id)
        
        if not process or not state:
            return
        
        # Resume from the persisted tail offsets rather than always 0. In the
        # current single-continuous-monitor-thread lifecycle this is a no-op
        # (a freshly started run's state always begins at 0), but it keeps
        # the durable offsets honest as the source of truth instead of a
        # value only these locals know about.
        twitter_position = state.twitter_log_position
        reddit_position = state.reddit_log_position

        monitor_error: Exception | None = None
        exit_code: int | None = None
        try:
            while process.poll() is None:  # 进程仍在运行
                # 读取 Twitter 动作日志
                if os.path.exists(twitter_actions_log):
                    twitter_position = cls._read_action_log(
                        twitter_actions_log, twitter_position, state, "twitter"
                    )
                    state.twitter_log_position = twitter_position

                # 读取 Reddit 动作日志
                if os.path.exists(reddit_actions_log):
                    reddit_position = cls._read_action_log(
                        reddit_actions_log, reddit_position, state, "reddit"
                    )
                    state.reddit_log_position = reddit_position

                # 更新状态
                cls._save_run_state(state)
                time.sleep(2)

            # 进程结束后，最后读取一次日志
            if os.path.exists(twitter_actions_log):
                twitter_position = cls._read_action_log(twitter_actions_log, twitter_position, state, "twitter")
                state.twitter_log_position = twitter_position
            if os.path.exists(reddit_actions_log):
                reddit_position = cls._read_action_log(reddit_actions_log, reddit_position, state, "reddit")
                state.reddit_log_position = reddit_position

            exit_code = process.returncode
            
        except Exception as e:
            logger.error(f"监控线程异常: {simulation_id}, error={str(e)}")
            monitor_error = e
        
        finally:
            # Manual stop and natural completion can observe the same process
            # exit. Serialize terminal state and updater drain so only one path
            # owns the final result.
            with cls._finalization_lock(simulation_id):
                latest_state = cls.get_run_state(simulation_id)
                if latest_state is not None:
                    state = latest_state

                if state.runner_status not in {
                    RunnerStatus.STOPPED,
                    RunnerStatus.FAILED,
                }:
                    manual_stop = simulation_id in cls._manual_stop_requests
                    # Keep the durable field in lockstep with the in-memory
                    # set it mirrors, so a later restart-recovery finalization
                    # (which has no in-memory set to consult) can still tell
                    # a deliberate stop apart from an involuntary one.
                    state.manual_stop_requested = manual_stop
                    desired_status = (
                        RunnerStatus.STOPPED
                        if manual_stop
                        else RunnerStatus.COMPLETED
                    )
                    error_message = None
                    if not manual_stop and monitor_error is not None:
                        desired_status = RunnerStatus.FAILED
                        error_message = str(monitor_error)
                    elif not manual_stop and exit_code != 0:
                        desired_status = RunnerStatus.FAILED
                        main_log_path = os.path.join(sim_dir, "simulation.log")
                        error_info = ""
                        try:
                            if os.path.exists(main_log_path):
                                with open(main_log_path, 'r', encoding='utf-8') as f:
                                    error_info = f.read()[-2000:]
                        except Exception:
                            pass
                        error_message = (
                            f"进程退出码: {exit_code}, 错误: {error_info}"
                        )

                    state.twitter_running = False
                    state.reddit_running = False

                    if cls._graph_memory_enabled.get(simulation_id, False):
                        # STOPPING is a non-terminal ingestion barrier. The UI
                        # and report API must not observe COMPLETED until every
                        # accepted episode is durably written to the graph.
                        state.runner_status = RunnerStatus.STOPPING
                        cls._save_run_state(state)
                        cls._sync_simulation_status(
                            simulation_id,
                            RunnerStatus.STOPPING,
                        )
                        try:
                            ZepGraphMemoryManager.stop_updater(simulation_id)
                            cls._graph_memory_enabled.pop(simulation_id, None)
                            state.graph_ingestion_complete = True
                            logger.info(
                                "已停止图谱记忆更新: simulation_id=%s",
                                simulation_id,
                            )
                        except Exception as error:
                            logger.error(f"停止图谱记忆更新器失败: {error}")
                            desired_status = RunnerStatus.FAILED
                            error_message = f"图谱写入未完整完成: {error}"

                    state.runner_status = desired_status
                    state.error = error_message
                    state.completed_at = datetime.now().isoformat()
                    cls._save_run_state(state)
                    cls._sync_simulation_status(
                        simulation_id,
                        desired_status,
                        error_message,
                    )
                    if desired_status == RunnerStatus.COMPLETED:
                        logger.info(f"模拟完成: {simulation_id}")
                    else:
                        logger.error(f"模拟失败: {simulation_id}, error={state.error}")
                cls._manual_stop_requests.discard(simulation_id)
            
            # 清理进程资源
            cls._processes.pop(simulation_id, None)
            cls._action_queues.pop(simulation_id, None)
            cls._monitor_threads.pop(simulation_id, None)
            
            # 关闭日志文件句柄
            if simulation_id in cls._stdout_files:
                try:
                    cls._stdout_files[simulation_id].close()
                except Exception:
                    pass
                cls._stdout_files.pop(simulation_id, None)
            if simulation_id in cls._stderr_files and cls._stderr_files[simulation_id]:
                try:
                    cls._stderr_files[simulation_id].close()
                except Exception:
                    pass
                cls._stderr_files.pop(simulation_id, None)
    
    @classmethod
    def _read_action_log(
        cls, 
        log_path: str, 
        position: int, 
        state: SimulationRunState,
        platform: str
    ) -> int:
        """
        读取动作日志文件
        
        Args:
            log_path: 日志文件路径
            position: 上次读取位置
            state: 运行状态对象
            platform: 平台名称 (twitter/reddit)
            
        Returns:
            新的读取位置
        """
        # 检查是否启用了图谱记忆更新
        graph_memory_enabled = cls._graph_memory_enabled.get(state.simulation_id, False)
        graph_updater = None
        if graph_memory_enabled:
            graph_updater = ZepGraphMemoryManager.get_updater(state.simulation_id)
        
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                f.seek(position)
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            action_data = json.loads(line)
                            
                            # 处理事件类型的条目
                            if "event_type" in action_data:
                                event_type = action_data.get("event_type")
                                
                                # 检测 simulation_end 事件，标记平台已完成
                                if event_type == "simulation_end":
                                    if platform == "twitter":
                                        state.twitter_completed = True
                                        state.twitter_running = False
                                        logger.info(f"Twitter 模拟已完成: {state.simulation_id}, total_rounds={action_data.get('total_rounds')}, total_actions={action_data.get('total_actions')}")
                                    elif platform == "reddit":
                                        state.reddit_completed = True
                                        state.reddit_running = False
                                        logger.info(f"Reddit 模拟已完成: {state.simulation_id}, total_rounds={action_data.get('total_rounds')}, total_actions={action_data.get('total_actions')}")
                                    
                                    # 检查是否所有启用的平台都已完成
                                    # 如果只运行了一个平台，只检查那个平台
                                    # 如果运行了两个平台，需要两个都完成
                                    all_completed = cls._check_all_platforms_completed(state)
                                    if all_completed:
                                        # Platform completion is only an input
                                        # signal. The monitor publishes the
                                        # terminal status after the process has
                                        # exited and graph ingestion has drained.
                                        logger.info(
                                            f"所有平台已结束，等待进程与图谱写入完成: "
                                            f"{state.simulation_id}"
                                        )
                                
                                # 更新轮次信息（从 round_end 事件）
                                elif event_type == "round_end":
                                    round_num = action_data.get("round", 0)
                                    simulated_hours = action_data.get("simulated_hours", 0)
                                    # 真实前进信号：只在current_round/*_current_round
                                    # 实际增加时才刷新，绝不能用每次monitor tick都会
                                    # 更新的updated_at代替（否则停滞检测形同虚设）
                                    advanced = False

                                    # 更新各平台独立的轮次和时间
                                    if platform == "twitter":
                                        if round_num > state.twitter_current_round:
                                            state.twitter_current_round = round_num
                                            advanced = True
                                        state.twitter_simulated_hours = simulated_hours
                                    elif platform == "reddit":
                                        if round_num > state.reddit_current_round:
                                            state.reddit_current_round = round_num
                                            advanced = True
                                        state.reddit_simulated_hours = simulated_hours

                                    # 总体轮次取两个平台的最大值
                                    if round_num > state.current_round:
                                        state.current_round = round_num
                                        advanced = True
                                    # 总体时间取两个平台的最大值
                                    state.simulated_hours = max(state.twitter_simulated_hours, state.reddit_simulated_hours)

                                    if advanced:
                                        state.last_round_advance_at = datetime.now().isoformat()
                                
                                continue
                            
                            action = AgentAction(
                                round_num=action_data.get("round", 0),
                                timestamp=action_data.get("timestamp", datetime.now().isoformat()),
                                platform=platform,
                                agent_id=action_data.get("agent_id", 0),
                                agent_name=action_data.get("agent_name", ""),
                                action_type=action_data.get("action_type", ""),
                                action_args=action_data.get("action_args", {}),
                                result=action_data.get("result"),
                                success=action_data.get("success", True),
                            )
                            state.add_action(action)
                            
                            # 更新轮次
                            if action.round_num and action.round_num > state.current_round:
                                state.current_round = action.round_num
                            
                            # 如果启用了图谱记忆更新，将活动发送到图谱
                            if graph_updater:
                                graph_updater.add_activity_from_dict(action_data, platform)
                            
                        except json.JSONDecodeError:
                            pass
                return f.tell()
        except Exception as e:
            logger.warning(f"读取动作日志失败: {log_path}, error={e}")
            return position
    
    @classmethod
    def _check_all_platforms_completed(cls, state: SimulationRunState) -> bool:
        """
        检查所有启用的平台是否都已完成模拟
        
        通过检查对应的 actions.jsonl 文件是否存在来判断平台是否被启用
        
        Returns:
            True 如果所有启用的平台都已完成
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, state.simulation_id)
        twitter_log = os.path.join(sim_dir, "twitter", "actions.jsonl")
        reddit_log = os.path.join(sim_dir, "reddit", "actions.jsonl")
        
        # 检查哪些平台被启用（通过文件是否存在判断）
        twitter_enabled = os.path.exists(twitter_log)
        reddit_enabled = os.path.exists(reddit_log)
        
        # 如果平台被启用但未完成，则返回 False
        if twitter_enabled and not state.twitter_completed:
            return False
        if reddit_enabled and not state.reddit_completed:
            return False
        
        # 至少有一个平台被启用且已完成
        return twitter_enabled or reddit_enabled
    
    @classmethod
    def _terminate_process(cls, process: subprocess.Popen, simulation_id: str, timeout: int = 10):
        """
        跨平台终止进程及其子进程
        
        Args:
            process: 要终止的进程
            simulation_id: 模拟ID（用于日志）
            timeout: 等待进程退出的超时时间（秒）
        """
        if IS_WINDOWS:
            # Windows: 使用 taskkill 命令终止进程树
            # /F = 强制终止, /T = 终止进程树（包括子进程）
            logger.info(f"终止进程树 (Windows): simulation={simulation_id}, pid={process.pid}")
            try:
                # 先尝试优雅终止
                subprocess.run(
                    ['taskkill', '/PID', str(process.pid), '/T'],
                    capture_output=True,
                    timeout=5
                )
                try:
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    # 强制终止
                    logger.warning(f"进程未响应，强制终止: {simulation_id}")
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(process.pid), '/T'],
                        capture_output=True,
                        timeout=5
                    )
                    process.wait(timeout=5)
            except Exception as e:
                logger.warning(f"taskkill 失败，尝试 terminate: {e}")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
        else:
            # Unix: 使用进程组终止
            # 由于使用了 start_new_session=True，进程组 ID 等于主进程 PID
            pgid = os.getpgid(process.pid)
            logger.info(f"终止进程组 (Unix): simulation={simulation_id}, pgid={pgid}")
            
            # 先发送 SIGTERM 给整个进程组
            os.killpg(pgid, signal.SIGTERM)
            
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # 如果超时后还没结束，强制发送 SIGKILL
                logger.warning(f"进程组未响应 SIGTERM，强制终止: {simulation_id}")
                os.killpg(pgid, signal.SIGKILL)
                process.wait(timeout=5)

    @classmethod
    def _terminate_orphaned_pid(
        cls, pid: int, simulation_id: str, timeout: int = 10
    ) -> None:
        """
        终止一个只有PID、没有Popen句柄的孤儿进程（后端重启后原Popen对象已丢失）

        与 _terminate_process 使用相同的两阶段 SIGTERM -> SIGKILL 逻辑（以及
        Windows 下的 taskkill 分支），但由于这里的进程并非当前Python进程的
        子进程（重启后已被 init/launchd 收养），Popen.wait()/os.waitpid() 会
        抛出 ECHILD，因此改用 psutil.pid_exists 轮询判断其是否已退出。

        Args:
            pid: 要终止的进程ID
            simulation_id: 模拟ID（用于日志）
            timeout: 等待进程退出的超时时间（秒）
        """
        if not psutil.pid_exists(pid):
            logger.info(
                f"孤儿进程已不存在，无需终止: simulation={simulation_id}, pid={pid}"
            )
            return

        def _wait_gone(deadline: float) -> bool:
            while time.time() < deadline:
                if not psutil.pid_exists(pid):
                    return True
                time.sleep(0.2)
            return not psutil.pid_exists(pid)

        if IS_WINDOWS:
            logger.info(f"终止孤儿进程树 (Windows): simulation={simulation_id}, pid={pid}")
            try:
                subprocess.run(
                    ['taskkill', '/PID', str(pid), '/T'],
                    capture_output=True,
                    timeout=5
                )
                if not _wait_gone(time.time() + timeout):
                    logger.warning(f"孤儿进程未响应，强制终止: {simulation_id}")
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(pid), '/T'],
                        capture_output=True,
                        timeout=5
                    )
                    _wait_gone(time.time() + 5)
            except Exception as e:
                logger.warning(f"taskkill 失败: {simulation_id}, error={e}")
        else:
            # 由于原进程使用了 start_new_session=True，进程组 ID 等于主进程 PID，
            # 这一点在重启后依然成立（进程组ID不随收养而改变）。
            try:
                pgid = os.getpgid(pid)
            except ProcessLookupError:
                logger.info(
                    f"孤儿进程在终止前已退出: simulation={simulation_id}, pid={pid}"
                )
                return

            logger.info(
                f"终止孤儿进程组 (Unix): simulation={simulation_id}, pgid={pgid}"
            )
            try:
                os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                # ProcessLookupError: the group is already gone. PermissionError
                # is the observed behavior on some platforms when the group
                # leader is already a zombie awaiting reap (e.g. init/launchd
                # hasn't reaped it yet) -- the group is effectively already
                # gone from a signaling standpoint either way.
                return

            if not _wait_gone(time.time() + timeout):
                logger.warning(f"孤儿进程组未响应 SIGTERM，强制终止: {simulation_id}")
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    return
                _wait_gone(time.time() + 5)

    @classmethod
    def _is_same_process(
        cls,
        pid: Optional[int],
        process_started_epoch: Optional[float],
        simulation_id: str,
    ) -> bool:
        """
        PID重用安全的存活性判断

        仅凭 psutil.pid_exists(pid) 是不够的：操作系统会回收PID，一份长期
        未处理的过期 run_state.json 里记录的pid，此刻完全可能已经属于一个
        无关进程。用 create_time()（启动进程后立即捕获并持久化为
        process_started_epoch）加上 cmdline 中是否包含 simulation_id（每次
        启动的命令行都带有 --config <sim_dir>/simulation_config.json，
        sim_dir 以 simulation_id 命名）双重验证后，才能确认这个pid仍然是
        "我们的"进程。

        Args:
            pid: 持久化的进程ID
            process_started_epoch: 持久化的进程启动时间（epoch秒），可能为
                None（早于该字段引入的历史run_state.json）
            simulation_id: 模拟ID

        Returns:
            True 表示该pid大概率仍是本次模拟的进程且存活
        """
        if not pid:
            return False
        try:
            if not psutil.pid_exists(pid):
                return False
            proc = psutil.Process(pid)

            if process_started_epoch is not None:
                try:
                    actual_create_time = proc.create_time()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    return False
                # 不同平台/psutil版本下浮点精度可能有细微差异，允许小容差
                if abs(actual_create_time - process_started_epoch) > 2.0:
                    return False

            try:
                cmdline = proc.cmdline()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                return False
            if not any(simulation_id in part for part in cmdline):
                return False

            return True
        except psutil.NoSuchProcess:
            return False

    @classmethod
    def stop_simulation(cls, simulation_id: str) -> SimulationRunState:
        """停止模拟"""
        with cls._finalization_lock(simulation_id):
            state = cls.get_run_state(simulation_id)
            if not state:
                raise ValueError(f"模拟不存在: {simulation_id}")
            if state.runner_status == RunnerStatus.STOPPED:
                return state

            pending_updater = ZepGraphMemoryManager.get_updater(simulation_id)
            retrying_finalization = (
                pending_updater is not None
                and state.runner_status in {
                    RunnerStatus.STOPPING,
                    RunnerStatus.FAILED,
                }
            )
            if (
                state.runner_status not in [
                    RunnerStatus.STARTING,
                    RunnerStatus.RUNNING,
                    RunnerStatus.PAUSED,
                    RunnerStatus.STOPPING,
                ]
                and not retrying_finalization
            ):
                raise ValueError(
                    f"模拟未在运行: {simulation_id}, status={state.runner_status}"
                )

            state.runner_status = RunnerStatus.STOPPING
            cls._manual_stop_requests.add(simulation_id)
            state.manual_stop_requested = True
            cls._save_run_state(state)
            cls._sync_simulation_status(simulation_id, RunnerStatus.STOPPING)

            # 终止进程
            process = cls._processes.get(simulation_id)
            if process and process.poll() is None:
                try:
                    cls._terminate_process(process, simulation_id)
                except ProcessLookupError:
                    pass
                except Exception as e:
                    logger.error(f"终止进程组失败: {simulation_id}, error={e}")
                    try:
                        process.terminate()
                        process.wait(timeout=5)
                    except Exception:
                        process.kill()

        # Let the monitor consume the final action-log tail and own the single
        # updater drain. It will publish STOPPED (rather than COMPLETED) because
        # the manual-stop marker is set above.
        monitor = cls._monitor_threads.get(simulation_id)
        if (
            not retrying_finalization
            and
            monitor is not None
            and monitor is not threading.current_thread()
            and monitor.is_alive()
        ):
            wait_timeout = max(
                30.0,
                ZEP_INGESTION_WAIT_TIMEOUT_SECONDS
                + GRAPHITI_QUERY_TIMEOUT_SECONDS
                + 5,
            )
            monitor.join(timeout=wait_timeout)
            if monitor.is_alive():
                # The monitor still owns finalization and may be inside one
                # bounded HTTP request. Do not block on or overwrite its lock;
                # leave the observable state as STOPPING and let polling expose
                # the eventual STOPPED/FAILED result.
                raise SimulationStopPending(
                    f"模拟仍在停止中，图谱写入未在 {wait_timeout:.0f}s 内完成"
                )
        else:
            # Restart recovery or tests may have no monitor thread. Complete
            # the same barrier synchronously in this request.
            cls._finalize_without_monitor(simulation_id, state)

        state = cls.get_run_state(simulation_id) or state
        if state.runner_status == RunnerStatus.FAILED:
            raise RuntimeError(state.error or "模拟停止失败")
        if state.runner_status != RunnerStatus.STOPPED:
            raise RuntimeError(
                f"模拟停止未达到终态: {simulation_id}, status={state.runner_status}"
            )

        logger.info(f"模拟已停止: {simulation_id}")
        return state

    @classmethod
    def _finalize_without_monitor(
        cls, simulation_id: str, state: SimulationRunState
    ) -> SimulationRunState:
        """
        在没有监控线程的情况下同步完成终态收尾

        用于两种场景：
        1. stop_simulation() 在本请求内检测到监控线程已不存在/已退出（原有行为）。
        2. reconcile_on_boot() 处理后端重启后遗留的孤儿运行状态 —— 此时
           _monitor_threads、_processes、_graph_memory_enabled 等所有内存中
           的注册表都是空的，唯一可信的是本次调用前已持久化在state中的字段。

        判断"是否启用了图谱记忆更新"时同时OR上 cls._graph_memory_enabled
        （原有的纯内存判断，重启后总是为空）和持久化的 state.graph_memory_enabled
        —— 前者保证既有测试/正常运行路径行为不变，后者保证重启后（内存字典
        必然为空）依然能读到真实值，不会把"重启丢失了记录"误判为"从未启用"。

        本方法也刻意不在这里调用 cls._manual_stop_requests.add(...) 或强制
        state.manual_stop_requested，而是直接读取调用方已经设置好的值 ——
        对于真实的用户停止请求，stop_simulation() 在获取本方法调用权之前
        已经把该字段置为True；对于reconcile_on_boot()的孤儿调解，字段保留
        重启前的真实值（多数情况下是False，因为用户从未主动停止过它），
        从而让STOPPED只用于真正的用户停止，其余一律诚实地标记为FAILED。

        Args:
            simulation_id: 模拟ID
            state: 调用方持有的运行状态（会被本方法就地更新并持久化）

        Returns:
            更新后的运行状态

        Raises:
            RuntimeError: 图谱写入排空失败时
        """
        with cls._finalization_lock(simulation_id):
            state = cls.get_run_state(simulation_id) or state

            # OR the in-memory flag with the durable one: normal (non-restart)
            # callers -- including existing tests that only ever populate
            # cls._graph_memory_enabled -- keep working exactly as before,
            # while a restart-recovery caller (whose in-memory dict is always
            # empty) still gets a correct answer from the persisted field.
            graph_memory_was_enabled = (
                cls._graph_memory_enabled.get(simulation_id, False)
                or state.graph_memory_enabled
            )

            if graph_memory_was_enabled:
                # Capture liveness *before* calling stop_updater (which pops
                # the registry entry on success): a restart leaves no
                # in-memory updater behind, so stop_updater() is a safe
                # no-op (it already tolerates "updater is None") but must not
                # be silently read as "ingestion confirmed complete".
                had_live_updater = (
                    ZepGraphMemoryManager.get_updater(simulation_id) is not None
                )
                try:
                    ZepGraphMemoryManager.stop_updater(simulation_id)
                    cls._graph_memory_enabled.pop(simulation_id, None)
                    if had_live_updater:
                        state.graph_ingestion_complete = True
                    else:
                        # Nothing was actually drained just now -- most likely
                        # a restart lost the in-memory updater. False is the
                        # honest answer; a restart's true completion state is
                        # unknowable, so never infer success from a no-op.
                        state.graph_ingestion_complete = False
                        logger.warning(
                            "图谱记忆更新器已启用但找不到存活实例，"
                            "无法确认写入是否完整完成: simulation_id=%s",
                            simulation_id,
                        )
                except Exception as error:
                    state.runner_status = RunnerStatus.FAILED
                    state.twitter_running = False
                    state.reddit_running = False
                    state.completed_at = datetime.now().isoformat()
                    state.error = f"图谱写入未完整完成: {error}"
                    cls._save_run_state(state)
                    cls._sync_simulation_status(
                        simulation_id,
                        RunnerStatus.FAILED,
                        state.error,
                    )
                    raise RuntimeError(state.error) from error

            desired_status = (
                RunnerStatus.STOPPED
                if state.manual_stop_requested
                else RunnerStatus.FAILED
            )
            if desired_status == RunnerStatus.STOPPED:
                state.error = None
            elif not state.error:
                state.error = (
                    "模拟在未收到用户停止请求的情况下终止运行"
                    "（很可能是后端重启导致进程成为孤儿），"
                    "已由停止收尾逻辑标记为失败，可视需要重新开始"
                )

            state.runner_status = desired_status
            state.twitter_running = False
            state.reddit_running = False
            state.completed_at = datetime.now().isoformat()
            cls._save_run_state(state)
            cls._sync_simulation_status(
                simulation_id,
                desired_status,
                state.error,
            )
            cls._manual_stop_requests.discard(simulation_id)

        return cls.get_run_state(simulation_id) or state

    @classmethod
    def _read_actions_from_file(
        cls,
        file_path: str,
        default_platform: Optional[str] = None,
        platform_filter: Optional[str] = None,
        agent_id: Optional[int] = None,
        round_num: Optional[int] = None
    ) -> List[AgentAction]:
        """
        从单个动作文件中读取动作
        
        Args:
            file_path: 动作日志文件路径
            default_platform: 默认平台（当动作记录中没有 platform 字段时使用）
            platform_filter: 过滤平台
            agent_id: 过滤 Agent ID
            round_num: 过滤轮次
        """
        if not os.path.exists(file_path):
            return []
        
        actions = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    
                    # 跳过非动作记录（如 simulation_start, round_start, round_end 等事件）
                    if "event_type" in data:
                        continue
                    
                    # 跳过没有 agent_id 的记录（非 Agent 动作）
                    if "agent_id" not in data:
                        continue
                    
                    # 获取平台：优先使用记录中的 platform，否则使用默认平台
                    record_platform = data.get("platform") or default_platform or ""
                    
                    # 过滤
                    if platform_filter and record_platform != platform_filter:
                        continue
                    if agent_id is not None and data.get("agent_id") != agent_id:
                        continue
                    if round_num is not None and data.get("round") != round_num:
                        continue
                    
                    actions.append(AgentAction(
                        round_num=data.get("round", 0),
                        timestamp=data.get("timestamp", ""),
                        platform=record_platform,
                        agent_id=data.get("agent_id", 0),
                        agent_name=data.get("agent_name", ""),
                        action_type=data.get("action_type", ""),
                        action_args=data.get("action_args", {}),
                        result=data.get("result"),
                        success=data.get("success", True),
                    ))
                    
                except json.JSONDecodeError:
                    continue
        
        return actions
    
    @classmethod
    def get_all_actions(
        cls,
        simulation_id: str,
        platform: Optional[str] = None,
        agent_id: Optional[int] = None,
        round_num: Optional[int] = None
    ) -> List[AgentAction]:
        """
        获取所有平台的完整动作历史（无分页限制）
        
        Args:
            simulation_id: 模拟ID
            platform: 过滤平台（twitter/reddit）
            agent_id: 过滤Agent
            round_num: 过滤轮次
            
        Returns:
            完整的动作列表（按时间戳排序，新的在前）
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        actions = []
        
        # 读取 Twitter 动作文件（根据文件路径自动设置 platform 为 twitter）
        twitter_actions_log = os.path.join(sim_dir, "twitter", "actions.jsonl")
        if not platform or platform == "twitter":
            actions.extend(cls._read_actions_from_file(
                twitter_actions_log,
                default_platform="twitter",  # 自动填充 platform 字段
                platform_filter=platform,
                agent_id=agent_id, 
                round_num=round_num
            ))
        
        # 读取 Reddit 动作文件（根据文件路径自动设置 platform 为 reddit）
        reddit_actions_log = os.path.join(sim_dir, "reddit", "actions.jsonl")
        if not platform or platform == "reddit":
            actions.extend(cls._read_actions_from_file(
                reddit_actions_log,
                default_platform="reddit",  # 自动填充 platform 字段
                platform_filter=platform,
                agent_id=agent_id,
                round_num=round_num
            ))
        
        # 如果分平台文件不存在，尝试读取旧的单一文件格式
        if not actions:
            actions_log = os.path.join(sim_dir, "actions.jsonl")
            actions = cls._read_actions_from_file(
                actions_log,
                default_platform=None,  # 旧格式文件中应该有 platform 字段
                platform_filter=platform,
                agent_id=agent_id,
                round_num=round_num
            )
        
        # 按时间戳排序（新的在前）
        actions.sort(key=lambda x: x.timestamp, reverse=True)
        
        return actions
    
    @classmethod
    def get_actions(
        cls,
        simulation_id: str,
        limit: int = 100,
        offset: int = 0,
        platform: Optional[str] = None,
        agent_id: Optional[int] = None,
        round_num: Optional[int] = None
    ) -> List[AgentAction]:
        """
        获取动作历史（带分页）
        
        Args:
            simulation_id: 模拟ID
            limit: 返回数量限制
            offset: 偏移量
            platform: 过滤平台
            agent_id: 过滤Agent
            round_num: 过滤轮次
            
        Returns:
            动作列表
        """
        actions = cls.get_all_actions(
            simulation_id=simulation_id,
            platform=platform,
            agent_id=agent_id,
            round_num=round_num
        )
        
        # 分页
        return actions[offset:offset + limit]
    
    @classmethod
    def get_timeline(
        cls,
        simulation_id: str,
        start_round: int = 0,
        end_round: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        获取模拟时间线（按轮次汇总）
        
        Args:
            simulation_id: 模拟ID
            start_round: 起始轮次
            end_round: 结束轮次
            
        Returns:
            每轮的汇总信息
        """
        actions = cls.get_actions(simulation_id, limit=10000)
        
        # 按轮次分组
        rounds: Dict[int, Dict[str, Any]] = {}
        
        for action in actions:
            round_num = action.round_num
            
            if round_num < start_round:
                continue
            if end_round is not None and round_num > end_round:
                continue
            
            if round_num not in rounds:
                rounds[round_num] = {
                    "round_num": round_num,
                    "twitter_actions": 0,
                    "reddit_actions": 0,
                    "active_agents": set(),
                    "action_types": {},
                    "first_action_time": action.timestamp,
                    "last_action_time": action.timestamp,
                }
            
            r = rounds[round_num]
            
            if action.platform == "twitter":
                r["twitter_actions"] += 1
            else:
                r["reddit_actions"] += 1
            
            r["active_agents"].add(action.agent_id)
            r["action_types"][action.action_type] = r["action_types"].get(action.action_type, 0) + 1
            r["last_action_time"] = action.timestamp
        
        # 转换为列表
        result = []
        for round_num in sorted(rounds.keys()):
            r = rounds[round_num]
            result.append({
                "round_num": round_num,
                "twitter_actions": r["twitter_actions"],
                "reddit_actions": r["reddit_actions"],
                "total_actions": r["twitter_actions"] + r["reddit_actions"],
                "active_agents_count": len(r["active_agents"]),
                "active_agents": list(r["active_agents"]),
                "action_types": r["action_types"],
                "first_action_time": r["first_action_time"],
                "last_action_time": r["last_action_time"],
            })
        
        return result
    
    @classmethod
    def get_agent_stats(cls, simulation_id: str) -> List[Dict[str, Any]]:
        """
        获取每个Agent的统计信息
        
        Returns:
            Agent统计列表
        """
        actions = cls.get_actions(simulation_id, limit=10000)
        
        agent_stats: Dict[int, Dict[str, Any]] = {}
        
        for action in actions:
            agent_id = action.agent_id
            
            if agent_id not in agent_stats:
                agent_stats[agent_id] = {
                    "agent_id": agent_id,
                    "agent_name": action.agent_name,
                    "total_actions": 0,
                    "twitter_actions": 0,
                    "reddit_actions": 0,
                    "action_types": {},
                    "first_action_time": action.timestamp,
                    "last_action_time": action.timestamp,
                }
            
            stats = agent_stats[agent_id]
            stats["total_actions"] += 1
            
            if action.platform == "twitter":
                stats["twitter_actions"] += 1
            else:
                stats["reddit_actions"] += 1
            
            stats["action_types"][action.action_type] = stats["action_types"].get(action.action_type, 0) + 1
            stats["last_action_time"] = action.timestamp
        
        # 按总动作数排序
        result = sorted(agent_stats.values(), key=lambda x: x["total_actions"], reverse=True)
        
        return result
    
    @classmethod
    def cleanup_simulation_logs(cls, simulation_id: str) -> Dict[str, Any]:
        """
        清理模拟的运行日志（用于强制重新开始模拟）
        
        会删除以下文件：
        - run_state.json
        - twitter/actions.jsonl
        - reddit/actions.jsonl
        - simulation.log
        - stdout.log / stderr.log
        - twitter_simulation.db（模拟数据库）
        - reddit_simulation.db（模拟数据库）
        - env_status.json（环境状态）
        
        注意：不会删除配置文件（simulation_config.json）和 profile 文件
        
        Args:
            simulation_id: 模拟ID
            
        Returns:
            清理结果信息
        """
        import shutil
        
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        
        if not os.path.exists(sim_dir):
            return {"success": True, "message": "模拟目录不存在，无需清理"}
        
        cleaned_files = []
        errors = []
        
        # 要删除的文件列表（包括数据库文件）
        files_to_delete = [
            "run_state.json",
            "simulation.log",
            "stdout.log",
            "stderr.log",
            "twitter_simulation.db",  # Twitter 平台数据库
            "reddit_simulation.db",   # Reddit 平台数据库
            "env_status.json",        # 环境状态文件
        ]
        
        # 要删除的目录列表（包含动作日志）
        dirs_to_clean = ["twitter", "reddit"]
        
        # 删除文件
        for filename in files_to_delete:
            file_path = os.path.join(sim_dir, filename)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    cleaned_files.append(filename)
                except Exception as e:
                    errors.append(f"删除 {filename} 失败: {str(e)}")
        
        # 清理平台目录中的动作日志
        for dir_name in dirs_to_clean:
            dir_path = os.path.join(sim_dir, dir_name)
            if os.path.exists(dir_path):
                actions_file = os.path.join(dir_path, "actions.jsonl")
                if os.path.exists(actions_file):
                    try:
                        os.remove(actions_file)
                        cleaned_files.append(f"{dir_name}/actions.jsonl")
                    except Exception as e:
                        errors.append(f"删除 {dir_name}/actions.jsonl 失败: {str(e)}")
        
        # 清理图谱写入日志（graph_ingestion/）
        #
        # This MUST be deleted together with actions.jsonl above. The journal's
        # cursor records a byte offset into actions.jsonl plus the next episode
        # sequence number; a force-restart truncates actions.jsonl back to
        # offset 0. If a stale cursor survives, it claims a large
        # `batched_offset`, and every genuinely-new batch of the restarted run
        # is silently skipped as "already batched" -- no error, just a graph
        # that quietly stops receiving the new run's activity. The sha256 check
        # in resume_ingestion is a secondary defence, not the guard.
        graph_ingestion_dir = os.path.join(sim_dir, "graph_ingestion")
        if os.path.exists(graph_ingestion_dir):
            try:
                shutil.rmtree(graph_ingestion_dir)
                cleaned_files.append("graph_ingestion/")
            except Exception as e:
                errors.append(f"删除 graph_ingestion/ 失败: {str(e)}")

        # 清理内存中的运行状态
        if simulation_id in cls._run_states:
            del cls._run_states[simulation_id]
        
        logger.info(f"清理模拟日志完成: {simulation_id}, 删除文件: {cleaned_files}")
        
        return {
            "success": len(errors) == 0,
            "cleaned_files": cleaned_files,
            "errors": errors if errors else None
        }
    
    # 防止重复清理的标志
    _cleanup_done = False
    
    @classmethod
    def cleanup_all_simulations(cls):
        """
        清理所有运行中的模拟进程
        
        在服务器关闭时调用，确保所有子进程被终止
        """
        # 防止重复清理
        if cls._cleanup_done:
            return
        cls._cleanup_done = True

        updater_ids = set(ZepGraphMemoryManager.get_simulation_ids())
        simulation_ids = sorted(
            set(cls._processes)
            | set(cls._graph_memory_enabled)
            | updater_ids
        )
        if not simulation_ids:
            return

        logger.info("正在安全完成所有模拟进程与图谱写入...")
        cleanup_failed = False

        # Each simulation follows the normal stop/finalization path: terminate
        # its producer, let the monitor consume the final action-log tail, and
        # only then drain the graph updater. This avoids dropping actions emitted during
        # SIGTERM handling.
        for simulation_id in simulation_ids:
            try:
                state = cls.get_run_state(simulation_id)
                updater = ZepGraphMemoryManager.get_updater(simulation_id)
                process = cls._processes.get(simulation_id)

                if state is None:
                    # Missing/corrupt state is exceptional, but retain the
                    # critical producer-before-consumer shutdown ordering.
                    if process is not None and process.poll() is None:
                        cls._terminate_process(process, simulation_id, timeout=5)
                    if updater is not None:
                        ZepGraphMemoryManager.stop_updater(simulation_id)
                    continue

                if updater is not None:
                    cls._graph_memory_enabled[simulation_id] = True
                    if state.runner_status in {
                        RunnerStatus.IDLE,
                        RunnerStatus.STOPPED,
                        RunnerStatus.COMPLETED,
                    }:
                        # A retained updater means the old terminal projection
                        # was premature. Restore the ingestion barrier first.
                        state.runner_status = RunnerStatus.STOPPING
                        cls._save_run_state(state)
                        cls._sync_simulation_status(
                            simulation_id,
                            RunnerStatus.STOPPING,
                        )

                needs_finalization = bool(
                    (process is not None and process.poll() is None)
                    or updater is not None
                    or state.runner_status in {
                        RunnerStatus.STARTING,
                        RunnerStatus.RUNNING,
                        RunnerStatus.PAUSED,
                        RunnerStatus.STOPPING,
                    }
                )
                if needs_finalization:
                    cls.stop_simulation(simulation_id)

                # A recovery path without a monitor does not run the monitor's
                # resource cleanup block. Release only successfully stopped
                # resources; FAILED/STOPPING resources remain retryable.
                latest = cls.get_run_state(simulation_id)
                if latest and latest.runner_status == RunnerStatus.STOPPED:
                    stopped_process = cls._processes.get(simulation_id)
                    if stopped_process is None or stopped_process.poll() is not None:
                        cls._processes.pop(simulation_id, None)
                        cls._action_queues.pop(simulation_id, None)
                        cls._monitor_threads.pop(simulation_id, None)
                        for file_map in (cls._stdout_files, cls._stderr_files):
                            file_handle = file_map.pop(simulation_id, None)
                            if file_handle:
                                try:
                                    file_handle.close()
                                except Exception:
                                    pass
            except Exception as error:
                cleanup_failed = True
                logger.error(
                    "清理模拟失败，保留状态以便重试: simulation_id=%s, error=%s",
                    simulation_id,
                    error,
                )

        if cleanup_failed:
            # Retained updaters and FAILED run states continue to block report
            # generation and graph deletion. Permit an explicit retry.
            cls._cleanup_done = False
            logger.error("部分模拟未安全完成清理")
        else:
            logger.info("模拟进程与图谱写入清理完成")
    
    @classmethod
    def register_cleanup(cls):
        """
        注册清理函数
        
        在 Flask 应用启动时调用，确保服务器关闭时清理所有模拟进程
        """
        global _cleanup_registered
        
        if _cleanup_registered:
            return
        
        # Flask debug 模式下，只在 reloader 子进程中注册清理（实际运行应用的进程）
        # WERKZEUG_RUN_MAIN=true 表示是 reloader 子进程
        # 如果不是 debug 模式，则没有这个环境变量，也需要注册
        is_reloader_process = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
        is_debug_mode = os.environ.get('FLASK_DEBUG') == '1' or os.environ.get('WERKZEUG_RUN_MAIN') is not None
        
        # 在 debug 模式下，只在 reloader 子进程中注册；非 debug 模式下始终注册
        if is_debug_mode and not is_reloader_process:
            _cleanup_registered = True  # 标记已注册，防止子进程再次尝试
            return
        
        # 保存原有的信号处理器
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)
        # SIGHUP 只在 Unix 系统存在（macOS/Linux），Windows 没有
        original_sighup = None
        has_sighup = hasattr(signal, 'SIGHUP')
        if has_sighup:
            original_sighup = signal.getsignal(signal.SIGHUP)
        
        def cleanup_handler(signum=None, frame=None):
            """信号处理器：先清理模拟进程，再调用原处理器"""
            # 只有在有进程需要清理时才打印日志
            if cls._processes or cls._graph_memory_enabled:
                logger.info(f"收到信号 {signum}，开始清理...")
            cls.cleanup_all_simulations()
            
            # 调用原有的信号处理器，让 Flask 正常退出
            if signum == signal.SIGINT and callable(original_sigint):
                original_sigint(signum, frame)
            elif signum == signal.SIGTERM and callable(original_sigterm):
                original_sigterm(signum, frame)
            elif has_sighup and signum == signal.SIGHUP:
                # SIGHUP: 终端关闭时发送
                if callable(original_sighup):
                    original_sighup(signum, frame)
                else:
                    # 默认行为：正常退出
                    sys.exit(0)
            else:
                # 如果原处理器不可调用（如 SIG_DFL），则使用默认行为
                raise KeyboardInterrupt
        
        # 注册 atexit 处理器（作为备用）
        atexit.register(cls.cleanup_all_simulations)
        
        # 注册信号处理器（仅在主线程中）
        try:
            # SIGTERM: kill 命令默认信号
            signal.signal(signal.SIGTERM, cleanup_handler)
            # SIGINT: Ctrl+C
            signal.signal(signal.SIGINT, cleanup_handler)
            # SIGHUP: 终端关闭（仅 Unix 系统）
            if has_sighup:
                signal.signal(signal.SIGHUP, cleanup_handler)
        except ValueError:
            # 不在主线程中，只能使用 atexit
            logger.warning("无法注册信号处理器（不在主线程），仅使用 atexit")
        
        _cleanup_registered = True

    @classmethod
    def reconcile_on_boot(cls) -> Dict[str, Any]:
        """
        启动自检：调解因后端重启而遗留为孤儿的模拟运行状态

        每一个正常追踪"正在运行"的注册表都是纯内存的
        （_processes/_monitor_threads/_graph_memory_enabled/
        _manual_stop_requests/_finalization_locks），后端一重启就全部清空。
        如果重启前恰好有模拟处于非终态（STARTING/RUNNING/PAUSED/STOPPING），
        它的子进程此刻要么已经变成脱离监控的孤儿进程（原Popen句柄已丢失，
        父进程也从本进程变成了init/launchd），要么本身也已经退出——但无论
        哪种情况，run_state.json都会永远停留在非终态，"生成报告"按钮也会
        永远被禁用（backend/app/api/report.py只接受终态）。

        本方法在应用启动时扫描 uploads/simulations/*/run_state.json：
        1. 跳过已经是终态（IDLE/STOPPED/COMPLETED/FAILED）的运行。
        2. 对每个非终态运行，用 _is_same_process 安全判断其持久化的pid是否
           仍然是"我们的"进程（而不是操作系统重用同一pid启动的无关进程）。
        3. 如果确实存活，终止它——设计上刻意选择终止而不是重新挂接监控线程：
           重启后原Popen句柄已经丢失，子进程也已被系统收养，
           Popen.wait()/os.waitpid() 都无法再用于该进程，
           一个"重新挂接"的监控线程只能靠轮询判断存活，永远拿不到真实退出码，
           这是一套语义完全不同的第二套监控路径。直接终止并诚实地标记为可恢复，
           能与另一位工程师正在开发的"按轮次恢复"逻辑自然衔接。
        4. 无论是否终止了存活进程，都通过 _finalize_without_monitor 将其
           调解为终态（STOPPED仅用于重启前已收到过停止请求的运行，其余为
           FAILED），并写日志说明每一步判断依据。

        本方法只在真正提供服务的进程中执行一次，由调用方（create_app）复用
        Flask reloader已有的判断逻辑负责去重，避免debug模式下的reloader
        父进程重复扫描、误杀刚由子进程启动的模拟。

        Returns:
            {"scanned": 已扫描目录数, "reconciled": 已调解数,
             "skipped_terminal": 跳过的终态数, "errors": [...]} 的统计字典
        """
        result: Dict[str, Any] = {
            "scanned": 0,
            "reconciled": 0,
            "skipped_terminal": 0,
            "errors": [],
        }

        if not os.path.isdir(cls.RUN_STATE_DIR):
            return result

        terminal_statuses = {
            RunnerStatus.IDLE,
            RunnerStatus.STOPPED,
            RunnerStatus.COMPLETED,
            RunnerStatus.FAILED,
        }

        try:
            simulation_ids = sorted(os.listdir(cls.RUN_STATE_DIR))
        except OSError as error:
            logger.error(f"启动自检: 无法列出模拟目录: {error}")
            return result

        for simulation_id in simulation_ids:
            sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
            if simulation_id.startswith('.') or not os.path.isdir(sim_dir):
                continue
            state_file = os.path.join(sim_dir, "run_state.json")
            if not os.path.exists(state_file):
                continue

            result["scanned"] += 1
            try:
                state = cls._load_run_state(simulation_id)
                if state is None:
                    # _load_run_state 已经容忍了缺失/损坏的文件并记录了日志；
                    # 没有足够可信的状态可供调解，跳过而不是猜测。
                    logger.warning(
                        "启动自检: 无法加载运行状态，跳过: simulation_id=%s",
                        simulation_id,
                    )
                    continue

                if state.runner_status in terminal_statuses:
                    result["skipped_terminal"] += 1
                    continue

                cls._run_states[simulation_id] = state

                pid = state.process_pid
                alive = bool(pid) and cls._is_same_process(
                    pid, state.process_started_epoch, simulation_id
                )

                if alive:
                    logger.warning(
                        "启动自检: 发现存活的孤儿模拟进程，正在终止: "
                        "simulation_id=%s, pid=%s, status=%s",
                        simulation_id, pid, state.runner_status.value,
                    )
                    cls._terminate_orphaned_pid(pid, simulation_id)
                    state.error = (
                        f"后端重启导致模拟进程成为孤儿（pid={pid}），"
                        "启动自检已将其终止并标记为可恢复"
                    )
                else:
                    logger.warning(
                        "启动自检: 模拟处于非终态但进程已不存在/不再是同一进程，"
                        "直接标记终态: simulation_id=%s, pid=%s, status=%s",
                        simulation_id, pid, state.runner_status.value,
                    )
                    state.error = (
                        f"后端重启后发现该模拟已无存活进程（pid={pid}）"
                    )

                cls._finalize_without_monitor(simulation_id, state)
                final_state = cls.get_run_state(simulation_id)
                logger.warning(
                    "启动自检: 模拟 %s 已调解为终态 %s",
                    simulation_id,
                    final_state.runner_status.value if final_state else "?",
                )
                result["reconciled"] += 1
            except Exception as error:
                logger.error(
                    "启动自检: 调解模拟失败，保留原状态以便重试: "
                    "simulation_id=%s, error=%s",
                    simulation_id, error,
                )
                result["errors"].append(
                    {"simulation_id": simulation_id, "error": str(error)}
                )

        logger.info(
            "模拟运行状态启动自检完成: scanned=%s, reconciled=%s, "
            "skipped_terminal=%s, errors=%s",
            result["scanned"], result["reconciled"],
            result["skipped_terminal"], len(result["errors"]),
        )
        return result

    @classmethod
    def get_running_simulations(cls) -> List[str]:
        """
        获取所有正在运行的模拟ID列表
        """
        running = []
        for sim_id, process in cls._processes.items():
            if process.poll() is None:
                running.append(sim_id)
        return running
    
    # ============== Interview 功能 ==============
    
    @classmethod
    def check_env_alive(cls, simulation_id: str) -> bool:
        """
        检查模拟环境是否存活（可以接收Interview命令）

        Args:
            simulation_id: 模拟ID

        Returns:
            True 表示环境存活，False 表示环境已关闭
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        if not os.path.exists(sim_dir):
            return False

        ipc_client = SimulationIPCClient(sim_dir)
        return ipc_client.check_env_alive()

    @classmethod
    def get_env_status_detail(cls, simulation_id: str) -> Dict[str, Any]:
        """
        获取模拟环境的详细状态信息

        Args:
            simulation_id: 模拟ID

        Returns:
            状态详情字典，包含 status, twitter_available, reddit_available, timestamp
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        status_file = os.path.join(sim_dir, "env_status.json")
        
        default_status = {
            "status": "stopped",
            "twitter_available": False,
            "reddit_available": False,
            "timestamp": None
        }
        
        if not os.path.exists(status_file):
            return default_status
        
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                status = json.load(f)
            return {
                "status": status.get("status", "stopped"),
                "twitter_available": status.get("twitter_available", False),
                "reddit_available": status.get("reddit_available", False),
                "timestamp": status.get("timestamp")
            }
        except (json.JSONDecodeError, OSError):
            return default_status

    @classmethod
    def interview_agent(
        cls,
        simulation_id: str,
        agent_id: int,
        prompt: str,
        platform: str = None,
        timeout: float = 60.0
    ) -> Dict[str, Any]:
        """
        采访单个Agent

        Args:
            simulation_id: 模拟ID
            agent_id: Agent ID
            prompt: 采访问题
            platform: 指定平台（可选）
                - "twitter": 只采访Twitter平台
                - "reddit": 只采访Reddit平台
                - None: 双平台模拟时同时采访两个平台，返回整合结果
            timeout: 超时时间（秒）

        Returns:
            采访结果字典

        Raises:
            ValueError: 模拟不存在或环境未运行
            TimeoutError: 等待响应超时
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        if not os.path.exists(sim_dir):
            raise ValueError(f"模拟不存在: {simulation_id}")

        ipc_client = SimulationIPCClient(sim_dir)

        if not ipc_client.check_env_alive():
            raise ValueError(f"模拟环境未运行或已关闭，无法执行Interview: {simulation_id}")

        logger.info(f"发送Interview命令: simulation_id={simulation_id}, agent_id={agent_id}, platform={platform}")

        response = ipc_client.send_interview(
            agent_id=agent_id,
            prompt=prompt,
            platform=platform,
            timeout=timeout
        )

        if response.status.value == "completed":
            return {
                "success": True,
                "agent_id": agent_id,
                "prompt": prompt,
                "result": response.result,
                "timestamp": response.timestamp
            }
        else:
            return {
                "success": False,
                "agent_id": agent_id,
                "prompt": prompt,
                "error": response.error,
                "timestamp": response.timestamp
            }
    
    @classmethod
    def interview_agents_batch(
        cls,
        simulation_id: str,
        interviews: List[Dict[str, Any]],
        platform: str = None,
        timeout: float = 120.0
    ) -> Dict[str, Any]:
        """
        批量采访多个Agent

        Args:
            simulation_id: 模拟ID
            interviews: 采访列表，每个元素包含 {"agent_id": int, "prompt": str, "platform": str(可选)}
            platform: 默认平台（可选，会被每个采访项的platform覆盖）
                - "twitter": 默认只采访Twitter平台
                - "reddit": 默认只采访Reddit平台
                - None: 双平台模拟时每个Agent同时采访两个平台
            timeout: 超时时间（秒）

        Returns:
            批量采访结果字典

        Raises:
            ValueError: 模拟不存在或环境未运行
            TimeoutError: 等待响应超时
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        if not os.path.exists(sim_dir):
            raise ValueError(f"模拟不存在: {simulation_id}")

        ipc_client = SimulationIPCClient(sim_dir)

        if not ipc_client.check_env_alive():
            raise ValueError(f"模拟环境未运行或已关闭，无法执行Interview: {simulation_id}")

        logger.info(f"发送批量Interview命令: simulation_id={simulation_id}, count={len(interviews)}, platform={platform}")

        response = ipc_client.send_batch_interview(
            interviews=interviews,
            platform=platform,
            timeout=timeout
        )

        if response.status.value == "completed":
            return {
                "success": True,
                "interviews_count": len(interviews),
                "result": response.result,
                "timestamp": response.timestamp
            }
        else:
            return {
                "success": False,
                "interviews_count": len(interviews),
                "error": response.error,
                "timestamp": response.timestamp
            }
    
    @classmethod
    def interview_all_agents(
        cls,
        simulation_id: str,
        prompt: str,
        platform: str = None,
        timeout: float = 180.0
    ) -> Dict[str, Any]:
        """
        采访所有Agent（全局采访）

        使用相同的问题采访模拟中的所有Agent

        Args:
            simulation_id: 模拟ID
            prompt: 采访问题（所有Agent使用相同问题）
            platform: 指定平台（可选）
                - "twitter": 只采访Twitter平台
                - "reddit": 只采访Reddit平台
                - None: 双平台模拟时每个Agent同时采访两个平台
            timeout: 超时时间（秒）

        Returns:
            全局采访结果字典
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        if not os.path.exists(sim_dir):
            raise ValueError(f"模拟不存在: {simulation_id}")

        # 从配置文件获取所有Agent信息
        config_path = os.path.join(sim_dir, "simulation_config.json")
        if not os.path.exists(config_path):
            raise ValueError(f"模拟配置不存在: {simulation_id}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        agent_configs = config.get("agent_configs", [])
        if not agent_configs:
            raise ValueError(f"模拟配置中没有Agent: {simulation_id}")

        # 构建批量采访列表
        interviews = []
        for agent_config in agent_configs:
            agent_id = agent_config.get("agent_id")
            if agent_id is not None:
                interviews.append({
                    "agent_id": agent_id,
                    "prompt": prompt
                })

        logger.info(f"发送全局Interview命令: simulation_id={simulation_id}, agent_count={len(interviews)}, platform={platform}")

        return cls.interview_agents_batch(
            simulation_id=simulation_id,
            interviews=interviews,
            platform=platform,
            timeout=timeout
        )
    
    @classmethod
    def close_simulation_env(
        cls,
        simulation_id: str,
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        """
        关闭模拟环境（而不是停止模拟进程）
        
        向模拟发送关闭环境命令，使其优雅退出等待命令模式
        
        Args:
            simulation_id: 模拟ID
            timeout: 超时时间（秒）
            
        Returns:
            操作结果字典
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        if not os.path.exists(sim_dir):
            raise ValueError(f"模拟不存在: {simulation_id}")
        
        ipc_client = SimulationIPCClient(sim_dir)
        
        if not ipc_client.check_env_alive():
            return {
                "success": True,
                "message": "环境已经关闭"
            }
        
        logger.info(f"发送关闭环境命令: simulation_id={simulation_id}")
        
        try:
            response = ipc_client.send_close_env(timeout=timeout)
            
            return {
                "success": response.status.value == "completed",
                "message": "环境关闭命令已发送",
                "result": response.result,
                "timestamp": response.timestamp
            }
        except TimeoutError:
            # 超时可能是因为环境正在关闭
            return {
                "success": True,
                "message": "环境关闭命令已发送（等待响应超时，环境可能正在关闭）"
            }
    
    @classmethod
    def _get_interview_history_from_db(
        cls,
        db_path: str,
        platform_name: str,
        agent_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """从单个数据库获取Interview历史"""
        import sqlite3
        
        if not os.path.exists(db_path):
            return []
        
        results = []
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            if agent_id is not None:
                cursor.execute("""
                    SELECT user_id, info, created_at
                    FROM trace
                    WHERE action = 'interview' AND user_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (agent_id, limit))
            else:
                cursor.execute("""
                    SELECT user_id, info, created_at
                    FROM trace
                    WHERE action = 'interview'
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (limit,))
            
            for user_id, info_json, created_at in cursor.fetchall():
                try:
                    info = json.loads(info_json) if info_json else {}
                except json.JSONDecodeError:
                    info = {"raw": info_json}
                
                results.append({
                    "agent_id": user_id,
                    "response": info.get("response", info),
                    "prompt": info.get("prompt", ""),
                    "timestamp": created_at,
                    "platform": platform_name
                })
            
            conn.close()
            
        except Exception as e:
            logger.error(f"读取Interview历史失败 ({platform_name}): {e}")
        
        return results

    @classmethod
    def get_interview_history(
        cls,
        simulation_id: str,
        platform: str = None,
        agent_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        获取Interview历史记录（从数据库读取）
        
        Args:
            simulation_id: 模拟ID
            platform: 平台类型（reddit/twitter/None）
                - "reddit": 只获取Reddit平台的历史
                - "twitter": 只获取Twitter平台的历史
                - None: 获取两个平台的所有历史
            agent_id: 指定Agent ID（可选，只获取该Agent的历史）
            limit: 每个平台返回数量限制
            
        Returns:
            Interview历史记录列表
        """
        sim_dir = os.path.join(cls.RUN_STATE_DIR, simulation_id)
        
        results = []
        
        # 确定要查询的平台
        if platform in ("reddit", "twitter"):
            platforms = [platform]
        else:
            # 不指定platform时，查询两个平台
            platforms = ["twitter", "reddit"]
        
        for p in platforms:
            db_path = os.path.join(sim_dir, f"{p}_simulation.db")
            platform_results = cls._get_interview_history_from_db(
                db_path=db_path,
                platform_name=p,
                agent_id=agent_id,
                limit=limit
            )
            results.extend(platform_results)
        
        # 按时间降序排序
        results.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        # 如果查询了多个平台，限制总数
        if len(platforms) > 1 and len(results) > limit:
            results = results[:limit]
        
        return results
