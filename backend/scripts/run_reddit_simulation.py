"""
OASIS Reddit模拟预设脚本
此脚本读取配置文件中的参数来执行模拟，实现全程自动化

功能特性:
- 完成模拟后不立即关闭环境，进入等待命令模式
- 支持通过IPC接收Interview命令
- 支持单个Agent采访和批量采访
- 支持远程关闭环境命令

使用方式:
    python run_reddit_simulation.py --config /path/to/simulation_config.json
    python run_reddit_simulation.py --config /path/to/simulation_config.json --no-wait  # 完成后立即关闭
"""

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

# 全局变量：用于信号处理
_shutdown_event = None
_cleanup_done = False

# 添加项目路径
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.abspath(os.path.join(_scripts_dir, '..'))
_project_root = os.path.abspath(os.path.join(_backend_dir, '..'))
sys.path.insert(0, _scripts_dir)
sys.path.insert(0, _backend_dir)

# 加载项目根目录的 .env 文件（包含 LLM_API_KEY 等配置）
from dotenv import load_dotenv
_env_file = os.path.join(_project_root, '.env')
if os.path.exists(_env_file):
    load_dotenv(_env_file)
else:
    _backend_env = os.path.join(_backend_dir, '.env')
    if os.path.exists(_backend_env):
        load_dotenv(_backend_env)


import re


class UnicodeFormatter(logging.Formatter):
    """自定义格式化器，将 Unicode 转义序列转换为可读字符"""
    
    UNICODE_ESCAPE_PATTERN = re.compile(r'\\u([0-9a-fA-F]{4})')
    
    def format(self, record):
        result = super().format(record)
        
        def replace_unicode(match):
            try:
                return chr(int(match.group(1), 16))
            except (ValueError, OverflowError):
                return match.group(0)
        
        return self.UNICODE_ESCAPE_PATTERN.sub(replace_unicode, result)


class MaxTokensWarningFilter(logging.Filter):
    """过滤掉 camel-ai 关于 max_tokens 的警告（我们故意不设置 max_tokens，让模型自行决定）"""
    
    def filter(self, record):
        # 过滤掉包含 max_tokens 警告的日志
        if "max_tokens" in record.getMessage() and "Invalid or missing" in record.getMessage():
            return False
        return True


# 在模块加载时立即添加过滤器，确保在 camel 代码执行前生效
logging.getLogger().addFilter(MaxTokensWarningFilter())


def setup_oasis_logging(log_dir: str):
    """配置 OASIS 的日志，使用固定名称的日志文件"""
    os.makedirs(log_dir, exist_ok=True)
    
    # 清理旧的日志文件
    for f in os.listdir(log_dir):
        old_log = os.path.join(log_dir, f)
        if os.path.isfile(old_log) and f.endswith('.log'):
            try:
                os.remove(old_log)
            except OSError:
                pass
    
    formatter = UnicodeFormatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
    
    loggers_config = {
        "social.agent": os.path.join(log_dir, "social.agent.log"),
        "social.twitter": os.path.join(log_dir, "social.twitter.log"),
        "social.rec": os.path.join(log_dir, "social.rec.log"),
        "oasis.env": os.path.join(log_dir, "oasis.env.log"),
        "table": os.path.join(log_dir, "table.log"),
    }
    
    for logger_name, log_file in loggers_config.items():
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.propagate = False


from action_logger import PlatformActionLogger
from action_db import get_agent_names_from_config, fetch_new_actions_from_db
from round_checkpoint import (
    CHECKPOINT_FILENAME,
    CHECKPOINT_SCHEMA_VERSION,
    load_resume_state,
    write_round_checkpoint,
    clear_checkpoint_platform_section,
    _db_schema_is_complete,
)
from credentials_reload import (
    CREDENTIALS_RELOAD_FILENAME,
    _apply_credentials_reload,
    _poll_and_apply_credentials_reload,
)

try:
    from camel.models import ModelFactory
    from camel.types import ModelPlatformType
    import oasis
    from oasis import (
        ActionType,
        LLMAction,
        ManualAction,
        Platform,
        generate_reddit_agent_graph
    )
    # Platform/Clock/Channel 组合用于续跑时的Reddit时钟连续性修复
    # （见 RedditSimulationRunner.run 中的说明）。oasis 顶层包没有重新
    # 导出 Clock 和 Channel，因此直接从子模块导入。
    from oasis.clock.clock import Clock
    from oasis.social_platform.channel import Channel
except ImportError as e:
    print(f"错误: 缺少依赖 {e}")
    print("请先安装: pip install oasis-ai camel-ai")
    sys.exit(1)


# IPC相关常量
IPC_COMMANDS_DIR = "ipc_commands"
IPC_RESPONSES_DIR = "ipc_responses"
ENV_STATUS_FILE = "env_status.json"

class CommandType:
    """命令类型常量"""
    INTERVIEW = "interview"
    BATCH_INTERVIEW = "batch_interview"
    CLOSE_ENV = "close_env"


class IPCHandler:
    """IPC命令处理器"""
    
    def __init__(self, simulation_dir: str, env, agent_graph):
        self.simulation_dir = simulation_dir
        self.env = env
        self.agent_graph = agent_graph
        self.commands_dir = os.path.join(simulation_dir, IPC_COMMANDS_DIR)
        self.responses_dir = os.path.join(simulation_dir, IPC_RESPONSES_DIR)
        self.status_file = os.path.join(simulation_dir, ENV_STATUS_FILE)
        self._running = True
        
        # 确保目录存在
        os.makedirs(self.commands_dir, exist_ok=True)
        os.makedirs(self.responses_dir, exist_ok=True)
    
    def update_status(self, status: str):
        """更新环境状态"""
        with open(self.status_file, 'w', encoding='utf-8') as f:
            json.dump({
                "status": status,
                "timestamp": datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)
    
    def poll_command(self) -> Optional[Dict[str, Any]]:
        """轮询获取待处理命令"""
        if not os.path.exists(self.commands_dir):
            return None
        
        # 获取命令文件（按时间排序）
        command_files = []
        for filename in os.listdir(self.commands_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.commands_dir, filename)
                command_files.append((filepath, os.path.getmtime(filepath)))
        
        command_files.sort(key=lambda x: x[1])
        
        for filepath, _ in command_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
        
        return None
    
    def send_response(self, command_id: str, status: str, result: Dict = None, error: str = None):
        """发送响应"""
        response = {
            "command_id": command_id,
            "status": status,
            "result": result,
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        
        response_file = os.path.join(self.responses_dir, f"{command_id}.json")
        with open(response_file, 'w', encoding='utf-8') as f:
            json.dump(response, f, ensure_ascii=False, indent=2)
        
        # 删除命令文件
        command_file = os.path.join(self.commands_dir, f"{command_id}.json")
        try:
            os.remove(command_file)
        except OSError:
            pass
    
    async def handle_interview(self, command_id: str, agent_id: int, prompt: str) -> bool:
        """
        处理单个Agent采访命令
        
        Returns:
            True 表示成功，False 表示失败
        """
        try:
            # 获取Agent
            agent = self.agent_graph.get_agent(agent_id)
            
            # 创建Interview动作
            interview_action = ManualAction(
                action_type=ActionType.INTERVIEW,
                action_args={"prompt": prompt}
            )
            
            # 执行Interview
            actions = {agent: interview_action}
            await self.env.step(actions)
            
            # 从数据库获取结果
            result = self._get_interview_result(agent_id)
            
            self.send_response(command_id, "completed", result=result)
            print(f"  Interview完成: agent_id={agent_id}")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"  Interview失败: agent_id={agent_id}, error={error_msg}")
            self.send_response(command_id, "failed", error=error_msg)
            return False
    
    async def handle_batch_interview(self, command_id: str, interviews: List[Dict]) -> bool:
        """
        处理批量采访命令
        
        Args:
            interviews: [{"agent_id": int, "prompt": str}, ...]
        """
        try:
            # 构建动作字典
            actions = {}
            agent_prompts = {}  # 记录每个agent的prompt
            
            for interview in interviews:
                agent_id = interview.get("agent_id")
                prompt = interview.get("prompt", "")
                
                try:
                    agent = self.agent_graph.get_agent(agent_id)
                    actions[agent] = ManualAction(
                        action_type=ActionType.INTERVIEW,
                        action_args={"prompt": prompt}
                    )
                    agent_prompts[agent_id] = prompt
                except Exception as e:
                    print(f"  警告: 无法获取Agent {agent_id}: {e}")
            
            if not actions:
                self.send_response(command_id, "failed", error="没有有效的Agent")
                return False
            
            # 执行批量Interview
            await self.env.step(actions)
            
            # 获取所有结果
            results = {}
            for agent_id in agent_prompts.keys():
                result = self._get_interview_result(agent_id)
                results[agent_id] = result
            
            self.send_response(command_id, "completed", result={
                "interviews_count": len(results),
                "results": results
            })
            print(f"  批量Interview完成: {len(results)} 个Agent")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"  批量Interview失败: {error_msg}")
            self.send_response(command_id, "failed", error=error_msg)
            return False
    
    def _get_interview_result(self, agent_id: int) -> Dict[str, Any]:
        """从数据库获取最新的Interview结果"""
        db_path = os.path.join(self.simulation_dir, "reddit_simulation.db")
        
        result = {
            "agent_id": agent_id,
            "response": None,
            "timestamp": None
        }
        
        if not os.path.exists(db_path):
            return result
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 查询最新的Interview记录
            cursor.execute("""
                SELECT user_id, info, created_at
                FROM trace
                WHERE action = ? AND user_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (ActionType.INTERVIEW.value, agent_id))
            
            row = cursor.fetchone()
            if row:
                user_id, info_json, created_at = row
                try:
                    info = json.loads(info_json) if info_json else {}
                    result["response"] = info.get("response", info)
                    result["timestamp"] = created_at
                except json.JSONDecodeError:
                    result["response"] = info_json
            
            conn.close()
            
        except Exception as e:
            print(f"  读取Interview结果失败: {e}")
        
        return result
    
    async def process_commands(self) -> bool:
        """
        处理所有待处理命令
        
        Returns:
            True 表示继续运行，False 表示应该退出
        """
        command = self.poll_command()
        if not command:
            return True
        
        command_id = command.get("command_id")
        command_type = command.get("command_type")
        args = command.get("args", {})
        
        print(f"\n收到IPC命令: {command_type}, id={command_id}")
        
        if command_type == CommandType.INTERVIEW:
            await self.handle_interview(
                command_id,
                args.get("agent_id", 0),
                args.get("prompt", "")
            )
            return True
            
        elif command_type == CommandType.BATCH_INTERVIEW:
            await self.handle_batch_interview(
                command_id,
                args.get("interviews", [])
            )
            return True
            
        elif command_type == CommandType.CLOSE_ENV:
            print("收到关闭环境命令")
            self.send_response(command_id, "completed", result={"message": "环境即将关闭"})
            return False
        
        else:
            self.send_response(command_id, "failed", error=f"未知命令类型: {command_type}")
            return True


class RedditSimulationRunner:
    """Reddit模拟运行器"""
    
    # Reddit可用动作（不包含INTERVIEW，INTERVIEW只能通过ManualAction手动触发）
    AVAILABLE_ACTIONS = [
        ActionType.LIKE_POST,
        ActionType.DISLIKE_POST,
        ActionType.CREATE_POST,
        ActionType.CREATE_COMMENT,
        ActionType.LIKE_COMMENT,
        ActionType.DISLIKE_COMMENT,
        ActionType.SEARCH_POSTS,
        ActionType.SEARCH_USER,
        ActionType.TREND,
        ActionType.REFRESH,
        ActionType.DO_NOTHING,
        ActionType.FOLLOW,
        ActionType.MUTE,
    ]
    
    def __init__(self, config_path: str, wait_for_commands: bool = True):
        """
        初始化模拟运行器
        
        Args:
            config_path: 配置文件路径 (simulation_config.json)
            wait_for_commands: 模拟完成后是否等待命令（默认True）
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.simulation_dir = os.path.dirname(config_path)
        self.wait_for_commands = wait_for_commands
        self.env = None
        self.agent_graph = None
        self.ipc_handler = None
        self.action_logger: Optional[PlatformActionLogger] = None
        self.agent_names: Dict[int, str] = {}
        # LLM凭证热重载：见模块顶部"LLM 凭证热重载"一节
        self._cred_reload_last_mtime: Optional[float] = None
        self._cred_reload_last_version: int = 0

    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _get_profile_path(self) -> str:
        """获取Profile文件路径"""
        return os.path.join(self.simulation_dir, "reddit_profiles.json")
    
    def _get_db_path(self) -> str:
        """获取数据库路径"""
        return os.path.join(self.simulation_dir, "reddit_simulation.db")
    
    def _create_model(self):
        """
        创建LLM模型
        
        统一使用项目根目录 .env 文件中的配置（优先级最高）：
        - LLM_API_KEY: API密钥
        - LLM_BASE_URL: API基础URL
        - LLM_MODEL_NAME: 模型名称
        """
        # 优先从 .env 读取配置
        llm_api_key = os.environ.get("LLM_API_KEY", "")
        llm_base_url = os.environ.get("LLM_BASE_URL", "")
        llm_model = os.environ.get("LLM_MODEL_NAME", "")
        
        # 如果 .env 中没有，则使用 config 作为备用
        if not llm_model:
            llm_model = self.config.get("llm_model", "gpt-4o-mini")
        
        # 设置 camel-ai 所需的环境变量
        if llm_api_key:
            os.environ["OPENAI_API_KEY"] = llm_api_key
        
        if not os.environ.get("OPENAI_API_KEY"):
            raise ValueError("缺少 API Key 配置，请在项目根目录 .env 文件中设置 LLM_API_KEY")
        
        if llm_base_url:
            os.environ["OPENAI_API_BASE_URL"] = llm_base_url
        
        print(f"LLM配置: model={llm_model}, base_url={llm_base_url[:40] if llm_base_url else '默认'}...")
        
        return ModelFactory.create(
            model_platform=ModelPlatformType.OPENAI,
            model_type=llm_model,
        )
    
    def _get_active_agents_for_round(
        self, 
        env, 
        current_hour: int,
        round_num: int
    ) -> List:
        """
        根据时间和配置决定本轮激活哪些Agent
        """
        simulation_id = self.config.get("simulation_id", "unknown")
        rng = random.Random(f"{simulation_id}:reddit:{round_num}")

        time_config = self.config.get("time_config", {})
        agent_configs = self.config.get("agent_configs", [])

        base_min = time_config.get("agents_per_hour_min", 5)
        base_max = time_config.get("agents_per_hour_max", 20)

        peak_hours = time_config.get("peak_hours", [9, 10, 11, 14, 15, 20, 21, 22])
        off_peak_hours = time_config.get("off_peak_hours", [0, 1, 2, 3, 4, 5])

        if current_hour in peak_hours:
            multiplier = time_config.get("peak_activity_multiplier", 1.5)
        elif current_hour in off_peak_hours:
            multiplier = time_config.get("off_peak_activity_multiplier", 0.3)
        else:
            multiplier = 1.0

        target_count = int(rng.uniform(base_min, base_max) * multiplier)

        candidates = []
        for cfg in agent_configs:
            agent_id = cfg.get("agent_id", 0)
            active_hours = cfg.get("active_hours", list(range(8, 23)))
            activity_level = cfg.get("activity_level", 0.5)

            if current_hour not in active_hours:
                continue

            if rng.random() < activity_level:
                candidates.append(agent_id)

        selected_ids = rng.sample(
            candidates,
            min(target_count, len(candidates))
        ) if candidates else []

        active_agents = []
        for agent_id in selected_ids:
            try:
                agent = env.agent_graph.get_agent(agent_id)
                active_agents.append((agent_id, agent))
            except Exception:
                pass

        return active_agents

    async def run(self, max_rounds: int = None, resume: bool = False):
        """运行Reddit模拟

        Args:
            max_rounds: 最大模拟轮数（可选，用于截断过长的模拟）
            resume: 是否从 round_checkpoint.json 记录的断点续跑。

                Reddit的created_at用的是
                sandbox_clock.time_transfer(now, start_time)——真实经过的
                时间乘以60倍加到start_time上——而 oasis.make() 在传入
                DefaultPlatformType.REDDIT 时只会创建一个全新的Platform
                (sandbox_clock=None -> 新Clock(60), start_time=None ->
                进程重启那一刻的datetime.now())。如果续跑时对Reddit也走
                这条默认路径，新Platform的start_time会被重置成"续跑这
                一刻"的墙钟时间，制造出一次相对原时间线的巨大跳变。
                修复方式：自己构造一个 Platform 实例（和 OasisEnv 内部
                REDDIT分支用完全相同的参数），把 start_time 回拨到"原始
                start_time + 已完成轮数隐含的模拟分钟数"，再把这个
                Platform实例（而不是DefaultPlatformType.REDDIT）传给
                oasis.make()——OasisEnv.__init__ 的
                isinstance(platform, Platform) 分支会正确地把 channel 和
                recsys_type 都从这个Platform实例上转发过去。
        """
        print("=" * 60)
        print("OASIS Reddit模拟")
        print(f"配置文件: {self.config_path}")
        print(f"模拟ID: {self.config.get('simulation_id', 'unknown')}")
        print(f"等待命令模式: {'启用' if self.wait_for_commands else '禁用'}")
        print(f"续跑模式: {'启用（--resume）' if resume else '禁用（全新运行）'}")
        print("=" * 60)

        time_config = self.config.get("time_config", {})
        total_hours = time_config.get("total_simulation_hours", 72)
        minutes_per_round = time_config.get("minutes_per_round", 30)
        total_rounds = (total_hours * 60) // minutes_per_round

        # 如果指定了最大轮数，则截断
        if max_rounds is not None and max_rounds > 0:
            original_rounds = total_rounds
            total_rounds = min(total_rounds, max_rounds)
            if total_rounds < original_rounds:
                print(f"\n轮数已截断: {original_rounds} -> {total_rounds} (max_rounds={max_rounds})")

        print(f"\n模拟参数:")
        print(f"  - 总模拟时长: {total_hours}小时")
        print(f"  - 每轮时间: {minutes_per_round}分钟")
        print(f"  - 总轮数: {total_rounds}")
        if max_rounds:
            print(f"  - 最大轮数限制: {max_rounds}")
        print(f"  - Agent数量: {len(self.config.get('agent_configs', []))}")

        db_path = self._get_db_path()

        # ---- Gated site 1: 续跑校验（必须在做任何昂贵的工作之前尽早失败）----
        resume_state = {
            "found": False, "start_round": 0, "last_rowid": 0,
            "total_actions": 0, "original_start_time": None,
        }
        if resume:
            resume_state = load_resume_state(self.simulation_dir, "reddit")
            if not resume_state["found"]:
                print(
                    "错误: 请求续跑(--resume)但未在 round_checkpoint.json "
                    "中找到 reddit 平台的记录，拒绝继续（避免跳过初始事件"
                    "播种或在空数据库上运行）。请改用强制重新开始。"
                )
                return
            if not _db_schema_is_complete(db_path):
                print(f"错误: 数据库表结构不完整，拒绝在此基础上续跑: {db_path}")
                return
            print(
                f"续跑模式: 从 round {resume_state['start_round']} 开始，"
                f"last_rowid={resume_state['last_rowid']}, "
                f"已记录动作数={resume_state['total_actions']}"
            )

        print("\n初始化LLM模型...")
        model = self._create_model()

        print("加载Agent Profile...")
        profile_path = self._get_profile_path()
        if not os.path.exists(profile_path):
            print(f"错误: Profile文件不存在: {profile_path}")
            return

        self.agent_graph = await generate_reddit_agent_graph(
            profile_path=profile_path,
            model=model,
            available_actions=self.AVAILABLE_ACTIONS,
        )

        # 从配置文件获取 Agent 真实名称映射（使用 entity_name 而非默认的 Agent_X）
        self.agent_names = get_agent_names_from_config(self.config)
        # 如果配置中没有某个 agent，则使用 OASIS 的默认名称
        for agent_id, agent in self.agent_graph.get_agents():
            if agent_id not in self.agent_names:
                self.agent_names[agent_id] = getattr(agent, 'name', f'Agent_{agent_id}')

        # ---- Gated site 2: 数据库删除（resume时必须保留，否则等于从头开始）----
        if not resume:
            if os.path.exists(db_path):
                os.remove(db_path)
                print(f"已删除旧数据库: {db_path}")
            await clear_checkpoint_platform_section(self.simulation_dir, "reddit")

        # Reddit 时钟连续性修复（见run()的docstring）：始终自己构造
        # Platform，而不是让 oasis.make() 用 DefaultPlatformType.REDDIT
        # 隐式创建一个 start_time=进程启动时刻的新Platform。这样无论是否
        # resume，start_time 都是一个我们自己知道、可以持久化进checkpoint
        # 的值。
        if resume and resume_state["original_start_time"] is not None:
            original_start_time = resume_state["original_start_time"]
        else:
            original_start_time = datetime.now()

        if resume:
            # 把 start_time 按"已完成轮数隐含的模拟分钟数"向前推，而不是
            # 简单复用 original_start_time 本身——否则新产生的帖子
            # created_at会直接跳回模拟刚开始的时间点。用round_num（而
            # 不是恢复前的真实耗时）作为推进依据是刻意的：这样无论续跑
            # 多少次，同一个start_round总是映射到同一个
            # platform_start_time，不会因为"这次恢复走了多久"而漂移。
            platform_start_time = original_start_time + timedelta(
                minutes=resume_state["start_round"] * minutes_per_round
            )
            print(
                f"Reddit 时钟连续性: original_start_time="
                f"{original_start_time.isoformat()}, 回拨后 start_time="
                f"{platform_start_time.isoformat()} "
                f"(start_round={resume_state['start_round']} x "
                f"{minutes_per_round}分钟/轮)"
            )
        else:
            platform_start_time = original_start_time

        reddit_channel = Channel()
        reddit_platform = Platform(
            db_path=db_path,
            channel=reddit_channel,
            sandbox_clock=Clock(60),
            start_time=platform_start_time,
            recsys_type="reddit",
            allow_self_rating=True,
            show_score=True,
            max_rec_post_len=100,
            refresh_rec_post_count=5,
        )

        print("创建OASIS环境...")
        self.env = oasis.make(
            agent_graph=self.agent_graph,
            platform=reddit_platform,
            database_path=db_path,
            semaphore=30,  # 限制最大并发 LLM 请求数，防止 API 过载
        )

        # env.reset() 保持无条件执行：它重启 platform.running() 消息循环，
        # 并重新signup所有agent——针对一个保留下来的数据库重跑sign_up是
        # 安全的，oasis Platform.sign_up 把 INSERT INTO user 包在
        # try/except Exception 里，失败(主键冲突)只会返回
        # {"success": False}，绝不抛异常，也不会写入重复的SIGNUP trace行。
        await self.env.reset()
        print("环境初始化完成\n")

        # 初始化动作日志记录器（供后端监控 UI 动作流 / 图谱记忆摄取使用）
        self.action_logger = PlatformActionLogger("reddit", self.simulation_dir)
        self.action_logger.log_simulation_start(self.config)

        total_actions = resume_state["total_actions"] if resume else 0
        # 跟踪数据库中最后处理的行号（使用 rowid 避免 created_at 格式差异）。
        # resume时必须从checkpoint恢复——不恢复的话 fetch_new_actions_from_db
        # 会把保留数据库里全部历史trace行都当成"新动作"重新写入
        # actions.jsonl，污染UI动作流和图谱摄取源。
        last_rowid = resume_state["last_rowid"] if resume else 0

        # 初始化IPC处理器
        self.ipc_handler = IPCHandler(self.simulation_dir, self.env, self.agent_graph)
        self.ipc_handler.update_status("running")

        # ---- Gated site 3: 初始事件播种（resume时必须跳过，否则重复发帖）----
        if not resume:
            event_config = self.config.get("event_config", {})
            initial_posts = event_config.get("initial_posts", [])

            # 记录 round 0 开始（初始事件阶段）
            self.action_logger.log_round_start(0, 0)  # round 0, simulated_hour 0

            initial_action_count = 0
            if initial_posts:
                print(f"执行初始事件 ({len(initial_posts)}条初始帖子)...")
                initial_actions = {}
                for post in initial_posts:
                    agent_id = post.get("poster_agent_id", 0)
                    content = post.get("content", "")
                    try:
                        agent = self.env.agent_graph.get_agent(agent_id)
                        if agent in initial_actions:
                            if not isinstance(initial_actions[agent], list):
                                initial_actions[agent] = [initial_actions[agent]]
                            initial_actions[agent].append(ManualAction(
                                action_type=ActionType.CREATE_POST,
                                action_args={"content": content}
                            ))
                        else:
                            initial_actions[agent] = ManualAction(
                                action_type=ActionType.CREATE_POST,
                                action_args={"content": content}
                            )

                        self.action_logger.log_action(
                            round_num=0,
                            agent_id=agent_id,
                            agent_name=self.agent_names.get(agent_id, f"Agent_{agent_id}"),
                            action_type="CREATE_POST",
                            action_args={"content": content}
                        )
                        total_actions += 1
                        initial_action_count += 1
                    except Exception as e:
                        print(f"  警告: 无法为Agent {agent_id}创建初始帖子: {e}")

                if initial_actions:
                    await self.env.step(initial_actions)
                    print(f"  已发布 {len(initial_actions)} 条初始帖子")

            # 记录 round 0 结束
            self.action_logger.log_round_end(0, initial_action_count)

            # round 0（初始事件轮）跑完，落一次checkpoint：如果接下来主
            # 循环还没开始就被杀死，续跑时知道初始事件已经做过。
            await write_round_checkpoint(
                self.simulation_dir, "reddit",
                next_round_index=0,
                last_rowid=last_rowid,
                total_actions=total_actions,
                original_start_time=original_start_time,
            )
        else:
            print("续跑模式: 跳过初始事件播种（round 0 已在上次运行中完成）")

        # ---- Gated site 4: 主循环起点 ----
        # 如果checkpoint显示该平台此前已经跑完（start_round >=
        # total_rounds），range()会自然产生一个空区间，循环体一次都不会
        # 执行，无需额外分支。
        start_round = resume_state["start_round"] if resume else 0
        if resume and start_round >= total_rounds:
            print(f"续跑模式: 该平台此前已完成全部 {total_rounds} 轮，无需继续")

        # 主模拟循环
        print("\n开始模拟循环...")
        start_time = datetime.now()

        for round_num in range(start_round, total_rounds):
            # 检查是否收到退出信号
            if _shutdown_event and _shutdown_event.is_set():
                print(f"\n收到退出信号，在第 {round_num + 1} 轮停止模拟")
                break

            # 检查Flask后端是否广播了新的LLM凭证（见模块顶部"LLM 凭证热
            # 重载"一节），有则原地重建model内部的OpenAI客户端
            self._cred_reload_last_mtime, self._cred_reload_last_version = (
                _poll_and_apply_credentials_reload(
                    self.simulation_dir, model,
                    self._cred_reload_last_mtime, self._cred_reload_last_version,
                    platform="reddit",
                )
            )

            simulated_minutes = round_num * minutes_per_round
            simulated_hour = (simulated_minutes // 60) % 24
            simulated_day = simulated_minutes // (60 * 24) + 1

            active_agents = self._get_active_agents_for_round(
                self.env, simulated_hour, round_num
            )

            # 无论是否有活跃agent，都记录round开始
            self.action_logger.log_round_start(round_num + 1, simulated_hour)

            if not active_agents:
                # 没有活跃agent时也记录round结束（actions_count=0）
                self.action_logger.log_round_end(round_num + 1, 0)
                await write_round_checkpoint(
                    self.simulation_dir, "reddit",
                    next_round_index=round_num + 1,
                    last_rowid=last_rowid,
                    total_actions=total_actions,
                    original_start_time=original_start_time,
                )
                continue

            actions = {
                agent: LLMAction()
                for _, agent in active_agents
            }

            await self.env.step(actions)

            # 从数据库获取实际执行的动作并记录
            actual_actions, last_rowid = fetch_new_actions_from_db(
                db_path, last_rowid, self.agent_names
            )

            round_action_count = 0
            for action_data in actual_actions:
                self.action_logger.log_action(
                    round_num=round_num + 1,
                    agent_id=action_data['agent_id'],
                    agent_name=action_data['agent_name'],
                    action_type=action_data['action_type'],
                    action_args=action_data['action_args']
                )
                total_actions += 1
                round_action_count += 1

            self.action_logger.log_round_end(round_num + 1, round_action_count)

            # checkpoint必须紧跟在log_round_end之后写入。
            await write_round_checkpoint(
                self.simulation_dir, "reddit",
                next_round_index=round_num + 1,
                last_rowid=last_rowid,
                total_actions=total_actions,
                original_start_time=original_start_time,
            )

            if (round_num + 1) % 10 == 0 or round_num == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                progress = (round_num + 1) / total_rounds * 100
                print(f"  [Day {simulated_day}, {simulated_hour:02d}:00] "
                      f"Round {round_num + 1}/{total_rounds} ({progress:.1f}%) "
                      f"- {len(active_agents)} agents active "
                      f"- elapsed: {elapsed:.1f}s")

        self.action_logger.log_simulation_end(total_rounds, total_actions)

        total_elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n模拟循环完成!")
        print(f"  - 总耗时: {total_elapsed:.1f}秒")
        print(f"  - 总动作数: {total_actions}")
        print(f"  - 数据库: {db_path}")
        
        # 是否进入等待命令模式
        if self.wait_for_commands:
            print("\n" + "=" * 60)
            print("进入等待命令模式 - 环境保持运行")
            print("支持的命令: interview, batch_interview, close_env")
            print("=" * 60)
            
            self.ipc_handler.update_status("alive")
            
            # 等待命令循环（使用全局 _shutdown_event）
            try:
                while not _shutdown_event.is_set():
                    should_continue = await self.ipc_handler.process_commands()
                    if not should_continue:
                        break
                    try:
                        await asyncio.wait_for(_shutdown_event.wait(), timeout=0.5)
                        break  # 收到退出信号
                    except asyncio.TimeoutError:
                        pass
            except KeyboardInterrupt:
                print("\n收到中断信号")
            except asyncio.CancelledError:
                print("\n任务被取消")
            except Exception as e:
                print(f"\n命令处理出错: {e}")
            
            print("\n关闭环境...")
        
        # 关闭环境
        self.ipc_handler.update_status("stopped")
        await self.env.close()
        
        print("环境已关闭")
        print("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description='OASIS Reddit模拟')
    parser.add_argument(
        '--config', 
        type=str, 
        required=True,
        help='配置文件路径 (simulation_config.json)'
    )
    parser.add_argument(
        '--max-rounds',
        type=int,
        default=None,
        help='最大模拟轮数（可选，用于截断过长的模拟）'
    )
    parser.add_argument(
        '--no-wait',
        action='store_true',
        default=False,
        help='模拟完成后立即关闭环境，不进入等待命令模式'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        default=False,
        help=(
            '从 round_checkpoint.json 记录的断点续跑：跳过数据库删除和'
            '初始事件播种，从上次记录的round继续。要求该模拟目录下已经'
            '存在有效的 round_checkpoint.json，否则会拒绝启动而不是悄悄'
            '从round 0重跑。'
        )
    )

    args = parser.parse_args()

    # 在 main 函数开始时创建 shutdown 事件
    global _shutdown_event
    _shutdown_event = asyncio.Event()

    if not os.path.exists(args.config):
        print(f"错误: 配置文件不存在: {args.config}")
        sys.exit(1)

    # 初始化日志配置（使用固定文件名，清理旧日志）
    simulation_dir = os.path.dirname(args.config) or "."
    setup_oasis_logging(os.path.join(simulation_dir, "log"))

    runner = RedditSimulationRunner(
        config_path=args.config,
        wait_for_commands=not args.no_wait
    )
    await runner.run(max_rounds=args.max_rounds, resume=args.resume)


def setup_signal_handlers():
    """
    设置信号处理器，确保收到 SIGTERM/SIGINT 时能够正确退出
    让程序有机会正常清理资源（关闭数据库、环境等）
    """
    def signal_handler(signum, frame):
        global _cleanup_done
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        print(f"\n收到 {sig_name} 信号，正在退出...")
        if not _cleanup_done:
            _cleanup_done = True
            if _shutdown_event:
                _shutdown_event.set()
        else:
            # 重复收到信号才强制退出
            print("强制退出...")
            sys.exit(1)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


if __name__ == "__main__":
    setup_signal_handlers()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被中断")
    except SystemExit:
        pass
    finally:
        print("模拟进程已退出")

