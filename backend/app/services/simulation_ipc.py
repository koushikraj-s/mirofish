"""
模拟IPC通信模块
用于Flask后端和模拟脚本之间的进程间通信

通过文件系统实现简单的命令/响应模式：
1. Flask写入命令到 commands/ 目录
2. 模拟脚本轮询命令目录，执行命令并写入响应到 responses/ 目录
3. Flask轮询响应目录获取结果
"""

import os
import json
import time
import uuid
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ..utils.atomic_io import atomic_write_json, read_json_tolerant
from ..utils.logger import get_logger

logger = get_logger('mirofish.simulation_ipc')


class CommandType(str, Enum):
    """命令类型"""
    INTERVIEW = "interview"           # 单个Agent采访
    BATCH_INTERVIEW = "batch_interview"  # 批量采访
    CLOSE_ENV = "close_env"           # 关闭环境


class CommandStatus(str, Enum):
    """命令状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IPCCommand:
    """IPC命令"""
    command_id: str
    command_type: CommandType
    args: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "command_type": self.command_type.value,
            "args": self.args,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IPCCommand':
        return cls(
            command_id=data["command_id"],
            command_type=CommandType(data["command_type"]),
            args=data.get("args", {}),
            timestamp=data.get("timestamp", datetime.now().isoformat())
        )


@dataclass
class IPCResponse:
    """IPC响应"""
    command_id: str
    status: CommandStatus
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IPCResponse':
        return cls(
            command_id=data["command_id"],
            status=CommandStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            timestamp=data.get("timestamp", datetime.now().isoformat())
        )


# ============================================================
# LLM 凭证热重载广播（credentials_reload.json）
# ============================================================
#
# 背景：一个已经在跑的模拟子进程在启动时通过 `_create_model()`/
# `create_model()` 一次性读取 LLM_API_KEY/LLM_BASE_URL 并构造出 camel-ai
# 的 OpenAIModel——凭证在构造时就被烤进了内部的 OpenAI/AsyncOpenAI 客户端
# 对象。当用户在设置界面通过 `settings_store.apply_and_propagate` 热替换
# Flask 进程的凭证时，仅仅更新 `Config`/`os.environ` 对这个已经在跑的子
# 进程没有任何影响——它需要一种方式在运行期间得知"凭证变了"，并原地重建
# 自己的 model 对象。
#
# 为什么不能复用上面的 ipc_commands/ 命令机制：那一套是单消费者、读完
# 即删（delete-on-read，见 `SimulationIPCServer.poll_commands`/
# `send_response`）。并行模式下 twitter 和 reddit 是两个独立的协程，各自
# 独立轮询；如果凭证更新也走 ipc_commands/，两个协程里先轮到的那个会把
# 命令文件删掉，另一个协程就永远收不到这次更新，静默地继续用旧（可能已
# 欠费）的凭证跑下去。凭证更新因此改用一份独立的、只增不改语义的"广播"
# 文件：所有消费者都可以各自独立、重复地读取同一份文件，谁都不会把它
# "消费掉"。
#
# 使用方式：
#   - Flask 侧（`settings_store.apply_and_propagate`）在凭证变化时调用
#     `write_credentials_reload_broadcast()`，version 严格递增，文件
#     永远不会被删除。
#   - 模拟子进程侧（run_twitter_simulation.py/run_reddit_simulation.py/
#     run_parallel_simulation.py）在各自的round循环里，用一份轻量的
#     mtime检查外加自己独立维护的"上次应用到的version"来决定要不要
#     重建model——三份脚本各自维护一份等价实现（这几个脚本本身就是彼此
#     独立的裸入口，出于同样原因，round_checkpoint.json的读写逻辑在那三
#     个脚本里也是各自拷贝的一份，而不是从这里import，见各脚本模块顶部
#     "不复用 backend/app/utils/atomic_io.py 的原因"一节的说明：那样会把
#     整个 Flask app 包带进子进程的import graph）。这里只保留 Flask 侧
#     用得到的写入函数。

CREDENTIALS_RELOAD_FILENAME = "credentials_reload.json"


def write_credentials_reload_broadcast(
    simulation_dir: str,
    *,
    llm_api_key: Optional[str],
    llm_base_url: Optional[str],
    llm_model_name: Optional[str] = None,
) -> int:
    """原子地把 <simulation_dir>/credentials_reload.json 的 version 加一，
    并写入当前生效的 LLM 凭证字段。

    这是一份"广播"而不是一条"命令"：从不删除，也从不要求消费者确认。
    version 严格递增（读取旧文件里的 version 再加一；旧文件不存在/损坏则
    从 1 开始），consumer 只需要记住自己上次应用过的 version、拿新读到的
    version 跟它比较即可，不需要跟任何其它进程协调。

    Args:
        simulation_dir: 模拟数据目录（<uploads/simulations>/<simulation_id>）。
        llm_api_key: 当前生效的 LLM_API_KEY（Config.LLM_API_KEY，已经完成
            override/env/default 分层之后的最终值）。
        llm_base_url: 当前生效的 LLM_BASE_URL，可以是 None（表示走 OpenAI
            默认endpoint）。
        llm_model_name: 当前生效的 LLM_MODEL_NAME，仅作记录/日志用途——
            子进程的相机（camel-ai）model对象一旦构造完成就不会再重新绑定
            model_type，这里不会、也不能让正在运行的模拟切换到一个不同的
            模型类型。

    Returns:
        写入后的新 version（整数）。
    """

    os.makedirs(simulation_dir, exist_ok=True)
    path = os.path.join(simulation_dir, CREDENTIALS_RELOAD_FILENAME)

    existing = read_json_tolerant(path, default=None)
    previous_version = existing.get("version") if isinstance(existing, dict) else None
    new_version = (previous_version + 1) if isinstance(previous_version, int) else 1

    document = {
        "version": new_version,
        "updated_at": datetime.now().isoformat(),
        "llm_api_key": llm_api_key,
        "llm_base_url": llm_base_url,
        "llm_model_name": llm_model_name,
    }
    atomic_write_json(path, document, mode=0o600)

    logger.info(
        "已广播LLM凭证热重载: simulation_dir=%s, version=%s",
        simulation_dir, new_version,
    )
    return new_version


class SimulationIPCClient:
    """
    模拟IPC客户端（Flask端使用）
    
    用于向模拟进程发送命令并等待响应
    """
    
    def __init__(self, simulation_dir: str):
        """
        初始化IPC客户端
        
        Args:
            simulation_dir: 模拟数据目录
        """
        self.simulation_dir = simulation_dir
        self.commands_dir = os.path.join(simulation_dir, "ipc_commands")
        self.responses_dir = os.path.join(simulation_dir, "ipc_responses")
        
        # 确保目录存在
        os.makedirs(self.commands_dir, exist_ok=True)
        os.makedirs(self.responses_dir, exist_ok=True)
    
    def send_command(
        self,
        command_type: CommandType,
        args: Dict[str, Any],
        timeout: float = 60.0,
        poll_interval: float = 0.5
    ) -> IPCResponse:
        """
        发送命令并等待响应
        
        Args:
            command_type: 命令类型
            args: 命令参数
            timeout: 超时时间（秒）
            poll_interval: 轮询间隔（秒）
            
        Returns:
            IPCResponse
            
        Raises:
            TimeoutError: 等待响应超时
        """
        command_id = str(uuid.uuid4())
        command = IPCCommand(
            command_id=command_id,
            command_type=command_type,
            args=args
        )
        
        # 写入命令文件
        command_file = os.path.join(self.commands_dir, f"{command_id}.json")
        with open(command_file, 'w', encoding='utf-8') as f:
            json.dump(command.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"发送IPC命令: {command_type.value}, command_id={command_id}")
        
        # 等待响应
        response_file = os.path.join(self.responses_dir, f"{command_id}.json")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if os.path.exists(response_file):
                try:
                    with open(response_file, 'r', encoding='utf-8') as f:
                        response_data = json.load(f)
                    response = IPCResponse.from_dict(response_data)
                    
                    # 清理命令和响应文件
                    try:
                        os.remove(command_file)
                        os.remove(response_file)
                    except OSError:
                        pass
                    
                    logger.info(f"收到IPC响应: command_id={command_id}, status={response.status.value}")
                    return response
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"解析响应失败: {e}")
            
            time.sleep(poll_interval)
        
        # 超时
        logger.error(f"等待IPC响应超时: command_id={command_id}")
        
        # 清理命令文件
        try:
            os.remove(command_file)
        except OSError:
            pass
        
        raise TimeoutError(f"等待命令响应超时 ({timeout}秒)")
    
    def send_interview(
        self,
        agent_id: int,
        prompt: str,
        platform: str = None,
        timeout: float = 60.0
    ) -> IPCResponse:
        """
        发送单个Agent采访命令
        
        Args:
            agent_id: Agent ID
            prompt: 采访问题
            platform: 指定平台（可选）
                - "twitter": 只采访Twitter平台
                - "reddit": 只采访Reddit平台  
                - None: 双平台模拟时同时采访两个平台，单平台模拟时采访该平台
            timeout: 超时时间
            
        Returns:
            IPCResponse，result字段包含采访结果
        """
        args = {
            "agent_id": agent_id,
            "prompt": prompt
        }
        if platform:
            args["platform"] = platform
            
        return self.send_command(
            command_type=CommandType.INTERVIEW,
            args=args,
            timeout=timeout
        )
    
    def send_batch_interview(
        self,
        interviews: List[Dict[str, Any]],
        platform: str = None,
        timeout: float = 120.0
    ) -> IPCResponse:
        """
        发送批量采访命令
        
        Args:
            interviews: 采访列表，每个元素包含 {"agent_id": int, "prompt": str, "platform": str(可选)}
            platform: 默认平台（可选，会被每个采访项的platform覆盖）
                - "twitter": 默认只采访Twitter平台
                - "reddit": 默认只采访Reddit平台
                - None: 双平台模拟时每个Agent同时采访两个平台
            timeout: 超时时间
            
        Returns:
            IPCResponse，result字段包含所有采访结果
        """
        args = {"interviews": interviews}
        if platform:
            args["platform"] = platform
            
        return self.send_command(
            command_type=CommandType.BATCH_INTERVIEW,
            args=args,
            timeout=timeout
        )
    
    def send_close_env(self, timeout: float = 30.0) -> IPCResponse:
        """
        发送关闭环境命令
        
        Args:
            timeout: 超时时间
            
        Returns:
            IPCResponse
        """
        return self.send_command(
            command_type=CommandType.CLOSE_ENV,
            args={},
            timeout=timeout
        )
    
    def check_env_alive(self) -> bool:
        """
        检查模拟环境是否存活
        
        通过检查 env_status.json 文件来判断
        """
        status_file = os.path.join(self.simulation_dir, "env_status.json")
        if not os.path.exists(status_file):
            return False
        
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                status = json.load(f)
            return status.get("status") == "alive"
        except (json.JSONDecodeError, OSError):
            return False


class SimulationIPCServer:
    """
    模拟IPC服务器（模拟脚本端使用）
    
    轮询命令目录，执行命令并返回响应
    """
    
    def __init__(self, simulation_dir: str):
        """
        初始化IPC服务器
        
        Args:
            simulation_dir: 模拟数据目录
        """
        self.simulation_dir = simulation_dir
        self.commands_dir = os.path.join(simulation_dir, "ipc_commands")
        self.responses_dir = os.path.join(simulation_dir, "ipc_responses")
        
        # 确保目录存在
        os.makedirs(self.commands_dir, exist_ok=True)
        os.makedirs(self.responses_dir, exist_ok=True)
        
        # 环境状态
        self._running = False
    
    def start(self):
        """标记服务器为运行状态"""
        self._running = True
        self._update_env_status("alive")
    
    def stop(self):
        """标记服务器为停止状态"""
        self._running = False
        self._update_env_status("stopped")
    
    def _update_env_status(self, status: str):
        """更新环境状态文件"""
        status_file = os.path.join(self.simulation_dir, "env_status.json")
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({
                "status": status,
                "timestamp": datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)
    
    def poll_commands(self) -> Optional[IPCCommand]:
        """
        轮询命令目录，返回第一个待处理的命令
        
        Returns:
            IPCCommand 或 None
        """
        if not os.path.exists(self.commands_dir):
            return None
        
        # 按时间排序获取命令文件
        command_files = []
        for filename in os.listdir(self.commands_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.commands_dir, filename)
                command_files.append((filepath, os.path.getmtime(filepath)))
        
        command_files.sort(key=lambda x: x[1])
        
        for filepath, _ in command_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return IPCCommand.from_dict(data)
            except (json.JSONDecodeError, KeyError, OSError) as e:
                logger.warning(f"读取命令文件失败: {filepath}, {e}")
                continue
        
        return None
    
    def send_response(self, response: IPCResponse):
        """
        发送响应
        
        Args:
            response: IPC响应
        """
        response_file = os.path.join(self.responses_dir, f"{response.command_id}.json")
        with open(response_file, 'w', encoding='utf-8') as f:
            json.dump(response.to_dict(), f, ensure_ascii=False, indent=2)
        
        # 删除命令文件
        command_file = os.path.join(self.commands_dir, f"{response.command_id}.json")
        try:
            os.remove(command_file)
        except OSError:
            pass
    
    def send_success(self, command_id: str, result: Dict[str, Any]):
        """发送成功响应"""
        self.send_response(IPCResponse(
            command_id=command_id,
            status=CommandStatus.COMPLETED,
            result=result
        ))
    
    def send_error(self, command_id: str, error: str):
        """发送错误响应"""
        self.send_response(IPCResponse(
            command_id=command_id,
            status=CommandStatus.FAILED,
            error=error
        ))
