"""
Round级断点续跑：round_checkpoint.json 读写

背景：模拟子进程此前在启动时无条件删除自己的sqlite数据库
（os.remove(db_path)），这意味着一次中途被杀死（kill -9 / 崩溃 /
宿主机重启）的多小时模拟，唯一的恢复方式就是从round 0整个重跑。

本模块级函数组实现的续跑方案：
  1. 每一轮（包括round 0的初始事件轮）结束、log_round_end 写入之后，
     立即把该平台的进度（下一轮要跑的round_num、trace表的last_rowid
     高水位、累计动作数）原子写入 <sim_dir>/round_checkpoint.json。
  2. --resume 时读取该文件，跳过数据库删除、跳过初始事件播种，
     从 next_round_index 开始继续主循环，并且从记录的 last_rowid继续
     读取trace表（这是最关键的一步——last_rowid不恢复就会把历史trace
     行当成"新动作"重新写入actions.jsonl，污染UI动作流和图谱摄取源）。

并发注意：并行模式下 Twitter / Reddit 两个协程跑在同一个事件循环里，
共享同一份 checkpoint 文件。一次朴素的"整体读取->原地修改->整体写回"
跨越了函数调用边界（虽然磁盘IO本身是同步的，但外层函数是协程，取锁
之前完全可能被事件循环切换到另一个协程），如果不加锁，一个平台的写
入可能会把另一个平台刚刚写入的section覆盖掉。用一把模块级
asyncio.Lock 序列化"读-改-写"整个过程即可解决。

本脚本是被 subprocess.Popen 启动的独立子进程；没有复用
backend/app/utils/atomic_io.py 中同样逻辑的 atomic_write_json，是因为
那个模块位于 app 包内，import 它会连带触发 app/__init__.py 及
app/utils/__init__.py 的完整导入链（Flask、flask_cors、
file_parser、llm_client 等），给一个本该轻量、且刻意与后端Flask应用
解耦的模拟子进程引入不必要的强耦合。这里就地复刻同样的
write-temp+fsync+rename 技术。

此前这一整套函数在 run_parallel_simulation.py / run_twitter_simulation.py /
run_reddit_simulation.py 三个模拟脚本里各自维护一份拷贝，现在提取为
脚本共享模块，三个脚本改为直接 import 使用；并行模式下 Twitter/Reddit
两个协程之所以能安全共享同一份 checkpoint 文件，正是因为它们现在也
共享同一个模块级 `_checkpoint_lock`。
"""

import os
import json
import sqlite3
import asyncio
from datetime import datetime
from typing import Dict, Any


CHECKPOINT_FILENAME = "round_checkpoint.json"
CHECKPOINT_SCHEMA_VERSION = 1

# oasis.social_platform.database.create_db() 对每张表都用不带
# "IF NOT EXISTS" 的 CREATE TABLE，且全部语句共享同一个 try/except：
# 只要第一条 CREATE TABLE 因表已存在而抛出 sqlite3.Error，后面所有表的
# 创建语句都会被静默跳过。这对一个完整的数据库无害（表本来就都存在），
# 但如果被保留下来的数据库是在它自己第一轮结束前就被杀死、schema本身
# 就不完整，续跑就会在一个残缺的数据库上运行而不自知。resume之前显式
# 核对 sqlite_master 中的表集合，缺任何一张就拒绝续跑。
_REQUIRED_DB_TABLES = {
    "user", "post", "follow", "mute", "like", "dislike", "report",
    "trace", "rec", "comment", "comment_like", "comment_dislike", "product",
}

# 并行模式下 Twitter / Reddit 协程共享同一份 checkpoint 文件，需要序列化
# "读-改-写"。
_checkpoint_lock = asyncio.Lock()


def _atomic_write_checkpoint_json(path: str, data: Dict[str, Any]) -> None:
    """临时文件 + fsync + os.replace，任何时刻打开该路径要么看到完整旧
    文件，要么看到完整新文件，绝不会看到被 kill -9 打断的半截JSON。

    本脚本是被 subprocess.Popen 启动的独立子进程；没有复用
    backend/app/utils/atomic_io.py 中同样逻辑的 atomic_write_json，是因为
    那个模块位于 app 包内，import 它会连带触发 app/__init__.py 及
    app/utils/__init__.py 的完整导入链（Flask、flask_cors、
    file_parser、llm_client 等），给一个本该轻量、且刻意与后端Flask应用
    解耦的模拟子进程引入不必要的强耦合。这里就地复刻同样的
    write-temp+fsync+rename 技术。
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.tmp"
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
            json.dump(data, tmp_file, ensure_ascii=False, indent=2, sort_keys=True)
            tmp_file.write("\n")
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _read_checkpoint_file(simulation_dir: str) -> Dict[str, Any]:
    """宽容读取 round_checkpoint.json：文件不存在/为空/损坏一律返回{}，
    绝不因为一份坏掉的checkpoint而让整个子进程崩溃——找不到有效续跑点时
    调用方会拒绝续跑，而不是崩溃。"""
    path = os.path.join(simulation_dir, CHECKPOINT_FILENAME)
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except (FileNotFoundError, OSError):
        return {}
    if not content.strip():
        return {}
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _db_schema_is_complete(db_path: str) -> bool:
    """resume之前核对被保留下来的数据库确实具备完整表结构（见上方模块
    docstring）。"""
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing = {row[0] for row in cursor.fetchall()}
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    return _REQUIRED_DB_TABLES.issubset(existing)


def load_resume_state(simulation_dir: str, platform: str) -> Dict[str, Any]:
    """从 round_checkpoint.json 还原指定平台("twitter"/"reddit")的续跑
    位置。

    找不到checkpoint、平台对应的section缺失、或schema_version不匹配时，
    一律返回 found=False——拒绝盲目续跑好过悄悄跑出脏数据，由调用方决定
    如何处理（脚本层面一律选择直接报错退出，见 run_twitter_simulation /
    run_reddit_simulation 中的用法）。
    """
    not_found = {
        "found": False,
        "start_round": 0,
        "last_rowid": 0,
        "total_actions": 0,
        "original_start_time": None,
    }
    data = _read_checkpoint_file(simulation_dir)
    if data.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        return not_found
    section = data.get(platform)
    if not isinstance(section, dict):
        return not_found

    original_start_time = None
    start_time_str = data.get("start_time")
    if start_time_str:
        try:
            original_start_time = datetime.fromisoformat(start_time_str)
        except (ValueError, TypeError):
            original_start_time = None

    try:
        start_round = int(section.get("next_round_index", 0) or 0)
        last_rowid = int(section.get("last_rowid", 0) or 0)
        total_actions = int(section.get("total_actions_logged", 0) or 0)
    except (TypeError, ValueError):
        return not_found

    return {
        "found": True,
        "start_round": start_round,
        "last_rowid": last_rowid,
        "total_actions": total_actions,
        "original_start_time": original_start_time,
    }


async def write_round_checkpoint(
    simulation_dir: str,
    platform: str,
    next_round_index: int,
    last_rowid: int,
    total_actions: int,
    original_start_time: datetime,
) -> None:
    """把某一平台跑完一轮之后的进度原子写入共享的 round_checkpoint.json。

    Args:
        simulation_dir: 模拟目录
        platform: "twitter" 或 "reddit"
        next_round_index: 下一次应该从哪个 round_num 开始主循环（0表示
            初始事件轮已完成、主循环尚未开始任何一轮）
        last_rowid: 该平台trace表的高水位rowid（用于下次续跑时避免把
            历史trace行当成新动作重复写入actions.jsonl）
        total_actions: 该平台目前为止累计记录的动作总数
        original_start_time: 本次模拟最初启动时的墙钟时间。只在
            checkpoint文件里还没有start_time字段时写入一次，之后的每次
            写入都保留最初的值不变——这样连续多次resume（重启->跑几轮->
            又被杀死->再resume）用来做Reddit时钟连续性锚点的start_time
            才不会一次比一次漂移。
    """
    async with _checkpoint_lock:
        data = _read_checkpoint_file(simulation_dir)
        if data.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            data = {}
        data["schema_version"] = CHECKPOINT_SCHEMA_VERSION
        if not data.get("start_time"):
            data["start_time"] = original_start_time.isoformat()
        data[platform] = {
            "next_round_index": next_round_index,
            "last_rowid": last_rowid,
            "total_actions_logged": total_actions,
            "updated_at": datetime.now().isoformat(),
        }
        path = os.path.join(simulation_dir, CHECKPOINT_FILENAME)
        _atomic_write_checkpoint_json(path, data)


async def clear_checkpoint_platform_section(simulation_dir: str, platform: str) -> None:
    """在一次全新（非--resume）运行开始时，清掉 round_checkpoint.json 里
    该平台残留的旧section。

    这份checkpoint文件不在本脚本的"强制重新开始"清理范围内（后端的
    cleanup_simulation_logs 删除 run_state.json/数据库/actions.jsonl 等，
    但不认识这份新引入的文件），所以理论上存在一个窄窗口：强制重启之后、
    新一轮round 0跑完之前，如果对同一个模拟目录发起 --resume，会读到
    强制重启之前遗留的旧checkpoint——它的last_rowid是相对于已经被删除
    的旧数据库算出来的，而新数据库的rowid从1重新开始，会把新产生的真实
    动作误判成"已经处理过"而跳过。在这里，全新运行一开始就主动清空自己
    这个平台的旧section，把这个窗口关掉。
    """
    async with _checkpoint_lock:
        data = _read_checkpoint_file(simulation_dir)
        if platform in data:
            data.pop(platform, None)
            path = os.path.join(simulation_dir, CHECKPOINT_FILENAME)
            if data.get("schema_version") == CHECKPOINT_SCHEMA_VERSION and (
                "twitter" in data or "reddit" in data
            ):
                _atomic_write_checkpoint_json(path, data)
            else:
                try:
                    os.remove(path)
                except OSError:
                    pass
