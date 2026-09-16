"""
图谱记忆更新服务
将模拟中的Agent活动动态更新到图谱中
"""

import hashlib
import json
import os
import time
import threading
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone
from queue import Queue, Empty

from graphiti_core.nodes import EpisodeType

from ..config import Config
from ..utils.logger import get_logger
from ..utils.locale import get_locale, set_locale
from ..utils.zep import (
    ZEP_INGESTION_WAIT_TIMEOUT_SECONDS,
    _cached_graphiti_client,
    close_client,
    get_zep_client,
    run_async,
)
from ..utils.zep_paging import fetch_all_episodes
from .graph_ingestion_journal import (
    GraphIngestionJournal,
    actions_log_path,
    iter_action_lines,
    load_cursor,
    save_cursor,
)

logger = get_logger('mirofish.zep_graph_memory_updater')

# The exact marker/truncation rule used both when an episode is built for the
# first time (`_build_episode_payloads`) and when one is reconstructed byte
# range from actions.jsonl during `resume_ingestion` -- the reconstructed
# text's sha256 must match what was recorded in the journal's INTENT record,
# which only holds if both paths truncate identically.
_TRUNCATION_MARKER = "... [truncated by MiroFish]"


def _truncate_for_episode(text: str, max_chars: int) -> str:
    if len(text) > max_chars:
        return text[: max_chars - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER
    return text


def _agent_activity_from_action_data(data: Dict[str, Any], platform: str) -> "AgentActivity":
    """Build an AgentActivity from one parsed actions.jsonl line.

    Shared by the live tailer path (`add_activity_from_dict`) and the
    `resume_ingestion` byte-range rebuild path, so the two can never
    silently diverge on field mapping/defaults.
    """

    return AgentActivity(
        platform=platform,
        agent_id=data.get("agent_id", 0),
        agent_name=data.get("agent_name", ""),
        action_type=data.get("action_type", ""),
        action_args=data.get("action_args", {}),
        round_num=data.get("round", 0),
        timestamp=data.get("timestamp", datetime.now().isoformat()),
    )


def _activities_from_action_lines(
    platform: str, raw_lines: List[bytes]
) -> List["AgentActivity"]:
    """Parse raw actions.jsonl lines into the AgentActivity list that would
    have resulted from feeding each one through `add_activity_from_dict` +
    `add_activity` -- i.e. applying the exact same filters (skip event
    entries, skip `success: false`, skip `DO_NOTHING`) in the exact same
    order. Used by `resume_ingestion`'s byte-range rebuild, where there is
    no live updater instance to call those methods on.
    """

    activities: List[AgentActivity] = []
    for raw_line in raw_lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            data = json.loads(raw_line.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(data, dict) or "event_type" in data:
            continue
        if data.get("success") is False:
            continue
        activity = _agent_activity_from_action_data(data, platform)
        if activity.action_type == "DO_NOTHING":
            continue
        activities.append(activity)
    return activities


def _rebuild_intent_episode_text(
    sim_dir: str, intent: Dict[str, Any]
) -> Optional[Tuple[str, datetime]]:
    """Re-derive the exact episode text (and reference_time) an INTENT
    record describes, from the recorded byte range in actions.jsonl, and
    verify it still hashes to the recorded sha256.

    Returns None if the range can't be read at all, or reconstructs to a
    different sha256 than what was recorded -- e.g. actions.jsonl was
    truncated/replaced by a force-restart between the crash and this call.
    Replaying a mismatched reconstruction would risk sending the wrong text
    under an already-used episode name, so this refuses rather than
    guesses.
    """

    platform = intent.get("platform")
    start = intent.get("start")
    end = intent.get("end")
    if (
        not isinstance(platform, str)
        or not isinstance(start, int)
        or not isinstance(end, int)
        or end < start
    ):
        return None

    path = actions_log_path(sim_dir, platform)
    try:
        with open(path, "rb") as f:
            f.seek(start)
            chunk = f.read(end - start)
    except OSError:
        return None

    activities = _activities_from_action_lines(platform, chunk.split(b"\n"))
    if not activities:
        return None

    lines = [
        _truncate_for_episode(a.to_episode_text(), ZepGraphMemoryUpdater.MAX_EPISODE_CHARS)
        for a in activities
    ]
    combined_text = "\n".join(lines)

    digest = hashlib.sha256(combined_text.encode("utf-8")).hexdigest()
    if digest != intent.get("sha256"):
        return None

    reference_time = ZepGraphMemoryUpdater._to_datetime(activities[-1].timestamp)
    return combined_text, reference_time


@dataclass
class AgentActivity:
    """Agent活动记录"""
    platform: str           # twitter / reddit
    agent_id: int
    agent_name: str
    action_type: str        # CREATE_POST, LIKE_POST, etc.
    action_args: Dict[str, Any]
    round_num: int
    timestamp: str
    # Half-open byte range [journal_start, journal_end) this activity's raw
    # line occupies in <sim_dir>/<platform>/actions.jsonl, as determined by
    # ZepGraphMemoryUpdater's own independent scan of that file at the
    # moment `add_activity_from_dict` received it (see that method). None
    # when the activity did not come from a real, on-disk actions.jsonl
    # (e.g. constructed directly via `add_activity` in tests) -- such
    # activities are sent exactly as before, without write-ahead journaling.
    journal_start: Optional[int] = None
    journal_end: Optional[int] = None

    def to_episode_text(self) -> str:
        """
        将活动转换为可以发送给图谱的文本描述
        
        采用自然语言描述格式，让Graphiti能够从中提取实体和关系
        不添加模拟相关的前缀，避免误导图谱更新
        """
        # 根据不同的动作类型生成不同的描述
        action_descriptions = {
            "CREATE_POST": self._describe_create_post,
            "LIKE_POST": self._describe_like_post,
            "DISLIKE_POST": self._describe_dislike_post,
            "REPOST": self._describe_repost,
            "QUOTE_POST": self._describe_quote_post,
            "FOLLOW": self._describe_follow,
            "CREATE_COMMENT": self._describe_create_comment,
            "LIKE_COMMENT": self._describe_like_comment,
            "DISLIKE_COMMENT": self._describe_dislike_comment,
            "SEARCH_POSTS": self._describe_search,
            "SEARCH_USER": self._describe_search_user,
            "MUTE": self._describe_mute,
        }
        
        describe_func = action_descriptions.get(self.action_type, self._describe_generic)
        description = describe_func()
        
        # Keep the event time in the source text as well as episode metadata so
        # temporal extraction does not collapse a multi-action batch.
        return (
            f"[{self.timestamp}] [{self.platform} round {self.round_num}] "
            f"{self.agent_name}: {description}"
        )
    
    def _describe_create_post(self) -> str:
        content = self.action_args.get("content", "")
        if content:
            return f"发布了一条帖子：「{content}」"
        return "发布了一条帖子"
    
    def _describe_like_post(self) -> str:
        """点赞帖子 - 包含帖子原文和作者信息"""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"点赞了{post_author}的帖子：「{post_content}」"
        elif post_content:
            return f"点赞了一条帖子：「{post_content}」"
        elif post_author:
            return f"点赞了{post_author}的一条帖子"
        return "点赞了一条帖子"
    
    def _describe_dislike_post(self) -> str:
        """踩帖子 - 包含帖子原文和作者信息"""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"踩了{post_author}的帖子：「{post_content}」"
        elif post_content:
            return f"踩了一条帖子：「{post_content}」"
        elif post_author:
            return f"踩了{post_author}的一条帖子"
        return "踩了一条帖子"
    
    def _describe_repost(self) -> str:
        """转发帖子 - 包含原帖内容和作者信息"""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        
        if original_content and original_author:
            return f"转发了{original_author}的帖子：「{original_content}」"
        elif original_content:
            return f"转发了一条帖子：「{original_content}」"
        elif original_author:
            return f"转发了{original_author}的一条帖子"
        return "转发了一条帖子"
    
    def _describe_quote_post(self) -> str:
        """引用帖子 - 包含原帖内容、作者信息和引用评论"""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        quote_content = self.action_args.get("quote_content", "") or self.action_args.get("content", "")
        
        base = ""
        if original_content and original_author:
            base = f"引用了{original_author}的帖子「{original_content}」"
        elif original_content:
            base = f"引用了一条帖子「{original_content}」"
        elif original_author:
            base = f"引用了{original_author}的一条帖子"
        else:
            base = "引用了一条帖子"
        
        if quote_content:
            base += f"，并评论道：「{quote_content}」"
        return base
    
    def _describe_follow(self) -> str:
        """关注用户 - 包含被关注用户的名称"""
        target_user_name = self.action_args.get("target_user_name", "")
        
        if target_user_name:
            return f"关注了用户「{target_user_name}」"
        return "关注了一个用户"
    
    def _describe_create_comment(self) -> str:
        """发表评论 - 包含评论内容和所评论的帖子信息"""
        content = self.action_args.get("content", "")
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if content:
            if post_content and post_author:
                return f"在{post_author}的帖子「{post_content}」下评论道：「{content}」"
            elif post_content:
                return f"在帖子「{post_content}」下评论道：「{content}」"
            elif post_author:
                return f"在{post_author}的帖子下评论道：「{content}」"
            return f"评论道：「{content}」"
        return "发表了评论"
    
    def _describe_like_comment(self) -> str:
        """点赞评论 - 包含评论内容和作者信息"""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"点赞了{comment_author}的评论：「{comment_content}」"
        elif comment_content:
            return f"点赞了一条评论：「{comment_content}」"
        elif comment_author:
            return f"点赞了{comment_author}的一条评论"
        return "点赞了一条评论"
    
    def _describe_dislike_comment(self) -> str:
        """踩评论 - 包含评论内容和作者信息"""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"踩了{comment_author}的评论：「{comment_content}」"
        elif comment_content:
            return f"踩了一条评论：「{comment_content}」"
        elif comment_author:
            return f"踩了{comment_author}的一条评论"
        return "踩了一条评论"
    
    def _describe_search(self) -> str:
        """搜索帖子 - 包含搜索关键词"""
        query = self.action_args.get("query", "") or self.action_args.get("keyword", "")
        return f"搜索了「{query}」" if query else "进行了搜索"
    
    def _describe_search_user(self) -> str:
        """搜索用户 - 包含搜索关键词"""
        query = self.action_args.get("query", "") or self.action_args.get("username", "")
        return f"搜索了用户「{query}」" if query else "搜索了用户"
    
    def _describe_mute(self) -> str:
        """屏蔽用户 - 包含被屏蔽用户的名称"""
        target_user_name = self.action_args.get("target_user_name", "")
        
        if target_user_name:
            return f"屏蔽了用户「{target_user_name}」"
        return "屏蔽了一个用户"
    
    def _describe_generic(self) -> str:
        # 对于未知的动作类型，生成通用描述
        return f"执行了{self.action_type}操作"


class _DrainDeadlineExceeded(TimeoutError):
    def __init__(self, processed_count: int):
        super().__init__("Graph memory updater drain deadline elapsed")
        self.processed_count = processed_count


class ZepGraphMemoryUpdater:
    """
    图谱记忆更新器
    
    监控模拟的actions日志文件，将新的agent活动实时更新到图谱中。
    按平台分组，每累积BATCH_SIZE条活动后批量发送到图谱。
    
    所有有意义的行为都会被更新到图谱，action_args中会包含完整的上下文信息：
    - 点赞/踩的帖子原文
    - 转发/引用的帖子原文
    - 关注/屏蔽的用户名
    - 点赞/踩的评论原文
    """
    
    # 批量发送大小（每个平台累积多少条后发送）
    BATCH_SIZE = 5
    
    # 平台名称映射（用于控制台显示）
    PLATFORM_DISPLAY_NAMES = {
        'twitter': '世界1',
        'reddit': '世界2',
    }
    
    # 发送间隔（秒），避免请求过快
    SEND_INTERVAL = 0.5
    
    # Keep an episode body reasonably small so a single LLM extraction call
    # stays fast and cheap, even though local Graphiti/Neo4j has no hard
    # per-episode size limit the way Zep Cloud did.
    MAX_EPISODE_CHARS = 9_500

    def __init__(
        self,
        graph_id: str,
        simulation_id: Optional[str] = None,
        client: Optional[Any] = None,
    ):
        """
        初始化更新器

        Args:
            graph_id: 图谱ID（Graphiti group_id）
            simulation_id: 模拟ID，用于日志与episode命名
            client: 显式注入的Graphiti客户端（可选）。仅供
                `ZepGraphMemoryManager.resume_ingestion` 使用，以确保恢复
                流程使用一个全新构建的客户端而非进程共享的缓存客户端——否则
                凭据修复后仍会用回旧（可能已失效）的客户端。默认为 None，
                此时退回到共享的 `get_zep_client()`，与之前行为一致。
        """
        self.graph_id = graph_id
        self.simulation_id = simulation_id or "unknown"
        self.client = client if client is not None else get_zep_client()

        # Durable write-ahead journal for this simulation's graph ingestion.
        # <sim_dir> uses the same on-disk convention as
        # `SimulationRunner.RUN_STATE_DIR` (both resolve to
        # backend/uploads/simulations/<simulation_id>); imported from
        # `Config` rather than `simulation_runner.py` to avoid a circular
        # import (simulation_runner already imports this module).
        self.sim_dir = os.path.join(Config.OASIS_SIMULATION_DATA_DIR, self.simulation_id)
        self._journal = GraphIngestionJournal(self.sim_dir)
        # Per-platform: byte offset in actions.jsonl this updater has
        # scanned through so far (live tailer path), and the next episode
        # sequence number to hand out. Seeded from whatever was last
        # durably persisted, so a fresh updater for a simulation that has
        # ingested before (e.g. after `resume_ingestion` ran, or a plain
        # process restart) continues instead of re-scanning from byte 0.
        self._scan_pos: Dict[str, int] = {}
        self._next_seq_counter: Dict[str, int] = {}
        for _platform in ("twitter", "reddit"):
            cursor = load_cursor(self.sim_dir, _platform)
            self._scan_pos[_platform] = cursor["batched_offset"]
            self._next_seq_counter[_platform] = cursor["next_seq"]

        # 活动队列
        self._activity_queue: Queue = Queue()

        # 按平台分组的活动缓冲区（每个平台各自累积到BATCH_SIZE后批量发送）
        self._platform_buffers: Dict[str, List[AgentActivity]] = {
            'twitter': [],
            'reddit': [],
        }
        self._buffer_lock = threading.Lock()
        self._acceptance_lock = threading.Lock()

        # 控制标志
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None

        # 统计
        self._total_activities = 0  # 实际添加到队列的活动数
        self._total_sent = 0        # 成功发送的批次数
        self._total_items_sent = 0  # 成功发送的活动条数
        self._failed_count = 0      # 发送失败的批次数
        self._skipped_count = 0     # 被过滤跳过的活动数（DO_NOTHING）
        self._failed_batches: List[Dict[str, Any]] = []

        logger.info(f"ZepGraphMemoryUpdater 初始化完成: graph_id={graph_id}, batch_size={self.BATCH_SIZE}")
    
    def _get_platform_display_name(self, platform: str) -> str:
        """获取平台的显示名称"""
        return self.PLATFORM_DISPLAY_NAMES.get(platform.lower(), platform)
    
    def start(self):
        """启动后台工作线程"""
        if self._running:
            return

        # Capture locale before spawning background thread
        current_locale = get_locale()

        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            args=(current_locale,),
            daemon=True,
            name=f"ZepMemoryUpdater-{self.graph_id[:8]}"
        )
        self._worker_thread.start()
        logger.info(f"ZepGraphMemoryUpdater 已启动: graph_id={self.graph_id}")
    
    def stop(self):
        """Drain the worker and flush tail events.

        Graphiti's `add_episode` blocks until the episode is fully written to
        Neo4j (no separate async "processing" step to poll like Zep Cloud
        had), so once `_flush_remaining` returns, every accepted activity is
        already durably in the graph -- there is nothing left to wait for.
        """
        deadline = time.time() + ZEP_INGESTION_WAIT_TIMEOUT_SECONDS
        # Serialize the accepting->closed transition with add_activity's
        # check+enqueue operation. This closes the small race where a producer
        # could enqueue after both the worker and final flush had exited.
        with self._acceptance_lock:
            self._running = False

        if self._worker_thread and self._worker_thread.is_alive():
            join_timeout = max(0.0, deadline - time.time())
            self._worker_thread.join(timeout=join_timeout)
            if self._worker_thread.is_alive():
                raise TimeoutError(
                    f"Graph memory updater worker did not stop within {join_timeout:.0f}s"
                )

        # The worker has drained the queue. Only now is it safe to flush
        # buffers; doing this before join loses an item already dequeued by the
        # worker but not yet buffered.
        self._flush_remaining(deadline=deadline)

        if self._failed_batches:
            raise RuntimeError(
                f"{len(self._failed_batches)} graph activity batch(es) failed; "
                "simulation graph ingestion is incomplete"
            )

        logger.info(f"ZepGraphMemoryUpdater 已停止: graph_id={self.graph_id}, "
                   f"total_activities={self._total_activities}, "
                   f"batches_sent={self._total_sent}, "
                   f"items_sent={self._total_items_sent}, "
                   f"failed={self._failed_count}, "
                   f"skipped={self._skipped_count}")
    
    def add_activity(self, activity: AgentActivity):
        """
        添加一个agent活动到队列
        
        所有有意义的行为都会被添加到队列，包括：
        - CREATE_POST（发帖）
        - CREATE_COMMENT（评论）
        - QUOTE_POST（引用帖子）
        - SEARCH_POSTS（搜索帖子）
        - SEARCH_USER（搜索用户）
        - LIKE_POST/DISLIKE_POST（点赞/踩帖子）
        - REPOST（转发）
        - FOLLOW（关注）
        - MUTE（屏蔽）
        - LIKE_COMMENT/DISLIKE_COMMENT（点赞/踩评论）
        
        action_args中会包含完整的上下文信息（如帖子原文、用户名等）。
        
        Args:
            activity: Agent活动记录
        """
        # 跳过DO_NOTHING类型的活动
        if activity.action_type == "DO_NOTHING":
            self._skipped_count += 1
            return

        with self._acceptance_lock:
            if not self._running:
                raise RuntimeError("Graph memory updater is not running")
            self._activity_queue.put(activity)
            self._total_activities += 1
        logger.debug(f"添加活动到图谱更新队列: {activity.agent_name} - {activity.action_type}")
    
    def add_activity_from_dict(self, data: Dict[str, Any], platform: str):
        """
        从字典数据添加活动

        Args:
            data: 从actions.jsonl解析的字典数据
            platform: 平台名称 (twitter/reddit)
        """
        # 跳过事件类型的条目
        if "event_type" in data:
            return

        # Determine this line's exact byte range in actions.jsonl *before*
        # any downstream filtering (success=False, DO_NOTHING), and
        # unconditionally -- SimulationRunner._read_action_log calls this
        # method exactly once per non-event line in file order, so our own
        # independent forward scan (starting from wherever it last left
        # off) stays in lockstep and lands on the same line every time.
        # Skipping the scan for a filtered-out entry would desync our
        # cursor from the real file.
        journal_start, journal_end = self._scan_next_activity_line(platform.lower())

        if data.get("success") is False:
            self._skipped_count += 1
            return

        activity = _agent_activity_from_action_data(data, platform)
        activity.journal_start = journal_start
        activity.journal_end = journal_end

        self.add_activity(activity)

    def _scan_next_activity_line(self, platform: str) -> Tuple[Optional[int], Optional[int]]:
        """Advance this updater's own read cursor over actions.jsonl for
        *platform* by exactly one non-event line, and return that line's
        `(start, end)` byte range.

        This is a best-effort correlation, not a hard dependency: if the
        real actions.jsonl is missing (synthetic/unit-test activities that
        never actually came from a logged file) or the scan cannot find a
        matching line, this returns `(None, None)` and the caller falls
        back to un-journaled behavior for that one activity -- it never
        raises, since a missing byte range must not block ingestion.
        """

        path = actions_log_path(self.sim_dir, platform)
        start_pos = self._scan_pos.get(platform, 0)
        try:
            for line_start, line_end, parsed in iter_action_lines(path, start_pos):
                self._scan_pos[platform] = line_end
                if parsed is None:
                    # Malformed line -- SimulationRunner's tailer silently
                    # skips these too (`except json.JSONDecodeError: pass`);
                    # keep scanning past it.
                    continue
                if "event_type" in parsed:
                    continue
                return line_start, line_end
        except (FileNotFoundError, OSError):
            return None, None
        return None, None
    
    def _worker_loop(self, locale: str = 'zh'):
        """后台工作循环 - 按平台批量发送活动到图谱"""
        set_locale(locale)
        while self._running or not self._activity_queue.empty():
            try:
                # 尝试从队列获取活动（超时1秒）
                try:
                    activity = self._activity_queue.get(timeout=1)
                    
                    # 将活动添加到对应平台的缓冲区
                    platform = activity.platform.lower()
                    batch = None
                    with self._buffer_lock:
                        if platform not in self._platform_buffers:
                            self._platform_buffers[platform] = []
                        self._platform_buffers[platform].append(activity)
                        
                        # 检查该平台是否达到批量大小
                        if len(self._platform_buffers[platform]) >= self.BATCH_SIZE:
                            batch = self._platform_buffers[platform][:self.BATCH_SIZE]
                            self._platform_buffers[platform] = self._platform_buffers[platform][self.BATCH_SIZE:]

                    # Never hold the buffer lock across network I/O or sleep.
                    if batch:
                        self._send_batch_activities(batch, platform)
                        time.sleep(self.SEND_INTERVAL)
                    
                except Empty:
                    pass
                    
            except Exception as e:
                logger.error(f"工作循环异常: {e}")
                time.sleep(1)
    
    def _build_episode_payloads(
        self,
        activities: List[AgentActivity],
    ) -> List[tuple[List[AgentActivity], str]]:
        payloads: List[tuple[List[AgentActivity], str]] = []
        current_activities: List[AgentActivity] = []
        current_lines: List[str] = []
        current_length = 0

        for activity in activities:
            text = _truncate_for_episode(activity.to_episode_text(), self.MAX_EPISODE_CHARS)
            projected_length = current_length + (1 if current_lines else 0) + len(text)
            if current_lines and projected_length > self.MAX_EPISODE_CHARS:
                payloads.append((current_activities, "\n".join(current_lines)))
                current_activities = []
                current_lines = []
                current_length = 0
            current_activities.append(activity)
            current_lines.append(text)
            current_length += (1 if len(current_lines) > 1 else 0) + len(text)

        if current_lines:
            payloads.append((current_activities, "\n".join(current_lines)))
        return payloads

    def _send_batch_activities(
        self,
        activities: List[AgentActivity],
        platform: str,
        *,
        deadline: float | None = None,
    ) -> int:
        """
        批量发送活动到图谱（合并为一条文本）
        
        Args:
            activities: Agent活动列表
            platform: 平台名称
        """
        if not activities:
            return 0

        processed_count = 0
        for payload_activities, combined_text in self._build_episode_payloads(activities):
            if deadline is not None and time.time() >= deadline:
                raise _DrainDeadlineExceeded(processed_count)

            seq = self._reserve_seq(platform)
            first_round = min(a.round_num for a in payload_activities)
            last_round = max(a.round_num for a in payload_activities)
            # Deterministic name (no timestamp/random suffix): replaying the
            # exact same (simulation_id, platform, seq, round range) always
            # produces the exact same name, which is what makes a resend
            # during resume_ingestion detectable in Neo4j (fetch_all_episodes
            # + name match) instead of silently duplicating the episode.
            episode_name = (
                f"{self.simulation_id}_{platform}_seq{seq:06d}_r{first_round}-{last_round}"
            )

            byte_range = self._resolve_payload_byte_range(payload_activities)
            journaled = byte_range is not None
            if journaled:
                byte_start, byte_end = byte_range
                digest = hashlib.sha256(combined_text.encode("utf-8")).hexdigest()
                # Write-ahead ordering (the whole point of this module):
                # 1) INTENT is fsync'd, 2) the cursor is advanced atomically,
                # and *only then* 3) add_episode is attempted -- the one step
                # that can be interrupted unknowably (process killed, LLM
                # provider dies mid-extraction, machine sleeps). Steps 1-2
                # are local fsync'd writes that always complete first, so a
                # crash between them and step 3 leaves a dangling INTENT that
                # is itself the durable record of an ambiguous batch --
                # resume_ingestion is the only place that ever resolves it.
                self._journal.append_intent(
                    seq=seq,
                    platform=platform,
                    episode_name=episode_name,
                    start=byte_start,
                    end=byte_end,
                    sha256=digest,
                )
                save_cursor(
                    self.sim_dir,
                    platform,
                    batched_offset=byte_end,
                    next_seq=seq + 1,
                )

            try:
                # Graphiti's add_episode has no generic metadata bag like
                # Zep Cloud's graph.add did (name/source_description/source/
                # group_id only); the per-activity detail that used to live
                # in that metadata dict (round numbers, agent ids, action
                # types) is already embedded inline in combined_text by
                # AgentActivity.to_episode_text().
                result = run_async(
                    self.client.add_episode(
                        name=episode_name,
                        episode_body=combined_text,
                        source_description=(
                            f"MiroFish simulation {self.simulation_id} "
                            f"({platform}) activity batch"
                        ),
                        reference_time=self._to_datetime(payload_activities[-1].timestamp),
                        source=EpisodeType.text,
                        group_id=self.graph_id,
                    )
                )

                episode_uuid = result.episode.uuid
                if journaled:
                    self._journal.append_committed(
                        seq=seq,
                        platform=platform,
                        episode_name=episode_name,
                        episode_uuid=episode_uuid,
                    )
                self._total_sent += 1
                self._total_items_sent += len(payload_activities)
                display_name = self._get_platform_display_name(platform)
                logger.info(f"成功批量发送 {len(payload_activities)} 条{display_name}活动到图谱 {self.graph_id} (episode={episode_uuid})")
                logger.debug(f"批量内容预览: {combined_text[:200]}...")

            except Exception as e:
                # add_episode has no idempotency key either. Replaying after
                # an ambiguous failure can duplicate extracted facts, so fail
                # closed and surface the incomplete batch to SimulationRunner
                # instead of retrying automatically. When journaled, the
                # dangling INTENT (no matching COMMITTED) already written
                # above *is* the durable record of this ambiguity -- nothing
                # further to persist here. Recovery is only ever done
                # explicitly via ZepGraphMemoryManager.resume_ingestion.
                logger.error(f"批量发送到图谱失败，未自动重放非幂等写入: {e}")
                self._failed_count += 1
                self._failed_batches.append({
                    "platform": platform,
                    "activities": payload_activities,
                    "error": str(e),
                })
            finally:
                # Successes are already durably written (add_episode blocks
                # until Neo4j ingestion completes); failures are kept in
                # _failed_batches and must never be replayed. Either way this
                # payload is accounted for before moving on.
                processed_count += len(payload_activities)
        return processed_count

    def _reserve_seq(self, platform: str) -> int:
        """Hand out the next per-platform episode sequence number.

        Purely in-memory bookkeeping; the persisted `next_seq` in
        `cursor_{platform}.json` is only advanced once a batch is actually
        journaled (see `_send_batch_activities`), so un-journaled sends
        (synthetic activities with no real actions.jsonl backing) never
        touch disk -- they still get a unique, monotonically increasing
        name suffix for the lifetime of this process, just not a durable
        one.
        """

        seq = self._next_seq_counter.get(platform, 1)
        self._next_seq_counter[platform] = seq + 1
        return seq

    @staticmethod
    def _resolve_payload_byte_range(
        payload_activities: List[AgentActivity],
    ) -> Optional[Tuple[int, int]]:
        """Return the `[start, end)` byte range in actions.jsonl spanned by
        every activity in *payload_activities*, or None if any of them is
        missing byte-range info (not sourced from a real on-disk
        actions.jsonl -- e.g. constructed directly via `add_activity` in
        tests). All-or-nothing: a payload is either fully journaled or not
        journaled at all, never partially.
        """

        if not payload_activities:
            return None
        if any(a.journal_start is None or a.journal_end is None for a in payload_activities):
            return None
        return payload_activities[0].journal_start, payload_activities[-1].journal_end

    @staticmethod
    def _to_datetime(value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.astimezone()
            return parsed
        except (AttributeError, TypeError, ValueError):
            return datetime.now(timezone.utc)

    def _flush_remaining(self, *, deadline: float | None = None):
        """发送队列和缓冲区中剩余的活动"""
        # 首先处理队列中剩余的活动，添加到缓冲区
        while not self._activity_queue.empty():
            try:
                activity = self._activity_queue.get_nowait()
                platform = activity.platform.lower()
                with self._buffer_lock:
                    if platform not in self._platform_buffers:
                        self._platform_buffers[platform] = []
                    self._platform_buffers[platform].append(activity)
            except Empty:
                break
        
        for platform in list(self._platform_buffers):
            with self._buffer_lock:
                buffer = list(self._platform_buffers.get(platform, []))
            if not buffer:
                continue
            display_name = self._get_platform_display_name(platform)
            logger.info(f"发送{display_name}平台剩余的 {len(buffer)} 条活动")
            if deadline is not None and time.time() >= deadline:
                raise TimeoutError(
                    "Graph memory updater drain deadline elapsed before flushing all activities"
                )
            try:
                processed_count = self._send_batch_activities(
                    buffer,
                    platform,
                    deadline=deadline,
                )
            except _DrainDeadlineExceeded as error:
                with self._buffer_lock:
                    del self._platform_buffers[platform][:error.processed_count]
                raise TimeoutError(str(error)) from error
            else:
                with self._buffer_lock:
                    del self._platform_buffers[platform][:processed_count]

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._buffer_lock:
            buffer_sizes = {p: len(b) for p, b in self._platform_buffers.items()}
        
        return {
            "graph_id": self.graph_id,
            "batch_size": self.BATCH_SIZE,
            "total_activities": self._total_activities,  # 添加到队列的活动总数
            "batches_sent": self._total_sent,            # 成功发送的批次数
            "items_sent": self._total_items_sent,        # 成功发送的活动条数
            "failed_count": self._failed_count,          # 发送失败的批次数
            "skipped_count": self._skipped_count,        # 被过滤跳过的活动数（DO_NOTHING）
            "queue_size": self._activity_queue.qsize(),
            "buffer_sizes": buffer_sizes,                # 各平台缓冲区大小
            "running": self._running,
        }


class ZepGraphMemoryManager:
    """
    管理多个模拟的图谱记忆更新器
    
    每个模拟可以有自己的更新器实例
    """
    
    _updaters: Dict[str, ZepGraphMemoryUpdater] = {}
    _lock = threading.Lock()
    
    @classmethod
    def create_updater(cls, simulation_id: str, graph_id: str) -> ZepGraphMemoryUpdater:
        """
        为模拟创建图谱记忆更新器
        
        Args:
            simulation_id: 模拟ID
            graph_id: 图谱ID
            
        Returns:
            ZepGraphMemoryUpdater实例
        """
        with cls._lock:
            # 如果已存在，先停止旧的
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
            
            updater = ZepGraphMemoryUpdater(
                graph_id,
                simulation_id=simulation_id,
            )
            updater.start()
            cls._updaters[simulation_id] = updater
            cls._stop_all_done = False
            
            logger.info(f"创建图谱记忆更新器: simulation_id={simulation_id}, graph_id={graph_id}")
            return updater
    
    @classmethod
    def get_updater(cls, simulation_id: str) -> Optional[ZepGraphMemoryUpdater]:
        """获取模拟的更新器"""
        with cls._lock:
            return cls._updaters.get(simulation_id)

    @classmethod
    def get_simulation_ids_for_graph(cls, graph_id: str) -> List[str]:
        """Return simulations whose updater still owns or drains this graph."""

        with cls._lock:
            return sorted(
                simulation_id
                for simulation_id, updater in cls._updaters.items()
                if updater.graph_id == graph_id
            )

    @classmethod
    def get_simulation_ids(cls) -> List[str]:
        """Return every simulation with a retained updater."""

        with cls._lock:
            return sorted(cls._updaters)

    @classmethod
    def discard_inactive_updater(cls, simulation_id: str) -> bool:
        """Discard a failed, fully stopped updater during graph destruction."""

        with cls._lock:
            updater = cls._updaters.get(simulation_id)
            if updater is None:
                return False
            worker_alive = bool(
                updater._worker_thread and updater._worker_thread.is_alive()
            )
            if updater._running or worker_alive:
                raise RuntimeError(
                    f"Graph memory updater for {simulation_id} is still active"
                )
            cls._updaters.pop(simulation_id, None)
        logger.warning(
            "Discarded incomplete graph memory updater during explicit graph deletion: "
            "simulation_id=%s, graph_id=%s",
            simulation_id,
            updater.graph_id,
        )
        return True
    
    @classmethod
    def stop_updater(cls, simulation_id: str):
        """停止并移除模拟的更新器"""
        with cls._lock:
            updater = cls._updaters.get(simulation_id)
        if updater is None:
            return

        # Do not hold the manager lock through up to several minutes of Cloud
        # polling. Crucially, only remove the updater after a successful drain;
        # on failure it remains visible to report/deletion barriers and can be
        # stopped again.
        updater.stop()
        with cls._lock:
            if cls._updaters.get(simulation_id) is updater:
                cls._updaters.pop(simulation_id, None)
        logger.info(f"已停止图谱记忆更新器: simulation_id={simulation_id}")
    
    # 防止 stop_all 重复调用的标志
    _stop_all_done = False
    
    @classmethod
    def stop_all(cls):
        """停止所有更新器"""
        # 防止重复调用
        if cls._stop_all_done:
            return

        with cls._lock:
            simulation_ids = list(cls._updaters)

        errors = []
        for simulation_id in simulation_ids:
            try:
                cls.stop_updater(simulation_id)
            except Exception as error:
                # Keep a failed updater registered so the caller can retry and
                # lifecycle/report guards still see the incomplete ingestion.
                logger.error(
                    "停止更新器失败: simulation_id=%s, error=%s",
                    simulation_id,
                    error,
                )
                errors.append((simulation_id, error))

        with cls._lock:
            cls._stop_all_done = not cls._updaters

        if errors:
            details = "; ".join(
                f"{simulation_id}: {error}"
                for simulation_id, error in errors
            )
            raise RuntimeError(f"部分图谱更新器未完整停止: {details}")
        logger.info("已停止所有图谱记忆更新器")
    
    @classmethod
    def get_all_stats(cls) -> Dict[str, Dict[str, Any]]:
        """获取所有更新器的统计信息"""
        return {
            sim_id: updater.get_stats()
            for sim_id, updater in cls._updaters.items()
        }

    @classmethod
    def clear_simulation_episodes(cls, graph_id: str, simulation_id: str) -> int:
        """Remove every episode a specific simulation run contributed to
        `graph_id`, plus any node/edge exclusively derived from those
        episodes -- without touching the project's base seed graph (built
        once in the "Map Construction" step, before any simulation runs) or
        another simulation's episodes.

        Must be called before force-restarting a simulation. `add_episode`
        writes here name every episode `f"{simulation_id}_{platform}_..."`
        (see `_send_batch` above); force-restarting a simulation without
        this reset the local run-state files (`SimulationRunner.
        cleanup_simulation_logs`) but left the abandoned run's agent-action
        history sitting in the graph, so the new run's memory ended up
        polluted with events from a timeline it never actually experienced.

        `Graphiti.remove_episode` only deletes nodes/edges that are *solely*
        mentioned by the episode being removed (it counts other mentioning
        episodes first), so entities the base seed graph or another
        simulation also reference are preserved correctly.

        Returns the number of episodes removed.
        """
        client = get_zep_client()
        prefix = f"{simulation_id}_"
        episodes = fetch_all_episodes(client, graph_id)
        matches = [ep for ep in episodes if ep.name.startswith(prefix)]

        removed = 0
        for episode in matches:
            run_async(client.remove_episode(episode.uuid))
            removed += 1

        logger.info(
            "已清除模拟 %s 在图谱 %s 中贡献的 %d 个episode（共扫描 %d 个）",
            simulation_id,
            graph_id,
            removed,
            len(episodes),
        )
        return removed

    @classmethod
    def resume_ingestion(cls, simulation_id: str, graph_id: str) -> Dict[str, Any]:
        """Finish a graph-ingestion drain that was interrupted by a crash, a
        killed process, or a since-fixed credential/provider failure.

        Must be called with no live updater for `simulation_id` (the API
        route enforces this -- see `POST
        /api/simulation/<simulation_id>/graph/retry-ingestion` in
        `app/api/simulation.py`); it reads directly from the durable
        journal + actions.jsonl on disk, so it works even with no running
        subprocess.

        Two phases:

        1. Reconcile every dangling INTENT (no matching COMMITTED) written
           by a previous run. `fetch_all_episodes` is called *once* for the
           whole pass (never per-batch -- it pages all of Neo4j) to build
           the set of episode names that actually made it in. For each
           pending seq: name present -> it really did commit; synthesize
           the missing COMMITTED record and do NOT resend. Name absent ->
           it never landed; rebuild the exact original episode text from
           the recorded byte range, verify its sha256 still matches (a
           mismatch means actions.jsonl changed since the crash -- e.g. a
           force-restart truncated it -- so replay is refused rather than
           risking sending the wrong text under an already-used name), and
           resend under the identical deterministic name.

        2. Resume tailing: once every pending INTENT is resolved, each
           platform's persisted `batched_offset` correctly points past
           every batch that was ever attempted. Anything appended to
           actions.jsonl beyond that (activities that were durably logged
           but the process died before they were ever batched -- e.g. fewer
           than BATCH_SIZE had accumulated when it died) is read, batched
           the same way the live worker does, and sent through the normal
           write-ahead path.

        A fresh Graphiti client is constructed for this call (bypassing
        `get_zep_client()`'s process-wide `@lru_cache`) so a credential fix
        applied via the settings UI after the failure is actually picked up
        -- a long-lived `ZepGraphMemoryUpdater` pins whatever client was
        cached at construction and would otherwise keep using the stale
        one.

        Returns `{"pending_before", "resent", "already_committed",
        "still_failed", "tail_items_sent"}`.
        """

        sim_dir = os.path.join(Config.OASIS_SIMULATION_DATA_DIR, simulation_id)
        journal = GraphIngestionJournal(sim_dir)
        pending = journal.pending_intents()

        result: Dict[str, Any] = {
            "pending_before": len(pending),
            "resent": 0,
            "already_committed": 0,
            "still_failed": 0,
        }

        # Bypass get_zep_client()'s @lru_cache(maxsize=1) deliberately (see
        # docstring above). `.__wrapped__` is functools.lru_cache's own
        # documented handle to the undecorated function -- the only way to
        # get a genuinely fresh client without editing zep.py, which is
        # outside this change's file ownership.
        fresh_client = _cached_graphiti_client.__wrapped__(
            Config.NEO4J_URI, Config.NEO4J_USER, Config.NEO4J_PASSWORD
        )
        try:
            if pending:
                episodes = fetch_all_episodes(fresh_client, graph_id)
                existing_uuid_by_name = {ep.name: ep.uuid for ep in episodes}

                for intent in pending:
                    cls._reconcile_one_intent(
                        journal,
                        sim_dir,
                        simulation_id,
                        graph_id,
                        fresh_client,
                        intent,
                        existing_uuid_by_name,
                        result,
                    )

            tail_updater = ZepGraphMemoryUpdater(
                graph_id, simulation_id=simulation_id, client=fresh_client
            )
            for platform in ("twitter", "reddit"):
                cls._drain_tail_for_platform(tail_updater, sim_dir, platform)

            result["tail_items_sent"] = tail_updater._total_items_sent
            if tail_updater._failed_batches:
                result["still_failed"] += len(tail_updater._failed_batches)
        finally:
            close_client(fresh_client)

        logger.info(
            "resume_ingestion 完成: simulation_id=%s, graph_id=%s, %s",
            simulation_id,
            graph_id,
            result,
        )
        return result

    @classmethod
    def _reconcile_one_intent(
        cls,
        journal: "GraphIngestionJournal",
        sim_dir: str,
        simulation_id: str,
        graph_id: str,
        fresh_client: Any,
        intent: Dict[str, Any],
        existing_uuid_by_name: Dict[str, str],
        result: Dict[str, Any],
    ) -> None:
        seq = intent.get("seq")
        platform = intent.get("platform")
        episode_name = intent.get("episode_name")

        existing_uuid = existing_uuid_by_name.get(episode_name)
        if existing_uuid is not None:
            # It really did commit before the crash; Neo4j has it under
            # this exact deterministic name. Synthesize the missing
            # COMMITTED record instead of resending -- resending would
            # duplicate the extracted facts.
            journal.append_committed(
                seq=seq,
                platform=platform,
                episode_name=episode_name,
                episode_uuid=existing_uuid,
            )
            result["already_committed"] += 1
            return

        rebuild = _rebuild_intent_episode_text(sim_dir, intent)
        if rebuild is None:
            result["still_failed"] += 1
            logger.error(
                "resume_ingestion: 拒绝重放 seq=%s (%s)：actions.jsonl 字节范围 "
                "[%s,%s) 不可读或sha256不匹配，重建内容可能已不再与原始批次一致",
                seq,
                episode_name,
                intent.get("start"),
                intent.get("end"),
            )
            return

        combined_text, reference_time = rebuild
        try:
            add_result = run_async(
                fresh_client.add_episode(
                    name=episode_name,
                    episode_body=combined_text,
                    source_description=(
                        f"MiroFish simulation {simulation_id} "
                        f"({platform}) activity batch (resumed)"
                    ),
                    reference_time=reference_time,
                    source=EpisodeType.text,
                    group_id=graph_id,
                )
            )
        except Exception as error:
            result["still_failed"] += 1
            logger.error(
                "resume_ingestion: 重放 seq=%s (%s) 失败，仍保持为未提交: %s",
                seq,
                episode_name,
                error,
            )
            return

        journal.append_committed(
            seq=seq,
            platform=platform,
            episode_name=episode_name,
            episode_uuid=add_result.episode.uuid,
        )
        result["resent"] += 1

    @classmethod
    def _drain_tail_for_platform(
        cls,
        updater: "ZepGraphMemoryUpdater",
        sim_dir: str,
        platform: str,
    ) -> None:
        """Send every activity durably logged after the platform's
        persisted `batched_offset` that was never batched (the process died
        before BATCH_SIZE activities accumulated, or before `stop()`'s
        final flush ever ran). One-shot: batches everything available right
        now into BATCH_SIZE-sized groups (plus a final partial group) and
        sends each through the normal write-ahead `_send_batch_activities`
        path, exactly like the live worker would have.
        """

        path = actions_log_path(sim_dir, platform)
        if not os.path.exists(path):
            return

        cursor = load_cursor(sim_dir, platform)
        activities: List[AgentActivity] = []
        for line_start, line_end, parsed in iter_action_lines(path, cursor["batched_offset"]):
            if parsed is None or "event_type" in parsed:
                continue
            if parsed.get("success") is False:
                continue
            activity = _agent_activity_from_action_data(parsed, platform)
            if activity.action_type == "DO_NOTHING":
                continue
            activity.journal_start = line_start
            activity.journal_end = line_end
            activities.append(activity)

        for i in range(0, len(activities), updater.BATCH_SIZE):
            batch = activities[i : i + updater.BATCH_SIZE]
            updater._send_batch_activities(batch, platform)
