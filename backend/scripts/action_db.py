"""
Trace 表读取与动作上下文补充

从各平台模拟的 sqlite 数据库（trace 表）中读取新产生的动作，并为每条
动作补充可读的上下文信息（帖子内容、评论内容、用户名等），供
action_logger.PlatformActionLogger 写入 actions.jsonl 使用。

本模块与 run_parallel_simulation.py / run_twitter_simulation.py /
run_reddit_simulation.py 三个模拟脚本一样，是刻意与后端 Flask 应用
解耦的独立模块：不 import 任何 app.* 包，避免被三个脚本 import 时
连带拉入 Flask、flask_cors、graphiti_core、sentence_transformers 等
一整套 ML 依赖（这些脚本以 subprocess.Popen 启动为独立子进程，只需要
读写 sqlite 和 JSON 文件本身）。

此前这一整套函数在三个模拟脚本里各自维护一份拷贝（三份完全相同的
代码，只有文档字符串的详略程度不同），现在提取为脚本共享模块，
三个脚本改为直接 import 使用。
"""

import os
import sqlite3
import json
from typing import Dict, Any, List, Optional, Tuple


# 需要过滤掉的非核心动作类型（这些动作对分析价值较低）
FILTERED_ACTIONS = {'refresh', 'sign_up'}

# 动作类型映射表（数据库中的名称 -> 标准名称）
ACTION_TYPE_MAP = {
    'create_post': 'CREATE_POST',
    'like_post': 'LIKE_POST',
    'dislike_post': 'DISLIKE_POST',
    'repost': 'REPOST',
    'quote_post': 'QUOTE_POST',
    'follow': 'FOLLOW',
    'mute': 'MUTE',
    'create_comment': 'CREATE_COMMENT',
    'like_comment': 'LIKE_COMMENT',
    'dislike_comment': 'DISLIKE_COMMENT',
    'search_posts': 'SEARCH_POSTS',
    'search_user': 'SEARCH_USER',
    'trend': 'TREND',
    'do_nothing': 'DO_NOTHING',
    'interview': 'INTERVIEW',
}


def get_agent_names_from_config(config: Dict[str, Any]) -> Dict[int, str]:
    """
    从 simulation_config 中获取 agent_id -> entity_name 的映射

    这样可以在 actions.jsonl 中显示真实的实体名称，而不是 "Agent_0" 这样的代号

    Args:
        config: simulation_config.json 的内容

    Returns:
        agent_id -> entity_name 的映射字典
    """
    agent_names = {}
    agent_configs = config.get("agent_configs", [])

    for agent_config in agent_configs:
        agent_id = agent_config.get("agent_id")
        entity_name = agent_config.get("entity_name", f"Agent_{agent_id}")
        if agent_id is not None:
            agent_names[agent_id] = entity_name

    return agent_names


def fetch_new_actions_from_db(
    db_path: str,
    last_rowid: int,
    agent_names: Dict[int, str]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    从数据库中获取新的动作记录，并补充完整的上下文信息

    Args:
        db_path: 数据库文件路径
        last_rowid: 上次读取的最大 rowid 值（使用 rowid 而不是 created_at，因为不同平台的 created_at 格式不同）
        agent_names: agent_id -> agent_name 映射

    Returns:
        (actions_list, new_last_rowid)
        - actions_list: 动作列表，每个元素包含 agent_id, agent_name, action_type, action_args（含上下文信息）
        - new_last_rowid: 新的最大 rowid 值
    """
    actions = []
    new_last_rowid = last_rowid

    if not os.path.exists(db_path):
        return actions, new_last_rowid

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 使用 rowid 来追踪已处理的记录（rowid 是 SQLite 的内置自增字段）
        # 这样可以避免 created_at 格式差异问题（Twitter 用整数，Reddit 用日期时间字符串）
        cursor.execute("""
            SELECT rowid, user_id, action, info
            FROM trace
            WHERE rowid > ?
            ORDER BY rowid ASC
        """, (last_rowid,))

        for rowid, user_id, action, info_json in cursor.fetchall():
            # 更新最大 rowid
            new_last_rowid = rowid

            # 过滤非核心动作
            if action in FILTERED_ACTIONS:
                continue

            # 解析动作参数
            try:
                action_args = json.loads(info_json) if info_json else {}
            except json.JSONDecodeError:
                action_args = {}

            # 精简 action_args，只保留关键字段（保留完整内容，不截断）
            simplified_args = {}
            if 'content' in action_args:
                simplified_args['content'] = action_args['content']
            if 'post_id' in action_args:
                simplified_args['post_id'] = action_args['post_id']
            if 'comment_id' in action_args:
                simplified_args['comment_id'] = action_args['comment_id']
            if 'quoted_id' in action_args:
                simplified_args['quoted_id'] = action_args['quoted_id']
            if 'new_post_id' in action_args:
                simplified_args['new_post_id'] = action_args['new_post_id']
            if 'follow_id' in action_args:
                simplified_args['follow_id'] = action_args['follow_id']
            if 'query' in action_args:
                simplified_args['query'] = action_args['query']
            if 'like_id' in action_args:
                simplified_args['like_id'] = action_args['like_id']
            if 'dislike_id' in action_args:
                simplified_args['dislike_id'] = action_args['dislike_id']

            # 转换动作类型名称
            action_type = ACTION_TYPE_MAP.get(action, action.upper())

            # 补充上下文信息（帖子内容、用户名等）
            _enrich_action_context(cursor, action_type, simplified_args, agent_names)

            actions.append({
                'agent_id': user_id,
                'agent_name': agent_names.get(user_id, f'Agent_{user_id}'),
                'action_type': action_type,
                'action_args': simplified_args,
            })

        conn.close()
    except Exception as e:
        print(f"读取数据库动作失败: {e}")

    return actions, new_last_rowid


def _enrich_action_context(
    cursor,
    action_type: str,
    action_args: Dict[str, Any],
    agent_names: Dict[int, str]
) -> None:
    """
    为动作补充上下文信息（帖子内容、用户名等）

    Args:
        cursor: 数据库游标
        action_type: 动作类型
        action_args: 动作参数（会被修改）
        agent_names: agent_id -> agent_name 映射
    """
    try:
        # 点赞/踩帖子：补充帖子内容和作者
        if action_type in ('LIKE_POST', 'DISLIKE_POST'):
            post_id = action_args.get('post_id')
            if post_id:
                post_info = _get_post_info(cursor, post_id, agent_names)
                if post_info:
                    action_args['post_content'] = post_info.get('content', '')
                    action_args['post_author_name'] = post_info.get('author_name', '')

        # 转发帖子：补充原帖内容和作者
        elif action_type == 'REPOST':
            new_post_id = action_args.get('new_post_id')
            if new_post_id:
                # 转发帖子的 original_post_id 指向原帖
                cursor.execute("""
                    SELECT original_post_id FROM post WHERE post_id = ?
                """, (new_post_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    original_post_id = row[0]
                    original_info = _get_post_info(cursor, original_post_id, agent_names)
                    if original_info:
                        action_args['original_content'] = original_info.get('content', '')
                        action_args['original_author_name'] = original_info.get('author_name', '')

        # 引用帖子：补充原帖内容、作者和引用评论
        elif action_type == 'QUOTE_POST':
            quoted_id = action_args.get('quoted_id')
            new_post_id = action_args.get('new_post_id')

            if quoted_id:
                original_info = _get_post_info(cursor, quoted_id, agent_names)
                if original_info:
                    action_args['original_content'] = original_info.get('content', '')
                    action_args['original_author_name'] = original_info.get('author_name', '')

            # 获取引用帖子的评论内容（quote_content）
            if new_post_id:
                cursor.execute("""
                    SELECT quote_content FROM post WHERE post_id = ?
                """, (new_post_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    action_args['quote_content'] = row[0]

        # 关注用户：补充被关注用户的名称
        elif action_type == 'FOLLOW':
            follow_id = action_args.get('follow_id')
            if follow_id:
                # 从 follow 表获取 followee_id
                cursor.execute("""
                    SELECT followee_id FROM follow WHERE follow_id = ?
                """, (follow_id,))
                row = cursor.fetchone()
                if row:
                    followee_id = row[0]
                    target_name = _get_user_name(cursor, followee_id, agent_names)
                    if target_name:
                        action_args['target_user_name'] = target_name

        # 屏蔽用户：补充被屏蔽用户的名称
        elif action_type == 'MUTE':
            # 从 action_args 中获取 user_id 或 target_id
            target_id = action_args.get('user_id') or action_args.get('target_id')
            if target_id:
                target_name = _get_user_name(cursor, target_id, agent_names)
                if target_name:
                    action_args['target_user_name'] = target_name

        # 点赞/踩评论：补充评论内容和作者
        elif action_type in ('LIKE_COMMENT', 'DISLIKE_COMMENT'):
            comment_id = action_args.get('comment_id')
            if comment_id:
                comment_info = _get_comment_info(cursor, comment_id, agent_names)
                if comment_info:
                    action_args['comment_content'] = comment_info.get('content', '')
                    action_args['comment_author_name'] = comment_info.get('author_name', '')

        # 发表评论：补充所评论的帖子信息
        elif action_type == 'CREATE_COMMENT':
            post_id = action_args.get('post_id')
            if post_id:
                post_info = _get_post_info(cursor, post_id, agent_names)
                if post_info:
                    action_args['post_content'] = post_info.get('content', '')
                    action_args['post_author_name'] = post_info.get('author_name', '')

    except Exception as e:
        # 补充上下文失败不影响主流程
        print(f"补充动作上下文失败: {e}")


def _get_post_info(
    cursor,
    post_id: int,
    agent_names: Dict[int, str]
) -> Optional[Dict[str, str]]:
    """
    获取帖子信息

    Args:
        cursor: 数据库游标
        post_id: 帖子ID
        agent_names: agent_id -> agent_name 映射

    Returns:
        包含 content 和 author_name 的字典，或 None
    """
    try:
        cursor.execute("""
            SELECT p.content, p.user_id, u.agent_id
            FROM post p
            LEFT JOIN user u ON p.user_id = u.user_id
            WHERE p.post_id = ?
        """, (post_id,))
        row = cursor.fetchone()
        if row:
            content = row[0] or ''
            user_id = row[1]
            agent_id = row[2]

            # 优先使用 agent_names 中的名称
            author_name = ''
            if agent_id is not None and agent_id in agent_names:
                author_name = agent_names[agent_id]
            elif user_id:
                # 从 user 表获取名称
                cursor.execute("SELECT name, user_name FROM user WHERE user_id = ?", (user_id,))
                user_row = cursor.fetchone()
                if user_row:
                    author_name = user_row[0] or user_row[1] or ''

            return {'content': content, 'author_name': author_name}
    except Exception:
        pass
    return None


def _get_user_name(
    cursor,
    user_id: int,
    agent_names: Dict[int, str]
) -> Optional[str]:
    """
    获取用户名称

    Args:
        cursor: 数据库游标
        user_id: 用户ID
        agent_names: agent_id -> agent_name 映射

    Returns:
        用户名称，或 None
    """
    try:
        cursor.execute("""
            SELECT agent_id, name, user_name FROM user WHERE user_id = ?
        """, (user_id,))
        row = cursor.fetchone()
        if row:
            agent_id = row[0]
            name = row[1]
            user_name = row[2]

            # 优先使用 agent_names 中的名称
            if agent_id is not None and agent_id in agent_names:
                return agent_names[agent_id]
            return name or user_name or ''
    except Exception:
        pass
    return None


def _get_comment_info(
    cursor,
    comment_id: int,
    agent_names: Dict[int, str]
) -> Optional[Dict[str, str]]:
    """
    获取评论信息

    Args:
        cursor: 数据库游标
        comment_id: 评论ID
        agent_names: agent_id -> agent_name 映射

    Returns:
        包含 content 和 author_name 的字典，或 None
    """
    try:
        cursor.execute("""
            SELECT c.content, c.user_id, u.agent_id
            FROM comment c
            LEFT JOIN user u ON c.user_id = u.user_id
            WHERE c.comment_id = ?
        """, (comment_id,))
        row = cursor.fetchone()
        if row:
            content = row[0] or ''
            user_id = row[1]
            agent_id = row[2]

            # 优先使用 agent_names 中的名称
            author_name = ''
            if agent_id is not None and agent_id in agent_names:
                author_name = agent_names[agent_id]
            elif user_id:
                # 从 user 表获取名称
                cursor.execute("SELECT name, user_name FROM user WHERE user_id = ?", (user_id,))
                user_row = cursor.fetchone()
                if user_row:
                    author_name = user_row[0] or user_row[1] or ''

            return {'content': content, 'author_name': author_name}
    except Exception:
        pass
    return None
