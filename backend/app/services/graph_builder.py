"""
图谱构建服务
接口2：使用 Graphiti + 本地 Neo4j 构建图谱（graph_id 即 Graphiti 的 group_id）
"""

import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass

from graphiti_core.nodes import EpisodeType
from graphiti_core.utils.maintenance import clear_data

from ..models.task import TaskManager, TaskStatus
from ..utils.zep_paging import fetch_all_nodes, fetch_all_edges
from ..utils.zep import get_zep_client, run_async
from .ontology_generator import OntologyGenerator
from .text_processor import TextProcessor
from ..utils.locale import t, get_locale, set_locale


@dataclass
class GraphInfo:
    """图谱信息"""
    graph_id: str
    node_count: int
    edge_count: int
    entity_types: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "entity_types": self.entity_types,
        }


class GraphBuilderService:
    """
    图谱构建服务
    负责调用 Graphiti 在本地 Neo4j 中构建知识图谱

    Graphiti 的 add_episode 是同步等待完成的（没有 Zep Cloud 式的异步批处理/
    轮询 processed 状态的概念），因此这个服务比原来的 Zep Cloud 版本简单得多：
    不再有 Batch API、operation_id 幂等重放、批次状态轮询这些专门为保护
    Zep Cloud 计量 API 而存在的防御性机制。本地 Neo4j 没有配额或限流，一次调用
    要么成功要么抛出真实错误，调用方（build_graph_async 的后台线程 / api/graph.py）
    据此更新任务状态即可。
    """

    def __init__(self):
        self.client = get_zep_client()
        self.task_manager = TaskManager()
        # graph_id -> (entity_types, edge_types, edge_type_map)，由 set_ontology 写入，
        # add_text_batches 消费。Graphiti 没有 Zep Cloud 那样"在图谱上设置本体"的
        # 服务端调用；entity_types/edge_types 是每次 add_episode 调用直接传入的
        # 运行时 Pydantic 类型，因此这里用进程内字典在两次调用之间传递它们。
        self._graph_ontologies: Dict[str, tuple] = {}
        self._ontology_lock = threading.Lock()

    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 350
    ) -> str:
        """
        异步构建图谱

        Args:
            text: 输入文本
            ontology: 本体定义（来自接口1的输出）
            graph_name: 图谱名称
            chunk_size: 文本块大小
            chunk_overlap: 块重叠大小
            batch_size: 保留参数（不再对应 Zep Cloud Batch API 的批大小限制，Graphiti 无此概念），
                仅用于进度回调的分组粒度

        Returns:
            任务ID
        """
        # 创建任务
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={
                "graph_name": graph_name,
                "chunk_size": chunk_size,
                "text_length": len(text),
            }
        )

        # Capture locale before spawning background thread
        current_locale = get_locale()

        # 在后台线程中执行构建
        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size, current_locale)
        )
        thread.daemon = True
        thread.start()

        return task_id

    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int,
        locale: str = 'zh'
    ):
        """图谱构建工作线程"""
        set_locale(locale)
        try:
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=5,
                message=t('progress.startBuildingGraph')
            )

            chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
            total_chunks = len(chunks)

            # 1. 创建图谱（本地生成 graph_id，无需远程调用）
            graph_id = self.create_graph(graph_name)
            self.task_manager.update_task(
                task_id,
                progress=10,
                message=t('progress.graphCreated', graphId=graph_id)
            )

            # 2. 设置本体（构建运行时 Pydantic 类型）
            self.set_ontology(graph_id, ontology)
            self.task_manager.update_task(
                task_id,
                progress=15,
                message=t('progress.ontologySet')
            )

            self.task_manager.update_task(
                task_id,
                progress=20,
                message=t('progress.textSplit', count=total_chunks)
            )

            # 3. 逐块通过 Graphiti add_episode 写入 Neo4j（同步等待，无需再轮询处理状态）
            def add_progress_callback(msg, progress_ratio):
                progress = 20 + int(progress_ratio * 70)  # 20% - 90%
                self.task_manager.update_task(
                    task_id,
                    message=msg,
                    progress=progress
                )

            episode_uuids = self.add_text_batches(
                graph_id,
                chunks,
                batch_size=batch_size,
                progress_callback=add_progress_callback,
            )

            # 4. 获取图谱信息
            self.task_manager.update_task(
                task_id,
                progress=95,
                message=t('progress.fetchingGraphInfo')
            )

            graph_info = self._get_graph_info(graph_id)

            # 完成
            self.task_manager.complete_task(task_id, {
                "graph_id": graph_id,
                "graph_info": graph_info.to_dict(),
                "chunks_processed": total_chunks,
                "episode_count": len(episode_uuids),
            })

        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            self.task_manager.fail_task(task_id, error_msg)

    def create_graph(
        self,
        name: str,
        *,
        graph_id: str | None = None,
        graph_id_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Allocate a graph_id (Graphiti group_id).

        Graphiti has no server-side "create graph" call -- a group_id is
        just a partitioning string that comes into existence the first time
        an episode is added under it. This keeps the same client-generated,
        durable-ID shape the old Zep Cloud version used (and still invokes
        `graph_id_callback` immediately, before any Neo4j write) so callers
        that persist it early for crash recovery keep working unchanged.
        """

        graph_id = graph_id or f"mirofish_{uuid.uuid4().hex[:16]}"
        if graph_id_callback:
            graph_id_callback(graph_id)
        return graph_id

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        """构建本图谱的 Graphiti 运行时本体类型（实体/关系 Pydantic 模型）。

        Zep Cloud 需要单独的 `client.graph.set_ontology(...)` 调用把本体安装到
        服务端；Graphiti 没有这个概念，entity_types/edge_types/edge_type_map 是
        每次 `add_episode` 调用直接传入的参数。这里只是把
        `OntologyGenerator.build_pydantic_types` 的结果缓存下来，供
        `add_text_batches` 使用。
        """

        entity_types, edge_types, edge_type_map = OntologyGenerator.build_pydantic_types(
            ontology
        )
        with self._ontology_lock:
            self._graph_ontologies[graph_id] = (entity_types, edge_types, edge_type_map)

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 350,
        progress_callback: Optional[Callable] = None,
    ) -> List[str]:
        """将文档分块依次写入 Graphiti（每块一个 episode）。

        Graphiti 要求同一 group 内的 episode 顺序 await 添加（后一个 episode
        的抽取会引用前面 episode 的上下文），所以这里按顺序逐个调用
        `add_episode` 并等待其完成，而不是并发触发。`batch_size` 只用于控制
        进度回调的报告粒度，不再是一次网络请求的条目上限。

        Returns:
            成功写入的 episode UUID 列表
        """

        if not graph_id:
            raise ValueError("graph_id is required")
        if not chunks:
            raise ValueError("At least one text chunk is required")

        with self._ontology_lock:
            entity_types, edge_types, edge_type_map = self._graph_ontologies.get(
                graph_id, ({}, {}, {})
            )

        total_chunks = len(chunks)
        episode_uuids: List[str] = []

        for index, chunk in enumerate(chunks):
            if progress_callback and index % max(1, batch_size // 10) == 0:
                progress_callback(
                    t('progress.addingChunks', count=total_chunks),
                    index / total_chunks,
                )

            result = run_async(
                self.client.add_episode(
                    name=f"{graph_id}_chunk_{index}",
                    episode_body=chunk,
                    source_description="MiroFish source document chunk",
                    reference_time=datetime.now(timezone.utc),
                    source=EpisodeType.text,
                    group_id=graph_id,
                    entity_types=entity_types or None,
                    edge_types=edge_types or None,
                    edge_type_map=edge_type_map or None,
                )
            )
            episode_uuids.append(result.episode.uuid)

        if progress_callback:
            progress_callback(
                t('progress.processingComplete', completed=total_chunks, total=total_chunks),
                1.0,
            )

        return episode_uuids

    def _get_graph_info(self, graph_id: str) -> GraphInfo:
        """获取图谱信息"""
        nodes = fetch_all_nodes(self.client, graph_id)
        edges = fetch_all_edges(self.client, graph_id)

        entity_types = set()
        for node in nodes:
            for label in (node.labels or []):
                if label not in ["Entity", "Node"]:
                    entity_types.add(label)

        return GraphInfo(
            graph_id=graph_id,
            node_count=len(nodes),
            edge_count=len(edges),
            entity_types=list(entity_types)
        )

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """
        获取完整图谱数据（包含详细信息）

        Args:
            graph_id: 图谱ID

        Returns:
            包含nodes和edges的字典，包括时间信息、属性等详细数据
        """
        nodes = fetch_all_nodes(self.client, graph_id)
        edges = fetch_all_edges(self.client, graph_id)

        node_map = {node.uuid: (node.name or "") for node in nodes}

        nodes_data = []
        for node in nodes:
            created_at = getattr(node, 'created_at', None)
            nodes_data.append({
                "uuid": node.uuid,
                "name": node.name,
                "labels": node.labels or [],
                "summary": node.summary or "",
                "attributes": node.attributes or {},
                "created_at": str(created_at) if created_at else None,
            })

        edges_data = []
        for edge in edges:
            created_at = getattr(edge, 'created_at', None)
            valid_at = getattr(edge, 'valid_at', None)
            invalid_at = getattr(edge, 'invalid_at', None)
            expired_at = getattr(edge, 'expired_at', None)
            episodes = getattr(edge, 'episodes', None) or []

            edges_data.append({
                "uuid": edge.uuid,
                "name": edge.name or "",
                "fact": edge.fact or "",
                "fact_type": edge.name or "",
                "source_node_uuid": edge.source_node_uuid,
                "target_node_uuid": edge.target_node_uuid,
                "source_node_name": node_map.get(edge.source_node_uuid, ""),
                "target_node_name": node_map.get(edge.target_node_uuid, ""),
                "attributes": edge.attributes or {},
                "created_at": str(created_at) if created_at else None,
                "valid_at": str(valid_at) if valid_at else None,
                "invalid_at": str(invalid_at) if invalid_at else None,
                "expired_at": str(expired_at) if expired_at else None,
                "episodes": [str(e) for e in episodes],
            })

        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }

    def delete_graph(self, graph_id: str):
        """删除图谱（Neo4j 中 group_id 等于 graph_id 的全部节点与关系）"""
        run_async(clear_data(self.client.driver, group_ids=[graph_id]))
        with self._ontology_lock:
            self._graph_ontologies.pop(graph_id, None)
