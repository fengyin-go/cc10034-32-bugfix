"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping


def event_source_coord(event: Mapping[str, Any] | Any) -> str:
    """返回变更数据的源坐标。

    源坐标是重放和回执的唯一标识：优先使用 GTID，其次使用
    ``binlog_file:binlog_pos``，最后退化为事件 ID。调用方同时支持
    ``ChangeEventRecord`` 和普通字典（中文业务字段）。
    """
    def _pick(*names: str) -> Any:
        for name in names:
            value = event.get(name) if isinstance(event, Mapping) else getattr(event, name, None)
            if value not in (None, ""):
                return value
        return None

    explicit = _pick("源坐标", "source_coord")
    if explicit:
        return str(explicit)
    gtid = _pick("gtid", "Gtid集合")
    if gtid:
        return str(gtid)
    binlog_file = _pick("binlog_file", "Binlog名称")
    binlog_pos = _pick("binlog_pos", "位点值")
    if binlog_file is not None and binlog_pos is not None:
        return f"{binlog_file}:{binlog_pos}"
    event_id = _pick("event_id", "事件ID", "id")
    return str(event_id) if event_id is not None else ""


def parse_event_time(event: Mapping[str, Any] | Any) -> datetime | None:
    """解析事件时间，统一返回本地朴素时间，失败返回 None。

    统一去掉时区信息，避免窗口区间比较时出现 aware/naive 混用。
    """
    raw = event.get("事件时间") if isinstance(event, Mapping) else getattr(event, "event_time", None)
    if isinstance(raw, datetime):
        return raw.astimezone().replace(tzinfo=None) if raw.tzinfo else raw
    if raw in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo else parsed


def json_text(value: Any) -> str:
    """把镜像等结构稳定序列化为文本，便于清单落库和比对。"""
    if isinstance(value, str):
        return value
    if value is None:
        return "{}"
    return json.dumps(value, ensure_ascii=False, sort_keys=True)



@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


@dataclass
class DedupWindowRecord:
    """按事件时间锚定的去重窗口。

    窗口区间是 ``[start, end)``；同一去重键可以存在多个相邻甚至互相重叠的窗口。
    一条变更数据（以源坐标标识）只能归入其中一个窗口，归属规则由
    ``DedupWindowService`` 统一裁决，保证重放和换序提交得到相同结果。
    """
    window_id: str
    key: str
    start: datetime
    end: datetime
    size_sec: float
    strategy: str
    sequence: int
    entries: dict[str, dict[str, Any]] = field(default_factory=dict)
    suppressed: list[dict[str, Any]] = field(default_factory=list)
    selected_coord: str | None = None
    delivered: bool = False
    delivered_coord: str | None = None
    delivered_at: datetime | None = None
    created_at: datetime = field(default_factory=datetime.now)

    def contains(self, moment: datetime) -> bool:
        return self.start <= moment < self.end


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 按事件时间锚定的去重窗口；同一去重键可存在多个相邻/重叠窗口
        self.dedup_windows: list[DedupWindowRecord] = []
        # 去重后的变更明细（交付清单）— dedup_window 产出，change_event 消费
        self.dedup_manifest: list[dict[str, Any]] = []
        # 已消费回执：源坐标 -> 回执，保证重复提交/重放只记一笔
        self.consumed_coords: dict[str, dict[str, Any]] = {}
        # 入窗归属冲突留痕，便于按当时的窗口复核
        self.dedup_conflicts: list[dict[str, Any]] = []
        self._window_seq = 0
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    def count_manifest_delivery(self, count: int) -> None:
        """统计去重清单交付量，让运营概览的消费卡片与实际派发一致。"""
        self.pipeline_metrics["total_output"] += count

    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    # ------------------------------------------------------------------
    # 按事件时间锚定的窗口去重
    # ------------------------------------------------------------------
    def next_window_sequence(self) -> int:
        """分配窗口创建序号，用于同键窗口的稳定排序（与提交顺序无关）。"""
        self._window_seq += 1
        return self._window_seq

    def register_dedup_window(self, window: DedupWindowRecord) -> None:
        """登记去重窗口。"""
        self.dedup_windows.append(window)

    def windows_for_key(self, key: str) -> list[DedupWindowRecord]:
        """返回某个去重键的全部窗口，按锚定时间与创建序号稳定排序。"""
        return sorted(
            (w for w in self.dedup_windows if w.key == key),
            key=lambda w: (w.start, w.end, w.sequence),
        )

    def find_window(self, window_id: str) -> DedupWindowRecord | None:
        for window in self.dedup_windows:
            if window.window_id == window_id:
                return window
        return None

    def record_dedup_conflict(self, conflict: dict[str, Any]) -> None:
        """记录入窗归属冲突，保留两边当时的候选窗口以便复核。"""
        self.dedup_conflicts.append(conflict)

    def is_delivered(self, coord: str) -> bool:
        """源坐标是否已经交付（回执落库即视为已交付）。"""
        return coord in self.consumed_coords

    def note_repeat_submission(self, coord: str) -> int:
        """记录一次重复提交，返回累计的重复次数。"""
        receipt = self.consumed_coords.get(coord)
        if receipt is None:
            return 0
        receipt["repeat_submissions"] = receipt.get("repeat_submissions", 0) + 1
        self.pipeline_metrics["dedup_hits"] += 1
        return receipt["repeat_submissions"]

    def record_receipt(
        self,
        coord: str,
        *,
        window_id: str,
        window_start: datetime,
        window_end: datetime,
        event_id: str,
        manifest_key: str,
        consumer: str,
        result: dict[str, Any],
        duplicate_of: str | None = None,
    ) -> bool:
        """落库消费回执。返回 True 表示首次落库，False 表示重复提交只记一笔。"""
        if coord in self.consumed_coords:
            self.consumed_coords[coord]["repeat_submissions"] = (
                self.consumed_coords[coord].get("repeat_submissions", 0) + 1
            )
            return False
        self.consumed_coords[coord] = {
            "源坐标": coord,
            "窗口ID": window_id,
            "窗口区间": [window_start.isoformat(), window_end.isoformat()],
            "事件ID": event_id,
            "清单键": manifest_key,
            "消费者": consumer,
            "执行结果": result,
            "结果写回": "已回写去重清单",
            "重复于": duplicate_of,
            "已交付": True,
            "回执时间": datetime.now().isoformat(),
            "repeat_submissions": 0,
        }
        return True

    def write_receipt_result(self, coord: str, result_key: str, value: Any) -> None:
        """把执行结果回写到已落库回执。"""
        receipt = self.consumed_coords.get(coord)
        if receipt is not None:
            receipt.setdefault("执行结果", {})[result_key] = value
            receipt["结果写回"] = "已回写去重清单"

    def update_manifest_entry(self, coord: str, **changes: Any) -> dict[str, Any] | None:
        """把消费状态回写到去重清单中的对应条目。"""
        for entry in self.dedup_manifest:
            if entry.get("源坐标") == coord:
                entry.update(changes)
                return entry
        return None

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用":
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
        }


pipeline_bus = PipelineBus()