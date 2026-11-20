"""变更事件领域服务：直接消费去重后的变更明细，落库源坐标回执并支持重放。

上游：binlog_parser 产出事件 → dedup_window 按事件时间去重生成变更明细
消费：change_event 只消费去重清单中尚未交付的明细，回执写回清单
下游：format_adapter 格式适配 → event_filter 过滤 → route_dispatcher 路由分发
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import json_text, pipeline_bus


class ChangeEventService(BaseModuleService):
    MODULE = "change_event"
    ENTITY = "变更记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["事件ID", "来源表", "操作类型", "变更前镜像", "变更后镜像", "事务ID", "事件时间"]
    REQUIRED_FIELDS = ["事件ID", "来源表", "操作类型"]
    STATUS_ORDER = ["已捕获", "已解析", "已下发", "解析失败"]
    ACTION_RULES = {"解析事件": "已捕获", "下发事件": "已解析", "标记失败": "已解析"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 去重清单消费
    # ------------------------------------------------------------------
    def consume_dedup_manifest(self, config: dict[str, Any] | None = None,
                               batch_size: int = 50) -> list[dict[str, Any]]:
        """直接消费去重后的变更明细，落库源坐标回执并回写执行结果。

        * 只取去重清单里尚未交付的条目，已交付的源坐标在重放时直接跳过；
        * 每消费一条就落一笔回执并用源坐标去重，重复提交不会产生第二笔；
        * 执行结果（解析状态、增量字段等）回写到去重清单对应条目。
        """
        dedup = self._dedup_service()
        if dedup is None:
            return []
        pending = dedup.pending_manifest()
        if not pending:
            return []

        entries: list[dict[str, Any]] = []
        for item in pending[:batch_size]:
            coord = item.get("源坐标", "")
            if not coord or pipeline_bus.is_delivered(coord):
                continue
            entry = self._manifest_to_entry(item)
            entry["执行结果"] = self._apply_manifest_entry(entry)
            dedup.acknowledge(
                coord,
                event_id=item.get("事件ID", ""),
                window_id=item.get("窗口ID", ""),
                result=entry["执行结果"],
                consumer="change_event",
            )
            receipt = pipeline_bus.consumed_coords[coord]
            entry["源坐标"] = coord
            entry["窗口ID"] = item.get("窗口ID", "")
            entry["窗口区间"] = item.get("窗口区间")
            entry["是否重复提交"] = True if receipt.get("repeat_submissions") else False
            self._record_entry(entry)
            entries.append(entry)

        pipeline_bus.count_manifest_delivery(len(entries))
        pipeline_bus.write_audit(
            "变更事件消费", f"{len(entries)}条去重明细",
            f"消费去重清单 {len(entries)} 条，源坐标回执已落库并回写清单",
            "change_event", "已解析",
        )
        return entries

    # 兼容不同调用口径：都指向同一套去重清单消费逻辑
    consume_dedup_events = consume_dedup_manifest
    consume_manifest = consume_dedup_manifest
    consume_pending_manifest = consume_dedup_manifest

    def replay_dedup_manifest(self, *, key: str | None = None,
                              window_ids: set[str] | None = None,
                              batch_size: int = 50) -> dict[str, Any]:
        """重放去重清单：已交付（回执在库）的事件跳过，只产出未交付部分。"""
        dedup = self._dedup_service()
        if dedup is None:
            return {"consumed": [], "skipped_coords": []}
        replay = dedup.replay_manifest(key=key, window_ids=window_ids)
        consumed: list[dict[str, Any]] = []
        for item in replay["pending"][:batch_size]:
            coord = item.get("源坐标", "")
            entry = self._manifest_to_entry(item)
            entry["执行结果"] = self._apply_manifest_entry(entry)
            dedup.acknowledge(
                coord,
                event_id=item.get("事件ID", ""),
                window_id=item.get("窗口ID", ""),
                result=entry["执行结果"],
                consumer="change_event",
            )
            entry["源坐标"] = coord
            entry["窗口ID"] = item.get("窗口ID", "")
            entry["窗口区间"] = item.get("窗口区间")
            self._record_entry(entry)
            consumed.append(entry)
        pipeline_bus.count_manifest_delivery(len(consumed))
        return {
            "consumed": consumed,
            "skipped_coords": list(replay["skipped_coords"]),
        }

    def consumed_coordinates(self) -> dict[str, dict[str, Any]]:
        """返回已落库的源坐标回执，供重放和页面复核。"""
        return {coord: dict(receipt) for coord, receipt in pipeline_bus.consumed_coords.items()}

    def _dedup_service(self):
        """延迟获取去重窗口服务，避免服务注册表循环导入。"""
        from app.services import service_for

        return service_for("dedup_window")

    def _manifest_to_entry(self, item: dict[str, Any]) -> dict[str, Any]:
        """把去重明细转换成变更事件页面的记录结构。"""
        return {
            "事件ID": item.get("事件ID", ""),
            "来源表": item.get("来源表", ""),
            "操作类型": item.get("操作类型", ""),
            "变更前镜像": item.get("变更前镜像", "{}"),
            "变更后镜像": item.get("变更后镜像", "{}"),
            "事务ID": item.get("事务ID", ""),
            "事件时间": item.get("事件时间", ""),
            "源坐标": item.get("源坐标", ""),
            "binlog文件": item.get("binlog文件", ""),
            "位点": item.get("位点", ""),
            "窗口ID": item.get("窗口ID", ""),
            "status": "已解析",
            "pending": True,
            "abnormal": False,
        }

    def _apply_manifest_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        """对去重后的明细执行变更事件处理，产出可回写的执行结果。"""
        delta = self.delta_computation(entry)
        return {
            "状态": "已解析",
            "操作类型": entry.get("操作类型", ""),
            "变更字段数": delta.get("delta_count", 0),
            "变更字段": delta.get("delta_fields", []),
            "处理后镜像": entry.get("变更后镜像", "{}"),
        }

    def _record_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把消费结果落到变更事件页面数据里；同一源坐标只记一条。"""
        from app.store import store

        rows = store.rows(self.MODULE)
        coord = entry.get("源坐标", "")
        for row in rows:
            if coord and row.get("源坐标") == coord:
                row.update({field: entry.get(field) for field in self.LIST_FIELDS})
                row["status"] = entry.get("status", "已解析")
                row["源坐标"] = coord
                return row
        row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        row.update({field: entry.get(field) for field in self.LIST_FIELDS})
        row.update({
            "status": entry.get("status", "已解析"),
            "pending": True,
            "abnormal": False,
            "源坐标": coord,
            "窗口ID": entry.get("窗口ID", ""),
        })
        rows.append(row)
        return row

    # ------------------------------------------------------------------
    # 原始管道队列（保留兼容）
    # ------------------------------------------------------------------
    def consume_pipeline_events(self, batch_size: int = 50) -> list[dict[str, Any]]:
        """从管道总线消费变更事件并分类汇总。

        消费 pipeline_bus 中 binlog_parser 产出的缓冲区事件，
        按操作类型 (INSERT/UPDATE/DELETE) 和来源表分组。已接入去重清单的
        链路请使用 ``consume_dedup_manifest``。
        """
        raw_events = pipeline_bus.consume_events(batch_size=batch_size)
        if not raw_events:
            return []
        entries: list[dict[str, Any]] = []
        for evt in raw_events:
            after_str = json_text(evt.after_image) if evt.after_image else "{}"
            entries.append({
                "事件ID": evt.event_id,
                "来源表": f"{evt.source_db}.{evt.source_table}",
                "操作类型": evt.operation,
                "变更前镜像": json_text(evt.before_image) if evt.before_image else "{}",
                "变更后镜像": after_str,
                "事务ID": evt.tx_id,
                "事件时间": evt.event_time.isoformat(),
                "raw_format": evt.raw_format,
                "binlog_pos": evt.binlog_pos,
                "源坐标": evt.gtid or f"{evt.binlog_file}:{evt.binlog_pos}",
            })
        pipeline_bus.write_audit(
            "事件消费", f"{len(entries)}条事件",
            f"消费管道事件 {len(entries)} 条，按业务类型分组完成",
            "change_event", "已解析"
        )
        return entries

    def classify_by_operation(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """对消费后的事件按操作类型分类统计。

        为下游 format_adapter 提供操作类型分布以决定格式适配策略。
        """
        ops: dict[str, int] = {"insert": 0, "update": 0, "delete": 0}
        tables: dict[str, int] = {}
        total_rows_affected = 0
        for evt in events:
            op = evt.get("操作类型", "").upper()
            if op == "INSERT":
                ops["insert"] += 1
            elif op == "UPDATE":
                ops["update"] += 1
            elif op == "DELETE":
                ops["delete"] += 1
            tbl = evt.get("来源表", "unknown")
            tables[tbl] = tables.get(tbl, 0) + 1
            total_rows_affected += 1
        return {
            "total_events": len(events),
            "by_operation": ops,
            "by_table": tables,
            "insert_ratio": round(ops["insert"] / max(len(events), 1) * 100, 1),
            "update_ratio": round(ops["update"] / max(len(events), 1) * 100, 1),
            "delete_ratio": round(ops["delete"] / max(len(events), 1) * 100, 1),
        }

    def delta_computation(self, event: dict[str, Any]) -> dict[str, Any]:
        """对变更前后镜像执行差异计算。

        计算 UPDATE 操作中哪些字段实际发生了变化，
        产出变更增量列表供下游 SCD 策略消费。
        """
        try:
            before = json.loads(event.get("变更前镜像", "{}"))
            after_raw = event.get("变更后镜像", "{}")
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
        except (json.JSONDecodeError, TypeError):
            return {"delta_fields": [], "delta_count": 0}
        changed = []
        for key in after:
            if key not in before or before[key] != after[key]:
                delta_val = {"field": key, "old": before.get(key), "new": after[key]}
                changed.append(delta_val)
        return {"delta_fields": changed, "delta_count": len(changed)}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "下发事件":
            pipeline_bus.write_audit(
                "事件下发", entry.get("来源表", ""),
                f"事件 {entry_id} 下发给下游消费者",
                "change_event", "已下发"
            )
        return entry, msg