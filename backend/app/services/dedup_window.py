"""去重窗口领域服务：按事件时间划分有效窗口，产出可重放的去重变更明细。

上游：binlog_parser 产出的事件 → dedup_window 按事件时间锚定窗口并去重
下游：change_event 直接消费去重后的变更明细，落库源坐标回执

窗口语义
--------
* 窗口区间是 ``[start, start + size)``，按事件自身的**事件时间**锚定，
  固定对齐到窗口大小的网格上，与提交/到达顺序无关。
* 同一个去重键可以同时存在多个相邻甚至重叠的窗口（例如 10:00:00–10:01:00
  与 10:00:40–10:01:40，后者通过 ``open_window`` 显式锚定），
  每个窗口各自保留自己窗口内的记录。
* 同一笔源坐标只允许归属一个窗口：重叠区间里的记录先看是否有以它的事件
  时间作为起点的窗口（锚定优先），否则归属起点最早且尚未交付的窗口；
  归属冲突会留痕，供事后按当时的窗口复核。
* 窗口内保留最早还是最晚由 ``去重策略`` 决定（first/retain_first 与
  last/retain_last），该判定只在**单个窗口内部**进行，窗口之间互不影响。
* 窗口交付（回执落库）后不再接收新的归属；迟到记录改投其他尚未交付的
  有效窗口，没有可投的窗口时以它的事件时间另开窗口。这样换序提交、补入
  迟到记录和整体重放都不会改变已交付窗口的镜像。
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timedelta
from typing import Any

from app.pipeline import (
    DedupWindowRecord,
    event_source_coord,
    json_text,
    parse_event_time,
    pipeline_bus,
)
from app.services.base import BaseModuleService


FIRST_STRATEGIES = {"first", "retain_first", "保留最早", "最早"}
LAST_STRATEGIES = {"last", "retain_last", "保留最晚", "最新", "最晚"}


class DedupWindowService(BaseModuleService):
    MODULE = "dedup_window"
    ENTITY = "窗口配置"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["配置名称", "去重键", "窗口大小", "去重策略", "保留版本", "过期清理", "水位字段"]
    REQUIRED_FIELDS = ["配置名称", "去重键", "窗口大小"]
    STATUS_ORDER = ["草稿", "已启用", "已过期"]
    ACTION_RULES = {"启用窗口": "草稿", "标记过期": "已启用"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 窗口锚定与去重明细
    # ------------------------------------------------------------------
    def dedup_events(self, events: list[dict[str, Any]],
                     config: dict[str, Any]) -> list[dict[str, Any]]:
        """对事件执行按事件时间的窗口去重，返回去重后的变更明细。

        每条明细带窗口 ID、窗口区间、源坐标、是否交付等字段。重复调用（含
        换序提交、整体重放）会复用已有窗口和已落库回执，不会产生第二笔。
        """
        window_sec = self._parse_window(config.get("窗口大小", "3600"))
        key_field = config.get("去重键", "事件ID")
        strategy = self._normalise_strategy(config.get("去重策略", "last"))
        expire_cleanup = str(config.get("过期清理", "自动")).lower() in (
            "自动", "auto", "true", "是",
        )

        for event in events:
            self._ingest(event, key_field=key_field, window_sec=window_sec, strategy=strategy)

        if expire_cleanup:
            self._expire_state(window_sec)

        await_keys = {
            event.get(key_field) for event in events if event.get(key_field)
        }
        pipeline_bus.dedup_manifest = self._select_entries()
        if not await_keys:
            return [dict(item) for item in pipeline_bus.dedup_manifest]
        return [
            dict(item) for item in pipeline_bus.dedup_manifest
            if item.get("去重键") in await_keys
        ]

    def open_window(self, key: str, start: datetime | str, *, window_sec: float,
                    strategy: str = "last") -> DedupWindowRecord:
        """显式锚定一个去重窗口（支持与同键其他窗口相邻或重叠）。

        窗口 ID 由去重键、锚定起点和窗口大小决定，重复打开同一窗口会拿到
        同一个对象；因此先消费后复现也不会因为提交顺序不同而漂移。
        """
        if isinstance(start, str):
            parsed = parse_event_time({"事件时间": start})
            if parsed is None:
                raise ValueError(f"无法解析窗口起点：{start}")
            start = parsed
        strategy = self._normalise_strategy(strategy)
        window_id = f"{key}@{start.isoformat()}~{int(window_sec)}s"
        existing = pipeline_bus.find_window(window_id)
        if existing is not None:
            return existing
        window = DedupWindowRecord(
            window_id=window_id,
            key=key,
            start=start,
            end=start + timedelta(seconds=window_sec),
            size_sec=window_sec,
            strategy=strategy,
            sequence=pipeline_bus.next_window_sequence(),
        )
        pipeline_bus.register_dedup_window(window)
        return window

    def _ingest(self, event: dict[str, Any], *, key_field: str, window_sec: float,
                strategy: str) -> None:
        """把一条变更数据归入唯一窗口，并更新窗口内的去重结果。"""
        coord = event_source_coord(event)
        if not coord:
            return
        if pipeline_bus.is_delivered(coord):
            # 回执已落库：重复提交只累加计数，不再入窗、不再产生第二笔。
            pipeline_bus.note_repeat_submission(coord)
            return
        moment = parse_event_time(event)
        if moment is None:
            return
        key = self._get_key(event, key_field) or coord

        window, conflicted = self._assign_window(
            key=key, moment=moment, window_sec=window_sec, strategy=strategy, coord=coord
        )
        if conflicted is not None:
            pipeline_bus.record_dedup_conflict(conflicted)

        entry = {
            "源坐标": coord,
            "事件ID": event.get("事件ID", ""),
            "去重键": key,
            "窗口ID": window.window_id,
            "窗口区间": [window.start.isoformat(), window.end.isoformat()],
            "事件时间": moment.isoformat(),
            "操作类型": event.get("操作类型", ""),
            "来源表": event.get("来源表", ""),
            "事务ID": event.get("事务ID", ""),
            "变更前镜像": json_text(event.get("变更前镜像")),
            "变更后镜像": json_text(event.get("变更后镜像")),
            "binlog文件": event.get("binlog_file", event.get("Binlog名称", "")),
            "位点": event.get("binlog_pos", event.get("位点值", "")),
        }
        previous = window.entries.get(coord)
        if previous is not None:
            # 同一源坐标重复提交：只保留一笔，内容以首次为准。
            previous["重复提交"] = previous.get("重复提交", 0) + 1
            pipeline_bus.pipeline_metrics["dedup_hits"] += 1
            return
        window.entries[coord] = entry
        self._resolve_window(window)

    def _assign_window(self, *, key: str, moment: datetime, window_sec: float,
                       strategy: str, coord: str) -> tuple[DedupWindowRecord, dict[str, Any] | None]:
        """为一条记录选定唯一窗口，必要时新建窗口。

        归属顺序：
        1. 以该记录事件时间为起点的未交付窗口（锚定优先）；
        2. 其余包含该时刻的未交付窗口，取 ``(窗口起点, 创建序号)`` 最小者；
        3. 都没有时以网格锚定新建窗口（若命中的窗口均已交付则另开新窗口）。
        命中多个未交付候选且无锚定窗口时记一条冲突留痕，便于复核。
        """
        candidates = [
            window for window in pipeline_bus.windows_for_key(key)
            if window.contains(moment)
        ]
        open_candidates = [window for window in candidates if not window.delivered]
        anchored = [window for window in open_candidates if window.start == moment]

        conflicted: dict[str, Any] | None = None
        if len(open_candidates) > 1 and not anchored:
            conflicted = {
                "源坐标": coord,
                "去重键": key,
                "事件时间": moment.isoformat(),
                "候选窗口": [window.window_id for window in candidates],
                "候选区间": [
                    [window.start.isoformat(), window.end.isoformat()]
                    for window in candidates
                ],
                "已交付窗口": [
                    window.window_id for window in candidates if window.delivered
                ],
                "裁决规则": "锚定窗口优先，其次窗口起点最早且尚未交付",
            }

        if open_candidates:
            chosen = (anchored or open_candidates)[0]
            if conflicted is not None:
                conflicted["归属窗口"] = chosen.window_id
            return chosen, conflicted

        if candidates:
            # 命中的窗口都已交付：迟到记录以自身事件时间为锚另开新窗口，
            # 已交付窗口保持交付当时的保留结果，仍可按原窗口复核。
            start = moment
            conflict_note = {
                "源坐标": coord,
                "去重键": key,
                "事件时间": moment.isoformat(),
                "候选窗口": [window.window_id for window in candidates],
                "候选区间": [
                    [window.start.isoformat(), window.end.isoformat()]
                    for window in candidates
                ],
                "已交付窗口": [window.window_id for window in candidates],
                "裁决规则": "命中的窗口均已交付，迟到记录另开新窗口",
            }
        else:
            start = self._anchor(moment, window_sec)
            conflict_note = None
        window = self._open_fresh_window(key, start, moment, window_sec, strategy)
        if conflict_note is not None:
            conflict_note["归属窗口"] = window.window_id
            return window, conflict_note
        return window, None

    def _open_fresh_window(self, key: str, start: datetime, moment: datetime,
                           window_sec: float, strategy: str) -> DedupWindowRecord:
        """打开一个可接收记录的窗口，绝不返回已交付窗口。

        优先复用同一锚定起点且尚未交付的窗口；若该起点已被交付窗口占用，
        则以记录自身事件时间为锚再试；仍然冲突时追加序号，保证幂等重放。
        """
        for anchor, label in ((start, f"{key}@{start.isoformat()}~{int(window_sec)}s"),
                              (moment, f"{key}@{moment.isoformat()}~{int(window_sec)}s")):
            existing = pipeline_bus.find_window(label)
            if existing is None or not existing.delivered:
                return existing or self.open_window(
                    key, anchor, window_sec=window_sec, strategy=strategy
                )
        base = f"{key}@{moment.isoformat()}~{int(window_sec)}s"
        suffix = 2
        while True:
            candidate_id = f"{base}#{suffix}"
            existing = pipeline_bus.find_window(candidate_id)
            if existing is not None and not existing.delivered:
                return existing
            if existing is None:
                window = DedupWindowRecord(
                    window_id=candidate_id,
                    key=key,
                    start=moment,
                    end=moment + timedelta(seconds=window_sec),
                    size_sec=window_sec,
                    strategy=strategy,
                    sequence=pipeline_bus.next_window_sequence(),
                )
                pipeline_bus.register_dedup_window(window)
                return window
            suffix += 1

    def _anchor(self, moment: datetime, window_sec: float) -> datetime:
        """把事件时间对齐到窗口网格起点，保证锚定与提交顺序无关。"""
        epoch = moment.timestamp()
        steps = math.floor(epoch / window_sec)
        return datetime.fromtimestamp(steps * window_sec, tz=moment.tzinfo)

    def _resolve_window(self, window: DedupWindowRecord) -> None:
        """在单个窗口内部按该窗口的保留最早/最晚策略选出最终交付记录。"""
        ordered = sorted(
            window.entries.values(),
            key=lambda entry: (entry["事件时间"], str(entry["源坐标"])),
        )
        if not ordered:
            window.selected_coord = None
            window.suppressed = []
            return
        if window.strategy == "first":
            keep, dropped = ordered[0], ordered[1:]
        else:
            keep, dropped = ordered[-1], ordered[:-1]
        window.selected_coord = keep["源坐标"]
        window.suppressed = [
            {
                "源坐标": entry["源坐标"],
                "事件ID": entry["事件ID"],
                "事件时间": entry["事件时间"],
                "原因": "窗口内被保留策略抑制",
            }
            for entry in dropped
        ]

    def _select_entries(self) -> list[dict[str, Any]]:
        """汇总各窗口的保留记录，生成去重后的变更明细。"""
        manifest: list[dict[str, Any]] = []
        for window in sorted(pipeline_bus.dedup_windows, key=lambda w: (w.start, w.sequence)):
            if window.selected_coord is None:
                continue
            entry = dict(window.entries[window.selected_coord])
            receipt = pipeline_bus.consumed_coords.get(entry["源坐标"])
            entry.update({
                "去重策略": window.strategy,
                "去重结果": "保留",
                "同窗抑制数": len(window.suppressed),
                "同窗抑制": list(window.suppressed),
                "窗口内记录数": len(window.entries),
                "已交付": receipt is not None or window.delivered,
                "交付源坐标": window.delivered_coord,
                "交付时间": window.delivered_at.isoformat() if window.delivered_at else None,
            })
            if receipt is not None:
                entry["回执"] = receipt
                entry["重复提交"] = receipt.get("repeat_submissions", 0)
            manifest.append(entry)
        manifest.sort(key=lambda item: (item["事件时间"], str(item["源坐标"])))
        return manifest

    def _expire_state(self, window_sec: float) -> None:
        """清理过期的滑动去重缓冲；窗口与回执保留用于复核。"""
        horizon = max(window_sec * 2, window_sec)
        now = time.time()
        for key in list(pipeline_bus.dedup_state.keys()):
            pipeline_bus.dedup_state[key] = [
                e for e in pipeline_bus.dedup_state[key]
                if now - e["ts"] < horizon
            ]

    # ------------------------------------------------------------------
    # 交付与重放
    # ------------------------------------------------------------------
    def refresh_manifest(self) -> list[dict[str, Any]]:
        """重新计算并缓存去重清单。"""
        pipeline_bus.dedup_manifest = self._select_entries()
        return [dict(item) for item in pipeline_bus.dedup_manifest]

    def pending_manifest(self) -> list[dict[str, Any]]:
        """返回尚未交付的去重明细，供变更事件消费端取用。"""
        return [dict(item) for item in self.refresh_manifest() if not item.get("已交付")]

    def acknowledge(self, coord: str, *, event_id: str, window_id: str,
                    result: dict[str, Any], consumer: str = "change_event") -> bool:
        """落库消费回执并把执行结果回写到去重清单。

        返回 True 表示首次落库；False 表示该源坐标此前已交付、本次只记一笔。
        """
        window = pipeline_bus.find_window(window_id)
        first_time = pipeline_bus.record_receipt(
            coord,
            window_id=window_id,
            window_start=window.start if window else datetime.min,
            window_end=window.end if window else datetime.min,
            event_id=event_id,
            manifest_key=window.key if window else "",
            consumer=consumer,
            result=result,
        )
        receipt = pipeline_bus.consumed_coords[coord]
        if not first_time:
            pipeline_bus.update_manifest_entry(
                coord,
                **{
                    "已交付": True,
                    "重复提交": receipt.get("repeat_submissions", 0),
                    "回执": receipt,
                },
            )
            return False

        if window is not None:
            window.delivered = True
            window.delivered_coord = coord
            window.delivered_at = datetime.now()
        pipeline_bus.update_manifest_entry(
            coord,
            **{
                "已交付": True,
                "交付源坐标": coord,
                "交付时间": receipt["回执时间"],
                "回执": receipt,
            },
        )
        return True

    def replay_manifest(self, *, key: str | None = None,
                        window_ids: set[str] | None = None) -> dict[str, Any]:
        """重放去重清单：按已落库的源坐标回执跳过已交付事件。"""
        entries = self.refresh_manifest()
        pending: list[dict[str, Any]] = []
        skipped: list[str] = []
        for entry in entries:
            if key is not None and entry.get("去重键") != key:
                continue
            if window_ids is not None and entry.get("窗口ID") not in window_ids:
                continue
            if pipeline_bus.is_delivered(entry["源坐标"]):
                skipped.append(entry["源坐标"])
                continue
            pending.append(entry)
        return {
            "key": key,
            "window_ids": sorted(window_ids) if window_ids else None,
            "pending": pending,
            "skipped_coords": skipped,
            "requested": len(pending) + len(skipped),
            "delivered": len(skipped),
        }

    def window_overview(self, key: str) -> list[dict[str, Any]]:
        """列出某个去重键的全部窗口，包含保留记录和交付状态。"""
        return [
            {
                "窗口ID": window.window_id,
                "去重键": window.key,
                "窗口区间": [window.start.isoformat(), window.end.isoformat()],
                "窗口大小": window.size_sec,
                "去重策略": window.strategy,
                "窗口内记录": [
                    {
                        "源坐标": entry["源坐标"],
                        "事件ID": entry["事件ID"],
                        "事件时间": entry["事件时间"],
                    }
                    for entry in sorted(
                        window.entries.values(), key=lambda item: item["事件时间"]
                    )
                ],
                "保留源坐标": window.selected_coord,
                "同窗抑制": list(window.suppressed),
                "已交付": window.delivered,
                "交付源坐标": window.delivered_coord,
            }
            for window in pipeline_bus.windows_for_key(key)
        ]

    # ------------------------------------------------------------------
    # 兼容旧入口与辅助
    # ------------------------------------------------------------------
    def _get_key(self, event: dict[str, Any], key_field: str) -> str:
        val = event.get(key_field, "")
        return str(val) if val else ""

    def _normalise_strategy(self, raw: Any) -> str:
        value = str(raw or "").strip().lower()
        if value in FIRST_STRATEGIES:
            return "first"
        if value in LAST_STRATEGIES:
            return "last"
        if value in ("compact", "压缩"):
            return "last"
        return "last"

    def _within_window(self, events: list[dict[str, Any]], waterm_field: str,
                       window_sec: float) -> bool:
        """判断事件组是否在水位窗口内（兼容旧调用）。"""
        if len(events) <= 1:
            return True
        try:
            t0 = self._parse_ts(events[0].get(waterm_field, ""))
            tn = self._parse_ts(events[-1].get(waterm_field, ""))
            return abs(tn - t0) <= window_sec
        except (ValueError, TypeError):
            return True

    def _parse_ts(self, ts_str: str) -> float:
        """解析时间戳字符串为秒时间戳。"""
        try:
            dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, TypeError):
            return time.time()

    def _parse_window(self, window_str: str) -> float:
        """解析窗口大小字符串为秒。"""
        w = str(window_str).strip().lower()
        if w.endswith("min"):
            return float(w[:-3]) * 60
        if w.endswith("hour"):
            return float(w[:-4]) * 3600
        if w.endswith("h"):
            return float(w[:-1]) * 3600
        if w.endswith("s") and not w.endswith("ms"):
            return float(w[:-1])
        try:
            return float(w)
        except ValueError:
            return 3600.0

    def compute_dedup_rate(self, total_events: int, deduped_count: int) -> dict[str, Any]:
        """计算去重率。"""
        dedup_rate = round(deduped_count / max(total_events, 1) * 100, 2)
        return {
            "total": total_events,
            "deduplicated": deduped_count,
            "dedup_rate_pct": dedup_rate,
            "state_buffer_size": sum(len(v) for v in pipeline_bus.dedup_state.values()),
            "window_count": len(pipeline_bus.dedup_windows),
            "manifest_size": len(pipeline_bus.dedup_manifest),
        }

    def watermark_lag_check(self, config: dict[str, Any]) -> dict[str, Any]:
        """检查水位延迟。"""
        watermark_col = config.get("水位字段", "事件时间")
        window_sec = self._parse_window(config.get("窗口大小", "3600"))
        now = time.time()
        buffer_count = sum(len(v) for v in pipeline_bus.dedup_state.values())
        lag = now - max(
            (e["ts"] for v in pipeline_bus.dedup_state.values() for e in v), default=now
        )
        return {
            "waterm_field": watermark_col,
            "window_sec": window_sec,
            "current_lag_sec": round(lag, 1),
            "buffer_entries": buffer_count,
            "status": "正常" if lag < window_sec else "水位滞后",
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用窗口":
            key = entry.get("去重键", "事件ID")
            pipeline_bus.write_audit(
                "去重窗口启用", key,
                f"窗口 {entry.get('配置名称')} 已启用，去重键 {key}",
                "dedup_window", "已启用",
            )
        return entry, msg
