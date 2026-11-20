"""去重窗口与变更事件消费链路的回归验证。

不依赖 pytest，直接运行：

    cd backend && .venv/bin/python tests/test_dedup_pipeline.py

覆盖两个复现场景：
1. 原问题：相隔 5 分钟的 10:00:00 与 10:05:00 两笔同键 UPDATE，换序提交后
   去重结果必须一致且两笔都在（旧实现会塌成一边）。
2. 60 秒窗口下，同一键的相邻窗口 10:00:00–10:01:00 与 10:00:40–10:01:40，
   各有一条锚定记录，消费前补入 10:00:50 的迟到更新，核对实际派出的
   事件坐标与镜像，并验证回执、重复提交和重放。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pipeline import pipeline_bus  # noqa: E402
from app.services.change_event import ChangeEventService  # noqa: E402
from app.services.dedup_window import DedupWindowService  # noqa: E402


KEY = "shop.t_order"
CONFIG = {"去重键": "来源表", "窗口大小": "60", "去重策略": "last"}


def reset() -> None:
    """清空管道级去重状态，避免用例之间互相污染。"""
    pipeline_bus.dedup_windows.clear()
    pipeline_bus.dedup_manifest.clear()
    pipeline_bus.consumed_coords.clear()
    pipeline_bus.dedup_conflicts.clear()
    pipeline_bus.dedup_state.clear()
    pipeline_bus._window_seq = 0


def event(event_id: str, moment: str, mirror: dict, pos: int) -> dict:
    return {
        "事件ID": event_id,
        "来源表": KEY,
        "操作类型": "UPDATE",
        "变更前镜像": {},
        "变更后镜像": mirror,
        "事务ID": "tx1",
        "事件时间": moment,
        "binlog_file": "mysql-bin.000001",
        "binlog_pos": pos,
    }


def case_swapped_updates() -> None:
    """场景一：10:00:00 与 10:05:00 换序提交，两笔都必须各自成窗并交付。"""
    early = event("e1", "2026-11-20T10:00:00", {"v": "10:00"}, 100)
    late = event("e2", "2026-11-20T10:05:00", {"v": "10:05"}, 200)

    dispatched: list[list[tuple[str, str]]] = []
    for order in ([early, late], [late, early]):
        reset()
        dedup = DedupWindowService()
        change_event = ChangeEventService()
        dedup.dedup_events(order, CONFIG)
        consumed = change_event.consume_dedup_manifest()
        dispatched.append([(item["源坐标"], item["变更后镜像"]) for item in consumed])

    assert dispatched[0] == dispatched[1], f"换序后消费明细不一致：{dispatched}"
    assert dispatched[0] == [
        ("mysql-bin.000001:100", '{"v": "10:00"}'),
        ("mysql-bin.000001:200", '{"v": "10:05"}'),
    ], dispatched[0]

    # 每个窗口只锚定一笔，且按各自的保留策略取值；
    # 换序后源坐标与镜像必须完全一致，不再出现“只剩第一笔/最后一笔”。
    windows = pipeline_bus.windows_for_key(KEY)
    assert len(windows) == 2, [w.window_id for w in windows]
    assert [w.delivered for w in windows] == [True, True]


def case_overlapping_windows_with_late_update() -> None:
    """场景二：相邻重叠窗口 + 消费前后补入 10:00:50 的迟到更新。"""
    reset()
    dedup = DedupWindowService()
    change_event = ChangeEventService()
    dedup.open_window(KEY, "2026-11-20T10:00:00", window_sec=60, strategy="last")
    dedup.open_window(KEY, "2026-11-20T10:00:40", window_sec=60, strategy="last")

    dedup.dedup_events([
        event("e_a1", "2026-11-20T10:00:00", {"v": "A"}, 100),
        event("e_b1", "2026-11-20T10:00:40", {"v": "B"}, 200),
    ], CONFIG)
    first = change_event.consume_dedup_manifest()
    assert [(item["源坐标"], item["变更后镜像"]) for item in first] == [
        ("mysql-bin.000001:100", '{"v": "A"}'),
        ("mysql-bin.000001:200", '{"v": "B"}'),
    ], first

    # 两窗都已交付；迟到记录落在两窗重合处 10:00:50。
    dedup.dedup_events([event("e_late", "2026-11-20T10:00:50", {"v": "LATE"}, 300)], CONFIG)
    assert len(pipeline_bus.dedup_conflicts) == 1
    conflict = pipeline_bus.dedup_conflicts[0]
    assert conflict["源坐标"] == "mysql-bin.000001:300"
    assert len(conflict["候选窗口"]) == 2
    late_window = conflict["归属窗口"]
    assert late_window is not None

    second = change_event.consume_dedup_manifest()
    assert [(item["源坐标"], item["变更后镜像"], item["窗口ID"]) for item in second] == [
        ("mysql-bin.000001:300", '{"v": "LATE"}', late_window)
    ], second
    # 已交付的两个窗口镜像保持不变，仍可按当时的窗口复核。
    delivered = {
        item["源坐标"]: item["变更后镜像"]
        for item in dedup.refresh_manifest()
        if item["窗口ID"] != late_window
    }
    assert delivered == {
        "mysql-bin.000001:100": '{"v": "A"}',
        "mysql-bin.000001:200": '{"v": "B"}',
    }, delivered

    # 同一源坐标重复提交只记一笔；重放跳过所有已交付坐标。
    dedup.dedup_events([
        event("e_a1", "2026-11-20T10:00:00", {"v": "A"}, 100),
        event("e_late", "2026-11-20T10:00:50", {"v": "LATE"}, 300),
    ], CONFIG)
    assert len(pipeline_bus.consumed_coords) == 3
    assert pipeline_bus.consumed_coords["mysql-bin.000001:100"]["repeat_submissions"] == 1
    replay = change_event.replay_dedup_manifest()
    assert replay["consumed"] == []
    assert replay["skipped_coords"] == [
        "mysql-bin.000001:100", "mysql-bin.000001:200", "mysql-bin.000001:300",
    ]


def case_per_window_strategy() -> None:
    """窗口内保留最早/最晚只在单窗内生效，不影响另一窗。"""
    reset()
    dedup = DedupWindowService()
    dedup.open_window(KEY, "2026-11-20T10:00:00", window_sec=60, strategy="last")
    dedup.open_window(KEY, "2026-11-20T10:00:40", window_sec=60, strategy="first")
    dedup.dedup_events([
        event("e_a1", "2026-11-20T10:00:00", {"v": "A"}, 100),
        event("e_b1", "2026-11-20T10:00:40", {"v": "B"}, 200),
        event("e_late", "2026-11-20T10:00:50", {"v": "LATE"}, 300),
    ], CONFIG)
    manifest = dedup.dedup_events([], CONFIG)
    kept = {
        item["源坐标"]: item["变更后镜像"]
        for item in manifest
        if item["去重键"] == KEY
    }
    # 10:00:00 窗口（last）保留更晚的 10:00:50；10:00:40 窗口（first）保留自己的锚定记录。
    assert kept == {
        "mysql-bin.000001:300": '{"v": "LATE"}',
        "mysql-bin.000001:200": '{"v": "B"}',
    }, kept


def case_strategy_switch_is_stable() -> None:
    """同一批数据在 first / last 下结果不同，但各自都不受提交顺序影响。"""
    rows = [
        event("e_a1", "2026-11-20T10:00:00", {"v": "A"}, 100),
        event("e_late", "2026-11-20T10:00:50", {"v": "LATE"}, 300),
    ]
    results = {}
    for strategy in ("first", "last"):
        for order in (rows, list(reversed(rows))):
            reset()
            dedup = DedupWindowService()
            dedup.open_window(KEY, "2026-11-20T10:00:00", window_sec=60, strategy=strategy)
            dedup.dedup_events(order, {**CONFIG, "去重策略": strategy})
            results.setdefault(strategy, set()).add(
                json.dumps([item["变更后镜像"] for item in dedup.refresh_manifest()])
            )
    assert results["first"] == {'["{\\"v\\": \\"A\\"}"]'}, results["first"]
    assert results["last"] == {'["{\\"v\\": \\"LATE\\"}"]'}, results["last"]


CASES = [
    case_swapped_updates,
    case_overlapping_windows_with_late_update,
    case_per_window_strategy,
    case_strategy_switch_is_stable,
]


def main() -> int:
    for case in CASES:
        case()
        print(f"PASS {case.__name__}")
    print(f"{len(CASES)} cases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
