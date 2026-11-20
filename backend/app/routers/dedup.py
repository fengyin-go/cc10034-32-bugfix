"""去重与消费管道接口：按事件时间分窗、生成去重清单、消费回执和重放。

这些是领域动作接口，不是通用资源 CRUD，所以单独成一路由，注册在通用
``/api/{module}`` 之前，避免被动态路径抢先匹配。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.pipeline import ChangeEventRecord, parse_event_time, pipeline_bus
from app.services import service_for
from app.services.dedup_window import DedupWindowService

router = APIRouter(prefix="/api/dedup", tags=["去重管道"])


class WindowInput(BaseModel):
    """单条变更数据的入参；镜像可以是对象或 JSON 文本。"""
    event_id: str | None = None
    source_db: str = "shop"
    source_table: str = "t_order"
    operation: str = "UPDATE"
    before_image: Any = None
    after_image: Any = None
    tx_id: str = ""
    binlog_file: str = ""
    binlog_pos: int | str = ""
    gtid: str = ""
    event_time: str | None = None
    dedup_key: str | None = None


class WindowRef(BaseModel):
    key: str
    anchor: str
    window_sec: float = 60
    strategy: str = "last"


class DedupConfig(BaseModel):
    dedup_key: str = "来源表"
    window_sec: float = 60
    strategy: str = "last"
    watermark: str = "事件时间"
    expire_cleanup: bool = True


class DedupRequest(BaseModel):
    events: list[WindowInput]
    config: DedupConfig = Field(default_factory=DedupConfig)
    windows: list[WindowRef] = Field(default_factory=list)


class ConsumeRequest(BaseModel):
    batch_size: int = 50


def _dedup_service() -> DedupWindowService:
    service = service_for("dedup_window")
    assert isinstance(service, DedupWindowService)
    return service


def to_manifest_event(payload: WindowInput, default_key: str) -> dict[str, Any]:
    """把接口入参转成去重窗口服务认识的事件字典。"""
    moment = parse_event_time({"事件时间": payload.event_time}) if payload.event_time else None
    if payload.event_time and moment is None:
        raise HTTPException(status_code=422, detail=f"无法解析事件时间：{payload.event_time}")
    event_time = (moment or datetime.now()).isoformat()
    source_coord = payload.gtid or (
        f"{payload.binlog_file}:{payload.binlog_pos}" if payload.binlog_file else ""
    ) or payload.event_id or ""
    return {
        "事件ID": payload.event_id or source_coord,
        "来源表": f"{payload.source_db}.{payload.source_table}",
        "操作类型": payload.operation,
        "变更前镜像": payload.before_image,
        "变更后镜像": payload.after_image,
        "事务ID": payload.tx_id,
        "事件时间": event_time,
        "源坐标": source_coord,
        "binlog_file": payload.binlog_file,
        "binlog_pos": payload.binlog_pos,
        "gtid": payload.gtid,
        default_key: payload.dedup_key or f"{payload.source_db}.{payload.source_table}",
    }


@router.post("/windows")
def publish_and_dedup(request: DedupRequest) -> dict[str, Any]:
    """登记（可选）显式窗口，按事件时间归窗去重，返回去重清单。"""
    service = _dedup_service()
    config = {
        "去重键": request.config.dedup_key,
        "窗口大小": str(request.config.window_sec),
        "去重策略": request.config.strategy,
        "水位字段": request.config.watermark,
        "过期清理": "自动" if request.config.expire_cleanup else "关闭",
    }
    for ref in request.windows:
        service.open_window(
            ref.key, ref.anchor, window_sec=ref.window_sec, strategy=ref.strategy
        )
    events = [to_manifest_event(item, request.config.dedup_key) for item in request.events]
    manifest = service.dedup_events(events, config)
    keys = {ref.key for ref in request.windows}
    keys.update(item.get("去重键", "") for item in manifest)
    keys.discard("")
    return {
        "manifest": manifest,
        "windows": {
            key: service.window_overview(key) for key in sorted(keys)
        },
        "conflicts": [
            item for item in pipeline_bus.dedup_conflicts if item.get("去重键") in keys
        ],
        "dedup_rate": service.compute_dedup_rate(len(events), len(manifest)),
    }


@router.get("/windows")
def window_overview(key: str = Query(...)) -> dict[str, Any]:
    """查看某个去重键的全部有效窗口及归属、交付情况。"""
    service = _dedup_service()
    return {
        "key": key,
        "windows": service.window_overview(key),
        "conflicts": [
            item for item in pipeline_bus.dedup_conflicts if item.get("去重键") == key
        ],
    }


@router.get("/manifest")
def get_manifest(only_pending: bool = Query(default=False)) -> dict[str, Any]:
    """返回去重后的变更明细（可只看未交付部分）。"""
    service = _dedup_service()
    manifest = service.pending_manifest() if only_pending else service.refresh_manifest()
    return {
        "items": manifest,
        "total": len(manifest),
        "receipts": {coord: dict(value) for coord, value in pipeline_bus.consumed_coords.items()},
    }


@router.post("/consume")
def consume_manifest(request: ConsumeRequest) -> dict[str, Any]:
    """变更事件直接消费去重清单，落库源坐标回执并回写执行结果。"""
    change_event = service_for("change_event")
    if change_event is None:
        raise HTTPException(status_code=500, detail="变更事件服务未注册")
    consumed = change_event.consume_dedup_manifest(batch_size=request.batch_size)
    return {
        "consumed": consumed,
        "coordinates": [item["源坐标"] for item in consumed],
        "receipts": {coord: dict(value) for coord, value in pipeline_bus.consumed_coords.items()},
    }


@router.post("/replay")
def replay_manifest(
    request: ConsumeRequest,
    key: str | None = Query(default=None),
    window_id: list[str] | None = Query(default=None),
) -> dict[str, Any]:
    """重放去重清单：已交付源坐标跳过，只消费未交付部分。"""
    change_event = service_for("change_event")
    if change_event is None:
        raise HTTPException(status_code=500, detail="变更事件服务未注册")
    window_ids = set(window_id) if window_id else None
    result = change_event.replay_dedup_manifest(
        key=key, window_ids=window_ids, batch_size=request.batch_size
    )
    return {
        "consumed": result["consumed"],
        "coordinates": [item["源坐标"] for item in result["consumed"]],
        "skipped_coords": result["skipped_coords"],
        "receipts": {coord: dict(value) for coord, value in pipeline_bus.consumed_coords.items()},
    }


@router.post("/publish")
def publish_events(request: DedupRequest) -> dict[str, Any]:
    """把入参事件发布到原始管道队列（验证旧链路用）。"""
    published = []
    for item in request.events:
        moment = parse_event_time({"事件时间": item.event_time}) if item.event_time else None
        record = ChangeEventRecord(
            event_id=item.event_id or f"evt_{len(pipeline_bus.event_queue)}",
            source_db=item.source_db,
            source_table=item.source_table,
            operation=item.operation,
            before_image=item.before_image if isinstance(item.before_image, dict) else None,
            after_image=item.after_image if isinstance(item.after_image, dict) else None,
            tx_id=item.tx_id,
            binlog_file=item.binlog_file,
            binlog_pos=int(item.binlog_pos or 0),
            gtid=item.gtid,
            event_time=moment or datetime.now(),
        )
        published.append(pipeline_bus.publish_event(record))
    return {"published": published, "queue_depth": len(pipeline_bus.event_queue)}
