"""拓扑切换与批量编排接口（序号27）。

- POST /api/ops/failover/switch          作业切换备用拓扑（继承有效限流状态）
- GET  /api/ops/failover/manifests       故障转移清单列表
- GET  /api/ops/failover/manifests/{id}  故障转移清单详情
- GET  /api/ops/throughput/active        活动限流状态键核查（确认无两个活动配置）
- GET  /api/ops/throughput/inherited     按作业版本读取继承的限流配置
- POST /api/ops/batch/submit             批量处理作业编排数据（整组提交）
- GET  /api/ops/batch/groups             批量组列表
- GET  /api/ops/batch/groups/{id}        批量组详情（逐条成功失败）
- POST /api/ops/batch/groups/{id}/ledger 另一个入口生成整组处理台账
- GET  /api/ops/batch/ledgers            整组处理台账列表
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.batch import batch_orchestrator
from app.failover import SWITCH_MODES, failover_manager
from app.services import service_for

router = APIRouter(prefix="/api/ops", tags=["拓扑切换与批量编排"])


class SwitchPayload(BaseModel):
    job_id: int
    target_topology: str
    mode: str = "inherit"
    idempotency_key: str | None = None
    fail_point: str | None = None


class BatchPayload(BaseModel):
    job_ids: list[int] = Field(default_factory=list)
    operation: str
    idempotency_key: str | None = None
    atomic: bool = True


class LedgerPayload(BaseModel):
    entry: str = "batch_page"


def _throughput_service():
    service = service_for("throughput_mgr")
    if service is None:
        raise HTTPException(status_code=404, detail="吞吐管理模块未注册")
    return service


@router.post("/failover/switch")
def switch_topology(payload: SwitchPayload) -> dict[str, Any]:
    """执行拓扑切换：读取作业配置 → 继承有效吞吐状态 → 生成故障转移清单。"""
    result = failover_manager.switch_job_topology(
        payload.job_id,
        payload.target_topology.strip(),
        mode=payload.mode.strip() or "inherit",
        idempotency_key=(payload.idempotency_key or "").strip() or None,
        fail_point=payload.fail_point,
    )
    if not result.get("ok") and "manifest" not in result:
        raise HTTPException(status_code=400, detail=result.get("error", "切换失败"))
    return result


@router.get("/failover/modes")
def switch_modes() -> dict[str, Any]:
    """列出可用的限流继承模式及取舍说明。"""
    return {
        "modes": list(SWITCH_MODES),
        "desc": {
            "inherit": "沿用旧限流值直到稳定：切换不放量，适合旧拓扑队列未清空的场景",
            "conservative": "先降级到保守阈值再逐步恢复：先降到上限50%再分步回升",
        },
    }


@router.get("/failover/manifests")
def list_manifests() -> dict[str, Any]:
    items = failover_manager.list_manifests()
    return {"items": items, "total": len(items)}


@router.get("/failover/manifests/{manifest_id}")
def get_manifest(manifest_id: int) -> dict[str, Any]:
    manifest = failover_manager.get_manifest(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"故障转移清单 {manifest_id} 不存在")
    return manifest


@router.get("/throughput/active")
def active_throttles(source: str | None = Query(default=None)) -> dict[str, Any]:
    """活动限流状态键核查：切换期间同一数据源不应出现两个活动配置。"""
    service = _throughput_service()
    return service.list_active_state_keys(source)


@router.get("/throughput/effective")
def effective_limit(source: str = Query(...)) -> dict[str, Any]:
    """读取数据源当前生效的限流值（切换继承基准）。"""
    service = _throughput_service()
    return service.effective_limit(source)


@router.get("/throughput/inherited")
def inherited_limit(
    job_id: int = Query(...),
    version: int | None = Query(default=None),
) -> dict[str, Any]:
    """按作业版本读取继承的限流配置；同一版本只对应一个生效限流值。"""
    service = _throughput_service()
    return service.inherited_limit_for_job(job_id, version)


@router.post("/batch/submit")
def submit_batch(payload: BatchPayload) -> dict[str, Any]:
    """批量处理多条作业编排数据：一次提交保留整组结果，异常回滚整组。"""
    result = batch_orchestrator.submit_batch(
        [int(j) for j in payload.job_ids],
        payload.operation.strip(),
        idempotency_key=(payload.idempotency_key or "").strip() or None,
        atomic=payload.atomic,
    )
    if not result.get("ok") and "group" not in result:
        raise HTTPException(status_code=400, detail=result.get("error", "批量提交失败"))
    return result


@router.get("/batch/groups")
def list_groups() -> dict[str, Any]:
    items = batch_orchestrator.list_groups()
    return {"items": items, "total": len(items)}


@router.get("/batch/groups/{group_id}")
def get_group(group_id: int) -> dict[str, Any]:
    group = batch_orchestrator.get_group(group_id)
    if group is None:
        raise HTTPException(status_code=404, detail=f"批量组 {group_id} 不存在")
    return group


@router.post("/batch/groups/{group_id}/ledger")
def generate_ledger(group_id: int, payload: LedgerPayload) -> dict[str, Any]:
    """由另一个入口为整组结果生成处理台账；并发重试只记一次。"""
    result = batch_orchestrator.generate_ledger(group_id, entry=payload.entry.strip() or "batch_page")
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "台账生成失败"))
    return result


@router.get("/batch/ledgers")
def list_ledgers() -> dict[str, Any]:
    items = batch_orchestrator.list_ledgers()
    return {"items": items, "total": len(items)}
