"""批量编排处理器：对多条作业编排数据执行整组操作（序号27）。

能力：
  - 批量选择多条流式作业，一次提交保留整组结果（组级结果 + 逐条结果）；
  - 逐条记录成功失败，详情页可逐条核对；
  - 并发重试只接收一组版本（幂等键去重，重复提交返回同一组）；
  - 事务异常后回滚整组（atomic 模式下任一条失败即恢复全部快照）；
  - 作业编排数据按组级和逐条结果形成不同版本（组版本 vs 逐条版本）；
  - 另一个入口可联动生成整组处理台账，台账并发重试只记一次。

事务边界：批量操作以「组」为事务边界。atomic=True 时，任一作业编排数据
处理失败即触发整组回滚，已应用的变更按提交前快照恢复，组结果标记
「已回滚」；atomic=False 时退化为逐条独立提交，失败条目不影响其他条目，
组结果标记「部分失败」。
"""
from __future__ import annotations

import copy
import threading
import time
from typing import Any

from app.store import store

# 批量操作支持的动作及其状态流转：动作 → (目标状态, 允许的源状态集合)。
JOB_TRANSITIONS: dict[str, tuple[str, set[str]]] = {
    "启动作业": ("运行中", {"草稿", "待启动", "已暂停"}),
    "暂停作业": ("已暂停", {"运行中"}),
    "停止作业": ("已停止", {"待启动", "运行中", "已暂停"}),
}

GROUP_STATUS = ("已完成", "部分失败", "已回滚")


class BatchOrchestrator:
    """批量编排：串行化提交，保证并发重试只产生一组结果。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0

    def _next_group_no(self) -> str:
        self._seq += 1
        return f"GRP-{int(time.time())}-{self._seq:04d}"

    @staticmethod
    def _group_by_key(idempotency_key: str) -> dict[str, Any] | None:
        for row in store.rows("batch_group"):
            if row.get("幂等键") == idempotency_key:
                return row
        return None

    def submit_batch(
        self,
        job_ids: list[int],
        operation: str,
        *,
        idempotency_key: str | None = None,
        atomic: bool = True,
    ) -> dict[str, Any]:
        """批量处理多条作业编排数据，一次提交保留整组结果。

        幂等：同一幂等键的并发/重试只接收一组版本，直接返回已有组结果。
        事务：atomic 模式下任一条目失败即回滚整组。
        """
        if operation not in JOB_TRANSITIONS:
            return {"ok": False, "error": f"不支持的操作「{operation}」，可选：{'、'.join(JOB_TRANSITIONS)}"}
        if not job_ids:
            return {"ok": False, "error": "未选择任何作业编排数据"}

        with self._lock:
            key = idempotency_key or f"batch-{operation}-{'-'.join(str(j) for j in sorted(job_ids))}"
            existing = self._group_by_key(key)
            if existing is not None:
                return {"ok": existing["status"] != "已回滚", "deduplicated": True, "group": existing}

            target_status, allowed_from = JOB_TRANSITIONS[operation]
            snapshots: list[tuple[dict[str, Any], dict[str, Any]]] = []
            items: list[dict[str, Any]] = []
            any_failed = False

            for seq, job_id in enumerate(job_ids, start=1):
                job = store.find("stream_job", int(job_id))
                item: dict[str, Any] = {
                    "seq": seq,
                    "job_id": job_id,
                    "作业名称": (job or {}).get("作业名称", ""),
                    "ok": False,
                    "message": "",
                    "item_version": seq,  # 逐条结果版本
                    "rolled_back": False,
                }
                if job is None:
                    item["message"] = f"作业 {job_id} 不存在或已归档"
                    any_failed = True
                    items.append(item)
                    continue
                before_status = str(job.get("status", ""))
                before_version = int(job.get("version", 1) or 1)
                item["before_status"] = before_status
                item["job_version_before"] = before_version
                if before_status not in allowed_from:
                    item["message"] = f"状态「{before_status}」不允许执行「{operation}」"
                    any_failed = True
                    items.append(item)
                    continue
                snapshots.append((job, copy.deepcopy(job)))
                job["status"] = target_status
                job["pending"] = target_status != "已停止"
                job["abnormal"] = False
                job["version"] = before_version + 1  # 作业编排数据自身版本递增
                item["ok"] = True
                item["message"] = f"{operation}成功：{before_status} → {target_status}"
                item["after_status"] = target_status
                item["job_version_after"] = before_version + 1
                items.append(item)

            group_status = "已完成"
            if any_failed:
                if atomic:
                    # 事务异常：回滚整组，恢复全部作业编排数据快照。
                    for job, snapshot in snapshots:
                        job.clear()
                        job.update(snapshot)
                    for item in items:
                        if item["ok"]:
                            item["rolled_back"] = True
                            item["after_status"] = item.get("before_status", "")
                            item["job_version_after"] = item.get("job_version_before", 1)
                    group_status = "已回滚"
                else:
                    group_status = "部分失败"

            group = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("batch_group")), default=0
                ) + 1,
                "组编号": self._next_group_no(),
                "操作": operation,
                "幂等键": key,
                "原子提交": atomic,
                "status": group_status,
                "version": 1,  # 组级结果版本
                "total": len(items),
                "success_count": sum(1 for item in items if item["ok"]),
                "fail_count": sum(1 for item in items if not item["ok"]),
                "items": items,
                "ledger_id": None,
                "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "_created_ts": time.time(),
            }
            store.rows("batch_group").append(group)
            return {"ok": group_status != "已回滚", "deduplicated": False, "group": group}

    # ---- 查询 ----

    @staticmethod
    def list_groups() -> list[dict[str, Any]]:
        return list(store.rows("batch_group"))

    @staticmethod
    def get_group(group_id: int) -> dict[str, Any] | None:
        for row in store.rows("batch_group"):
            if int(row.get("id", 0)) == int(group_id):
                return row
        return None

    # ---- 整组处理台账（另一个入口联动生成） ----

    def generate_ledger(self, group_id: int, *, entry: str = "batch_page") -> dict[str, Any]:
        """为整组结果生成处理台账；同一组并发重试只记一次。"""
        with self._lock:
            group = self.get_group(group_id)
            if group is None:
                return {"ok": False, "error": f"批量组 {group_id} 不存在"}
            for row in store.rows("batch_ledger"):
                if row.get("group_id") == group["id"]:
                    return {"ok": True, "deduplicated": True, "ledger": row}
            ledger = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("batch_ledger")), default=0
                ) + 1,
                "台账编号": f"LGR-{int(time.time())}-{group['id']:04d}",
                "group_id": group["id"],
                "组编号": group["组编号"],
                "来源入口": entry,
                "操作": group["操作"],
                "组状态": group["status"],
                "组版本": group["version"],
                "条目数": group["total"],
                "成功数": group["success_count"],
                "失败数": group["fail_count"],
                "作业版本快照": [
                    {
                        "job_id": item["job_id"],
                        "作业名称": item.get("作业名称", ""),
                        "job_version": item.get("job_version_after", item.get("job_version_before")),
                        "结果": "成功" if item["ok"] else "失败",
                        "已回滚": item.get("rolled_back", False),
                    }
                    for item in group["items"]
                ],
                "生成时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            store.rows("batch_ledger").append(ledger)
            group["ledger_id"] = ledger["id"]
            return {"ok": True, "deduplicated": False, "ledger": ledger}

    @staticmethod
    def list_ledgers() -> list[dict[str, Any]]:
        return list(store.rows("batch_ledger"))


batch_orchestrator = BatchOrchestrator()
