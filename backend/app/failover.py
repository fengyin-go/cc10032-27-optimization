"""拓扑切换引擎：作业切换备用拓扑时继承有效限流状态并生成故障转移清单。

背景问题（序号27）：现有作业切换到备用拓扑后重新读取默认QPS（配置上限），
原背压状态没有随作业编排数据迁移，备用作业先按最大QPS启动、数秒后才被背压
模块限制，切换瞬间再次过载并产生重复积压。

改善后的切换流程：
  1. 读取作业配置：从 stream_job 取出作业编排数据及其来源版本；
  2. 继承有效吞吐状态：以 throughput_mgr 当前生效的限流值为继承基准，
     备用拓扑按继承值启动，而不是默认最大QPS；
  3. 生成故障转移清单：全部切换步骤完成后清单才标记「可用」。

切换不变量：
  - 旧拓扑和新拓扑不能同时持有写权限（单写交接，先收后放）；
  - 切换期间同一数据源只保留一个活动状态键；
  - 同一作业版本只对应一个生效限流值，并保留作业编排数据的来源版本；
  - 并发切换操作只产生一组结果（幂等键去重，重试返回同一清单）。

两种继承模式的取舍：
  - inherit（沿用旧限流值直到稳定）：切换瞬间不放量，适合旧拓扑仍在处理
    队列、下游积压未清空的场景，代价是吞吐恢复慢；
  - conservative（先降级到保守阈值再逐步恢复）：先降到配置上限的 50%，
    再按恢复计划分步回升，适合旧拓扑队列已排空、希望快速探底的场景，
    代价是切换初期吞吐更低。
"""
from __future__ import annotations

import copy
import threading
import time
from typing import Any

from app.services.throughput_mgr import (
    CONSERVATIVE_RATIO,
    RECOVERY_STEPS,
    effective_limit_for,
    upsert_inherited_limit,
    write_single_active_key,
)
from app.store import store

MODE_INHERIT = "inherit"
MODE_CONSERVATIVE = "conservative"
SWITCH_MODES = (MODE_INHERIT, MODE_CONSERVATIVE)

# 事务失败注入点（仅供测试核对回滚边界）：
#   after_handoff   写权限交接完成后失败
#   after_state_key 唯一活动状态键写入后失败
FAIL_POINTS = ("after_handoff", "after_state_key")


class SwitchError(RuntimeError):
    """拓扑切换事务中的可回滚异常。"""


class FailoverManager:
    """拓扑切换编排器：串行化并发切换，保证幂等与单写。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0

    # ---- 读取作业配置 ----

    @staticmethod
    def read_job_config(job_id: int) -> dict[str, Any] | None:
        """读取作业编排数据；缺省版本视为 1。"""
        job = store.find("stream_job", job_id)
        if job is not None and not job.get("version"):
            job["version"] = 1
        return job

    @staticmethod
    def _find_topology(name: str) -> dict[str, Any] | None:
        for row in store.rows("task_topology"):
            if row.get("拓扑名称") == name:
                return row
        return None

    def _next_manifest_no(self) -> str:
        self._seq += 1
        return f"FO-{int(time.time())}-{self._seq:04d}"

    @staticmethod
    def _manifest_by_key(idempotency_key: str) -> dict[str, Any] | None:
        for row in store.rows("failover_manifest"):
            if row.get("幂等键") == idempotency_key and row.get("status") in ("切换中", "可用"):
                return row
        return None

    # ---- 继承值计算 ----

    @staticmethod
    def plan_inheritance(mode: str, effective: float, max_qps: float) -> dict[str, Any]:
        """按继承模式计算备用拓扑的启动限流值与恢复计划。"""
        if mode == MODE_CONSERVATIVE:
            inherited = min(effective, max_qps * CONSERVATIVE_RATIO)
            plan = [
                {"step": idx + 1, "ratio": ratio, "target_limit": round(max_qps * ratio, 4)}
                for idx, ratio in enumerate(RECOVERY_STEPS)
                if max_qps * ratio > inherited
            ]
            return {"inherited_limit": inherited, "recovery_plan": plan}
        return {"inherited_limit": effective, "recovery_plan": []}

    # ---- 切换主流程 ----

    def switch_job_topology(
        self,
        job_id: int,
        target_topology: str,
        *,
        mode: str = MODE_INHERIT,
        idempotency_key: str | None = None,
        fail_point: str | None = None,
    ) -> dict[str, Any]:
        """执行作业到备用拓扑的切换，继承当前有效限流状态。

        全程持锁串行：并发/重试请求命中同一幂等键时返回已生成的同一组结果。
        fail_point 仅用于测试事务回滚边界。
        """
        if mode not in SWITCH_MODES:
            return {"ok": False, "error": f"未知继承模式「{mode}」，可选：{'、'.join(SWITCH_MODES)}"}
        if fail_point is not None and fail_point not in FAIL_POINTS:
            return {"ok": False, "error": f"未知失败注入点「{fail_point}」"}

        with self._lock:
            job = self.read_job_config(job_id)
            if job is None:
                return {"ok": False, "error": f"流式作业 {job_id} 不存在或已归档"}
            source_version = int(job.get("version", 1))
            old_topology = str(job.get("计算拓扑", ""))
            key = idempotency_key or f"sw-{job_id}-{target_topology}-{source_version}"

            # 幂等：同一幂等键的并发/重试只产生一组结果。
            existing = self._manifest_by_key(key)
            if existing is not None:
                return {"ok": existing["status"] == "可用", "deduplicated": True, "manifest": existing}

            # 幂等：作业已在目标拓扑上，直接返回最近一次可用清单。
            if old_topology == target_topology:
                for row in reversed(store.rows("failover_manifest")):
                    if (
                        int(row.get("作业ID", 0)) == int(job_id)
                        and row.get("目标拓扑") == target_topology
                        and row.get("status") == "可用"
                    ):
                        return {"ok": True, "deduplicated": True, "manifest": row}
                return {"ok": False, "error": f"作业「{job.get('作业名称')}」已在拓扑「{target_topology}」上"}

            target_row = self._find_topology(target_topology)
            if target_row is None:
                return {"ok": False, "error": f"目标拓扑「{target_topology}」不存在"}

            source_name = str(job.get("源端配置", ""))
            effective = effective_limit_for(source_name)
            plan = self.plan_inheritance(mode, effective["effective_limit"], effective["max_qps"])
            new_version = source_version + 1

            manifest = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("failover_manifest")), default=0
                ) + 1,
                "清单编号": self._next_manifest_no(),
                "作业ID": job_id,
                "作业名称": job.get("作业名称", ""),
                "数据源": source_name,
                "源拓扑": old_topology,
                "目标拓扑": target_topology,
                "继承模式": mode,
                "生效限流值": effective["effective_limit"],
                "继承限流值": plan["inherited_limit"],
                "配置上限": effective["max_qps"],
                "来源版本": source_version,
                "作业版本": new_version,
                "幂等键": key,
                "恢复计划": plan["recovery_plan"],
                "status": "切换中",
                "pending": True,
                "abnormal": False,
                "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "完成时间": "",
                "_created_ts": time.time(),
            }
            store.rows("failover_manifest").append(manifest)

            # ---- 事务：快照 → 交接 → 写唯一状态键 → 升级作业版本 ----
            old_row = self._find_topology(old_topology)
            snapshots: list[tuple[dict[str, Any], dict[str, Any]]] = []
            for row in (job, old_row, target_row):
                if row is not None:
                    snapshots.append((row, copy.deepcopy(row)))
            appended: list[tuple[str, dict[str, Any]]] = []
            key_snapshot = {
                id(row): copy.deepcopy(row)
                for row in store.rows("state_store")
                if str(row.get("状态键模式", "")).startswith("throttle://")
            }
            inherited_snapshot = {
                id(row): copy.deepcopy(row)
                for row in store.rows("throttle_inherited")
            }

            try:
                # 1) 单写交接：先收回旧拓扑写权限，再授予新拓扑，二者不同时可写。
                if old_row is not None:
                    old_row["_write_perm"] = False
                    if old_row.get("status") == "运行中":
                        old_row["status"] = "已停止"
                        old_row["运行状态"] = "已停止"
                target_row["_write_perm"] = True
                target_row["status"] = "运行中"
                target_row["运行状态"] = "运行中"
                target_row["pending"] = True
                if fail_point == "after_handoff":
                    raise SwitchError("注入失败：写权限交接后")

                # 2) 写入数据源唯一活动状态键（旧键全部停用，只留一个）。
                state_entry = write_single_active_key(
                    source_name,
                    plan["inherited_limit"],
                    job_id=job_id,
                    job_version=new_version,
                    source_version=source_version,
                    mode=mode,
                )
                appended.append(("state_store", state_entry))
                if fail_point == "after_state_key":
                    raise SwitchError("注入失败：活动状态键写入后")

                # 3) 记录作业新版本的继承限流配置（该版本唯一生效值）。
                inherited_entry = upsert_inherited_limit(
                    job_id,
                    new_version,
                    source_name=source_name,
                    inherited_limit=plan["inherited_limit"],
                    mode=mode,
                    manifest_no=manifest["清单编号"],
                    source_version=source_version,
                )
                appended.append(("throttle_inherited", inherited_entry))

                # 4) 更新作业编排数据：指向新拓扑、版本+1、保留来源版本。
                job["计算拓扑"] = target_topology
                job["version"] = new_version
                job["_orchestration_source_version"] = source_version
                job["_inherited_limit"] = plan["inherited_limit"]
                job["status"] = "运行中"
                job["pending"] = True
            except Exception as exc:  # noqa: BLE001 - 事务边界：任何异常都整组回滚
                for row, snapshot in snapshots:
                    row.clear()
                    row.update(snapshot)
                for row in store.rows("state_store"):
                    snap = key_snapshot.get(id(row))
                    if snap is not None:
                        row.clear()
                        row.update(snap)
                for row in store.rows("throttle_inherited"):
                    snap = inherited_snapshot.get(id(row))
                    if snap is not None:
                        row.clear()
                        row.update(snap)
                for table, entry in appended:
                    rows = store.rows(table)
                    if entry in rows:
                        rows.remove(entry)
                manifest["status"] = "已回滚"
                manifest["abnormal"] = True
                manifest["pending"] = False
                manifest["完成时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
                manifest["回滚原因"] = str(exc)
                return {"ok": False, "error": f"切换失败已回滚：{exc}", "manifest": manifest}

            # 切换完成后清单才标记可用。
            manifest["status"] = "可用"
            manifest["pending"] = False
            manifest["完成时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
            return {
                "ok": True,
                "deduplicated": False,
                "manifest": manifest,
                "inherited_limit": plan["inherited_limit"],
                "effective_before": effective["effective_limit"],
                "job_version": new_version,
                "source_version": source_version,
            }

    # ---- 清单查询 ----

    @staticmethod
    def list_manifests() -> list[dict[str, Any]]:
        return list(store.rows("failover_manifest"))

    @staticmethod
    def get_manifest(manifest_id: int) -> dict[str, Any] | None:
        for row in store.rows("failover_manifest"):
            if int(row.get("id", 0)) == int(manifest_id):
                return row
        return None


failover_manager = FailoverManager()
