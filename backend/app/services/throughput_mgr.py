"""吞吐管理领域服务：管理流式计算任务的吞吐容量。

增强能力: 背压自动检测、QPS限流触发、扩容规则评估、降级策略验证、关联topology状态

序号27增强：拓扑切换场景下的限流继承读取。
  - effective_limit_for：读取数据源当前生效的限流值（而不是默认最大QPS）；
  - active_state_keys：列出活动限流状态键，切换期间同一数据源只保留一个；
  - read_inherited_limit：按作业版本读取继承的限流配置，同一版本只对应一个生效限流值；
  - write_single_active_key：写入唯一活动状态键并保留作业编排数据的来源版本。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 限流策略对应的降级系数，与 apply_throttle_action 保持一致。
STRATEGY_FACTOR = {"token_bucket": 0.7, "leaky_bucket": 0.5}
DEFAULT_STRATEGY_FACTOR = 0.8
# 保守降级模式：先降到配置上限的 50%，再按恢复计划逐步回升。
CONSERVATIVE_RATIO = 0.5
RECOVERY_STEPS = (0.5, 0.75, 1.0)


def _state_key_pattern(source_name: str) -> str:
    return f"throttle://{source_name}"


def _find_config(source_name: str) -> dict[str, Any] | None:
    for row in store.rows("throughput_mgr"):
        if row.get("数据源") == source_name:
            return row
    return None


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def active_state_keys(source_name: str | None = None) -> list[dict[str, Any]]:
    """列出状态存储中处于「已生效」的限流状态键。

    切换期间同一数据源只能有一个活动状态键；该函数用于核查是否出现
    两个活动配置并存的情况。
    """
    keys = [
        row for row in store.rows("state_store")
        if str(row.get("状态键模式", "")).startswith("throttle://")
        and row.get("status") == "已生效"
    ]
    if source_name is not None:
        keys = [row for row in keys if row.get("状态键模式") == _state_key_pattern(source_name)]
    return keys


def effective_limit_for(source_name: str) -> dict[str, Any]:
    """读取数据源当前生效的限流值。

    优先取活动状态键上的限流值（背压模块已写入的降级结果），
    其次按吞吐配置的触发状态与限流策略折算，最后才回落到配置上限。
    拓扑切换必须继承这个值，而不是重新读取默认最大QPS。
    """
    config = _find_config(source_name)
    max_qps = _safe_float((config or {}).get("最大QPS"), 1000.0)
    strategy = (config or {}).get("限流策略", "token_bucket")
    keys = active_state_keys(source_name)
    if keys:
        latest = max(keys, key=lambda row: _safe_float(row.get("_timestamp"), 0.0))
        effective = _safe_float(latest.get("_new_limit"), max_qps)
        throttled = True
        origin = "active_state_key"
    elif (config or {}).get("status") == "已触发":
        factor = STRATEGY_FACTOR.get(strategy, DEFAULT_STRATEGY_FACTOR)
        effective = max_qps * factor
        throttled = True
        origin = "triggered_config"
    else:
        effective = max_qps
        throttled = False
        origin = "config_max"
    return {
        "source": source_name,
        "max_qps": max_qps,
        "effective_limit": effective,
        "throttled": throttled,
        "strategy": strategy,
        "origin": origin,
        "active_key_count": len(keys),
    }


def write_single_active_key(
    source_name: str,
    limit: float,
    *,
    job_id: int,
    job_version: int,
    source_version: int,
    mode: str,
) -> dict[str, Any]:
    """把限流值写成数据源唯一的活动状态键。

    先将该数据源已有的活动键全部停用（保留历史），再追加唯一的新活动键，
    键上记录作业编排数据的当前版本与来源版本。
    """
    pattern = _state_key_pattern(source_name)
    for row in store.rows("state_store"):
        if row.get("状态键模式") == pattern and row.get("status") == "已生效":
            row["status"] = "已停用"
            row["pending"] = False
    entry = {
        "id": max(
            (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
        ) + 1,
        "配置名称": f"throttle_{source_name}_{int(time.time())}",
        "后端类型": "memory",
        "状态键模式": pattern,
        "TTL时长": "3600",
        "检查点间隔": "30",
        "增量模式": "overwrite",
        "清理策略": "ttl_expire",
        "status": "已生效",
        "pending": True,
        "abnormal": False,
        "_throttle_source": source_name,
        "_new_limit": limit,
        "_job_id": job_id,
        "_job_version": job_version,
        "_source_version": source_version,
        "_mode": mode,
        "_timestamp": time.time(),
    }
    store.rows("state_store").append(entry)
    return entry


def read_inherited_limit(job_id: int, version: int | None = None) -> dict[str, Any]:
    """按作业版本读取继承的限流配置。

    每个作业版本只保留一条继承配置；active 标记当前生效的那一条。
    指定版本时返回该版本的继承配置（无论是否仍是当前生效值），
    不传版本时返回当前生效的那一条。
    """
    rows = [
        row for row in store.rows("throttle_inherited")
        if int(row.get("job_id", 0)) == int(job_id)
    ]
    if version is not None:
        matched = [row for row in rows if int(row.get("job_version", 0)) == int(version)]
        record = matched[-1] if matched else None
        return {
            "ok": record is not None,
            "job_id": job_id,
            "job_version": version,
            "records": matched,
            "record": record,
            "record_count": len(matched),
            "is_current": bool(record and record.get("active")),
        }
    active = [row for row in rows if row.get("active")]
    return {
        "ok": bool(active),
        "job_id": job_id,
        "job_version": None,
        "records": rows,
        "active": active[-1] if active else None,
        "active_count": len(active),
    }


def upsert_inherited_limit(
    job_id: int,
    job_version: int,
    *,
    source_name: str,
    inherited_limit: float,
    mode: str,
    manifest_no: str,
    source_version: int,
) -> dict[str, Any]:
    """写入作业某个版本的继承限流配置。

    不变量：每个 (作业, 版本) 只保留一条继承配置；全作业只有最新版本
    处于 active（当前版本只对应一个生效限流值）。
    """
    rows = store.rows("throttle_inherited")
    existing: dict[str, Any] | None = None
    for row in rows:
        if int(row.get("job_id", 0)) != int(job_id):
            continue
        if int(row.get("job_version", 0)) == int(job_version):
            existing = row
        row["active"] = False
    if existing is not None:
        existing.update({
            "数据源": source_name,
            "继承限流值": inherited_limit,
            "继承模式": mode,
            "清单编号": manifest_no,
            "source_version": source_version,
            "active": True,
            "created_at": time.time(),
        })
        return existing
    entry = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "job_id": job_id,
        "job_version": job_version,
        "数据源": source_name,
        "继承限流值": inherited_limit,
        "继承模式": mode,
        "清单编号": manifest_no,
        "source_version": source_version,
        "active": True,
        "created_at": time.time(),
    }
    rows.append(entry)
    return entry


class ThroughputMgrService(BaseModuleService):
    MODULE = "throughput_mgr"
    ENTITY = "吞吐配置"
    LIST_FIELDS = ["配置名称", "数据源", "最大QPS", "并发分区", "背压阈值", "限流策略", "扩容规则"]
    REQUIRED_FIELDS = ["配置名称", "数据源", "最大QPS"]
    STATUS_ORDER = ["草稿", "已生效", "已触发", "已关闭"]
    ACTION_RULES = {"启用配置": "草稿", "关闭配置": "已生效"}
    NEGATIVE_ACTIONS = []

    def monitor_backpressure(
        self, source_name: str, current_qps: float,
    ) -> dict[str, Any]:
        """监控背压状态：计算负载比、触发限流判断。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("数据源") == source_name:
                config = r
                break
        try:
            max_qps = float((config or {}).get("最大QPS", "1000"))
            threshold = float((config or {}).get("背压阈值", "0.75"))
        except (ValueError, TypeError):
            max_qps, threshold = 1000.0, 0.75

        load_ratio = current_qps / max(max_qps, 1)
        is_overloaded = load_ratio > threshold

        if config and is_overloaded and config.get("status") != "已触发":
            config["status"] = "已触发"
            config["pending"] = True
            config["abnormal"] = True

        return {
            "ok": True,
            "source": source_name,
            "current_qps": current_qps,
            "max_qps": max_qps,
            "load_ratio": round(load_ratio, 4),
            "threshold": threshold,
            "overloaded": is_overloaded,
            "strategy": (config or {}).get("限流策略", "token_bucket"),
        }

    def apply_throttle_action(
        self, source_name: str,
    ) -> dict[str, Any]:
        """应用限流策略：计算降级后的QPS并写入状态存储。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("数据源") == source_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"吞吐配置「{source_name}」不存在"}

        try:
            max_qps = float(config.get("最大QPS", "1000"))
        except (ValueError, TypeError):
            max_qps = 1000.0

        strategy = config.get("限流策略", "token_bucket")
        if strategy == "token_bucket":
            new_limit = max_qps * 0.7
        elif strategy == "leaky_bucket":
            new_limit = max_qps * 0.5
        else:
            new_limit = max_qps * 0.8

        # 写入状态存储记录
        state_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
            ) + 1,
            "配置名称": f"throttle_{source_name}_{int(time.time())}",
            "后端类型": "memory",
            "状态键模式": f"throttle://{source_name}",
            "TTL时长": "3600",
            "检查点间隔": "30",
            "增量模式": "overwrite",
            "清理策略": "ttl_expire",
            "status": "已生效",
            "pending": True,
            "abnormal": True,
            "_throttle_source": source_name,
            "_original_qps": max_qps,
            "_new_limit": new_limit,
            "_strategy": strategy,
            "_timestamp": time.time(),
        }
        store.rows("state_store").append(state_entry)

        return {
            "ok": True,
            "source": source_name,
            "strategy": strategy,
            "original_qps": max_qps,
            "new_limit": new_limit,
            "reduction_pct": round((1 - new_limit / max_qps) * 100, 1),
        }

    def evaluate_auto_scale(self, source_name: str) -> dict[str, Any]:
        """评估自动扩容规则：判断是否需要扩容及扩容数量。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("数据源") == source_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"吞吐配置「{source_name}」不存在"}

        scale_rule = config.get("扩容规则", "").lower()
        try:
            current_partitions = int(config.get("并发分区", "4"))
        except (ValueError, TypeError):
            current_partitions = 4

        if "double" in scale_rule or "翻倍" in scale_rule:
            new_partitions = min(current_partitions * 2, 64)
        elif "incremental" in scale_rule or "增量" in scale_rule:
            new_partitions = min(current_partitions + 4, 64)
        else:
            new_partitions = current_partitions

        should_scale = config.get("status") == "已触发"

        return {
            "ok": True,
            "source": source_name,
            "should_scale": should_scale,
            "current_partitions": current_partitions,
            "new_partitions": new_partitions if should_scale else current_partitions,
            "scale_rule": scale_rule,
        }

    def effective_limit(self, source_name: str) -> dict[str, Any]:
        """读取数据源当前生效的限流值（拓扑切换时的继承基准）。"""
        return effective_limit_for(source_name)

    def list_active_state_keys(self, source_name: str | None = None) -> dict[str, Any]:
        """列出活动限流状态键，确认切换期间没有两个活动配置。"""
        keys = active_state_keys(source_name)
        by_source: dict[str, int] = {}
        for row in keys:
            src = str(row.get("_throttle_source") or row.get("状态键模式", "").removeprefix("throttle://"))
            by_source[src] = by_source.get(src, 0) + 1
        return {
            "ok": all(count <= 1 for count in by_source.values()),
            "total_active": len(keys),
            "by_source": by_source,
            "violations": [src for src, count in by_source.items() if count > 1],
            "keys": keys,
        }

    def inherited_limit_for_job(self, job_id: int, version: int | None = None) -> dict[str, Any]:
        """按作业版本读取继承的限流配置。"""
        return read_inherited_limit(job_id, version)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["triggered"] = sum(1 for r in rows if r.get("status") == "已触发")
        base["active"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["active_state_keys"] = len(active_state_keys())
        return base