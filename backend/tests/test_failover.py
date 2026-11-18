"""拓扑切换与批量编排测试（序号27）。

覆盖：
  - 切换继承当前有效限流值，而不是重新读取默认最大QPS
  - 保守降级模式：先降到保守阈值再按恢复计划逐步回升
  - 单写交接：旧拓扑和新拓扑不同时持有写权限
  - 切换期间同一数据源只保留一个活动状态键
  - 故障转移清单在切换完成后才标记「可用」，异常则整组回滚
  - 并发/重试切换只产生一组结果（幂等）
  - 吞吐管理按作业版本读取继承的限流配置，一个版本一个生效值
  - 批量处理：逐条成功失败、整组结果保留、并发重试只接收一组版本
  - 事务异常回滚整组；另一个入口生成整组台账且只记一次
"""
from __future__ import annotations

import threading
import time

import pytest

from app.batch import batch_orchestrator
from app.failover import failover_manager
from app.services.throughput_mgr import (
    active_state_keys,
    effective_limit_for,
    read_inherited_limit,
)
from app.store import store


# ---------------------------------------------------------------------------
# 测试辅助：构造唯一的作业编排数据、拓扑、吞吐配置与限流状态键
# ---------------------------------------------------------------------------

_seq = 0


def _uniq(prefix: str) -> str:
    global _seq
    _seq += 1
    return f"{prefix}_{_seq}_{int(time.time() * 1000) % 100000}"


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _make_job(source: str, topology: str, status: str = "运行中", version: int = 1) -> dict:
    job = {
        "id": _next_id("stream_job"),
        "作业名称": _uniq("作业"),
        "源端配置": source,
        "窗口策略": "窗口策略样例",
        "计算拓扑": topology,
        "并行度": "4",
        "启动时间": "2026-11-18",
        "运行水位": "0",
        "status": status,
        "pending": False,
        "abnormal": False,
        "version": version,
    }
    store.rows("stream_job").append(job)
    return job


def _make_topology(name: str | None = None, status: str = "运行中") -> dict:
    row = {
        "id": _next_id("task_topology"),
        "拓扑名称": name or _uniq("topo"),
        "算子链": "源→算子→落地",
        "并行度": "4",
        "数据分发": "hash",
        "故障转移": "standby",
        "资源配额": "cpu:4",
        "运行状态": status,
        "status": status,
        "pending": False,
        "abnormal": False,
    }
    store.rows("task_topology").append(row)
    return row


def _make_throughput(source: str, max_qps: str = "1000", strategy: str = "token_bucket",
                     status: str = "已生效") -> dict:
    row = {
        "id": _next_id("throughput_mgr"),
        "配置名称": _uniq("吞吐"),
        "数据源": source,
        "最大QPS": max_qps,
        "并发分区": "4",
        "背压阈值": "0.75",
        "限流策略": strategy,
        "扩容规则": "double",
        "status": status,
        "pending": False,
        "abnormal": False,
    }
    store.rows("throughput_mgr").append(row)
    return row


def _make_throttle_key(source: str, limit: float, ts: float | None = None) -> dict:
    row = {
        "id": _next_id("state_store"),
        "配置名称": _uniq("throttle"),
        "后端类型": "memory",
        "状态键模式": f"throttle://{source}",
        "TTL时长": "3600",
        "检查点间隔": "30",
        "增量模式": "overwrite",
        "清理策略": "ttl_expire",
        "status": "已生效",
        "pending": True,
        "abnormal": True,
        "_throttle_source": source,
        "_new_limit": limit,
        "_timestamp": ts if ts is not None else time.time(),
    }
    store.rows("state_store").append(row)
    return row


def _active_keys(source: str) -> list[dict]:
    return active_state_keys(source)


def _switch_pair(status_old: str = "运行中", status_new: str = "已部署"):
    """造一组旧/新拓扑并返回 (old_row, new_row)。"""
    old = _make_topology(status=status_old)
    new = _make_topology(status=status_new)
    return old, new


# ---------------------------------------------------------------------------
# 拓扑切换：限流继承
# ---------------------------------------------------------------------------

class TestTopologySwitchInheritance:
    """切换必须继承当前有效限流状态，而不是默认最大QPS。"""

    def test_switch_inherits_effective_limit_not_default_max(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="1000")
        _make_throttle_key(source, 700.0)  # 背压模块已把限流降到 700
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], mode="inherit",
        )
        assert result["ok"] is True
        # 继承值是背压后的 700，而不是默认上限 1000
        assert result["inherited_limit"] == pytest.approx(700.0)
        assert result["effective_before"] == pytest.approx(700.0)
        manifest = result["manifest"]
        assert manifest["继承限流值"] == pytest.approx(700.0)
        assert manifest["配置上限"] == pytest.approx(1000.0)

    def test_switch_without_throttle_uses_config_max(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="800")
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert result["ok"] is True
        assert result["inherited_limit"] == pytest.approx(800.0)

    def test_conservative_mode_degrades_then_recovers(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="1000")
        _make_throttle_key(source, 700.0)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], mode="conservative",
        )
        assert result["ok"] is True
        # 先降级到保守阈值（上限的 50%）
        assert result["inherited_limit"] == pytest.approx(500.0)
        plan = result["manifest"]["恢复计划"]
        assert len(plan) >= 2, "保守模式应带逐步恢复计划"
        assert plan[-1]["target_limit"] == pytest.approx(1000.0)

    def test_inherit_mode_has_no_recovery_plan(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="1000")
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], mode="inherit",
        )
        assert result["ok"] is True
        assert result["manifest"]["恢复计划"] == []

    def test_unknown_mode_rejected(self):
        old = _make_topology()
        job = _make_job(_uniq("src"), old["拓扑名称"])
        result = failover_manager.switch_job_topology(job["id"], "topo_x", mode="wild")
        assert result["ok"] is False
        assert "继承模式" in result["error"]


# ---------------------------------------------------------------------------
# 拓扑切换：单写交接、唯一活动状态键、清单可用时机
# ---------------------------------------------------------------------------

class TestSwitchInvariants:
    """单写交接 + 单一活动状态键 + 清单完成才可用。"""

    def test_single_writer_handoff(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert result["ok"] is True
        # 旧拓扑失去写权限，新拓扑持有写权限，二者不同时可写
        assert old["_write_perm"] is False
        assert new["_write_perm"] is True
        assert not (old["_write_perm"] and new["_write_perm"])
        assert old["status"] == "已停止"
        assert new["status"] == "运行中"

    def test_single_active_state_key_after_switch(self):
        source = _uniq("src")
        _make_throughput(source)
        now = time.time()
        _make_throttle_key(source, 700.0, ts=now - 10)  # 历史遗留的旧活动键
        _make_throttle_key(source, 650.0, ts=now)       # 最新生效键
        assert len(_active_keys(source)) == 2
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert result["ok"] is True
        keys = _active_keys(source)
        assert len(keys) == 1, "切换后同一数据源只能有一个活动状态键"
        assert keys[0]["_new_limit"] == pytest.approx(650.0)  # 继承最新生效值
        assert keys[0]["_job_version"] == 2
        assert keys[0]["_source_version"] == 1

    def test_manifest_available_only_after_completion(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert result["ok"] is True
        assert result["manifest"]["status"] == "可用"
        assert result["manifest"]["完成时间"]

    def test_rollback_on_transaction_failure(self):
        source = _uniq("src")
        _make_throughput(source)
        _make_throttle_key(source, 700.0)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"], version=3)

        result = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], fail_point="after_state_key",
        )
        assert result["ok"] is False
        assert result["manifest"]["status"] == "已回滚"
        # 作业编排数据恢复：拓扑、版本、来源版本
        assert job["计算拓扑"] == old["拓扑名称"]
        assert job["version"] == 3
        # 单写状态恢复：旧拓扑重新可写，新拓扑不持有写权限
        assert old.get("_write_perm") is not False
        assert new.get("_write_perm") is not True
        assert new["status"] == "已部署"
        # 活动状态键恢复为切换前那一个，且不新增键
        keys = _active_keys(source)
        assert len(keys) == 1
        assert keys[0]["_new_limit"] == pytest.approx(700.0)
        # 继承配置不留存
        inherited = read_inherited_limit(job["id"])
        assert inherited["active"] is None

    def test_rollback_at_handoff_point(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        result = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], fail_point="after_handoff",
        )
        assert result["ok"] is False
        assert result["manifest"]["status"] == "已回滚"
        assert job["计算拓扑"] == old["拓扑名称"]
        assert _active_keys(source) == []


# ---------------------------------------------------------------------------
# 拓扑切换：幂等与版本
# ---------------------------------------------------------------------------

class TestSwitchIdempotencyAndVersion:
    """并发切换只产生一组结果；按作业版本读取继承配置。"""

    def test_same_idempotency_key_returns_one_manifest(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        key = _uniq("sw")
        first = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], idempotency_key=key,
        )
        second = failover_manager.switch_job_topology(
            job["id"], new["拓扑名称"], idempotency_key=key,
        )
        assert first["ok"] is True and second["ok"] is True
        assert second["deduplicated"] is True
        assert first["manifest"]["清单编号"] == second["manifest"]["清单编号"]
        manifests = [
            m for m in failover_manager.list_manifests() if m.get("幂等键") == key
        ]
        assert len(manifests) == 1, "同一幂等键只产生一组结果"

    def test_concurrent_switch_produces_one_result_set(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])
        key = _uniq("sw")
        results: list[dict] = []

        def _switch():
            results.append(
                failover_manager.switch_job_topology(
                    job["id"], new["拓扑名称"], idempotency_key=key,
                )
            )

        threads = [threading.Thread(target=_switch) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) == 5
        manifest_ids = {r["manifest"]["清单编号"] for r in results}
        assert len(manifest_ids) == 1, "并发切换只产生一组结果"
        assert sum(1 for r in results if r.get("deduplicated")) == 4

    def test_job_version_bumped_and_source_version_preserved(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"], version=5)

        result = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert result["ok"] is True
        assert job["version"] == 6
        assert job["_orchestration_source_version"] == 5
        assert result["manifest"]["来源版本"] == 5
        assert result["manifest"]["作业版本"] == 6

    def test_inherited_limit_read_by_job_version(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="1000")
        _make_throttle_key(source, 700.0)
        old, new = _switch_pair()
        third = _make_topology(status="已部署")
        job = _make_job(source, old["拓扑名称"], version=1)

        first = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert first["ok"] is True
        second = failover_manager.switch_job_topology(job["id"], third["拓扑名称"])
        assert second["ok"] is True

        v2 = read_inherited_limit(job["id"], version=2)
        v3 = read_inherited_limit(job["id"], version=3)
        assert v2["ok"] and v3["ok"]
        assert v2["record"]["job_version"] == 2
        assert v3["record"]["job_version"] == 3
        # 每个版本各自只保留一条继承配置
        assert v2["record_count"] == 1
        assert v3["record_count"] == 1
        # 当前生效的只有最新版本那一条
        assert v2["is_current"] is False
        assert v3["is_current"] is True
        current = read_inherited_limit(job["id"])
        assert current["active_count"] == 1
        assert current["active"]["job_version"] == 3

    def test_switch_to_same_topology_deduplicates(self):
        source = _uniq("src")
        _make_throughput(source)
        old, new = _switch_pair()
        job = _make_job(source, old["拓扑名称"])

        first = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert first["ok"] is True
        again = failover_manager.switch_job_topology(job["id"], new["拓扑名称"])
        assert again["ok"] is True
        assert again["deduplicated"] is True
        assert again["manifest"]["清单编号"] == first["manifest"]["清单编号"]

    def test_switch_to_missing_target_rejected(self):
        old = _make_topology()
        job = _make_job(_uniq("src"), old["拓扑名称"])
        result = failover_manager.switch_job_topology(job["id"], "topo_ghost")
        assert result["ok"] is False
        assert "不存在" in result["error"]


# ---------------------------------------------------------------------------
# 有效限流值读取
# ---------------------------------------------------------------------------

class TestEffectiveLimit:
    """吞吐管理读取当前生效限流值的优先级。"""

    def test_active_state_key_wins(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="1000")
        _make_throttle_key(source, 600.0)
        eff = effective_limit_for(source)
        assert eff["effective_limit"] == pytest.approx(600.0)
        assert eff["throttled"] is True
        assert eff["origin"] == "active_state_key"

    def test_triggered_config_folds_by_strategy(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="1000", strategy="leaky_bucket", status="已触发")
        eff = effective_limit_for(source)
        assert eff["effective_limit"] == pytest.approx(500.0)
        assert eff["origin"] == "triggered_config"

    def test_default_is_config_max(self):
        source = _uniq("src")
        _make_throughput(source, max_qps="900")
        eff = effective_limit_for(source)
        assert eff["effective_limit"] == pytest.approx(900.0)
        assert eff["throttled"] is False


# ---------------------------------------------------------------------------
# 批量编排：整组结果、逐条结果、幂等、回滚、台账
# ---------------------------------------------------------------------------

class TestBatchOrchestration:
    """批量处理作业编排数据：整组提交、逐条结果、事务边界。"""

    def test_batch_keeps_group_and_per_item_results(self):
        jobs = [_make_job(_uniq("src"), "topo_x", status="待启动") for _ in range(3)]
        result = batch_orchestrator.submit_batch(
            [j["id"] for j in jobs], "启动作业",
        )
        assert result["ok"] is True
        group = result["group"]
        assert group["status"] == "已完成"
        assert group["total"] == 3
        assert group["success_count"] == 3
        # 一次提交保留整组结果：组级版本 + 逐条版本
        assert group["version"] == 1
        seqs = [item["item_version"] for item in group["items"]]
        assert seqs == [1, 2, 3]
        for j in jobs:
            assert j["status"] == "运行中"
            assert j["version"] == 2  # 作业编排数据版本递增

    def test_batch_per_item_success_failure(self):
        ok_job = _make_job(_uniq("src"), "topo_x", status="待启动")
        bad_job = _make_job(_uniq("src"), "topo_x", status="已停止")
        result = batch_orchestrator.submit_batch(
            [ok_job["id"], bad_job["id"]], "启动作业", atomic=False,
        )
        group = result["group"]
        assert group["status"] == "部分失败"
        assert group["success_count"] == 1
        assert group["fail_count"] == 1
        ok_item = next(i for i in group["items"] if i["job_id"] == ok_job["id"])
        bad_item = next(i for i in group["items"] if i["job_id"] == bad_job["id"])
        assert ok_item["ok"] is True
        assert bad_item["ok"] is False
        assert "不允许" in bad_item["message"]

    def test_batch_concurrent_retry_accepts_one_version_set(self):
        jobs = [_make_job(_uniq("src"), "topo_x", status="待启动") for _ in range(2)]
        key = _uniq("batch")
        results: list[dict] = []

        def _submit():
            results.append(
                batch_orchestrator.submit_batch(
                    [j["id"] for j in jobs], "启动作业", idempotency_key=key,
                )
            )

        threads = [threading.Thread(target=_submit) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        group_nos = {r["group"]["组编号"] for r in results}
        assert len(group_nos) == 1, "并发重试只接收一组版本"
        groups = [g for g in batch_orchestrator.list_groups() if g.get("幂等键") == key]
        assert len(groups) == 1
        # 作业只被处理一次：版本只递增一次
        for j in jobs:
            assert j["version"] == 2

    def test_batch_transaction_failure_rolls_back_whole_group(self):
        job_a = _make_job(_uniq("src"), "topo_x", status="待启动", version=4)
        job_b = _make_job(_uniq("src"), "topo_x", status="待启动", version=7)
        ghost_id = 999999  # 不存在的作业编排数据
        result = batch_orchestrator.submit_batch(
            [job_a["id"], ghost_id, job_b["id"]], "启动作业", atomic=True,
        )
        assert result["ok"] is False
        group = result["group"]
        assert group["status"] == "已回滚"
        # 整组回滚：已成功的条目也恢复提交前状态与版本
        assert job_a["status"] == "待启动"
        assert job_a["version"] == 4
        assert job_b["status"] == "待启动"
        assert job_b["version"] == 7
        rolled = [i for i in group["items"] if i.get("rolled_back")]
        assert len(rolled) == 2, "成功条目应标记为已回滚"

    def test_batch_invalid_operation_rejected(self):
        result = batch_orchestrator.submit_batch([1], "删除作业")
        assert result["ok"] is False
        assert "不支持" in result["error"]

    def test_ledger_generated_once_from_another_entry(self):
        jobs = [_make_job(_uniq("src"), "topo_x", status="待启动")]
        submitted = batch_orchestrator.submit_batch([jobs[0]["id"]], "启动作业")
        group = submitted["group"]

        ledgers: list[dict] = []

        def _gen():
            ledgers.append(
                batch_orchestrator.generate_ledger(group["id"], entry="failover_page")
            )

        threads = [threading.Thread(target=_gen) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        ledger_ids = {l["ledger"]["台账编号"] for l in ledgers}
        assert len(ledger_ids) == 1, "整组台账并发重试只记一次"
        ledger = ledgers[0]["ledger"]
        assert ledger["来源入口"] == "failover_page"
        assert ledger["条目数"] == 1
        assert ledger["作业版本快照"][0]["job_id"] == jobs[0]["id"]
        # 组结果回链台账
        assert batch_orchestrator.get_group(group["id"])["ledger_id"] == ledger["id"]

    def test_ledger_for_missing_group_rejected(self):
        result = batch_orchestrator.generate_ledger(999999)
        assert result["ok"] is False
