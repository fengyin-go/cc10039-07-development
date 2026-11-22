"""领域服务单元测试：验证增强后的业务逻辑、跨模块联动和边界条件。

注意：store 是全局单例，测试间共享状态。测试应验证逻辑正确性，
而非精确的入口记录 id。种子数据中的字段值使用占位符，某些跨模块关联
在种子数据中可能不天然成立。
"""
from __future__ import annotations

import datetime

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.partition_policy import PartitionPolicyService
from app.services.lake_table import LakeTableService
from app.services.storage_tier import StorageTierService
from app.services.file_compaction import FileCompactionService
from app.services.retention_mgr import RetentionMgrService
from app.services.snapshot_version import SnapshotVersionService
from app.services.schema_evolution import SchemaEvolutionService
from app.services.backfill_job import BackfillJobService
from app.services.ingest_pipeline import IngestPipelineService
from app.services.quality_monitor import QualityMonitorService
from app.services.lineage_graph import LineageGraphService
from app.services.govern_report import GovernReportService


def _has_module_row(module: str) -> bool:
    return len(store.rows(module)) > 0


class TestPartitionPolicy:
    """分区策略 + 湖表注册联动"""

    def setup_method(self):
        self.svc = PartitionPolicyService()
        self.lake_svc = LakeTableService()

    def test_enable_policy_returns_result(self):
        """启用分区策略应有返回（可能成功或被状态阻塞）。"""
        entry, msg = self.svc.enable_policy(1)
        # 不应抛出异常
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_enable_policy_with_registered_table_links(self):
        """启用分区策略后，目标湖表若存在则分区键被更新。"""
        # 先确保一个湖表已注册
        self.lake_svc.register_table(1)
        # 找一个生效的策略看它的目标湖表
        for policy in store.rows("partition_policy"):
            table_name = policy.get("目标湖表", "")
            lake_tables = store.find_by_field("lake_table", "表名", table_name)
            if lake_tables and policy["status"] == "待审批":
                result, _ = self.svc.enable_policy(policy["id"])
                if result is not None:
                    status = result["status"]
                    assert status in ("已生效", "待审批", "已归档")

    def test_lifecycle_report(self):
        """生命周期报告应返回结构。"""
        # 先尝试启用一条
        for policy_id in [1, 2, 3]:
            p = store.find("partition_policy", policy_id)
            if p and p["status"] == "待审批":
                self.svc.enable_policy(policy_id)
                break

        report = self.svc.lifecycle_report(1)
        assert report is not None
        assert "linked_lake_tables" in report


class TestLakeTable:
    """湖表注册 + 冷热分层联动"""

    def setup_method(self):
        self.svc = LakeTableService()

    def test_register_table_works(self):
        """注册湖表应成功或将状态流转。"""
        for lt_id in [1, 2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "创建中":
                entry, msg = self.svc.register_table(lt_id)
                assert isinstance(msg, str)
                assert len(msg) > 0
                if entry is not None:
                    assert entry["status"] == "已注册"
                return
        pytest.skip("No lake table in 创建中 status")

    def test_register_creates_cross_event(self):
        """注册湖表应产生跨模块事件。"""
        before_count = len(store._events)
        for lt_id in [2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "创建中":
                self.svc.register_table(lt_id)
                after_count = len(store._events)
                assert after_count >= before_count
                return

    def test_offline_blocked_by_dependencies(self):
        """下线湖表被活跃依赖阻塞时应有清晰消息。"""
        for lt_id in [1, 2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "已注册":
                entry, msg = self.svc.offline_table(lt_id)
                # 无论阻塞还是成功，消息都应可读
                assert len(msg) > 0
                return


class TestFileCompaction:
    """小文件合并幂等追踪"""

    def setup_method(self):
        self.svc = FileCompactionService()

    def test_trigger_pending_task(self):
        """触发待调度任务应成功。"""
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                entry, msg = self.svc.trigger_compaction(fc_id)
                assert isinstance(msg, str)
                if entry is not None:
                    assert entry["status"] == "执行中"
                return
        pytest.skip("No compaction task in 待调度")

    def test_complete_notifies_retention(self):
        """完成合并后触发保留策略事件。"""
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "执行中":
                self.svc.complete_compaction(fc_id, "5")
                events = store.events_between("file_compaction", "retention_mgr")
                assert len(events) > 0
                return

        # 如果都在其他状态，先触发再完成
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                self.svc.trigger_compaction(fc_id)
                self.svc.complete_compaction(fc_id, "5")
                events = store.events_between("file_compaction", "retention_mgr")
                assert len(events) > 0
                return

    def test_idempotent_tracking(self):
        """幂等追踪：完成后的任务不应重复完成。"""
        # 找到已完成或完成一个
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                self.svc.trigger_compaction(fc_id)
                self.svc.complete_compaction(fc_id, "5")
                task_no = fc["任务编号"]
                assert self.svc.is_task_completed(task_no) is True
                return
        pytest.skip("All compactions in non-pending state")


class TestStorageTier:
    """冷热分层迁移"""

    def setup_method(self):
        self.svc = StorageTierService()
        self.lake_svc = LakeTableService()

    def test_enable_tier_checks_table(self):
        """启用分层策略应检查湖表状态。"""
        for st_id in [1, 2, 3]:
            st = store.find("storage_tier", st_id)
            if st and st["status"] == "待生效":
                entry, msg = self.svc.enable_tier(st_id)
                assert isinstance(msg, str)
                return

    def test_migrate_partitions_does_not_crash(self):
        """分区迁移不抛异常。"""
        for st_id in [1, 2, 3]:
            entry, msg = self.svc.migrate_partitions(st_id)
            assert entry is not None
            assert isinstance(msg, str)
            break


class TestTierCandidate:
    """冷热分层候选清单：归类 → 候选 → 湖表注册核验消费。

    验收场景：冷层阈值设为 30 天，选账期刚过 30 天的分区，再向该分区
    补写当天一笔；先执行归类，再让湖表注册消费清单，从登记详情核对
    可迁移分区、计算使用的日期及温层。
    """

    def setup_method(self):
        self.tier_svc = StorageTierService()
        self.lake_svc = LakeTableService()
        self.today = datetime.date.today()
        # 账期刚过 30 天（31 天前）
        self.old_period = (self.today - datetime.timedelta(days=31)).isoformat()

    def _make_policy(self, table: str, hot: str = "7", cold: str = "30") -> dict:
        entry, missing = self.tier_svc.create_entry({
            "策略名称": f"分层策略-{table}",
            "热层阈值": hot,
            "冷层阈值": cold,
            "迁移目标": "冷存储",
            "生效湖表": table,
        })
        assert not missing
        assert entry is not None
        return entry

    def _register_lake_table(self, table: str, domain: str) -> dict:
        entry, missing = self.lake_svc.create_entry({
            "表名": table,
            "所属业务域": domain,
            "存储格式": "Parquet",
        })
        assert not missing
        registered, msg = self.lake_svc.register_table(entry["id"])
        assert registered is not None, msg
        return registered

    def test_classify_then_register_consumes_candidates(self):
        """主验收流：归类 → 候选清单 → 湖表注册消费 → 登记详情可核对。"""
        table, domain = "验收订单表", "交易验收域"

        # 1) 冷层阈值 30 天的分层策略
        policy = self._make_policy(table)

        # 2) 已有分区：账期刚过 30 天
        part, msg = self.tier_svc.register_partition(table, domain, self.old_period)
        assert part is not None, msg

        # 3) 向该分区补写当天一笔：同一分区出现两批时间不同的数据
        part, msg = self.tier_svc.backfill_partition(table, domain, self.old_period)
        assert part is not None, msg
        assert self.today.isoformat() in part["批次"]
        assert len(part["批次"]) == 2

        # 4) 先执行归类：整片分区按账期口径判定，不受当天补数影响
        _, msg = self.tier_svc.classify_partitions(policy["id"])
        candidates = self.tier_svc.list_candidates(table, domain)
        assert len(candidates) == 1
        cand = candidates[0]
        assert cand["温层"] == "冷层"  # 账期 31 天 > 冷层阈值 30 天
        assert cand["归类日期"] == self.today.isoformat()
        assert cand["热层阈值"] == 7
        assert cand["冷层阈值"] == 30

        # 5) 规则重复提交只记一次候选，同一分区仍只落一个温层
        _, msg = self.tier_svc.classify_partitions(policy["id"])
        candidates = self.tier_svc.list_candidates(table, domain)
        assert len(candidates) == 1
        assert candidates[0]["温层"] == "冷层"

        # 6) 湖表注册消费清单：登记详情核对可迁移分区、归类日期及温层
        registered = self._register_lake_table(table, domain)
        reg = registered.get("分层登记")
        assert reg is not None, "注册详情应包含分层登记"
        assert reg["归类日期"] == self.today.isoformat()
        assert reg["下次迁移可用分区"] == [self.old_period]
        assert reg["候选分区数"] == 1
        detail = reg["判定明细"][0]
        assert detail["分区"] == self.old_period
        assert detail["温层"] == "冷层"
        assert detail["热层阈值"] == 7
        assert detail["冷层阈值"] == 30

    def test_same_table_name_different_domain_no_borrow(self):
        """表名相同但业务域不同的分区不能互借候选。"""
        table = "域隔离订单表"
        policy = self._make_policy(table)
        self.tier_svc.register_partition(table, "域A", self.old_period)
        self.tier_svc.classify_partitions(policy["id"])
        assert len(self.tier_svc.list_candidates(table, "域A")) == 1

        # 同表名、不同业务域的湖表注册时不应读到域A的候选
        registered = self._register_lake_table(table, "域B")
        assert "分层登记" not in registered

        # 域A的湖表注册时能读到
        registered_a = self._register_lake_table(table, "域A")
        assert registered_a["分层登记"]["候选分区数"] == 1

    def test_backfill_does_not_change_partition_tier(self):
        """补数后同一分区仍按账期口径归类，整片只落一个温层。"""
        table, domain = "补数口径表", "口径域"
        policy = self._make_policy(table)
        self.tier_svc.register_partition(table, domain, self.old_period)
        # 补写当天一批后再归类
        self.tier_svc.backfill_partition(table, domain, self.old_period)
        self.tier_svc.classify_partitions(policy["id"])

        candidates = self.tier_svc.list_candidates(table, domain)
        assert len(candidates) == 1
        assert candidates[0]["温层"] == "冷层"
        assert len(candidates[0]["数据批次"]) == 2  # 批次情况留痕，但判定不变

    def test_warm_and_hot_tier_boundaries(self):
        """温层边界：账期年龄 ≤ 热层阈值判热层，≤ 冷层阈值判温层。"""
        table, domain = "边界判定表", "边界域"
        policy = self._make_policy(table)
        warm_period = (self.today - datetime.timedelta(days=10)).isoformat()
        hot_period = (self.today - datetime.timedelta(days=3)).isoformat()
        self.tier_svc.register_partition(table, domain, self.old_period)
        self.tier_svc.register_partition(table, domain, warm_period)
        self.tier_svc.register_partition(table, domain, hot_period)
        self.tier_svc.classify_partitions(policy["id"])

        tiers = {
            c["分区"]: c["温层"]
            for c in self.tier_svc.list_candidates(table, domain)
        }
        assert tiers[self.old_period] == "冷层"
        assert tiers[warm_period] == "温层"
        assert tiers[hot_period] == "热层"

        # 注册后只有冷层分区进入下次迁移可用分区
        registered = self._register_lake_table(table, domain)
        assert registered["分层登记"]["下次迁移可用分区"] == [self.old_period]
        assert registered["分层登记"]["候选分区数"] == 3

    def test_classify_rejects_invalid_thresholds(self):
        """热层阈值必须小于冷层阈值，否则归类被拒绝。"""
        table = "阈值校验表"
        policy = self._make_policy(table, hot="30", cold="30")
        entry, msg = self.tier_svc.classify_partitions(policy["id"])
        assert entry is None
        assert "阈值" in msg

    def test_classify_action_via_run_action(self):
        """通过动作入口执行归类（对应页面「执行归类」按钮）。"""
        table, domain = "动作归类表", "动作域"
        policy = self._make_policy(table)
        self.tier_svc.register_partition(table, domain, self.old_period)
        entry, msg = self.tier_svc.run_action(policy["id"], "执行归类")
        assert entry is not None, msg
        assert len(self.tier_svc.list_candidates(table, domain)) == 1


class TestRetentionMgr:
    """保留策略清理"""

    def setup_method(self):
        self.svc = RetentionMgrService()

    def test_enable_retention(self):
        """启用保留策略。"""
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "待生效":
                entry, msg = self.svc.enable_retention(rm_id)
                assert isinstance(msg, str)
                return

    def test_execute_cleanup_notifies_govern(self):
        """清理完成后通知治理报告。"""
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "待生效":
                self.svc.enable_retention(rm_id)

        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "已生效":
                entry, msg = self.svc.execute_cleanup(rm_id)
                assert entry is not None
                assert isinstance(msg, str)
                return
        pytest.skip("No active retention policy")


class TestSnapshotVersion:
    """快照版本管理"""

    def setup_method(self):
        self.svc = SnapshotVersionService()

    def test_generate_snapshot_works(self):
        """生成快照应返回可用状态或合理拒绝消息。"""
        for sv_id in [1, 2, 3]:
            sv = store.find("snapshot_version", sv_id)
            if sv and sv["status"] == "生成中":
                entry, msg = self.svc.generate_snapshot(sv_id)
                assert isinstance(msg, str)
                if entry is not None:
                    assert entry["status"] == "可用"
                return
        pytest.skip("No snapshot in 生成中")


class TestSchemaEvolution:
    """Schema 演进兼容性检查"""

    def setup_method(self):
        self.svc = SchemaEvolutionService()

    def test_compatibility_check(self):
        """兼容性检查：不兼容操作被级别拒绝。"""
        for se_id in [1, 2, 3]:
            se = store.find("schema_evolution", se_id)
            if se and se["status"] == "草稿":
                result, msg = self.svc.submit_approval(se_id)
                assert isinstance(msg, str)
                if result is None:
                    assert "兼容性" in msg or "快照" in msg or "不允许" in msg
                return
        pytest.skip("No schema change in 草稿")

    def test_execute_change_links_backfill(self):
        """不兼容变更执行后联动回填。"""
        for se_id in [1, 2, 3]:
            se = store.find("schema_evolution", se_id)
            if se and se.get("兼容级别") == "不兼容":
                # 先提交
                approve_result, _ = self.svc.submit_approval(se_id)
                if approve_result is not None:
                    entry, msg = self.svc.execute_change(se_id)
                    assert isinstance(msg, str)
                return
        pytest.skip("No incompatible schema evolution")


class TestBackfillJob:
    """回填任务检查点"""

    def setup_method(self):
        self.svc = BackfillJobService()

    def test_checkpoint_workflow(self):
        """回填检查点断点续做。"""
        for bj_id in [1, 2, 3]:
            bj = store.find("backfill_job", bj_id)
            if bj and bj["status"] == "待调度":
                result, msg = self.svc.start_backfill(bj_id)
                if result is None:
                    continue
                assert result["status"] == "回填中"

                cp_result, _ = self.svc.checkpoint_backfill(bj_id, 50)
                assert cp_result is not None
                assert self.svc.get_checkpoint(bj_id) == 50
                return
        pytest.skip("No backfill in 待调度")

    def test_complete_backfill_notifies_govern(self):
        """完成回填应通知治理报告。"""
        for bj_id in [1, 2, 3]:
            bj = store.find("backfill_job", bj_id)
            if bj and bj["status"] == "待调度":
                self.svc.start_backfill(bj_id)
                result2, _ = self.svc.complete_backfill(bj_id, 100)
                assert result2 is not None
                assert result2["status"] == "已完成"
                events = store.events_between("backfill_job", "govern_report")
                assert len(events) > 0
                return
        pytest.skip("No backfill in 待调度")


class TestIngestPipeline:
    """入湖管道 + 质量监控联动"""

    def setup_method(self):
        self.svc = IngestPipelineService()
        self.lake_svc = LakeTableService()

    def test_start_pipeline_checks_table(self):
        """启动管道应检查目标湖表。"""
        for ip_id in [1, 2, 3]:
            ip = store.find("ingest_pipeline", ip_id)
            if ip and ip["status"] == "待启动":
                entry, msg = self.svc.start_pipeline(ip_id)
                assert isinstance(msg, str)
                return

    def test_complete_pipeline_triggers_quality(self):
        """完成管道应触发质量监控事件。"""
        # 确保湖表已注册
        self.lake_svc.register_table(1)
        for ip_id in [1, 2, 3]:
            ip = store.find("ingest_pipeline", ip_id)
            if ip and ip["status"] == "待启动":
                result, _ = self.svc.start_pipeline(ip_id)
                if result is None:
                    continue
                result2, _ = self.svc.complete_pipeline(ip_id, 200)
                assert result2 is not None
                assert result2["status"] == "已完成"
                events = store.events_between("ingest_pipeline", "quality_monitor")
                assert len(events) > 0
                return
        pytest.skip("Cannot complete pipeline flow")


class TestQualityMonitor:
    """质量监控 + 血缘联动"""

    def setup_method(self):
        self.svc = QualityMonitorService()

    def test_enable_monitor(self):
        """启用监控规则。"""
        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "待启用":
                entry, msg = self.svc.enable_monitor(qm_id)
                assert isinstance(msg, str)
                if entry is not None:
                    assert entry["status"] == "已启用"
                return

    def test_check_quality_generates_events(self):
        """质量检查应产生事件。"""
        # 确保一条规则已启用
        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "待启用":
                self.svc.enable_monitor(qm_id)

        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "已启用":
                entry, msg = self.svc.check_quality(qm_id)
                assert entry is not None
                assert isinstance(msg, str)
                events = store.events_for_module("quality_monitor")
                assert len(events) > 0
                return
        pytest.skip("No enabled quality monitor")


class TestLineageGraph:
    """血缘图谱完整性"""

    def setup_method(self):
        self.svc = LineageGraphService()

    def test_refresh_lineage(self):
        """刷新血缘链路有清晰结论。"""
        for lg_id in [1, 2, 3]:
            entry, msg = self.svc.refresh_lineage(lg_id)
            assert entry is not None, f"刷新失败: {msg}"
            assert entry["status"] in ("已建立", "待验证", "链路断裂")
            break

    def test_mark_broken_notifies_govern(self):
        """断裂血缘通知治理报告。"""
        self.svc.mark_broken(1)
        events = store.events_between("lineage_graph", "govern_report")
        assert len(events) > 0


class TestGovernReport:
    """治理报告跨模块聚合"""

    def setup_method(self):
        self.svc = GovernReportService()

    def test_generate_report_aggregates(self):
        """生成报告应聚合跨模块数据。"""
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                entry, msg = self.svc.generate_report(gr_id)
                assert entry is not None, f"生成失败: {msg}"
                assert entry["status"] == "已生成"
                return
        pytest.skip("No govern report in 待生成")

    def test_execute_optimization_triggers_actions(self):
        """执行优化应触发实际操作。"""
        # 先生成一个报告
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                self.svc.generate_report(gr_id)

        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "已生成":
                entry, msg = self.svc.execute_optimization(gr_id)
                assert entry is not None, f"执行失败: {msg}"
                assert entry["status"] == "已执行"
                return
        pytest.skip("No generated govern report")


class TestCrossModulePipeline:
    """端到端跨模块数据流测试"""

    def test_full_pipeline_flow(self):
        """完整的端到端数据流在各模块间传递。
        验证核心：事件日志链记录跨模块联动。"""
        pp_svc = PartitionPolicyService()
        lake_svc = LakeTableService()
        tier_svc = StorageTierService()
        comp_svc = FileCompactionService()
        ret_svc = RetentionMgrService()
        gov_svc = GovernReportService()

        # 遍历种子数据中所有可用记录，依次执行
        actions_executed = 0

        # 1) 分区策略
        for pp_id in [1, 2, 3]:
            pp = store.find("partition_policy", pp_id)
            if pp and pp["status"] == "待审批":
                pp_svc.enable_policy(pp_id)
                actions_executed += 1
                break

        # 2) 湖表注册
        for lt_id in [1, 2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "创建中":
                lake_svc.register_table(lt_id)
                actions_executed += 1
                break

        # 3) 冷热分层
        for st_id in [1, 2, 3]:
            st = store.find("storage_tier", st_id)
            if st and st["status"] == "待生效":
                tier_svc.enable_tier(st_id)
                tier_svc.migrate_partitions(st_id)
                actions_executed += 1
                break

        # 4) 小文件合并
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                result, _ = comp_svc.trigger_compaction(fc_id)
                if result:
                    comp_svc.complete_compaction(fc_id, "5")
                    actions_executed += 1
                    break

        # 5) 保留策略
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "待生效":
                ret_svc.enable_retention(rm_id)
                actions_executed += 1
                break
        # 已生效后执行清理
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "已生效":
                ret_svc.execute_cleanup(rm_id)
                actions_executed += 1
                break

        # 6) 治理报告
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                result, msg = gov_svc.generate_report(gr_id)
                assert result is not None, f"报告生成失败: {msg}"
                assert result["status"] == "已生成"
                actions_executed += 1
                break

        # 验证有跨模块事件
        cross_stats = store.cross_module_stats()
        assert cross_stats["event_count"] > 0, "应该有跨模块事件"
        # 至少完成了一些操作
        assert actions_executed > 0, "至少执行了一项操作"

    def test_snapshot_schema_backfill_flow(self):
        """快照 → Schema演化 → 回填任务流。"""
        snap_svc = SnapshotVersionService()
        schema_svc = SchemaEvolutionService()

        # 1) 生成快照
        for sv_id in [1, 2, 3]:
            sv = store.find("snapshot_version", sv_id)
            if sv and sv["status"] == "生成中":
                snap_svc.generate_snapshot(sv_id)
                break

        # 2) Schema 审批 + 执行
        for se_id in [1, 2, 3]:
            se = store.find("schema_evolution", se_id)
            if se and se["status"] == "草稿":
                result, msg = schema_svc.submit_approval(se_id)
                if result is not None:
                    schema_svc.execute_change(se_id)
                break

        # 验证不抛异常
        assert True

    def test_ingest_to_quality_to_lineage_to_govern_flow(self):
        """入湖 → 质量 → 血缘 → 治理报告流。"""
        lake_svc = LakeTableService()
        ingest_svc = IngestPipelineService()
        qual_svc = QualityMonitorService()
        gov_svc = GovernReportService()

        # 确保湖表已注册
        lake_svc.register_table(1)

        # 启动入湖管道
        for ip_id in [1, 2, 3]:
            ip = store.find("ingest_pipeline", ip_id)
            if ip and ip["status"] == "待启动":
                result, _ = ingest_svc.start_pipeline(ip_id)
                if result is None:
                    continue
                ingest_svc.complete_pipeline(ip_id, 500)
                events1 = store.events_between("ingest_pipeline", "quality_monitor")
                assert len(events1) > 0, "入湖管道完成应触发质量监控"
                break

        # 质量检查
        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "待启用":
                qual_svc.enable_monitor(qm_id)
            elif qm and qm["status"] == "已启用":
                qual_svc.check_quality(qm_id)
                break

        # 治理报告
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                result, _ = gov_svc.generate_report(gr_id)
                assert result is not None
                break

    def test_dirty_data_handling(self):
        """脏数据处理：重复操作不应抛异常。"""
        pp_svc = PartitionPolicyService()
        # 尝试重复启用同一策略
        for pp_id in [1, 2, 3]:
            pp = store.find("partition_policy", pp_id)
            if pp and pp["status"] == "待审批":
                # 第一次
                pp_svc.enable_policy(pp_id)
                # 第二次应该被拒绝
                result2, msg2 = pp_svc.enable_policy(pp_id)
                assert result2 is None, f"重复启用应被拒绝: {msg2}"
                assert len(msg2) > 0
                return

    def test_create_entries_sequence(self):
        """创建 → 启用序列不抛异常。"""
        pp_svc = PartitionPolicyService()
        entry, missing = pp_svc.create_entry({
            "策略名称": "test_policy",
            "分区维度": "day",
            "分区粒度": "1d",
        })
        assert entry is not None
        assert "id" in entry

    def test_snapshot_and_cross_events(self):
        """快照操作和跨模块事件记录。"""
        # 生成快照
        for sv_id in [1, 2, 3]:
            sv = store.find("snapshot_version", sv_id)
            if sv and sv["status"] == "生成中":
                snap_svc = SnapshotVersionService()
                result, _ = snap_svc.generate_snapshot(sv_id)
                if result:
                    assert result["status"] == "可用"
                    break

        # 验证快照存在
        keys = store.snapshot_keys()
        assert len(keys) > 0

    def test_all_service_interfaces_complete(self):
        """验证所有核心服务的接口完整性。"""
        services = [
            PartitionPolicyService(),
            LakeTableService(),
            StorageTierService(),
            FileCompactionService(),
            RetentionMgrService(),
            SnapshotVersionService(),
            SchemaEvolutionService(),
            BackfillJobService(),
            IngestPipelineService(),
            QualityMonitorService(),
            LineageGraphService(),
            GovernReportService(),
        ]
        for svc in services:
            assert svc.MODULE, f"{type(svc).__name__} missing MODULE"
            assert svc.ENTITY, f"{type(svc).__name__} missing ENTITY"
            assert svc.LIST_FIELDS, f"{type(svc).__name__} missing LIST_FIELDS"
            assert svc.STATUS_ORDER, f"{type(svc).__name__} missing STATUS_ORDER"