"""跨模块编排端点：暴露跨模块数据流链路、事件日志和快照信息。

提供 /api/cross/events, /api/cross/snapshots, /api/cross/pipeline-status 等端点，
让前端和治理报告可以消费跨模块链路数据。
/api/cross/tier/* 一组端点承载冷热分层的分区数据登记、补数、归类和候选清单查询。
"""
from __future__ import annotations

import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import service_for, SERVICES
from app.store import store
from app.services.partition_policy import PartitionPolicyService
from app.services.lake_table import LakeTableService
from app.services.storage_tier import StorageTierService
from app.services.file_compaction import FileCompactionService
from app.services.retention_mgr import RetentionMgrService
from app.services.backfill_job import BackfillJobService
from app.services.govern_report import GovernReportService

router = APIRouter(prefix="/api/cross", tags=["跨模块编排"])


@router.get("/events")
def list_cross_events(
    source: str | None = Query(default=None),
    target: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """查询跨模块事件日志，支持按 source/target 筛选。"""
    if source and target:
        events = store.events_between(source, target)
    elif source or target:
        events = store.events_for_module(source or target or "")
    else:
        events = list(store._events)
    return {
        "total": len(events),
        "items": events[-limit:],
    }


@router.get("/snapshots")
def list_snapshots() -> dict[str, Any]:
    """查询所有模块快照记录。"""
    return {
        "total": len(store._snapshots),
        "keys": store.snapshot_keys(),
    }


@router.get("/overview")
def cross_module_overview() -> dict[str, Any]:
    """跨模块统计概览。"""
    return store.cross_module_stats()


# ---- 冷热分层：分区数据、补数、归类与候选清单 ----

@router.get("/tier/partitions")
def list_partition_data(
    table_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
) -> dict[str, Any]:
    """查询已有分区数据及其数据批次。"""
    rows = list(store.extra_rows("partition_data"))
    if table_name:
        rows = [r for r in rows if r.get("表名") == table_name]
    if domain:
        rows = [r for r in rows if r.get("业务域") == domain]
    return {"total": len(rows), "items": rows}


@router.post("/tier/partitions")
def register_partition(payload: dict[str, Any]) -> dict[str, Any]:
    """登记已有分区数据：表名、业务域、账期必填，批次时间默认取账期当天。"""
    svc = StorageTierService()
    entry, message = svc.register_partition(
        payload.get("表名"), payload.get("业务域"),
        payload.get("账期"), payload.get("批次时间"),
    )
    return {"ok": entry is not None, "message": message, "entry": entry}


@router.post("/tier/partitions/backfill")
def backfill_partition(payload: dict[str, Any]) -> dict[str, Any]:
    """向已有分区补写一批数据（默认当天），模拟补数场景。"""
    svc = StorageTierService()
    entry, message = svc.backfill_partition(
        payload.get("表名"), payload.get("业务域"),
        payload.get("账期"), payload.get("批次时间"),
    )
    return {"ok": entry is not None, "message": message, "entry": entry}


@router.post("/tier/classify")
def classify_tier(payload: dict[str, Any]) -> dict[str, Any]:
    """执行归类：按热层/冷层阈值对分区做温层判定，产出分层候选清单。"""
    svc = StorageTierService()
    try:
        policy_id = int(payload.get("policy_id") or 0)
    except (TypeError, ValueError):
        return {"ok": False, "message": "policy_id 必须是整数"}
    as_of = None
    raw_as_of = str(payload.get("as_of_date") or "").strip()
    if raw_as_of:
        try:
            as_of = datetime.date.fromisoformat(raw_as_of)
        except ValueError:
            return {"ok": False, "message": f"归类日期 {raw_as_of} 不是有效日期（YYYY-MM-DD）"}
    entry, message = svc.classify_partitions(policy_id, as_of)
    return {"ok": entry is not None, "message": message}


@router.get("/tier/candidates")
def list_tier_candidates(
    table_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
) -> dict[str, Any]:
    """查询分层候选清单：温层判定、归类日期和判定时的阈值。"""
    svc = StorageTierService()
    rows = svc.list_candidates(table_name, domain)
    return {"total": len(rows), "items": rows}


@router.get("/pipeline-status")
def pipeline_status() -> dict[str, Any]:
    """汇总数据湖治理全链路状态。
    从分区策略 → 湖表注册 → 冷热分层 → 小文件合并 → 保留策略 → 治理报告。
    """
    # 分区策略概览
    policies = store.rows("partition_policy")
    active_policies = [p for p in policies if p.get("status") == "已生效"]

    # 湖表注册概览
    lake_tables = store.rows("lake_table")
    registered_tables = [lt for lt in lake_tables if lt.get("status") == "已注册"]

    # 冷热分层概览
    tiers = store.rows("storage_tier")
    active_tiers = [t for t in tiers if t.get("status") == "已生效"]

    # 小文件合并概览
    compactions = store.rows("file_compaction")
    pending_compactions = [c for c in compactions if c.get("status") == "待调度"]
    running_compactions = [c for c in compactions if c.get("status") == "执行中"]
    completed_compactions = [c for c in compactions if c.get("status") == "已完成"]

    # 保留策略概览
    retentions = store.rows("retention_mgr")
    active_retentions = [r for r in retentions if r.get("status") == "已生效"]

    # 回填任务概览
    backfills = store.rows("backfill_job")
    active_backfills = [b for b in backfills if b.get("status") in ("待调度", "回填中")]

    # 血缘链路概览
    lineages = store.rows("lineage_graph")
    broken_lineages = [l for l in lineages if l.get("status") == "链路断裂"]

    # 治理报告概览
    reports = store.rows("govern_report")

    # 跨模块事件
    cross_stats = store.cross_module_stats()

    return {
        "pipeline": {
            "partition_policies": {"total": len(policies), "active": len(active_policies)},
            "lake_tables": {"total": len(lake_tables), "registered": len(registered_tables)},
            "storage_tiers": {"total": len(tiers), "active": len(active_tiers)},
            "file_compactions": {
                "total": len(compactions),
                "pending": len(pending_compactions),
                "running": len(running_compactions),
                "completed": len(completed_compactions),
            },
            "retention_mgrs": {"total": len(retentions), "active": len(active_retentions)},
            "backfill_jobs": {"total": len(backfills), "active": len(active_backfills)},
            "lineage_graphs": {"total": len(lineages), "broken": len(broken_lineages)},
            "govern_reports": {"total": len(reports)},
        },
        "cross_module": cross_stats,
        "health": {
            "has_broken_lineages": len(broken_lineages) > 0,
            "has_active_backfills": len(active_backfills) > 0,
            "all_lake_tables_registered": len(registered_tables) == len(lake_tables),
        },
    }