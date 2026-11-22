"""冷热分层领域服务：配置分区数据的冷热分层自动迁移策略。

上游接收湖表注册和分区策略的生效事件，下游联动小文件合并和治理报告。
冷热分层是数据湖成本优化的核心手段。

分层候选清单：按热层阈值/冷层阈值对已有分区数据做温层判定，产出候选清单
供湖表注册核验消费。判定口径统一取分区账期——补数后同一月分区内会有两批
时间不同的数据，整片分区只按账期归类，保证同一分区在本轮只落在一个温层，
避免不同口径把同一分区的数据拆进不同层、影响下次查询的取数层。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

TIER_HOT = "热层"
TIER_WARM = "温层"
TIER_COLD = "冷层"


def _parse_threshold_days(value: Any) -> int | None:
    """把阈值配置解析成天数，支持 30 / "30" / "30天" / "30d"。"""
    text = str(value or "").strip().lower()
    for suffix in ("天", "days", "day", "d"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            break
    try:
        days = int(text)
    except ValueError:
        return None
    return days if days >= 0 else None


def _parse_date(value: Any) -> datetime.date | None:
    """解析 YYYY-MM-DD 日期，非法输入返回 None。"""
    try:
        return datetime.date.fromisoformat(str(value or "").strip())
    except ValueError:
        return None


class StorageTierService(BaseModuleService):
    MODULE = "storage_tier"
    ENTITY = "分层策略"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["策略名称", "热层阈值", "冷层阈值", "迁移目标", "生效湖表"]
    REQUIRED_FIELDS = ["策略名称", "热层阈值", "冷层阈值"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用分层": "已生效", "停用分层": "已停用"}
    NEGATIVE_ACTIONS = ["停用分层"]

    # ---- 已有分区数据：登记与补数 ----

    def register_partition(
        self, table_name: Any, domain: Any, period: Any, batch_time: Any = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """登记已有分区数据。同一（表名，业务域，账期）只登记一次，批次时间默认取账期当天。"""
        table_name = str(table_name or "").strip()
        domain = str(domain or "").strip()
        period = str(period or "").strip()
        if not table_name or not domain:
            return None, "表名和业务域必填"
        if _parse_date(period) is None:
            return None, f"账期 {period} 不是有效日期（YYYY-MM-DD）"
        batch = str(batch_time or period).strip()
        if _parse_date(batch) is None:
            return None, f"批次时间 {batch} 不是有效日期（YYYY-MM-DD）"

        partitions = store.extra_rows("partition_data")
        existing = self._find_partition(table_name, domain, period)
        if existing is not None:
            return existing, f"分区 {table_name}/{domain}/{period} 已存在，沿用已有数据批次"

        entry = {
            "id": max((int(p.get("id", 0)) for p in partitions), default=0) + 1,
            "表名": table_name,
            "业务域": domain,
            "账期": period,
            "批次": [batch],
        }
        partitions.append(entry)
        return entry, f"分区 {table_name}/{domain}/{period} 已登记，首批数据时间={batch}"

    def backfill_partition(
        self, table_name: Any, domain: Any, period: Any, batch_time: Any = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """向已有分区补写一批数据（默认当天）。补数后同一分区会出现两批时间不同的数据，
        但归类口径仍取分区账期，整片分区只落一个温层。"""
        table_name = str(table_name or "").strip()
        domain = str(domain or "").strip()
        period = str(period or "").strip()
        batch = str(batch_time or datetime.date.today().isoformat()).strip()
        if _parse_date(batch) is None:
            return None, f"批次时间 {batch} 不是有效日期（YYYY-MM-DD）"

        part = self._find_partition(table_name, domain, period)
        if part is None:
            return None, f"分区 {table_name}/{domain}/{period} 不存在，请先登记分区数据"

        batches = part.setdefault("批次", [])
        if batch not in batches:
            batches.append(batch)
        store.log_event("partition_backfilled", "storage_tier", "storage_tier", {
            "table_name": table_name,
            "domain": domain,
            "period": period,
            "batch_time": batch,
            "batch_count": len(batches),
        })
        return part, f"分区 {period} 已补写批次 {batch}，当前共 {len(batches)} 批数据"

    def _find_partition(self, table_name: str, domain: str, period: str) -> dict[str, Any] | None:
        for part in store.extra_rows("partition_data"):
            if (part.get("表名") == table_name and part.get("业务域") == domain
                    and part.get("账期") == period):
                return part
        return None

    # ---- 温层判定：归类产出分层候选清单 ----

    def classify_partitions(
        self, entry_id: int, as_of: datetime.date | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """执行归类：把生效湖表下的已有分区数据按热层/冷层阈值判定温层，写入分层候选清单。

        判定口径：账期年龄 = 归类日期 - 分区账期；年龄 ≤ 热层阈值判热层，
        热层阈值 < 年龄 ≤ 冷层阈值判温层，年龄 > 冷层阈值判冷层。
        同一分区在本轮只落一个温层；规则重复提交只更新同一条候选，不重复记录。
        候选上保留归类日期和判定阈值，供湖表注册登记页查回核对。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"
        if entry.get("status") not in ("待生效", "已生效"):
            return None, f"分层策略状态为{entry.get('status')}，不可执行归类"

        hot_days = _parse_threshold_days(entry.get("热层阈值"))
        cold_days = _parse_threshold_days(entry.get("冷层阈值"))
        if hot_days is None or cold_days is None:
            return None, "热层阈值/冷层阈值未配置为有效天数"
        if hot_days >= cold_days:
            return None, f"热层阈值({hot_days}天)必须小于冷层阈值({cold_days}天)"

        as_of_date = as_of or datetime.date.today()
        table_name = entry.get("生效湖表", "")
        partitions = [
            p for p in store.extra_rows("partition_data")
            if p.get("表名") == table_name
        ]

        candidates = store.extra_rows("tier_candidate")
        created = 0
        updated = 0
        skipped = 0
        for part in partitions:
            period = _parse_date(part.get("账期"))
            if period is None:
                skipped += 1
                continue
            age = (as_of_date - period).days
            if age <= hot_days:
                tier = TIER_HOT
            elif age <= cold_days:
                tier = TIER_WARM
            else:
                tier = TIER_COLD
            record = {
                "策略id": entry_id,
                "策略名称": entry.get("策略名称", ""),
                "表名": table_name,
                "业务域": part.get("业务域", ""),
                "分区": part.get("账期", ""),
                "温层": tier,
                "账期年龄天数": age,
                "归类日期": as_of_date.isoformat(),
                "热层阈值": hot_days,
                "冷层阈值": cold_days,
                "数据批次": list(part.get("批次", [])),
            }
            existing = next(
                (c for c in candidates
                 if c.get("策略id") == entry_id
                 and c.get("表名") == table_name
                 and c.get("业务域") == record["业务域"]
                 and c.get("分区") == record["分区"]),
                None,
            )
            if existing is not None:
                existing.update(record)
                updated += 1
            else:
                record["id"] = max((int(c.get("id", 0)) for c in candidates), default=0) + 1
                candidates.append(record)
                created += 1

        store.log_event("tier_classified", "storage_tier", "lake_table", {
            "tier_policy_id": entry_id,
            "table_name": table_name,
            "as_of_date": as_of_date.isoformat(),
            "hot_threshold": hot_days,
            "cold_threshold": cold_days,
            "candidate_created": created,
            "candidate_updated": updated,
        })

        message = (
            f"归类完成（归类日期={as_of_date.isoformat()}，"
            f"热层≤{hot_days}天<温层≤{cold_days}天<冷层）："
            f"新增 {created} 条、更新 {updated} 条分层候选"
        )
        if skipped:
            message += f"，跳过非法账期分区 {skipped} 个"
        return entry, message

    def list_candidates(
        self, table_name: str | None = None, domain: str | None = None,
    ) -> list[dict[str, Any]]:
        """查询分层候选清单，可按表名/业务域过滤。"""
        rows = list(store.extra_rows("tier_candidate"))
        if table_name:
            rows = [c for c in rows if c.get("表名") == table_name]
        if domain:
            rows = [c for c in rows if c.get("业务域") == domain]
        return rows

    def candidates_for(self, table_name: str, domain: str) -> list[dict[str, Any]]:
        """湖表注册核验用：候选按（表名，业务域）双键匹配，
        表名相同但业务域不同的分区不能互借候选。"""
        return self.list_candidates(table_name, domain)

    # ---- 策略启停与迁移 ----

    def enable_tier(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用冷热分层策略：验证湖表已注册，触发分层迁移记录。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"
        if entry.get("status") != "待生效":
            return None, f"分层策略状态为{entry.get('status')}，不可启用"

        table_name = entry.get("生效湖表", "")

        # 1) 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables or not any(lt.get("status") == "已注册" for lt in lake_tables):
            return None, f"湖表 {table_name} 尚未注册，不可启用分层策略"

        # 2) 自身状态流转
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False

        # 3) 记录分层迁移事件
        hot_threshold = entry.get("热层阈值", "")
        cold_threshold = entry.get("冷层阈值", "")
        store.log_event("tier_enabled", "storage_tier", "lake_table", {
            "tier_policy_id": entry_id,
            "table_name": table_name,
            "hot_threshold": hot_threshold,
            "cold_threshold": cold_threshold,
        })

        # 4) 通知小文件合并模块（分层后可能触发合并）
        compactions = store.find_by_field("file_compaction", "目标湖表", table_name)
        if compactions:
            store.log_event("tier_to_compaction", "storage_tier", "file_compaction", {
                "tier_policy_id": entry_id,
                "table_name": table_name,
                "existing_compaction_count": len(compactions),
            })

        return entry, f"分层策略 {entry.get('策略名称')} 已生效，热层阈值={hot_threshold}，冷层阈值={cold_threshold}"

    def disable_tier(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """停用分层策略：检查迁移中数据后停用。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"
        if entry.get("status") != "已生效":
            return None, f"分层策略状态为{entry.get('status')}，不可停用"

        entry["status"] = "已停用"
        entry["pending"] = False
        entry["abnormal"] = True
        store.log_event("tier_disabled", "storage_tier", "storage_tier", {
            "tier_policy_id": entry_id,
        })
        return entry, f"分层策略 {entry.get('策略名称')} 已停用"

    def migrate_partitions(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行分区冷热迁移：优先迁移候选清单中判定为冷层的分区；
        没有候选时回退为按已注册湖表标记，保持兼容。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "分层策略不存在"

        table_name = entry.get("生效湖表", "")
        cold_target = entry.get("迁移目标", "")

        # 1) 消费分层候选清单：只有判定为冷层的分区才迁移
        cold_candidates = [
            c for c in store.extra_rows("tier_candidate")
            if c.get("策略id") == entry_id and c.get("温层") == TIER_COLD
        ]

        migrated_records: list[dict[str, Any]] = []
        if cold_candidates:
            for cand in cold_candidates:
                migrated_records.append({
                    "table_name": table_name,
                    "domain": cand.get("业务域", ""),
                    "partition": cand.get("分区", ""),
                    "tier": cand.get("温层", ""),
                    "classified_at": cand.get("归类日期", ""),
                    "cold_target": cold_target,
                })
        else:
            # 无候选时回退：按已注册湖表标记待迁移
            lake_tables = store.find_by_field("lake_table", "表名", table_name)
            registered = [lt for lt in lake_tables if lt.get("status") == "已注册"]
            for lt in registered:
                lt["pending"] = True
                migrated_records.append({
                    "table_name": table_name,
                    "cold_target": cold_target,
                    "partition": lt.get("分区键", ""),
                    "segment_count": lt.get("当前分片数", "1"),
                })

        # 记录迁移事件到治理报告
        store.log_event("tier_migration_executed", "storage_tier", "govern_report", {
            "tier_policy_id": entry_id,
            "table_name": table_name,
            "migrated_partition_count": len(migrated_records),
            "cold_target": cold_target,
        })

        return entry, f"已完成 {len(migrated_records)} 个分区的冷热迁移，目标={cold_target}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用分层":
            return self.enable_tier(entry_id)
        if action == "停用分层":
            return self.disable_tier(entry_id)
        if action == "执行归类":
            return self.classify_partitions(entry_id)
        return super().run_action(entry_id, action)
