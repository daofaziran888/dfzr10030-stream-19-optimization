"""流式质检阈值审计证据：样本、阈值快照、执行批次与复核结果。

这个模块补齐现有质量扫描缺的两块证据：

* 扫描命中的 **质量样本**（含采集时间、样本来路、执行批次）；
* 样本比较时实际使用的 **阈值快照**（版本号 + 当时上下限），而不是事后被改过的当前阈值。

导出包一次生成、可多次读取：已经归档的行不再重新计算，阈值按原版本读取；
同一执行批次里迟到的样本以补录行追加，缺测样本标记为「缺测」且不计入合格统计。
"""
from __future__ import annotations

import copy
import csv
import io
from datetime import datetime, timezone
from typing import Any

from app.store import store

RULE_TABLE = "_quality_rule"
THRESHOLD_TABLE = "_quality_threshold"
SAMPLE_TABLE = "_quality_sample"
EXECUTION_TABLE = "_quality_execution"

# 历史报告（/api/data_quality/export）的列顺序。审计包把这 10 列原样放在最前，
# 后面的列才是证据字段，旧报告的使用方按列名读取时不会被打断。
HISTORICAL_FIELDS = [
    "id", "规则编号", "关联流", "检查项", "上限阈值", "下限阈值",
    "告警级别", "统计窗口", "连续异常", "status",
]
# 证据字段。规则编号/关联流/检查项沿用历史报告的列，避免 CSV 出现同名表头。
EVIDENCE_FIELDS = [
    "记录类型", "执行批次", "证据位置",
    "样本编号", "监控对象", "样本来路",
    "采集时间", "到达时间",
    "当时阈值版本", "当时下限阈值", "当时上限阈值",
    "实测值", "判定", "缺测原因", "是否计入统计", "迟到",
    "封包截止时间", "入包时间",
    "当前阈值版本", "当前判定", "旧判断可解释",
    "复核结论", "复核依据", "复核时间",
]
PACKAGE_FIELDS = [*HISTORICAL_FIELDS, *EVIDENCE_FIELDS]

SEEDED_AT = "2026-11-01T00:00:00Z"

# 质检调用链上的样本口径：fixtures.QUALITY_SAMPLES 是这些样本的初始来源，
# 这里补上采集时间与样本来路，扫描与导出都从同一张表取数。
_SEED_RULES: list[dict[str, Any]] = [
    {"rule_id": "QUAL-LATENCY", "object": "stream_transform", "check_item": "处理延迟",
     "lower": 0.0, "upper": 5000.0, "source": "fixtures.QUALITY_SAMPLES#QUAL-LATENCY"},
    {"rule_id": "QUAL-THROUGHPUT", "object": "cdc_task", "check_item": "吞吐量",
     "lower": 100.0, "upper": 100000.0, "source": "fixtures.QUALITY_SAMPLES#QUAL-THROUGHPUT"},
    {"rule_id": "QUAL-COMPLETENESS", "object": "sink_writer", "check_item": "完整性",
     "lower": 0.999, "upper": 1.0, "source": "fixtures.QUALITY_SAMPLES#QUAL-COMPLETENESS"},
    {"rule_id": "QUAL-DUPLICATE", "object": "dedup_mgr", "check_item": "重复率",
     "lower": 0.0, "upper": 0.01, "source": "fixtures.QUALITY_SAMPLES#QUAL-DUPLICATE"},
]

_SEED_SAMPLES: list[dict[str, Any]] = [
    {"sample_id": "QS-0001", "rule_id": "QUAL-LATENCY", "object": "stream_transform",
     "check_item": "处理延迟", "value": 320.5, "sampled_at": "2026-11-20T08:00:00Z",
     "provenance": "stream_transform/算子耗时采样器", "missing_reason": ""},
    {"sample_id": "QS-0002", "rule_id": "QUAL-LATENCY", "object": "stream_transform",
     "check_item": "处理延迟", "value": 5600.0, "sampled_at": "2026-11-20T08:01:00Z",
     "provenance": "stream_transform/算子耗时采样器", "missing_reason": ""},
    {"sample_id": "QS-0003", "rule_id": "QUAL-THROUGHPUT", "object": "cdc_task",
     "check_item": "吞吐量", "value": 12500.0, "sampled_at": "2026-11-20T08:02:00Z",
     "provenance": "cdc_task/位点计数器", "missing_reason": ""},
    {"sample_id": "QS-0004", "rule_id": "QUAL-THROUGHPUT", "object": "cdc_task",
     "check_item": "吞吐量", "value": 98.0, "sampled_at": "2026-11-20T08:03:00Z",
     "provenance": "cdc_task/位点计数器", "missing_reason": ""},
    {"sample_id": "QS-0005", "rule_id": "QUAL-COMPLETENESS", "object": "sink_writer",
     "check_item": "完整性", "value": 0.9995, "sampled_at": "2026-11-20T08:04:00Z",
     "provenance": "sink_writer/落盘行数核对", "missing_reason": ""},
    {"sample_id": "QS-0006", "rule_id": "QUAL-DUPLICATE", "object": "dedup_mgr",
     "check_item": "重复率", "value": None, "sampled_at": "2026-11-20T08:05:00Z",
     "provenance": "dedup_mgr/去重键统计", "missing_reason": "采集端超时未回填"},
]


class ExportInterrupted(RuntimeError):
    """模拟落盘中断：已写入的行保留在归档里，重试时从这里继续。"""

    def __init__(self, execution_id: str, written_rows: int, total_rows: int) -> None:
        self.execution_id = execution_id
        self.written_rows = written_rows
        self.total_rows = total_rows
        super().__init__(
            f"导出中断：{execution_id} 已写入 {written_rows}/{total_rows} 行，重试会从断点继续")


# ---------------------------------------------------------------------------
# 时间与取值辅助
# ---------------------------------------------------------------------------
def _now() -> str:
    return _iso(datetime.now(timezone.utc))


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _num(value: Any) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _cell(value: Any) -> str:
    text = "" if value is None else str(value)
    if text and text[0] in "=+-@":
        text = "'" + text
    return text


def _verdict(value: float | None, lower: float, upper: float) -> str:
    if value is None:
        return "缺测"
    return "合格" if lower <= value <= upper else "异常"


def _row_key(row: dict[str, Any]) -> tuple[str, str]:
    if row.get("记录类型") == "样本":
        return ("样本", str(row.get("样本编号", "")))
    return ("规则", str(row.get("规则编号", "")))


# ---------------------------------------------------------------------------
# 服务
# ---------------------------------------------------------------------------
class QualityAuditService:
    # -- 初始化 ---------------------------------------------------------
    def _ensure_seed(self) -> None:
        rules = store.rows(RULE_TABLE)
        if not rules:
            for index, rule in enumerate(_SEED_RULES, start=1):
                rules.append({"id": index, **copy.deepcopy(rule)})
        thresholds = store.rows(THRESHOLD_TABLE)
        if not thresholds:
            for rule in rules:
                thresholds.append({
                    "id": len(thresholds) + 1,
                    "rule_id": rule["rule_id"],
                    "version": 1,
                    "lower": float(rule["lower"]),
                    "upper": float(rule["upper"]),
                    "effective_at": SEEDED_AT,
                    "superseded_at": None,
                    "reason": "初始阈值",
                    "archived_at": SEEDED_AT,
                })
        samples = store.rows(SAMPLE_TABLE)
        if not samples:
            for index, sample in enumerate(_SEED_SAMPLES, start=1):
                samples.append({
                    "id": index,
                    **copy.deepcopy(sample),
                    "arrived_at": sample["sampled_at"],
                })

    # -- 规则与阈值 -----------------------------------------------------
    def list_rules(self) -> list[dict[str, Any]]:
        self._ensure_seed()
        result: list[dict[str, Any]] = []
        for rule in store.rows(RULE_TABLE):
            current = self.current_threshold(rule["rule_id"])
            result.append({
                "规则编号": rule["rule_id"],
                "监控对象": rule["object"],
                "检查项": rule["check_item"],
                "样本来路": rule["source"],
                "当前阈值版本": current["version"] if current else None,
                "当前下限阈值": current["lower"] if current else None,
                "当前上限阈值": current["upper"] if current else None,
                "阈值版本数": len(self.threshold_versions(rule["rule_id"])),
            })
        return result

    def _rule(self, rule_id: str) -> dict[str, Any] | None:
        for rule in store.rows(RULE_TABLE):
            if rule["rule_id"] == rule_id:
                return rule
        return None

    def threshold_versions(self, rule_id: str) -> list[dict[str, Any]]:
        self._ensure_seed()
        rows = [row for row in store.rows(THRESHOLD_TABLE) if row["rule_id"] == rule_id]
        return sorted(rows, key=lambda row: row["version"])

    def current_threshold(self, rule_id: str) -> dict[str, Any] | None:
        rows = [row for row in self.threshold_versions(rule_id) if row.get("superseded_at") is None]
        if not rows:
            return None
        return max(rows, key=lambda row: row["version"])

    def threshold_at(self, rule_id: str, at: Any) -> dict[str, Any] | None:
        """按生效时间解析阈值版本，供历史执行对时使用。"""
        moment = _parse_dt(at)
        candidates = []
        for row in self.threshold_versions(rule_id):
            effective = _parse_dt(row.get("effective_at"))
            superseded = _parse_dt(row.get("superseded_at"))
            if effective is None:
                continue
            if moment is not None and effective > moment:
                continue
            if superseded is not None and moment is not None and superseded <= moment:
                continue
            candidates.append(row)
        if not candidates:
            return None
        return max(candidates, key=lambda row: row["version"])

    def set_threshold(
        self,
        rule_id: str,
        *,
        upper: Any = None,
        lower: Any = None,
        reason: str = "",
    ) -> dict[str, Any] | None:
        """追加一个阈值版本；旧版本只标记失效时间，内容保持原样。"""
        self._ensure_seed()
        current = self.current_threshold(rule_id)
        if current is None:
            return None
        new_upper = float(current["upper"]) if upper is None else _num(upper)
        new_lower = float(current["lower"]) if lower is None else _num(lower)
        if new_upper is None or new_lower is None or new_lower > new_upper:
            raise ValueError("阈值必须是数字，且下限不能大于上限")
        archived_at = _now()
        current["superseded_at"] = archived_at
        row = {
            "id": len(store.rows(THRESHOLD_TABLE)) + 1,
            "rule_id": rule_id,
            "version": int(current["version"]) + 1,
            "lower": new_lower,
            "upper": new_upper,
            "effective_at": archived_at,
            "superseded_at": None,
            "reason": reason or "阈值调整",
            "archived_at": archived_at,
        }
        store.rows(THRESHOLD_TABLE).append(row)
        return row

    # -- 样本 -----------------------------------------------------------
    def list_samples(self) -> list[dict[str, Any]]:
        self._ensure_seed()
        return [copy.deepcopy(row) for row in store.rows(SAMPLE_TABLE)]

    # -- 执行批次 -------------------------------------------------------
    def _next_sequence(self) -> int:
        rows = store.rows(EXECUTION_TABLE)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def create_execution(
        self,
        *,
        rule_ids: list[str] | None = None,
        cutoff_at: Any = None,
    ) -> dict[str, Any]:
        """创建一个执行批次，并把每个规则当时生效的阈值冻结进快照。"""
        self._ensure_seed()
        rules = store.rows(RULE_TABLE)
        selected = [rule["rule_id"] for rule in rules]
        if rule_ids:
            wanted = {str(item) for item in rule_ids}
            selected = [rule_id for rule_id in selected if rule_id in wanted]
        if not selected:
            raise ValueError("没有可审计的质检规则")
        seq = self._next_sequence()
        created_at = _now()
        stamp = (_parse_dt(created_at) or datetime.now(timezone.utc)).strftime("%Y%m%d")
        snapshot: dict[str, dict[str, Any]] = {}
        for rule_id in selected:
            current = self.current_threshold(rule_id)
            if current is None:
                continue
            snapshot[rule_id] = {
                "version": current["version"],
                "lower": float(current["lower"]),
                "upper": float(current["upper"]),
                "archived_at": current["archived_at"],
                "effective_at": current["effective_at"],
            }
        execution = {
            "id": seq,
            "execution_id": f"QE-{stamp}-{seq:04d}",
            "batch_no": f"QB-{stamp}-{seq:04d}",
            "created_at": created_at,
            "cutoff_at": str(cutoff_at) if cutoff_at else created_at,
            "sealed_at": None,
            "status": "采集中",
            "rule_ids": list(snapshot),
            "rule_snapshot": snapshot,
            "rows": [],
            "late_sample_ids": [],
            "review": None,
            "review_rows": {},
            "export": {
                "status": "未导出",
                "attempts": 0,
                "package_version": 0,
                "written_rows": 0,
                "total_rows": 0,
                "last_result": "",
                "partial_rows": None,
                "written_keys": [],
            },
        }
        store.rows(EXECUTION_TABLE).append(execution)
        return execution

    def list_executions(self) -> list[dict[str, Any]]:
        rows = sorted(store.rows(EXECUTION_TABLE), key=lambda row: int(row["id"]), reverse=True)
        return [self.public_execution(row) for row in rows]

    def get_execution(self, execution_id: str) -> dict[str, Any] | None:
        for row in store.rows(EXECUTION_TABLE):
            if row.get("execution_id") == execution_id or row.get("batch_no") == execution_id:
                return row
        return None

    def public_execution(self, execution: dict[str, Any]) -> dict[str, Any]:
        rows = execution.get("rows", [])
        return {
            "execution_id": execution["execution_id"],
            "batch_no": execution["batch_no"],
            "created_at": execution["created_at"],
            "cutoff_at": execution["cutoff_at"],
            "sealed_at": execution["sealed_at"],
            "status": execution["status"],
            "rule_ids": list(execution["rule_ids"]),
            "rule_snapshot": copy.deepcopy(execution["rule_snapshot"]),
            "sample_rows": len(rows),
            "missing_rows": sum(1 for row in rows if row.get("判定") == "缺测"),
            "late_rows": sum(1 for row in rows if row.get("迟到") == "是"),
            "export": {
                key: value for key, value in execution.get("export", {}).items()
                if key not in ("partial_rows", "written_keys")
            },
            "review": copy.deepcopy(execution.get("review")),
            "review_rows": [copy.deepcopy(row) for row in execution.get("review_rows", {}).values()],
        }

    def _snapshot_for(self, execution: dict[str, Any], rule_id: str, at: Any = None) -> dict[str, Any]:
        """优先使用执行冻结的阈值；没有快照时才按时间回查归档。"""
        frozen = execution["rule_snapshot"].get(rule_id)
        if frozen:
            return frozen
        resolved = self.threshold_at(rule_id, at or execution["cutoff_at"])
        if resolved is None:
            raise ValueError(f"规则 {rule_id} 在 {at or execution['cutoff_at']} 没有可用阈值")
        return {
            "version": resolved["version"],
            "lower": float(resolved["lower"]),
            "upper": float(resolved["upper"]),
            "archived_at": resolved["archived_at"],
            "effective_at": resolved["effective_at"],
        }

    def _evidence_location(self, execution: dict[str, Any], kind: str, key: str) -> str:
        return f"audit://{execution['execution_id']}/package.csv#{kind}={key}"

    def _sample_row(self, execution: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
        rule_id = str(sample["rule_id"])
        snapshot = self._snapshot_for(execution, rule_id, sample.get("sampled_at"))
        value = _num(sample.get("value"))
        missing_reason = str(sample.get("missing_reason") or "")
        if value is None and not missing_reason:
            missing_reason = "实测值缺失"
        verdict = _verdict(value, snapshot["lower"], snapshot["upper"])
        arrived_at = sample.get("arrived_at") or sample.get("sampled_at")
        cutoff = _parse_dt(execution["cutoff_at"])
        arrived = _parse_dt(arrived_at)
        # 封包后才补录的样本一律按迟到处理：即使补录时自报的到达时间早于截止时间，
        # 它也没有进入审计员下载的那一版包，不能回头算成当时的合格记录。
        late = bool(sample.get("_late_registered")) or (
            cutoff is not None and arrived is not None and arrived > cutoff)
        # 缺测（无实测值）与迟到补录都不算合格记录，也不进合格统计的分母。
        included = verdict in ("合格", "异常") and not late
        return {
            "sample_id": str(sample["sample_id"]),
            "记录类型": "样本",
            "执行批次": execution["batch_no"],
            "执行编号": execution["execution_id"],
            "证据位置": self._evidence_location(execution, "sample", str(sample["sample_id"])),
            "样本编号": str(sample["sample_id"]),
            "规则编号": rule_id,
            "关联流": str(sample.get("object") or ""),
            "监控对象": str(sample.get("object") or ""),
            "检查项": str(sample.get("check_item") or ""),
            "样本来路": str(sample.get("provenance") or ""),
            "采集时间": str(sample.get("sampled_at") or ""),
            "到达时间": str(arrived_at or ""),
            "当时阈值版本": snapshot["version"],
            "当时下限阈值": snapshot["lower"],
            "当时上限阈值": snapshot["upper"],
            "实测值": value,
            "判定": verdict,
            "缺测原因": missing_reason if verdict == "缺测" else "",
            "是否计入统计": "是" if included else "否",
            "迟到": "是" if late else "否",
            "封包截止时间": execution["cutoff_at"],
            "入包时间": execution.get("sealed_at") or _now(),
            "是否计入合格统计": verdict == "合格" and included,
        }

    # 归档后不允许被补录覆盖的字段：阈值按当时的快照原样读取，证据位置也保持首版，
    # 迟到标记一旦确定为「是」也不回退，避免重试把已排除的样本重新算进统计。
    FROZEN_FIELDS = (
        "证据位置", "入包时间", "当时阈值版本", "当时下限阈值", "当时上限阈值",
        "封包截止时间", "执行批次", "执行编号",
    )

    def _upsert_row(self, execution: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
        """按样本编号去重：同一次执行里重复补录只保留一行，并保住原始证据位置与阈值。"""
        rows = execution.setdefault("rows", [])
        key = _row_key(row)
        for existing in rows:
            if _row_key(existing) == key:
                frozen = {field: existing.get(field) for field in self.FROZEN_FIELDS}
                was_late = existing.get("迟到") == "是" or row.get("迟到") == "是"
                existing.update(row)
                for field, value in frozen.items():
                    if value is not None:
                        existing[field] = value
                if was_late:
                    existing["迟到"] = "是"
                    existing["是否计入统计"] = "否"
                    existing["是否计入合格统计"] = False
                return existing
        rows.append(row)
        return row

    def seal(self, execution_id: str, *, cutoff_at: Any = None) -> dict[str, Any] | None:
        """封包：把截止前到达的样本固化成证据行；重复调用不会重复追加。"""
        execution = self.get_execution(execution_id)
        if execution is None:
            return None
        if cutoff_at:
            execution["cutoff_at"] = str(cutoff_at)
        if execution.get("sealed_at"):
            return execution
        cutoff = _parse_dt(execution["cutoff_at"])
        for sample in store.rows(SAMPLE_TABLE):
            if sample["rule_id"] not in execution["rule_ids"]:
                continue
            sampled = _parse_dt(sample.get("sampled_at"))
            if cutoff is not None and sampled is not None and sampled > cutoff:
                continue
            row = self._sample_row(execution, sample)
            self._upsert_row(execution, row)
        execution["sealed_at"] = execution.get("sealed_at") or _now()
        execution["status"] = "已封包"
        return execution

    def add_sample(self, execution_id: str, values: dict[str, Any]) -> dict[str, Any]:
        """补录同一执行批次的迟到样本；重复样本编号只更新不新增。"""
        execution = self.get_execution(execution_id)
        if execution is None:
            raise KeyError(f"执行批次 {execution_id} 不存在")
        already_sealed = bool(execution.get("sealed_at"))
        if not already_sealed:
            self.seal(execution_id)
            execution = self.get_execution(execution_id) or execution
        rule_id = str(values.get("规则编号") or values.get("rule_id") or "")
        if rule_id not in execution["rule_ids"]:
            raise ValueError(f"规则 {rule_id} 不属于执行批次 {execution['batch_no']}")
        sampled_at = values.get("采集时间") or values.get("采样时间") or values.get("sampled_at") or _now()
        arrived_at = values.get("到达时间") or values.get("arrived_at") or _now()
        sample_id = str(values.get("样本编号") or values.get("sample_id") or "")
        if not sample_id:
            sample_id = f"QS-LATE-{len(execution['rows']) + 1:04d}"
        sample = {
            "sample_id": sample_id,
            "rule_id": rule_id,
            "object": values.get("监控对象") or values.get("object") or "",
            "check_item": values.get("检查项") or values.get("check_item") or "",
            "value": values.get("实测值", values.get("value")),
            "sampled_at": sampled_at,
            "arrived_at": arrived_at,
            "provenance": values.get("样本来路") or values.get("provenance") or "补录",
            "missing_reason": values.get("缺测原因") or values.get("missing_reason") or "",
        }
        if already_sealed:
            sample["_late_registered"] = True
        row = self._sample_row(execution, sample)
        row["_late_registered"] = bool(sample.get("_late_registered"))
        stored = self._upsert_row(execution, row)
        stored.pop("_late_registered", None)
        if stored.get("迟到") == "是" and stored["sample_id"] not in execution["late_sample_ids"]:
            execution["late_sample_ids"].append(stored["sample_id"])
        return stored

    # -- 证据行 ---------------------------------------------------------
    def _current_verdict(self, row: dict[str, Any]) -> tuple[int | None, str]:
        current = self.current_threshold(str(row.get("规则编号", "")))
        if current is None:
            return None, "无阈值"
        return int(current["version"]), _verdict(
            _num(row.get("实测值")), float(current["lower"]), float(current["upper"]))

    def _decorate_sample_row(self, execution: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
        """在归档行上补充“按当前阈值会怎么判”，归档字段本身保持原样。"""
        decorated = copy.deepcopy(row)
        current_version, current_verdict = self._current_verdict(row)
        archived_lower = _num(row.get("当时下限阈值"))
        archived_upper = _num(row.get("当时上限阈值"))
        if archived_lower is None or archived_upper is None:
            explainable = "否"
        else:
            recomputed = _verdict(_num(row.get("实测值")), archived_lower, archived_upper)
            explainable = "是" if recomputed == row.get("判定") else "否"
        decorated["当前阈值版本"] = current_version
        decorated["当前判定"] = current_verdict
        decorated["旧判断可解释"] = explainable
        review_row = execution.get("review_rows", {}).get(str(row.get("样本编号", "")))
        if review_row:
            decorated["复核结论"] = review_row.get("复核结论", "")
            decorated["复核依据"] = review_row.get("复核依据", "")
            decorated["复核时间"] = review_row.get("复核时间", "")
        else:
            decorated["复核结论"] = ""
            decorated["复核依据"] = ""
            decorated["复核时间"] = ""
        return decorated

    def _rule_row(self, execution: dict[str, Any], rule_id: str) -> dict[str, Any]:
        snapshot = execution["rule_snapshot"].get(rule_id, {})
        rule = self._rule(rule_id) or {}
        row: dict[str, Any] = {field: "" for field in PACKAGE_FIELDS}
        row.update({
            "id": "—",
            "规则编号": rule_id,
            "关联流": str(rule.get("object") or ""),
            "检查项": str(rule.get("check_item") or ""),
            "上限阈值": snapshot.get("upper", ""),
            "下限阈值": snapshot.get("lower", ""),
            "status": "已封包",
            "记录类型": "规则",
            "执行批次": execution["batch_no"],
            "证据位置": self._evidence_location(execution, "rule", rule_id),
            "监控对象": str(rule.get("object") or ""),
            "样本来路": str(rule.get("source") or ""),
            "采集时间": execution["created_at"],
            "到达时间": execution["created_at"],
            "当时阈值版本": snapshot.get("version", ""),
            "当时下限阈值": snapshot.get("lower", ""),
            "当时上限阈值": snapshot.get("upper", ""),
            "是否计入统计": "否",
            "迟到": "否",
            "封包截止时间": execution["cutoff_at"],
            "入包时间": execution.get("sealed_at") or _now(),
        })
        return row

    def manifest(self, execution_id: str) -> dict[str, Any] | None:
        execution = self.get_execution(execution_id)
        if execution is None:
            return None
        if not execution.get("sealed_at"):
            self.seal(execution_id)
            execution = self.get_execution(execution_id) or execution
        rows = execution.get("rows", [])
        sample_rows = [row for row in rows if row.get("记录类型", "样本") == "样本"]
        missing = sum(1 for row in sample_rows if row.get("判定") == "缺测")
        late = sum(1 for row in sample_rows if row.get("迟到") == "是")
        included = sum(1 for row in sample_rows if row.get("是否计入统计") == "是")
        # 合格/异常只统计计入统计口径的行，迟到补录与缺测不抬高合格数。
        anomalies = sum(
            1 for row in sample_rows
            if row.get("判定") == "异常" and row.get("是否计入统计") == "是")
        passed = sum(
            1 for row in sample_rows
            if row.get("判定") == "合格" and row.get("是否计入统计") == "是")
        export = execution.get("export", {})
        return {
            "执行编号": execution["execution_id"],
            "execution_id": execution["execution_id"],
            "执行批次": execution["batch_no"],
            "batch_no": execution["batch_no"],
            "创建时间": execution["created_at"],
            "封包截止时间": execution["cutoff_at"],
            "封包时间": execution.get("sealed_at"),
            "状态": execution.get("status"),
            "导出状态": export.get("status", "未导出"),
            "包版本": export.get("package_version", 0),
            "样本总数": len(sample_rows),
            "计入统计样本数": included,
            "合格样本数": passed,
            "异常样本数": anomalies,
            "缺测样本数": missing,
            "迟到样本数": late,
            "完整": late == 0 and missing == 0,
            "统计口径": "只计入封包截止前到达且实测值有效的样本；缺测样本与迟到补录样本不计入合格统计",
            "证据位置": f"audit://{execution['execution_id']}/package.csv",
            "复核": copy.deepcopy(execution.get("review")),
        }

    def package_rows(self, execution_id: str) -> list[dict[str, Any]]:
        """返回分好记录类型的证据行（规则行 + 样本行，样本行已补充当前阈值判断）。"""
        execution = self.get_execution(execution_id)
        if execution is None:
            raise KeyError(f"执行批次 {execution_id} 不存在")
        if not execution.get("sealed_at"):
            self.seal(execution_id)
            execution = self.get_execution(execution_id) or execution
        rules = [self._rule_row(execution, rule_id) for rule_id in execution["rule_ids"]]
        samples = [
            self._decorate_sample_row(execution, row)
            for row in sorted(execution.get("rows", []), key=lambda item: str(item.get("样本编号", "")))
        ]
        return [*rules, *samples]

    def sample_evidence(self, execution_id: str) -> list[dict[str, Any]]:
        return [row for row in self.package_rows(execution_id) if row.get("记录类型") == "样本"]

    # -- 导出 -----------------------------------------------------------
    def _csv_content(self, rows: list[dict[str, Any]]) -> str:
        output = io.StringIO()
        output.write("﻿")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(PACKAGE_FIELDS)
        for row in rows:
            writer.writerow([_cell(row.get(field, "")) for field in PACKAGE_FIELDS])
        return output.getvalue()

    def render_package(
        self,
        execution_id: str,
        *,
        interrupt_after: int | None = None,
        fmt: str = "csv",
        increment_version: bool = True,
    ) -> dict[str, Any]:
        """生成审计包。中断时保留已写行，重试从断点继续且不重复。"""
        execution = self.get_execution(execution_id)
        if execution is None:
            raise KeyError(f"执行批次 {execution_id} 不存在")
        if not execution.get("sealed_at"):
            self.seal(execution_id)
            execution = self.get_execution(execution_id) or execution
        all_rows = self.package_rows(execution_id)
        state = execution.setdefault("export", {
            "status": "未导出", "attempts": 0, "package_version": 0,
            "written_rows": 0, "total_rows": 0, "last_result": "",
            "partial_rows": None, "written_keys": [],
        })

        if interrupt_after is not None:
            written_rows = all_rows[: max(0, min(int(interrupt_after), len(all_rows)))]
            state["partial_rows"] = copy.deepcopy(written_rows)
            state["written_keys"] = [list(_row_key(row)) for row in written_rows]
            state["written_rows"] = len(written_rows)
            state["total_rows"] = len(all_rows)
            state["attempts"] = int(state.get("attempts", 0)) + 1
            state["status"] = "导出中断"
            state["last_result"] = "落盘中断，归档阈值保持不变"
            execution["status"] = "导出中断"
            raise ExportInterrupted(execution["execution_id"], len(written_rows), len(all_rows))

        written = list(state.get("partial_rows") or [])
        seen = {tuple(key) for key in (state.get("written_keys") or [])}
        for row in all_rows:
            key = _row_key(row)
            if key in seen:
                continue
            written.append(row)
            seen.add(key)
        # 归档行按样本编号去重，防止重试把同一行写两次。
        deduped: list[dict[str, Any]] = []
        unique: set[tuple[str, str]] = set()
        for row in written:
            key = _row_key(row)
            if key in unique:
                continue
            unique.add(key)
            deduped.append(row)

        state["partial_rows"] = None
        state["written_keys"] = []
        state["attempts"] = int(state.get("attempts", 0)) + 1
        state["written_rows"] = len(deduped)
        state["total_rows"] = len(all_rows)
        state["status"] = "已导出"
        state["last_result"] = "导出完成"
        if increment_version or int(state.get("package_version", 0)) == 0:
            state["package_version"] = int(state.get("package_version", 0)) + 1
        if execution.get("status") not in ("复核完成",):
            execution["status"] = "已导出"
        manifest = self.manifest(execution_id) or {}
        if fmt == "json":
            content: Any = {"manifest": manifest, "rows": deduped}
        else:
            content = self._csv_content(deduped)
        return {
            "execution_id": execution["execution_id"],
            "batch_no": execution["batch_no"],
            "content": content,
            "rows": deduped,
            "manifest": manifest,
            "package_version": state["package_version"],
        }

    # -- 复核 -----------------------------------------------------------
    def _parse_package(self, content: str) -> list[dict[str, str]]:
        reader = csv.DictReader(io.StringIO(content.lstrip("﻿")))
        return [row for row in reader if row.get("记录类型") == "样本"]

    def replay(self, execution_id: str) -> dict[str, Any]:
        """读审计包并实际重算校验，结果回写审计清单（同一执行重复复核不产生重复行）。

        这里刻意从导出包的 CSV 内容重新解析，而不是复用内存里的执行行，
        保证复核复核的是审计员下载到的那份材料。
        """
        execution = self.get_execution(execution_id)
        if execution is None:
            raise KeyError(f"执行批次 {execution_id} 不存在")
        package = self.render_package(execution_id, increment_version=False)
        parsed_rows = self._parse_package(str(package["content"]))
        reviewed_at = _now()
        review_rows: dict[str, dict[str, Any]] = {}
        for parsed in parsed_rows:
            value = _num(parsed.get("实测值"))
            lower = _num(parsed.get("当时下限阈值"))
            upper = _num(parsed.get("当时上限阈值"))
            version = parsed.get("当时阈值版本", "")
            if lower is None or upper is None:
                recomputed = "缺测"
            else:
                recomputed = _verdict(value, lower, upper)
            exported_verdict = parsed.get("判定", "")
            consistent = recomputed == exported_verdict
            current = self.current_threshold(str(parsed.get("规则编号", "")))
            if current is not None and lower is not None and upper is not None:
                current_verdict = _verdict(value, float(current["lower"]), float(current["upper"]))
                current_version: Any = current["version"]
            else:
                current_verdict = "缺测"
                current_version = None
            sample_id = str(parsed.get("样本编号", ""))
            review_rows[sample_id] = {
                "样本编号": sample_id,
                "执行批次": parsed.get("执行批次", execution["batch_no"]),
                "规则编号": parsed.get("规则编号", ""),
                "监控对象": parsed.get("监控对象", ""),
                "采集时间": parsed.get("采集时间", ""),
                "导出判定": exported_verdict,
                "复核判定": recomputed,
                "复核结论": "一致" if consistent else "不一致",
                "复核依据": f"阈值版本 {version}：下限 {parsed.get('当时下限阈值', '')}，上限 {parsed.get('当时上限阈值', '')}",
                "当时阈值版本": version,
                "当前阈值版本": current_version,
                "当前判定": current_verdict,
                "旧判断可解释": "是" if consistent else "否",
                "是否计入统计": parsed.get("是否计入统计", "否"),
                "证据位置": parsed.get("证据位置", ""),
                "复核时间": reviewed_at,
            }
        rows = list(review_rows.values())
        consistent_count = sum(1 for row in rows if row["复核结论"] == "一致")
        execution["review_rows"] = review_rows
        execution["review"] = {
            "复核时间": reviewed_at,
            "执行批次": execution["batch_no"],
            "包版本": package["package_version"],
            "证据位置": f"audit://{execution['execution_id']}/package.csv",
            "复核行数": len(rows),
            "一致行数": consistent_count,
            "不一致行数": len(rows) - consistent_count,
            "缺测行数": sum(1 for row in rows if row["复核判定"] == "缺测"),
            "旧判断全部可解释": all(row["旧判断可解释"] == "是" for row in rows),
        }
        if execution.get("status") != "复核完成":
            execution["status"] = "复核完成"
        return {
            "execution_id": execution["execution_id"],
            "batch_no": execution["batch_no"],
            "package_version": package["package_version"],
            "evidence_location": execution["review"]["证据位置"],
            "review": copy.deepcopy(execution["review"]),
            "rows": rows,
        }

    def list_reviews(self) -> list[dict[str, Any]]:
        result = []
        for execution in sorted(store.rows(EXECUTION_TABLE), key=lambda row: int(row["id"]), reverse=True):
            if not execution.get("review"):
                continue
            result.append({
                "execution_id": execution["execution_id"],
                "batch_no": execution["batch_no"],
                "status": execution.get("status"),
                "review": copy.deepcopy(execution["review"]),
                "rows": [copy.deepcopy(row) for row in execution.get("review_rows", {}).values()],
            })
        return result


quality_audit = QualityAuditService()
