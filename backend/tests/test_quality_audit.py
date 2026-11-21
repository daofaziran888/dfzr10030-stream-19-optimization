"""阈值审计证据测试：样本、阈值快照、导出中断续写与复核回写。"""
from __future__ import annotations

import csv
import io

import pytest

from app.services.pipeline_orchestrator import run_quality_scan
from app.services.quality_audit import (
    ExportInterrupted, PACKAGE_FIELDS, quality_audit,
)


def parse_package(content: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(content.lstrip("﻿"))))


def sample_rows(content: str) -> list[dict[str, str]]:
    return [row for row in parse_package(content) if row["记录类型"] == "样本"]


def test_package_has_evidence_columns_and_batch():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]
    package = quality_audit.render_package(execution_id)
    rows = parse_package(package["content"])

    assert list(rows[0].keys()) == PACKAGE_FIELDS
    assert all(row["执行批次"] == execution["batch_no"] for row in rows)
    assert all(row["采集时间"] for row in rows)
    assert all(row["证据位置"].startswith(f"audit://{execution_id}/") for row in rows)
    # 历史报告十列原样保留在最前面
    assert list(rows[0].keys())[:10] == [
        "id", "规则编号", "关联流", "检查项", "上限阈值", "下限阈值",
        "告警级别", "统计窗口", "连续异常", "status",
    ]


def test_sample_rows_carry_source_and_applied_threshold():
    execution = quality_audit.create_execution()
    rows = sample_rows(quality_audit.render_package(execution["execution_id"])["content"])
    latency = [row for row in rows if row["样本编号"] == "QS-0002"][0]

    assert latency["样本来路"] == "stream_transform/算子耗时采样器"
    assert latency["采集时间"] == "2026-11-20T08:01:00Z"
    assert latency["当时阈值版本"] == "1"
    assert float(latency["当时上限阈值"]) == 5000.0
    assert latency["判定"] == "异常"


def test_missing_sample_not_counted_as_pass():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]
    package = quality_audit.render_package(execution_id)
    missing = [row for row in sample_rows(package["content"]) if row["样本编号"] == "QS-0006"]

    assert len(missing) == 1
    assert missing[0]["判定"] == "缺测"
    assert missing[0]["缺测原因"] == "采集端超时未回填"
    assert missing[0]["是否计入统计"] == "否"
    manifest = package["manifest"]
    assert manifest["缺测样本数"] == 1
    assert manifest["计入统计样本数"] == manifest["样本总数"] - 1
    assert manifest["合格样本数"] + manifest["异常样本数"] == manifest["计入统计样本数"]


def test_threshold_change_keeps_archived_threshold():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]
    quality_audit.render_package(execution_id)

    quality_audit.set_threshold("QUAL-LATENCY", upper=6000.0, reason="调高延迟上限")

    rows = sample_rows(quality_audit.render_package(execution_id)["content"])
    latency = [row for row in rows if row["样本编号"] == "QS-0002"][0]
    # 归档阈值照原版读取，当前阈值另行标注
    assert latency["当时阈值版本"] == "1"
    assert float(latency["当时上限阈值"]) == 5000.0
    assert latency["判定"] == "异常"
    assert latency["当前阈值版本"] == "2"
    assert latency["当前判定"] == "合格"
    assert latency["旧判断可解释"] == "是"


def test_review_recomputes_from_package_and_writes_back():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]
    quality_audit.render_package(execution_id, increment_version=False)
    quality_audit.set_threshold("QUAL-LATENCY", upper=6000.0)

    result = quality_audit.replay(execution_id)
    summary = result["review"]

    assert summary["复核行数"] == summary["一致行数"] + summary["不一致行数"]
    assert summary["旧判断全部可解释"] is True
    latency = [row for row in result["rows"] if row["样本编号"] == "QS-0002"][0]
    assert latency["导出判定"] == "异常"
    assert latency["复核判定"] == "异常"
    assert latency["当前判定"] == "合格"
    assert latency["证据位置"] == f"audit://{execution_id}/package.csv#sample=QS-0002"
    assert latency["复核依据"] == "阈值版本 1：下限 0.0，上限 5000.0"

    # 回写审计清单
    reviews = quality_audit.list_reviews()
    assert len(reviews) == 1
    assert reviews[0]["review"]["证据位置"] == f"audit://{execution_id}/package.csv"


def test_late_sample_added_after_seal_is_not_counted_and_not_duplicated():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]
    quality_audit.seal(execution_id)
    before = quality_audit.manifest(execution_id)

    row = quality_audit.add_sample(execution_id, {
        "样本编号": "QS-LATE-1",
        "rule_id": "QUAL-LATENCY",
        "采集时间": "2026-11-20T08:30:00Z",
        "到达时间": "2026-11-20T08:30:00Z",
        "value": 4800.0,
        "provenance": "stream_transform/算子耗时采样器",
    })
    assert row["迟到"] == "是"
    assert row["是否计入统计"] == "否"
    assert row["判定"] == "合格"

    # 同一迟到样本重复补录只保留一行
    quality_audit.add_sample(execution_id, {
        "样本编号": "QS-LATE-1", "rule_id": "QUAL-LATENCY", "value": 4800.0,
    })
    ids = [item["样本编号"] for item in quality_audit.sample_evidence(execution_id)]
    assert ids.count("QS-LATE-1") == 1

    after = quality_audit.manifest(execution_id)
    assert after["迟到样本数"] == 1
    assert after["计入统计样本数"] == before["计入统计样本数"]
    assert after["合格样本数"] == before["合格样本数"]


def test_repeated_late_add_keeps_late_flag():
    """同一迟到样本补录两次，第二次不带到达时间也不能把迟到标记洗掉。"""
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]
    quality_audit.add_sample(execution_id, {
        "样本编号": "QS-LATE-3", "rule_id": "QUAL-DUPLICATE", "value": 0.5,
        "采集时间": "2026-11-20T08:40:00Z", "到达时间": "2026-11-20T08:40:00Z",
    })
    quality_audit.add_sample(execution_id, {
        "样本编号": "QS-LATE-3", "rule_id": "QUAL-DUPLICATE", "value": 0.5,
    })
    rows = [row for row in quality_audit.sample_evidence(execution_id)
            if row["样本编号"] == "QS-LATE-3"]
    assert len(rows) == 1
    assert rows[0]["迟到"] == "是"
    assert rows[0]["是否计入统计"] == "否"


def test_interrupted_export_resumes_without_duplicate_rows():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]

    with pytest.raises(ExportInterrupted) as caught:
        quality_audit.render_package(execution_id, interrupt_after=3)
    assert caught.value.written_rows == 3

    state = quality_audit.get_execution(execution_id)["export"]
    assert state["status"] == "导出中断"
    assert state["partial_rows"]

    package = quality_audit.render_package(execution_id)
    rows = parse_package(package["content"])
    keys = [(row["记录类型"], row["样本编号"] or row["规则编号"]) for row in rows]
    assert len(keys) == len(set(keys))
    assert package["manifest"]["导出状态"] == "已导出"
    assert state["attempts"] == 2


def test_repeated_review_and_resume_do_not_duplicate_audit_rows():
    execution = quality_audit.create_execution()
    execution_id = execution["execution_id"]

    with pytest.raises(ExportInterrupted):
        quality_audit.render_package(execution_id, interrupt_after=2)
    quality_audit.add_sample(execution_id, {
        "样本编号": "QS-LATE-2", "rule_id": "QUAL-THROUGHPUT", "value": 6000.0,
        "采集时间": "2026-11-20T08:31:00Z",
    })
    quality_audit.render_package(execution_id)

    first = quality_audit.replay(execution_id)
    second = quality_audit.replay(execution_id)
    sample_ids = [row["样本编号"] for row in second["rows"]]

    assert len(sample_ids) == len(set(sample_ids))
    assert first["review"]["复核行数"] == second["review"]["复核行数"]
    assert len(quality_audit.list_reviews()) == 1


def test_quality_scan_returns_samples_and_applied_threshold():
    scan = run_quality_scan()
    samples = {row["样本编号"]: row for row in scan["samples"]}

    assert samples["QS-0002"]["判定"] == "异常"
    assert samples["QS-0002"]["阈值版本"] == 1
    assert samples["QS-0002"]["采集时间"] == "2026-11-20T08:01:00Z"
    assert samples["QS-0006"]["判定"] == "缺测"
    assert samples["QS-0006"]["是否计入统计"] == "否"
    assert scan["missing_samples"] == 1
    assert scan["scored_samples"] == len(samples) - 1
    # 异常计数与合格统计口径一致
    assert len(scan["anomalies"]) == sum(1 for row in samples.values() if row["判定"] == "异常")
