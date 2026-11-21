"""流式管道编排器：实现 7 条跨模块数据管道。

每条管道都读取上游 fixtures 或 store 数据，执行真实流式计算，
把结果写入 store._pipeline_results 供下游消费。
"""
from __future__ import annotations

import math, statistics
from datetime import datetime
from typing import Any

from app.fixtures import (
    SOURCE_EVENTS, TRANSFORM_RECORDS, WINDOW_CONFIGS,
    RULE_TRIGGERS, METRIC_DEFS,
    PARTITION_STATS, EVENT_ROUTES,
)
from app.services.quality_audit import quality_audit
from app.store import store


def _to_float(v: Any) -> float:
    try: return float(v)
    except (TypeError, ValueError): return 0.0


def _wr(pid: str, payload: dict) -> None:
    t = store.rows("_pipeline_results")
    for r in t:
        if r.get("pipeline_id") == pid:
            r.update(payload); r["updated_at"] = datetime.now().isoformat(); return
    e = {"pipeline_id": pid, "created_at": datetime.now().isoformat()}
    e.update(payload); t.append(e)


def _rr(pid: str) -> dict | None:
    for r in store.rows("_pipeline_results"):
        if r.get("pipeline_id") == pid: return dict(r)
    return None


# Pipeline 1: source_binding -> cdc_task -> stream_transform
def run_source_cdc_pipeline() -> dict:
    """模拟源端事件流：统计各源端的事件类型分布和吞吐量。"""
    events = list(SOURCE_EVENTS)
    src_stats: dict[str, dict] = {}
    for e in events:
        src = e["源端编号"]
        if src not in src_stats:
            src_stats[src] = {"total": 0, "INSERT": 0, "UPDATE": 0, "DELETE": 0, "总行数": 0}
        src_stats[src]["total"] += 1
        src_stats[src][e["事件类型"]] += 1
        src_stats[src]["总行数"] += e["数据行数"]

    result = {
        "source_count": len(src_stats),
        "total_events": len(events),
        "total_rows": sum(s["总行数"] for s in src_stats.values()),
        "sources": {k: dict(v) for k, v in src_stats.items()},
    }
    _wr("source_cdc_transform", {"source": ["source_binding", "cdc_task"], "target": "stream_transform", "payload": result})
    return result


# Pipeline 2: stream_transform -> window_agg -> metric_calc
def run_window_metric_pipeline(window_id: str | None = None) -> dict:
    """执行窗口聚合并计算指标。"""
    records = list(TRANSFORM_RECORDS)
    configs = WINDOW_CONFIGS
    if window_id:
        configs = [c for c in configs if c["窗口编号"] == window_id]

    results: dict[str, Any] = {"total_records": len(records)}
    for cfg in configs:
        gmv = sum(_to_float(r["金额"]) for r in records)
        cnt = len(records)
        users = set(r["用户ID"] for r in records)
        channel_dist: dict[str, int] = {}
        for r in records:
            channel_dist[r["渠道"]] = channel_dist.get(r["渠道"], 0) + 1

        results[cfg["窗口编号"]] = {
            "窗口类型": cfg["窗口类型"], "记录数": cnt,
            "gmv": round(gmv, 2), "独立用户": len(users),
            "平均订单": round(gmv / cnt, 2) if cnt else 0.0,
            "渠道分布": channel_dist,
        }

    # 聚合指标
    metrics = {}
    for m in METRIC_DEFS:
        mid = m["指标编号"]
        if "count" in m["计算公式"]:
            metrics[mid] = len(records)
        elif "sum" in m["计算公式"]:
            metrics[mid] = round(sum(_to_float(r["金额"]) for r in records), 2)
        elif "avg" in m["计算公式"]:
            vals = [_to_float(r["金额"]) for r in records]
            metrics[mid] = round(statistics.mean(vals), 2) if vals else 0.0
        elif "distinct" in m["计算公式"]:
            metrics[mid] = len(set(r["用户ID"] for r in records))

    results["metrics"] = metrics
    _wr("window_metric_calc", {"source": ["stream_transform", "window_agg"], "target": "metric_calc", "payload": results})
    return results


# Pipeline 3: rule_engine -> event_router -> pipeline_orch
def run_rule_router_pipeline() -> dict:
    """规则引擎触发后的事件路由。"""
    triggers = list(RULE_TRIGGERS)
    routes = {r["规则编号"]: r.get("目标管道", "default") for r in EVENT_ROUTES}
    routing: dict[str, list] = {}
    total_hits = 0
    for t in triggers:
        rid = t["规则编号"]
        route = routes.get(rid, "default")
        if route not in routing:
            routing[route] = []
        hits = len(t.get("命中用户", []))
        total_hits += hits
        routing[route].append({"规则": rid, "动作": t["动作"], "命中": hits})

    result = {"rules_fired": len(triggers), "total_hits": total_hits, "routing": routing}
    _wr("rule_router_orch", {"source": ["rule_engine", "event_router"], "target": "pipeline_orch", "payload": result})
    return result


# Pipeline 4: metric_calc -> pipeline_orch -> sink_writer
def run_metric_sink_pipeline() -> dict:
    """指标结果写入下游 sink。"""
    # 先跑 window_metric 拿到指标
    wm = run_window_metric_pipeline()
    metrics = wm.get("metrics", {})

    sinks: list[dict] = []
    for mid, val in metrics.items():
        sinks.append({"指标编号": mid, "值": val, "写入时间": datetime.now().isoformat(), "写入状态": "成功"})

    result = {"sink_count": len(sinks), "sinks": sinks}
    _wr("metric_pipeline_sink", {"source": ["metric_calc", "pipeline_orch"], "target": "sink_writer", "payload": result})
    return result


# Pipeline 5: data_quality -> pipeline_orch
def _quality_evidence_rows() -> list[dict]:
    """从质检调用链取证据：样本、样本来路、采集时间与当时适用的阈值。"""
    audit = quality_audit
    rules = {rule["规则编号"]: rule for rule in audit.list_rules()}
    rows: list[dict] = []
    for sample in audit.list_samples():
        rule_id = str(sample["rule_id"])
        rule = rules.get(rule_id)
        if rule is None:
            continue
        threshold = audit.threshold_at(rule_id, sample.get("sampled_at")) or audit.current_threshold(rule_id)
        if threshold is None:
            continue
        value = _to_float(sample.get("value")) if sample.get("value") is not None else None
        missing_reason = str(sample.get("missing_reason") or "")
        if value is None and not missing_reason:
            missing_reason = "实测值缺失"
        lo = float(threshold["lower"])
        hi = float(threshold["upper"])
        if value is None:
            verdict = "缺测"
        else:
            verdict = "合格" if lo <= value <= hi else "异常"
        rows.append({
            "样本编号": sample["sample_id"],
            "规则编号": rule_id,
            "监控对象": sample.get("object") or rule["监控对象"],
            "检查项": sample.get("check_item") or rule["检查项"],
            "样本来路": sample.get("provenance", ""),
            "采集时间": sample.get("sampled_at", ""),
            "到达时间": sample.get("arrived_at", ""),
            "实测值": value,
            "判定": verdict,
            "缺测原因": missing_reason if verdict == "缺测" else "",
            "阈值版本": threshold["version"],
            "下限阈值": lo,
            "上限阈值": hi,
            "阈值归档时间": threshold.get("archived_at"),
            "阈值字符串": f"[{lo}, {hi}]",
            "是否计入统计": "是" if verdict in ("合格", "异常") else "否",
        })
    return rows


def run_quality_scan() -> dict:
    """质量扫描：用每个样本采集时适用的阈值判定，并把证据一并带回。"""
    evidence = _quality_evidence_rows()
    anomalies = []
    score = 100.0
    for row in evidence:
        if row["判定"] == "异常":
            anomalies.append({
                "规则": row["规则编号"],
                "对象": row["监控对象"],
                "当前值": row["实测值"],
                "阈值": row["阈值字符串"],
                "阈值版本": row["阈值版本"],
                "样本编号": row["样本编号"],
                "采集时间": row["采集时间"],
            })
            score -= 25.0

    missing = sum(1 for row in evidence if row["判定"] == "缺测")
    scored_rows = [row for row in evidence if row["是否计入统计"] == "是"]

    # 扫描 store 模块 abnormal 标记
    mod_issues = []
    for mod in store.module_names():
        if mod.startswith("_"): continue
        rs = store.rows(mod)
        ab = sum(1 for r in rs if r.get("abnormal"))
        if ab:
            mod_issues.append({"module": mod, "abnormal": ab})

    result = {
        "quality_score": max(0, score),
        "anomalies": anomalies,
        "module_issues": mod_issues,
        "samples": evidence,
        "missing_samples": missing,
        "scored_samples": len(scored_rows),
    }
    _wr("quality_scan", {"source": ["data_quality"], "target": "_global", "payload": result})
    return result


# Pipeline 8: data_quality(审计包) -> pipeline_orch
def run_quality_replay_pipeline(execution_id: str | None = None) -> dict:
    """读阈值审计导出包、实际重新执行校验，并把复核结果回写审计清单。

    不传 execution_id 时，对最近一个已封包的执行批次做复核。
    """
    if execution_id is None:
        executions = quality_audit.list_executions()
        if not executions:
            resealed = quality_audit.create_execution()
            execution_id = resealed["execution_id"]
        else:
            execution_id = executions[0]["execution_id"]
    if quality_audit.get_execution(execution_id) is None:
        raise KeyError(f"执行批次 {execution_id} 不存在")
    reviewed = quality_audit.replay(execution_id)
    payload = {
        "execution_id": reviewed["execution_id"],
        "batch_no": reviewed["batch_no"],
        "package_version": reviewed["package_version"],
        "evidence_location": reviewed["evidence_location"],
        "summary": reviewed["review"],
        "rows": reviewed["rows"],
    }
    _wr("quality_audit_replay", {
        "source": ["data_quality"], "target": "pipeline_orch", "payload": payload,
    })
    return payload


# Pipeline 6: grp_partition -> backpressure
def run_partition_backpressure_pipeline() -> dict:
    """分区统计与背压评估。"""
    stats = list(PARTITION_STATS)
    bp_alerts = []
    for s in stats:
        vals = [_to_float(s[f"分区{i}行数"]) for i in range(int(s["分区数量"]))]
        if vals:
            mx = max(vals)
            mn = min(vals)
            skew = (mx - mn) / statistics.mean(vals) if statistics.mean(vals) > 0 else 0.0
            if skew > 1.0 or s.get("再平衡建议"):
                bp_alerts.append({"分区键": s["分区键"], "歪斜度": round(skew, 2), "热点": s["热点分区"], "建议": "触发再平衡"})

    result = {"partition_count": len(stats), "backpressure_alerts": bp_alerts, "alert_count": len(bp_alerts)}
    _wr("partition_backpressure", {"source": ["grp_partition"], "target": "backpressure", "payload": result})
    return result


# Pipeline 7: checkpoint -> state_store -> schema_evolve
def run_checkpoint_state_pipeline() -> dict:
    """检查点与状态一致性检查。"""
    ckpts = [r for r in store.rows("checkpoint")]
    states = [r for r in store.rows("state_store")]
    schemas = [r for r in store.rows("schema_evolve")]

    ckpt_available = sum(1 for c in ckpts if c["status"] == "可用")
    state_total = len(states)
    schema_pending = sum(1 for s in schemas if s["status"] == "待执行")

    consistency = {
        "checkpoint_available": ckpt_available,
        "checkpoint_total": len(ckpts),
        "state_count": state_total,
        "schema_pending": schema_pending,
        "health": "healthy" if ckpt_available >= 1 and schema_pending == 0 else "degraded",
    }
    result = consistency
    _wr("checkpoint_state", {"source": ["checkpoint", "state_store"], "target": "schema_evolve", "payload": result})
    return result


def run_all_pipelines() -> dict:
    return {
        "source_cdc": run_source_cdc_pipeline(),
        "window_metric": run_window_metric_pipeline(),
        "rule_router": run_rule_router_pipeline(),
        "metric_sink": run_metric_sink_pipeline(),
        "quality_scan": run_quality_scan(),
        "partition_bp": run_partition_backpressure_pipeline(),
        "checkpoint_state": run_checkpoint_state_pipeline(),
    }


def list_pipeline_results() -> list[dict]:
    return [dict(r) for r in store.rows("_pipeline_results")]