"""流式质检接口：维护质检规则，覆盖启用规则、关闭规则、确认告警等动作。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.data_quality import DataQualityService
from app.services.quality_audit import ExportInterrupted, quality_audit

router = APIRouter(prefix="/api/data_quality", tags=["流式质检"])

service = DataQualityService()

LIST_FIELDS = ["规则编号", "关联流", "检查项", "上限阈值", "下限阈值", "告警级别", "统计窗口", "连续异常"]
STATUSES = ["未启用", "运行中", "已告警", "已关闭"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="未启用、运行中、已告警、已关闭"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、规则编号关键字与状态过滤流式质检列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。

    列顺序保持历史报告原样；质量样本、执行批次与当时阈值由阈值审计导出包
    （``/audit/executions/{execution_id}/package``）承载，避免改动旧报表口径。
    """
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="data_quality.csv"'})


# ---------------------------------------------------------------------------
# 阈值审计：规则阈值版本、执行批次、证据包、复核清单
# 这些路由都声明在 /{entry_id} 之前，路径分段更多，不会被单条明细路由吞掉。
# ---------------------------------------------------------------------------
@router.get("/audit/rules")
def audit_rules() -> dict[str, Any]:
    """可审计的质检规则及其当前生效阈值。"""
    return {"items": quality_audit.list_rules()}


@router.get("/audit/rules/{rule_id}/thresholds")
def audit_threshold_versions(rule_id: str) -> dict[str, Any]:
    """阈值的全部版本；旧版本只标记失效，内容保持原样供历史对时。"""
    versions = quality_audit.threshold_versions(rule_id)
    if not versions:
        raise HTTPException(status_code=404, detail=f"质检规则 {rule_id} 没有阈值版本")
    return {"rule_id": rule_id, "items": versions}


@router.post("/audit/rules/{rule_id}/thresholds")
def audit_set_threshold(rule_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """调整阈值：追加新版本而不是就地改写，旧判断仍可按旧版本解释。"""
    if quality_audit.current_threshold(rule_id) is None:
        raise HTTPException(status_code=404, detail=f"质检规则 {rule_id} 没有可用阈值")
    try:
        row = quality_audit.set_threshold(
            rule_id,
            upper=payload.get("upper", payload.get("上限阈值")),
            lower=payload.get("lower", payload.get("下限阈值")),
            reason=str(payload.get("reason") or payload.get("原因") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "message": f"已生成阈值版本 {row['version']}", "threshold": row}


@router.get("/audit/samples")
def audit_samples() -> dict[str, Any]:
    """质量样本及样本来路、采集时间；缺测样本带缺测原因。"""
    samples = quality_audit.list_samples()
    return {
        "items": samples,
        "total": len(samples),
        "missing": sum(1 for row in samples if row.get("value") is None),
    }


@router.get("/audit/executions")
def audit_executions() -> dict[str, Any]:
    return {"items": quality_audit.list_executions()}


@router.post("/audit/executions")
def audit_create_execution(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """创建执行批次：把当时生效的阈值冻结进快照，后续改阈值不影响本批证据。"""
    body = payload or {}
    try:
        execution = quality_audit.create_execution(
            rule_ids=body.get("rule_ids"),
            cutoff_at=body.get("cutoff_at"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "execution": quality_audit.public_execution(execution)}


@router.get("/audit/executions/{execution_id}")
def audit_execution(execution_id: str) -> dict[str, Any]:
    execution = quality_audit.get_execution(execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail=f"执行批次 {execution_id} 不存在")
    return quality_audit.public_execution(execution)


@router.post("/audit/executions/{execution_id}/seal")
def audit_seal(execution_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """封包：固化截止前到达的样本；重复封包不会追加重复行。"""
    body = payload or {}
    execution = quality_audit.seal(execution_id, cutoff_at=body.get("cutoff_at"))
    if execution is None:
        raise HTTPException(status_code=404, detail=f"执行批次 {execution_id} 不存在")
    return {
        "ok": True,
        "execution": quality_audit.public_execution(execution),
        "manifest": quality_audit.manifest(execution_id),
    }


@router.post("/audit/executions/{execution_id}/samples")
def audit_add_sample(execution_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """补录同一执行批次的迟到样本：出包更早、证据更全在这里取舍。

    迟到样本以「迟到=是」「是否计入统计=否」单独追加，不会把同一次执行的
    审计行写重，也不会被算成合格记录。
    """
    try:
        row = quality_audit.add_sample(execution_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0] if exc.args else exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "message": "迟到样本已补录", "row": row}


@router.get("/audit/executions/{execution_id}/manifest")
def audit_manifest(execution_id: str) -> dict[str, Any]:
    manifest = quality_audit.manifest(execution_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"执行批次 {execution_id} 不存在")
    return manifest


@router.get("/audit/executions/{execution_id}/package")
def audit_package(
    execution_id: str,
    fmt: str = Query(default="csv", pattern="^(csv|json)$"),
    interrupt_after: int | None = Query(default=None, ge=0, description="仅用于演练落盘中断"),
    new_version: bool = Query(default=True, description="重试时是否递增包版本"),
) -> Any:
    """下载阈值审计导出包。

    包内每行都有采集时间、所属批次与证据位置；缺测样本单独标注原因。
    ``interrupt_after=N`` 模拟落盘中断：已写行进入归档，返回 503 并带断点信息；
    再次请求会从断点继续、不重复写同一行，阈值仍按原版本读取。
    """
    if quality_audit.get_execution(execution_id) is None:
        raise HTTPException(status_code=404, detail=f"执行批次 {execution_id} 不存在")
    try:
        package = quality_audit.render_package(
            execution_id, interrupt_after=interrupt_after, fmt=fmt, increment_version=new_version)
    except ExportInterrupted as exc:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": str(exc),
                "execution_id": exc.execution_id,
                "written_rows": exc.written_rows,
                "total_rows": exc.total_rows,
                "resume_hint": f"再次请求 /api/data_quality/audit/executions/{exc.execution_id}/package 即可续写",
            },
        )
    if fmt == "json":
        return JSONResponse(content=package["content"])
    filename = f"{execution_id}-package-v{package['package_version']}.csv"
    return StreamingResponse(
        iter([package["content"]]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.post("/audit/executions/{execution_id}/replay")
def audit_replay(execution_id: str) -> dict[str, Any]:
    """管道编排：读审计包并实际重算校验，复核结果回写审计清单。"""
    try:
        result = quality_audit.replay(execution_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0] if exc.args else exc)) from exc
    return {"ok": True, "result": result}


@router.get("/audit/reviews")
def audit_reviews() -> dict[str, Any]:
    """审计清单：每次执行的最新复核结果与导出时的证据位置。"""
    return {"items": quality_audit.list_reviews()}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条质检规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"质检规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条质检规则，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="质检规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条质检规则执行启用规则、关闭规则、确认告警；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
