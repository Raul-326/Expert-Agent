from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from app.models.base import ProjectGroup, ProjectGroupOverride, Run, PersonMetrics, get_session
from app.core.personnel import personnel_manager
from typing import Any, Dict, Iterable, List, Optional

router = APIRouter()
ACCURACY_ROLES = {"annotator", "初标", "质检", "qa"}


def _latest_runs_by_group(session: Session) -> Dict[int, Run]:
    rows = session.exec(
        select(Run).order_by(Run.project_group_id, Run.run_at.desc(), Run.id.desc())
    ).all()
    latest: Dict[int, Run] = {}
    for run in rows:
        if run.project_group_id not in latest:
            latest[run.project_group_id] = run
    return latest


def _load_metrics_for_run(session: Session, run_id: int) -> List[PersonMetrics]:
    return session.exec(
        select(PersonMetrics).where(PersonMetrics.run_id == run_id)
    ).all()


def _safe_number(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _weighted_accuracy_from_rows(rows: Iterable[PersonMetrics]) -> float:
    inspected_total = 0.0
    pass_total = 0.0
    for row in rows:
        if row.role not in ACCURACY_ROLES:
            continue
        inspected_total += _safe_number(row.inspected_count)
        pass_total += _safe_number(row.pass_count)
    if inspected_total <= 0:
        return 0.0
    return pass_total / inspected_total


def _active_override_map(
    session: Session,
    project_group_id: int,
) -> Dict[tuple[str, str, str], ProjectGroupOverride]:
    rows = session.exec(
        select(ProjectGroupOverride).where(
            ProjectGroupOverride.project_group_id == project_group_id,
            ProjectGroupOverride.is_active == True,
        )
    ).all()
    lookup: Dict[tuple[str, str, str], ProjectGroupOverride] = {}
    for row in rows:
        key = (
            row.person_name or "",
            row.role or "",
            row.metric_key or "",
        )
        lookup[key] = row
    return lookup


def _apply_override(metric: PersonMetrics, override_map: Dict[tuple[str, str, str], ProjectGroupOverride]) -> Dict[str, Any]:
    item = {
        "person_name": metric.person_name,
        "role": metric.role,
        "volume": int(metric.volume or 0),
        "inspected_count": int(metric.inspected_count or 0),
        "pass_count": int(metric.pass_count or 0),
    }
    for field in ("volume", "inspected_count", "pass_count"):
        override = override_map.get((item["person_name"], item["role"], field))
        if override is not None:
            item[field] = int(float(override.override_value))
    if item["inspected_count"] > 0:
        item["accuracy"] = item["pass_count"] / item["inspected_count"]
    else:
        item["accuracy"] = None
    return item

@router.get("/")
def list_projects(session: Session = Depends(get_session)):
    """
    Boss 面板专用查大盘总揽的接口。
    """
    project_groups = session.exec(select(ProjectGroup)).all()
    latest_runs = _latest_runs_by_group(session)

    results = []
    for pg in project_groups:
        latest_run = latest_runs.get(pg.id)
        metrics_rows: List[Dict[str, Any]] = []
        if latest_run is not None:
            override_map = _active_override_map(session, pg.id)
            metrics_rows = [
                _apply_override(row, override_map)
                for row in _load_metrics_for_run(session, latest_run.id)
            ]

        total_volume = sum(int(row["volume"]) for row in metrics_rows)
        person_count = len({row["person_name"] for row in metrics_rows if row["person_name"]})
        overall_accuracy = _weighted_accuracy_from_rows(
            [
                PersonMetrics(
                    run_id=latest_run.id if latest_run else 0,
                    person_name=row["person_name"],
                    role=row["role"],
                    volume=row["volume"],
                    inspected_count=row["inspected_count"],
                    pass_count=row["pass_count"],
                    accuracy=row["accuracy"],
                )
                for row in metrics_rows
            ]
        )

        results.append({
            "project_group_id": pg.id,
            "project_name": pg.project_group_name,
            "poc_name": pg.poc_name or "-",
            "date": (latest_run.run_at if latest_run else pg.created_at).date().isoformat(),
            "person_count": person_count,
            "total_volume": total_volume,
            "overall_accuracy": round(overall_accuracy, 4),
        })

    return {"data": results}

@router.get("/{project_id}/detail")
def get_project_detail(project_id: int, session: Session = Depends(get_session)):
    """
    获取项目详情，包括基本信息和人员明细。
    """
    pg = session.get(ProjectGroup, project_id)
    if not pg:
        raise HTTPException(status_code=404, detail="Project not found")

    latest_run = _latest_runs_by_group(session).get(project_id)
    if latest_run is None:
        return {
            "project_name": pg.project_group_name,
            "poc_name": pg.poc_name,
            "created_at": pg.created_at.isoformat(),
            "people": [],
        }

    override_map = _active_override_map(session, project_id)
    raw_rows = [
        _apply_override(row, override_map)
        for row in _load_metrics_for_run(session, latest_run.id)
    ]
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in raw_rows:
        key = (row["person_name"], row["role"])
        item = grouped.setdefault(
            key,
            {
                "person_name": row["person_name"],
                "role": row["role"],
                "volume_total": 0,
                "inspected_total": 0,
                "pass_total": 0,
            },
        )
        item["volume_total"] += int(row["volume"])
        item["inspected_total"] += int(row["inspected_count"])
        item["pass_total"] += int(row["pass_count"])

    people_list = []
    for row in grouped.values():
        accuracy = None
        if row["inspected_total"] > 0:
            accuracy = row["pass_total"] / row["inspected_total"]
        people_list.append({
            "person_name": row["person_name"],
            "role": row["role"],
            "volume_total": row["volume_total"],
            "inspected_total": row["inspected_total"],
            "pass_total": row["pass_total"],
            "accuracy": round(float(accuracy), 4) if accuracy is not None else None,
        })
    people_list.sort(key=lambda item: (-item["volume_total"], item["person_name"], item["role"]))

    return {
        "project_name": pg.project_group_name,
        "poc_name": pg.poc_name,
        "created_at": latest_run.run_at.isoformat(),
        "people": people_list,
    }

@router.get("/people/search")
def search_people(keyword: str = None, session: Session = Depends(get_session)):
    """
    搜索所有人员并返回产出及准确率统计。
    """
    latest_runs = _latest_runs_by_group(session)
    target_runs = list(latest_runs.values())

    search_term: Optional[str] = None
    if keyword:
        resolved_name = personnel_manager.resolve_name(keyword)
        search_term = (resolved_name if resolved_name else keyword).strip().lower()

    grouped: Dict[str, Dict[str, Any]] = {}
    for run in target_runs:
        override_map = _active_override_map(session, run.project_group_id)
        for row in _load_metrics_for_run(session, run.id):
            metric = _apply_override(row, override_map)
            person_name = metric["person_name"]
            if not person_name:
                continue
            if search_term and search_term not in person_name.lower():
                continue

            item = grouped.setdefault(
                person_name,
                {
                    "person_name": person_name,
                    "roles": set(),
                    "project_groups": set(),
                    "volume_total": 0,
                    "inspected_total": 0,
                    "pass_total": 0,
                },
            )
            item["roles"].add(metric["role"])
            item["project_groups"].add(run.project_group_id)
            item["volume_total"] += int(metric["volume"])
            item["inspected_total"] += int(metric["inspected_count"])
            item["pass_total"] += int(metric["pass_count"])

    results = []
    for row in grouped.values():
        accuracy = 0.0
        if row["inspected_total"] > 0:
            accuracy = row["pass_total"] / row["inspected_total"]
        results.append({
            "person_name": row["person_name"],
            "roles": ", ".join(sorted(row["roles"])),
            "project_count": len(row["project_groups"]),
            "volume_total": row["volume_total"],
            "inspected_total": row["inspected_total"],
            "pass_total": row["pass_total"],
            "accuracy": round(float(accuracy), 4),
        })
    results.sort(key=lambda item: (-item["volume_total"], item["person_name"]))
    return {"data": results}

@router.get("/people/{name}/detail")
def get_person_detail(name: str, session: Session = Depends(get_session)):
    """
    特定人员在所有项目中的表现趋势。
    """
    latest_runs = _latest_runs_by_group(session)
    projects = []
    for project_group_id, run in latest_runs.items():
        override_map = _active_override_map(session, project_group_id)
        project_group = session.get(ProjectGroup, project_group_id)
        if project_group is None:
            continue
        for row in _load_metrics_for_run(session, run.id):
            metric = _apply_override(row, override_map)
            if metric["person_name"] != name:
                continue
            accuracy = metric["pass_count"] / metric["inspected_count"] if metric["inspected_count"] > 0 else None
            projects.append({
                "project_name": project_group.project_group_name,
                "role": metric["role"],
                "volume": metric["volume"],
                "inspected": metric["inspected_count"],
                "passed": metric["pass_count"],
                "accuracy": round(float(accuracy), 4) if accuracy is not None else None,
                "date": run.run_at.date().isoformat(),
            })
    projects.sort(key=lambda item: (item["date"], item["project_name"], item["role"]))

    return {
        "person_name": name,
        "projects": projects,
    }

@router.post("/{project_id}/override")
def override_metric(
    project_id: int, 
    person_name: str, 
    metric_key: str, 
    value: float, 
    reason: str,
    operator: str = "Admin",
    session: Session = Depends(get_session)
):
    """
    人工覆盖/修订某个人的指标数据，并记录审计日志。
    """
    # 1. 查找对应的指标记录 (取最近一次 Run 的)
    from app.models.base import ProjectGroupOverride, AuditLog
    
    # 记录审计日志
    audit = AuditLog(
        table_name="person_metrics",
        record_id=project_id,
        action="OVERRIDE",
        operator=operator,
        reason=reason,
        new_value=f"{person_name}.{metric_key}={value}"
    )
    session.add(audit)
    
    # 记录覆盖配置
    override = ProjectGroupOverride(
        project_group_id=project_id,
        person_name=person_name,
        role="unknown", # 简化处理
        metric_key=metric_key,
        override_value=value,
        reason=reason,
        updated_by=operator
    )
    session.add(override)
    session.commit()
    
    return {"status": "success", "message": "指标已成功覆盖，并已记录审计日志。"}
