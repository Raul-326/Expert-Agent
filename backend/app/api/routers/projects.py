from sqlmodel import Session, select, func
from fastapi import APIRouter, Depends
from app.models.base import ProjectGroup, Run, PersonMetrics, get_session
from app.core.personnel import personnel_manager
from typing import List, Dict, Any

router = APIRouter()

@router.get("/")
def list_projects(session: Session = Depends(get_session)):
    """
    Boss 面板专用查大盘总揽的接口。
    """
    statement = select(ProjectGroup)
    project_groups = session.exec(statement).all()
    
    results = []
    for pg in project_groups:
        # 计算该项目组的总产量、人员数
        metrics_stmt = (
            select(
                func.sum(PersonMetrics.volume).label("total_volume"),
                func.count(func.distinct(PersonMetrics.person_name)).label("person_count"),
            )
            .join(Run, Run.id == PersonMetrics.run_id)
            .where(Run.project_group_id == pg.id)
        )
        metrics = session.exec(metrics_stmt).first()
        
        # 计算该项目组的平均准确率 (过滤掉 NULL 的情况)
        # 兼容多种角色名称 (初标, annotator, 质检, qa)
        acc_stmt = (
            select(func.avg(PersonMetrics.accuracy))
            .join(Run, Run.id == PersonMetrics.run_id)
            .where(
                Run.project_group_id == pg.id, 
                PersonMetrics.accuracy != None,
                PersonMetrics.role.in_(['annotator', '初标', '质检', 'qa'])
            )
        )
        avg_acc = session.exec(acc_stmt).first() or 0.0

        results.append({
            "project_group_id": pg.id,
            "project_name": pg.project_group_name,
            "poc_name": pg.poc_name or "-",
            "date": pg.created_at.date().isoformat(),
            "person_count": metrics.person_count if metrics else 0,
            "total_volume": metrics.total_volume if metrics else 0,
            "overall_accuracy": round(float(avg_acc), 4) if avg_acc else 0.0
        })

    return {"data": results}

@router.get("/{project_id}/detail")
def get_project_detail(project_id: int, session: Session = Depends(get_session)):
    """
    获取项目详情，包括基本信息和人员明细。
    """
    pg = session.get(ProjectGroup, project_id)
    if not pg:
        return {"error": "Project not found"}
    
    people_statement = (
        select(
            PersonMetrics.person_name,
            PersonMetrics.role,
            func.sum(PersonMetrics.volume).label('volume_total'),
            func.sum(PersonMetrics.inspected_count).label('inspected_total'),
            func.sum(PersonMetrics.pass_count).label('pass_total'),
            func.avg(PersonMetrics.accuracy).label('avg_accuracy')
        )
        .join(Run, Run.id == PersonMetrics.run_id)
        .where(Run.project_group_id == project_id)
        .group_by(PersonMetrics.person_name, PersonMetrics.role)
    )
    
    people_rows = session.exec(people_statement).all()
    people_list = []
    for row in people_rows:
        people_list.append({
            "person_name": row.person_name,
            "role": row.role,
            "volume_total": row.volume_total,
            "inspected_total": row.inspected_total,
            "pass_total": row.pass_total,
            "accuracy": round(float(row.avg_accuracy), 4) if row.avg_accuracy else None
        })
    
    return {
        "project_name": pg.project_group_name,
        "poc_name": pg.poc_name,
        "created_at": pg.created_at.isoformat(),
        "people": people_list
    }

@router.get("/people/search")
def search_people(keyword: str = None, session: Session = Depends(get_session)):
    """
    搜索所有人员并返回产出及准确率统计。
    """
    # 针对 PostgreSQL 使用 string_agg
    roles_func = func.string_agg(func.distinct(PersonMetrics.role), ', ')
    
    statement = (
        select(
            PersonMetrics.person_name,
            roles_func.label('roles'),
            func.count(func.distinct(Run.project_group_id)).label('project_count'),
            func.sum(PersonMetrics.volume).label('volume_total'),
            func.sum(PersonMetrics.inspected_count).label('inspected_total'),
            func.sum(PersonMetrics.pass_count).label('pass_total'),
            func.avg(PersonMetrics.accuracy).label('avg_accuracy')
        )
        .join(Run, Run.id == PersonMetrics.run_id)
        .group_by(PersonMetrics.person_name)
    )
    
    if keyword:
        # 如果关键词是别名，解析为全名搜索
        resolved_name = personnel_manager.resolve_name(keyword)
        search_term = resolved_name if resolved_name else keyword
        statement = statement.where(PersonMetrics.person_name.contains(search_term))
        
    rows = session.exec(statement).all()
    results = []
    for row in rows:
        results.append({
            "person_name": row.person_name,
            "roles": row.roles,
            "project_count": row.project_count,
            "volume_total": row.volume_total,
            "inspected_total": row.inspected_total,
            "pass_total": row.pass_total,
            "accuracy": round(float(row.avg_accuracy), 4) if row.avg_accuracy else 0.0
        })
    return {"data": results}

@router.get("/people/{name}/detail")
def get_person_detail(name: str, session: Session = Depends(get_session)):
    """
    特定人员在所有项目中的表现趋势。
    """
    statement = (
        select(
            ProjectGroup.project_group_name,
            PersonMetrics.role,
            PersonMetrics.volume,
            PersonMetrics.inspected_count,
            PersonMetrics.pass_count,
            PersonMetrics.accuracy,
            ProjectGroup.created_at
        )
        .join(Run, Run.id == PersonMetrics.run_id)
        .join(ProjectGroup, ProjectGroup.id == Run.project_group_id)
        .where(PersonMetrics.person_name == name)
        .order_by(ProjectGroup.created_at)
    )
    
    rows = session.exec(statement).all()
    projects = []
    for row in rows:
        projects.append({
            "project_name": row.project_group_name,
            "role": row.role,
            "volume": row.volume,
            "inspected": row.inspected_count,
            "passed": row.pass_count,
            "accuracy": round(float(row.accuracy), 4) if row.accuracy else None,
            "date": row.created_at.date().isoformat()
        })
    
    return {
        "person_name": name,
        "projects": projects
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
