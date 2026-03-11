from sqlmodel import Session, select, func
from fastapi import APIRouter, Depends
from app.models.base import ProjectGroup, Run, PersonMetrics, get_session
from typing import List, Dict, Any

router = APIRouter()

@router.get("/")
def list_projects(session: Session = Depends(get_session)):
    """
    Boss 面板专用查大盘总揽的接口。
    代替原生 panel_db.py 里面复杂的 SQL 拼接，采用 SQLModel。
    """
    statement = select(ProjectGroup)
    project_groups = session.exec(statement).all()
    
    # 构建高层的大盘汇总 (简化版逻辑呈现)
    results = []
    for pg in project_groups:
        run_statement = select(func.count(Run.id)).where(Run.project_group_id == pg.id)
        run_count = session.exec(run_statement).one_or_none() or 0
        
        results.append({
            "project_group_id": pg.id,
            "project_name": pg.project_group_name,
            "poc_name": pg.poc_name,
            "run_count": run_count,
            "date": pg.created_at.date().isoformat()
        })

    return {"data": results}

@router.get("/{project_id}/people")
def get_project_people(project_id: int, session: Session = Depends(get_session)):
    """
    项目内人员产量的获取
    """
    statement = (
        select(
            PersonMetrics.person_name,
            PersonMetrics.role,
            func.sum(PersonMetrics.volume).label('volume_total'),
            func.sum(PersonMetrics.inspected_count).label('inspected_total'),
            func.sum(PersonMetrics.pass_count).label('pass_total'),
        )
        .join(Run, Run.id == PersonMetrics.run_id)
        .where(Run.project_group_id == project_id)
        .group_by(PersonMetrics.person_name, PersonMetrics.role)
    )
    
    rows = session.exec(statement).all()
    results = []
    for row in rows:
        results.append({
            "person_name": row.person_name,
            "role": row.role,
            "volume_total": row.volume_total,
            "inspected_total": row.inspected_total,
            "pass_total": row.pass_total,
        })
    return {"data": results}
