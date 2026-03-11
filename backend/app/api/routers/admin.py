from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select
from typing import List, Any
from app.models.base import ProjectGroupOverride, AuditLog, get_session
from app.schemas.admin import OverrideCreate, AuditLogResponse

router = APIRouter()

def get_current_admin():
    # Placeholder for actual authentication
    return "admin_user"

@router.post("/projects/{project_group_id}/overrides", response_model=dict)
def create_override(
    project_group_id: int, 
    override_data: OverrideCreate, 
    session: Session = Depends(get_session),
    admin_user: str = Depends(get_current_admin)
):
    """
    Admin 面板专用：修改项目内某人某角色的准去率等指标（行内编辑写回）
    """
    # 查找是否有现存的活跃 override，使其失效
    statement = select(ProjectGroupOverride).where(
        ProjectGroupOverride.project_group_id == project_group_id,
        ProjectGroupOverride.person_name == override_data.person_name,
        ProjectGroupOverride.role == override_data.role,
        ProjectGroupOverride.metric_key == override_data.metric_key,
        ProjectGroupOverride.is_active == True
    )
    existing_ov = session.exec(statement).first()
    
    old_val = None
    if existing_ov:
        old_val = str(existing_ov.override_value)
        existing_ov.is_active = False
        session.add(existing_ov)
    
    # 创建新 override
    new_override = ProjectGroupOverride(
        project_group_id=project_group_id,
        person_name=override_data.person_name,
        role=override_data.role,
        metric_key=override_data.metric_key,
        override_value=override_data.override_value,
        reason=override_data.reason,
        updated_by=admin_user
    )
    session.add(new_override)
    session.commit()
    session.refresh(new_override)
    
    # 插入审计日志
    audit = AuditLog(
        table_name="project_group_overrides",
        record_id=new_override.id,
        action="UPDATE" if old_val else "INSERT",
        operator=admin_user,
        old_value=old_val,
        new_value=str(override_data.override_value),
        reason=override_data.reason
    )
    session.add(audit)
    session.commit()

    return {"status": "success", "override_id": new_override.id}

@router.get("/audit_logs", response_model=List[AuditLogResponse])
def get_audit_logs(limit: int = 50, session: Session = Depends(get_session)):
    """
    Admin 面板专用：查看操作审计日志
    """
    statement = select(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit)
    logs = session.exec(statement).all()
    return logs
