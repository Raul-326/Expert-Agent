from sqlmodel import SQLModel, Field, create_engine, Session
from datetime import datetime
from typing import Optional
from app.core.config import settings

engine = create_engine(settings.SQLITE_URL, echo=False)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

class ProjectGroupBase(SQLModel):
    project_group_name: str
    spreadsheet_token: str
    poc_name: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ProjectGroup(ProjectGroupBase, table=True):
    __tablename__ = "project_groups"
    id: Optional[int] = Field(default=None, primary_key=True)

class RunBase(SQLModel):
    project_group_id: int
    batch_project_name: Optional[str] = None
    batch_no: Optional[str] = None
    run_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "COMPLETED"

class Run(RunBase, table=True):
    __tablename__ = "runs"
    id: Optional[int] = Field(default=None, primary_key=True)

class ProjectSheetBase(SQLModel):
    run_id: int
    sheet_ref: str
    sheet_title: Optional[str] = None
    schema_type: Optional[str] = None

class ProjectSheet(ProjectSheetBase, table=True):
    __tablename__ = "project_sheets"
    id: Optional[int] = Field(default=None, primary_key=True)

class PersonMetricsBase(SQLModel):
    run_id: int
    person_name: str
    role: str
    volume: int
    inspected_count: int
    pass_count: int
    accuracy: Optional[float] = None
    weighted_accuracy: Optional[float] = None
    difficulty_coef: Optional[float] = 1.0

class PersonMetrics(PersonMetricsBase, table=True):
    __tablename__ = "person_metrics_base"
    id: Optional[int] = Field(default=None, primary_key=True)

class ProjectMetricsBase(SQLModel):
    run_id: int
    metric_group: str  # e.g., '整体' or '角色:初标'
    volume_total: int
    inspected_total: int
    pass_total: int
    accuracy: Optional[float] = None
    weighted_accuracy: Optional[float] = None

class ProjectMetrics(ProjectMetricsBase, table=True):
    __tablename__ = "project_metrics_base"
    id: Optional[int] = Field(default=None, primary_key=True)

class ProjectGroupOverrideBase(SQLModel):
    project_group_id: int
    person_name: str
    role: str
    metric_key: str
    override_value: float
    is_active: bool = True
    updated_by: str
    reason: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class ProjectGroupOverride(ProjectGroupOverrideBase, table=True):
    __tablename__ = "project_group_overrides"
    id: Optional[int] = Field(default=None, primary_key=True)

class PocScoreBase(SQLModel):
    project_group_id: int
    run_id: Optional[int] = None
    sop_source_type: Optional[str] = None
    model_name: Optional[str] = None
    total_score: float
    grade: Optional[str] = None
    sop_score: float
    sheet_score: float
    project_owner: Optional[str] = None
    details_json: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class PocScore(PocScoreBase, table=True):
    __tablename__ = "poc_scores"
    id: Optional[int] = Field(default=None, primary_key=True)

class AuditLogBase(SQLModel):
    table_name: str
    record_id: int
    action: str
    operator: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    reason: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class AuditLog(AuditLogBase, table=True):
    __tablename__ = "audit_logs"
    id: Optional[int] = Field(default=None, primary_key=True)
