from typing import Optional, List, Any
from pydantic import BaseModel
from datetime import datetime

class OverrideCreate(BaseModel):
    person_name: str
    role: str
    metric_key: str
    override_value: float
    reason: str

class AuditLogResponse(BaseModel):
    id: int
    table_name: str
    record_id: int
    action: str
    operator: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    reason: Optional[str] = None
    created_at: datetime

class PersonMetricResponse(BaseModel):
    person_name: str
    role: str
    volume_total: int
    inspected_total: int
    pass_total: int
    overall_accuracy: Optional[float] = None
    overall_weighted_accuracy: Optional[float] = None
