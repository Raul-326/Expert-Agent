from pydantic import BaseModel
from typing import List, Optional
from datetime import date

class ProjectOverviewResponse(BaseModel):
    project_group_id: int
    project_name: str
    poc_name: Optional[str] = None
    run_count: int
    date: date

class ProjectListResponse(BaseModel):
    data: List[ProjectOverviewResponse]

class PersonMetricItem(BaseModel):
    person_name: str
    role: str
    volume_total: int
    inspected_total: int
    pass_total: int

class ProjectPeopleResponse(BaseModel):
    data: List[PersonMetricItem]
