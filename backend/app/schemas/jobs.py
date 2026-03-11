from pydantic import BaseModel
from typing import Optional

class JobComputeRequest(BaseModel):
    source_url: str
    user_access_token: str
    sheet_name: Optional[str] = None
    project_group_name: Optional[str] = "Default Project"
    poc_name: Optional[str] = None
