from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import uuid


def _new_task_id() -> str:
    return str(uuid.uuid4())


@dataclass
class AgentTaskRequest:
    source_url: str
    sheet_refs: List[str] = field(default_factory=list)
    sop_url: str = ""
    manual_sop_score: Optional[float] = None
    poc_owner: str = ""
    result_target: Optional[Dict[str, Any]] = None
    flags: Dict[str, Any] = field(default_factory=dict)
    auth_mode: str = "user"
    user_access_token: str = ""
    db_path: str = "./metrics_panel.db"
    operator: str = "agent"
    task_id: str = field(default_factory=_new_task_id)


@dataclass
class SkillResult:
    skill_name: str
    status: str
    output: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    error: str = ""


@dataclass
class AgentRunResult:
    task_id: str
    run_ids: List[str] = field(default_factory=list)
    project_group_id: str = ""
    poc_score_id: Optional[int] = None
    writeback_status: str = "skipped"
    warnings: List[str] = field(default_factory=list)
    score_card: Dict[str, Any] = field(default_factory=dict)
