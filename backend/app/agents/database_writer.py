import pandas as pd
from typing import Dict, Optional, Any, List
from datetime import datetime
import json

import sys
import os

from sqlmodel import Session, select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# 引入新的 ORM
from app.models.base import engine, ProjectGroup, Run, PersonMetrics, ProjectMetrics, ProjectSheet, PocScore
from app.core.personnel import personnel_manager
from panel_metrics import safe_float, recompute_weighted_accuracy

class DatabaseWriterAgent:
    def __init__(self):
        pass

    def _build_project_metrics(
        self,
        person_metrics: List[Dict[str, Any]],
        difficulty_coef: float,
    ) -> List[ProjectMetrics]:
        grouped: Dict[str, Dict[str, float]] = {}
        overall_volume = 0.0
        overall_inspected = 0.0
        overall_pass = 0.0

        for row in person_metrics:
            role = row["role"]
            item = grouped.setdefault(
                role,
                {
                    "volume_total": 0.0,
                    "inspected_total": 0.0,
                    "pass_total": 0.0,
                },
            )
            item["volume_total"] += float(row["volume"])
            item["inspected_total"] += float(row["inspected_count"])
            item["pass_total"] += float(row["pass_count"])

            if role in {"annotator", "qa", "初标", "质检"}:
                overall_volume += float(row["volume"])
                overall_inspected += float(row["inspected_count"])
                overall_pass += float(row["pass_count"])

        metric_rows: List[ProjectMetrics] = []
        for role, item in grouped.items():
            accuracy = (item["pass_total"] / item["inspected_total"]) if item["inspected_total"] > 0 else None
            metric_rows.append(
                ProjectMetrics(
                    run_id=0,
                    metric_group=role,
                    volume_total=int(item["volume_total"]),
                    inspected_total=int(item["inspected_total"]),
                    pass_total=int(item["pass_total"]),
                    accuracy=accuracy,
                    weighted_accuracy=recompute_weighted_accuracy(accuracy, difficulty_coef),
                )
            )

        overall_accuracy = (overall_pass / overall_inspected) if overall_inspected > 0 else None
        metric_rows.append(
            ProjectMetrics(
                run_id=0,
                metric_group="整体",
                volume_total=int(overall_volume),
                inspected_total=int(overall_inspected),
                pass_total=int(overall_pass),
                accuracy=overall_accuracy,
                weighted_accuracy=recompute_weighted_accuracy(overall_accuracy, difficulty_coef),
            )
        )
        return metric_rows

    def write(
        self,
        project_group_name: str,
        spreadsheet_token: str,
        poc_name: Optional[str],
        stats: Dict[str, pd.DataFrame],
        sheet_ref: str = "",
        sheet_title: str = "",
        schema_type: str = "unknown",
        poc_score: Optional[Dict[str, Any]] = None,
        difficulty_coef: float = 1.0,
    ) -> Dict[str, Optional[int]]:
        """
        负责拿到 Evaluator 给出的统计后，将项目、run、sheet、人员指标和项目汇总写入数据库。
        """
        annotator_stats = stats.get("annotator_stats", pd.DataFrame())
        qa_stats = stats.get("qa_stats", pd.DataFrame())
        poc_stats = stats.get("poc_stats", pd.DataFrame())

        with Session(engine) as session:
            # 1. 查询或创建 ProjectGroup
            statement = select(ProjectGroup).where(ProjectGroup.spreadsheet_token == spreadsheet_token)
            group = session.exec(statement).first()
            if not group:
                group = ProjectGroup(
                    project_group_name=project_group_name,
                    spreadsheet_token=spreadsheet_token,
                    poc_name=poc_name
                )
                session.add(group)
                session.commit()
                session.refresh(group)
            else:
                if poc_name and group.poc_name != poc_name:
                    group.poc_name = poc_name
                    session.add(group)
                    session.commit()
                    session.refresh(group)

            # 2. 创建 Run
            new_run = Run(
                project_group_id=group.id,
                batch_project_name=f"Run-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            )
            session.add(new_run)
            session.commit()
            session.refresh(new_run)

            if sheet_ref or sheet_title:
                session.add(
                    ProjectSheet(
                        run_id=new_run.id,
                        sheet_ref=sheet_ref or sheet_title or spreadsheet_token,
                        sheet_title=sheet_title or sheet_ref or None,
                        schema_type=schema_type or "unknown",
                    )
                )

            inserted_metrics: List[Dict[str, Any]] = []

            # 3. 写入 PersonMetrics
            if not annotator_stats.empty:
                for _, row in annotator_stats.iterrows():
                    raw_name = str(row.get('初标人', 'Unknown'))
                    full_name = personnel_manager.resolve_name(raw_name) or raw_name.strip()
                    if not full_name:
                        continue

                    inspected = safe_float(row.get('被质检数', 0))
                    passed = safe_float(row.get('质检通过数', 0))
                    acc = (passed / inspected) if (inspected and inspected > 0) else None
                    weighted_acc = recompute_weighted_accuracy(acc, difficulty_coef)
                    volume = int(safe_float(row.get('初标总产量', 0)) or 0)
                    inspected_count = int(inspected or 0)
                    pass_count = int(passed or 0)

                    metric = PersonMetrics(
                        run_id=new_run.id,
                        person_name=full_name,
                        role="annotator",
                        volume=volume,
                        inspected_count=inspected_count,
                        pass_count=pass_count,
                        accuracy=acc,
                        weighted_accuracy=weighted_acc,
                        difficulty_coef=difficulty_coef
                    )
                    session.add(metric)
                    inserted_metrics.append(
                        {
                            "role": "annotator",
                            "person_name": full_name,
                            "volume": volume,
                            "inspected_count": inspected_count,
                            "pass_count": pass_count,
                        }
                    )

            if not qa_stats.empty:
                for _, row in qa_stats.iterrows():
                    raw_name = str(row.get('质检人', 'Unknown'))
                    full_name = personnel_manager.resolve_name(raw_name) or raw_name.strip()
                    if not full_name:
                        continue

                    inspected = safe_float(row.get('被抽检数', 0))
                    passed = safe_float(row.get('抽检通过数', 0))
                    acc = (passed / inspected) if (inspected and inspected > 0) else None
                    weighted_acc = recompute_weighted_accuracy(acc, difficulty_coef)
                    volume = int(safe_float(row.get('质检总产量', 0)) or 0)
                    inspected_count = int(inspected or 0)
                    pass_count = int(passed or 0)

                    metric = PersonMetrics(
                        run_id=new_run.id,
                        person_name=full_name,
                        role="qa",
                        volume=volume,
                        inspected_count=inspected_count,
                        pass_count=pass_count,
                        accuracy=acc,
                        weighted_accuracy=weighted_acc,
                        difficulty_coef=difficulty_coef
                    )
                    session.add(metric)
                    inserted_metrics.append(
                        {
                            "role": "qa",
                            "person_name": full_name,
                            "volume": volume,
                            "inspected_count": inspected_count,
                            "pass_count": pass_count,
                        }
                    )

            if not poc_stats.empty:
                for _, row in poc_stats.iterrows():
                    raw_name = str(row.get('POC 姓名', 'Unknown'))
                    full_name = personnel_manager.resolve_name(raw_name) or raw_name.strip()
                    if not full_name:
                        continue
                    volume = int(safe_float(row.get('抽检产量', 0)) or 0)

                    metric = PersonMetrics(
                        run_id=new_run.id,
                        person_name=full_name,
                        role="poc",
                        volume=volume,
                        inspected_count=0,
                        pass_count=0,
                        accuracy=None,
                        weighted_accuracy=None,
                        difficulty_coef=difficulty_coef
                    )
                    session.add(metric)
                    inserted_metrics.append(
                        {
                            "role": "poc",
                            "person_name": full_name,
                            "volume": volume,
                            "inspected_count": 0,
                            "pass_count": 0,
                        }
                    )

            for metric_row in self._build_project_metrics(inserted_metrics, difficulty_coef):
                metric_row.run_id = new_run.id
                session.add(metric_row)

            poc_score_id: Optional[int] = None
            if poc_score:
                score_row = PocScore(
                    project_group_id=group.id,
                    run_id=new_run.id,
                    sop_source_type=poc_score.get("sop_source_type"),
                    model_name=poc_score.get("model_name"),
                    total_score=float(poc_score.get("total_score") or 0.0),
                    grade=poc_score.get("grade"),
                    sop_score=float(poc_score.get("sop_score") or 0.0),
                    sheet_score=float(poc_score.get("sheet_score") or 0.0),
                    project_owner=poc_score.get("project_owner") or poc_name,
                    details_json=json.dumps(poc_score.get("details_json") or {}, ensure_ascii=False),
                )
                session.add(score_row)
                session.flush()
                poc_score_id = score_row.id

            session.commit()
            return {
                "run_id": new_run.id,
                "poc_score_id": poc_score_id,
            }
