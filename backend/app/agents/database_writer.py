import pandas as pd
from typing import Dict, Optional
from datetime import datetime

import sys
import os

from sqlmodel import Session, select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# 引入新的 ORM
from app.models.base import engine, ProjectGroup, Run, PersonMetrics

class DatabaseWriterAgent:
    def __init__(self):
        pass

    def write(self, project_group_name: str, spreadsheet_token: str, poc_name: Optional[str], stats: Dict[str, pd.DataFrame]) -> int:
        """
        负责拿到 Evaluator 给出的统计后，调用 ProjectGroup, Run 和 PersonMetrics 模型将数据存入数据库。
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

            # 3. 写入 PersonMetrics
            # 写初标人
            if not annotator_stats.empty:
                for _, row in annotator_stats.iterrows():
                    metric = PersonMetrics(
                        run_id=new_run.id,
                        person_name=str(row.get('初标人', 'Unknown')),
                        role="annotator",
                        volume=int(row.get('初标总产量', 0) or 0),
                        inspected_count=int(row.get('被抽检量', 0) or 0),
                        pass_count=int(row.get('被抽检通过量', 0) or 0),
                        accuracy=float(row.get('准确率_原始', 0.0)) if '准确率_原始' in row and not pd.isna(row['准确率_原始']) else None,
                        weighted_accuracy=None,
                        difficulty_coef=1.0
                    )
                    session.add(metric)

            # 写质检人
            if not qa_stats.empty:
                for _, row in qa_stats.iterrows():
                    metric = PersonMetrics(
                        run_id=new_run.id,
                        person_name=str(row.get('质检人', 'Unknown')),
                        role="qa",
                        volume=int(row.get('质检总产量', 0) or 0),
                        inspected_count=0,
                        pass_count=0,
                        accuracy=None,
                        weighted_accuracy=None,
                        difficulty_coef=1.0
                    )
                    session.add(metric)

            # 写 POC
            if not poc_stats.empty:
                for _, row in poc_stats.iterrows():
                    metric = PersonMetrics(
                        run_id=new_run.id,
                        person_name=str(row.get('POC 姓名', 'Unknown')),
                        role="poc",
                        volume=int(row.get('抽检产量', 0) or 0),
                        inspected_count=0,
                        pass_count=0,
                        accuracy=None,
                        weighted_accuracy=None,
                        difficulty_coef=1.0
                    )
                    session.add(metric)

            session.commit()
            return new_run.id
