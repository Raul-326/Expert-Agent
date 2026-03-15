import pandas as pd
from typing import List, Dict, Any, Optional

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from agent.skills import (
    detect_project_owner,
    poc_score_aggregate_skill,
    sheet_quality_skill,
    sop_quality_skill,
)
from workflow_feishu import (
    intelligent_column_mapping,
    calculate_accuracy_workflow,
    detect_back_to_back_schema,
    calculate_back_to_back_annotator_stats
)

class EvaluatorAgent:
    def __init__(self):
        pass

    def evaluate(
        self,
        rows: List[Dict[str, Any]],
        token: str = "",
        sop_url: str = "",
        manual_sop_score: Optional[float] = None,
        manual_project_owner: str = "",
    ) -> Dict[str, Any]:
        """
        核心评价逻辑：
        1. 自动检测表格类型（普通 vs 背靠背双盲）
        2. 调用对应的算分算法
        """
        if not rows:
            return {
                "stats": {"annotator_stats": pd.DataFrame(), "qa_stats": pd.DataFrame(), "poc_stats": pd.DataFrame()},
                "schema_type": "unknown",
                "mapping": {},
                "project_owner": manual_project_owner or "",
                "poc_score": None,
                "warnings": [],
            }

        df = pd.DataFrame(rows)
        warnings: List[str] = []
        
        # 1. 自动判定 Schema 类型
        is_b2b = detect_back_to_back_schema(df)
        schema_type = "b2b" if is_b2b else "normal"
        mapping: Dict[str, str] = {}

        if is_b2b:
            # 2a. 调用背靠背专用算法
            annotator_stats, qa_stats, poc_stats = calculate_back_to_back_annotator_stats(df)
        else:
            # 2b. 普通流水线算法：解析列名映射并计算
            mapping = intelligent_column_mapping(df.columns.tolist(), df=df)
            annotator_stats, qa_stats, poc_stats = calculate_accuracy_workflow(
                df=df,
                column_mapping=mapping
            )

        project_owner = detect_project_owner(
            raw_dfs=[df],
            pocs_frames=[poc_stats],
            manual_owner=manual_project_owner,
        )

        poc_score = None
        if sop_url or manual_sop_score is not None:
            try:
                sop_result = sop_quality_skill(
                    sop_url=sop_url,
                    token=token,
                    manual_sop_score=manual_sop_score,
                )
                sheet_result = sheet_quality_skill(
                    dfs=[df],
                    mappings=[mapping],
                )
                aggregate_result = poc_score_aggregate_skill(
                    sop_score=sop_result["sop_score"],
                    sheet_score=sheet_result["sheet_score"],
                    project_owner=project_owner,
                )
                poc_score = {
                    "project_owner": aggregate_result.get("project_owner") or project_owner,
                    "total_score": aggregate_result["poc_total_score"],
                    "grade": aggregate_result["grade"],
                    "sop_score": sop_result["sop_score"],
                    "sheet_score": sheet_result["sheet_score"],
                    "sop_source_type": sop_result.get("source_type") or "manual",
                    "model_name": sheet_result.get("model_name") or sop_result.get("model_name") or "",
                    "details_json": {
                        "sop_reason": sop_result.get("sop_reason", ""),
                        "sop_evidence": sop_result.get("sop_evidence", []),
                        "sheet_reason": sheet_result.get("sheet_reason", ""),
                        "sheet_evidence": sheet_result.get("sheet_evidence", []),
                        "summary": aggregate_result.get("summary", ""),
                    },
                }
            except Exception as exc:
                warnings.append(f"POC 评分失败: {exc}")

        return {
            "stats": {
                "annotator_stats": annotator_stats,
                "qa_stats": qa_stats,
                "poc_stats": poc_stats
            },
            "schema_type": schema_type,
            "mapping": mapping,
            "project_owner": project_owner,
            "poc_score": poc_score,
            "warnings": warnings,
        }
