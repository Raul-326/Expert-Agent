import pandas as pd
from typing import List, Dict, Any

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workflow_feishu import (
    intelligent_column_mapping,
    calculate_accuracy_workflow,
    detect_back_to_back_schema,
    calculate_back_to_back_annotator_stats
)

class EvaluatorAgent:
    def __init__(self):
        pass

    def evaluate(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        核心评价逻辑：
        1. 自动检测表格类型（普通 vs 背靠背双盲）
        2. 调用对应的算分算法
        """
        if not rows:
            return {
                "stats": {"annotator_stats": pd.DataFrame(), "qa_stats": pd.DataFrame(), "poc_stats": pd.DataFrame()},
                "schema_type": "unknown"
            }

        df = pd.DataFrame(rows)
        
        # 1. 自动判定 Schema 类型
        is_b2b = detect_back_to_back_schema(df)
        schema_type = "b2b" if is_b2b else "normal"

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

        return {
            "stats": {
                "annotator_stats": annotator_stats,
                "qa_stats": qa_stats,
                "poc_stats": poc_stats
            },
            "schema_type": schema_type
        }
