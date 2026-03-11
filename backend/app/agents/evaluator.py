import pandas as pd
from typing import List, Dict, Any

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workflow_feishu import (
    intelligent_column_mapping,
    calculate_accuracy_workflow
)

class EvaluatorAgent:
    def __init__(self):
        pass

    def evaluate(self, rows: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        抽象与大模型打分的流程以及计算准确率的流程。
        输入行数据字典，输出各角色（初标/质检/POC）的数据统计 DataFrame。
        """
        if not rows:
            return {"annotator_stats": pd.DataFrame(), "qa_stats": pd.DataFrame(), "poc_stats": pd.DataFrame()}

        df = pd.DataFrame(rows)

        # 1. 智能推断列映射
        actual_columns = df.columns.tolist()
        column_mapping = intelligent_column_mapping(actual_columns, df=df)

        # 2. 调用原先的大模型打分和准确率计算的核心逻辑
        annotator_stats, qa_stats, poc_stats = calculate_accuracy_workflow(
            df=df,
            column_mapping=column_mapping
        )

        return {
            "annotator_stats": annotator_stats,
            "qa_stats": qa_stats,
            "poc_stats": poc_stats
        }
