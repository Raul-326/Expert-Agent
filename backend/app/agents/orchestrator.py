import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from app.agents.data_fetcher import DataFetcherAgent
from app.agents.evaluator import EvaluatorAgent
from app.agents.database_writer import DatabaseWriterAgent
from workflow_feishu import (
    resolve_feishu_access_token,
    resolve_spreadsheet_token_from_url,   # 支持 wiki URL 格式
)

def ProcessWorkflow(
    feishu_url: str,
    user_access_token: str,
    sheet_name: str = None,
    project_group_name: str = "Default Project",
    poc_name: str = None,
    difficulty_coef: float = 1.0
) -> dict:
    """
    核心调度函数，供 FastAPI 调用，协调各项子 Agent 的工作。
    支持飞书 sheets 和 wiki?sheet= 两种 URL 格式。
    """
    try:
        # 0. 鉴权 & 解析 spreadsheet_token（支持 wiki 格式）
        token = resolve_feishu_access_token(
            auth_mode="user",
            user_access_token=user_access_token
        )
        spreadsheet_token = resolve_spreadsheet_token_from_url(feishu_url, token)

        # 1. 启动 DataFetcherAgent 拉取并清洗数据
        fetcher = DataFetcherAgent(feishu_url, user_access_token)
        rows = fetcher.fetch(sheet_name)

        # 2. 启动 EvaluatorAgent 执行评价和大模型打分算分流程
        evaluator = EvaluatorAgent()
        eval_result = evaluator.evaluate(rows)
        stats = eval_result["stats"]
        schema_type = eval_result["schema_type"]

        # 3. 启动 DatabaseWriterAgent 持久化到新 ORM 数据库
        writer = DatabaseWriterAgent()
        run_id = writer.write(
            project_group_name=project_group_name,
            spreadsheet_token=spreadsheet_token,
            poc_name=poc_name,
            stats=stats,
            difficulty_coef=difficulty_coef
        )

        return {
            "status": "success",
            "run_id": run_id,
            "message": f"Workflow processed successfully, run_id={run_id}"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
