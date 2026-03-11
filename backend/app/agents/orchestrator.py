import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from app.agents.data_fetcher import DataFetcherAgent
from app.agents.evaluator import EvaluatorAgent
from app.agents.database_writer import DatabaseWriterAgent
from workflow_feishu import extract_spreadsheet_token_from_url

def ProcessWorkflow(
    feishu_url: str,
    user_access_token: str,
    sheet_name: str = None,
    project_group_name: str = "Default Project",
    poc_name: str = None
) -> dict:
    """
    核心调度函数，供 FastAPI 调用，协调各项子 Agent 的工作。
    """
    try:
        # 1. 启动 DataFetcherAgent 拉取并清洗数据
        fetcher = DataFetcherAgent(feishu_url, user_access_token)
        rows = fetcher.fetch(sheet_name)

        # 2. 启动 EvaluatorAgent 执行评价和大模型打分算分流程
        evaluator = EvaluatorAgent()
        stats = evaluator.evaluate(rows)

        # 3. 启动 DatabaseWriterAgent 持久化到新 ORM 数据库
        spreadsheet_token = extract_spreadsheet_token_from_url(feishu_url)
        writer = DatabaseWriterAgent()
        run_id = writer.write(
            project_group_name=project_group_name,
            spreadsheet_token=spreadsheet_token,
            poc_name=poc_name,
            stats=stats
        )

        return {
            "status": "success",
            "run_id": run_id,
            "message": "Workflow processed successfully"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
