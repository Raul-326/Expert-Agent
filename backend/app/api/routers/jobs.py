from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.schemas.jobs import JobComputeRequest
from app.agents.orchestrator import ProcessWorkflow

router = APIRouter()

@router.post("/compute")
async def trigger_compute(request: JobComputeRequest, background_tasks: BackgroundTasks):
    """
    触发多 Agent 协作流水线。
    使用 FastAPI 的 BackgroundTasks 以后台异步方式运行，防止 HTTP 超时。
    """
    # 验证关键参数
    if not request.source_url or not request.user_access_token:
        raise HTTPException(status_code=400, detail="Missing source_url or user_access_token")

    # 定义异步运行函数（包裹多 Agent 逻辑）
    def run_agent_pipeline():
        result = ProcessWorkflow(
            feishu_url=request.source_url,
            user_access_token=request.user_access_token,
            sheet_name=request.sheet_name,
            project_group_name=request.project_group_name,
            poc_name=request.poc_name
        )
        print(f"Agent Pipeline Execution Finished: {result}")

    # 加入后台任务队列
    background_tasks.add_task(run_agent_pipeline)

    return {
        "status": "accepted",
        "message": "Multi-Agent pipeline has been triggered in background.",
        "project": request.project_group_name
    }
