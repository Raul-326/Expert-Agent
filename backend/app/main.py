from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routers import api_router
from app.core.config import settings
from app.models.base import create_db_and_tables

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
def on_startup():
    import os
    # 同步配置到环境变量，确保 Agent 逻辑能读到
    if settings.ARK_API_KEY:
        os.environ["ARK_API_KEY"] = settings.ARK_API_KEY
    if settings.FEISHU_APP_ID:
        os.environ["FEISHU_APP_ID"] = settings.FEISHU_APP_ID
    if settings.FEISHU_APP_SECRET:
        os.environ["FEISHU_APP_SECRET"] = settings.FEISHU_APP_SECRET
    create_db_and_tables()

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "1.0.0", "message": "Multi-Agent System Backend"}
