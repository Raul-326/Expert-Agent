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
    create_db_and_tables()

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "1.0.0", "message": "Multi-Agent System Backend"}
