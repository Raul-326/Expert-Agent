from fastapi import APIRouter
from . import projects, jobs, admin

api_router = APIRouter()
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])
