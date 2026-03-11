from fastapi import APIRouter

router = APIRouter()

@router.post("/compute")
def trigger_compute():
    return {"status": "accepted", "job_id": "job_abc123"}
