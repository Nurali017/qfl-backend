from fastapi import APIRouter

from app.api.v2.stats import router as stats_router

router = APIRouter()
router.include_router(stats_router)
