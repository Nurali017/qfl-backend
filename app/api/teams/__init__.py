from fastapi import APIRouter

from app.api.teams.router import router as _core_router
from app.api.teams.overview import router as _overview_router
from app.api.teams.stats import router as _stats_router
from app.api.teams.head_to_head import router as _h2h_router

router = APIRouter()
router.include_router(_core_router)
router.include_router(_overview_router)
router.include_router(_stats_router)
router.include_router(_h2h_router)
