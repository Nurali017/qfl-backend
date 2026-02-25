from fastapi import APIRouter

from app.api.games.router import router as _router_base
from app.api.games.stats import router as _router_stats
from app.api.games.lineup import router as _router_lineup

router = APIRouter()
router.include_router(_router_base)
router.include_router(_router_stats)
router.include_router(_router_lineup)
