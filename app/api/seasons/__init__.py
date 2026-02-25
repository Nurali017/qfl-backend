"""Seasons API package — assembles sub-routers into a single router."""

from fastapi import APIRouter

from app.api.seasons.router import router as _base_router
from app.api.seasons.table import router as _table_router
from app.api.seasons.stats import router as _stats_router
from app.api.seasons.games import router as _games_router

# Re-export helpers used by other modules (e.g. app.api.cup)
from app.api.seasons.router import (  # noqa: F401
    _build_season_response,
    GOAL_PERIOD_LABELS,
    _get_goal_period_index,
)

# Re-export calculate_dynamic_table from its new home for backward compatibility
from app.services.standings import calculate_dynamic_table  # noqa: F401

router = APIRouter()
router.include_router(_base_router)
router.include_router(_table_router)
router.include_router(_stats_router)
router.include_router(_games_router)
