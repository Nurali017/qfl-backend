from fastapi import APIRouter

from app.api.admin.auth import router as auth_router
from app.api.admin.users import router as users_router
from app.api.admin.news import router as news_router
from app.api.admin.pages import router as pages_router
from app.api.admin.players import router as players_router
from app.api.admin.files import router as files_router
from app.api.admin.ops import router as ops_router
from app.api.admin.championships import router as championships_router
from app.api.admin.stages import router as stages_router
from app.api.admin.clubs import router as clubs_router
from app.api.admin.cities import router as cities_router
from app.api.admin.partners import router as partners_router
from app.api.admin.season_participants import router as season_participants_router
from app.api.admin.team_of_week import router as team_of_week_router
from app.api.admin.seasons import router as seasons_router

router = APIRouter(prefix="/admin")
router.include_router(auth_router)
router.include_router(users_router)
router.include_router(news_router)
router.include_router(pages_router)
router.include_router(players_router)
router.include_router(files_router)
router.include_router(ops_router)
router.include_router(championships_router)
router.include_router(stages_router)
router.include_router(clubs_router)
router.include_router(cities_router)
router.include_router(partners_router)
router.include_router(season_participants_router)
router.include_router(team_of_week_router)
router.include_router(seasons_router)
