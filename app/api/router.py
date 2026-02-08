from fastapi import APIRouter

from app.api.seasons import router as seasons_router
from app.api.teams import router as teams_router
from app.api.players import router as players_router
from app.api.games import router as games_router
from app.api.sync import router as sync_router
from app.api.live import router as live_router
from app.api.pages import router as pages_router
from app.api.news import router as news_router
from app.api.files import router as files_router
from app.api.countries import router as countries_router
from app.api.admin.router import router as admin_router

api_router = APIRouter()

# SOTA API data
api_router.include_router(seasons_router)
api_router.include_router(teams_router)
api_router.include_router(players_router)
api_router.include_router(games_router)
api_router.include_router(sync_router)
api_router.include_router(live_router)

# CMS content
api_router.include_router(pages_router)
api_router.include_router(news_router)

# File storage (MinIO)
api_router.include_router(files_router)

# Reference data
api_router.include_router(countries_router)

# Admin API
api_router.include_router(admin_router)
