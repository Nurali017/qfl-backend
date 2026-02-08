from fastapi import APIRouter

from app.api.admin.auth import router as auth_router
from app.api.admin.users import router as users_router
from app.api.admin.news import router as news_router
from app.api.admin.pages import router as pages_router
from app.api.admin.files import router as files_router
from app.api.admin.ops import router as ops_router

router = APIRouter(prefix="/admin")
router.include_router(auth_router)
router.include_router(users_router)
router.include_router(news_router)
router.include_router(pages_router)
router.include_router(files_router)
router.include_router(ops_router)
