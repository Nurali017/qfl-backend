from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import engine
from app.minio_client import init_minio
from app.caching import init_cache

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_minio()
    await init_cache()
    yield
    await engine.dispose()


app = FastAPI(
    title="QFL Backend",
    description="Backend API for Kazakhstan Football League statistics",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
_origins = (
    settings.allowed_origins.split(",")
    if settings.allowed_origins != "*"
    else ["*"]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# Import and include routers after app is created
from app.api.router import api_router
app.include_router(api_router, prefix="/api/v1")
