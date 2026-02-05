import asyncio
from contextlib import asynccontextmanager
from contextlib import suppress
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import engine
from app.minio_client import init_minio
from app.services.live_event_bus import listen_live_messages, close_redis
from app.services.websocket_manager import get_websocket_manager

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_minio()
    stop_event = asyncio.Event()

    manager = get_websocket_manager()

    async def handle_message(message: dict[str, Any]) -> None:
        msg_type = message.get("type")
        game_id = message.get("game_id")
        if not msg_type or not game_id:
            return

        if msg_type == "event":
            data = message.get("data")
            if isinstance(data, dict):
                await manager.broadcast_event(game_id, data)
        elif msg_type == "lineup":
            data = message.get("data")
            if isinstance(data, dict):
                await manager.broadcast_lineup(game_id, data)
        elif msg_type == "status":
            status = message.get("status")
            if isinstance(status, str) and status:
                await manager.broadcast_game_status(game_id, status)

    subscriber_task = asyncio.create_task(
        listen_live_messages(handle_message, stop_event=stop_event)
    )
    yield
    # Shutdown
    stop_event.set()
    subscriber_task.cancel()
    with suppress(asyncio.CancelledError):
        await subscriber_task

    await close_redis()
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
