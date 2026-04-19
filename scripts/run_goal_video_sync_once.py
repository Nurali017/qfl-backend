"""Manually trigger one pass of the goal-video sync.

Run from ``backend/`` directory with ``.env`` loaded (or env vars exported).
Typical usage in staging:

    cd backend
    DATABASE_URL='postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/qfl_staging?ssl=disable' \
    MINIO_ENDPOINT='127.0.0.1:9000' \
    python3 scripts/run_goal_video_sync_once.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)


async def main() -> int:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from app.config import get_settings
    from app.services.goal_video_sync_service import sync_goal_videos

    settings = get_settings()
    if not settings.google_drive_enabled:
        print("GOOGLE_DRIVE_ENABLED=false — nothing to do", file=sys.stderr)
        return 0

    db_url = os.environ.get("DATABASE_URL", settings.database_url)
    engine = create_async_engine(db_url)
    async with AsyncSession(engine, expire_on_commit=False) as db:
        result = await sync_goal_videos(db)
    print()
    print("== sync_goal_videos summary ==")
    print(f"listed                     = {result.listed}")
    print(f"matched (timing)           = {result.matched}")
    print(f"matched (AI event fallback)= {result.ai_event_matched}")
    print(f"folders resolved by AI     = {result.ai_folder_matched}")
    print(f"skipped_already_processed  = {result.skipped_already_processed}")
    print(f"skipped_no_game            = {result.skipped_no_game}")
    print(f"unmatched                  = {result.unmatched}")
    print(f"errors                     = {result.errors}")
    return 0 if result.errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
