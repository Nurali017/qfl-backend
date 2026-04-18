"""YouTube view_count sync service.

Adaptive caching: 5 tiers for different video freshness.
Quota cost: 1 unit per batch of up to 50 IDs via `videos.list?part=statistics`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models.game import Game, GameStatus
from app.models.media_video import MediaVideo
from app.utils.timestamps import utcnow
from app.utils.youtube import extract_youtube_id

logger = logging.getLogger(__name__)

_YT_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
_BATCH_SIZE = 50
_HTTP_TIMEOUT = 10.0

Tier = Literal["live", "fresh", "medium", "old", "media"]


@dataclass(frozen=True)
class _Target:
    """A YouTube video ID together with info for updating DB row."""
    source: Literal["game_live", "game_review", "media"]
    ref_id: int
    yt_id: str


def _parse_season_ids(raw: str) -> list[int]:
    """Parse comma-separated season IDs from settings string."""
    result = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            result.append(int(chunk))
        except ValueError:
            logger.warning("youtube_stats_season_ids: invalid value %r", chunk)
    return result


async def _collect_targets(db: AsyncSession, tier: Tier) -> list[_Target]:
    """Return IDs to sync for the given tier.

    Game tiers respect the ``youtube_stats_season_ids`` config filter so that
    only selected seasons (e.g. current year) are synced. ``media`` tier is
    unfiltered since media_videos are season-agnostic.
    """
    now = datetime.now(timezone.utc)
    targets: list[_Target] = []

    settings = get_settings()
    season_ids = _parse_season_ids(settings.youtube_stats_season_ids)

    if tier == "media":
        stmt = select(MediaVideo.id, MediaVideo.youtube_id).where(MediaVideo.is_active.is_(True))
        for media_id, yt_id in (await db.execute(stmt)).all():
            if yt_id:
                targets.append(_Target("media", media_id, yt_id))
        return targets

    if tier == "live":
        conditions = [
            Game.status == GameStatus.live,
            Game.youtube_live_url.is_not(None),
        ]
        if season_ids:
            conditions.append(Game.season_id.in_(season_ids))

        stmt = select(Game.id, Game.youtube_live_url).where(*conditions)
        for game_id, url in (await db.execute(stmt)).all():
            yt_id = extract_youtube_id(url) if url else None
            if yt_id:
                targets.append(_Target("game_live", game_id, yt_id))
        return targets

    # Review/past-live tiers — filter by finished_at age.
    # Each game may have up to 2 URLs (live stream + review), both synced.
    if tier == "fresh":
        min_finished = now - timedelta(hours=24)
        max_finished = None
    elif tier == "medium":
        min_finished = now - timedelta(days=7)
        max_finished = now - timedelta(hours=24)
    elif tier == "old":
        min_finished = None
        max_finished = now - timedelta(days=7)
    else:
        raise ValueError(f"unknown tier: {tier}")

    conditions = [
        (Game.video_review_url.is_not(None)) | (Game.youtube_live_url.is_not(None))
    ]
    if min_finished is not None:
        conditions.append(Game.finished_at >= min_finished)
    if max_finished is not None:
        conditions.append(Game.finished_at < max_finished)
    if season_ids:
        conditions.append(Game.season_id.in_(season_ids))

    stmt = select(Game.id, Game.youtube_live_url, Game.video_review_url).where(*conditions)
    for game_id, live_url, review_url in (await db.execute(stmt)).all():
        live_yt = extract_youtube_id(live_url) if live_url else None
        if live_yt:
            targets.append(_Target("game_live", game_id, live_yt))
        review_yt = extract_youtube_id(review_url) if review_url else None
        if review_yt:
            targets.append(_Target("game_review", game_id, review_yt))
    return targets


async def fetch_view_counts(yt_ids: list[str]) -> dict[str, int]:
    """Call videos.list in batches of 50. Returns {yt_id: view_count}."""
    if not yt_ids:
        return {}

    settings = get_settings()
    api_key = settings.youtube_api_key
    if not api_key:
        logger.warning("YOUTUBE_API_KEY is empty; skipping view_count sync")
        return {}

    result: dict[str, int] = {}
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        for i in range(0, len(yt_ids), _BATCH_SIZE):
            batch = yt_ids[i : i + _BATCH_SIZE]
            try:
                resp = await client.get(
                    _YT_VIDEOS_URL,
                    params={"id": ",".join(batch), "part": "statistics", "key": api_key},
                )
                resp.raise_for_status()
            except httpx.HTTPError as e:
                logger.exception("YouTube videos.list batch failed: %s", e)
                continue

            for item in resp.json().get("items", []):
                yt_id = item.get("id")
                count_str = item.get("statistics", {}).get("viewCount")
                if yt_id and count_str is not None:
                    try:
                        result[yt_id] = int(count_str)
                    except (TypeError, ValueError):
                        continue
    return result


async def _apply_counts(
    db: AsyncSession, targets: list[_Target], counts: dict[str, int]
) -> int:
    """Update DB rows with fetched view counts. Returns number of updates."""
    now = utcnow()
    updated = 0

    # Group by source to reduce statements
    game_live_updates: dict[int, int] = {}
    game_review_updates: dict[int, int] = {}
    media_updates: dict[int, int] = {}

    for t in targets:
        count = counts.get(t.yt_id)
        if count is None:
            continue
        if t.source == "game_live":
            game_live_updates[t.ref_id] = count
        elif t.source == "game_review":
            game_review_updates[t.ref_id] = count
        elif t.source == "media":
            media_updates[t.ref_id] = count

    for game_id, count in game_live_updates.items():
        await db.execute(
            update(Game)
            .where(Game.id == game_id)
            .values(youtube_live_view_count=count, youtube_stats_updated_at=now)
        )
        updated += 1

    for game_id, count in game_review_updates.items():
        await db.execute(
            update(Game)
            .where(Game.id == game_id)
            .values(video_review_view_count=count, youtube_stats_updated_at=now)
        )
        updated += 1

    for media_id, count in media_updates.items():
        await db.execute(
            update(MediaVideo)
            .where(MediaVideo.id == media_id)
            .values(view_count=count, stats_updated_at=now)
        )
        updated += 1

    return updated


async def sync_tier(tier: Tier) -> dict[str, int]:
    """Sync a single tier. Returns {'targets': N, 'fetched': N, 'updated': N}."""
    async with AsyncSessionLocal() as db:
        targets = await _collect_targets(db, tier)
        if not targets:
            logger.info("youtube_stats.sync_tier(%s): no targets", tier)
            return {"targets": 0, "fetched": 0, "updated": 0}

        yt_ids = list({t.yt_id for t in targets})
        counts = await fetch_view_counts(yt_ids)

        updated = await _apply_counts(db, targets, counts)
        await db.commit()

        logger.info(
            "youtube_stats.sync_tier(%s): targets=%d unique_ids=%d fetched=%d updated=%d",
            tier, len(targets), len(yt_ids), len(counts), updated,
        )
        return {"targets": len(targets), "fetched": len(counts), "updated": updated}
