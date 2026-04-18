"""One-time backfill: scan entire YouTube channel history and link live URLs
to games whose `youtube_live_url` is NULL.

Unlike the periodic `link_youtube_videos` task which is limited to:
  - seasons with `is_current=true`
  - today±days date window
  - last 50 videos on channel

This script paginates through the FULL channel upload history and matches
against all requested seasons/games with missing URLs. Safe to re-run
(uses `setattr` only when field is NULL).

Usage:
    # On prod (celery-worker has YOUTUBE_API_KEY + YOUTUBE_CHANNEL_ID env):
    docker exec \\
      -e BACKFILL_SEASON_IDS='61,71,80,84,85' \\
      -e BACKFILL_MAX_PAGES=80 \\
      qfl-celery-worker \\
      python -m scripts.backfill_youtube_links
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models.game import Game
from app.services.youtube_linker import (
    _YT_API,
    PendingGameIndex,
    _enrich_videos,
    _get_match_date,
    _get_uploads_playlist_id,
    _url_fields_for_type,
    classify_video,
    parse_video_title,
)
from app.services.youtube_stats import sync_tier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill")


async def _fetch_all_videos(
    playlist_id: str, api_key: str, max_pages: int = 80
) -> list[dict]:
    """Paginate through uploads playlist. Returns list of {video_id, title, published_at}."""
    videos: list[dict] = []
    page_token: str | None = None
    pages = 0
    async with httpx.AsyncClient(timeout=15) as client:
        while pages < max_pages:
            params = {
                "playlistId": playlist_id,
                "part": "snippet",
                "maxResults": 50,
                "key": api_key,
            }
            if page_token:
                params["pageToken"] = page_token
            resp = await client.get(f"{_YT_API}/playlistItems", params=params)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("items", []):
                snippet = item.get("snippet", {})
                resource = snippet.get("resourceId", {})
                vid = resource.get("videoId")
                if vid:
                    videos.append({
                        "video_id": vid,
                        "title": snippet.get("title", ""),
                        "published_at": snippet.get("publishedAt"),
                    })
            page_token = data.get("nextPageToken")
            pages += 1
            if not page_token:
                break
    logger.info("Fetched %d videos across %d pages from playlist %s", len(videos), pages, playlist_id)
    return videos


def _parse_season_ids(raw: str) -> list[int]:
    out: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if chunk:
            try:
                out.append(int(chunk))
            except ValueError:
                logger.warning("Invalid season id: %r", chunk)
    return out


async def main() -> None:
    settings = get_settings()
    api_key = settings.youtube_api_key
    channel_id = settings.youtube_channel_id
    if not api_key or not channel_id:
        logger.error("YOUTUBE_API_KEY or YOUTUBE_CHANNEL_ID not configured")
        return

    raw_seasons = os.getenv("BACKFILL_SEASON_IDS", "")
    season_ids = _parse_season_ids(raw_seasons)
    if not season_ids:
        logger.error("BACKFILL_SEASON_IDS must be set to comma-separated season IDs")
        return

    max_pages = int(os.getenv("BACKFILL_MAX_PAGES", "80"))

    # Build ordered channel list: primary + reserves
    reserve_ids = [
        cid.strip()
        for cid in (settings.youtube_reserve_channel_ids or "").split(",")
        if cid.strip() and cid.strip() != channel_id
    ]
    channel_ids = [channel_id] + reserve_ids
    logger.info("Channels: %s", channel_ids)
    logger.info("Target seasons: %s (max_pages/channel=%d)", season_ids, max_pages)

    async with AsyncSessionLocal() as db:
        games_result = await db.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(
                Game.season_id.in_(season_ids),
                Game.youtube_live_url.is_(None) | Game.video_review_url.is_(None),
            )
        )
        pending_games = list(games_result.scalars().all())
        logger.info("Pending games (missing live or review URL): %d", len(pending_games))
        if not pending_games:
            return

        index = PendingGameIndex.build(pending_games)

        # Override date window — PendingGameIndex.find_match uses ±2 days by default,
        # but we want to match historical videos. We'll override the tolerance in-place
        # by calling find_match with the actual video date (date tolerance still ±2 is OK
        # because video is typically posted within 2 days of the match date).

        linked = 0
        skipped_unparsed = 0
        classified_none = 0
        no_match = 0
        duplicates = 0

        all_videos: list[dict] = []
        for cid in channel_ids:
            try:
                playlist_id = await _get_uploads_playlist_id(cid, api_key)
                vids = await _fetch_all_videos(playlist_id, api_key, max_pages=max_pages)
                all_videos.extend(vids)
            except Exception:
                logger.exception("Failed to fetch videos from channel %s", cid)

        logger.info("Total videos to process: %d", len(all_videos))

        # Enrich in batches
        video_ids = [v["video_id"] for v in all_videos]
        enriched = await _enrich_videos(video_ids, api_key)
        logger.info("Enriched videos: %d / %d", len(enriched), len(video_ids))

        # Track which game+field already got a URL in this run to avoid double-link
        assigned: set[tuple[int, str]] = set()

        for v in all_videos:
            vid = v["video_id"]
            info = enriched.get(vid)
            if not info:
                continue
            snippet = info["snippet"]
            lsd = info["live_streaming_details"]

            video_type = classify_video(snippet, lsd)
            if video_type is None:
                classified_none += 1
                continue

            parsed = parse_video_title(snippet.get("title", ""))
            if parsed is None:
                skipped_unparsed += 1
                continue

            match_date = _get_match_date(video_type, snippet, lsd)
            if match_date is None:
                continue

            game = index.find_match(parsed, match_date, video_type)
            if game is None:
                no_match += 1
                continue

            youtube_url = f"https://www.youtube.com/watch?v={vid}"
            for url_field in _url_fields_for_type(video_type):
                key = (game.id, url_field)
                if key in assigned:
                    duplicates += 1
                    continue
                current_value = getattr(game, url_field)
                if current_value is None:
                    setattr(game, url_field, youtube_url)
                    assigned.add(key)
                    linked += 1
                    logger.info(
                        "Linked %s → game %d (%s) [%s]",
                        vid, game.id, url_field, snippet.get("title", "")[:60]
                    )

        await db.commit()
        logger.info(
            "DONE. linked=%d no_match=%d classified_none=%d unparsed=%d duplicates=%d",
            linked, no_match, classified_none, skipped_unparsed, duplicates
        )

    # Re-fetch view counts for the now-linked seasons
    os.environ["YOUTUBE_STATS_SEASON_IDS"] = ",".join(str(s) for s in season_ids)
    get_settings.cache_clear()  # type: ignore[attr-defined]
    logger.info("Running sync_tier(old) for seasons: %s", season_ids)
    result = await sync_tier("old")
    logger.info("sync_tier result: %s", result)


if __name__ == "__main__":
    asyncio.run(main())
