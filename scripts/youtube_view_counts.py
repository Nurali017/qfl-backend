"""One-time script: fetch YouTube view counts for all games with broadcast/review URLs.

Usage:
    python -m scripts.youtube_view_counts
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
from sqlalchemy import select, or_

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models.game import Game
from app.models.season import Season
from app.utils.youtube import extract_youtube_id

_YT_API = "https://www.googleapis.com/youtube/v3"


async def fetch_view_counts(video_ids: list[str], api_key: str) -> dict[str, int]:
    result: dict[str, int] = {}
    async with httpx.AsyncClient(timeout=10) as client:
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            resp = await client.get(
                f"{_YT_API}/videos",
                params={"id": ",".join(batch), "part": "statistics", "key": api_key},
            )
            resp.raise_for_status()
            for item in resp.json().get("items", []):
                count = item.get("statistics", {}).get("viewCount")
                if count is not None:
                    result[item["id"]] = int(count)
    return result


async def main():
    settings = get_settings()
    api_key = settings.youtube_api_key
    if not api_key:
        print("ERROR: YOUTUBE_API_KEY not set")
        return

    async with AsyncSessionLocal() as db:
        # Load all games with a YouTube URL + their season name
        result = await db.execute(
            select(Game, Season.name)
            .join(Season, Game.season_id == Season.id, isouter=True)
            .where(
                or_(
                    Game.youtube_live_url.isnot(None),
                    Game.video_review_url.isnot(None),
                )
            )
            .order_by(Game.season_id, Game.date)
        )
        rows = result.all()

    if not rows:
        print("No games with YouTube URLs found.")
        return

    # Extract video IDs
    video_to_games: dict[str, tuple[Game, str]] = {}
    for game, season_name in rows:
        for url in (game.youtube_live_url, game.video_review_url):
            if not url:
                continue
            vid = extract_youtube_id(url)
            if vid and vid not in video_to_games:
                video_to_games[vid] = (game, season_name or "—")

    print(f"Found {len(rows)} games, {len(video_to_games)} unique videos. Fetching counts...")

    counts = await fetch_view_counts(list(video_to_games.keys()), api_key)

    # Print results sorted by view count desc
    results = []
    for vid, (game, season_name) in video_to_games.items():
        view_count = counts.get(vid)
        results.append((view_count or 0, vid, game, season_name))

    results.sort(reverse=True)

    print(f"\n{'Просмотры':>12}  {'Дата':<12}  {'Сезон':<30}  {'Видео'}")
    print("-" * 90)
    for view_count, vid, game, season_name in results:
        url = f"https://youtu.be/{vid}"
        print(f"{view_count:>12,}  {str(game.date):<12}  {season_name:<30}  {url}")

    total = sum(v for v, *_ in results)
    print("-" * 90)
    print(f"{total:>12,}  ИТОГО ({len(results)} видео)")


if __name__ == "__main__":
    asyncio.run(main())
