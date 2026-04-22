"""Fetch frontend-rendered daily-results card PNGs for Telegram posts."""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://kffleague.kz"
HTTP_TIMEOUT_S = 30


async def render_daily_results_card_png(
    season_id: int,
    for_date: date,
    out_path: Path,
    locale: str = "kz",
    base_url: str | None = None,
) -> Path | None:
    """Fetch the daily results Telegram card image from the frontend OG route."""
    front_base = base_url or os.environ.get("FRONTEND_PUBLIC_URL") or DEFAULT_BASE_URL
    url = (
        f"{front_base.rstrip('/')}/og/daily-results"
        f"?seasonId={season_id}&forDate={for_date.isoformat()}&locale={locale}&fresh=1"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S, follow_redirects=True) as client:
            response = await client.get(url)
        if response.status_code == 200 and response.content:
            out_path.write_bytes(response.content)
            logger.info(
                "daily results PNG fetched via OG endpoint season=%s date=%s locale=%s",
                season_id,
                for_date,
                locale,
            )
            return out_path
        logger.warning(
            "daily results OG endpoint returned %s for season=%s date=%s locale=%s",
            response.status_code,
            season_id,
            for_date,
            locale,
        )
        return None
    except Exception:
        logger.exception(
            "daily results OG fetch failed for season=%s date=%s locale=%s",
            season_id,
            for_date,
            locale,
        )
        return None
