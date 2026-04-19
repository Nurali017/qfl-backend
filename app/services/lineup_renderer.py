"""Render the match lineup field visualization to a PNG file.

Delegates rendering to the frontend OG endpoint
(Next.js `/api/og/lineup/[gameId]` → ImageResponse via Satori), avoiding a
Chromium dependency on the backend.

Falls back to Playwright screenshot only if FRONTEND_OG_LINEUP_URL isn't
reachable (useful for local dev when the frontend isn't running).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://kffleague.kz"
HTTP_TIMEOUT_S = 30


async def render_lineup_field_png(
    game_id: int,
    out_path: Path,
    locale: str = "kz",
    base_url: str | None = None,
) -> Path | None:
    """Fetch the lineup OG image from the frontend and save as PNG.

    Returns Path on success, None on failure.
    """
    front_base = (
        base_url
        or os.environ.get("FRONTEND_PUBLIC_URL")
        or DEFAULT_BASE_URL
    )
    url = f"{front_base.rstrip('/')}/og/lineup/{game_id}?locale={locale}"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S, follow_redirects=True) as c:
            r = await c.get(url)
        if r.status_code == 200 and r.content:
            out_path.write_bytes(r.content)
            logger.info("lineup PNG for game %s fetched via OG endpoint", game_id)
            return out_path
        logger.warning(
            "OG lineup endpoint returned %s for game %s", r.status_code, game_id
        )
        return None
    except Exception:
        logger.exception("lineup OG fetch failed for game %s", game_id)
        return None
