"""Emit a single structured log line describing current feature-flag state.

Called from backend FastAPI startup and from celery worker on worker_ready.
Purpose: make env-drift between .env and container visible in the first log
line of every service start — no more silent no-ops when a flag wasn't wired
through docker-compose.
"""
from __future__ import annotations

import logging

from app.config import get_settings


def log_feature_flags(logger: logging.Logger, *, service: str) -> None:
    s = get_settings()
    logger.info(
        "feature_flags service=%s "
        "telegram_public_posts=%s telegram_match_start=%s telegram_tour_announce=%s "
        "telegram_notifications=%s "
        "sota=%s fcms=%s weather=%s ticket_search=%s "
        "youtube_auto_link=%s google_drive=%s",
        service,
        s.telegram_public_posts_enabled,
        s.telegram_match_start_enabled,
        s.telegram_tour_announce_enabled,
        s.telegram_notifications_enabled,
        s.sota_enabled,
        s.fcms_enabled,
        s.weather_enabled,
        s.ticket_search_enabled,
        s.youtube_auto_link_enabled,
        s.google_drive_enabled,
    )
