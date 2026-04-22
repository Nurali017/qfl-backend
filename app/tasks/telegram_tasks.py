"""Celery wrappers for public Telegram post functions.

Retries HTTP errors with backoff. The dedup flag inside each service function
guarantees idempotency even if a retry lands after a successful send.
"""
from __future__ import annotations

import logging
from datetime import date as date_cls, timedelta

import httpx
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import Game
from app.models.game import GameStatus
from app.services.telegram_posts import (
    find_ready_daily_results_payloads,
    post_game_event,
    post_daily_results_digest,
    post_goal_video,
    post_match_finish,
    post_match_start,
    post_pregame_lineup,
    post_tour_announcement,
)
from app.services.telegram_user_client import TelegramTransientError
from app.tasks import celery_app
from app.utils.async_celery import run_async

logger = logging.getLogger(__name__)

_RETRY_KW = {
    "autoretry_for": (httpx.HTTPError, TelegramTransientError),
    "retry_backoff": True,
    "retry_backoff_max": 300,
    "max_retries": 3,
}


# ---------------------------------------------------------------------- #
#  Individual post tasks                                                  #
# ---------------------------------------------------------------------- #

async def _post_match_start(game_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        return await post_match_start(db, game_id)


@celery_app.task(name="app.tasks.telegram_tasks.post_match_start_task", **_RETRY_KW)
def post_match_start_task(game_id: int):
    return run_async(_post_match_start(game_id))


async def _post_match_finish(game_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        return await post_match_finish(db, game_id)


@celery_app.task(name="app.tasks.telegram_tasks.post_match_finish_task", **_RETRY_KW)
def post_match_finish_task(game_id: int):
    return run_async(_post_match_finish(game_id))


async def _post_game_event(event_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        return await post_game_event(db, event_id)


@celery_app.task(name="app.tasks.telegram_tasks.post_game_event_task", **_RETRY_KW)
def post_game_event_task(event_id: int):
    return run_async(_post_game_event(event_id))


async def _post_goal_video(event_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        return await post_goal_video(db, event_id)


@celery_app.task(name="app.tasks.telegram_tasks.post_goal_video_task", **_RETRY_KW)
def post_goal_video_task(event_id: int):
    return run_async(_post_goal_video(event_id))


async def _post_pregame_lineup(game_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        return await post_pregame_lineup(db, game_id)


@celery_app.task(name="app.tasks.telegram_tasks.post_pregame_lineup_task", **_RETRY_KW)
def post_pregame_lineup_task(game_id: int):
    return run_async(_post_pregame_lineup(game_id))


async def _scan_daily_results_cards(locale: str = "kz") -> dict:
    today = date_cls.today()
    date_from = today - timedelta(days=1)
    async with AsyncSessionLocal() as db:
        payloads = await find_ready_daily_results_payloads(
            db,
            locale=locale,
            date_from=date_from,
            date_to=today,
        )
        posted: list[tuple[int, str]] = []
        skipped: list[tuple[int, str]] = []
        for payload in payloads:
            try:
                ok = await post_daily_results_digest(
                    db,
                    season_id=payload.season_id,
                    for_date=payload.for_date,
                    locale=locale,
                    payload=payload,
                )
                target = (payload.season_id, str(payload.for_date))
                (posted if ok else skipped).append(target)
            except httpx.HTTPError:
                raise
            except Exception:
                logger.exception(
                    "daily results digest failed for season=%s date=%s",
                    payload.season_id,
                    payload.for_date,
                )
                skipped.append((payload.season_id, str(payload.for_date)))
        return {
            "locale": locale,
            "posted": posted,
            "skipped": skipped,
        }


@celery_app.task(
    name="app.tasks.telegram_tasks.scan_daily_results_cards",
    autoretry_for=(httpx.HTTPError, TelegramTransientError),
    retry_backoff=True,
    max_retries=3,
)
def scan_daily_results_cards(locale: str = "kz"):
    return run_async(_scan_daily_results_cards(locale))


# ---------------------------------------------------------------------- #
#  Tour announcement (daily beat)                                         #
# ---------------------------------------------------------------------- #

async def _tour_announce_daily() -> dict:
    """Find all (season_id, tour) groups with games tomorrow and post each.

    Groups games by (season_id, tour). Deduplication is per-game inside
    post_tour_announcement — if some games of a tour were already announced,
    the whole group is skipped.
    """
    tomorrow = date_cls.today() + timedelta(days=1)
    async with AsyncSessionLocal() as db:
        q = (
            select(Game.season_id, Game.tour)
            .where(
                Game.date == tomorrow,
                Game.status == GameStatus.created,
                Game.season_id.is_not(None),
                Game.tour.is_not(None),
                Game.announce_telegram_sent_at.is_(None),
            )
            .distinct()
        )
        rows = (await db.execute(q)).all()
        posted: list[tuple[int, int]] = []
        skipped: list[tuple[int, int]] = []
        for season_id, tour in rows:
            try:
                ok = await post_tour_announcement(db, season_id, tour, tomorrow)
                (posted if ok else skipped).append((season_id, tour))
            except httpx.HTTPError:
                raise
            except Exception:
                logger.exception(
                    "tour_announce failed for season=%s tour=%s", season_id, tour
                )
                skipped.append((season_id, tour))
        return {"date": str(tomorrow), "posted": posted, "skipped": skipped}


@celery_app.task(
    name="app.tasks.telegram_tasks.tour_announce_daily",
    autoretry_for=(httpx.HTTPError, TelegramTransientError),
    retry_backoff=True,
    max_retries=3,
)
def tour_announce_daily():
    return run_async(_tour_announce_daily())


# ---------------------------------------------------------------------- #
#  Live-sync hook helper                                                  #
# ---------------------------------------------------------------------- #

async def dispatch_pending_event_posts(db, game_id: int) -> int:
    """Dispatch post_game_event for events with telegram_sent_at IS NULL.

    Called from live sync task after commit. Returns number dispatched.
    """
    from app.models.game_event import GameEvent, GameEventType
    from app.services.telegram_posts import POSTABLE_EVENT_TYPES

    q = select(GameEvent.id).where(
        GameEvent.game_id == game_id,
        GameEvent.telegram_sent_at.is_(None),
        GameEvent.event_type.in_(POSTABLE_EVENT_TYPES),
    )
    ids = (await db.execute(q)).scalars().all()
    for eid in ids:
        try:
            post_game_event_task.delay(eid)
        except Exception:
            logger.exception("Failed to enqueue post_game_event_task(%s)", eid)
    return len(ids)
