"""Shared guardrails and diagnostics for sync services."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from app.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class DeadSeasonCounters:
    attempted: int = 0
    not_found: int = 0
    successes: int = 0

    def record_not_found(self) -> None:
        self.attempted += 1
        self.not_found += 1

    def record_empty(self) -> None:
        self.attempted += 1

    def record_success(self) -> None:
        self.attempted += 1
        self.successes += 1

    def should_mark_dead(self) -> bool:
        settings = get_settings()
        if self.attempted == 0:
            return False
        return (
            self.not_found >= settings.sota_dead_season_min_404
            and self.successes == 0
            and (self.not_found / self.attempted) >= settings.sota_dead_season_404_ratio
        )


def dead_season_cache_key(local_season_id: int, sota_season_id: int) -> str:
    return f"qfl:sota:dead-season:{local_season_id}:{sota_season_id}"


async def is_dead_season_pair(local_season_id: int, sota_season_id: int) -> bool:
    try:
        from app.utils.live_flag import get_redis

        redis = await get_redis()
        return bool(await redis.exists(dead_season_cache_key(local_season_id, sota_season_id)))
    except Exception:
        logger.debug(
            "Unable to check dead SOTA pair local=%s sota=%s",
            local_season_id,
            sota_season_id,
            exc_info=True,
        )
        return False


async def mark_dead_season_pair(local_season_id: int, sota_season_id: int) -> None:
    try:
        from app.utils.live_flag import get_redis

        settings = get_settings()
        redis = await get_redis()
        await redis.set(
            dead_season_cache_key(local_season_id, sota_season_id),
            "1",
            ex=settings.sota_dead_season_ttl_seconds,
        )
    except Exception:
        logger.debug(
            "Unable to mark dead SOTA pair local=%s sota=%s",
            local_season_id,
            sota_season_id,
            exc_info=True,
        )


@dataclass
class SyncTimingMetrics:
    enabled: bool
    fetch_seconds: float = 0.0
    db_seconds: float = 0.0
    sleep_seconds: float = 0.0
    not_found_count: int = 0
    success_count: int = 0
    players_processed: int = 0
    _started_at: float = field(default_factory=time.monotonic)

    def add_fetch(self, elapsed: float) -> None:
        self.fetch_seconds += elapsed

    def add_db(self, elapsed: float) -> None:
        self.db_seconds += elapsed

    def add_sleep(self, elapsed: float) -> None:
        self.sleep_seconds += elapsed

    @property
    def total_seconds(self) -> float:
        return time.monotonic() - self._started_at

    def log_summary(self, *, service: str, season_id: int, extra: dict | None = None) -> None:
        if not self.enabled:
            return
        payload = {
            "service": service,
            "season_id": season_id,
            "fetch_seconds": round(self.fetch_seconds, 3),
            "db_seconds": round(self.db_seconds, 3),
            "sleep_seconds": round(self.sleep_seconds, 3),
            "total_seconds": round(self.total_seconds, 3),
            "players_processed": self.players_processed,
            "not_found_count": self.not_found_count,
            "success_count": self.success_count,
        }
        if extra:
            payload.update(extra)
        logger.info("sync_timings %s", payload)
