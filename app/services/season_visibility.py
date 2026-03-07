import time

from fastapi import HTTPException
from sqlalchemy import exists, or_, select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Season

# ── In-memory cache for visible season IDs (60s TTL) ──
_visible_ids: set[int] | None = None
_current_id: int | None = None
_expires_at: float = 0
_CACHE_TTL = 60


def _is_cache_valid() -> bool:
    return _visible_ids is not None and time.monotonic() < _expires_at


async def _load_season_cache(db: AsyncSession) -> None:
    global _visible_ids, _current_id, _expires_at
    result = await db.execute(
        select(Season.id, Season.is_current).where(Season.is_visible.is_(True))
    )
    rows = result.all()
    _visible_ids = {row.id for row in rows}
    _current_id = next((row.id for row in rows if row.is_current), None)
    _expires_at = time.monotonic() + _CACHE_TTL


def invalidate_season_cache() -> None:
    global _visible_ids, _current_id, _expires_at
    _visible_ids = None
    _current_id = None
    _expires_at = 0


def is_season_visible_clause() -> ColumnElement[bool]:
    """SQL clause for filtering only visible seasons."""
    return Season.is_visible.is_(True)


def has_visible_season_clause(season_id_column: ColumnElement[int | None]) -> ColumnElement[bool]:
    """SQL clause that checks whether a FK season_id points to a visible season."""
    return exists(
        select(Season.id).where(
            Season.id == season_id_column,
            is_season_visible_clause(),
        )
    )


def season_unscoped_or_visible_clause(season_id_column: ColumnElement[int | None]) -> ColumnElement[bool]:
    """SQL clause for nullable season refs: keep NULL and visible season references."""
    return or_(season_id_column.is_(None), has_visible_season_clause(season_id_column))


async def ensure_visible_season_or_404(db: AsyncSession, season_id: int) -> None:
    """Raise a generic 404 for hidden or missing seasons."""
    if not _is_cache_valid():
        await _load_season_cache(db)
    if season_id in _visible_ids:  # type: ignore[operator]
        return
    raise HTTPException(status_code=404, detail="Season not found")


async def get_current_season_id(db: AsyncSession) -> int:
    """Get current season ID from DB (is_current=True), falling back to config.

    This allows admins to switch the current season dynamically without restart.
    """
    from app.config import get_settings
    if not _is_cache_valid():
        await _load_season_cache(db)
    if _current_id is not None:
        return _current_id
    return get_settings().current_season_id


async def resolve_visible_season_id(db: AsyncSession, season_id: int | None) -> int:
    """Resolve optional season_id to a concrete visible season.

    Falls back to current season (DB is_current, then config) when season_id is None.
    Raises 404 if the resolved season is hidden or missing.
    """
    if season_id is not None:
        resolved = season_id
    else:
        resolved = await get_current_season_id(db)
    await ensure_visible_season_or_404(db, resolved)
    return resolved
