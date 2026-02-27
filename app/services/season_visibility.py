from fastapi import HTTPException
from sqlalchemy import exists, or_, select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Season


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
    result = await db.execute(
        select(Season.id).where(
            Season.id == season_id,
            is_season_visible_clause(),
        )
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Season not found")


async def get_current_season_id(db: AsyncSession) -> int:
    """Get current season ID from DB (is_current=True), falling back to config.

    This allows admins to switch the current season dynamically without restart.
    """
    from app.config import get_settings
    result = await db.execute(
        select(Season.id).where(Season.is_current == True, is_season_visible_clause()).limit(1)
    )
    db_current = result.scalar_one_or_none()
    return db_current if db_current is not None else get_settings().current_season_id


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
