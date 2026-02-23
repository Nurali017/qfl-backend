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
