"""Shared helpers for filtering seasons by group and final stage."""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import Season, SeasonParticipant


def normalize_stage_ids(raw: object) -> list[int]:
    """Normalize JSON payload to list[int] stage IDs."""
    if not isinstance(raw, list):
        return []

    stage_ids: list[int] = []
    for value in raw:
        try:
            stage_ids.append(int(value))
        except (TypeError, ValueError):
            continue
    return stage_ids


async def get_group_team_ids(
    db: AsyncSession, season_id: int, group: str
) -> list[int]:
    """Return team_ids belonging to a specific group within a season."""
    result = await db.execute(
        select(SeasonParticipant.team_id).where(
            SeasonParticipant.season_id == season_id,
            SeasonParticipant.group_name == group,
        )
    )
    return [row[0] for row in result.all()]


async def get_group_for_team(
    db: AsyncSession, season_id: int, team_id: int
) -> str | None:
    """Return group_name of a team in a season (None if no group / not found)."""
    result = await db.execute(
        select(SeasonParticipant.group_name).where(
            SeasonParticipant.season_id == season_id,
            SeasonParticipant.team_id == team_id,
        )
    )
    row = result.first()
    if row is None:
        return None
    return row[0]


async def get_final_stage_ids(db: AsyncSession, season_id: int) -> list[int]:
    """Return configured final stage IDs for a season."""
    result = await db.execute(
        select(Season.final_stage_ids).where(Season.id == season_id)
    )
    row = result.first()
    if row is None:
        return []
    return normalize_stage_ids(row[0])
