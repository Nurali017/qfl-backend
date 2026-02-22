from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Team, SeasonParticipant


@dataclass
class ResolvedParticipant:
    """Resolved season participant."""

    team_id: int
    team: Team
    entry_id: int | None = None
    season_id: int | None = None
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    sort_order: int = 0


async def resolve_season_participants(
    db: AsyncSession,
    season_id: int,
    lang: str = "ru",
) -> list[ResolvedParticipant]:
    """Resolve season participants from the season_participant table."""
    result = await db.execute(
        select(SeasonParticipant)
        .where(SeasonParticipant.season_id == season_id)
        .options(selectinload(SeasonParticipant.team))
        .order_by(SeasonParticipant.group_name, SeasonParticipant.sort_order, SeasonParticipant.id)
    )
    return [
        ResolvedParticipant(
            team_id=tt.team_id,
            team=tt.team,
            entry_id=tt.id,
            season_id=season_id,
            group_name=tt.group_name,
            is_disqualified=bool(tt.is_disqualified),
            fine_points=tt.fine_points or 0,
            sort_order=tt.sort_order or 0,
        )
        for tt in result.scalars().all()
        if tt.team is not None
    ]
