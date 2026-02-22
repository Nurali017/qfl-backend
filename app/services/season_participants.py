from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, ScoreTable, Team, TeamSeasonStats, TeamTournament
from app.utils.localization import get_localized_field


@dataclass
class SeasonParticipant:
    """Resolved season participant with optional TeamTournament metadata."""

    team_id: int
    team: Team
    entry_id: int | None = None
    season_id: int | None = None
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    sort_order: int = 0
    source: str = "team_tournament"


def _sort_participants(
    participants: dict[int, SeasonParticipant],
    lang: str,
) -> list[SeasonParticipant]:
    return sorted(
        participants.values(),
        key=lambda p: (
            (get_localized_field(p.team, "name", lang) or "").casefold(),
            p.team_id,
        ),
    )


async def resolve_season_participants(
    db: AsyncSession,
    season_id: int,
    lang: str = "ru",
) -> list[SeasonParticipant]:
    """Resolve season participants with source fallback.

    Source priority:
    1) TeamTournament
    2) ScoreTable
    3) Games (home/away teams)
    4) TeamSeasonStats
    """

    participants: dict[int, SeasonParticipant] = {}

    def add_participant(
        *,
        team: Team | None,
        source: str,
        entry_id: int | None = None,
        group_name: str | None = None,
        is_disqualified: bool = False,
        fine_points: int = 0,
        sort_order: int = 0,
    ) -> None:
        if team is None:
            return

        existing = participants.get(team.id)
        if existing is None:
            participants[team.id] = SeasonParticipant(
                team_id=team.id,
                team=team,
                entry_id=entry_id,
                season_id=season_id,
                group_name=group_name,
                is_disqualified=is_disqualified,
                fine_points=fine_points,
                sort_order=sort_order,
                source=source,
            )
            return

        # TeamTournament has highest priority metadata.
        if source == "team_tournament":
            existing.entry_id = entry_id
            existing.group_name = group_name
            existing.is_disqualified = is_disqualified
            existing.fine_points = fine_points
            existing.sort_order = sort_order
            existing.source = source

    # 1) TeamTournament
    team_tournament_result = await db.execute(
        select(TeamTournament)
        .where(TeamTournament.season_id == season_id)
        .options(selectinload(TeamTournament.team))
        .order_by(TeamTournament.group_name, TeamTournament.sort_order, TeamTournament.id)
    )
    for tt in team_tournament_result.scalars().all():
        add_participant(
            team=tt.team,
            source="team_tournament",
            entry_id=tt.id,
            group_name=tt.group_name,
            is_disqualified=bool(tt.is_disqualified),
            fine_points=tt.fine_points or 0,
            sort_order=tt.sort_order or 0,
        )

    # 2) ScoreTable
    score_table_result = await db.execute(
        select(Team)
        .join(ScoreTable, ScoreTable.team_id == Team.id)
        .where(ScoreTable.season_id == season_id)
    )
    for team in score_table_result.scalars().all():
        add_participant(team=team, source="score_table")

    # 3) Games
    game_team_ids_result = await db.execute(
        select(Game.home_team_id, Game.away_team_id).where(Game.season_id == season_id)
    )
    game_team_ids: set[int] = set()
    for home_team_id, away_team_id in game_team_ids_result.all():
        if home_team_id is not None:
            game_team_ids.add(home_team_id)
        if away_team_id is not None:
            game_team_ids.add(away_team_id)

    if game_team_ids:
        game_teams_result = await db.execute(
            select(Team).where(Team.id.in_(sorted(game_team_ids)))
        )
        for team in game_teams_result.scalars().all():
            add_participant(team=team, source="games")

    # 4) TeamSeasonStats
    team_stats_result = await db.execute(
        select(Team)
        .join(TeamSeasonStats, TeamSeasonStats.team_id == Team.id)
        .where(TeamSeasonStats.season_id == season_id)
    )
    for team in team_stats_result.scalars().all():
        add_participant(team=team, source="team_season_stats")

    return _sort_participants(participants, lang)
