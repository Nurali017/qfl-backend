"""Game lineup endpoint."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game, GameStatus, Player, PlayerTeam, GameLineup, GameReferee, Coach, TeamCoach, Season,
)
from app.models.referee import Referee
from app.schemas.game_lineup import (
    GameLineupResponse, LineupCoach, LineupCoaches, LineupCountryBrief,
    LineupPlayer, LineupReferee, LineupRendering, LineupTeam, LineupTeams,
)
from app.services.lineup import (
    resolve_lineup_position_fallback,
    detect_formation,
    get_position_order,
    is_field_allowed_by_rules,
    team_has_valid_field_data,
    has_any_lineup_data,
    normalize_lineup_source,
)

router = APIRouter(prefix="/games", tags=["games"])


def _build_country(country) -> LineupCountryBrief | None:
    if not country:
        return None
    return LineupCountryBrief(
        id=country.id,
        code=country.code,
        name=country.name,
        flag_url=country.flag_url,
    )


@router.get("/{game_id}/lineup", response_model=GameLineupResponse)
async def get_game_lineup(
    game_id: int,
    lang: str = Query(default="kz", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get pre-game lineup data for a game.
    Includes referees, coaches for both teams, and player lineups.
    """
    # Get game to know home/away team IDs
    game_result = await db.execute(
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season)
            .selectinload(Season.championship),
        )
    )
    game = game_result.scalar_one_or_none()

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    empty_lineup = LineupTeam(team_id=None, team_name=None)
    empty_rendering = LineupRendering(mode="hidden")

    # Early return for technical wins — no lineup data exists
    if game.status == GameStatus.technical_defeat:
        return GameLineupResponse(
            game_id=game_id,
            has_lineup=False,
            rendering=empty_rendering,
            referees=[],
            coaches=LineupCoaches(),
            lineups=LineupTeams(
                home_team=LineupTeam(
                    team_id=game.home_team_id,
                    team_name=game.home_team.name if game.home_team else None,
                ),
                away_team=LineupTeam(
                    team_id=game.away_team_id,
                    team_name=game.away_team.name if game.away_team else None,
                ),
            ),
        )

    # Get referees for this game (with country eager-loaded)
    referees_result = await db.execute(
        select(GameReferee)
        .where(GameReferee.game_id == game_id)
        .options(
            selectinload(GameReferee.referee).selectinload(Referee.country),
        )
    )
    game_referees = referees_result.scalars().all()

    referees_response = []
    for gr in game_referees:
        ref = gr.referee
        if ref:
            # Select name based on lang
            if lang == "kz":
                first_name = ref.first_name_kz or ref.first_name
                last_name = ref.last_name_kz or ref.last_name
            elif lang == "en":
                first_name = ref.first_name_en or ref.first_name
                last_name = ref.last_name_en or ref.last_name
            else:
                first_name = ref.first_name
                last_name = ref.last_name

            referees_response.append(LineupReferee(
                id=ref.id,
                first_name=first_name,
                last_name=last_name,
                role=gr.role.value,
                photo_url=ref.photo_url,
                country=_build_country(ref.country),
            ))

    # Get coaches for home and away teams
    async def get_team_coaches(team_id: int) -> list[LineupCoach]:
        coaches_result = await db.execute(
            select(TeamCoach)
            .where(TeamCoach.team_id == team_id, TeamCoach.season_id == game.season_id)
            .options(
                selectinload(TeamCoach.coach).selectinload(Coach.country),
            )
        )
        team_coaches = coaches_result.scalars().all()

        coaches_list = []
        for tc in team_coaches:
            coach = tc.coach
            if coach:
                if lang == "kz":
                    first_name = coach.first_name_kz or coach.first_name
                    last_name = coach.last_name_kz or coach.last_name
                elif lang == "en":
                    first_name = coach.first_name_en or coach.first_name
                    last_name = coach.last_name_en or coach.last_name
                else:
                    first_name = coach.first_name
                    last_name = coach.last_name

                coaches_list.append(LineupCoach(
                    id=coach.id,
                    first_name=first_name,
                    last_name=last_name,
                    role=tc.role.value,
                    photo_url=coach.photo_url,
                    country=_build_country(coach.country),
                ))
        return coaches_list

    home_coaches = await get_team_coaches(game.home_team_id) if game.home_team_id else []
    away_coaches = await get_team_coaches(game.away_team_id) if game.away_team_id else []

    # Get lineups for home and away teams
    async def get_team_lineup(
        team_id: int,
        team_name: str | None,
        formation: str | None,
        kit_color: str | None,
    ) -> LineupTeam:
        lineup_result = await db.execute(
            select(GameLineup)
            .where(GameLineup.game_id == game_id, GameLineup.team_id == team_id)
            .options(
                selectinload(GameLineup.player).selectinload(Player.country),
                selectinload(GameLineup.player).selectinload(Player.player_teams),
            )
            .order_by(GameLineup.lineup_type)
        )
        lineup_entries = lineup_result.scalars().all()

        starters = []
        substitutes = []

        for entry in lineup_entries:
            player = entry.player
            # Use match-specific position from GameLineup, fallback to player's general position
            role_hint = player.top_role if player else None
            position = entry.amplua or role_hint
            resolved_amplua, resolved_field_position = resolve_lineup_position_fallback(
                entry.amplua,
                entry.field_position,
                role_hint,
            )

            player_model = LineupPlayer(
                player_id=entry.player_id,
                first_name=player.first_name if player else None,
                last_name=player.last_name if player else None,
                country=_build_country(player.country) if player else None,
                shirt_number=(
                    next(
                        (pt.number for pt in player.player_teams
                         if pt.team_id == team_id
                         and pt.season_id == game.season_id
                         and pt.number is not None),
                        None,
                    )
                    or entry.shirt_number
                ) if player else entry.shirt_number,
                is_captain=entry.is_captain,
                position=position,
                amplua=resolved_amplua,
                field_position=resolved_field_position,
                photo_url=(next((pt.photo_url for pt in player.player_teams if pt.team_id == team_id and pt.season_id == game.season_id and pt.photo_url), None) or player.photo_url) if player else None,
            )

            sort_order = get_position_order(resolved_amplua, resolved_field_position)

            if entry.lineup_type.value == "starter":
                starters.append((sort_order, player_model))
            else:
                substitutes.append(player_model)

        # Sort starters by position (GK first, then DEF L→R, then MID L→R, then FWD L→R)
        starters.sort(key=lambda x: x[0])
        sorted_starters = [p for _, p in starters]

        # Prefer formation synced from SOTA /em feed and persisted in Game.
        raw_formation = formation.strip() if isinstance(formation, str) and formation.strip() else None
        if raw_formation is not None:
            final_formation = raw_formation
        else:
            positions = [p.amplua for p in sorted_starters]
            final_formation = detect_formation(positions)

        return LineupTeam(
            team_id=team_id,
            team_name=team_name,
            formation=final_formation,
            kit_color=kit_color,
            starters=sorted_starters,
            substitutes=substitutes,
        )

    # Use formations from game (synced from SOTA)
    home_lineup = (
        await get_team_lineup(
            game.home_team_id,
            game.home_team.name if game.home_team else None,
            game.home_formation,
            game.home_kit_color,
        )
        if game.home_team_id
        else empty_lineup
    )
    away_lineup = (
        await get_team_lineup(
            game.away_team_id,
            game.away_team.name if game.away_team else None,
            game.away_formation,
            game.away_kit_color,
        )
        if game.away_team_id
        else empty_lineup
    )

    # Convert to dicts for lineup utility checks
    home_lineup_dict = home_lineup.model_dump()
    away_lineup_dict = away_lineup.model_dump()

    has_lineup_data = has_any_lineup_data(home_lineup_dict, away_lineup_dict)
    field_allowed = is_field_allowed_by_rules(game)
    field_data_valid = team_has_valid_field_data(home_lineup_dict) and team_has_valid_field_data(away_lineup_dict)

    if not has_lineup_data:
        rendering_mode = "hidden"
    elif field_allowed and field_data_valid:
        rendering_mode = "field"
    else:
        rendering_mode = "list"

    source = normalize_lineup_source(game.lineup_source, has_lineup_data)

    return GameLineupResponse(
        game_id=game_id,
        has_lineup=has_lineup_data,
        rendering=LineupRendering(
            mode=rendering_mode,
            source=source,
            field_allowed_by_rules=field_allowed,
            field_data_valid=field_data_valid,
        ),
        referees=referees_response,
        coaches=LineupCoaches(
            home_team=home_coaches,
            away_team=away_coaches,
        ),
        lineups=LineupTeams(
            home_team=home_lineup,
            away_team=away_lineup,
        ),
    )
