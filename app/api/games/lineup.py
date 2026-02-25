"""Game lineup endpoint."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game, Player, GameLineup, GameReferee, Coach, TeamCoach, Season,
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


@router.get("/{game_id}/lineup")
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

    # Early return for technical wins — no lineup data exists
    if game.is_technical:
        return {
            "game_id": game_id,
            "has_lineup": False,
            "rendering": {
                "mode": "hidden",
                "source": None,
                "field_allowed_by_rules": False,
                "field_data_valid": False,
            },
            "referees": [],
            "coaches": {
                "home_team": [],
                "away_team": [],
            },
            "lineups": {
                "home_team": {"team_id": game.home_team_id, "team_name": game.home_team.name if game.home_team else None, "formation": None, "kit_color": None, "starters": [], "substitutes": []},
                "away_team": {"team_id": game.away_team_id, "team_name": game.away_team.name if game.away_team else None, "formation": None, "kit_color": None, "starters": [], "substitutes": []},
            },
        }

    # Get referees for this game
    referees_result = await db.execute(
        select(GameReferee)
        .where(GameReferee.game_id == game_id)
        .options(selectinload(GameReferee.referee))
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

            referees_response.append({
                "id": ref.id,
                "first_name": first_name,
                "last_name": last_name,
                "role": gr.role.value,
                "photo_url": ref.photo_url,
                "country": ref.country,
            })

    # Get coaches for home and away teams
    async def get_team_coaches(team_id: int) -> list:
        coaches_result = await db.execute(
            select(TeamCoach)
            .where(TeamCoach.team_id == team_id, TeamCoach.season_id == game.season_id)
            .options(selectinload(TeamCoach.coach))
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

                coaches_list.append({
                    "id": coach.id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "role": tc.role.value,
                    "photo_url": coach.photo_url,
                    "country": coach.country,
                })
        return coaches_list

    home_coaches = await get_team_coaches(game.home_team_id) if game.home_team_id else []
    away_coaches = await get_team_coaches(game.away_team_id) if game.away_team_id else []

    # Get lineups for home and away teams
    async def get_team_lineup(
        team_id: int,
        team_name: str | None,
        formation: str | None,
        kit_color: str | None,
    ) -> dict:
        lineup_result = await db.execute(
            select(GameLineup)
            .where(GameLineup.game_id == game_id, GameLineup.team_id == team_id)
            .options(selectinload(GameLineup.player).selectinload(Player.country))
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

            # Build country data
            country_data = None
            if player and player.country:
                country_data = {
                    "id": player.country.id,
                    "code": player.country.code,
                    "name": player.country.name,
                    "flag_url": player.country.flag_url,
                }

            player_data = {
                "player_id": entry.player_id,
                "first_name": player.first_name if player else None,
                "last_name": player.last_name if player else None,
                "country": country_data,
                "shirt_number": entry.shirt_number,
                "is_captain": entry.is_captain,
                "position": position,
                "amplua": resolved_amplua,
                "field_position": resolved_field_position,
                "photo_url": player.photo_url if player else None,
                "_sort_order": get_position_order(resolved_amplua, resolved_field_position),
            }

            if entry.lineup_type.value == "starter":
                starters.append(player_data)
            else:
                substitutes.append(player_data)

        # Sort starters by position (GK first, then DEF L→R, then MID L→R, then FWD L→R)
        starters.sort(key=lambda x: x.get("_sort_order", 99))

        # Remove internal sort field before returning
        for p in starters:
            p.pop("_sort_order", None)
        for p in substitutes:
            p.pop("_sort_order", None)

        # Prefer formation synced from SOTA /em feed and persisted in Game.
        raw_formation = formation.strip() if isinstance(formation, str) and formation.strip() else None
        if raw_formation is not None:
            final_formation = raw_formation
        else:
            positions = [p.get("amplua") for p in starters]
            final_formation = detect_formation(positions)

        return {
            "team_id": team_id,
            "team_name": team_name,
            "formation": final_formation,
            "kit_color": kit_color,
            "starters": starters,
            "substitutes": substitutes,
        }

    # Use formations from game (synced from SOTA)
    home_lineup = (
        await get_team_lineup(
            game.home_team_id,
            game.home_team.name if game.home_team else None,
            game.home_formation,
            game.home_kit_color,
        )
        if game.home_team_id
        else {"team_id": None, "team_name": None, "formation": None, "kit_color": None, "starters": [], "substitutes": []}
    )
    away_lineup = (
        await get_team_lineup(
            game.away_team_id,
            game.away_team.name if game.away_team else None,
            game.away_formation,
            game.away_kit_color,
        )
        if game.away_team_id
        else {"team_id": None, "team_name": None, "formation": None, "kit_color": None, "starters": [], "substitutes": []}
    )

    has_lineup_data = has_any_lineup_data(home_lineup, away_lineup)
    field_allowed = is_field_allowed_by_rules(game)
    field_data_valid = team_has_valid_field_data(home_lineup) and team_has_valid_field_data(away_lineup)

    if not has_lineup_data:
        rendering_mode = "hidden"
    elif field_allowed and field_data_valid:
        rendering_mode = "field"
    else:
        rendering_mode = "list"

    source = normalize_lineup_source(game.lineup_source, has_lineup_data)

    return {
        "game_id": game_id,
        "has_lineup": has_lineup_data,
        "rendering": {
            "mode": rendering_mode,
            "source": source,
            "field_allowed_by_rules": field_allowed,
            "field_data_valid": field_data_valid,
        },
        "referees": referees_response,
        "coaches": {
            "home_team": home_coaches,
            "away_team": away_coaches,
        },
        "lineups": {
            "home_team": home_lineup,
            "away_team": away_lineup,
        },
    }
