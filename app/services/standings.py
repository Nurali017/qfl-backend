"""Standings calculation logic: dynamic table, pre-computed score_table, next games."""

from collections import defaultdict
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
from sqlalchemy.orm import selectinload

from app.models import Game, ScoreTable
from app.utils.localization import get_localized_field
from app.schemas.stats import NextGameInfo


async def get_next_games_for_teams(
    db: AsyncSession, season_id: int, team_ids: list[int]
) -> dict[int, NextGameInfo]:
    """Get next upcoming game for each team."""
    if not team_ids:
        return {}

    today = date.today()
    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.is_(None),
            Game.date >= today,
            or_(
                Game.home_team_id.in_(team_ids),
                Game.away_team_id.in_(team_ids),
            ),
        )
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .order_by(Game.date, Game.time)
    )

    result = await db.execute(query)
    games = result.scalars().all()

    next_games: dict[int, NextGameInfo] = {}
    for game in games:
        if game.home_team_id in team_ids and game.home_team_id not in next_games:
            next_games[game.home_team_id] = NextGameInfo(
                game_id=game.id,
                date=game.date,
                opponent_id=game.away_team_id,
                opponent_name=game.away_team.name if game.away_team else None,
                opponent_logo=game.away_team.logo_url if game.away_team else None,
                is_home=True,
            )
        if game.away_team_id in team_ids and game.away_team_id not in next_games:
            next_games[game.away_team_id] = NextGameInfo(
                game_id=game.id,
                date=game.date,
                opponent_id=game.home_team_id,
                opponent_name=game.home_team.name if game.home_team else None,
                opponent_logo=game.home_team.logo_url if game.home_team else None,
                is_home=False,
            )

    return next_games


async def calculate_dynamic_table(
    db: AsyncSession,
    season_id: int,
    tour_from: int | None,
    tour_to: int | None,
    home_away: str | None,
    lang: str = "ru",
    group_team_ids: list[int] | None = None,
    final_stage_ids: list[int] | None = None,
) -> list[dict]:
    """Calculate league table dynamically from games with filters."""
    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
        )
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .order_by(Game.tour, Game.date, Game.time)
    )

    if tour_from is not None:
        query = query.where(Game.tour >= tour_from)
    if tour_to is not None:
        query = query.where(Game.tour <= tour_to)
    if group_team_ids is not None:
        query = query.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids is not None:
        query = query.where(Game.stage_id.in_(final_stage_ids))

    result = await db.execute(query)
    games = result.scalars().all()

    team_stats: dict[int, dict] = defaultdict(lambda: {
        "team_id": 0,
        "team_name": None,
        "team_logo": None,
        "games_played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_scored": 0,
        "goals_conceded": 0,
        "points": 0,
        "form_list": [],
    })

    for game in games:
        home_id = game.home_team_id
        away_id = game.away_team_id
        home_score = game.home_score
        away_score = game.away_score

        if home_away != "away":
            stats = team_stats[home_id]
            stats["team_id"] = home_id
            stats["team_name"] = get_localized_field(game.home_team, "name", lang) if game.home_team else None
            stats["team_logo"] = game.home_team.logo_url if game.home_team else None
            stats["games_played"] += 1
            stats["goals_scored"] += home_score
            stats["goals_conceded"] += away_score

            if home_score > away_score:
                stats["wins"] += 1
                stats["points"] += 3
                stats["form_list"].append("W")
            elif home_score < away_score:
                stats["losses"] += 1
                stats["form_list"].append("L")
            else:
                stats["draws"] += 1
                stats["points"] += 1
                stats["form_list"].append("D")

        if home_away != "home":
            stats = team_stats[away_id]
            stats["team_id"] = away_id
            stats["team_name"] = get_localized_field(game.away_team, "name", lang) if game.away_team else None
            stats["team_logo"] = game.away_team.logo_url if game.away_team else None
            stats["games_played"] += 1
            stats["goals_scored"] += away_score
            stats["goals_conceded"] += home_score

            if away_score > home_score:
                stats["wins"] += 1
                stats["points"] += 3
                stats["form_list"].append("W")
            elif away_score < home_score:
                stats["losses"] += 1
                stats["form_list"].append("L")
            else:
                stats["draws"] += 1
                stats["points"] += 1
                stats["form_list"].append("D")

    table_list = []
    for team_id, stats in team_stats.items():
        if stats["games_played"] > 0:
            stats["goal_difference"] = stats["goals_scored"] - stats["goals_conceded"]
            stats["form"] = "".join(stats["form_list"][-5:])
            del stats["form_list"]
            table_list.append(stats)

    table_list.sort(key=lambda x: (-x["points"], -x["goal_difference"], -x["goals_scored"]))

    for i, entry in enumerate(table_list, 1):
        entry["position"] = i

    return table_list


async def read_score_table(db: AsyncSession, season_id: int, group_team_ids: list[int] | None, lang: str):
    """Read standings from the score_table (pre-computed standings)."""
    query = (
        select(ScoreTable)
        .where(ScoreTable.season_id == season_id)
        .options(selectinload(ScoreTable.team))
    )
    if group_team_ids is not None:
        query = query.where(ScoreTable.team_id.in_(group_team_ids))
    query = query.order_by(ScoreTable.position)

    result = await db.execute(query)
    entries = result.scalars().all()

    return [{
        "position": i if group_team_ids else e.position,
        "team_id": e.team_id,
        "team_name": get_localized_field(e.team, "name", lang) if e.team else None,
        "team_logo": e.team.logo_url if e.team else None,
        "games_played": e.games_played,
        "wins": e.wins,
        "draws": e.draws,
        "losses": e.losses,
        "goals_scored": e.goals_scored,
        "goals_conceded": e.goals_conceded,
        "goal_difference": e.goal_difference,
        "points": e.points,
        "form": e.form,
    } for i, e in enumerate(entries, 1)]
