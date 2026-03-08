"""Standings calculation logic: dynamic table, pre-computed score_table, next games."""

from collections import defaultdict
from datetime import date
from itertools import groupby

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import or_, select
from sqlalchemy.orm import selectinload

from app.models import Game, GameStatus, GameTeamStats, ScoreTable
from app.utils.localization import get_localized_field
from app.utils.team_logo_fallback import resolve_team_logo_url
from app.schemas.stats import NextGameInfo


def _primary_sort_key(x: dict):
    """Tiebreakers 1-3: points, GD, wins, goals scored."""
    return (-x["points"], -x["goal_difference"], -x["wins"], -x["goals_scored"])


def _compute_h2h_stats(team_id: int, tied_ids: set[int], games: list) -> dict:
    """Compute head-to-head mini-table stats for a team among tied teams."""
    points = wins = gd = gs = 0
    for g in games:
        if g.home_team_id not in tied_ids or g.away_team_id not in tied_ids:
            continue
        if g.home_team_id == team_id:
            gs += g.home_score
            gd += g.home_score - g.away_score
            if g.home_score > g.away_score:
                points += 3
                wins += 1
            elif g.home_score == g.away_score:
                points += 1
        elif g.away_team_id == team_id:
            gs += g.away_score
            gd += g.away_score - g.home_score
            if g.away_score > g.home_score:
                points += 3
                wins += 1
            elif g.home_score == g.away_score:
                points += 1
    return {"h2h_points": points, "h2h_wins": wins, "h2h_gd": gd, "h2h_gs": gs}


def _resolve_tied_group(
    tied_entries: list[dict], games: list, card_stats: dict[int, dict],
) -> list[dict]:
    """Resolve a group of teams tied on primary criteria using H2H then cards."""
    if len(tied_entries) <= 1:
        return tied_entries

    tied_ids = {e["team_id"] for e in tied_entries}

    # Filter to H2H games between tied teams
    h2h_games = [
        g for g in games
        if g.home_team_id in tied_ids and g.away_team_id in tied_ids
    ]

    # Compute H2H stats per team
    h2h_map = {
        e["team_id"]: _compute_h2h_stats(e["team_id"], tied_ids, h2h_games)
        for e in tied_entries
    }

    def h2h_key(e):
        s = h2h_map[e["team_id"]]
        return (-s["h2h_points"], -s["h2h_wins"], -s["h2h_gd"], -s["h2h_gs"])

    tied_entries.sort(key=h2h_key)

    # Group by H2H key and recursively resolve sub-ties
    resolved = []
    for _, sub_group in groupby(tied_entries, key=h2h_key):
        sub = list(sub_group)
        if len(sub) == 1 or len(sub) == len(tied_entries):
            # H2H didn't help (all still tied) — fall through to cards
            if len(sub) == len(tied_entries):
                break
            resolved.extend(sub)
        else:
            # H2H partially resolved — recurse with smaller group
            resolved.extend(_resolve_tied_group(sub, games, card_stats))
    else:
        return resolved

    # Tiebreakers #5-6: fewer red cards, then fewer yellow cards
    def card_key(e):
        cs = card_stats.get(e["team_id"], {})
        return (cs.get("red_cards", 0), cs.get("yellow_cards", 0))

    tied_entries.sort(key=card_key)
    return tied_entries


def _sort_with_tiebreakers(
    table_list: list[dict], games: list, card_stats: dict[int, dict],
) -> list[dict]:
    """Sort table using all 6 regulation tiebreakers."""
    # First sort by primary criteria
    table_list.sort(key=_primary_sort_key)

    # Group teams tied on primary criteria and resolve with H2H + cards
    resolved = []
    for _, group in groupby(table_list, key=_primary_sort_key):
        tied = list(group)
        resolved.extend(_resolve_tied_group(tied, games, card_stats))

    return resolved


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
                opponent_logo=resolve_team_logo_url(game.away_team),
                is_home=True,
            )
        if game.away_team_id in team_ids and game.away_team_id not in next_games:
            next_games[game.away_team_id] = NextGameInfo(
                game_id=game.id,
                date=game.date,
                opponent_id=game.home_team_id,
                opponent_name=game.home_team.name if game.home_team else None,
                opponent_logo=resolve_team_logo_url(game.home_team),
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
    include_live: bool = False,
) -> list[dict]:
    """Calculate league table dynamically from games with filters.

    When include_live=True, also includes live games (treating NULL scores as 0).
    """
    finished_statuses = [GameStatus.finished, GameStatus.technical_defeat]
    if include_live:
        score_filter = (
            Game.status.in_(finished_statuses + [GameStatus.live]),
        )
    else:
        score_filter = (Game.status.in_(finished_statuses),)

    query = (
        select(Game)
        .where(
            Game.season_id == season_id,
            *score_filter,
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
        "note": None,
        "form_list": [],
    })

    for game in games:
        home_id = game.home_team_id
        away_id = game.away_team_id
        # Skip live games that haven't had scores set yet
        if game.home_score is None and game.away_score is None:
            continue
        home_score = game.home_score if game.home_score is not None else 0
        away_score = game.away_score if game.away_score is not None else 0

        if home_away != "away":
            stats = team_stats[home_id]
            stats["team_id"] = home_id
            stats["team_name"] = get_localized_field(game.home_team, "name", lang) if game.home_team else None
            stats["team_logo"] = resolve_team_logo_url(game.home_team)
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
            stats["team_logo"] = resolve_team_logo_url(game.away_team)
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

    # Collect card stats from GameTeamStats
    game_ids = [g.id for g in games]
    card_stats: dict[int, dict] = defaultdict(lambda: {"red_cards": 0, "yellow_cards": 0})
    if game_ids:
        card_query = select(GameTeamStats).where(GameTeamStats.game_id.in_(game_ids))
        card_result = await db.execute(card_query)
        for gts in card_result.scalars().all():
            card_stats[gts.team_id]["red_cards"] += gts.red_cards or 0
            card_stats[gts.team_id]["yellow_cards"] += gts.yellow_cards or 0

    table_list = []
    for team_id, stats in team_stats.items():
        if stats["games_played"] > 0:
            stats["goal_difference"] = stats["goals_scored"] - stats["goals_conceded"]
            stats["form"] = "".join(stats["form_list"][-5:])
            del stats["form_list"]
            cs = card_stats.get(team_id, {})
            stats["total_red_cards"] = cs.get("red_cards", 0)
            stats["total_yellow_cards"] = cs.get("yellow_cards", 0)
            table_list.append(stats)

    table_list = _sort_with_tiebreakers(table_list, games, card_stats)

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
        "team_logo": resolve_team_logo_url(e.team),
        "games_played": e.games_played,
        "wins": e.wins,
        "draws": e.draws,
        "losses": e.losses,
        "goals_scored": e.goals_scored,
        "goals_conceded": e.goals_conceded,
        "goal_difference": e.goal_difference,
        "points": e.points,
        "form": e.form,
        "note": e.note,
    } for i, e in enumerate(entries, 1)]
