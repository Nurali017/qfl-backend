"""Helper functions for the team overview endpoint."""

import re

from app.models import Game
from app.schemas.team import (
    TeamOverviewFormEntry,
    TeamOverviewMatch,
    TeamOverviewMatchTeam,
    TeamOverviewStadium,
    TeamOverviewStandingEntry,
    TeamOverviewSummary,
)
from app.utils.game_status import compute_game_status as _match_status
from app.utils.localization import get_localized_name, get_localized_city
from app.utils.team_logo_fallback import resolve_team_logo_url


def _safe_int(value: int | float | None) -> int:
    return int(value) if value is not None else 0


def _extract_year(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"(\d{4})", value)
    return int(match.group(1)) if match else None


def _build_overview_match(game: Game, lang: str) -> TeamOverviewMatch:
    home_team = game.home_team
    away_team = game.away_team

    if game.stadium_rel:
        stadium = TeamOverviewStadium(
            name=get_localized_name(game.stadium_rel, lang),
            city=get_localized_city(game.stadium_rel, lang) if hasattr(game.stadium_rel, "city") else None,
        )
    elif game.stadium:
        stadium = TeamOverviewStadium(name=game.stadium, city=None)
    else:
        stadium = None

    return TeamOverviewMatch(
        id=str(game.id),
        date=game.date,
        time=game.time,
        tour=game.tour,
        status=_match_status(game),
        home_score=game.home_score,
        away_score=game.away_score,
        has_stats=bool(game.has_stats),
        has_lineup=bool(game.has_lineup),
        home_team=TeamOverviewMatchTeam(
            id=home_team.id if home_team else (game.home_team_id or 0),
            name=get_localized_name(home_team, lang) if home_team else "—",
            logo_url=resolve_team_logo_url(home_team),
        ),
        away_team=TeamOverviewMatchTeam(
            id=away_team.id if away_team else (game.away_team_id or 0),
            name=get_localized_name(away_team, lang) if away_team else "—",
            logo_url=resolve_team_logo_url(away_team),
        ),
        stadium=stadium,
    )


def _compute_summary_from_games(team_id: int, finished_games: list[Game]) -> TeamOverviewSummary:
    wins = draws = losses = goals_scored = goals_conceded = points = 0

    for game in finished_games:
        is_home = game.home_team_id == team_id
        team_score = _safe_int(game.home_score if is_home else game.away_score)
        opp_score = _safe_int(game.away_score if is_home else game.home_score)
        goals_scored += team_score
        goals_conceded += opp_score

        if team_score > opp_score:
            wins += 1
            points += 3
        elif team_score < opp_score:
            losses += 1
        else:
            draws += 1
            points += 1

    return TeamOverviewSummary(
        games_played=len(finished_games),
        wins=wins,
        draws=draws,
        losses=losses,
        goals_scored=goals_scored,
        goals_conceded=goals_conceded,
        goal_difference=goals_scored - goals_conceded,
        points=points,
    )


def _window_around_team(
    entries: list[TeamOverviewStandingEntry],
    team_id: int,
    window: int = 5,
) -> list[TeamOverviewStandingEntry]:
    if not entries:
        return []

    idx = next((i for i, item in enumerate(entries) if item.team_id == team_id), -1)
    if idx < 0:
        return entries[:window]

    start = max(0, idx - 2)
    end = min(len(entries), start + window)
    start = max(0, end - window)
    return entries[start:end]
