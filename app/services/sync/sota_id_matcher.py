"""Match local games to SOTA games to backfill Game.sota_id.

Game schedule is managed locally (see game_sync.py — sync_games is disabled),
so some games never get sota_id set. Without sota_id we can't run live
scorebot, extended stats, or player-rating syncs for that game.

This module provides the pure-function matcher used both by
`scripts/backfill_sota_id.py` (one-time historical backfill) and by a
periodic live-sync hook that picks up new SOTA matches as they're
registered.

Strategy per local game:
  1. Date must match (Asia/Almaty day).
  2. Team-ID match — SOTA team.id == local team_id. Works for Премьер-Лига
     where SOTA IDs == local IDs.
  3. Team-name match (normalized) — fallback for 2Л/Кубок where SOTA and
     local team IDs diverge. Uses the same normalization as the rest of
     the sync code (`app.utils.team_name_matcher`).

Women's league (season 205) has no sota_season_id, so the caller skips
it naturally — SOTA doesn't cover women's football in Kazakhstan.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from app.models import Game, Team
from app.services.sota_client import SotaClient
from app.services.sync.base import parse_date
from app.utils.team_name_matcher import _collect_team_names, normalize_team_name

logger = logging.getLogger(__name__)


def match_game_to_sota(
    local_game: Game,
    home_team: Team | None,
    away_team: Team | None,
    sota_games: list[dict[str, Any]],
) -> tuple[UUID | None, str]:
    """Return (sota_id, reason) for a single local game against a pre-fetched
    SOTA games list.

    Reason values:
      - "matched_by_id"       : unambiguous match on team IDs + date
      - "matched_by_name"     : unambiguous match on team names + date
      - "no_match"            : no SOTA game matched
      - "multiple_matches"    : more than one candidate (ambiguous, needs review)
      - "missing_date"        : local game has no date — can't match
      - "missing_teams"       : local game has no home/away team id
    """
    if not local_game.date:
        return None, "missing_date"
    if not local_game.home_team_id or not local_game.away_team_id:
        return None, "missing_teams"

    home_names = _collect_team_names(home_team) if home_team else set()
    away_names = _collect_team_names(away_team) if away_team else set()

    id_matches: list[dict[str, Any]] = []
    name_matches: list[dict[str, Any]] = []

    for sg in sota_games:
        sg_date_str = sg.get("date")
        if not sg_date_str:
            continue
        try:
            sg_date = parse_date(sg_date_str)
        except Exception:
            continue
        if sg_date != local_game.date:
            continue

        sg_home = sg.get("home_team") or {}
        sg_away = sg.get("away_team") or {}
        sg_home_id = sg_home.get("id")
        sg_away_id = sg_away.get("id")

        if (
            sg_home_id == local_game.home_team_id
            and sg_away_id == local_game.away_team_id
        ):
            id_matches.append(sg)
            continue

        sg_home_name = normalize_team_name(sg_home.get("name"))
        sg_away_name = normalize_team_name(sg_away.get("name"))
        if (
            sg_home_name and home_names and sg_home_name in home_names
            and sg_away_name and away_names and sg_away_name in away_names
        ):
            name_matches.append(sg)

    if len(id_matches) == 1:
        return _parse_sota_id(id_matches[0]), "matched_by_id"
    if len(id_matches) > 1:
        return None, "multiple_matches"
    if len(name_matches) == 1:
        return _parse_sota_id(name_matches[0]), "matched_by_name"
    if len(name_matches) > 1:
        return None, "multiple_matches"
    return None, "no_match"


def _parse_sota_id(sg: dict[str, Any]) -> UUID | None:
    raw = sg.get("id")
    if not raw:
        return None
    try:
        return UUID(str(raw))
    except (ValueError, TypeError):
        logger.warning("SOTA game has invalid UUID: %r", raw)
        return None


async def fetch_sota_games_for_season(
    client: SotaClient, sota_season_id: int
) -> list[dict[str, Any]]:
    """Single SOTA fetch; callers typically cache per-session."""
    return await client.get_games(sota_season_id)
