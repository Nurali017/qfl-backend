"""Cup season SOTA setup.

Reusable async service that discovers the SOTA season ID for a cup, matches
local cup games against SOTA games (by team names + date), assigns ``sota_id``
to local games, and enables ``sync_enabled`` on the season.

Used by:
- ``backend/scripts/match_cup_sota_ids.py`` (CLI)
- ``POST /api/v1/admin/ops/cup/setup-sota/{season_id}`` (admin endpoint)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, Season, Team
from app.services.sota_client import SotaClient
from app.utils.team_name_matcher import normalize_team_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GameMatch:
    local_game_id: int
    sota_id: str
    home: str
    away: str
    game_date: date


@dataclass(frozen=True)
class UnmatchedGame:
    local_game_id: int
    home: str
    away: str
    game_date: date


@dataclass
class CupSotaSetupResult:
    season_id: int
    sota_season_id: int | None
    sota_games_fetched: int = 0
    matched: list[GameMatch] = field(default_factory=list)
    unmatched: list[UnmatchedGame] = field(default_factory=list)
    sync_enabled_updated: bool = False
    dry_run: bool = False
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "season_id": self.season_id,
            "sota_season_id": self.sota_season_id,
            "sota_games_fetched": self.sota_games_fetched,
            "matched_count": len(self.matched),
            "unmatched_count": len(self.unmatched),
            "sync_enabled_updated": self.sync_enabled_updated,
            "dry_run": self.dry_run,
            "message": self.message,
            "matched": [
                {
                    "local_game_id": m.local_game_id,
                    "sota_id": m.sota_id,
                    "home": m.home,
                    "away": m.away,
                    "date": m.game_date.isoformat(),
                }
                for m in self.matched
            ],
            "unmatched": [
                {
                    "local_game_id": u.local_game_id,
                    "home": u.home,
                    "away": u.away,
                    "date": u.game_date.isoformat() if u.game_date else None,
                }
                for u in self.unmatched
            ],
        }


def _collect_names(team: Team) -> set[str]:
    """Collect all normalized name variants for a team."""
    names: set[str] = set()
    for field_name in ("name", "name_kz", "name_en"):
        value = getattr(team, field_name, None)
        if value:
            normalized = normalize_team_name(value)
            if normalized:
                names.add(normalized)
    return names


def _parse_sota_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _teams_match(local_names: set[str], sota_name: str) -> bool:
    if not sota_name:
        return False
    return any(h in sota_name or sota_name in h for h in local_names if h)


async def discover_cup_sota_season(client: SotaClient, year: str = "2026") -> int | None:
    """Discover the SOTA season ID for a cup in ``year``.

    Returns the ID if exactly one candidate is found, otherwise ``None``.
    """
    seasons = await client.get_seasons()
    logger.info("Found %d SOTA seasons total", len(seasons))

    candidates: list[dict] = []
    for season in seasons:
        name = (season.get("name") or "").lower()
        tournament = season.get("tournament") or {}
        tournament_name = (
            tournament.get("name", "").lower() if isinstance(tournament, dict) else ""
        )
        date_start = str(season.get("date_start") or "")
        if "кубок" in name or "cup" in name or "кубок" in tournament_name or "cup" in tournament_name:
            if year in date_start or year in name:
                candidates.append(season)
                logger.info(
                    "  Cup candidate: id=%s name='%s' tournament='%s' start=%s",
                    season.get("id"),
                    season.get("name"),
                    tournament_name,
                    date_start,
                )

    if len(candidates) == 1:
        return int(candidates[0]["id"])

    if candidates:
        logger.warning("Multiple cup candidates found — specify sota_season_id manually")
        for cand in candidates:
            logger.warning("  id=%s name='%s'", cand.get("id"), cand.get("name"))
        return None

    logger.warning("No cup %s season found in SOTA", year)
    return None


async def setup_cup_sota(
    db: AsyncSession,
    client: SotaClient,
    season_id: int,
    *,
    sota_season_id: int | None = None,
    dry_run: bool = False,
    enable_sync: bool = True,
    discover_year: str = "2026",
) -> CupSotaSetupResult:
    """Match local cup games with SOTA and optionally enable sync on the season.

    Steps:
        1. Discover or use provided ``sota_season_id``.
        2. Fetch SOTA games for that season.
        3. Match local games (``Game.season_id == season_id`` and
           ``Game.sota_id IS NULL``) to SOTA games by date + team names.
        4. Update ``Game.sota_id`` on matches.
        5. Set ``Season.sync_enabled=True`` + ``Season.sota_season_id`` if
           ``enable_sync`` is true.

    The function is idempotent: running it twice won't re-match already-matched
    games. Commits at the end unless ``dry_run`` is true.
    """
    result = CupSotaSetupResult(season_id=season_id, sota_season_id=sota_season_id, dry_run=dry_run)

    if sota_season_id is None:
        sota_season_id = await discover_cup_sota_season(client, year=discover_year)
        result.sota_season_id = sota_season_id
        if sota_season_id is None:
            result.message = "Could not determine SOTA season ID — provide it explicitly"
            return result

    logger.info("Using SOTA season ID: %d", sota_season_id)

    sota_games = await client.get_games(sota_season_id)
    result.sota_games_fetched = len(sota_games)
    logger.info("Fetched %d games from SOTA season %d", len(sota_games), sota_season_id)

    if not sota_games:
        result.message = f"No games found in SOTA season {sota_season_id}"
        return result

    local_games_result = await db.execute(
        select(Game)
        .where(Game.season_id == season_id, Game.sota_id.is_(None))
        .order_by(Game.date, Game.time, Game.id)
    )
    local_games = list(local_games_result.scalars().all())
    logger.info("Found %d local cup games without sota_id", len(local_games))

    if not local_games:
        if enable_sync and not dry_run:
            await _enable_sync(db, season_id, sota_season_id)
            result.sync_enabled_updated = True
        if not dry_run:
            await db.commit()
        result.message = "All cup games already have sota_id"
        return result

    team_ids: set[int] = set()
    for game in local_games:
        if game.home_team_id:
            team_ids.add(game.home_team_id)
        if game.away_team_id:
            team_ids.add(game.away_team_id)

    teams_result = await db.execute(select(Team).where(Team.id.in_(team_ids)))
    teams_by_id: dict[int, Team] = {team.id: team for team in teams_result.scalars().all()}

    used_sota_ids: set[str] = set()

    for local_game in local_games:
        home_team = teams_by_id.get(local_game.home_team_id)
        away_team = teams_by_id.get(local_game.away_team_id)
        if not home_team or not away_team:
            logger.warning("Game %d missing team data", local_game.id)
            result.unmatched.append(
                UnmatchedGame(
                    local_game_id=local_game.id,
                    home="?",
                    away="?",
                    game_date=local_game.date,
                )
            )
            continue

        home_names = _collect_names(home_team)
        away_names = _collect_names(away_team)

        match = _find_sota_match(
            local_date=local_game.date,
            home_names=home_names,
            away_names=away_names,
            sota_games=sota_games,
            used_sota_ids=used_sota_ids,
        )

        if match is None:
            logger.warning(
                "  UNMATCHED game %d: %s vs %s [%s]",
                local_game.id,
                home_team.name,
                away_team.name,
                local_game.date,
            )
            result.unmatched.append(
                UnmatchedGame(
                    local_game_id=local_game.id,
                    home=home_team.name,
                    away=away_team.name,
                    game_date=local_game.date,
                )
            )
            continue

        sota_id_str = str(match["id"])
        used_sota_ids.add(sota_id_str)
        sota_uuid = UUID(sota_id_str)
        logger.info(
            "  MATCHED game %d: %s vs %s [%s] → SOTA %s",
            local_game.id,
            home_team.name,
            away_team.name,
            local_game.date,
            sota_uuid,
        )
        if not dry_run:
            local_game.sota_id = sota_uuid
        result.matched.append(
            GameMatch(
                local_game_id=local_game.id,
                sota_id=sota_id_str,
                home=home_team.name,
                away=away_team.name,
                game_date=local_game.date,
            )
        )

    if enable_sync and not dry_run:
        await _enable_sync(db, season_id, sota_season_id)
        result.sync_enabled_updated = True

    if not dry_run:
        await db.commit()
        result.message = f"Committed {len(result.matched)} matches"
    else:
        result.message = f"[DRY RUN] Would match {len(result.matched)} games"

    logger.info(
        "Summary: %d matched, %d unmatched out of %d total",
        len(result.matched),
        len(result.unmatched),
        len(local_games),
    )
    return result


def _find_sota_match(
    *,
    local_date: date,
    home_names: set[str],
    away_names: set[str],
    sota_games: list[dict],
    used_sota_ids: set[str],
) -> dict | None:
    for sota_game in sota_games:
        sota_id_str = str(sota_game.get("id", ""))
        if not sota_id_str or sota_id_str in used_sota_ids:
            continue

        sota_date = _parse_sota_date(sota_game.get("date"))
        if sota_date != local_date:
            continue

        sota_home = sota_game.get("home_team") or {}
        sota_away = sota_game.get("away_team") or {}
        sota_home_name = normalize_team_name(sota_home.get("name"))
        sota_away_name = normalize_team_name(sota_away.get("name"))

        if _teams_match(home_names, sota_home_name) and _teams_match(away_names, sota_away_name):
            return sota_game
    return None


async def _enable_sync(db: AsyncSession, season_id: int, sota_season_id: int) -> None:
    await db.execute(
        update(Season)
        .where(Season.id == season_id)
        .values(
            sync_enabled=True,
            sota_season_id=sota_season_id,
            sota_season_ids=str(sota_season_id),
        )
    )
    logger.info(
        "Enabled sync_enabled=true and set sota_season_id=%d for season %d",
        sota_season_id,
        season_id,
    )
