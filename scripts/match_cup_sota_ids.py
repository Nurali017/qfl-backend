"""
Match Cup 2026 games with SOTA API and assign sota_id.

Steps:
  1. Discover SOTA season ID for Cup 2026 (by listing all seasons)
  2. Fetch games from SOTA for that season
  3. Match local cup games (season_id=202) with SOTA games by team names + date
  4. Update sota_id in the database
  5. Optionally enable sync_enabled on the season

Usage:
  docker exec qfl-backend python scripts/match_cup_sota_ids.py
  docker exec qfl-backend python scripts/match_cup_sota_ids.py --dry-run
  docker exec qfl-backend python scripts/match_cup_sota_ids.py --sota-season-id 176
"""
import argparse
import asyncio
import logging
from datetime import date
from uuid import UUID

from sqlalchemy import select, update

from app.database import AsyncSessionLocal
from app.models import Game, Season, Team
from app.services.sota_client import SotaClient
from app.utils.team_name_matcher import normalize_team_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CUP_SEASON_ID = 202


def _collect_names(team: Team) -> set[str]:
    """Collect all normalized name variants for a team."""
    names: set[str] = set()
    for field in ("name", "name_kz", "name_en"):
        value = getattr(team, field, None)
        if value:
            normalized = normalize_team_name(value)
            if normalized:
                names.add(normalized)
    return names


async def discover_cup_sota_season(client: SotaClient) -> int | None:
    """List SOTA seasons and find the Cup 2026 season ID."""
    seasons = await client.get_seasons()
    logger.info("Found %d SOTA seasons total", len(seasons))

    cup_candidates = []
    for s in seasons:
        name = (s.get("name") or "").lower()
        tournament_name = (s.get("tournament", {}).get("name", "") if isinstance(s.get("tournament"), dict) else "").lower()
        sid = s.get("id")
        # Look for "кубок" or "cup" in 2026
        if ("кубок" in name or "cup" in name or "кубок" in tournament_name or "cup" in tournament_name):
            date_start = s.get("date_start", "")
            if "2026" in str(date_start) or "2026" in name:
                cup_candidates.append(s)
                logger.info("  Cup candidate: id=%s name='%s' tournament='%s' start=%s",
                            sid, s.get("name"), tournament_name, date_start)

    if len(cup_candidates) == 1:
        return cup_candidates[0]["id"]
    if cup_candidates:
        logger.warning("Multiple cup candidates found — specify --sota-season-id manually")
        for c in cup_candidates:
            logger.warning("  id=%s name='%s'", c["id"], c.get("name"))
        return None
    logger.warning("No cup 2026 season found in SOTA. Available seasons:")
    for s in seasons:
        logger.info("  id=%s name='%s' start=%s", s.get("id"), s.get("name"), s.get("date_start"))
    return None


async def main(sota_season_id: int | None = None, dry_run: bool = False, enable_sync: bool = True):
    client = SotaClient()
    try:
        await client.ensure_authenticated()

        # Step 1: Discover or use provided SOTA season ID
        if sota_season_id is None:
            sota_season_id = await discover_cup_sota_season(client)
            if sota_season_id is None:
                logger.error("Could not determine SOTA season ID for Cup 2026. Use --sota-season-id")
                return

        logger.info("Using SOTA season ID: %d", sota_season_id)

        # Step 2: Fetch SOTA games
        sota_games = await client.get_games(sota_season_id)
        logger.info("Fetched %d games from SOTA season %d", len(sota_games), sota_season_id)

        if not sota_games:
            logger.error("No games found in SOTA season %d", sota_season_id)
            return

        # Log SOTA games for debugging
        for g in sota_games:
            home = g.get("home_team", {}) or {}
            away = g.get("away_team", {}) or {}
            logger.info(
                "  SOTA game: id=%s date=%s home='%s' away='%s'",
                g.get("id"), g.get("date"),
                home.get("name"), away.get("name"),
            )

        async with AsyncSessionLocal() as db:
            # Step 3: Load local cup games
            result = await db.execute(
                select(Game)
                .where(Game.season_id == CUP_SEASON_ID, Game.sota_id.is_(None))
                .order_by(Game.date, Game.time, Game.id)
            )
            local_games = list(result.scalars().all())
            logger.info("Found %d local cup games without sota_id", len(local_games))

            if not local_games:
                logger.info("All cup games already have sota_id — nothing to do")
                return

            # Preload teams
            team_ids = set()
            for g in local_games:
                if g.home_team_id:
                    team_ids.add(g.home_team_id)
                if g.away_team_id:
                    team_ids.add(g.away_team_id)

            teams_result = await db.execute(select(Team).where(Team.id.in_(team_ids)))
            teams_by_id: dict[int, Team] = {t.id: t for t in teams_result.scalars().all()}

            # Step 4: Match by team names + date
            matched = 0
            unmatched_local = []
            used_sota_ids: set[str] = set()

            for local_game in local_games:
                home_team = teams_by_id.get(local_game.home_team_id)
                away_team = teams_by_id.get(local_game.away_team_id)
                if not home_team or not away_team:
                    logger.warning("Game %d missing team data", local_game.id)
                    unmatched_local.append(local_game)
                    continue

                home_names = _collect_names(home_team)
                away_names = _collect_names(away_team)
                game_date = local_game.date

                best_match = None
                for sota_game in sota_games:
                    sota_id_str = str(sota_game.get("id", ""))
                    if sota_id_str in used_sota_ids:
                        continue

                    # Parse SOTA date
                    sota_date_str = sota_game.get("date", "")
                    try:
                        sota_date = date.fromisoformat(sota_date_str[:10]) if sota_date_str else None
                    except ValueError:
                        sota_date = None

                    if sota_date != game_date:
                        continue

                    # Normalize SOTA team names
                    sota_home = sota_game.get("home_team", {}) or {}
                    sota_away = sota_game.get("away_team", {}) or {}
                    sota_home_name = normalize_team_name(sota_home.get("name"))
                    sota_away_name = normalize_team_name(sota_away.get("name"))

                    # Check if any local name variant matches SOTA name (substring match)
                    home_match = any(
                        h in sota_home_name or sota_home_name in h
                        for h in home_names if h and sota_home_name
                    )
                    away_match = any(
                        a in sota_away_name or sota_away_name in a
                        for a in away_names if a and sota_away_name
                    )

                    if home_match and away_match:
                        best_match = sota_game
                        break

                if best_match:
                    sota_uuid = UUID(str(best_match["id"]))
                    used_sota_ids.add(str(best_match["id"]))
                    matched += 1
                    logger.info(
                        "  MATCHED game %d: %s vs %s [%s] → SOTA %s",
                        local_game.id,
                        home_team.name, away_team.name,
                        game_date, sota_uuid,
                    )
                    if not dry_run:
                        local_game.sota_id = sota_uuid
                else:
                    unmatched_local.append(local_game)
                    logger.warning(
                        "  UNMATCHED game %d: %s vs %s [%s]",
                        local_game.id,
                        home_team.name, away_team.name,
                        game_date,
                    )

            # Step 5: Enable sync on season
            if enable_sync and not dry_run:
                await db.execute(
                    update(Season)
                    .where(Season.id == CUP_SEASON_ID)
                    .values(
                        sync_enabled=True,
                        sota_season_id=sota_season_id,
                        sota_season_ids=str(sota_season_id),
                    )
                )
                logger.info("Enabled sync_enabled=true and set sota_season_id=%d for season %d",
                            sota_season_id, CUP_SEASON_ID)

            if not dry_run:
                await db.commit()
                logger.info("Committed %d matches to database", matched)
            else:
                logger.info("[DRY RUN] Would match %d games (no changes saved)", matched)

            if unmatched_local:
                logger.warning("%d local games could not be matched:", len(unmatched_local))
                for g in unmatched_local:
                    ht = teams_by_id.get(g.home_team_id)
                    at = teams_by_id.get(g.away_team_id)
                    logger.warning("  game %d: %s vs %s [%s]",
                                   g.id,
                                   ht.name if ht else "?",
                                   at.name if at else "?",
                                   g.date)

            logger.info("Summary: %d matched, %d unmatched out of %d total",
                        matched, len(unmatched_local), len(local_games))

    finally:
        if hasattr(client, "close"):
            await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Match Cup 2026 games with SOTA")
    parser.add_argument("--sota-season-id", type=int, default=None,
                        help="SOTA season ID for Cup 2026 (auto-discovered if not set)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't save changes, just show what would be matched")
    parser.add_argument("--no-enable-sync", action="store_true",
                        help="Don't enable sync_enabled on the season")
    args = parser.parse_args()

    asyncio.run(main(
        sota_season_id=args.sota_season_id,
        dry_run=args.dry_run,
        enable_sync=not args.no_enable_sync,
    ))
