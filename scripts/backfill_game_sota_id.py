"""Backfill Game.sota_id by matching local games to SOTA get_games results.

Complements scripts/backfill_sota_ids.py (which backfills PLAYER sota_id).
This one fills GAME sota_id for historical games that were created via
FCMS or manual admin entry and never linked to their SOTA counterpart.

Usage:
    docker exec qfl-backend python scripts/backfill_game_sota_id.py
    docker exec qfl-backend python scripts/backfill_game_sota_id.py --commit
    docker exec qfl-backend python scripts/backfill_game_sota_id.py --season 203 --commit

Default is dry-run — prints what WOULD be updated. Pass --commit to write.

Skips seasons with Season.sota_season_id IS NULL (Women's league, since
SOTA doesn't cover it).
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models import Game, Season
from app.services.sota_client import get_sota_client
from app.services.sync.sota_id_matcher import (
    fetch_sota_games_for_season,
    match_game_to_sota,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def process_season(db, client, season: Season, *, commit: bool) -> dict:
    """Match every local game in the season that has sota_id IS NULL."""
    games_q = (
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
        )
        .where(
            Game.season_id == season.id,
            Game.sota_id.is_(None),
        )
        .order_by(Game.date, Game.time)
    )
    missing_games = (await db.execute(games_q)).scalars().all()
    if not missing_games:
        return {"missing": 0, "matched": 0, "no_match": [], "ambiguous": []}

    logger.info(
        "Season %s (%s, sota_season_id=%s): %d games missing sota_id",
        season.id, season.name, season.sota_season_id, len(missing_games),
    )
    sota_games = await fetch_sota_games_for_season(client, season.sota_season_id)
    logger.info("  SOTA returned %d games for sota_season_id=%s",
                len(sota_games), season.sota_season_id)

    matched = 0
    no_match: list[Game] = []
    ambiguous: list[Game] = []

    for g in missing_games:
        sid, reason = match_game_to_sota(g, g.home_team, g.away_team, sota_games)
        if sid:
            matched += 1
            home = g.home_team.name if g.home_team else "?"
            away = g.away_team.name if g.away_team else "?"
            prefix = "WOULD SET" if not commit else "SET"
            logger.info(
                "  %s game=%d %s %s vs %s → %s (%s)",
                prefix, g.id, g.date, home, away, sid, reason,
            )
            if commit:
                g.sota_id = sid
        elif reason == "multiple_matches":
            ambiguous.append(g)
        else:
            no_match.append(g)

    if commit and matched:
        await db.commit()
        logger.info("  COMMITTED %d game sota_ids for season %s", matched, season.id)

    return {
        "missing": len(missing_games),
        "matched": matched,
        "no_match": no_match,
        "ambiguous": ambiguous,
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--commit", action="store_true",
        help="Actually write sota_id to DB (default: dry-run)",
    )
    parser.add_argument(
        "--season", type=int, default=None,
        help="Process only a specific local season_id",
    )
    args = parser.parse_args()

    if args.commit:
        logger.warning("COMMIT mode — will write sota_id to DB")
    else:
        logger.info("DRY-RUN mode — no DB writes; pass --commit to apply")

    async with AsyncSessionLocal() as db:
        client = get_sota_client()
        try:
            seasons_q = select(Season).where(Season.sota_season_id.isnot(None))
            if args.season is not None:
                seasons_q = seasons_q.where(Season.id == args.season)
            seasons_q = seasons_q.order_by(Season.id)
            seasons = (await db.execute(seasons_q)).scalars().all()

            totals = {"missing": 0, "matched": 0, "no_match": 0, "ambiguous": 0}
            unresolved_by_season: dict[int, dict] = {}

            for season in seasons:
                stats = await process_season(db, client, season, commit=args.commit)
                totals["missing"] += stats["missing"]
                totals["matched"] += stats["matched"]
                totals["no_match"] += len(stats["no_match"])
                totals["ambiguous"] += len(stats["ambiguous"])
                if stats["no_match"] or stats["ambiguous"]:
                    unresolved_by_season[season.id] = stats

            logger.info("=" * 60)
            logger.info(
                "TOTAL: missing=%d matched=%d no_match=%d ambiguous=%d",
                totals["missing"], totals["matched"],
                totals["no_match"], totals["ambiguous"],
            )

            for sid, stats in unresolved_by_season.items():
                if stats["no_match"]:
                    logger.info("--- Season %s NO-MATCH (%d) ---", sid, len(stats["no_match"]))
                    for g in stats["no_match"][:20]:
                        home = g.home_team.name if g.home_team else "?"
                        away = g.away_team.name if g.away_team else "?"
                        logger.info(
                            "    game=%d %s %s (home_id=%s) vs %s (away_id=%s)",
                            g.id, g.date, home, g.home_team_id, away, g.away_team_id,
                        )
                    if len(stats["no_match"]) > 20:
                        logger.info("    ... and %d more", len(stats["no_match"]) - 20)
                if stats["ambiguous"]:
                    logger.info("--- Season %s AMBIGUOUS (%d) ---", sid, len(stats["ambiguous"]))
                    for g in stats["ambiguous"]:
                        logger.info("    game=%d %s", g.id, g.date)
        finally:
            if hasattr(client, "close"):
                await client.close()


if __name__ == "__main__":
    asyncio.run(main())
