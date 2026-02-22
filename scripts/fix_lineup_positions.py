"""
Retroactive fix: recalculate game_lineups.field_position from formation + top_role.

For every game with has_lineup=True and a formation, re-derives field_position
using the formation slot template and players' top_role lateral hints.

Usage:
    cd backend && python -m scripts.fix_lineup_positions --dry-run
    cd backend && python -m scripts.fix_lineup_positions
    cd backend && python -m scripts.fix_lineup_positions --season-id=157
"""

import argparse
import asyncio
import logging
import sys

from sqlalchemy import select, and_

from app.database import AsyncSessionLocal
from app.models import Game, GameLineup, LineupType, Player
from app.utils.lineup_positions import derive_field_positions, infer_formation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100


async def fix_game_side(
    session,
    game_id: int,
    team_id: int,
    formation: str,
    *,
    dry_run: bool = False,
) -> dict:
    """Fix field_positions for one team-side of a game."""
    rows = (
        await session.execute(
            select(GameLineup.player_id, GameLineup.amplua, GameLineup.field_position, Player.top_role)
            .join(Player, Player.id == GameLineup.player_id)
            .where(
                GameLineup.game_id == game_id,
                GameLineup.team_id == team_id,
                GameLineup.lineup_type == LineupType.starter,
                GameLineup.amplua.isnot(None),
            )
        )
    ).all()

    if not rows:
        return {"players": 0, "changed": 0}

    starters = [
        {"player_id": r[0], "amplua": r[1], "top_role": r[3]}
        for r in rows
    ]
    old_positions = {r[0]: r[2] for r in rows}  # player_id -> old field_position

    assignments = derive_field_positions(formation, starters)
    if not assignments:
        return {"players": len(rows), "changed": 0}

    changed = 0
    for assignment in assignments:
        pid = assignment["player_id"]
        new_pos = assignment["field_position"]
        old_pos = old_positions.get(pid)

        if old_pos == new_pos:
            continue

        changed += 1
        if not dry_run:
            await session.execute(
                GameLineup.__table__.update()
                .where(
                    GameLineup.game_id == game_id,
                    GameLineup.team_id == team_id,
                    GameLineup.player_id == pid,
                )
                .values(field_position=new_pos)
            )

    return {"players": len(rows), "changed": changed}


async def infer_formations_for_game(
    session,
    game,
    *,
    dry_run: bool = False,
) -> dict:
    """Infer formation from amplua for sides with NULL formation."""
    result = {"home_inferred": None, "away_inferred": None, "sides_fixed": 0}

    for team_id, formation_field in [
        (game.home_team_id, "home_formation"),
        (game.away_team_id, "away_formation"),
    ]:
        if not team_id:
            continue
        # Skip if formation already set
        if getattr(game, formation_field):
            continue

        rows = (
            await session.execute(
                select(GameLineup.amplua)
                .where(
                    GameLineup.game_id == game.id,
                    GameLineup.team_id == team_id,
                    GameLineup.lineup_type == LineupType.starter,
                    GameLineup.amplua.isnot(None),
                )
            )
        ).all()

        if not rows:
            continue

        starters = [{"amplua": r[0]} for r in rows]
        formation = infer_formation(starters)
        if not formation:
            continue

        side_key = "home_inferred" if formation_field == "home_formation" else "away_inferred"
        result[side_key] = formation
        result["sides_fixed"] += 1

        if not dry_run:
            setattr(game, formation_field, formation)

    return result


async def main(args):
    if args.infer_formations:
        await run_infer_formations(args)
    else:
        await run_fix_positions(args)


async def run_infer_formations(args):
    """Infer formations from amplua, then recalculate field_positions."""
    stats = {
        "games_scanned": 0,
        "formations_inferred": 0,
        "positions_recalculated": 0,
        "formation_counts": {},
    }

    async with AsyncSessionLocal() as session:
        # Find games with lineup but missing formation on at least one side
        query = (
            select(Game)
            .where(
                Game.has_lineup.is_(True),
                (Game.home_formation.is_(None)) | (Game.away_formation.is_(None)),
            )
            .order_by(Game.id)
        )
        if args.season_id:
            query = query.where(Game.season_id == args.season_id)
        if args.limit:
            query = query.limit(args.limit)

        result = await session.execute(query)
        games = result.scalars().all()

        logger.info(
            "%sFound %d games with lineup + missing formation",
            "[DRY-RUN] " if args.dry_run else "",
            len(games),
        )

        for i, game in enumerate(games):
            stats["games_scanned"] += 1

            infer_result = await infer_formations_for_game(
                session, game, dry_run=args.dry_run,
            )

            for side_key in ("home_inferred", "away_inferred"):
                formation = infer_result[side_key]
                if formation:
                    stats["formations_inferred"] += 1
                    stats["formation_counts"][formation] = (
                        stats["formation_counts"].get(formation, 0) + 1
                    )
                    logger.info(
                        "  Game %s %s → %s",
                        game.id,
                        side_key.replace("_inferred", ""),
                        formation,
                    )

            # After inferring, recalculate field_positions for newly-set formations
            for team_id, formation_field in [
                (game.home_team_id, "home_formation"),
                (game.away_team_id, "away_formation"),
            ]:
                if not team_id:
                    continue
                formation = getattr(game, formation_field)
                if not formation:
                    continue

                side_result = await fix_game_side(
                    session, game.id, team_id, formation, dry_run=args.dry_run,
                )
                stats["positions_recalculated"] += side_result["changed"]

            if not args.dry_run and (i + 1) % BATCH_SIZE == 0:
                await session.commit()
                logger.info("  ... committed after %d games", i + 1)

        if not args.dry_run:
            await session.commit()

    prefix = "[DRY-RUN] " if args.dry_run else ""
    logger.info("=" * 60)
    logger.info("%sINFER FORMATIONS RESULTS", prefix)
    logger.info("=" * 60)
    logger.info("  Games scanned:          %d", stats["games_scanned"])
    logger.info("  Formations inferred:    %d", stats["formations_inferred"])
    logger.info("  Positions recalculated: %d", stats["positions_recalculated"])
    if stats["formation_counts"]:
        logger.info("  Formation breakdown:")
        for fm, cnt in sorted(
            stats["formation_counts"].items(), key=lambda x: -x[1]
        ):
            logger.info("    %-12s  %d", fm, cnt)


async def run_fix_positions(args):
    """Original mode: recalculate field_positions from existing formations."""
    stats = {
        "games_processed": 0,
        "sides_processed": 0,
        "players_total": 0,
        "players_changed": 0,
        "games_with_changes": 0,
    }

    async with AsyncSessionLocal() as session:
        # Find games with lineup and at least one formation
        query = (
            select(Game)
            .where(
                Game.has_lineup.is_(True),
                and_(
                    Game.home_formation.isnot(None),
                    Game.away_formation.isnot(None),
                ).self_group()
                if not args.include_partial
                else (Game.home_formation.isnot(None)) | (Game.away_formation.isnot(None)),
            )
            .order_by(Game.id)
        )
        if args.season_id:
            query = query.where(Game.season_id == args.season_id)
        if args.limit:
            query = query.limit(args.limit)

        result = await session.execute(query)
        games = result.scalars().all()

        logger.info(
            "%sFound %d games with lineup + formation",
            "[DRY-RUN] " if args.dry_run else "",
            len(games),
        )

        for i, game in enumerate(games):
            game_changed = False

            for team_id, formation in [
                (game.home_team_id, game.home_formation),
                (game.away_team_id, game.away_formation),
            ]:
                if not team_id or not formation:
                    continue

                side_result = await fix_game_side(
                    session, game.id, team_id, formation, dry_run=args.dry_run,
                )
                stats["sides_processed"] += 1
                stats["players_total"] += side_result["players"]
                stats["players_changed"] += side_result["changed"]

                if side_result["changed"] > 0:
                    game_changed = True

            stats["games_processed"] += 1
            if game_changed:
                stats["games_with_changes"] += 1

            # Commit in batches
            if not args.dry_run and (i + 1) % BATCH_SIZE == 0:
                await session.commit()
                logger.info("  ... committed after %d games", i + 1)

        if not args.dry_run:
            await session.commit()

    prefix = "[DRY-RUN] " if args.dry_run else ""
    logger.info("=" * 60)
    logger.info("%sRESULTS", prefix)
    logger.info("=" * 60)
    logger.info("  Games processed:      %d", stats["games_processed"])
    logger.info("  Games with changes:   %d", stats["games_with_changes"])
    logger.info("  Sides processed:      %d", stats["sides_processed"])
    logger.info("  Players total:        %d", stats["players_total"])
    logger.info("  Players changed:      %d", stats["players_changed"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix field_position from formation + top_role")
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument("--season-id", type=int, help="Limit to a specific season")
    parser.add_argument("--limit", type=int, help="Limit number of games to process")
    parser.add_argument(
        "--include-partial", action="store_true",
        help="Include games where only one side has a formation",
    )
    parser.add_argument(
        "--infer-formations", action="store_true",
        help="Infer formations from amplua for games with NULL formation, then recalculate positions",
    )
    args = parser.parse_args()
    asyncio.run(main(args))
