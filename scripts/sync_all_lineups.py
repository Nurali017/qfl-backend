#!/usr/bin/env python3
"""
Script to sync amplua/field_position data for games from SOTA live endpoint.

Default mode is dry-run (no data changes). Use --apply to execute updates.
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import Game
from app.services.sync_service import SyncService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync live lineup positions from SOTA /em endpoints")
    parser.add_argument("--season-id", type=int, default=None, help="Limit processing to a specific season")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default behavior)")
    parser.add_argument("--apply", action="store_true", help="Apply updates to database")
    parser.add_argument("--output", type=str, default=None, help="Write JSON audit report to a file")
    return parser.parse_args()


async def sync_all_lineup_positions(
    season_id: int | None = None,
    apply: bool = False,
    output_path: str | None = None,
):
    """Sync amplua data for all games that have lineups (or preview in dry-run mode)."""
    async with AsyncSessionLocal() as db:
        sync_service = SyncService(db)

        query = (
            select(Game.id, Game.date, Game.home_team_id, Game.away_team_id)
            .where(Game.has_lineup == True)
            .order_by(Game.date.desc())
        )
        if season_id is not None:
            query = query.where(Game.season_id == season_id)

        result = await db.execute(query)
        games = result.all()

        mode = "APPLY" if apply else "DRY-RUN"
        season_info = f"season {season_id}" if season_id is not None else "all seasons"
        print(f"[{mode}] Found {len(games)} games with lineups in {season_info}")

        changed_count = 0
        no_data_count = 0
        failed_count = 0
        rows: list[dict] = []

        for game_id, game_date, home_id, away_id in games:
            game_id_str = str(game_id)
            if not apply:
                print(f"• {game_date} ({game_id_str[:8]}...): planned")
                rows.append(
                    {
                        "game_id": game_id_str,
                        "date": game_date.isoformat() if game_date else None,
                        "status": "planned",
                        "positions_updated": 0,
                        "players_added": 0,
                    }
                )
                continue

            try:
                sync_result = await sync_service.sync_live_lineup_positions(game_id_str)
                positions_updated = sync_result.get("positions_updated", 0)
                players_added = sync_result.get("players_added", 0)

                if positions_updated > 0 or players_added > 0:
                    print(
                        f"✓ {game_date} ({game_id_str[:8]}...): "
                        f"updated={positions_updated}, added={players_added}"
                    )
                    changed_count += 1
                    status = "changed"
                else:
                    print(f"- {game_date} ({game_id_str[:8]}...): no live data available")
                    no_data_count += 1
                    status = "no_data"

                rows.append(
                    {
                        "game_id": game_id_str,
                        "date": game_date.isoformat() if game_date else None,
                        "status": status,
                        "positions_updated": positions_updated,
                        "players_added": players_added,
                    }
                )
            except Exception as e:
                print(f"✗ {game_date} ({game_id_str[:8]}...): {str(e)[:80]}")
                failed_count += 1
                rows.append(
                    {
                        "game_id": game_id_str,
                        "date": game_date.isoformat() if game_date else None,
                        "status": "error",
                        "error": str(e),
                        "positions_updated": 0,
                        "players_added": 0,
                    }
                )

        if apply:
            print(
                f"\nDone! Changed: {changed_count}, "
                f"No data: {no_data_count}, Failed: {failed_count}"
            )
        else:
            print(f"\nDry-run complete. Planned games: {len(games)}")

        if output_path:
            report = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "mode": mode.lower(),
                "season_id": season_id,
                "total_games": len(games),
                "changed_count": changed_count if apply else 0,
                "no_data_count": no_data_count if apply else 0,
                "failed_count": failed_count if apply else 0,
                "rows": rows,
            }
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Audit report written to: {output_file}")


if __name__ == "__main__":
    args = parse_args()
    apply = bool(args.apply)
    if not args.apply and not args.dry_run:
        # Default behavior with no explicit mode is dry-run.
        args.dry_run = True
    asyncio.run(
        sync_all_lineup_positions(
            season_id=args.season_id,
            apply=apply,
            output_path=args.output,
        )
    )
