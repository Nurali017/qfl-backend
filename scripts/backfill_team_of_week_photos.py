"""Backfill team-of-week payload photos to the leaderboard crop.

Stored ``TeamOfWeek.payload`` snapshots resolved player photos with the old
avatar-first preference (``photo_url_avatar or photo_url_leaderboard or
photo_url``). The squad convention is leaderboard-first, so this rewrites
``payload[].photo`` in place to ``photo_url_leaderboard or photo_url`` —
matching ``app/services/sync/team_of_week_sync.py`` — **without** a full
re-sync (which would hit the SOTA API).

For each entry we resolve, per player (``payload[].person_id``):
  1. active ``PlayerTeam`` for the same season → ``photo_url_leaderboard or photo_url``;
  2. fallback to base ``Player.photo_url``;
then ``resolve_file_url()`` (idempotent on full URLs).

Usage (inside the container):
  docker exec qfl-backend python scripts/backfill_team_of_week_photos.py --dry-run
  docker exec qfl-backend python scripts/backfill_team_of_week_photos.py
  docker exec qfl-backend python scripts/backfill_team_of_week_photos.py --season-id 200
"""
import argparse
import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from app.database import AsyncSessionLocal
from app.models.player import Player
from app.models.player_team import PlayerTeam
from app.models.team_of_week import TeamOfWeek
from app.utils.file_urls import resolve_file_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _resolve_photos(
    db,
    season_id: int,
    person_ids: set[int],
) -> dict[int, str]:
    """Return ``{person_id: resolved_photo_url}`` using leaderboard-first preference."""
    photo_by_player: dict[int, str] = {}

    # 1. Active PlayerTeam rows for this season → leaderboard crop, else base
    if person_ids:
        res = await db.execute(
            select(PlayerTeam.player_id, PlayerTeam.photo_url_leaderboard, PlayerTeam.photo_url)
            .where(
                PlayerTeam.player_id.in_(person_ids),
                PlayerTeam.season_id == season_id,
                PlayerTeam.is_active.is_(True),
            )
        )
        for pid, leaderboard, base in res.all():
            src = leaderboard or base
            if src and pid not in photo_by_player:
                photo_by_player[pid] = resolve_file_url(src)

    # 2. Fallback to base Player.photo_url for anyone still unresolved
    missing = person_ids - photo_by_player.keys()
    if missing:
        res = await db.execute(select(Player.id, Player.photo_url).where(Player.id.in_(missing)))
        for pid, base in res.all():
            if base:
                photo_by_player[pid] = resolve_file_url(base)

    return photo_by_player


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--season-id", type=int, default=None)
    args = parser.parse_args()

    async with AsyncSessionLocal() as db:
        stmt = select(TeamOfWeek)
        if args.season_id is not None:
            stmt = stmt.where(TeamOfWeek.season_id == args.season_id)
        entries = (await db.execute(stmt)).scalars().all()
        logger.info("Loaded %d team-of-week entries", len(entries))

        changed_entries = 0
        changed_players = 0
        unchanged = 0

        for entry in entries:
            payload = entry.payload or []
            person_ids = {
                p["person_id"]
                for p in payload
                if isinstance(p, dict) and p.get("person_id") is not None
            }
            if not person_ids:
                continue

            photos = await _resolve_photos(db, entry.season_id, person_ids)
            entry_changed = False

            for p in payload:
                if not isinstance(p, dict):
                    continue
                new_photo = photos.get(p.get("person_id"))
                if new_photo is None:
                    unchanged += 1
                    continue
                if p.get("photo") != new_photo:
                    p["photo"] = new_photo
                    changed_players += 1
                    entry_changed = True
                else:
                    unchanged += 1

            if entry_changed:
                # JSONB isn't tracked for in-place dict mutation — force dirty.
                flag_modified(entry, "payload")
                changed_entries += 1

        logger.info(
            "Photos: changed=%d unchanged=%d across %d/%d entries",
            changed_players,
            unchanged,
            changed_entries,
            len(entries),
        )

        if args.dry_run:
            logger.info("[DRY-RUN] no changes written")
        elif changed_entries:
            await db.commit()
            logger.info("Committed %d entries", changed_entries)
        else:
            logger.info("Nothing to commit")


if __name__ == "__main__":
    asyncio.run(main())
