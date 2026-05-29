"""
Reclassify historical ``red_card`` events as ``second_yellow`` when the same
player already received a ``yellow_card`` earlier in the same game.

Why: around 2026-03 SOTA stopped emitting the ``2ЖК`` action code and started
sending ``КК`` for both direct reds and second-yellow removals. The
``sync_game_events`` mapping fix takes care of new syncs; this one-off script
backfills events already written as ``red_card``.

Only ``source='sota'`` events are touched. Manual entries are left alone.

Usage:
  docker exec qfl-backend python scripts/backfill_second_yellow_from_kk.py --dry-run
  docker exec qfl-backend python scripts/backfill_second_yellow_from_kk.py --apply
  docker exec qfl-backend python scripts/backfill_second_yellow_from_kk.py --apply --since 2026-03-01
"""
import argparse
import asyncio
import logging
from datetime import date

from sqlalchemy import text

from app.database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


_BASE_CTE = """
WITH yellows AS (
    SELECT
        ge.game_id,
        ge.player_id,
        LOWER(TRIM(ge.player_name)) AS name_norm,
        ge.half,
        ge.minute
    FROM game_events ge
    WHERE ge.event_type = 'yellow_card'
      AND ge.source = 'sota'
)
SELECT
    r.id,
    r.game_id,
    g.date AS game_date,
    g.season_id,
    r.half,
    r.minute,
    r.player_id,
    r.player_name,
    r.team_name
FROM game_events r
JOIN games g ON g.id = r.game_id
WHERE r.event_type = 'red_card'
  AND r.source = 'sota'
  {since_clause}
  AND EXISTS (
    SELECT 1 FROM yellows y
    WHERE y.game_id = r.game_id
      AND (
        (y.player_id IS NOT NULL AND y.player_id = r.player_id)
        OR (y.name_norm <> '' AND y.name_norm = LOWER(TRIM(r.player_name)))
      )
      AND (y.half < r.half OR (y.half = r.half AND y.minute <= r.minute))
  )
ORDER BY g.date, r.game_id, r.half, r.minute
"""


def _build_candidate_query(has_since: bool):
    since_clause = "AND g.date >= :since_date" if has_since else ""
    return text(_BASE_CTE.format(since_clause=since_clause))


UPDATE_QUERY = text(
    "UPDATE game_events SET event_type = 'second_yellow' WHERE id = ANY(:ids)"
)


async def main(apply: bool, since: date | None, limit: int | None) -> int:
    query = _build_candidate_query(has_since=since is not None)
    params = {"since_date": since} if since is not None else {}
    async with AsyncSessionLocal() as session:
        result = await session.execute(query, params)
        rows = result.mappings().all()

    if limit is not None:
        rows = list(rows[:limit])

    if not rows:
        logger.info("No candidates found.")
        return 0

    logger.info("Found %d red_card events that look like 2nd yellow:", len(rows))
    for r in rows:
        logger.info(
            "  game=%s date=%s season=%s h=%s m=%s player='%s' (%s) id=%s",
            r["game_id"],
            r["game_date"],
            r["season_id"],
            r["half"],
            r["minute"],
            r["player_name"],
            r["team_name"],
            r["id"],
        )

    if not apply:
        logger.info("DRY RUN — no changes made. Re-run with --apply to update.")
        return len(rows)

    ids = [r["id"] for r in rows]
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(UPDATE_QUERY, {"ids": ids})
    logger.info("Updated %d event(s) red_card → second_yellow.", len(ids))
    return len(ids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the update. Without this flag the script does a dry run.",
    )
    parser.add_argument(
        "--since",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Only consider games on or after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of events processed (handy for staged rollout).",
    )
    args = parser.parse_args()
    asyncio.run(main(apply=args.apply, since=args.since, limit=args.limit))
