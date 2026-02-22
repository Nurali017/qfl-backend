#!/usr/bin/env python3
"""
One-time backfill: import match events from legacy MySQL matches_actions
into PostgreSQL game_events table.

Legacy MySQL matches_actions schema:
  id, match_id, command_id (=team), player_id (always NULL), name (player name),
  time (varchar minute), action (int code), action_name (NULL), period (NULL),
  position, status

Action codes: 1=goal, 2=yellow_card, 8=substitution, 9=second_yellow, 10=red_card
106 records across 16 matches.

Usage:
    python -m scripts.backfill_legacy_events --dry-run
    python -m scripts.backfill_legacy_events
"""

from __future__ import annotations

import argparse
import logging
import os

import psycopg2
import pymysql
from sqlalchemy.engine.url import make_url

from app.config import get_settings

LOGGER = logging.getLogger("backfill_legacy_events")

# Legacy action code → event_type mapping
ACTION_MAP = {
    1: "goal",
    2: "yellow_card",
    8: "substitution",
    9: "red_card",   # second yellow
    10: "red_card",  # straight red
}


def get_pg_connection():
    settings = get_settings()
    parsed = make_url(settings.database_url)
    return psycopg2.connect(
        host=parsed.host,
        port=parsed.port,
        user=parsed.username,
        password=parsed.password,
        dbname=parsed.database,
    )


def get_legacy_connection():
    settings = get_settings()
    host = os.environ.get("LEGACY_MYSQL_HOST") or settings.legacy_mysql_host
    password = os.environ.get("LEGACY_MYSQL_PASSWORD") or settings.legacy_mysql_password
    user = os.environ.get("LEGACY_MYSQL_USER") or settings.legacy_mysql_user
    return pymysql.connect(
        host=host,
        port=settings.legacy_mysql_port,
        user=user,
        password=password,
        database=settings.legacy_mysql_db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=settings.legacy_mysql_connect_timeout_seconds,
    )


def fetch_legacy_actions(legacy_conn) -> list[dict]:
    """Fetch all rows from matches_actions with valid action codes."""
    with legacy_conn.cursor() as cur:
        cur.execute(
            "SELECT match_id, command_id, name, time, action "
            "FROM matches_actions "
            "WHERE action IS NOT NULL "
            "ORDER BY match_id, id"
        )
        rows = cur.fetchall()
    LOGGER.info("Legacy: %d action records fetched", len(rows))
    return rows


def parse_minute(time_val) -> int:
    """Parse minute from legacy 'time' varchar field."""
    if time_val is None:
        return 0
    try:
        return int(str(time_val).strip().rstrip("'").rstrip("+"))
    except (ValueError, TypeError):
        return 0


def infer_half(minute: int) -> int:
    """Infer half from minute: <=45 → 1st half, >45 → 2nd half."""
    return 1 if minute <= 45 else 2


def build_game_lookup(pg_conn) -> dict[int, int]:
    """Build legacy_id → game.id mapping from PostgreSQL."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT id, legacy_id FROM games WHERE legacy_id IS NOT NULL"
        )
        rows = cur.fetchall()
    lookup = {row[1]: row[0] for row in rows}
    LOGGER.info("PostgreSQL: %d games with legacy_id", len(lookup))
    return lookup


def build_team_lookup(pg_conn) -> dict[int, int]:
    """Build legacy_id → team.id mapping from PostgreSQL."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT id, legacy_id FROM teams WHERE legacy_id IS NOT NULL"
        )
        rows = cur.fetchall()
    lookup = {row[1]: row[0] for row in rows}
    LOGGER.info("PostgreSQL: %d teams with legacy_id", len(lookup))
    return lookup


def fetch_existing_event_game_ids(pg_conn) -> set[int]:
    """Return set of game IDs that already have events."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT game_id FROM game_events")
        return {row[0] for row in cur.fetchall()}


def run() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    pg_conn = get_pg_connection()
    legacy_conn = get_legacy_connection()

    try:
        # Step 1: Fetch legacy actions
        actions = fetch_legacy_actions(legacy_conn)
        if not actions:
            LOGGER.info("No legacy actions found, nothing to do")
            return 0

        # Step 2: Build lookups
        game_lookup = build_game_lookup(pg_conn)
        team_lookup = build_team_lookup(pg_conn)
        existing_games = fetch_existing_event_game_ids(pg_conn)

        # Step 3: Group actions by match
        by_match: dict[int, list[dict]] = {}
        for a in actions:
            mid = a.get("match_id")
            if mid is not None:
                by_match.setdefault(mid, []).append(a)

        LOGGER.info(
            "Legacy actions span %d matches, %d total records",
            len(by_match), len(actions),
        )

        # Step 4: Process each match
        matches_processed = 0
        events_inserted = 0
        skipped_no_game = 0
        skipped_has_events = 0
        skipped_unknown_action = 0

        with pg_conn.cursor() as cur:
            for legacy_match_id, match_actions in sorted(by_match.items()):
                game_id = game_lookup.get(legacy_match_id)
                if game_id is None:
                    skipped_no_game += 1
                    LOGGER.debug(
                        "Legacy match %d: no matching game in PostgreSQL",
                        legacy_match_id,
                    )
                    continue

                if game_id in existing_games:
                    skipped_has_events += 1
                    LOGGER.debug(
                        "Game %d (legacy %d): already has events, skipping",
                        game_id, legacy_match_id,
                    )
                    continue

                match_events_count = 0
                for action_row in match_actions:
                    action_code = action_row.get("action")
                    event_type = ACTION_MAP.get(action_code)
                    if not event_type:
                        skipped_unknown_action += 1
                        LOGGER.debug(
                            "  Skipping unknown action %s in match %d",
                            action_code, legacy_match_id,
                        )
                        continue

                    # Parse fields from real schema
                    minute = parse_minute(action_row.get("time"))
                    half = infer_half(minute)
                    player_name = (action_row.get("name") or "").strip() or None

                    # Resolve team: command_id (legacy) → PostgreSQL team_id
                    legacy_team_id = action_row.get("command_id")
                    team_id = team_lookup.get(legacy_team_id) if legacy_team_id else None

                    if args.dry_run:
                        LOGGER.info(
                            "  [DRY-RUN] Game %d: %s min=%d half=%d player=%s team_id=%s",
                            game_id, event_type, minute, half,
                            player_name, team_id,
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO game_events
                                (game_id, half, minute, event_type,
                                 team_id, player_name, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            """,
                            (game_id, half, minute, event_type,
                             team_id, player_name),
                        )

                    match_events_count += 1
                    events_inserted += 1

                if match_events_count > 0:
                    matches_processed += 1
                    LOGGER.info(
                        "Game %d (legacy %d): %d events %s",
                        game_id, legacy_match_id, match_events_count,
                        "[DRY-RUN]" if args.dry_run else "inserted",
                    )

            if not args.dry_run:
                pg_conn.commit()

        # Summary
        prefix = "[DRY-RUN] " if args.dry_run else ""
        LOGGER.info("=== Summary ===")
        LOGGER.info("%sLegacy matches with actions: %d", prefix, len(by_match))
        LOGGER.info("%sMatches processed: %d", prefix, matches_processed)
        LOGGER.info("%sEvents inserted: %d", prefix, events_inserted)
        LOGGER.info("%sSkipped (no matching game): %d", prefix, skipped_no_game)
        LOGGER.info("%sSkipped (already has events): %d", prefix, skipped_has_events)
        LOGGER.info("%sSkipped (unknown action code): %d", prefix, skipped_unknown_action)
        return 0

    finally:
        legacy_conn.close()
        pg_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time backfill of game events from legacy MySQL matches_actions",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read and calculate only, do not write",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logs",
    )
    return parser.parse_args()


def main():
    raise SystemExit(run())


if __name__ == "__main__":
    main()
