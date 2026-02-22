#!/usr/bin/env python3
"""
One-time backfill: extract YouTube URLs from legacy MySQL review_code
(iframe embed) and populate video_url in PostgreSQL games table.

Usage:
    python -m scripts.backfill_review_urls --dry-run
    python -m scripts.backfill_review_urls
"""

from __future__ import annotations

import argparse
import logging
import re

import psycopg2
import pymysql
from sqlalchemy.engine.url import make_url

from app.config import get_settings

LOGGER = logging.getLogger("backfill_review_urls")


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
    return pymysql.connect(
        host=settings.legacy_mysql_host,
        port=settings.legacy_mysql_port,
        user=settings.legacy_mysql_user,
        password=settings.legacy_mysql_password,
        database=settings.legacy_mysql_db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=settings.legacy_mysql_connect_timeout_seconds,
    )


def extract_video_url(review_code: str) -> str | None:
    """Extract the src URL from an iframe embed string.

    Handles escaped quotes (\\") as stored in legacy MySQL.
    """
    match = re.search(r'src=\\*["\']?(https?://[^"\'>\s\\]+)', review_code)
    return match.group(1) if match else None


def fetch_legacy_reviews(legacy_conn) -> dict[int, str]:
    """Fetch review_code from legacy matches, return {legacy_id: video_url}."""
    with legacy_conn.cursor() as cur:
        cur.execute(
            "SELECT id, review_code FROM matches "
            "WHERE review_code IS NOT NULL AND LENGTH(review_code) > 0"
        )
        rows = cur.fetchall()

    reviews: dict[int, str] = {}
    skipped = 0
    for row in rows:
        url = extract_video_url(row["review_code"])
        if url:
            reviews[row["id"]] = url
        else:
            skipped += 1

    LOGGER.info(
        "Legacy: %d matches with review_code, %d valid URLs extracted, %d skipped (no URL found)",
        len(rows), len(reviews), skipped,
    )
    return reviews


def fetch_pg_games_missing_video(pg_conn) -> list[dict]:
    """Fetch games that have a legacy_id but no video_url."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT id, legacy_id FROM games "
            "WHERE legacy_id IS NOT NULL AND (video_url IS NULL OR video_url = '')"
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def run() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    pg_conn = get_pg_connection()
    legacy_conn = get_legacy_connection()

    try:
        # Step 1: fetch legacy reviews
        legacy_reviews = fetch_legacy_reviews(legacy_conn)
        if not legacy_reviews:
            LOGGER.info("No legacy reviews found, nothing to do")
            return 0

        # Step 2: fetch PG games missing video_url
        pg_games = fetch_pg_games_missing_video(pg_conn)
        LOGGER.info("PostgreSQL: %d games missing video_url with legacy_id", len(pg_games))

        if not pg_games:
            LOGGER.info("No games to update")
            return 0

        # Step 3: match and update
        updated = 0
        skipped = 0
        with pg_conn.cursor() as cur:
            for index, game in enumerate(pg_games, 1):
                legacy_id = game["legacy_id"]
                video_url = legacy_reviews.get(legacy_id)

                if not video_url:
                    skipped += 1
                    continue

                if args.dry_run:
                    LOGGER.debug(
                        "[DRY-RUN] Would update game %s (legacy_id=%d) -> %s",
                        game["id"], legacy_id, video_url,
                    )
                else:
                    cur.execute(
                        "UPDATE games SET video_url = %s WHERE id = %s",
                        (video_url, game["id"]),
                    )

                updated += 1

                if not args.dry_run and index % args.batch_size == 0:
                    pg_conn.commit()
                    LOGGER.info("Committed batch at %d/%d", index, len(pg_games))

            if not args.dry_run:
                pg_conn.commit()

        # Summary
        prefix = "[DRY-RUN] " if args.dry_run else ""
        LOGGER.info("%sTotal legacy matches with review: %d", prefix, len(legacy_reviews))
        LOGGER.info("%sGames missing video_url with legacy_id: %d", prefix, len(pg_games))
        LOGGER.info("%sMatched and updated: %d", prefix, updated)
        LOGGER.info("%sSkipped (no matching legacy review): %d", prefix, skipped)
        return 0

    finally:
        legacy_conn.close()
        pg_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time backfill of video_url from legacy review_code"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read and calculate only, do not write",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100,
        help="Commit batch size (default: 100)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable verbose/debug logs",
    )
    return parser.parse_args()


def main():
    raise SystemExit(run())


if __name__ == "__main__":
    main()
