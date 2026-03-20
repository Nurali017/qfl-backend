"""One-time script to map FCMS matches to our games by date + teams.

Usage: python3 -m scripts.fcms_bulk_import

Fetches all matches from FCMS group 10733 (Premier League) and matches them
to our games by date + home_team + away_team. Updates game.fcms_match_id.
"""

import asyncio
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models import Game, Team, Season
from app.services.fcms_client import FcmsClient
from app.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

settings = get_settings()


def normalize(name: str | None) -> str:
    """Normalize team name for fuzzy matching."""
    if not name:
        return ""
    return name.strip().lower().replace("фк ", "").replace("fc ", "").strip()


# Latin-to-Cyrillic transliteration map for Kazakh team names
_LATIN_TO_CYRILLIC = {
    "qyzyljar": "қызылжар",
    "okzhetpes": "оқжетпес",
    "ertis": "ертіс",
    "tobol": "тобыл",
    "tobyl": "тобыл",
    "astana": "астана",
    "atyrau": "атырау",
    "ordabasy": "ордабасы",
    "kairat": "қайрат",
    "kaisar": "қайсар",
    "aktobe": "ақтөбе",
    "zhetysu": "жетісу",
    "zhenis": "жеңіс",
    "elimai": "елімай",
    "ulytau": "ұлытау",
    "kaspiy": "каспий",
    "shakhter": "шахтёр",
    "altai": "алтай",
    "turan": "тұран",
    "ertis": "ертіс",
}

# Kazakh-Cyrillic to Russian-Cyrillic for team names that differ
_KAZAKH_TO_RUSSIAN = {
    "ертіс": "иртыш",
}


def team_names_match(fcms_name: str, team: Team) -> bool:
    """Check if FCMS team name matches our Team by any name variant."""
    fcms_norm = normalize(fcms_name)
    if not fcms_norm:
        return False

    # Try Latin-to-Cyrillic transliteration
    fcms_cyrillic = _LATIN_TO_CYRILLIC.get(fcms_norm)

    # Collect all known names for the team
    team_names = set()
    for attr in ("name", "name_kz", "name_en"):
        val = getattr(team, attr, None)
        if val:
            team_names.add(normalize(val))

    # Direct match
    if fcms_norm in team_names:
        return True

    # Transliterated match
    if fcms_cyrillic and fcms_cyrillic in team_names:
        return True

    # Kazakh-to-Russian match (e.g. ЕРТІС → Иртыш)
    fcms_russian = _KAZAKH_TO_RUSSIAN.get(fcms_norm)
    if fcms_russian and fcms_russian in team_names:
        return True
    if fcms_cyrillic:
        fcms_russian2 = _KAZAKH_TO_RUSSIAN.get(fcms_cyrillic)
        if fcms_russian2 and fcms_russian2 in team_names:
            return True

    # Substring match (for "АЛТАЙ ӨСКЕМЕН" matching "алтай өскемен" in name_kz)
    for tn in team_names:
        if fcms_norm == tn or fcms_norm in tn or tn in fcms_norm:
            return True
        if fcms_cyrillic and (fcms_cyrillic == tn or fcms_cyrillic in tn or tn in fcms_cyrillic):
            return True

    return False


def _update_game_time(game: Game, fcms_match: dict) -> None:
    """Update game time from FCMS if it differs or game has tentative schedule."""
    from datetime import time as dt_time

    fcms_time_str = fcms_match.get("time")
    if not fcms_time_str:
        return

    try:
        parts = fcms_time_str.strip().split(":")
        fcms_time = dt_time(int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return

    if game.time != fcms_time:
        logger.info(
            "  Time updated: game %d %s → %s%s",
            game.id, game.time, fcms_time,
            " (was tentative)" if game.is_schedule_tentative else "",
        )
        game.time = fcms_time

    if game.is_schedule_tentative:
        game.is_schedule_tentative = False


async def bulk_import():
    client = FcmsClient()

    try:
        await client.authenticate()
        logger.info("FCMS authenticated")

        # Get FCMS group IDs from seasons table
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Season.fcms_group_id).where(Season.fcms_group_id.isnot(None))
            )
            group_ids = [row[0] for row in result.all()]

        if not group_ids:
            logger.warning("No seasons with fcms_group_id set, nothing to import")
            return

        logger.info("FCMS group IDs from DB: %s", group_ids)

        # Fetch matches from all FCMS groups
        all_fcms_matches = []
        for group_id in group_ids:
            page = 1
            group_count = 0
            while True:
                data = await client.list_matches(group_id, page=page, limit=100)
                matches = data.get("_embedded", {}).get("matches", [])
                if not matches:
                    break
                all_fcms_matches.extend(matches)
                group_count += len(matches)
                total_pages = data.get("page_count", 1)
                if page >= total_pages:
                    break
                page += 1
            logger.info("Fetched %d matches from FCMS group %d", group_count, group_id)

        logger.info("Total FCMS matches: %d", len(all_fcms_matches))

        async with AsyncSessionLocal() as db:
            # Load all games with team relationships
            games_result = await db.execute(
                select(Game).options(
                    selectinload(Game.home_team),
                    selectinload(Game.away_team),
                )
            )
            all_games = games_result.scalars().all()

            # Group games by date for fast lookup
            from collections import defaultdict
            games_by_date: dict[str, list[Game]] = defaultdict(list)
            for g in all_games:
                if g.home_team and g.away_team and g.date:
                    games_by_date[g.date.isoformat()].append(g)

            matched = 0
            unmatched = []
            already_set = 0

            for fm in all_fcms_matches:
                fcms_id = fm.get("id")
                date_str = fm.get("date") or fm.get("matchDate") or ""
                if not date_str:
                    unmatched.append({"fcms_id": fcms_id, "reason": "no date"})
                    continue

                try:
                    match_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                except (ValueError, TypeError):
                    try:
                        match_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        unmatched.append({"fcms_id": fcms_id, "reason": f"bad date: {date_str}"})
                        continue

                home_name = fm.get("homeCompetitorTitle") or ""
                away_name = fm.get("awayCompetitorTitle") or ""

                # Find matching game by date + team name matching
                candidates = games_by_date.get(match_date.isoformat(), [])
                found_game = None
                for game in candidates:
                    if (team_names_match(home_name, game.home_team)
                            and team_names_match(away_name, game.away_team)):
                        found_game = game
                        break

                if not found_game:
                    unmatched.append({
                        "fcms_id": fcms_id,
                        "reason": f"no game: {match_date} {home_name} vs {away_name}",
                    })
                    continue

                if found_game.fcms_match_id == fcms_id:
                    # Still update time if FCMS has it and ours differs
                    _update_game_time(found_game, fm)
                    already_set += 1
                    continue

                if found_game.fcms_match_id is not None and found_game.fcms_match_id != fcms_id:
                    logger.warning(
                        "Game %d already has fcms_match_id=%d, skipping new=%d",
                        found_game.id, found_game.fcms_match_id, fcms_id,
                    )
                    continue

                found_game.fcms_match_id = fcms_id
                _update_game_time(found_game, fm)
                matched += 1
                logger.info(
                    "Matched: game %d (%s) ← FCMS %d (%s vs %s)",
                    found_game.id, match_date, fcms_id, home_name, away_name,
                )

            await db.commit()

            logger.info("=" * 60)
            logger.info("RESULTS: matched=%d, already_set=%d, unmatched=%d", matched, already_set, len(unmatched))
            if unmatched:
                logger.info("Unmatched FCMS matches:")
                for u in unmatched:
                    logger.info("  FCMS %s: %s", u.get("fcms_id"), u.get("reason"))

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(bulk_import())
