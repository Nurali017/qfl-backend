"""FCMS match sync: bind, update dates/times, create new games, notify Telegram.

Usage: python3 -m scripts.fcms_bulk_import

Runs 2x/day via Celery beat (fcms_bulk_import task).

Flow:
1. Pass 1: Update date/time for games already bound by fcms_match_id
2. Pass 2: Bind unbound FCMS matches by date+teams, or create new games
3. Send Telegram summary of all changes
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, date as date_type, time as time_type

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models import Game, Team, Season
from app.services.fcms_client import FcmsClient
from app.services.telegram import send_telegram_message
from app.config import get_settings
from app.utils.async_celery import run_async

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

settings = get_settings()

# FCMS operators sometimes re-create a match (wrong round number, date typo)
# before we bind it — the new entry then looks like a brand-new fixture. A real
# repeat pairing in the same orientation only happens a full round apart, so a
# nearby twin is a duplicate, not a fixture.
DUPLICATE_GUARD_WINDOW_DAYS = 14


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

    fcms_cyrillic = _LATIN_TO_CYRILLIC.get(fcms_norm)

    team_names = set()
    for attr in ("name", "name_kz", "name_en"):
        val = getattr(team, attr, None)
        if val:
            team_names.add(normalize(val))

    if fcms_norm in team_names:
        return True
    if fcms_cyrillic and fcms_cyrillic in team_names:
        return True

    fcms_russian = _KAZAKH_TO_RUSSIAN.get(fcms_norm)
    if fcms_russian and fcms_russian in team_names:
        return True
    if fcms_cyrillic:
        fcms_russian2 = _KAZAKH_TO_RUSSIAN.get(fcms_cyrillic)
        if fcms_russian2 and fcms_russian2 in team_names:
            return True

    for tn in team_names:
        if fcms_norm == tn or fcms_norm in tn or tn in fcms_norm:
            return True
        if fcms_cyrillic and (fcms_cyrillic == tn or fcms_cyrillic in tn or tn in fcms_cyrillic):
            return True

    return False


def _parse_fcms_date(date_str: str) -> date_type | None:
    """Parse date from FCMS match data."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None


def _parse_fcms_time(time_str: str | None) -> time_type | None:
    """Parse time from FCMS match data."""
    if not time_str:
        return None
    try:
        parts = time_str.strip().split(":")
        return time_type(int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return None


def find_duplicate_twin(
    all_games: list[Game],
    season_id: int,
    home_team_id: int,
    away_team_id: int,
    match_date: date_type,
) -> Game | None:
    """Existing game of the same pairing (same orientation, same season)
    within DUPLICATE_GUARD_WINDOW_DAYS of match_date, else None."""
    return next(
        (
            g for g in all_games
            if g.season_id == season_id
            and g.home_team_id == home_team_id
            and g.away_team_id == away_team_id
            and g.date
            and abs((g.date - match_date).days) <= DUPLICATE_GUARD_WINDOW_DAYS
        ),
        None,
    )


def _parse_csv_ints(s: str | None) -> list[int]:
    if not s:
        return []
    out: list[int] = []
    for part in s.split(","):
        part = part.strip()
        if part:
            try:
                out.append(int(part))
            except ValueError:
                continue
    return out


async def _discover_groups_for_seasons(client: FcmsClient) -> tuple[list[int], dict[int, int], dict[int, str], list[dict]]:
    """Resolve season → group_ids using fcms_competition_id (auto) ∪ fcms_group_id (manual).

    Persists newly discovered groups back to seasons.fcms_group_id (CSV) so the admin
    UI reflects them and the next run starts already-warm.

    Returns (group_ids, group_to_season, group_to_season_name, new_groups_for_telegram).
    """
    new_groups_report: list[dict] = []
    group_ids: list[int] = []
    group_to_season: dict[int, int] = {}
    group_to_season_name: dict[int, str] = {}

    async with AsyncSessionLocal() as db:
        seasons_q = await db.execute(
            select(Season).where(
                (Season.fcms_group_id.isnot(None))
                | (Season.fcms_competition_id.isnot(None))
            )
        )
        seasons = list(seasons_q.scalars().all())

        for season in seasons:
            existing_groups = set(_parse_csv_ints(season.fcms_group_id))
            discovered: set[int] = set()
            titles_by_id: dict[int, str] = {}

            for comp_id in _parse_csv_ints(season.fcms_competition_id):
                try:
                    groups = await client.get_competition_groups(comp_id)
                except Exception as e:
                    logger.warning(
                        "FCMS get_competition_groups(%d) failed for season %d: %s",
                        comp_id, season.id, e,
                    )
                    continue
                for g in groups:
                    gid = g.get("id")
                    if isinstance(gid, int):
                        discovered.add(gid)
                        titles_by_id[gid] = g.get("title") or ""

            new_groups = discovered - existing_groups
            if new_groups:
                logger.info(
                    "Discovered %d new FCMS group(s) for season %d (%s): %s",
                    len(new_groups), season.id, season.name, sorted(new_groups),
                )
                for gid in sorted(new_groups):
                    new_groups_report.append({
                        "season_id": season.id,
                        "season_name": season.name,
                        "group_id": gid,
                        "group_title": titles_by_id.get(gid, ""),
                    })

                # Persist union of existing ∪ discovered — keeps any manually-added IDs.
                merged = sorted(existing_groups | discovered)
                season.fcms_group_id = ",".join(str(g) for g in merged)
                existing_groups = set(merged)

            for gid in existing_groups:
                group_ids.append(gid)
                group_to_season[gid] = season.id
                group_to_season_name[gid] = season.name

        if new_groups_report:
            await db.commit()

    return group_ids, group_to_season, group_to_season_name, new_groups_report


async def bulk_import():
    client = FcmsClient()

    # Collect changes for Telegram summary
    date_changes: list[dict] = []
    time_changes: list[dict] = []
    created_games: list[dict] = []
    suspected_duplicates: list[dict] = []

    try:
        await client.authenticate()
        logger.info("FCMS authenticated")

        # Resolve groups via competition_id auto-discovery + stored fcms_group_id.
        (
            group_ids,
            group_to_season,
            group_to_season_name,
            new_groups_report,
        ) = await _discover_groups_for_seasons(client)

        if not group_ids:
            logger.warning("No seasons with fcms_group_id/fcms_competition_id set, nothing to import")
            return

        logger.info("FCMS group IDs to scan: %s", group_ids)

        # Fetch all FCMS matches, track which group each belongs to
        all_fcms_matches: list[dict] = []
        PAGE_LIMIT = 100
        for group_id in group_ids:
            page = 1
            group_count = 0
            while True:
                data = await client.list_matches(group_id, page=page, limit=PAGE_LIMIT)
                matches = data.get("_embedded", {}).get("matches", [])
                if not matches:
                    break
                for m in matches:
                    m["_group_id"] = group_id
                all_fcms_matches.extend(matches)
                group_count += len(matches)
                # FCMS does not return `page_count`; rely on total_items when
                # present, otherwise stop once a short (< limit) page arrives.
                total_items = data.get("total_items") or data.get("total")
                if total_items is not None:
                    if group_count >= int(total_items):
                        break
                elif len(matches) < PAGE_LIMIT:
                    break
                page += 1
            logger.info("Fetched %d matches from FCMS group %d", group_count, group_id)

        logger.info("Total FCMS matches: %d", len(all_fcms_matches))

        # Build fcms_id → fcms_match lookup
        fcms_by_id: dict[int, dict] = {fm["id"]: fm for fm in all_fcms_matches}

        async with AsyncSessionLocal() as db:
            # Load all games with team relationships
            games_result = await db.execute(
                select(Game).options(
                    selectinload(Game.home_team),
                    selectinload(Game.away_team),
                    selectinload(Game.season),
                )
            )
            all_games = list(games_result.scalars().all())

            # Load all teams with fcms_team_id for creating new games
            teams_result = await db.execute(
                select(Team).where(Team.fcms_team_id.isnot(None))
            )
            teams_by_fcms_id: dict[int, Team] = {
                t.fcms_team_id: t for t in teams_result.scalars().all()
            }

            # ── Pass 1: Update date/time for already-bound games ──
            games_by_fcms_id: dict[int, Game] = {
                g.fcms_match_id: g for g in all_games if g.fcms_match_id is not None
            }
            processed_fcms_ids: set[int] = set()
            already_set = 0

            for fcms_id, game in games_by_fcms_id.items():
                fm = fcms_by_id.get(fcms_id)
                if not fm:
                    continue

                processed_fcms_ids.add(fcms_id)
                already_set += 1

                # Update date
                fcms_date = _parse_fcms_date(fm.get("date") or fm.get("matchDate") or "")
                if fcms_date and game.date != fcms_date:
                    old_date = game.date
                    game.date = fcms_date
                    date_changes.append({
                        "game_id": game.id,
                        "old": str(old_date),
                        "new": str(fcms_date),
                        "home": game.home_team.name if game.home_team else "?",
                        "away": game.away_team.name if game.away_team else "?",
                    })
                    logger.info("  Date updated: game %d %s → %s", game.id, old_date, fcms_date)

                # Update time
                fcms_time = _parse_fcms_time(fm.get("time"))
                if fcms_time and game.time != fcms_time:
                    old_time = game.time
                    game.time = fcms_time
                    time_changes.append({
                        "game_id": game.id,
                        "old": str(old_time) if old_time else "—",
                        "new": str(fcms_time),
                        "home": game.home_team.name if game.home_team else "?",
                        "away": game.away_team.name if game.away_team else "?",
                    })
                    logger.info("  Time updated: game %d %s → %s", game.id, old_time, fcms_time)

                if game.is_schedule_tentative:
                    game.is_schedule_tentative = False

            # ── Pass 2: Bind or create for unprocessed FCMS matches ──
            # Rebuild games_by_date (dates may have changed in pass 1)
            games_by_date: dict[str, list[Game]] = defaultdict(list)
            for g in all_games:
                if g.home_team and g.away_team and g.date:
                    games_by_date[g.date.isoformat()].append(g)

            matched = 0
            created = 0
            unmatched = []

            for fm in all_fcms_matches:
                fcms_id = fm["id"]
                if fcms_id in processed_fcms_ids:
                    continue

                date_str = fm.get("date") or fm.get("matchDate") or ""
                match_date = _parse_fcms_date(date_str)
                if not match_date:
                    unmatched.append({"fcms_id": fcms_id, "reason": f"bad date: {date_str}"})
                    continue

                home_name = fm.get("homeCompetitorTitle") or ""
                away_name = fm.get("awayCompetitorTitle") or ""

                # Try to find existing game by date + teams (name match OR fcms_team_id match)
                home_fcms_team_id = fm.get("homeCompetitorTeamId")
                away_fcms_team_id = fm.get("awayCompetitorTeamId")
                candidates = games_by_date.get(match_date.isoformat(), [])
                found_game = None
                for game in candidates:
                    # Match by fcms_team_id (most reliable)
                    home_match = (
                        (game.home_team and game.home_team.fcms_team_id == home_fcms_team_id)
                        if home_fcms_team_id else False
                    )
                    away_match = (
                        (game.away_team and game.away_team.fcms_team_id == away_fcms_team_id)
                        if away_fcms_team_id else False
                    )
                    if home_match and away_match:
                        found_game = game
                        break
                    # Fallback: match by name
                    if (team_names_match(home_name, game.home_team)
                            and team_names_match(away_name, game.away_team)):
                        found_game = game
                        break

                if found_game:
                    if found_game.fcms_match_id is not None and found_game.fcms_match_id != fcms_id:
                        logger.warning(
                            "Game %d already has fcms_match_id=%d, skipping new=%d",
                            found_game.id, found_game.fcms_match_id, fcms_id,
                        )
                        continue

                    found_game.fcms_match_id = fcms_id
                    fcms_time = _parse_fcms_time(fm.get("time"))
                    if fcms_time and found_game.time != fcms_time:
                        found_game.time = fcms_time
                    if found_game.is_schedule_tentative:
                        found_game.is_schedule_tentative = False
                    matched += 1
                    logger.info(
                        "Matched: game %d (%s) ← FCMS %d (%s vs %s)",
                        found_game.id, match_date, fcms_id, home_name, away_name,
                    )
                    continue

                # Try to create new game
                home_fcms_team_id = fm.get("homeCompetitorTeamId")
                away_fcms_team_id = fm.get("awayCompetitorTeamId")
                home_team = teams_by_fcms_id.get(home_fcms_team_id) if home_fcms_team_id else None
                away_team = teams_by_fcms_id.get(away_fcms_team_id) if away_fcms_team_id else None

                group_id = fm.get("_group_id")
                season_id = group_to_season.get(group_id) if group_id else None

                if home_team and away_team and season_id:
                    fcms_time = _parse_fcms_time(fm.get("time"))
                    tour = fm.get("matchDayNumber")

                    twin = find_duplicate_twin(
                        all_games, season_id, home_team.id, away_team.id, match_date
                    )
                    if twin:
                        suspected_duplicates.append({
                            "fcms_id": fcms_id,
                            "date": str(match_date),
                            "time": str(fcms_time) if fcms_time else "—",
                            "home": home_team.name,
                            "away": away_team.name,
                            "season": group_to_season_name.get(group_id, "?"),
                            "tour": tour,
                            "twin_game_id": twin.id,
                            "twin_date": str(twin.date),
                            "twin_tour": twin.tour,
                        })
                        logger.warning(
                            "Suspected duplicate: FCMS %d (%s %s vs %s, tour %s) "
                            "near game %d (%s, tour %s) — NOT creating",
                            fcms_id, match_date, home_team.name, away_team.name,
                            tour, twin.id, twin.date, twin.tour,
                        )
                        continue

                    new_game = Game(
                        date=match_date,
                        time=fcms_time,
                        season_id=season_id,
                        home_team_id=home_team.id,
                        away_team_id=away_team.id,
                        fcms_match_id=fcms_id,
                        tour=int(tour) if tour else None,
                    )
                    db.add(new_game)
                    # Visible to the duplicate guard for later matches this run
                    all_games.append(new_game)
                    created += 1
                    season_name = group_to_season_name.get(group_id, "?")
                    created_games.append({
                        "date": str(match_date),
                        "time": str(fcms_time) if fcms_time else "—",
                        "home": home_team.name,
                        "away": away_team.name,
                        "season": season_name,
                        "tour": tour,
                    })
                    logger.info(
                        "Created: %s %s %s vs %s (FCMS %d, %s)",
                        match_date, fcms_time or "??:??", home_team.name, away_team.name,
                        fcms_id, season_name,
                    )
                else:
                    reason_parts = []
                    if not home_team:
                        reason_parts.append(f"home fcms_team_id={home_fcms_team_id} not found")
                    if not away_team:
                        reason_parts.append(f"away fcms_team_id={away_fcms_team_id} not found")
                    if not season_id:
                        reason_parts.append(f"no season for group {group_id}")
                    unmatched.append({
                        "fcms_id": fcms_id,
                        "reason": f"can't create: {match_date} {home_name} vs {away_name} ({', '.join(reason_parts)})",
                    })

            await db.commit()

            logger.info("=" * 60)
            logger.info(
                "RESULTS: matched=%d, already_set=%d, created=%d, "
                "date_changes=%d, time_changes=%d, unmatched=%d",
                matched, already_set, created,
                len(date_changes), len(time_changes), len(unmatched),
            )
            if unmatched:
                logger.info("Unmatched FCMS matches:")
                for u in unmatched:
                    logger.info("  FCMS %s: %s", u.get("fcms_id"), u.get("reason"))

        # ── Telegram summary ──
        has_changes = (
            created_games or date_changes or time_changes
            or new_groups_report or suspected_duplicates
        )
        if has_changes:
            lines = ["<b>\U0001f4cb FCMS Sync</b>", ""]

            if new_groups_report:
                lines.append(f"\U0001f195 <b>Новые раунды FCMS: {len(new_groups_report)}</b>")
                for ng in new_groups_report:
                    title = ng.get("group_title") or f"group {ng['group_id']}"
                    lines.append(
                        f"  • [{ng['season_name']}] {title} (group_id={ng['group_id']})"
                    )
                lines.append("")

            if created_games:
                lines.append(f"\U0001f195 <b>Создано: {len(created_games)}</b>")
                for g in created_games:
                    tour_str = f" (тур {g['tour']})" if g.get("tour") else ""
                    lines.append(f"  \u2022 {g['date']} {g['time']} {g['home']} — {g['away']} ({g['season']}{tour_str})")
                lines.append("")

            if suspected_duplicates:
                lines.append(f"⚠️ <b>Подозрение на дубль (не создано): {len(suspected_duplicates)}</b>")
                for d in suspected_duplicates:
                    tour_str = f", тур {d['tour']}" if d.get("tour") else ""
                    twin_tour_str = f", тур {d['twin_tour']}" if d.get("twin_tour") else ""
                    lines.append(
                        f"  • FCMS {d['fcms_id']}: {d['date']} {d['time']} "
                        f"{d['home']} — {d['away']} ({d['season']}{tour_str}) "
                        f"похож на game #{d['twin_game_id']} ({d['twin_date']}{twin_tour_str})"
                    )
                lines.append("")

            if date_changes:
                lines.append(f"\U0001f4c5 <b>Дата изменена: {len(date_changes)}</b>")
                for c in date_changes:
                    lines.append(f"  \u2022 Game #{c['game_id']}: {c['old']} \u2192 {c['new']} ({c['home']} — {c['away']})")
                lines.append("")

            if time_changes:
                lines.append(f"\u23f0 <b>Время изменено: {len(time_changes)}</b>")
                for c in time_changes:
                    lines.append(f"  \u2022 Game #{c['game_id']}: {c['old']} \u2192 {c['new']} ({c['home']} — {c['away']})")

            await send_telegram_message("\n".join(lines))

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(bulk_import())
