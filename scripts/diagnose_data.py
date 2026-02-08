"""
QFL Data Diagnostic Report
===========================
Scans the database and reports fill-rate for critical fields across all tables.

Usage:
    cd backend && python scripts/diagnose_data.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, func, and_, or_, inspect as sa_inspect, String, Text

from app.database import AsyncSessionLocal
from app.models import (
    Team, Player, News, Game, GameEvent, GameLineup,
    Coach, Referee, Country, Stadium,
)


# ANSI colors
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"


def status_color(filled: int, total: int) -> str:
    if total == 0:
        return f"{YELLOW}N/A{RESET}"
    pct = filled / total * 100
    if pct >= 80:
        tag = f"{GREEN}OK{RESET}"
    elif pct >= 50:
        tag = f"{YELLOW}WARNING{RESET}"
    else:
        tag = f"{RED}CRITICAL{RESET}"
    return f"{filled}/{total} ({pct:.1f}%) {tag}"


def pct_str(filled: int, total: int) -> str:
    if total == 0:
        return "N/A"
    return f"{filled / total * 100:.1f}%"


async def count_filled(db, model, column):
    """Count rows where column IS NOT NULL (and != '' for string columns)."""
    col = getattr(model, column)
    total_q = await db.execute(select(func.count()).select_from(model))
    total = total_q.scalar()

    # Only check != '' for string/text columns, not integer/date/etc.
    col_type = col.property.columns[0].type
    is_string = isinstance(col_type, (String, Text))

    conditions = [col.isnot(None)]
    if is_string:
        conditions.append(col != "")

    filled_q = await db.execute(
        select(func.count()).select_from(model).where(and_(*conditions))
    )
    filled = filled_q.scalar()
    return filled, total


async def count_where(db, model, *conditions):
    """Count rows matching conditions."""
    q = select(func.count()).select_from(model)
    for cond in conditions:
        q = q.where(cond)
    result = await db.execute(q)
    return result.scalar()


async def diagnose_teams(db):
    filled_logo, total = await count_filled(db, Team, "logo_url")
    filled_primary, _ = await count_filled(db, Team, "primary_color")
    filled_secondary, _ = await count_filled(db, Team, "secondary_color")
    filled_city, _ = await count_filled(db, Team, "city")
    filled_stadium, _ = await count_filled(db, Team, "stadium_id")
    filled_website, _ = await count_filled(db, Team, "website")

    print(f"\n{BOLD}--- Teams ({total} total) ---{RESET}")
    print(f"  logo_url:        {status_color(filled_logo, total)}")
    print(f"  primary_color:   {status_color(filled_primary, total)}")
    print(f"  secondary_color: {status_color(filled_secondary, total)}")
    print(f"  city:            {status_color(filled_city, total)}")
    print(f"  stadium_id:      {status_color(filled_stadium, total)}")
    print(f"  website:         {status_color(filled_website, total)}")


async def diagnose_players(db):
    filled_photo, total = await count_filled(db, Player, "photo_url")
    filled_fn, _ = await count_filled(db, Player, "first_name")
    filled_ln, _ = await count_filled(db, Player, "last_name")
    filled_bday, _ = await count_filled(db, Player, "birthday")
    filled_role, _ = await count_filled(db, Player, "top_role")
    filled_country, _ = await count_filled(db, Player, "country_id")

    print(f"\n{BOLD}--- Players ({total} total) ---{RESET}")
    print(f"  photo_url:       {status_color(filled_photo, total)}")
    print(f"  first_name:      {status_color(filled_fn, total)}")
    print(f"  last_name:       {status_color(filled_ln, total)}")
    print(f"  birthday:        {status_color(filled_bday, total)}")
    print(f"  top_role:        {status_color(filled_role, total)}")
    print(f"  country_id:      {status_color(filled_country, total)}")


async def diagnose_news(db):
    filled_content, total = await count_filled(db, News, "content")
    filled_image, _ = await count_filled(db, News, "image_url")
    filled_excerpt, _ = await count_filled(db, News, "excerpt")
    filled_date, _ = await count_filled(db, News, "publish_date")
    filled_category, _ = await count_filled(db, News, "category")

    print(f"\n{BOLD}--- News ({total} total) ---{RESET}")
    print(f"  content:         {status_color(filled_content, total)}")
    print(f"  image_url:       {status_color(filled_image, total)}")
    print(f"  excerpt:         {status_color(filled_excerpt, total)}")
    print(f"  publish_date:    {status_color(filled_date, total)}")
    print(f"  category:        {status_color(filled_category, total)}")


async def diagnose_games(db):
    total_q = await db.execute(select(func.count()).select_from(Game))
    total = total_q.scalar()

    has_lineup = await count_where(db, Game, Game.has_lineup == True)
    has_stats = await count_where(db, Game, Game.has_stats == True)
    has_score = await count_where(db, Game, Game.home_score.isnot(None), Game.away_score.isnot(None))
    has_time = await count_where(db, Game, Game.time.isnot(None))
    has_formation = await count_where(db, Game, Game.home_formation.isnot(None))

    # Games with at least one event
    games_with_events_q = await db.execute(
        select(func.count(func.distinct(GameEvent.game_id)))
    )
    games_with_events = games_with_events_q.scalar()

    # Games with at least one lineup entry
    games_with_lineups_q = await db.execute(
        select(func.count(func.distinct(GameLineup.game_id)))
    )
    games_with_lineups = games_with_lineups_q.scalar()

    no_lineup = total - has_lineup
    no_stats = total - has_stats

    print(f"\n{BOLD}--- Games ({total} total) ---{RESET}")
    print(f"  has_score:       {status_color(has_score, total)}")
    print(f"  has_time:        {status_color(has_time, total)}")
    print(f"  has_lineup flag: {status_color(has_lineup, total)}")
    print(f"  has_stats flag:  {status_color(has_stats, total)}")
    print(f"  formation:       {status_color(has_formation, total)}")
    print(f"  with events:     {status_color(games_with_events, total)}")
    print(f"  with lineups:    {status_color(games_with_lineups, total)}")


async def diagnose_events(db):
    total_q = await db.execute(select(func.count()).select_from(GameEvent))
    total = total_q.scalar()

    with_player_id = await count_where(db, GameEvent, GameEvent.player_id.isnot(None))
    without_player_id = total - with_player_id

    # Events that have a name but no player_id
    name_but_no_id = await count_where(
        db, GameEvent,
        GameEvent.player_id.is_(None),
        GameEvent.player_name.isnot(None),
        GameEvent.player_name != "",
    )

    print(f"\n{BOLD}--- Game Events ({total} total) ---{RESET}")
    print(f"  with player_id:  {status_color(with_player_id, total)}")
    print(f"  name but no id:  {name_but_no_id} events (unlinked players)")


async def diagnose_lineups(db):
    total_q = await db.execute(select(func.count()).select_from(GameLineup))
    total = total_q.scalar()

    filled_amplua, _ = await count_filled(db, GameLineup, "amplua")
    filled_position, _ = await count_filled(db, GameLineup, "field_position")
    filled_number, _ = await count_filled(db, GameLineup, "shirt_number")

    print(f"\n{BOLD}--- Game Lineups ({total} total) ---{RESET}")
    print(f"  amplua:          {status_color(filled_amplua, total)}")
    print(f"  field_position:  {status_color(filled_position, total)}")
    print(f"  shirt_number:    {status_color(filled_number, total)}")


async def diagnose_coaches(db):
    filled_photo, total = await count_filled(db, Coach, "photo_url")
    filled_country, _ = await count_filled(db, Coach, "country_id")

    print(f"\n{BOLD}--- Coaches ({total} total) ---{RESET}")
    print(f"  photo_url:       {status_color(filled_photo, total)}")
    print(f"  country_id:      {status_color(filled_country, total)}")


async def diagnose_referees(db):
    filled_photo, total = await count_filled(db, Referee, "photo_url")
    filled_country, _ = await count_filled(db, Referee, "country_id")

    print(f"\n{BOLD}--- Referees ({total} total) ---{RESET}")
    print(f"  photo_url:       {status_color(filled_photo, total)}")
    print(f"  country_id:      {status_color(filled_country, total)}")


async def diagnose_countries(db):
    filled_flag, total = await count_filled(db, Country, "flag_url")
    filled_name_kz, _ = await count_filled(db, Country, "name_kz")
    filled_name_en, _ = await count_filled(db, Country, "name_en")

    print(f"\n{BOLD}--- Countries ({total} total) ---{RESET}")
    print(f"  flag_url:        {status_color(filled_flag, total)}")
    print(f"  name_kz:         {status_color(filled_name_kz, total)}")
    print(f"  name_en:         {status_color(filled_name_en, total)}")


async def diagnose_stadiums(db):
    filled_capacity, total = await count_filled(db, Stadium, "capacity")
    filled_address, _ = await count_filled(db, Stadium, "address")
    filled_city, _ = await count_filled(db, Stadium, "city")

    print(f"\n{BOLD}--- Stadiums ({total} total) ---{RESET}")
    print(f"  capacity:        {status_color(filled_capacity, total)}")
    print(f"  address:         {status_color(filled_address, total)}")
    print(f"  city:            {status_color(filled_city, total)}")


async def main():
    print(f"\n{BOLD}{'=' * 45}")
    print(f"  QFL Data Diagnostic Report")
    print(f"{'=' * 45}{RESET}")

    async with AsyncSessionLocal() as db:
        await diagnose_teams(db)
        await diagnose_players(db)
        await diagnose_news(db)
        await diagnose_games(db)
        await diagnose_events(db)
        await diagnose_lineups(db)
        await diagnose_coaches(db)
        await diagnose_referees(db)
        await diagnose_countries(db)
        await diagnose_stadiums(db)

    print(f"\n{BOLD}{'=' * 45}")
    print(f"  Legend: {GREEN}OK{RESET} (>=80%)  {YELLOW}WARNING{RESET} (50-80%)  {RED}CRITICAL{RESET} (<50%)")
    print(f"{BOLD}{'=' * 45}{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
