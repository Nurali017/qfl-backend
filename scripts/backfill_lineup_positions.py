#!/usr/bin/env python3
"""
Backfill game_lineups.amplua & field_position from multiple data sources.

Data sources (by priority):
  1. VSporte API — broadcast.vsporte.ru (requires games.vsporte_id)
  2. SOTA API — sota.id/em/ (requires games.sota_id)
  3. players.top_role — general position from SOTA player data
  4. Legacy contracts.amplua — GK(1)/D(2)/M(3)/F(4) from MySQL

Run from host machine (not Docker) — all services are port-mapped locally.

Usage:
    # Step 1: Populate vsporte_id from legacy MySQL
    python scripts/backfill_lineup_positions.py --step=vsporte-ids

    # Step 2: Dry-run full backfill (no changes)
    python scripts/backfill_lineup_positions.py --dry-run

    # Step 3: Apply full backfill
    python scripts/backfill_lineup_positions.py

    # Backfill only from players.top_role (no API calls)
    python scripts/backfill_lineup_positions.py --source=top_role

    # Backfill only from legacy contracts
    python scripts/backfill_lineup_positions.py --source=legacy
"""

import argparse
import asyncio
import logging
import re
import subprocess
import sys
from collections import defaultdict

import httpx
import psycopg2

# ---------------------------------------------------------------------------
# Connection constants
# ---------------------------------------------------------------------------
MYSQL_CONTAINER = "kff-mysql"
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpassword"
MYSQL_DB = "p-348074_db"

PG_HOST = "localhost"
PG_PORT = 5434
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "qfl_db"

BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# Position mapping (same as backend/app/api/games.py)
# ---------------------------------------------------------------------------
POSITION_CODE_TO_AMPLUA = {
    "GK": "Gk", "G": "Gk", "ВР": "Gk", "ГК": "Gk", "ВРТ": "Gk",
    "CD": "D", "LD": "D", "RD": "D", "LB": "D", "RB": "D", "CB": "D", "D": "D",
    "ЛЗ": "D", "ПЗ": "D", "ЦЗ": "D", "ЗЩ": "D", "ЗАЩ": "D",
    "DM": "DM", "ОП": "DM",
    "CM": "M", "M": "M", "LM": "M", "RM": "M", "LW": "M", "RW": "M",
    "ЦП": "M", "ЛП": "M", "ПП": "M",
    "AM": "AM", "АП": "AM",
    "CF": "F", "ST": "F", "FW": "F", "F": "F",
    "ЦН": "F", "НП": "F", "ЦФ": "F", "НАП": "F", "ЛН": "F", "ПН": "F",
}

LEGACY_AMPLUA_MAP = {
    1: "Gk",  # GK
    2: "D",   # Defender
    3: "M",   # Midfielder
    4: "F",   # Forward
}

VALID_AMPLUA = {"Gk", "D", "DM", "M", "AM", "F"}
VALID_FIELD_POSITIONS = {"L", "LC", "C", "RC", "R"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mysql_query(sql: str) -> list[dict]:
    """Run a MySQL query via docker exec and return rows as dicts."""
    cmd = [
        "docker", "exec", MYSQL_CONTAINER,
        "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASSWORD}",
        MYSQL_DB, "--default-character-set=utf8mb4",
        "-e", sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        stderr = result.stderr.replace(
            "mysql: [Warning] Using a password on the command line interface can be insecure.\n", ""
        )
        if stderr.strip():
            raise RuntimeError(f"MySQL query failed: {stderr.strip()}")
    output = result.stdout.strip()
    if not output:
        return []
    lines = output.split("\n")
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        vals = line.split("\t")
        row = {}
        for h, v in zip(headers, vals):
            row[h] = None if v == "NULL" else v
        rows.append(row)
    return rows


def get_pg_conn():
    """Get a psycopg2 connection to the QFL PostgreSQL database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB,
    )


def infer_amplua_from_role_hint(role_hint: str | None) -> str | None:
    if not isinstance(role_hint, str) or not role_hint.strip():
        return None
    normalized = role_hint.strip().upper()
    token = (normalized.split(maxsplit=1)[0] if normalized else "").strip()
    code = re.sub(r"[^A-ZА-ЯЁ0-9]", "", token)
    mapped = POSITION_CODE_TO_AMPLUA.get(code)
    if mapped:
        return mapped
    if "ВРАТ" in normalized:
        return "Gk"
    if "ОПОР" in normalized:
        return "DM"
    if "ПОЛУЗАЩ" in normalized:
        return "M"
    if "ЗАЩИТ" in normalized or "ЗАЩ" in normalized:
        return "D"
    if "НАПАД" in normalized or "НАП" in normalized:
        return "F"
    return None


def infer_field_position_from_role_hint(role_hint: str | None) -> str | None:
    if not isinstance(role_hint, str) or not role_hint.strip():
        return None
    normalized = role_hint.strip().upper()
    token = (normalized.split(maxsplit=1)[0] if normalized else "").strip()
    code = re.sub(r"[^A-ZА-ЯЁ0-9]", "", token)
    if code in {"L", "LB", "LD", "LM", "LW", "ЛЗ", "ЛП", "ЛН"}:
        return "L"
    if code in {"R", "RB", "RD", "RM", "RW", "ПЗ", "ПП", "ПН"}:
        return "R"
    if code in {"LC", "ЛЦ", "ЛЦЗ"}:
        return "LC"
    if code in {"RC", "ПЦ", "ПЦЗ"}:
        return "RC"
    if code in {"C", "CB", "CD", "CM", "CF", "ST", "DM", "AM", "ЦЗ", "ЦП", "ЦН", "ЦФ", "ОП"}:
        return "C"
    has_left = "ЛЕВ" in normalized
    has_right = "ПРАВ" in normalized
    has_center = "ЦЕНТР" in normalized or "CENTER" in normalized
    if has_left and has_center:
        return "LC"
    if has_right and has_center:
        return "RC"
    if has_left:
        return "L"
    if has_right:
        return "R"
    if has_center:
        return "C"
    return None


def normalize_amplua(amplua: str | None, *, gk: bool = False) -> str | None:
    if gk:
        return "Gk"
    if not isinstance(amplua, str):
        return None
    value = amplua.strip().upper()
    mapping = {"GK": "Gk", "D": "D", "DM": "DM", "M": "M", "AM": "AM", "F": "F"}
    return mapping.get(value)


def normalize_field_position(position: str | None) -> str | None:
    if not isinstance(position, str):
        return None
    value = position.strip().upper()
    return value if value in VALID_FIELD_POSITIONS else None


# ---------------------------------------------------------------------------
# Step: Populate vsporte_id
# ---------------------------------------------------------------------------

def step_vsporte_ids(dry_run: bool = False):
    """Populate games.vsporte_id from legacy MySQL matches.vsporte_id."""
    logger.info("Fetching vsporte_id from legacy MySQL...")
    rows = mysql_query(
        "SELECT id, vsporte_id FROM matches WHERE vsporte_id IS NOT NULL AND vsporte_id <> ''"
    )
    logger.info("Found %d matches with vsporte_id in legacy MySQL", len(rows))

    if not rows:
        return

    conn = get_pg_conn()
    cur = conn.cursor()

    updated = 0
    skipped = 0
    not_found = 0

    for row in rows:
        legacy_id = int(row["id"])
        vsporte_id = row["vsporte_id"]

        # Find game by legacy_id
        cur.execute(
            "SELECT id, vsporte_id FROM games WHERE legacy_id = %s", (legacy_id,)
        )
        game = cur.fetchone()
        if not game:
            not_found += 1
            continue

        game_id, existing_vsporte_id = game
        if existing_vsporte_id:
            skipped += 1
            continue

        if dry_run:
            updated += 1
            continue

        cur.execute(
            "UPDATE games SET vsporte_id = %s WHERE id = %s",
            (vsporte_id, game_id),
        )
        updated += 1

        if updated % BATCH_SIZE == 0:
            conn.commit()
            logger.info("  ... committed %d updates", updated)

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    prefix = "[DRY-RUN] " if dry_run else ""
    logger.info(
        "%svsporte_id: updated=%d, skipped(already set)=%d, not_found=%d",
        prefix, updated, skipped, not_found,
    )


# ---------------------------------------------------------------------------
# Step: Backfill from APIs (VSporte + SOTA)
# ---------------------------------------------------------------------------

async def fetch_vsporte_lineup(vsporte_id: str, side: str) -> list[dict] | None:
    """Fetch lineup from VSporte. side: 'host' or 'guest'."""
    url = f"https://broadcast.vsporte.ru/api/v2/qfl/files/{vsporte_id}_team_{side}.json"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
    except Exception as exc:
        logger.debug("VSporte %s %s failed: %s", vsporte_id, side, exc)
    return None


async def fetch_sota_lineup(sota_id: str, side: str, access_token: str) -> list[dict] | None:
    """Fetch lineup from SOTA. side: 'home' or 'away'."""
    url = f"https://sota.id/em/{sota_id}-team-{side}.json"
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(url, params={"access_token": access_token})
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
    except Exception as exc:
        logger.debug("SOTA %s %s failed: %s", sota_id, side, exc)
    return None


async def get_sota_token() -> str:
    """Authenticate to SOTA and return access token."""
    # Read credentials from .env or use defaults from config
    import os
    email = os.environ.get("SOTA_API_EMAIL", "")
    password = os.environ.get("SOTA_API_PASSWORD", "")
    if not email or not password:
        # Try multiple .env paths
        candidates = [".env", "backend/.env", "../.env"]
        for env_path in candidates:
            try:
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" not in line:
                            continue
                        key, val = line.split("=", 1)
                        # Strip quotes but preserve special chars like $
                        val = val.strip()
                        if (val.startswith('"') and val.endswith('"')) or \
                           (val.startswith("'") and val.endswith("'")):
                            val = val[1:-1]
                        if key == "SOTA_API_EMAIL":
                            email = val
                        elif key == "SOTA_API_PASSWORD":
                            password = val
                if email and password:
                    break
            except FileNotFoundError:
                continue
    if not email or not password:
        raise RuntimeError("SOTA_API_EMAIL and SOTA_API_PASSWORD must be set in environment or .env")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://sota.id/api/auth/token/",
            json={"email": email, "password": password},
        )
        resp.raise_for_status()
        return resp.json()["access"]


def parse_lineup_items(lineup_data: list[dict]) -> dict[int, dict]:
    """Parse lineup JSON into {shirt_number: {amplua, field_position}}."""
    result = {}
    for item in lineup_data:
        number = item.get("number")
        if not isinstance(number, (int, str)):
            continue
        if isinstance(number, str):
            if number in {"TEAM", "FORMATION", "COACH", "MAIN", "ОСНОВНЫЕ", "ЗАПАСНЫЕ"}:
                continue
            try:
                number = int(number)
            except ValueError:
                continue

        gk_flag_raw = item.get("gk", item.get("is_gk", False))
        gk_flag = bool(gk_flag_raw) if gk_flag_raw != "" else False
        amplua = normalize_amplua(item.get("amplua"), gk=gk_flag)
        field_position = normalize_field_position(item.get("position"))

        if amplua or field_position:
            result[number] = {"amplua": amplua, "field_position": field_position}
    return result


async def step_api_backfill(dry_run: bool = False):
    """Backfill from VSporte and SOTA APIs."""
    conn = get_pg_conn()
    cur = conn.cursor()

    # Get games that have lineup entries missing amplua
    cur.execute("""
        SELECT DISTINCT g.id, g.sota_id, g.vsporte_id,
               g.home_team_id, g.away_team_id
        FROM games g
        JOIN game_lineups gl ON gl.game_id = g.id
        WHERE gl.amplua IS NULL
          AND (g.sota_id IS NOT NULL OR g.vsporte_id IS NOT NULL)
        ORDER BY g.id
    """)
    games = cur.fetchall()
    logger.info("Found %d games with missing positions and API IDs", len(games))

    sota_token = None
    total_updated = 0
    games_updated = 0
    games_failed = 0

    for game_id, sota_id, vsporte_id, home_team_id, away_team_id in games:
        # Get lineups missing amplua for this game
        cur.execute("""
            SELECT gl.id, gl.team_id, gl.shirt_number
            FROM game_lineups gl
            WHERE gl.game_id = %s AND gl.amplua IS NULL AND gl.shirt_number IS NOT NULL
        """, (game_id,))
        missing_lineups = cur.fetchall()
        if not missing_lineups:
            continue

        # Build lookup: team_id -> side
        side_map = {}
        if home_team_id:
            side_map[home_team_id] = "home"
        if away_team_id:
            side_map[away_team_id] = "away"

        # Fetch lineup data from APIs
        # {team_id: {shirt_number: {amplua, field_position}}}
        team_positions: dict[int, dict[int, dict]] = {}
        game_has_data = False

        for team_id, side in side_map.items():
            lineup_data = None

            # Try VSporte first
            if vsporte_id:
                vsporte_side = "host" if side == "home" else "guest"
                lineup_data = await fetch_vsporte_lineup(vsporte_id, vsporte_side)

            # Fallback to SOTA
            if lineup_data is None and sota_id:
                if sota_token is None:
                    try:
                        sota_token = await get_sota_token()
                    except Exception as exc:
                        logger.warning("Failed to get SOTA token: %s", exc)
                        sota_token = ""

                if sota_token:
                    lineup_data = await fetch_sota_lineup(str(sota_id), side, sota_token)

            if lineup_data:
                parsed = parse_lineup_items(lineup_data)
                if parsed:
                    team_positions[team_id] = parsed
                    game_has_data = True

        if not game_has_data:
            games_failed += 1
            continue

        # Apply updates
        game_updated = 0
        for gl_id, team_id, shirt_number in missing_lineups:
            positions = team_positions.get(team_id, {})
            pos = positions.get(shirt_number)
            if not pos:
                continue

            amplua = pos.get("amplua")
            field_position = pos.get("field_position")
            if not amplua and not field_position:
                continue

            # Default field_position to C if we have amplua but no position
            if amplua and not field_position:
                field_position = "C"

            if dry_run:
                game_updated += 1
                continue

            updates = []
            params = []
            if amplua:
                updates.append("amplua = %s")
                params.append(amplua)
            if field_position:
                updates.append("field_position = %s")
                params.append(field_position)
            params.append(gl_id)

            cur.execute(
                f"UPDATE game_lineups SET {', '.join(updates)} WHERE id = %s",
                params,
            )
            game_updated += 1

        if game_updated > 0:
            total_updated += game_updated
            games_updated += 1

        if not dry_run and games_updated % 50 == 0 and games_updated > 0:
            conn.commit()
            logger.info("  ... committed after %d games, %d lineups updated", games_updated, total_updated)

    if not dry_run:
        conn.commit()

    prefix = "[DRY-RUN] " if dry_run else ""
    logger.info(
        "%sAPI backfill: games_updated=%d, games_failed=%d, lineups_updated=%d",
        prefix, games_updated, games_failed, total_updated,
    )

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Step: Backfill from players.top_role
# ---------------------------------------------------------------------------

def step_top_role_backfill(dry_run: bool = False):
    """Backfill amplua/field_position from players.top_role."""
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT gl.id, p.top_role
        FROM game_lineups gl
        JOIN players p ON p.id = gl.player_id
        WHERE gl.amplua IS NULL
          AND p.top_role IS NOT NULL
          AND p.top_role <> ''
    """)
    rows = cur.fetchall()
    logger.info("Found %d lineups to backfill from players.top_role", len(rows))

    updated = 0
    for gl_id, top_role in rows:
        amplua = infer_amplua_from_role_hint(top_role)
        field_position = infer_field_position_from_role_hint(top_role)

        if not amplua:
            continue

        # Default field_position to C if only amplua is known
        if not field_position:
            field_position = "C"

        if dry_run:
            updated += 1
            continue

        cur.execute(
            "UPDATE game_lineups SET amplua = %s, field_position = %s WHERE id = %s",
            (amplua, field_position, gl_id),
        )
        updated += 1

        if updated % BATCH_SIZE == 0:
            conn.commit()
            logger.info("  ... committed %d updates", updated)

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    prefix = "[DRY-RUN] " if dry_run else ""
    logger.info("%stop_role backfill: updated=%d out of %d candidates", prefix, updated, len(rows))


# ---------------------------------------------------------------------------
# Step: Backfill from legacy contracts.amplua
# ---------------------------------------------------------------------------

def step_legacy_backfill(dry_run: bool = False):
    """Backfill from legacy MySQL contracts.amplua field."""
    logger.info("Fetching contracts.amplua from legacy MySQL...")

    rows = mysql_query(
        "SELECT c.people_id, c.amplua, c.season_id "
        "FROM contracts c "
        "WHERE c.amplua IS NOT NULL AND c.amplua > 0 "
        "ORDER BY c.season_id DESC"
    )
    logger.info("Found %d contract rows with amplua", len(rows))

    # Build mapping: people_id -> amplua (latest season wins)
    legacy_amplua: dict[int, int] = {}
    for row in rows:
        pid = int(row["people_id"])
        if pid not in legacy_amplua:
            legacy_amplua[pid] = int(row["amplua"])

    logger.info("Unique players with legacy amplua: %d", len(legacy_amplua))

    conn = get_pg_conn()
    cur = conn.cursor()

    # Get lineups still missing amplua, join to players to get legacy_id
    cur.execute("""
        SELECT gl.id, p.legacy_id
        FROM game_lineups gl
        JOIN players p ON p.id = gl.player_id
        WHERE gl.amplua IS NULL
          AND p.legacy_id IS NOT NULL
    """)
    missing = cur.fetchall()
    logger.info("Found %d lineups still missing amplua with legacy_id", len(missing))

    updated = 0
    for gl_id, legacy_id in missing:
        amplua_code = legacy_amplua.get(legacy_id)
        if amplua_code is None:
            continue

        amplua = LEGACY_AMPLUA_MAP.get(amplua_code)
        if not amplua:
            continue

        field_position = "C"  # Default

        if dry_run:
            updated += 1
            continue

        cur.execute(
            "UPDATE game_lineups SET amplua = %s, field_position = %s WHERE id = %s",
            (amplua, field_position, gl_id),
        )
        updated += 1

        if updated % BATCH_SIZE == 0:
            conn.commit()
            logger.info("  ... committed %d updates", updated)

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    prefix = "[DRY-RUN] " if dry_run else ""
    logger.info("%slegacy backfill: updated=%d out of %d candidates", prefix, updated, len(missing))


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def print_stats():
    """Print current amplua/field_position fill rates."""
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(amplua) as has_amplua,
            COUNT(field_position) as has_fp
        FROM game_lineups
    """)
    total, has_amplua, has_fp = cur.fetchone()

    logger.info("=== Current Stats ===")
    logger.info("Total lineups: %d", total)
    logger.info("Has amplua:    %d (%.1f%%)", has_amplua, has_amplua / total * 100 if total else 0)
    logger.info("Has field_pos: %d (%.1f%%)", has_fp, has_fp / total * 100 if total else 0)
    logger.info("Missing both:  %d (%.1f%%)", total - has_amplua, (total - has_amplua) / total * 100 if total else 0)

    # Per-season breakdown
    cur.execute("""
        SELECT s.name, g.season_id,
               COUNT(gl.id) as total,
               COUNT(gl.amplua) as has_amplua
        FROM game_lineups gl
        JOIN games g ON gl.game_id = g.id
        JOIN seasons s ON g.season_id = s.id
        GROUP BY g.season_id, s.name
        ORDER BY g.season_id
    """)
    rows = cur.fetchall()
    logger.info("\n=== Per Season ===")
    for name, season_id, total, has_a in rows:
        pct = has_a / total * 100 if total else 0
        missing = total - has_a
        logger.info("  s%d %-40s  total=%5d  amplua=%5d (%.0f%%)  missing=%d", season_id, name, total, has_a, pct, missing)

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Backfill game_lineups positions")
    parser.add_argument(
        "--step",
        choices=["vsporte-ids", "all"],
        default="all",
        help="Which step to run (default: all)",
    )
    parser.add_argument(
        "--source",
        choices=["api", "top_role", "legacy", "all"],
        default="all",
        help="Which data source to use for backfill (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update the database")
    parser.add_argument("--stats", action="store_true", help="Print current stats and exit")
    args = parser.parse_args()

    if args.stats:
        print_stats()
        return

    if args.step == "vsporte-ids":
        step_vsporte_ids(dry_run=args.dry_run)
        return

    # Full backfill
    logger.info("=== BEFORE ===")
    print_stats()
    logger.info("")

    if args.source in ("api", "all"):
        logger.info("--- Step 1: API backfill (VSporte + SOTA) ---")
        asyncio.run(step_api_backfill(dry_run=args.dry_run))
        logger.info("")

    if args.source in ("top_role", "all"):
        logger.info("--- Step 2: players.top_role backfill ---")
        step_top_role_backfill(dry_run=args.dry_run)
        logger.info("")

    if args.source in ("legacy", "all"):
        logger.info("--- Step 3: legacy contracts.amplua backfill ---")
        step_legacy_backfill(dry_run=args.dry_run)
        logger.info("")

    logger.info("=== AFTER ===")
    print_stats()


if __name__ == "__main__":
    main()
