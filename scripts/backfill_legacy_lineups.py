#!/usr/bin/env python3
"""
One-time legacy lineup backfill into PostgreSQL.

Priority (per team):
1) matches.team1_squad / matches.team2_squad
2) sota.id /em/{sota_code}-team-{home|away}.json
3) VSporte files/{vsporte_id}_team_{host|guest}.json
4) matches_players (list-only fallback, no field coordinates)

This script is intentionally one-time and should be executed only during
legacy access window. Production runtime must not depend on legacy MySQL.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable
from uuid import UUID

import httpx
import psycopg2
from psycopg2.extras import DictCursor, execute_values
import pymysql
from sqlalchemy.engine.url import make_url

from app.config import get_settings
from app.utils.lineup_feed_parser import parse_maybe_json_array, parse_team_lineup_feed

LOGGER = logging.getLogger("backfill_legacy_lineups")

SOURCE_PRIORITY = {
    "team_squad": 0,
    "sota_api": 1,
    "vsporte_api": 2,
    "matches_players": 3,
    "none": 4,
}
ALLOWED_CHAMPIONSHIPS = {1, 2, 3}
CUTOFF_DATE = date(2025, 6, 1)
VALID_AMPLUA = {"Gk", "D", "DM", "M", "AM", "F"}
VALID_FIELD_POSITION = {"L", "LC", "C", "RC", "R"}
HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


@dataclass(slots=True)
class LegacyPerson:
    legacy_id: int
    sota_id: str | None
    first_name: str
    last_name: str


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


def build_name_key(first_name: str | None, last_name: str | None) -> tuple[str, str]:
    return normalize_name(first_name), normalize_name(last_name)


def split_full_name(full_name: str | None) -> tuple[str, str]:
    normalized = re.sub(r"\s+", " ", (full_name or "").strip())
    if not normalized:
        return "", ""
    parts = normalized.split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def normalize_uuid_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return str(UUID(text))
    except ValueError:
        return None


def normalize_kit_color(raw_color: Any) -> str | None:
    if not isinstance(raw_color, str):
        return None
    value = raw_color.strip()
    if HEX_COLOR_RE.match(value):
        return value.upper()
    return None


def normalize_amplua(amplua: Any, gk: bool = False) -> str | None:
    if gk:
        return "Gk"
    if not isinstance(amplua, str):
        return None
    value = amplua.strip()
    if value == "GK":
        value = "Gk"
    if value in VALID_AMPLUA:
        return value
    return None


def normalize_field_position(position: Any) -> str | None:
    if not isinstance(position, str):
        return None
    value = position.strip().upper()
    return value if value in VALID_FIELD_POSITION else None


def is_team_valid_for_field(starters: list[dict[str, Any]]) -> bool:
    if len(starters) < 11:
        return False
    for player in starters[:11]:
        if player.get("amplua") not in VALID_AMPLUA:
            return False
        if player.get("field_position") not in VALID_FIELD_POSITION:
            return False
    return True


def compute_render_mode(
    *,
    has_lineup: bool,
    field_allowed_by_rules: bool,
    home_valid: bool,
    away_valid: bool,
) -> str:
    if not has_lineup:
        return "hidden"
    if field_allowed_by_rules and home_valid and away_valid:
        return "field"
    return "list"


def pick_game_source(sources: Iterable[str], has_lineup: bool) -> str:
    if not has_lineup:
        return "none"
    normalized = [s for s in sources if s in SOURCE_PRIORITY and s != "none"]
    if not normalized:
        return "none"
    return min(normalized, key=lambda source: SOURCE_PRIORITY[source])


def chunked(values: list[int], size: int) -> Iterable[list[int]]:
    safe_size = max(1, size)
    for index in range(0, len(values), safe_size):
        yield values[index:index + safe_size]


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


def fetch_pg_games(
    pg_conn,
    *,
    season_id: int | None,
    game_ids: list[int] | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    where_parts = ["g.legacy_id IS NOT NULL"]
    params: list[Any] = []

    if season_id is not None:
        where_parts.append("g.season_id = %s")
        params.append(season_id)

    if game_ids:
        where_parts.append("g.id = ANY(%s)")
        params.append(game_ids)

    where_sql = " AND ".join(where_parts)
    limit_sql = "LIMIT %s" if limit is not None else ""
    if limit is not None:
        params.append(limit)

    query = f"""
        SELECT
            g.id,
            g.legacy_id,
            g.date,
            g.home_team_id,
            g.away_team_id,
            g.home_formation,
            g.away_formation,
            g.home_kit_color,
            g.away_kit_color,
            g.has_lineup,
            g.lineup_source,
            g.lineup_render_mode,
            c.id AS championship_id,
            c.legacy_id AS championship_legacy_id
        FROM games g
        LEFT JOIN seasons s ON s.id = g.season_id
        LEFT JOIN tournaments t ON t.id = s.tournament_id
        LEFT JOIN championships c ON c.id = t.championship_id
        WHERE {where_sql}
        ORDER BY g.id
        {limit_sql}
    """

    with pg_conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def fetch_legacy_matches(
    legacy_conn,
    legacy_ids: list[int],
    *,
    include_team_squads: bool,
) -> dict[int, dict[str, Any]]:
    if not legacy_ids:
        return {}

    result: dict[int, dict[str, Any]] = {}
    select_columns = [
        "id",
        "datetime",
        "status",
        "command1",
        "command2",
    ]
    if include_team_squads:
        select_columns.extend(["team1_squad", "team2_squad"])
    else:
        select_columns.extend(["NULL AS team1_squad", "NULL AS team2_squad"])
    select_columns.extend(["sota_code", "vsporte_id"])

    with legacy_conn.cursor() as cursor:
        for group in chunked(legacy_ids, 700):
            placeholders = ",".join(["%s"] * len(group))
            cursor.execute(
                f"""
                SELECT
                    {", ".join(select_columns)}
                FROM matches
                WHERE status != 0
                  AND id IN ({placeholders})
                """,
                group,
            )
            for row in cursor.fetchall():
                result[int(row["id"])] = row
    return result


def fetch_legacy_match_players(
    legacy_conn,
    legacy_ids: list[int],
) -> dict[int, dict[int, list[dict[str, Any]]]]:
    if not legacy_ids:
        return {}

    result: dict[int, dict[int, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    with legacy_conn.cursor() as cursor:
        for group in chunked(legacy_ids, 500):
            placeholders = ",".join(["%s"] * len(group))
            cursor.execute(
                f"""
                SELECT
                    mp.id,
                    mp.match_id,
                    mp.people_id,
                    mp.command_id,
                    mp.number,
                    mp.captain,
                    mp.line,
                    mp.position,
                    mp.name,
                    p.sota_id,
                    p.name_kz,
                    p.surname_kz,
                    p.name_ru,
                    p.surname_ru,
                    p.name_en,
                    p.surname_en
                FROM matches_players mp
                LEFT JOIN people p ON p.id = mp.people_id
                WHERE mp.status = 1
                  AND mp.match_id IN ({placeholders})
                ORDER BY mp.match_id, mp.command_id, mp.line, mp.position, mp.id
                """,
                group,
            )
            for row in cursor.fetchall():
                match_id = int(row["match_id"])
                command_id = int(row["command_id"])
                result[match_id][command_id].append(row)
    return result


def fetch_legacy_people(
    legacy_conn,
) -> tuple[
    dict[int, LegacyPerson],
    dict[str, LegacyPerson],
    dict[tuple[str, str], LegacyPerson],
]:
    by_id: dict[int, LegacyPerson] = {}
    by_sota: dict[str, LegacyPerson] = {}
    by_name: dict[tuple[str, str], LegacyPerson] = {}

    with legacy_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                sota_id,
                name_kz,
                surname_kz,
                name_ru,
                surname_ru,
                name_en,
                surname_en
            FROM people
            WHERE status = 1
            """
        )
        for row in cursor.fetchall():
            legacy_id = int(row["id"])
            first_name = (
                row.get("name_kz")
                or row.get("name_ru")
                or row.get("name_en")
                or ""
            )
            last_name = (
                row.get("surname_kz")
                or row.get("surname_ru")
                or row.get("surname_en")
                or ""
            )
            person = LegacyPerson(
                legacy_id=legacy_id,
                sota_id=normalize_uuid_text(row.get("sota_id")),
                first_name=str(first_name).strip(),
                last_name=str(last_name).strip(),
            )
            by_id[legacy_id] = person

            if person.sota_id:
                by_sota.setdefault(person.sota_id, person)

            key = build_name_key(person.first_name, person.last_name)
            if key != ("", ""):
                by_name.setdefault(key, person)

    return by_id, by_sota, by_name


def legacy_table_columns(legacy_conn, table_name: str) -> set[str]:
    columns: set[str] = set()
    with legacy_conn.cursor() as cursor:
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        for row in cursor.fetchall():
            column = row.get("Field")
            if isinstance(column, str):
                columns.add(column)
    return columns


def fetch_pg_players(
    pg_conn,
) -> tuple[
    dict[int, int],
    dict[str, int],
    dict[tuple[str, str], int],
]:
    by_legacy: dict[int, int] = {}
    by_sota: dict[str, int] = {}
    by_name: dict[tuple[str, str], int] = {}

    with pg_conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(
            """
            SELECT
                id,
                legacy_id,
                CAST(sota_id AS TEXT) AS sota_id,
                first_name,
                last_name
            FROM players
            """
        )
        for row in cursor.fetchall():
            player_id = int(row["id"])
            legacy_id = row.get("legacy_id")
            sota_id = normalize_uuid_text(row.get("sota_id"))
            first_name = row.get("first_name") or ""
            last_name = row.get("last_name") or ""

            if legacy_id is not None:
                by_legacy.setdefault(int(legacy_id), player_id)
            if sota_id:
                by_sota.setdefault(sota_id, player_id)

            key = build_name_key(str(first_name), str(last_name))
            if key != ("", ""):
                by_name.setdefault(key, player_id)

    return by_legacy, by_sota, by_name


def empty_team_payload() -> dict[str, Any]:
    return {
        "main": [],
        "subs": [],
        "assistants": [],
        "coach": None,
        "team_name": None,
        "formation_string": None,
        "formation_color": None,
        "has_starting_marker": False,
        "has_subs_marker": False,
        "is_valid_for_field": False,
    }


def fallback_team_payload_from_matches_players(rows: list[dict[str, Any]]) -> dict[str, Any]:
    main: list[dict[str, Any]] = []
    subs: list[dict[str, Any]] = []

    for row in rows:
        raw_name = (row.get("name") or "").strip()
        first_name = (
            row.get("name_kz")
            or row.get("name_ru")
            or row.get("name_en")
            or ""
        ).strip()
        last_name = (
            row.get("surname_kz")
            or row.get("surname_ru")
            or row.get("surname_en")
            or ""
        ).strip()
        if not first_name and not last_name:
            parsed_first, parsed_last = split_full_name(raw_name)
            first_name = parsed_first
            last_name = parsed_last

        player = {
            "number": int(row["number"]) if row.get("number") is not None else None,
            "name": raw_name or f"{first_name} {last_name}".strip(),
            "first_name": first_name,
            "last_name": last_name,
            "amplua": "",
            "position": "",
            "id": normalize_uuid_text(row.get("sota_id")) or "",
            "legacy_people_id": int(row["people_id"]) if row.get("people_id") is not None else None,
            "gk": False,
            "capitan": bool(int(row.get("captain") or 0)),
        }

        lineup_line = int(row.get("line") or 0)
        if lineup_line == 1:
            main.append(player)
        elif lineup_line == 2:
            subs.append(player)

    return {
        "main": main,
        "subs": subs,
        "assistants": [],
        "coach": None,
        "team_name": None,
        "formation_string": None,
        "formation_color": None,
        "has_starting_marker": bool(main),
        "has_subs_marker": bool(subs),
        "is_valid_for_field": False,
    }


def pick_team_source(
    *,
    team_squad_raw: Any,
    sota_payload: list[dict[str, Any]] | None,
    vsporte_payload: list[dict[str, Any]] | None,
    fallback_rows: list[dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    team_squad = parse_maybe_json_array(team_squad_raw)
    if team_squad:
        return "team_squad", parse_team_lineup_feed(team_squad)

    if isinstance(sota_payload, list) and len(sota_payload) > 0:
        return "sota_api", parse_team_lineup_feed(sota_payload)

    if isinstance(vsporte_payload, list) and len(vsporte_payload) > 0:
        return "vsporte_api", parse_team_lineup_feed(vsporte_payload)

    if fallback_rows:
        return "matches_players", fallback_team_payload_from_matches_players(fallback_rows)

    return "none", empty_team_payload()


def sanitize_player_entry(entry: dict[str, Any]) -> dict[str, Any]:
    legacy_people_id = entry.get("legacy_people_id")
    if legacy_people_id is not None:
        try:
            legacy_people_id = int(legacy_people_id)
        except (TypeError, ValueError):
            legacy_people_id = None

    first_name = (entry.get("first_name") or "").strip()
    last_name = (entry.get("last_name") or "").strip()
    if not first_name and not last_name:
        parsed_first, parsed_last = split_full_name(entry.get("name") or "")
        first_name = parsed_first
        last_name = parsed_last

    return {
        "number": entry.get("number"),
        "first_name": first_name,
        "last_name": last_name,
        "name": (entry.get("name") or f"{first_name} {last_name}".strip() or "").strip(),
        "amplua": normalize_amplua(entry.get("amplua"), gk=bool(entry.get("gk"))),
        "field_position": normalize_field_position(entry.get("position")),
        "is_captain": bool(entry.get("capitan") or entry.get("captain")),
        "sota_id": normalize_uuid_text(entry.get("id")),
        "legacy_people_id": legacy_people_id,
    }


class FeedFetcher:
    def __init__(self, timeout_seconds: float = 8.0):
        self.client = httpx.Client(timeout=timeout_seconds, follow_redirects=True)
        self.sota_cache: dict[tuple[str, str], list[dict[str, Any]] | None] = {}
        self.vsporte_cache: dict[tuple[str, str], list[dict[str, Any]] | None] = {}

    def close(self):
        self.client.close()

    def fetch_sota(self, sota_code: str, side: str) -> list[dict[str, Any]] | None:
        key = (sota_code, side)
        if key in self.sota_cache:
            return self.sota_cache[key]

        url = f"https://sota.id/em/{sota_code}-team-{side}.json"
        try:
            response = self.client.get(url)
            response.raise_for_status()
            payload = response.json()
            value = payload if isinstance(payload, list) else None
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("SOTA fetch failed %s %s: %s", sota_code, side, exc)
            value = None

        self.sota_cache[key] = value
        return value

    def fetch_vsporte(self, vsporte_id: str, side: str) -> list[dict[str, Any]] | None:
        key = (vsporte_id, side)
        if key in self.vsporte_cache:
            return self.vsporte_cache[key]

        url = f"https://broadcast.vsporte.ru/api/v2/qfl/files/{vsporte_id}_team_{side}.json"
        try:
            response = self.client.get(url)
            response.raise_for_status()
            payload = response.json()
            value = payload if isinstance(payload, list) else None
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("VSporte fetch failed %s %s: %s", vsporte_id, side, exc)
            value = None

        self.vsporte_cache[key] = value
        return value


class PlayerResolver:
    def __init__(
        self,
        *,
        pg_by_legacy: dict[int, int],
        pg_by_sota: dict[str, int],
        pg_by_name: dict[tuple[str, str], int],
        legacy_people_by_id: dict[int, LegacyPerson],
        legacy_people_by_sota: dict[str, LegacyPerson],
        legacy_people_by_name: dict[tuple[str, str], LegacyPerson],
        create_missing_players: bool,
    ):
        self.pg_by_legacy = pg_by_legacy
        self.pg_by_sota = pg_by_sota
        self.pg_by_name = pg_by_name
        self.legacy_people_by_id = legacy_people_by_id
        self.legacy_people_by_sota = legacy_people_by_sota
        self.legacy_people_by_name = legacy_people_by_name
        self.create_missing_players = create_missing_players
        self.players_created = 0

    def resolve(self, cursor, candidate: dict[str, Any]) -> int | None:
        legacy_people_id = candidate.get("legacy_people_id")
        sota_id = normalize_uuid_text(candidate.get("sota_id"))
        first_name = (candidate.get("first_name") or "").strip()
        last_name = (candidate.get("last_name") or "").strip()

        legacy_person: LegacyPerson | None = None

        if isinstance(legacy_people_id, int):
            player_id = self.pg_by_legacy.get(legacy_people_id)
            if player_id is not None:
                return player_id
            legacy_person = self.legacy_people_by_id.get(legacy_people_id)

        if sota_id:
            player_id = self.pg_by_sota.get(sota_id)
            if player_id is not None:
                return player_id
            if legacy_person is None:
                legacy_person = self.legacy_people_by_sota.get(sota_id)
            if legacy_person is not None:
                player_id = self.pg_by_legacy.get(legacy_person.legacy_id)
                if player_id is not None:
                    return player_id

        name_key = build_name_key(first_name, last_name)
        if name_key != ("", ""):
            player_id = self.pg_by_name.get(name_key)
            if player_id is not None:
                return player_id
            if legacy_person is None:
                legacy_person = self.legacy_people_by_name.get(name_key)
            if legacy_person is not None:
                player_id = self.pg_by_legacy.get(legacy_person.legacy_id)
                if player_id is not None:
                    return player_id

        if not self.create_missing_players:
            return None

        if legacy_person is None:
            if isinstance(legacy_people_id, int):
                legacy_person = self.legacy_people_by_id.get(legacy_people_id)
            if legacy_person is None and sota_id:
                legacy_person = self.legacy_people_by_sota.get(sota_id)
            if legacy_person is None and name_key != ("", ""):
                legacy_person = self.legacy_people_by_name.get(name_key)

        new_legacy_id = legacy_person.legacy_id if legacy_person else legacy_people_id
        new_sota_id = sota_id or (legacy_person.sota_id if legacy_person else None)
        new_first_name = first_name or (legacy_person.first_name if legacy_person else "")
        new_last_name = last_name or (legacy_person.last_name if legacy_person else "")

        if not new_first_name and not new_last_name:
            parsed_first, parsed_last = split_full_name(candidate.get("name"))
            new_first_name = parsed_first
            new_last_name = parsed_last

        cursor.execute(
            """
            INSERT INTO players (legacy_id, sota_id, first_name, last_name, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (
                new_legacy_id if isinstance(new_legacy_id, int) else None,
                new_sota_id,
                new_first_name or None,
                new_last_name or None,
            ),
        )
        inserted = cursor.fetchone()
        if inserted:
            player_id = int(inserted[0])
            self.players_created += 1
        else:
            player_id = None
            if isinstance(new_legacy_id, int):
                player_id = self.pg_by_legacy.get(new_legacy_id)
                if player_id is None:
                    cursor.execute("SELECT id FROM players WHERE legacy_id = %s", (new_legacy_id,))
                    row = cursor.fetchone()
                    if row:
                        player_id = int(row[0])
            if player_id is None and new_sota_id:
                player_id = self.pg_by_sota.get(new_sota_id)
                if player_id is None:
                    cursor.execute("SELECT id FROM players WHERE sota_id = %s::uuid", (new_sota_id,))
                    row = cursor.fetchone()
                    if row:
                        player_id = int(row[0])

        if player_id is None:
            return None

        if isinstance(new_legacy_id, int):
            self.pg_by_legacy[new_legacy_id] = player_id
        if new_sota_id:
            self.pg_by_sota[new_sota_id] = player_id

        name_key = build_name_key(new_first_name, new_last_name)
        if name_key != ("", ""):
            self.pg_by_name.setdefault(name_key, player_id)

        return player_id


def build_team_lineup_rows(
    *,
    team_id: int | None,
    team_payload: dict[str, Any],
    lineup_type: str,
    resolver: PlayerResolver,
    cursor,
) -> tuple[list[tuple[Any, ...]], list[dict[str, Any]], int]:
    if team_id is None:
        return [], [], 0

    raw_players = team_payload.get("main") if lineup_type == "starter" else team_payload.get("subs")
    raw_players = raw_players or []

    rows_by_player: dict[int, tuple[Any, ...]] = {}
    starters_for_validation: list[dict[str, Any]] = []
    unresolved = 0

    for raw_entry in raw_players:
        entry = sanitize_player_entry(raw_entry)
        shirt_number = entry.get("number")
        if shirt_number is not None:
            try:
                shirt_number = int(shirt_number)
            except (TypeError, ValueError):
                shirt_number = None

        player_id = resolver.resolve(cursor, entry)
        if player_id is None:
            unresolved += 1
            continue

        row = (
            team_id,
            player_id,
            lineup_type,
            shirt_number,
            bool(entry.get("is_captain")),
            entry.get("amplua"),
            entry.get("field_position"),
        )

        existing = rows_by_player.get(player_id)
        if existing is not None:
            if existing[2] == "substitute" and lineup_type == "starter":
                rows_by_player[player_id] = row
            continue

        rows_by_player[player_id] = row

        if lineup_type == "starter":
            starters_for_validation.append(
                {
                    "amplua": entry.get("amplua"),
                    "field_position": entry.get("field_position"),
                }
            )

    return list(rows_by_player.values()), starters_for_validation, unresolved


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-time backfill of legacy lineups")
    parser.add_argument("--dry-run", action="store_true", help="Read and calculate only, do not write")
    parser.add_argument("--season-id", type=int, help="Process only one PostgreSQL season_id")
    parser.add_argument(
        "--game-id",
        type=int,
        action="append",
        help="Process only selected game id(s); can be repeated",
    )
    parser.add_argument("--limit", type=int, help="Limit number of processed games")
    parser.add_argument("--batch-size", type=int, default=200, help="Commit batch size")
    parser.add_argument(
        "--no-create-missing-players",
        action="store_true",
        help="Do not create missing players in PostgreSQL",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logs",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    pg_conn = get_pg_connection()
    legacy_conn = get_legacy_connection()
    fetcher = FeedFetcher()

    try:
        pg_games = fetch_pg_games(
            pg_conn,
            season_id=args.season_id,
            game_ids=args.game_id,
            limit=args.limit,
        )
        if not pg_games:
            LOGGER.info("No candidate PostgreSQL games found")
            return 0

        legacy_ids = [int(game["legacy_id"]) for game in pg_games if game.get("legacy_id") is not None]
        matches_columns = legacy_table_columns(legacy_conn, "matches")
        has_team_squad_columns = {"team1_squad", "team2_squad"}.issubset(matches_columns)
        if not has_team_squad_columns:
            LOGGER.warning(
                "Legacy matches has no team1_squad/team2_squad columns; using API/fallback sources only"
            )

        legacy_matches = fetch_legacy_matches(
            legacy_conn,
            legacy_ids,
            include_team_squads=has_team_squad_columns,
        )
        legacy_match_players = fetch_legacy_match_players(legacy_conn, legacy_ids)
        (
            legacy_people_by_id,
            legacy_people_by_sota,
            legacy_people_by_name,
        ) = fetch_legacy_people(legacy_conn)
        pg_by_legacy, pg_by_sota, pg_by_name = fetch_pg_players(pg_conn)

        resolver = PlayerResolver(
            pg_by_legacy=pg_by_legacy,
            pg_by_sota=pg_by_sota,
            pg_by_name=pg_by_name,
            legacy_people_by_id=legacy_people_by_id,
            legacy_people_by_sota=legacy_people_by_sota,
            legacy_people_by_name=legacy_people_by_name,
            create_missing_players=not args.no_create_missing_players,
        )

        summary = Counter()
        source_counter = Counter()
        render_counter = Counter()
        unresolved_counter = Counter()

        with pg_conn.cursor() as cursor:
            for index, game in enumerate(pg_games, start=1):
                game_id = int(game["id"])
                legacy_match = legacy_matches.get(int(game["legacy_id"]))
                summary["games_seen"] += 1

                if not legacy_match:
                    summary["missing_legacy_match"] += 1
                    continue

                legacy_match_id = int(legacy_match["id"])
                home_legacy_team_id = int(legacy_match["command1"]) if legacy_match.get("command1") is not None else None
                away_legacy_team_id = int(legacy_match["command2"]) if legacy_match.get("command2") is not None else None

                fallback_by_team = legacy_match_players.get(legacy_match_id, {})
                fallback_home_rows = fallback_by_team.get(home_legacy_team_id or -1, [])
                fallback_away_rows = fallback_by_team.get(away_legacy_team_id or -1, [])

                home_sota = None
                away_sota = None
                sota_code = (legacy_match.get("sota_code") or "").strip()
                if sota_code:
                    home_sota = fetcher.fetch_sota(sota_code, "home")
                    away_sota = fetcher.fetch_sota(sota_code, "away")

                home_vsporte = None
                away_vsporte = None
                vsporte_id = (legacy_match.get("vsporte_id") or "").strip()
                if vsporte_id:
                    home_vsporte = fetcher.fetch_vsporte(vsporte_id, "host")
                    away_vsporte = fetcher.fetch_vsporte(vsporte_id, "guest")

                home_source, home_payload = pick_team_source(
                    team_squad_raw=legacy_match.get("team1_squad"),
                    sota_payload=home_sota,
                    vsporte_payload=home_vsporte,
                    fallback_rows=fallback_home_rows,
                )
                away_source, away_payload = pick_team_source(
                    team_squad_raw=legacy_match.get("team2_squad"),
                    sota_payload=away_sota,
                    vsporte_payload=away_vsporte,
                    fallback_rows=fallback_away_rows,
                )

                home_starter_rows, home_validation_players, home_unresolved = build_team_lineup_rows(
                    team_id=game.get("home_team_id"),
                    team_payload=home_payload,
                    lineup_type="starter",
                    resolver=resolver,
                    cursor=cursor,
                )
                home_sub_rows, _, home_sub_unresolved = build_team_lineup_rows(
                    team_id=game.get("home_team_id"),
                    team_payload=home_payload,
                    lineup_type="substitute",
                    resolver=resolver,
                    cursor=cursor,
                )
                away_starter_rows, away_validation_players, away_unresolved = build_team_lineup_rows(
                    team_id=game.get("away_team_id"),
                    team_payload=away_payload,
                    lineup_type="starter",
                    resolver=resolver,
                    cursor=cursor,
                )
                away_sub_rows, _, away_sub_unresolved = build_team_lineup_rows(
                    team_id=game.get("away_team_id"),
                    team_payload=away_payload,
                    lineup_type="substitute",
                    resolver=resolver,
                    cursor=cursor,
                )

                unresolved_total = home_unresolved + home_sub_unresolved + away_unresolved + away_sub_unresolved
                if unresolved_total:
                    unresolved_counter["players"] += unresolved_total

                deduped_rows: dict[int, tuple[Any, ...]] = {}
                for row in home_starter_rows + home_sub_rows + away_starter_rows + away_sub_rows:
                    player_id = row[1]
                    existing = deduped_rows.get(player_id)
                    if existing is None:
                        deduped_rows[player_id] = row
                        continue

                    existing_is_starter = existing[2] == "starter"
                    row_is_starter = row[2] == "starter"
                    existing_has_position = bool(existing[5] and existing[6])
                    row_has_position = bool(row[5] and row[6])

                    # Keep one row per game+player to satisfy uq_game_lineup_player.
                    # Prefer starters and then rows with explicit field coordinates.
                    if (not existing_is_starter and row_is_starter) or (
                        existing_is_starter == row_is_starter and not existing_has_position and row_has_position
                    ):
                        deduped_rows[player_id] = row

                all_rows = list(deduped_rows.values())
                has_lineup = len(all_rows) > 0

                championship_gate_id = game.get("championship_legacy_id") or game.get("championship_id")
                field_allowed_by_rules = (
                    championship_gate_id in ALLOWED_CHAMPIONSHIPS
                    and game.get("date") is not None
                    and game["date"] >= CUTOFF_DATE
                )
                home_valid = is_team_valid_for_field(home_validation_players)
                away_valid = is_team_valid_for_field(away_validation_players)
                render_mode = compute_render_mode(
                    has_lineup=has_lineup,
                    field_allowed_by_rules=field_allowed_by_rules,
                    home_valid=home_valid,
                    away_valid=away_valid,
                )
                source = pick_game_source((home_source, away_source), has_lineup=has_lineup)

                home_formation = (home_payload.get("formation_string") or "").strip() or None
                away_formation = (away_payload.get("formation_string") or "").strip() or None
                home_kit_color = normalize_kit_color(home_payload.get("formation_color"))
                away_kit_color = normalize_kit_color(away_payload.get("formation_color"))

                summary["games_processed"] += 1
                summary["lineup_rows"] += len(all_rows)
                source_counter[source] += 1
                render_counter[render_mode] += 1

                if not args.dry_run:
                    cursor.execute("DELETE FROM game_lineups WHERE game_id = %s", (game_id,))

                    if all_rows:
                        execute_values(
                            cursor,
                            """
                            INSERT INTO game_lineups
                                (game_id, team_id, player_id, lineup_type, shirt_number, is_captain, amplua, field_position)
                            VALUES %s
                            """,
                            [
                                (
                                    game_id,
                                    row[0],
                                    row[1],
                                    row[2],
                                    row[3],
                                    row[4],
                                    row[5],
                                    row[6],
                                )
                                for row in all_rows
                            ],
                        )

                    cursor.execute(
                        """
                        UPDATE games
                        SET
                            has_lineup = %s,
                            home_formation = %s,
                            away_formation = %s,
                            home_kit_color = %s,
                            away_kit_color = %s,
                            lineup_source = %s,
                            lineup_render_mode = %s,
                            lineup_backfilled_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            has_lineup,
                            home_formation,
                            away_formation,
                            home_kit_color,
                            away_kit_color,
                            source,
                            render_mode,
                            game_id,
                        ),
                    )

                if not args.dry_run and index % max(1, args.batch_size) == 0:
                    pg_conn.commit()
                    LOGGER.info("Committed %s/%s games", index, len(pg_games))

            if not args.dry_run:
                pg_conn.commit()

        summary["players_created"] = resolver.players_created
        summary["unresolved_players"] = unresolved_counter.get("players", 0)

        prefix = "[DRY-RUN] " if args.dry_run else ""
        LOGGER.info("%sProcessed games: %d", prefix, summary["games_processed"])
        LOGGER.info("%sMissing legacy match rows: %d", prefix, summary["missing_legacy_match"])
        LOGGER.info("%sInserted lineup rows: %d", prefix, summary["lineup_rows"])
        LOGGER.info("%sCreated players: %d", prefix, summary["players_created"])
        LOGGER.info("%sUnresolved players: %d", prefix, summary["unresolved_players"])
        LOGGER.info("%sSource breakdown: %s", prefix, dict(source_counter))
        LOGGER.info("%sRender mode breakdown: %s", prefix, dict(render_counter))
        return 0
    finally:
        fetcher.close()
        legacy_conn.close()
        pg_conn.close()


def main():
    raise SystemExit(run())


if __name__ == "__main__":
    main()
